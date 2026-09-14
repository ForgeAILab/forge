use std::{fmt::Display, process::ExitCode, sync::Arc, time::Duration};

use clap::Parser;
use forge_solo::{
    app::AgentCandidate,
    backend::{ShutdownIntent, SoloBackend},
    cli::Cli,
    controller::{ControllerConfig, ControllerExit, SoloController},
    runtime_backend::RuntimeBackend,
    startup::{SoloStartup, StartupError},
    tui::{
        CrosstermEventSource, TuiHost, DEFAULT_EVENT_CHANNEL_CAPACITY, DEFAULT_EVENT_POLL_INTERVAL,
    },
    ui_bridge::AppReducer,
    view,
};

const FORCED_SHUTDOWN_TIMEOUT: Duration = Duration::from_millis(500);

/// Forge's tool -> service -> repository -> `sqlx` call chains compose deep
/// async futures, and a debug build makes each frame larger still. The server
/// binary already raises its worker stack for exactly this reason; Solo runs
/// the same services in-process, so it must not inherit Tokio's 2 MiB default.
const RUNTIME_THREAD_STACK_SIZE: usize = 16 * 1024 * 1024;

fn main() -> ExitCode {
    let runtime = match tokio::runtime::Builder::new_multi_thread()
        .thread_stack_size(RUNTIME_THREAD_STACK_SIZE)
        .enable_all()
        .build()
    {
        Ok(runtime) => runtime,
        Err(error) => {
            eprintln!("forge-solo: failed to build the Forge runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    runtime.block_on(async {
        match run(Cli::parse()).await {
            Ok(()) => ExitCode::SUCCESS,
            Err(error) => {
                eprintln!("forge-solo: {}", safe_diagnostic(error));
                ExitCode::FAILURE
            }
        }
    })
}

async fn run(cli: Cli) -> Result<(), String> {
    let mut startup = SoloStartup::initialize(cli)
        .await
        .map_err(|error| format_startup_error(&error))?;
    let candidates = startup
        .bootstrap
        .agent_candidates
        .iter()
        .map(to_ui_candidate)
        .collect();
    let reducer = AppReducer::default().with_setup_candidates(candidates);

    let mut host = match TuiHost::enter() {
        Ok(host) => host,
        Err(error) => {
            shutdown_after_startup_failure(&startup.backend).await;
            return Err(format!(
                "could not enter the terminal UI: {}",
                safe_diagnostic(error)
            ));
        }
    };
    let input = match CrosstermEventSource::with_termination_signals(
        DEFAULT_EVENT_CHANNEL_CAPACITY,
        DEFAULT_EVENT_POLL_INTERVAL,
    ) {
        Ok(input) => input,
        Err(error) => {
            let _ = host.restore();
            shutdown_after_startup_failure(&startup.backend).await;
            return Err(format!(
                "could not initialize terminal input: {}",
                safe_diagnostic(error)
            ));
        }
    };
    let events = match startup.take_event_source() {
        Some(events) => events,
        None => {
            let _ = host.restore();
            shutdown_after_startup_failure(&startup.backend).await;
            return Err("Solo runtime event source was already consumed".to_owned());
        }
    };

    let backend = Arc::clone(&startup.backend);
    let mut controller =
        SoloController::new_shared(backend, input, events, reducer, ControllerConfig::default())
            .with_render_hook(
                host.render_hook::<AppReducer, _>(|frame, reducer, _status| {
                    view::render(frame, reducer.state());
                }),
            );
    let exit = controller.run().await;
    // Stop the blocking Crossterm reader before restoring the terminal. The
    // controller owns the input/event sources, so dropping it here also
    // releases their bounded channels on every exit path.
    drop(controller);
    let render_error = host.take_render_error();

    // A forced controller exit aborts its in-flight shutdown future. Give the
    // backend one short final chance to stop workers and release the daemon;
    // durable crash recovery handles anything that remains.
    let needs_forced_shutdown = matches!(&exit, ControllerExit::Forced { .. })
        || matches!(&exit, ControllerExit::Graceful(outcome) if outcome.timed_out)
        || matches!(&exit, ControllerExit::ShutdownFailed(_));
    if needs_forced_shutdown {
        let _ = tokio::time::timeout(
            FORCED_SHUTDOWN_TIMEOUT,
            startup
                .backend
                .shutdown(ShutdownIntent::Signal, FORCED_SHUTDOWN_TIMEOUT),
        )
        .await;
    }

    let restore_error = host.restore().err();
    if let Some(error) = restore_error {
        return Err(format!(
            "could not restore the terminal: {}",
            safe_diagnostic(error)
        ));
    }
    if let Some(error) = render_error {
        return Err(format!(
            "terminal rendering failed: {}",
            safe_diagnostic(error)
        ));
    }

    match exit {
        ControllerExit::Graceful(outcome) if outcome.timed_out => {
            eprintln!(
                "forge-solo: shutdown timed out with {} active operation(s); durable recovery will run on next launch",
                outcome.active_operations
            );
            Ok(())
        }
        ControllerExit::Graceful(_) | ControllerExit::Forced { .. } => Ok(()),
        ControllerExit::ShutdownFailed(error) => Err(format!(
            "runtime shutdown failed: {}",
            if error.is_public() {
                safe_diagnostic(error)
            } else {
                "inspect the Solo log for a protected runtime failure".to_owned()
            }
        )),
    }
}

async fn shutdown_after_startup_failure(backend: &Arc<RuntimeBackend>) {
    let _ = tokio::time::timeout(
        FORCED_SHUTDOWN_TIMEOUT,
        backend.shutdown(ShutdownIntent::StartupFailure, FORCED_SHUTDOWN_TIMEOUT),
    )
    .await;
}

fn to_ui_candidate(candidate: &services::solo_bootstrap::SoloAgentCandidate) -> AgentCandidate {
    AgentCandidate {
        id: candidate.identity_id.clone(),
        label: candidate.display_name.clone(),
        kind: candidate.executor_type.clone(),
        available: candidate.eligible,
        authenticated: matches!(
            candidate.availability,
            services::solo_bootstrap::SoloAgentAvailability::Authenticated
        ),
        detail: candidate
            .reason
            .clone()
            .or_else(|| candidate.next_step.clone())
            .unwrap_or_else(|| "authenticated local CLI harness".to_owned()),
    }
}

fn format_startup_error(error: &StartupError) -> String {
    match error {
        StartupError::AgentUnavailable { executor } => format!(
            "Agent `{executor}` is unavailable or unauthenticated; complete its CLI login and retry"
        ),
        StartupError::DaemonReadiness => {
            "the embedded local Agent Runtime did not become ready; retry after checking the local CLI installation".to_owned()
        }
        StartupError::Backend(error) if error.is_public() => {
            format!("Solo backend failed: {}", safe_diagnostic(error))
        }
        StartupError::Backend(_) => {
            "Solo backend failed; inspect the local Solo log for details".to_owned()
        }
        StartupError::Db(_) | StartupError::Services(_) | StartupError::Tracing(_) => {
            "Solo startup failed; inspect the local Solo log for details".to_owned()
        }
        other => safe_diagnostic(other),
    }
}

const MAX_DIAGNOSTIC_CHARS: usize = 2_048;

fn safe_diagnostic(detail: impl Display) -> String {
    let mut result = String::new();
    for character in detail.to_string().chars() {
        if character.is_control() {
            result.push(' ');
        } else {
            result.push(character);
        }
        if result.chars().count() >= MAX_DIAGNOSTIC_CHARS {
            break;
        }
    }
    if result.trim().is_empty() {
        "unspecified error".to_owned()
    } else {
        result
    }
}
