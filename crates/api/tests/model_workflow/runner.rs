//! Model sequence execution, shrinking, trace reporting and environment controls.

use super::*;

// ---------------------------------------------------------------------------
// Running, shrinking and reporting
// ---------------------------------------------------------------------------

pub(super) struct Failure {
    pub(super) step: Option<usize>,
    pub(super) violation: String,
    pub(super) trace: Vec<String>,
}

/// Run one sequence on a fresh stack. `Ok` carries the trace: one line per
/// step with every Task's state, which is what "deterministic" compares.
pub(super) async fn run(
    sequence: &[Step],
    avoid_open_findings: bool,
) -> Result<Vec<String>, Failure> {
    let mut world = World::new().await;
    world.avoid_open_findings = avoid_open_findings;
    let mut at = None;
    let outcome = async {
        for (number, step) in sequence.iter().enumerate() {
            at = Some(number);
            let applied = world.apply(&step.action).await?;
            world.check_committed().await?;
            if step.crash_before_drain {
                world.crash_and_restart().await?;
                world.check_committed().await?;
            }
            world.quiesce().await?;
            let state = world.check_quiescent().await?;
            world.trace.push(format!(
                "{number} {:?}{} -> {applied}: {state}",
                step.action,
                if step.crash_before_drain {
                    " +crash"
                } else {
                    ""
                }
            ));
        }
        at = None;
        world.drive_to_settlement().await
    }
    .await;
    if outcome.is_err() {
        world.record_history().await;
    }
    let operations = world.shared.lead_operations.lock().unwrap().clone();
    if !operations.is_empty() {
        world.trace.push(format!("lead operations: {operations:?}"));
    }
    // Leave nothing running behind the temporary directory.
    if let Some(live) = world.live.take() {
        live.script.alive.store(false, Ordering::SeqCst);
        live.script.waiting.lock().unwrap().clear();
        live.runtime.task_dispatcher.stop();
        live.checks.stop();
        let _ = tokio::time::timeout(Duration::from_secs(5), live.pool.close()).await;
    }
    match outcome {
        Ok(()) => Ok(world.trace),
        Err(violation) => Err(Failure {
            step: at,
            violation,
            trace: world.trace,
        }),
    }
}

/// The invariant letter of a violation: a shrunk case must break the same one.
pub(super) fn class(violation: &str) -> &str {
    violation.get(..3).unwrap_or(violation)
}

/// Simple delta shrink: drop one step at a time, from the end, and keep the
/// drop when the same invariant still breaks.
pub(super) async fn shrink(
    sequence: Vec<Step>,
    failure: Failure,
    avoid: bool,
) -> (Vec<Step>, Failure) {
    let wanted = class(&failure.violation).to_owned();
    let (mut sequence, mut failure) = (sequence, failure);
    // Steps after the failing one never ran.
    if let Some(step) = failure.step {
        sequence.truncate(step + 1);
    }
    let mut replays = 0;
    let mut index = if std::env::var("FORGE_MODEL_NO_SHRINK").is_ok() {
        0
    } else {
        sequence.len()
    };
    while index > 0 && replays < 40 {
        index -= 1;
        let mut candidate = sequence.clone();
        candidate.remove(index);
        replays += 1;
        if let Err(smaller) = run(&candidate, avoid).await {
            if class(&smaller.violation) == wanted {
                sequence = candidate;
                failure = smaller;
            }
        }
    }
    (sequence, failure)
}

pub(super) fn report(name: &str, sequence: &[Step], failure: &Failure) -> String {
    format!(
        "\n=== model violation ({name}) ===\n{}\nfailing step: {:?}\nminimized sequence ({} steps):\n{}\ntrace:\n{}\n",
        failure.violation,
        failure.step,
        sequence.len(),
        sequence
            .iter()
            .map(|step| format!(
                "  {:?}{}",
                step.action,
                if step.crash_before_drain { " +crash" } else { "" }
            ))
            .collect::<Vec<_>>()
            .join("\n"),
        failure.trace.join("\n"),
    )
}

pub(super) fn digest(trace: &[String]) -> u64 {
    // FNV-1a over action observations and the final settlement. Intermediate
    // settlement rounds observe asynchronous slot/check timing, not model
    // actions; retain them in the diagnostic trace without hashing them.
    trace
        .iter()
        .filter(|line| !line.starts_with("settle "))
        .flat_map(|line| line.bytes().chain(*b"\n"))
        .fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
            (hash ^ u64::from(byte)).wrapping_mul(0x0000_0100_0000_01b3)
        })
}

pub(super) fn env_number(name: &str) -> Option<u64> {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
}

pub(super) async fn run_cases(cases: Vec<(String, Vec<Step>)>) {
    run_all(cases, false).await;
}

/// `FORGE_MODEL_LOG=<level>` prints the stack's own log at that level
/// (`warn`, `info`, `debug`) with timestamps: what waited for the database,
/// who held it, what a worker gave up on.
pub(super) fn log_to_stderr() {
    if let Some(level) = std::env::var("FORGE_MODEL_LOG")
        .ok()
        .and_then(|level| level.parse::<tracing::Level>().ok())
    {
        let _ = tracing_subscriber::fmt()
            .with_max_level(level)
            .with_writer(std::io::stderr)
            .try_init();
    }
}

pub(super) async fn run_all(mut cases: Vec<(String, Vec<Step>)>, avoid: bool) {
    log_to_stderr();
    // `FORGE_MODEL_CASE=<text>` runs only the cases whose name contains it.
    if let Ok(only) = std::env::var("FORGE_MODEL_CASE") {
        cases.retain(|(name, _)| name.contains(&only));
    }
    let mut passed = Vec::new();
    let mut failed = Vec::new();
    // Cases are independent stacks; a few at a time keeps the wall time low.
    for batch in cases.chunks(4) {
        let outcomes =
            futures_util::future::join_all(batch.iter().map(|(_, sequence)| async move {
                let started = std::time::Instant::now();
                (run(sequence, avoid).await, started.elapsed())
            }))
            .await;
        for ((name, sequence), (outcome, took)) in batch.iter().zip(outcomes) {
            match outcome {
                Ok(trace) => {
                    if std::env::var("FORGE_MODEL_TRACE").is_ok() {
                        println!("--- {name}\n{}", trace.join("\n"));
                    }
                    passed.push(format!(
                        "{name} digest {:016x} ({} ms)",
                        digest(&trace),
                        took.as_millis()
                    ));
                }
                Err(failure) => failed.push((name.clone(), sequence.clone(), failure)),
            }
        }
    }
    passed.sort();
    for line in &passed {
        println!("model case ok: {line}");
    }
    let mut reports = String::new();
    for (name, sequence, failure) in failed {
        let (sequence, failure) = shrink(sequence, failure, avoid).await;
        reports.push_str(&report(&name, &sequence, &failure));
    }
    assert!(reports.is_empty(), "{reports}");
}
