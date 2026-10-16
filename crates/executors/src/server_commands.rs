//! Process-wide demo policy. Set once, before composing the server runtime.
//! Daemons are separate processes and retain their own execution policy.
//!
//! One switch decides every demo refusal: commands from user configuration,
//! agent turns inside the server process, and repositories on the server's
//! own filesystem. Each refusal has its own code; all of them read this flag.
use std::sync::atomic::{AtomicBool, Ordering};

pub const CODE: &str = "server_commands_disabled";
pub const MESSAGE: &str = "This demo does not run commands";
pub const AGENTS_CODE: &str = "server_agents_disabled";
pub const AGENTS_MESSAGE: &str = "This demo does not run agents or connect model providers";
pub const LOCATION_CODE: &str = "server_location_refused";
pub const LOCATION_MESSAGE: &str =
    "This demo reads nothing from the server and accepts only https:// repositories at a public address";
static DISABLED: AtomicBool = AtomicBool::new(false);

/// Monotonic: configuration, templates and requests cannot re-enable commands.
pub fn disable() {
    DISABLED.store(true, Ordering::SeqCst);
}

/// True when the server is a manager only (demo mode).
pub fn manager_only() -> bool {
    DISABLED.load(Ordering::SeqCst)
}

#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("server_commands_disabled: This demo does not run commands")]
pub struct ServerCommandsDisabled;

pub fn ensure_allowed() -> Result<(), ServerCommandsDisabled> {
    if manager_only() {
        Err(ServerCommandsDisabled)
    } else {
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("server_agents_disabled: This demo does not run agents or connect model providers")]
pub struct ServerAgentsDisabled;

/// Native/embedded agent turns and provider connections run in the server
/// process; a manager-only server refuses them.
pub fn ensure_agents_allowed() -> Result<(), ServerAgentsDisabled> {
    if manager_only() {
        Err(ServerAgentsDisabled)
    } else {
        Ok(())
    }
}
