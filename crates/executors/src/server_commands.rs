//! Process-wide demo policy. Set once, before composing the server runtime.
//! Daemons are separate processes and retain their own execution policy.
use std::sync::atomic::{AtomicBool, Ordering};

pub const CODE: &str = "server_commands_disabled";
pub const MESSAGE: &str = "This demo does not run commands";
static DISABLED: AtomicBool = AtomicBool::new(false);

/// Monotonic: configuration, templates and requests cannot re-enable commands.
pub fn disable() {
    DISABLED.store(true, Ordering::SeqCst);
}

#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("server_commands_disabled: This demo does not run commands")]
pub struct ServerCommandsDisabled;

pub fn ensure_allowed() -> Result<(), ServerCommandsDisabled> {
    if DISABLED.load(Ordering::SeqCst) {
        Err(ServerCommandsDisabled)
    } else {
        Ok(())
    }
}
