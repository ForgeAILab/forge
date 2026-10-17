pub mod context;
pub mod emitter;
pub mod plugin;
pub mod runner;

/// Idempotency-key prefix of the Forge comment that records a lifecycle hook
/// that was not run. Prompt loading leaves these comments out.
pub(crate) const HOOK_NOT_RUN_COMMENT_KEY: &str = "lifecycle-hook-not-run:";

pub use context::{project_env, LifecycleHookContext};
pub use emitter::LifecycleEventEmitter;
pub use plugin::{LifecyclePlugin, PluginError, PluginRegistry, PluginResult};
pub use runner::{LifecycleHookRun, LifecycleHookRunner};
