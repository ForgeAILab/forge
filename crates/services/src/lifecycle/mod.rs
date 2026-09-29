pub mod context;
pub mod emitter;
pub mod knowledge_capture;
pub mod knowledge_inject;
pub mod plugin;
pub mod runner;

pub use context::{project_env, LifecycleHookContext};
pub use emitter::LifecycleEventEmitter;
pub use plugin::{LifecyclePlugin, PluginError, PluginRegistry, PluginResult};
pub use runner::{LifecycleHookRun, LifecycleHookRunner};
