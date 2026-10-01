#![forbid(unsafe_code)]

mod capacity;
pub mod claude;
pub mod codex;
pub mod command;
pub mod commit;
pub mod cursor;
pub mod gemini;
pub mod null;
pub mod opencode;
pub mod shell;
pub mod smith;

pub use claude::ClaudeCodeAdapter;
pub use codex::CodexAdapter;
pub use cursor::CursorAdapter;
pub use gemini::GeminiAdapter;
pub use null::NullAdapter;
pub use opencode::OpencodeAdapter;
pub use shell::ShellAdapter;
pub use smith::SmithAdapter;

use executors::AdapterRegistry;

/// Build a registry with all built-in adapters.
pub fn default_registry() -> AdapterRegistry {
    let mut registry = AdapterRegistry::new();
    registry.register(Box::new(ShellAdapter::new()));
    registry.register(Box::new(CodexAdapter::new()));
    registry.register(Box::new(ClaudeCodeAdapter::new()));
    registry.register(Box::new(CursorAdapter::new()));
    registry.register(Box::new(OpencodeAdapter::new()));
    registry.register(Box::new(GeminiAdapter::new()));
    registry.register(Box::new(SmithAdapter::new()));
    registry
}
