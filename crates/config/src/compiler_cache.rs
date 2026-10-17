use serde::{Deserialize, Serialize};
use std::path::PathBuf;

/// Size cap of the shared compiler cache when none is configured.
pub const DEFAULT_COMPILER_CACHE_MAX_BYTES: u64 = 20 * 1024 * 1024 * 1024;

fn default_max_bytes() -> u64 {
    DEFAULT_COMPILER_CACHE_MAX_BYTES
}

/// `workspace.compiler_cache`: an operator-installed compiler-cache wrapper
/// (sccache, kache) that separate Tasks of one repository share. Machine
/// local: a daemon reads its own and never receives the server's.
///
/// `wrapper` unset means the feature is off; Forge installs nothing.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct CompilerCacheConfig {
    /// Absolute path of the wrapper, or a name resolved on `PATH` at start.
    pub wrapper: Option<String>,
    /// Size cap handed to the wrapper, and twice what the garbage collector
    /// trims the cache to while the disk is under its free-space floor.
    #[serde(default = "default_max_bytes")]
    pub max_bytes: u64,
    /// Where the cache lives. Unset: `<workspace root>/.forge/build/cache`.
    pub dir: Option<PathBuf>,
}

impl Default for CompilerCacheConfig {
    fn default() -> Self {
        Self {
            wrapper: None,
            max_bytes: DEFAULT_COMPILER_CACHE_MAX_BYTES,
            dir: None,
        }
    }
}

impl CompilerCacheConfig {
    /// This configuration with the given overrides applied (a daemon's CLI
    /// flags over its `daemon.yaml`). An empty wrapper turns the feature off.
    #[must_use]
    pub fn overridden(
        mut self,
        wrapper: Option<String>,
        max_bytes: Option<u64>,
        dir: Option<PathBuf>,
    ) -> Self {
        if let Some(wrapper) = wrapper {
            self.wrapper = Some(wrapper);
        }
        if let Some(max_bytes) = max_bytes {
            self.max_bytes = max_bytes;
        }
        if let Some(dir) = dir {
            self.dir = Some(dir);
        }
        self.wrapper = self.wrapper.filter(|wrapper| !wrapper.trim().is_empty());
        self
    }
}
