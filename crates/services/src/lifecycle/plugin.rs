use std::{collections::HashMap, sync::Arc};

use crate::lifecycle::LifecycleHookContext;

#[async_trait::async_trait]
pub trait LifecyclePlugin: Send + Sync {
    fn name(&self) -> &str;
    fn supported_events(&self) -> &[api_types::LifecycleEvent];
    async fn execute(&self, ctx: &LifecycleHookContext) -> Result<PluginResult, PluginError>;

    async fn execute_in_workspace(
        &self,
        ctx: &LifecycleHookContext,
        workspace: &crate::workspace_backend::ResolvedWorkspace,
    ) -> Result<PluginResult, PluginError> {
        // The built-in knowledge plugins use host filesystem APIs. Their
        // local entry point must never interpret a daemon handle as a path.
        let path = workspace.embedded_path().map_err(|error| PluginError {
            message: error.to_string(),
        })?;
        let mut ctx = ctx.clone();
        ctx.worktree_path = Some(path.to_string_lossy().into_owned());
        self.execute(&ctx).await
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PluginResult {
    Success,
    Skipped { reason: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PluginError {
    pub message: String,
}

pub struct PluginRegistry {
    plugins: HashMap<String, Arc<dyn LifecyclePlugin>>,
}

impl PluginRegistry {
    pub fn new() -> Self {
        Self {
            plugins: HashMap::new(),
        }
    }

    pub fn register(&mut self, plugin: Arc<dyn LifecyclePlugin>) {
        self.plugins.insert(plugin.name().to_owned(), plugin);
    }

    pub fn get(&self, name: &str) -> Option<&Arc<dyn LifecyclePlugin>> {
        self.plugins.get(name)
    }
}

impl Default for PluginRegistry {
    fn default() -> Self {
        Self::new()
    }
}
