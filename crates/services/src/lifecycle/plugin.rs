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
        // The emitter hands a plugin the worktree its workspace-manager
        // inspection just accepted, as `ctx.worktree_path`; a plugin runs on
        // the Forge host only.
        if workspace.placement.owner_kind != db::PlacementOwnerKind::Server {
            return Err(PluginError {
                message: crate::workspace_backend::WorkspaceBackendError::OwnerUnsupported {
                    owner_kind: workspace.placement.owner_kind.clone(),
                }
                .to_string(),
            });
        }
        if ctx.worktree_path.is_none() {
            return Err(PluginError {
                message: "lifecycle plugin has no validated worktree to run in".to_owned(),
            });
        }
        self.execute(ctx).await
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
