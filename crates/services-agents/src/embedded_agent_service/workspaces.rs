//! Embedded workspace roots, provisioning, and command allowlists.

use super::*;

impl EmbeddedAgentService {
    /// The allowlist a turn in `project_id`'s workspace may spawn from.
    ///
    /// A Project layers its own `command_allowlist` in Project settings over
    /// the configured baseline, by the same rules: `only` replaces the
    /// inherited set, `allow` adds to it. A Project that says nothing
    /// inherits the baseline unchanged.
    pub async fn effective_command_allowlist(
        &self,
        project_id: Option<&str>,
    ) -> Arc<CommandAllowlist> {
        let baseline = self
            .command_allowlist
            .read()
            .map(|slot| Arc::clone(&slot))
            .unwrap_or_else(|_| Arc::new(CommandAllowlist::builtin()));
        let Some(project_id) = project_id else {
            return baseline;
        };
        let Ok(Some(project)) = ProjectRepo::get_by_id(&*self.db, project_id).await else {
            return baseline;
        };
        let Ok(settings) = serde_json::from_str::<Value>(&project.settings) else {
            return baseline;
        };
        let Some(declared) = settings.get("command_allowlist") else {
            return baseline;
        };
        let only = declared
            .get("only")
            .and_then(Value::as_array)
            .map(|values| string_list(values));
        let allow = declared
            .get("allow")
            .and_then(Value::as_array)
            .map(|values| string_list(values))
            .unwrap_or_default();
        let (layered, rejected) = baseline.layer(only.as_deref(), &allow);
        if !rejected.is_empty() {
            tracing::warn!(
                project_id,
                rejected = ?rejected,
                "ignored Project command allowlist entries that are not bare program names"
            );
        }
        Arc::new(layered)
    }

    /// Attach the media storage root so a Task session can capture the
    /// artifacts its run produced as authoritative evidence.
    pub fn set_media_root(&self, media_root: std::path::PathBuf) {
        self.tool_provider.set_media_root(media_root);
    }

    /// Attach the workspace root used to provision a Project Agent's
    /// disposable verification checkout, and the data-dir root holding each
    /// Project's durable `.forge` directory.
    pub fn set_workspace_root(
        &self,
        workspace_root: std::path::PathBuf,
        project_docs_root: std::path::PathBuf,
    ) {
        if let Ok(mut slot) = self.workspace_root.write() {
            *slot = Some((workspace_root, project_docs_root));
        }
    }

    fn workspace_roots(&self) -> Option<(std::path::PathBuf, std::path::PathBuf)> {
        self.workspace_root
            .read()
            .ok()
            .and_then(|slot| slot.clone())
    }

    /// Resolve the Project Agent's workspace, provisioning it on first use.
    pub async fn project_agent_workspace(&self, project_id: &str) -> Option<std::path::PathBuf> {
        let (_, workspaces_root) = self.workspace_roots()?;
        match crate::task_service::ensure_project_agent_workspace(
            &self.db,
            &workspaces_root,
            project_id,
        )
        .await
        {
            Ok(workspace) => workspace,
            // A Project Agent without a workspace still works; it just cannot
            // exercise the software. Log rather than swallow, so the reason is
            // visible instead of showing up later as a denied scope.
            Err(error) => {
                tracing::warn!(%error, project_id, "Project Agent workspace unavailable");
                None
            }
        }
    }

    /// Resolve the Main Agent's scratch directory, provisioning it on first
    /// use. Unlike the Project Agent workspace this holds no repository at
    /// all: it is where Main and its inquiries keep findings and notes.
    pub async fn main_agent_workspace(&self, account_id: &str) -> Option<std::path::PathBuf> {
        let (_, workspaces_root) = self.workspace_roots()?;
        match crate::task_service::ensure_main_agent_workspace(&workspaces_root, account_id).await {
            Ok(workspace) => Some(workspace),
            // Main still works without a scratch directory; it just cannot
            // keep notes or dispatch an inquiry. Log rather than swallow.
            Err(error) => {
                tracing::warn!(%error, account_id, "Main Agent workspace unavailable");
                None
            }
        }
    }

    /// Resolve one inquiry's private directory inside the dispatching
    /// account's scratch root.
    pub async fn inquiry_workspace(
        &self,
        account_id: &str,
        inquiry_id: &str,
    ) -> Option<std::path::PathBuf> {
        let (_, workspaces_root) = self.workspace_roots()?;
        match crate::task_service::ensure_inquiry_workspace(
            &workspaces_root,
            account_id,
            inquiry_id,
        )
        .await
        {
            Ok(path) => Some(path),
            // Swallowing this silently turns into an opaque tool failure with
            // nothing to chase; say why the directory could not be made.
            Err(error) => {
                tracing::warn!(%error, account_id, inquiry_id, "inquiry workspace unavailable");
                None
            }
        }
    }
}
