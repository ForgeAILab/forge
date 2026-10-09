//! Passive mechanical-check identity. No process or persistence is dispatched here.
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const CHECK_SPEC_REVISION: u32 = 2;
pub const CHECK_DIGEST_SCHEMA: &str = "forge.check-execution/2";

/// Why a consumer asked for a check. It selects the bundle the builder
/// assembles and is recorded on the consumer row; it is NOT part of the spec
/// or the digest, so two purposes needing the same commands share one run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckPurpose {
    EntryCi,
    ReviewCi,
    Conformance,
    BeforeWork,
    Lifecycle,
    EnvironmentPreflight,
    ReadinessProbe,
    EnvironmentHelper,
    AgentSelected,
    QueueHeadCi,
}
impl CheckPurpose {
    pub const ALL: &'static [Self] = &[
        Self::EntryCi,
        Self::ReviewCi,
        Self::Conformance,
        Self::BeforeWork,
        Self::Lifecycle,
        Self::EnvironmentPreflight,
        Self::ReadinessProbe,
        Self::EnvironmentHelper,
        Self::AgentSelected,
        Self::QueueHeadCi,
    ];
    pub fn as_str(self) -> &'static str {
        match self {
            Self::EntryCi => "entry_ci",
            Self::ReviewCi => "review_ci",
            Self::Conformance => "conformance",
            Self::BeforeWork => "before_work",
            Self::Lifecycle => "lifecycle",
            Self::EnvironmentPreflight => "environment_preflight",
            Self::ReadinessProbe => "readiness_probe",
            Self::EnvironmentHelper => "environment_helper",
            Self::AgentSelected => "agent_selected",
            Self::QueueHeadCi => "queue_head_ci",
        }
    }
}
/// What one run of a bundle is good for, and therefore who may share it.
/// A bundle that only reads a commit is `Commit`: any Task at that commit
/// shares the run. A bundle that acts on a worktree carries that worktree's
/// durable identity, so a second worktree at the same commit is still prepared.
/// A script with no worktree at all is still owed to its own Task.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CheckScope {
    Commit,
    /// `generation` is the workspace placement generation: a re-placed or
    /// re-created worktree is a different target.
    Workspace {
        workspace_id: String,
        generation: u64,
    },
    Task {
        task_id: String,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckWorkingDirectory {
    TaskRoot,
    LifecycleFallback,
    RepositoryOrProbeScratch,
    SuppliedDirectory,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckFailurePolicy {
    StopBundle,
    Continue,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CheckCacheability {
    Uncacheable,
    DeclaredControlledInputs,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckCommandSpec {
    pub id: String,
    pub shell_text: String,
    /// Today's Bash login-shell semantics, not an implicit default.
    pub shell: String,
    pub working_directory: CheckWorkingDirectory,
    pub environment_keys: BTreeSet<String>,
    /// None describes today's unbounded CI; zero is never a bounded timeout.
    pub timeout_seconds: Option<u64>,
    pub failure_policy: CheckFailurePolicy,
    pub cacheability: CheckCacheability,
    pub requirement_ids: BTreeSet<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckSpec {
    pub schema_revision: u32,
    pub scope: CheckScope,
    pub commands: Vec<CheckCommandSpec>,
    /// True when the bundle has a cleanup step the runner must perform after
    /// the commands. No family configured today declares one.
    pub declares_cleanup: bool,
    /// Evidence only, excluded from the digest: how many steps the
    /// configuration listed, and which of them (zero-based, in configured
    /// order) were blank and therefore dropped from `commands`.
    pub configured_commands: usize,
    pub blank_commands: Vec<usize>,
    /// Runner policy identity, including inherited environment/removals and
    /// machine build policy. A policy change must change this revision.
    pub execution_policy: String,
}
impl CheckSpec {
    pub fn cacheable(&self) -> bool {
        !self.commands.is_empty()
            && self
                .commands
                .iter()
                .all(|command| command.cacheability == CheckCacheability::DeclaredControlledInputs)
    }
    pub fn validate(&self) -> Result<(), String> {
        if self.schema_revision != CHECK_SPEC_REVISION || self.execution_policy.is_empty() {
            return Err("invalid check spec revision or policy".into());
        }
        if match &self.scope {
            CheckScope::Commit => false,
            CheckScope::Workspace {
                workspace_id,
                generation,
            } => workspace_id.is_empty() || *generation == 0,
            CheckScope::Task { task_id } => task_id.is_empty(),
        } {
            return Err("check scope needs a workspace generation or a Task".into());
        }
        if self.commands.len() + self.blank_commands.len() != self.configured_commands
            || self
                .blank_commands
                .windows(2)
                .any(|pair| pair[0] >= pair[1])
            || self
                .blank_commands
                .last()
                .is_some_and(|index| *index >= self.configured_commands)
        {
            return Err("blank steps must account for every configured step".into());
        }
        let mut ids = BTreeSet::new();
        for command in &self.commands {
            if command.id.is_empty()
                || !ids.insert(&command.id)
                || command.shell_text.trim().is_empty()
                || command.shell.is_empty()
                || command.timeout_seconds == Some(0)
            {
                return Err("invalid or duplicate check command".into());
            }
        }
        Ok(())
    }
}

/// Secret material is represented ONLY by an owner-maintained opaque revision.
/// Callers must never classify a secret as a ControlledValue or hash its bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum CheckEnvironmentValue {
    ControlledValue(String),
    SecretRevision(String),
    Removed,
    Volatile,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CheckEnvironmentIdentity {
    Attested {
        input_digest: String,
    },
    /// Daemon identity is deliberately distinct until an owner attests inputs.
    NotAttested,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckExecutionRevision {
    pub number: u64,
    /// Required for a nonzero forced execution revision; excluded from digest.
    pub audit_ref: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckDigestInput {
    pub spec: CheckSpec,
    pub environment: BTreeMap<String, CheckEnvironmentValue>,
    pub environment_identity: CheckEnvironmentIdentity,
    pub execution_revision: CheckExecutionRevision,
}
impl CheckDigestInput {
    pub fn validate(&self) -> Result<(), String> {
        self.spec.validate()?;
        if self.execution_revision.number > 0
            && self
                .execution_revision
                .audit_ref
                .as_deref()
                .is_none_or(str::is_empty)
        {
            return Err("forced execution revision requires an audit reference".into());
        }
        let declared: BTreeSet<_> = self
            .spec
            .commands
            .iter()
            .flat_map(|c| c.environment_keys.iter().cloned())
            .collect();
        if declared != self.environment.keys().cloned().collect() {
            return Err("identity must declare every environment key exactly once".into());
        }
        if let CheckEnvironmentIdentity::Attested { input_digest } = &self.environment_identity {
            if input_digest.len() != 64
                || !input_digest
                    .bytes()
                    .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            {
                return Err("attested execution input identity must be a SHA-256 digest".into());
            }
        }
        Ok(())
    }
    pub fn reusable_inputs(&self) -> bool {
        self.spec.cacheable()
            && matches!(
                self.environment_identity,
                CheckEnvironmentIdentity::Attested { .. }
            )
            && !self
                .environment
                .values()
                .any(|v| matches!(v, CheckEnvironmentValue::Volatile))
    }
    /// Canonical compact UTF-8 JSON, recursive lexical object-key sorting,
    /// ordered arrays and a schema envelope. Audit metadata and the blank-step
    /// record are not semantic: they change no command that runs.
    pub fn encoding(&self) -> Result<String, String> {
        self.validate()?;
        let mut spec = serde_json::to_value(&self.spec).map_err(|e| e.to_string())?;
        if let Some(fields) = spec.as_object_mut() {
            fields.remove("configured_commands");
            fields.remove("blank_commands");
        }
        crate::canonical_json_with_schema(
            CHECK_DIGEST_SCHEMA,
            &serde_json::json!({
                "spec": spec, "environment": self.environment,
                "environment_identity": self.environment_identity,
                "execution_revision": self.execution_revision.number,
            }),
        )
        .map_err(|e| e.to_string())
    }
    pub fn digest(&self) -> Result<String, String> {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(self.encoding()?.as_bytes());
        Ok(digest.iter().map(|b| format!("{b:02x}")).collect())
    }
}

/// The server owner supplies revisioned, controlled inputs, not a runtime ID.
/// Revision strings are opaque non-secret identifiers. Asset revisions bind
/// target paths to content/revision; secret revisions never contain secret bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerCheckExecutionInputs {
    pub toolchain_revision: String,
    pub environment_revision: String,
    pub asset_revisions: BTreeMap<String, String>,
    pub secret_revisions: BTreeMap<String, String>,
    pub shell_revision: String,
    pub runner_revision: String,
}
impl ServerCheckExecutionInputs {
    /// Pure owner computation; no probe, network access or execution cutover.
    pub fn identity(&self) -> Result<CheckEnvironmentIdentity, String> {
        if [
            &self.toolchain_revision,
            &self.environment_revision,
            &self.shell_revision,
            &self.runner_revision,
        ]
        .iter()
        .any(|s| s.is_empty())
        {
            return Err("server execution inputs need explicit owner revisions".into());
        }
        let input_digest = crate::canonical_digest_with_schema(
            "forge.check-server-inputs/1",
            &serde_json::json!({
                "os": std::env::consts::OS, "arch": std::env::consts::ARCH, "inputs": self,
            }),
        )
        .map_err(|e| e.to_string())?;
        Ok(CheckEnvironmentIdentity::Attested { input_digest })
    }
}

/// The existing 3.2 persistence-free CheckRun command outcome, shared verbatim
/// so storage and the effect primitive cannot acquire competing result types.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CheckCommandOutcome {
    pub index: usize,
    pub command: String,
    pub exit_code: i32,
    pub stderr_tail: String,
    pub output_tail: String,
    pub started_at: String,
    pub finished_at: String,
}

#[cfg(test)]
mod tests;
