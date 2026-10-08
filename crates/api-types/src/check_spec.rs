//! Passive mechanical-check identity. No process or persistence is dispatched here.
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const CHECK_SPEC_REVISION: u32 = 1;
pub const CHECK_DIGEST_SCHEMA: &str = "forge.check-execution/1";

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
    pub purpose: CheckPurpose,
    pub commands: Vec<CheckCommandSpec>,
    /// Defined now; no executor reads this until the durable-runner cutover.
    pub whole_run_timeout_seconds: u64,
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
        if self.schema_revision != CHECK_SPEC_REVISION
            || self.whole_run_timeout_seconds == 0
            || self.execution_policy.is_empty()
        {
            return Err("invalid check spec revision, policy or wall timeout".into());
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
    /// ordered arrays and a schema envelope. Audit metadata is not semantic.
    pub fn encoding(&self) -> Result<String, String> {
        self.validate()?;
        crate::canonical_json_with_schema(
            CHECK_DIGEST_SCHEMA,
            &serde_json::json!({
                "spec": self.spec, "environment": self.environment,
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
