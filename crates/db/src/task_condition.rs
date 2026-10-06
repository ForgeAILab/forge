//! Stage-one shadow condition. Legacy columns still own runtime decisions.
//!
//! [`map_legacy_condition`] is the single deterministic mapping: a pure Rust
//! function used by the migration backfill, the writer seams and the invariant
//! check. No SQL trigger or view evaluates it, so no Task write statement
//! carries the mapping. No queue intent, publication claim or settlement
//! receipt is moved here.
use crate::{DbError, Result, SqliteDb, Task};
use serde::{de::IgnoredAny, Deserialize, Serialize};
use serde_json::value::RawValue;
use sqlx::{Row, Sqlite, SqliteConnection, Transaction};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum TaskCondition {
    Clear {
        evidence: ConditionEvidence,
    },
    Entering {
        state: String,
        epoch: i64,
        step_id: String,
        phase: String,
        since: String,
        evidence: ConditionEvidence,
    },
    Running {
        execution_id: String,
        role: String,
        epoch: i64,
        since: String,
        evidence: ConditionEvidence,
    },
    Deferred {
        until: String,
        reason: RetryCause,
        resume: ConditionContinuation,
        evidence: ConditionEvidence,
    },
    Parked {
        primary: ParkReason,
        additional: Vec<ParkReason>,
        resume: ConditionContinuation,
        since: Option<String>,
        evidence: ConditionEvidence,
    },
    Failed {
        failure: ParkReason,
        additional: Vec<ParkReason>,
        resume: ConditionContinuation,
        since: Option<String>,
        evidence: ConditionEvidence,
    },
    Settled {
        outcome: TerminalOutcome,
        evidence: ConditionEvidence,
    },
}

/// Legacy condition input, including malformed strings. Every value is capped
/// at [`EVIDENCE_VALUE_LIMIT`] bytes with a truncation marker; the legacy
/// columns keep the full data. Valid metadata keeps condition/evidence keys
/// only; unrelated keys and intent bodies remain untouched in metadata_json.
/// This evidence is private and non-authoritative.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionEvidence {
    pub error_annotation: Option<String>,
    pub blocked_json: Option<String>,
    pub failed_json: Option<String>,
    pub entry_barrier_json: Option<String>,
    /// Raw JSON fragments preserve large numbers, lone escapes and deeply
    /// nested legacy evidence without routing it through a Value decoder.
    pub metadata: BTreeMap<String, String>,
    pub unparsed_metadata: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LegacyConditionField {
    ErrorAnnotation,
    BlockedJson,
    FailedJson,
    EntryBarrierJson,
    MetadataJson,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConditionSource {
    pub field: LegacyConditionField,
    pub key: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UnknownConditionProblem {
    MalformedJson,
    NonObject,
    UnknownKind,
    InvalidShape,
    UnownedEntry,
    /// A BLOB or invalid UTF-8 value in a legacy TEXT column.
    NonText,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HumanBoundary {
    PlanReview,
    ExternalMerge,
    Legacy,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConditionCapacityScope {
    Agent,
    Machine,
    Project,
    OwnerBackpressure,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ConditionEnvironmentKind {
    EnvironmentProbePending,
    EnvironmentNotReady,
    EnvironmentUnverified,
    ProvisionFailed,
    Unknown,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ParkReason {
    Held {
        actor: String,
    },
    Failure {
        failure_kind: api_types::FailureKind,
    },
    /// Legacy `agent_timeout`: a blocking annotation kind that is not an
    /// `api_types::FailureKind`.
    AgentTimeout {
        source: ConditionSource,
    },
    BudgetExhausted {
        budget_kind: Option<crate::budget::Kind>,
        source: ConditionSource,
    },
    EntryBlocked {
        state: Option<String>,
        source: ConditionSource,
    },
    HumanDecision {
        boundary: HumanBoundary,
        source: ConditionSource,
    },
    Capacity {
        scope: ConditionCapacityScope,
    },
    DispatchRefusal {
        capability: Option<String>,
        blocker_digest: Option<String>,
    },
    ProjectPaused {
        state: Option<String>,
    },
    OwnerOffline {
        daemon_id: Option<String>,
        started_at: Option<String>,
    },
    Environment {
        wait_kind: ConditionEnvironmentKind,
        source: ConditionSource,
    },
    PlacementDenied {
        source: ConditionSource,
    },
    DaemonUpgradeRequired {
        source: ConditionSource,
    },
    RemoteCancelPending {
        source: ConditionSource,
    },
    Dependencies {
        cancelled: bool,
        source: ConditionSource,
    },
    PlanSettlementWait {
        execution_id: Option<String>,
    },
    UnknownCondition {
        source: ConditionSource,
        problem: UnknownConditionProblem,
    },
    WorkflowInvalid {
        state: String,
        definition_digest: String,
        cause: String,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryCause {
    ExecutionFailure,
    WorkflowGuard,
    ReviewCiInfrastructure,
    PlanTransport,
    Legacy,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum ConditionContinuation {
    Reconcile,
    Dispatch { target_state: String },
    RetryEntry { state: Option<String> },
    Integrate { state: Option<String> },
    SettlePlan { execution_id: Option<String> },
    ResumeQueuedCommand { intent_id: Option<String> },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TerminalOutcome {
    Completed,
    Cancelled,
}

impl TaskCondition {
    /// Whether the condition holds the Task out of dispatch. For a legacy-only
    /// mapping this agrees with what the dispatcher and readers block on.
    pub fn is_blocked(&self) -> bool {
        matches!(self, Self::Parked { .. } | Self::Failed { .. })
    }
}

/// The `error_annotation.type` values today's dispatcher refuses to dispatch
/// through. `services::task_dispatcher` reads this same list, so the mapping
/// and the dispatcher cannot drift. Any other annotation is informational.
pub const LEGACY_BLOCKING_ANNOTATION_KINDS: &[&str] = &[
    "manual_stop",
    "workflow_loop",
    "cascade_failed",
    "workspace_error",
    "agent_timeout",
    "recovery_required",
    "workspace_reset_required",
    "max_turns_exceeded",
    "before_work_hook_failed",
    "before_work_hook_timeout",
    "review_needs_owner",
    "dispatch_failed",
];

/// Per-value cap on evidence copied into a condition.
pub const EVIDENCE_VALUE_LIMIT: usize = 4096;
/// Cap on a typed string lifted out of legacy JSON (actor, state, id).
const TYPED_TEXT_LIMIT: usize = 1024;

const EVIDENCE_METADATA_KEYS: [&str; 24] = [
    "awaiting_human",
    "awaiting_human_marker_id",
    "awaiting_human_reason",
    "coordination_review_pending",
    "coordination_review_pending_id",
    "daemon_upgrade_refusal",
    "deferred_dispatch",
    "dispatch_disposition",
    "environment_wait",
    "executor_unavailable_execution_id",
    "last_execution_failure_at",
    "last_execution_failure_execution_id",
    "last_workflow_guard_execution_id",
    "last_workflow_guard_name",
    "last_workflow_guard_reason",
    "last_workflow_guard_rejection_at",
    "owner_wait",
    "paused_integration",
    "paused_integration_generation",
    "placement_refusal",
    "plan_settlement_wait",
    "planning_completed_at",
    "planning_execution_id",
    "planning_state_entry_token",
];
/// Metadata keys whose value must be an object (or null).
const OBJECT_METADATA_KEYS: [&str; 8] = [
    "deferred_dispatch",
    "dispatch_disposition",
    "paused_integration",
    "owner_wait",
    "environment_wait",
    "placement_refusal",
    "daemon_upgrade_refusal",
    "plan_settlement_wait",
];

/// A snapshot fence must include metadata: several existing metadata writers
/// intentionally leave Task.version unchanged. No timestamp enters the map.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct LegacyConditionInput {
    pub error_annotation: Option<String>,
    pub blocked_json: Option<String>,
    pub failed_json: Option<String>,
    pub entry_barrier_json: Option<String>,
    pub metadata_json: Option<String>,
    /// Columns holding a BLOB or invalid UTF-8 instead of text. Their value
    /// above is `None`; they map to `UnknownCondition(NonText)`.
    pub non_text: Vec<LegacyConditionField>,
}
impl From<&Task> for LegacyConditionInput {
    fn from(task: &Task) -> Self {
        Self {
            error_annotation: task.error_annotation.clone(),
            blocked_json: task.blocked_json.clone(),
            failed_json: task.failed_json.clone(),
            entry_barrier_json: task.entry_barrier_json.clone(),
            metadata_json: task.metadata_json.clone(),
            non_text: Vec::new(),
        }
    }
}

const LEGACY_FIELDS: [LegacyConditionField; 5] = [
    LegacyConditionField::ErrorAnnotation,
    LegacyConditionField::BlockedJson,
    LegacyConditionField::FailedJson,
    LegacyConditionField::EntryBarrierJson,
    LegacyConditionField::MetadataJson,
];
/// The five legacy columns of one row, read without assuming they hold text.
const LEGACY_SELECT: &str =
    "error_annotation,blocked_json,failed_json,entry_barrier_json,metadata_json,condition_json";

impl LegacyConditionInput {
    /// Never fails on a hostile value: a BLOB or invalid UTF-8 is recorded in
    /// `non_text` instead of aborting the read.
    fn from_row(row: &sqlx::sqlite::SqliteRow) -> Result<Self> {
        let mut input = Self::default();
        for (index, field) in LEGACY_FIELDS.iter().enumerate() {
            let raw = row.try_get_raw(index)?;
            let is_text = sqlx::ValueRef::type_info(&raw).to_string() == "TEXT";
            let bytes: Option<Vec<u8>> = row.try_get(index)?;
            let value = match bytes.map(String::from_utf8) {
                None => None,
                Some(Ok(text)) if is_text => Some(text),
                Some(_) => {
                    input.non_text.push(field.clone());
                    None
                }
            };
            *match field {
                LegacyConditionField::ErrorAnnotation => &mut input.error_annotation,
                LegacyConditionField::BlockedJson => &mut input.blocked_json,
                LegacyConditionField::FailedJson => &mut input.failed_json,
                LegacyConditionField::EntryBarrierJson => &mut input.entry_barrier_json,
                LegacyConditionField::MetadataJson => &mut input.metadata_json,
            } = value;
        }
        Ok(input)
    }
    fn view(&self) -> LegacyView<'_> {
        LegacyView {
            error_annotation: self.error_annotation.as_deref(),
            blocked_json: self.blocked_json.as_deref(),
            failed_json: self.failed_json.as_deref(),
            entry_barrier_json: self.entry_barrier_json.as_deref(),
            metadata_json: self.metadata_json.as_deref(),
            non_text: &self.non_text,
            metadata_is_object: false,
        }
    }
}

/// Borrowed mapping input, so a writer holding a Task never clones its JSON.
#[derive(Clone, Copy)]
pub(crate) struct LegacyView<'a> {
    pub error_annotation: Option<&'a str>,
    pub blocked_json: Option<&'a str>,
    pub failed_json: Option<&'a str>,
    pub entry_barrier_json: Option<&'a str>,
    pub metadata_json: Option<&'a str>,
    pub non_text: &'a [LegacyConditionField],
    /// The writer serialized `metadata_json` from a parsed metadata object
    /// itself, so it is known to be a JSON object (or absent).
    pub metadata_is_object: bool,
}
impl<'a> From<&'a Task> for LegacyView<'a> {
    fn from(task: &'a Task) -> Self {
        Self {
            error_annotation: task.error_annotation.as_deref(),
            blocked_json: task.blocked_json.as_deref(),
            failed_json: task.failed_json.as_deref(),
            entry_barrier_json: task.entry_barrier_json.as_deref(),
            metadata_json: task.metadata_json.as_deref(),
            non_text: &[],
            metadata_is_object: false,
        }
    }
}

/// One level of a JSON object. Values stay raw text, so an unrelated large
/// key is skipped by the scanner and never decoded or re-serialized, and
/// large numbers, lone escapes and deep nesting survive untouched.
type Fields<'a> = BTreeMap<String, &'a RawValue>;

enum Column<'a> {
    Absent,
    NonText,
    Malformed,
    NonObject,
    Object(Fields<'a>),
}
impl<'a> Column<'a> {
    fn read(raw: Option<&'a str>, non_text: bool) -> Self {
        if non_text {
            return Self::NonText;
        }
        let Some(raw) = raw else {
            return Self::Absent;
        };
        match serde_json::from_str::<Fields<'a>>(raw) {
            Ok(fields) => Self::Object(fields),
            Err(_) if serde_json::from_str::<IgnoredAny>(raw).is_ok() => Self::NonObject,
            Err(_) => Self::Malformed,
        }
    }
    fn object(&self) -> Option<&Fields<'a>> {
        match self {
            Self::Object(fields) => Some(fields),
            _ => None,
        }
    }
}

#[derive(PartialEq, Eq)]
enum Shape {
    Object,
    Text,
    True,
    False,
    Null,
    Other,
}
fn shape(raw: &RawValue) -> Shape {
    match raw.get().trim_start().as_bytes().first() {
        Some(b'{') => Shape::Object,
        Some(b'"') => Shape::Text,
        Some(b't') => Shape::True,
        Some(b'f') => Shape::False,
        Some(b'n') => Shape::Null,
        _ => Shape::Other,
    }
}
/// A JSON string value. A non-string, an undecodable escape (lone surrogate)
/// or an oversized value is an absent hint, never an error.
fn text(fields: &Fields<'_>, key: &str) -> Option<String> {
    let raw = fields.get(key)?.get();
    if raw.len() > TYPED_TEXT_LIMIT || !raw.starts_with('"') {
        return None;
    }
    serde_json::from_str(raw).ok()
}
fn is(fields: &Fields<'_>, key: &str, expected: &str) -> bool {
    text(fields, key).as_deref() == Some(expected)
}
fn object<'a>(fields: &Fields<'a>, key: &str) -> Option<Fields<'a>> {
    let raw: &'a RawValue = fields.get(key).copied()?;
    (shape(raw) == Shape::Object).then(|| serde_json::from_str(raw.get()).unwrap_or_default())
}
fn failure_kind(kind: &str) -> Option<api_types::FailureKind> {
    serde_json::from_value(serde_json::Value::String(kind.to_owned()))
        .ok()
        .filter(|kind| *kind != api_types::FailureKind::Unknown)
}
fn bounded(raw: &str) -> String {
    if raw.len() <= EVIDENCE_VALUE_LIMIT {
        return raw.to_owned();
    }
    let mut end = EVIDENCE_VALUE_LIMIT;
    while !raw.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}…[truncated; {} bytes]", &raw[..end], raw.len())
}

/// Whether today's dispatcher blocks on this annotation: it must parse as
/// JSON (with the dispatcher's own decoder) and carry a blocking `type`.
pub fn legacy_annotation_blocks(error_annotation: Option<&str>) -> bool {
    let Some(raw) = error_annotation else {
        return false;
    };
    serde_json::from_str::<Fields<'_>>(raw)
        .ok()
        .is_some_and(|fields| annotation_blocks(raw, &fields))
}
fn annotation_blocks(raw: &str, fields: &Fields<'_>) -> bool {
    text(fields, "type").is_some_and(|kind| LEGACY_BLOCKING_ANNOTATION_KINDS.contains(&kind.as_str()))
        // The cheap scan above accepts documents the dispatcher's decoder
        // rejects (out-of-range numbers, lone surrogates, nesting past 128).
        // Confirm with that decoder; only a blocking annotation pays for it.
        && serde_json::from_str::<serde_json::Value>(raw).is_ok()
}

fn source(field: &LegacyConditionField, key: Option<&str>) -> ConditionSource {
    ConditionSource {
        field: field.clone(),
        key: key.map(str::to_owned),
    }
}
fn unknown(
    field: &LegacyConditionField,
    key: Option<&str>,
    problem: UnknownConditionProblem,
) -> ParkReason {
    ParkReason::UnknownCondition {
        source: source(field, key),
        problem,
    }
}

/// `blocked_json` / `failed_json`: legacy blocks on their mere presence, so
/// every non-null value yields a reason.
fn interruption_reasons(
    out: &mut Vec<(u16, ParkReason)>,
    base: u16,
    field: LegacyConditionField,
    column: &Column<'_>,
) {
    let fields = match column {
        Column::Absent => return,
        Column::NonText => {
            return out.push((
                base,
                unknown(&field, None, UnknownConditionProblem::NonText),
            ))
        }
        Column::Malformed => {
            return out.push((
                base,
                unknown(&field, None, UnknownConditionProblem::MalformedJson),
            ))
        }
        Column::NonObject => {
            return out.push((
                base + 1,
                unknown(&field, None, UnknownConditionProblem::NonObject),
            ))
        }
        Column::Object(fields) => fields,
    };
    let kind = text(fields, "kind");
    let kind = kind.as_deref();
    let ci_exhausted = is(
        fields,
        "blocking_reason",
        "review_ci_infrastructure_exhausted",
    );
    let budget_kind = matches!(
        kind,
        Some("review_budget_exhausted" | "retry_exhausted" | "merge_fix_budget_exhausted")
    );
    if kind == Some("manual_stop") {
        out.push((
            base + 2,
            ParkReason::Held {
                actor: text(fields, "blocked_by").unwrap_or_else(|| "user".to_owned()),
            },
        ));
    }
    if budget_kind || ci_exhausted {
        out.push((
            base + 3,
            ParkReason::BudgetExhausted {
                budget_kind: if ci_exhausted {
                    Some(crate::budget::Kind::ReviewCiInfrastructure)
                } else {
                    match kind {
                        Some("review_budget_exhausted") => Some(crate::budget::Kind::Review),
                        Some("merge_fix_budget_exhausted") => Some(crate::budget::Kind::MergeFix),
                        _ => None,
                    }
                },
                source: source(&field, None),
            },
        ));
    }
    if kind == Some("agent_timeout") {
        out.push((
            base + 4,
            ParkReason::AgentTimeout {
                source: source(&field, None),
            },
        ));
    } else if let Some(failure_kind) = kind.and_then(failure_kind) {
        if kind != Some("manual_stop") && !budget_kind {
            out.push((base + 4, ParkReason::Failure { failure_kind }));
        }
    } else if is(fields, "source", "workflow_loop") {
        // The loop writer omits `kind` and identifies itself by `source`.
        out.push((
            base + 5,
            ParkReason::Failure {
                failure_kind: api_types::FailureKind::WorkflowLoop,
            },
        ));
    } else {
        out.push((
            base + 6,
            unknown(&field, None, UnknownConditionProblem::UnknownKind),
        ));
    }
}

/// Map an immutable legacy snapshot. Pure: no database, clock or ownership
/// table is read. `Running`/`Entering`/`Settled` need durable witnesses absent
/// from these five inputs; the stage-one mapping never fabricates them.
pub fn map_legacy_condition(input: &LegacyConditionInput) -> TaskCondition {
    map_view(&input.view())
}

pub(crate) fn map_view(view: &LegacyView<'_>) -> TaskCondition {
    use LegacyConditionField as Field;
    use UnknownConditionProblem as Problem;
    let non_text = |field: Field| view.non_text.contains(&field);
    let annotation = Column::read(view.error_annotation, non_text(Field::ErrorAnnotation));
    let blocked = Column::read(view.blocked_json, non_text(Field::BlockedJson));
    let failed = Column::read(view.failed_json, non_text(Field::FailedJson));
    let barrier = Column::read(view.entry_barrier_json, non_text(Field::EntryBarrierJson));
    let metadata = Column::read(view.metadata_json, non_text(Field::MetadataJson));
    let empty = Fields::new();
    let m = metadata.object().unwrap_or(&empty);

    // A deferral is a timer only when all three fields are usable.
    let deferred = object(m, "deferred_dispatch");
    let valid_deferred: Option<(String, String)> = deferred.as_ref().and_then(|deferred| {
        let until = text(deferred, "not_before")?;
        let target_state = text(deferred, "target_state")?;
        let reason_is_text = deferred
            .get("reason")
            .is_some_and(|r| shape(r) == Shape::Text);
        (reason_is_text && chrono::DateTime::parse_from_rfc3339(&until).is_ok())
            .then_some((until, target_state))
    });
    let annotation_reason = annotation
        .object()
        .and_then(|fields| text(fields, "blocking_reason"));
    let ci_infrastructure = annotation_reason.as_deref() == Some("review_ci_infrastructure");
    let ci_deferred = ci_infrastructure && valid_deferred.is_some();

    let mut found: Vec<(u16, ParkReason)> = Vec::new();
    interruption_reasons(&mut found, 0, Field::FailedJson, &failed);
    interruption_reasons(&mut found, 20, Field::BlockedJson, &blocked);

    // An annotation parks only where the dispatcher blocks on it. Anything
    // else (informational kinds, `{}`, untyped or malformed text) is evidence.
    let blocking_annotation = match (&annotation, view.error_annotation) {
        (Column::Object(fields), Some(raw)) if annotation_blocks(raw, fields) => Some(fields),
        _ => None,
    };
    if matches!(annotation, Column::NonText) {
        found.push((40, unknown(&Field::ErrorAnnotation, None, Problem::NonText)));
    }
    if let Some(fields) = blocking_annotation {
        let kind = text(fields, "type").unwrap_or_default();
        let from_annotation = || source(&Field::ErrorAnnotation, None);
        if kind == "manual_stop" {
            found.push((
                42,
                ParkReason::Held {
                    actor: text(fields, "blocked_by").unwrap_or_else(|| "user".to_owned()),
                },
            ));
        } else {
            let reason = annotation_reason.as_deref();
            if reason == Some("review_ci_infrastructure_exhausted") {
                found.push((
                    43,
                    ParkReason::BudgetExhausted {
                        budget_kind: Some(crate::budget::Kind::ReviewCiInfrastructure),
                        source: from_annotation(),
                    },
                ));
            }
            let specialised = matches!(
                reason,
                Some(
                    "review_ci_infrastructure_exhausted"
                        | "pending_remote_cancel"
                        | "dependency_cancelled"
                )
            );
            if !ci_deferred && !specialised {
                found.push((
                    44,
                    if kind == "agent_timeout" {
                        ParkReason::AgentTimeout {
                            source: from_annotation(),
                        }
                    } else if let Some(failure_kind) = failure_kind(&kind) {
                        ParkReason::Failure { failure_kind }
                    } else {
                        unknown(&Field::ErrorAnnotation, None, Problem::UnknownKind)
                    },
                ));
            }
            if reason == Some("pending_remote_cancel") {
                found.push((
                    47,
                    ParkReason::RemoteCancelPending {
                        source: from_annotation(),
                    },
                ));
            }
            if reason == Some("dependency_cancelled") {
                found.push((
                    48,
                    ParkReason::Dependencies {
                        cancelled: true,
                        source: from_annotation(),
                    },
                ));
            }
        }
    }

    match &barrier {
        Column::Absent => {}
        Column::NonText => found.push((
            60,
            unknown(&Field::EntryBarrierJson, None, Problem::NonText),
        )),
        Column::Malformed => found.push((
            60,
            unknown(&Field::EntryBarrierJson, None, Problem::MalformedJson),
        )),
        Column::NonObject => found.push((
            61,
            unknown(&Field::EntryBarrierJson, None, Problem::NonObject),
        )),
        Column::Object(fields) => {
            let exhausted = is(fields, "blocking_reason", "review retry budget exhausted");
            let is_blocked = is(fields, "status", "blocked");
            if exhausted {
                found.push((
                    62,
                    ParkReason::BudgetExhausted {
                        budget_kind: Some(crate::budget::Kind::Review),
                        source: source(&Field::EntryBarrierJson, None),
                    },
                ));
            }
            if is_blocked && !exhausted && !ci_deferred {
                found.push((
                    63,
                    ParkReason::EntryBlocked {
                        state: text(fields, "state"),
                        source: source(&Field::EntryBarrierJson, None),
                    },
                ));
            }
            if !is_blocked {
                found.push((
                    64,
                    unknown(&Field::EntryBarrierJson, None, Problem::UnownedEntry),
                ));
            }
        }
    }

    let in_metadata = |key: &str| source(&Field::MetadataJson, Some(key));
    match &metadata {
        Column::Absent | Column::Object(_) => {}
        Column::NonText => found.push((70, unknown(&Field::MetadataJson, None, Problem::NonText))),
        Column::Malformed => found.push((
            70,
            unknown(&Field::MetadataJson, None, Problem::MalformedJson),
        )),
        Column::NonObject => {
            found.push((71, unknown(&Field::MetadataJson, None, Problem::NonObject)))
        }
    }
    let awaiting_human = m.get("awaiting_human").map(|raw| shape(raw));
    if awaiting_human
        .as_ref()
        .is_some_and(|shape| !matches!(shape, Shape::True | Shape::False | Shape::Null))
    {
        found.push((
            80,
            unknown(
                &Field::MetadataJson,
                Some("awaiting_human"),
                Problem::InvalidShape,
            ),
        ));
    }
    for (offset, key) in OBJECT_METADATA_KEYS.iter().enumerate() {
        if m.get(*key)
            .is_some_and(|raw| !matches!(shape(raw), Shape::Object | Shape::Null))
        {
            found.push((
                81 + offset as u16,
                unknown(&Field::MetadataJson, Some(key), Problem::InvalidShape),
            ));
        }
    }
    // A `deferred_dispatch` object that is not a usable timer does not park:
    // the dispatcher's `is_pending` reads it as "not pending" and dispatches
    // through. Environment writers produce such a shape (`kind`/`reason`
    // only). It stays in evidence.
    let owner_wait = object(m, "owner_wait");
    if let Some(wait) = &owner_wait {
        found.push((
            100,
            ParkReason::OwnerOffline {
                daemon_id: text(wait, "daemon_id"),
                started_at: text(wait, "started_at"),
            },
        ));
    }
    if let Some(wait) = object(m, "environment_wait") {
        let wait_kind = match text(&wait, "kind").as_deref() {
            Some("environment_probe_pending") => ConditionEnvironmentKind::EnvironmentProbePending,
            Some("environment_not_ready") => ConditionEnvironmentKind::EnvironmentNotReady,
            Some("environment_unverified") => ConditionEnvironmentKind::EnvironmentUnverified,
            Some("provision_failed") => ConditionEnvironmentKind::ProvisionFailed,
            _ => ConditionEnvironmentKind::Unknown,
        };
        found.push((
            110,
            ParkReason::Environment {
                wait_kind,
                source: in_metadata("environment_wait"),
            },
        ));
    }
    if object(m, "placement_refusal").is_some() {
        found.push((
            120,
            ParkReason::PlacementDenied {
                source: in_metadata("placement_refusal"),
            },
        ));
    }
    if object(m, "daemon_upgrade_refusal").is_some() {
        found.push((
            130,
            ParkReason::DaemonUpgradeRequired {
                source: in_metadata("daemon_upgrade_refusal"),
            },
        ));
    }
    let paused = object(m, "paused_integration");
    if let Some(paused) = &paused {
        found.push((
            140,
            ParkReason::ProjectPaused {
                state: text(paused, "state"),
            },
        ));
    }
    if awaiting_human == Some(Shape::True) {
        found.push((
            150,
            ParkReason::HumanDecision {
                boundary: match text(m, "awaiting_human_reason").as_deref() {
                    Some("plan_review") => HumanBoundary::PlanReview,
                    Some("pull_request_merge") => HumanBoundary::ExternalMerge,
                    _ => HumanBoundary::Legacy,
                },
                source: in_metadata("awaiting_human"),
            },
        ));
    }
    if let Some(disposition) = object(m, "dispatch_disposition") {
        let capability = text(&disposition, "capability");
        found.push(match capability.as_deref() {
            Some("machine_capacity") => (
                160,
                ParkReason::Capacity {
                    scope: ConditionCapacityScope::Machine,
                },
            ),
            Some("project_capacity") => (
                160,
                ParkReason::Capacity {
                    scope: ConditionCapacityScope::Project,
                },
            ),
            _ => (
                161,
                ParkReason::DispatchRefusal {
                    capability,
                    blocker_digest: text(&disposition, "blocker_digest"),
                },
            ),
        });
    }
    let plan_wait = object(m, "plan_settlement_wait");
    if let (Some(wait), None) = (&plan_wait, &valid_deferred) {
        found.push((
            170,
            ParkReason::PlanSettlementWait {
                execution_id: text(wait, "execution_id"),
            },
        ));
    }

    // One reason per cause (a reason minus where it was read from); the
    // earliest source wins. Then failed-process reasons, holds and exhausted
    // budgets lead, and the rest keep source order.
    let mut reasons: Vec<(u16, u16, ParkReason)> = Vec::with_capacity(found.len());
    let mut causes: Vec<serde_json::Value> = Vec::new();
    found.sort_by_key(|(ordinal, _)| *ordinal);
    let single = found.len() == 1;
    for (ordinal, reason) in found {
        if !single {
            let mut cause = serde_json::to_value(&reason).unwrap_or_default();
            if let Some(cause) = cause.as_object_mut() {
                cause.remove("source");
            }
            if causes.contains(&cause) {
                continue;
            }
            causes.push(cause);
        }
        let priority = match &reason {
            _ if ordinal < 10 => ordinal,
            ParkReason::Held { .. } => 10,
            ParkReason::BudgetExhausted { .. } => 11,
            _ => ordinal + 20,
        };
        reasons.push((priority, ordinal, reason));
    }
    reasons.sort_by_key(|(priority, ordinal, _)| (*priority, *ordinal));
    let mut reasons = reasons.into_iter().map(|(_, _, reason)| reason);

    let evidence = ConditionEvidence {
        error_annotation: view.error_annotation.map(bounded),
        blocked_json: view.blocked_json.map(bounded),
        failed_json: view.failed_json.map(bounded),
        entry_barrier_json: view.entry_barrier_json.map(bounded),
        metadata: EVIDENCE_METADATA_KEYS
            .iter()
            .filter_map(|key| Some(((*key).to_owned(), bounded(m.get(*key)?.get()))))
            .collect(),
        unparsed_metadata: match &metadata {
            Column::Malformed | Column::NonObject => view.metadata_json.map(bounded),
            _ => None,
        },
    };
    let resume = || {
        if let Some(queued) = object(m, "queued_recovery") {
            ConditionContinuation::ResumeQueuedCommand {
                intent_id: text(&queued, "id"),
            }
        } else if let Some(paused) = &paused {
            ConditionContinuation::Integrate {
                state: text(paused, "state"),
            }
        } else if let Some(wait) = &plan_wait {
            ConditionContinuation::SettlePlan {
                execution_id: text(wait, "execution_id"),
            }
        } else if ci_infrastructure {
            ConditionContinuation::RetryEntry {
                state: barrier.object().and_then(|fields| text(fields, "state")),
            }
        } else if let Some((_, target_state)) = &valid_deferred {
            ConditionContinuation::Dispatch {
                target_state: target_state.clone(),
            }
        } else {
            ConditionContinuation::Reconcile
        }
    };
    let since = || {
        failed
            .object()
            .and_then(|fields| text(fields, "created_at"))
            .or_else(|| {
                blocked
                    .object()
                    .and_then(|fields| text(fields, "created_at"))
            })
            .or_else(|| blocking_annotation.and_then(|fields| text(fields, "blocked_at")))
            .or_else(|| {
                barrier
                    .object()
                    .and_then(|fields| text(fields, "started_at"))
            })
            .or_else(|| {
                owner_wait
                    .as_ref()
                    .and_then(|wait| text(wait, "started_at"))
            })
    };
    let Some(primary) = reasons.next() else {
        return match &valid_deferred {
            Some((until, _)) => TaskCondition::Deferred {
                until: until.clone(),
                reason: if plan_wait.is_some() {
                    RetryCause::PlanTransport
                } else if ci_infrastructure {
                    RetryCause::ReviewCiInfrastructure
                } else if text(m, "last_workflow_guard_execution_id").is_some() {
                    RetryCause::WorkflowGuard
                } else if text(m, "last_execution_failure_execution_id").is_some() {
                    RetryCause::ExecutionFailure
                } else {
                    RetryCause::Legacy
                },
                resume: resume(),
                evidence,
            },
            None => TaskCondition::Clear { evidence },
        };
    };
    let additional = reasons.collect();
    if matches!(failed, Column::Absent) {
        TaskCondition::Parked {
            primary,
            additional,
            resume: resume(),
            since: since(),
            evidence,
        }
    } else {
        TaskCondition::Failed {
            failure: primary,
            additional,
            resume: resume(),
            since: since(),
            evidence,
        }
    }
}

fn decode(raw: &str) -> Result<TaskCondition> {
    serde_json::from_str(raw).map_err(|e| DbError::Check(format!("invalid Task condition: {e}")))
}
fn encode(condition: &TaskCondition) -> String {
    serde_json::to_string(condition).expect("Task condition serializes")
}
/// The stored form of a Task's mapped condition, for a writer that folds the
/// shadow into its own legacy `UPDATE` (no extra statement).
pub(crate) fn condition_json(view: LegacyView<'_>) -> String {
    // The common Task write: no interruption column set and metadata that
    // names no condition key. That maps to the empty Clear without parsing.
    // A key name inside an unrelated value only costs the full mapping.
    let no_condition_metadata = match view.metadata_json {
        None => true,
        Some(raw) => {
            view.metadata_is_object && !EVIDENCE_METADATA_KEYS.iter().any(|key| raw.contains(key))
        }
    };
    if no_condition_metadata
        && view.non_text.is_empty()
        && view.error_annotation.is_none()
        && view.blocked_json.is_none()
        && view.failed_json.is_none()
        && view.entry_barrier_json.is_none()
    {
        static CLEAR: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        let clear = CLEAR.get_or_init(|| encode(&map_legacy_condition(&Default::default())));
        #[cfg(test)]
        assert_eq!(*clear, encode(&map_view(&view)));
        return clear.clone();
    }
    encode(&map_view(&view))
}

/// Unfenced shadow write shared by every seam: store `condition` unless the
/// row already holds exactly it. Touches only `condition_json`: no version,
/// timestamp, revision, event or Attention change.
async fn store_condition(
    connection: &mut SqliteConnection,
    task_id: &str,
    condition: &str,
) -> Result<()> {
    debug_assert!(
        decode(condition).is_ok_and(|decoded| encode(&decoded) == condition),
        "Task condition must round-trip through its stored form"
    );
    let written =
        sqlx::query("UPDATE task SET condition_json=?1 WHERE id=?2 AND condition_json IS NOT ?1")
            .bind(condition)
            .bind(task_id)
            .execute(connection)
            .await?
            .rows_affected();
    debug_assert!(written <= 1, "one Task owns one condition");
    Ok(())
}

/// Dual-write seam for a writer that holds the Task as it now stands in the
/// database (all five legacy fields current).
pub(crate) async fn store_task_condition(
    connection: &mut SqliteConnection,
    task: &Task,
) -> Result<()> {
    store_condition(connection, &task.id, &condition_json(task.into())).await
}

/// Dual-write seam for a writer whose SQL computed the legacy values (JSON
/// functions, CASE, bulk predicates): read the row's legacy facts, map them
/// and write the shadow only if it changed. A stored condition is compared as
/// text and never decoded, so an undecodable one is simply overwritten.
pub(crate) async fn sync_condition(connection: &mut SqliteConnection, task_id: &str) -> Result<()> {
    let row = sqlx::query(&format!("SELECT {LEGACY_SELECT} FROM task WHERE id=?"))
        .bind(task_id)
        .fetch_optional(&mut *connection)
        .await?;
    let Some(row) = row else {
        return Ok(()); // Hard deletion has no shadow row.
    };
    let condition = encode(&map_legacy_condition(&LegacyConditionInput::from_row(
        &row,
    )?));
    if row.try_get::<Vec<u8>, _>(5)? == condition.as_bytes() {
        return Ok(());
    }
    store_condition(connection, task_id, &condition).await
}

/// Whether a Task SQL statement can change a legacy condition column, so the
/// queue adapters skip the seam for writes that cannot.
pub(crate) fn writes_legacy_condition(query: &str) -> bool {
    let lower = query.to_ascii_lowercase();
    let assignments = lower.split(" where ").next().unwrap_or(&lower);
    [
        "error_annotation",
        "blocked_json",
        "failed_json",
        "entry_barrier_json",
        "metadata_json",
    ]
    .iter()
    .any(|column| assignments.contains(column))
}

/// Migration post-step for `V202610060030__task_condition`: map every existing
/// Task with the same function the writers use. Runs inside the migration's
/// transaction; only `condition_json` changes, and only where it differs from
/// the column default.
pub(crate) async fn backfill(connection: &mut SqliteConnection) -> Result<()> {
    let mut after: Option<String> = None;
    loop {
        let rows = sqlx::query(&format!(
            "SELECT {LEGACY_SELECT},id FROM task WHERE ?1 IS NULL OR id>?1 ORDER BY id LIMIT 200"
        ))
        .bind(after.as_deref())
        .fetch_all(&mut *connection)
        .await?;
        let Some(last) = rows.last() else {
            return Ok(());
        };
        after = Some(last.try_get(6)?);
        for row in &rows {
            let condition = encode(&map_legacy_condition(&LegacyConditionInput::from_row(row)?));
            if row.try_get::<Vec<u8>, _>(5)? != condition.as_bytes() {
                let id: String = row.try_get(6)?;
                store_condition(&mut *connection, &id, &condition).await?;
            }
        }
    }
}

impl SqliteDb {
    /// Stage-one diagnostic only; no REST/MCP Task fields or readers change.
    pub async fn task_condition(&self, task_id: &str) -> Result<TaskCondition> {
        let raw: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
            .bind(task_id)
            .fetch_optional(self.pool())
            .await?
            .ok_or(DbError::NotFound)?;
        decode(&raw)
    }

    /// Strict single-writer seam for a producer that states a condition. It
    /// requires the Task's claimed step and its live lease, never rebinds a
    /// stale version, and compares all five source values (metadata changes
    /// without advancing Task.version). In stage one the legacy fields are
    /// authoritative, so a condition other than their mapping is refused.
    /// The shadow write bumps no version and emits no event or Attention.
    pub async fn set_condition(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        task_id: &str,
        expected_version: i64,
        expected_legacy: &LegacyConditionInput,
        condition: &TaskCondition,
    ) -> Result<()> {
        if !crate::task_writer::owns_task(task_id) {
            return Err(DbError::Check(
                "set_condition requires the Task step lease".into(),
            ));
        }
        self.fence_task_lease_in_tx(tx, task_id, "set_condition")
            .await?;
        let row = sqlx::query(&format!(
            "SELECT {LEGACY_SELECT},version FROM task WHERE id=?"
        ))
        .bind(task_id)
        .fetch_optional(&mut **tx)
        .await?
        .ok_or(DbError::NotFound)?;
        if row.try_get::<i64, _>(6)? != expected_version
            || LegacyConditionInput::from_row(&row)? != *expected_legacy
        {
            return Err(DbError::VersionConflict);
        }
        if map_legacy_condition(expected_legacy) != *condition {
            return Err(DbError::Check(
                "Task condition differs from legacy fields".into(),
            ));
        }
        store_condition(tx, task_id, &encode(condition)).await
    }

    /// The dual-write seam for a direct SQL writer of a legacy condition
    /// column: call it in the same transaction, after the legacy write. It is
    /// not lease-fenced (Task creation, bulk clears and admission writes hold
    /// no step) and adds no rejection to the legacy write.
    pub async fn sync_condition_in_tx(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        task_id: &str,
    ) -> Result<()> {
        sync_condition(tx, task_id).await
    }

    /// Invariant usable by writer tests: the persisted shadow equals the
    /// mapping of the supplied Task snapshot.
    pub async fn check_task_condition_invariant(&self, task: &Task) -> Result<()> {
        let actual: String = sqlx::query_scalar("SELECT condition_json FROM task WHERE id=?")
            .bind(&task.id)
            .fetch_optional(self.pool())
            .await?
            .ok_or(DbError::NotFound)?;
        if actual != condition_json(task.into()) {
            return Err(DbError::Check(format!(
                "Task condition invariant failed for {}",
                task.id
            )));
        }
        Ok(())
    }

    /// Test-support sweep: ids of every Task whose shadow is not the mapping
    /// of its legacy fields. Stage three owns the production sweep.
    pub async fn task_condition_violations(&self) -> Result<Vec<String>> {
        let rows = sqlx::query(&format!("SELECT {LEGACY_SELECT},id FROM task ORDER BY id"))
            .fetch_all(self.pool())
            .await?;
        let mut violations = Vec::new();
        for row in &rows {
            let expected = encode(&map_legacy_condition(&LegacyConditionInput::from_row(row)?));
            if row.try_get::<Vec<u8>, _>(5)? != expected.as_bytes() {
                violations.push(row.try_get(6)?);
            }
        }
        Ok(violations)
    }
}

#[cfg(test)]
mod tests;
