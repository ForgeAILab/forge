//! The head state machine as data. The driver dispatches on `action` and
//! refuses any transition that is not an edge of the row it is in.
use db::IntegrationAttemptState as S;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeadAction {
    /// Stamp `started_at`, count the round, go to validation.
    Start,
    /// Read facts; decide landed / unchanged target / rebase.
    Validate,
    /// Fenced owner rebase onto the read tip (or adopt its receipt).
    Rebase,
    /// Wait for the check verdict the `request_check` step asked for.
    AwaitCheck,
    /// Wait for the `settle` step's permit.
    AwaitPermit,
    /// Cancel-or-commit: transfer the candidate, then CAS to `ff_inflight`.
    CommitFastForward,
    /// Fenced owner fast-forward of the exact candidate.
    FastForward,
    /// Settle an unknown effect by owner receipt or Git witness.
    Reconcile,
    /// Wait for the `result` step (or for the Task to leave `merging`).
    AwaitResult,
    /// Not a head: left to the sweeps and to Task steps.
    Released,
    /// A quarantined head is put back to `reconciling` on the timer.
    Requeue,
    Terminal,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeadTimeout {
    None,
    Validate,
    Rebase,
    Check,
    Step,
    Permit,
    FastForward,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CancelRule {
    /// The worker transitions to `cancelled` on its next pass / sweep.
    Now,
    /// The running effect is cancelled first; its receipt releases the head.
    AfterReceipt,
    /// Storage refuses the request; the effect runs to a known result.
    Refused,
    NotApplicable,
}

#[derive(Debug, Clone, Copy)]
pub struct HeadStateRow {
    pub state: S,
    pub action: HeadAction,
    pub success: &'static [S],
    pub failure: &'static [S],
    pub timeout: HeadTimeout,
    pub on_timeout: Option<S>,
    pub cancel: CancelRule,
    /// What a restart finds and does (the crash matrix test asserts it).
    pub crash_recovery: &'static str,
}
impl HeadStateRow {
    pub fn allows(&self, to: S) -> bool {
        self.success.contains(&to) || self.failure.contains(&to) || self.on_timeout == Some(to)
    }
}

pub const HEAD_TABLE: &[HeadStateRow] = &[
    HeadStateRow {
        state: S::Queued,
        action: HeadAction::Start,
        success: &[S::Validating],
        failure: &[S::Parked, S::Cancelled],
        timeout: HeadTimeout::None,
        on_timeout: None,
        cancel: CancelRule::Now,
        crash_recovery: "lease expires; the next claim starts it again",
    },
    HeadStateRow {
        state: S::PathWait,
        action: HeadAction::Start,
        success: &[S::Validating],
        failure: &[S::Parked, S::Cancelled],
        timeout: HeadTimeout::None,
        on_timeout: None,
        cancel: CancelRule::Now,
        crash_recovery: "as queued",
    },
    HeadStateRow {
        state: S::Validating,
        action: HeadAction::Validate,
        success: &[S::AwaitingTaskStep, S::Rebasing, S::Applied],
        failure: &[S::NeedsReview, S::Parked, S::Cancelled],
        timeout: HeadTimeout::Validate,
        on_timeout: Some(S::Parked),
        cancel: CancelRule::Now,
        crash_recovery: "no intent exists; the next claim validates again",
    },
    HeadStateRow {
        state: S::Rebasing,
        action: HeadAction::Rebase,
        success: &[S::Checking],
        failure: &[S::Ejected, S::Parked, S::Reconciling, S::Cancelled],
        timeout: HeadTimeout::Rebase,
        on_timeout: Some(S::Parked),
        cancel: CancelRule::AfterReceipt,
        crash_recovery:
            "intent without receipt: claim forces reconciling; receipt without transition: the next round adopts it",
    },
    HeadStateRow {
        state: S::Checking,
        action: HeadAction::AwaitCheck,
        success: &[S::AwaitingTaskStep],
        failure: &[S::Ejected, S::Parked, S::Cancelled],
        timeout: HeadTimeout::Check,
        on_timeout: Some(S::Parked),
        cancel: CancelRule::Now,
        crash_recovery: "the next claim asks again under a new effect_seq; the runner joins the same run",
    },
    HeadStateRow {
        state: S::AwaitingTaskStep,
        action: HeadAction::AwaitPermit,
        success: &[S::ReadyFf],
        failure: &[S::NeedsReview, S::Ejected, S::Parked, S::Cancelled],
        timeout: HeadTimeout::Step,
        on_timeout: Some(S::Parked),
        cancel: CancelRule::Now,
        crash_recovery: "the next claim asks for a permit bound to its own generation",
    },
    HeadStateRow {
        state: S::ReadyFf,
        action: HeadAction::CommitFastForward,
        success: &[S::FfInflight],
        failure: &[S::AwaitingTaskStep, S::Parked, S::Cancelled],
        timeout: HeadTimeout::Permit,
        on_timeout: Some(S::AwaitingTaskStep),
        cancel: CancelRule::Now,
        crash_recovery: "claim drops the permit and returns to awaiting_task_step",
    },
    HeadStateRow {
        state: S::FfInflight,
        action: HeadAction::FastForward,
        success: &[S::Applied],
        failure: &[S::Rebasing, S::Reconciling],
        timeout: HeadTimeout::FastForward,
        on_timeout: Some(S::Reconciling),
        cancel: CancelRule::Refused,
        crash_recovery: "claim forces reconciling; never repeated without a settled receipt",
    },
    HeadStateRow {
        state: S::Reconciling,
        action: HeadAction::Reconcile,
        success: &[S::Applied, S::Queued, S::Rebasing],
        failure: &[S::Parked, S::Quarantined],
        timeout: HeadTimeout::None,
        on_timeout: None,
        cancel: CancelRule::Refused,
        crash_recovery: "this state is the recovery; it is re-entered as it is",
    },
    HeadStateRow {
        state: S::Applied,
        action: HeadAction::AwaitResult,
        success: &[S::Completed],
        failure: &[],
        timeout: HeadTimeout::None,
        on_timeout: None,
        cancel: CancelRule::Refused,
        crash_recovery: "Git is done and recorded; the next claim re-readies the result step",
    },
    HeadStateRow {
        state: S::Ejected,
        action: HeadAction::Released,
        success: &[],
        failure: &[S::Cancelled],
        timeout: HeadTimeout::None,
        on_timeout: None,
        cancel: CancelRule::Now,
        crash_recovery: "slot already released; the send_back step is durable",
    },
    HeadStateRow {
        state: S::NeedsReview,
        action: HeadAction::Released,
        success: &[],
        failure: &[S::Cancelled],
        timeout: HeadTimeout::None,
        on_timeout: None,
        cancel: CancelRule::Now,
        crash_recovery: "as ejected",
    },
    HeadStateRow {
        state: S::Parked,
        action: HeadAction::Released,
        success: &[S::Queued],
        failure: &[S::Cancelled],
        timeout: HeadTimeout::None,
        on_timeout: None,
        cancel: CancelRule::Now,
        crash_recovery: "row and available_at are durable; the due-parked sweep is stateless",
    },
    HeadStateRow {
        state: S::Quarantined,
        action: HeadAction::Requeue,
        success: &[S::Reconciling],
        failure: &[],
        timeout: HeadTimeout::None,
        on_timeout: None,
        cancel: CancelRule::Refused,
        crash_recovery: "durable; the reconcile timer puts it back to reconciling",
    },
    HeadStateRow {
        state: S::Completed,
        action: HeadAction::Terminal,
        success: &[],
        failure: &[],
        timeout: HeadTimeout::None,
        on_timeout: None,
        cancel: CancelRule::NotApplicable,
        crash_recovery: "terminal",
    },
    HeadStateRow {
        state: S::Cancelled,
        action: HeadAction::Terminal,
        success: &[],
        failure: &[],
        timeout: HeadTimeout::None,
        on_timeout: None,
        cancel: CancelRule::NotApplicable,
        crash_recovery: "terminal",
    },
    HeadStateRow {
        state: S::Superseded,
        action: HeadAction::Terminal,
        success: &[],
        failure: &[],
        timeout: HeadTimeout::None,
        on_timeout: None,
        cancel: CancelRule::NotApplicable,
        crash_recovery: "terminal",
    },
];

pub fn head_row(state: S) -> &'static HeadStateRow {
    HEAD_TABLE
        .iter()
        .find(|row| row.state == state)
        .expect("total head table")
}
