//! Review repository and settlement requests.

use super::*;

/// Producer of a terminal review mutation; human decisions never impersonate
/// the runner's completion notification.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ReviewEventOrigin {
    Runner,
    User,
    /// A human decision made through a credential an agent may also hold
    /// (MCP). Audited as the user's, charged like the runner's.
    DelegatedUser,
    Executor,
}

impl ReviewEventOrigin {
    pub(crate) fn actor_type(self) -> &'static str {
        match self {
            Self::Runner | Self::Executor => "review_runner",
            Self::User | Self::DelegatedUser => "user",
        }
    }
    /// The owner's decision, or a reviewer execution that failed before giving
    /// a verdict, spends no Review budget.
    pub(crate) fn spends_no_budget(self) -> bool {
        matches!(self, Self::User | Self::Executor)
    }
    /// The origin of a human decision made by `actor`.
    pub fn for_human(actor: &api_types::Actor) -> Self {
        if actor.is_owner() {
            Self::User
        } else {
            Self::DelegatedUser
        }
    }
}

#[async_trait]
pub trait ReviewRepo: Send + Sync {
    async fn create(&self, input: CreateReview) -> Result<Review>;
    /// Atomically create a Review attempt only while the Task/project/candidate
    /// snapshot that selected it is still current. This is the Review-only
    /// authority boundary used by blocking workflow hooks; unlike a
    /// read-then-create sequence, the checks and INSERT share SQLite's writer
    /// transaction.
    async fn create_with_task_authority(
        &self,
        input: CreateReview,
        expected_task_version: i64,
        expected_task_status: &str,
        expected_project_version: Option<i64>,
        expected_workflow_definition: Option<&str>,
        expected_candidate_execution_id: Option<&str>,
    ) -> Result<Review>;
    /// Atomically record a user override of the latest failed Review as a new
    /// passed attempt and update the Task review projection. The failed source
    /// row remains immutable and all authority checks run under one writer
    /// transaction.
    async fn create_manual_pass_with_task_authority(
        &self,
        input: CreateManualReviewPass,
    ) -> Result<(Review, Task)>;
    /// Transactional variant used to commit a linked follow-up Task together
    /// with the manual pass. The caller owns commit and post-commit events.
    async fn create_manual_pass_with_task_authority_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateManualReviewPass,
    ) -> Result<(Review, Task)>;
    async fn update_status(
        &self,
        id: &str,
        status: ReviewStatus,
        step_results_json: String,
        finished_at: Option<String>,
        updated_at: &str,
    ) -> Result<Review>;
    /// Compensating cancellation for a Review attempt after its execution has
    /// been reserved.  The status and timestamp are an exact CAS snapshot;
    /// a concurrent Review writer wins and leaves the row untouched.  This
    /// intentionally does not update Task.review_passed_at: cancellation only
    /// revokes the stale Review authority.
    async fn cancel_if_unchanged(
        &self,
        id: &str,
        expected_status: ReviewStatus,
        expected_updated_at: &str,
        step_results_json: String,
        finished_at: &str,
        updated_at: &str,
    ) -> Result<Option<Review>>;
    /// Atomically settle a Review and its Task review-authority projection.
    ///
    /// The expected Task version is part of the same SQLite write transaction
    /// as the Review transition.  If another Task writer wins first, neither
    /// row is changed, so a terminal Review can never commit while its
    /// `review_passed_at` projection is left behind for reconciliation.
    #[allow(clippy::too_many_arguments)]
    async fn update_status_with_task_authority(
        &self,
        id: &str,
        status: ReviewStatus,
        step_results_json: String,
        finished_at: Option<String>,
        updated_at: &str,
        expected_task_version: i64,
        review_passed_at: Option<String>,
        origin: ReviewEventOrigin,
    ) -> Result<(Review, Task)>;
    /// Terminal Review settlement variant that also binds the result to the
    /// exact implementation execution currently selected for the Task.
    #[allow(clippy::too_many_arguments)]
    async fn update_status_with_task_authority_and_candidate(
        &self,
        id: &str,
        status: ReviewStatus,
        step_results_json: String,
        finished_at: Option<String>,
        updated_at: &str,
        expected_task_version: i64,
        review_passed_at: Option<String>,
        expected_candidate_execution_id: &str,
    ) -> Result<(Review, Task)>;
    /// Workflow-hook variant of terminal settlement that also binds the
    /// Review write to the exact Project workflow authority that selected the
    /// hook.
    #[allow(clippy::too_many_arguments)]
    async fn update_status_with_task_authority_and_project_candidate(
        &self,
        id: &str,
        status: ReviewStatus,
        step_results_json: String,
        finished_at: Option<String>,
        updated_at: &str,
        expected_task_version: i64,
        review_passed_at: Option<String>,
        expected_project_version: Option<i64>,
        expected_workflow_definition: Option<&str>,
        expected_review_status: ReviewStatus,
        expected_review_updated_at: &str,
        expected_candidate_execution_id: &str,
    ) -> Result<(Review, Task)>;
    /// Atomically update a non-terminal Review state while binding the write
    /// to the exact Task/Project/candidate snapshot that selected the hook.
    #[allow(clippy::too_many_arguments)]
    async fn update_status_with_review_authority(
        &self,
        id: &str,
        status: ReviewStatus,
        step_results_json: String,
        finished_at: Option<String>,
        updated_at: &str,
        expected_task_version: i64,
        expected_task_status: &str,
        expected_project_version: Option<i64>,
        expected_workflow_definition: Option<&str>,
        expected_review_status: ReviewStatus,
        expected_review_updated_at: &str,
        expected_candidate_execution_id: Option<&str>,
    ) -> Result<Review>;
    /// Atomically settle any Review status while binding it to the exact
    /// Task/Project/workflow/candidate snapshot. `task_projection` is `None`
    /// when the status change must not mutate Task.review_passed_at; `Some`
    /// applies the contained value in the same writer transaction.
    #[allow(clippy::too_many_arguments)]
    async fn update_status_with_review_authority_and_task_projection(
        &self,
        id: &str,
        status: ReviewStatus,
        step_results_json: String,
        finished_at: Option<String>,
        updated_at: &str,
        expected_task_version: i64,
        expected_task_status: &str,
        expected_project_version: Option<i64>,
        expected_workflow_definition: Option<&str>,
        expected_review_status: ReviewStatus,
        expected_review_updated_at: &str,
        expected_candidate_execution_id: &str,
        task_projection: Option<Option<String>>,
    ) -> Result<Review>;
    /// Settle the Running Review of a mechanical integration step as Passed
    /// under the previous review's authority, set `Task.review_passed_at`, and
    /// record the carried candidate, all in one writer transaction bound to the
    /// same Task/Project/workflow/candidate snapshot as any other settlement.
    async fn settle_carried_review(&self, input: SettleCarriedReview) -> Result<Review>;
    async fn get_by_id(&self, id: &str) -> Result<Option<Review>>;
    async fn list_by_task(&self, task_id: &str) -> Result<Vec<Review>>;
    async fn list_latest_reviews_for_tasks(&self, task_ids: &[&str]) -> Result<Vec<Review>>;
    async fn next_attempt_number(&self, task_id: &str) -> Result<i64>;
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CreateReview {
    pub id: String,
    pub task_id: String,
    pub execution_id: String,
    pub attempt_number: i64,
    pub status: ReviewStatus,
    pub step_results_json: String,
    pub started_at: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SettleCarriedReview {
    pub review_id: String,
    pub expected_review_updated_at: String,
    pub step_results_json: String,
    pub candidate_execution_id: String,
    pub expected_task_version: i64,
    pub expected_task_status: String,
    pub expected_project_version: i64,
    pub expected_workflow_definition: String,
    pub carry: crate::NewReviewAuthorityCarry,
    pub occurred_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CreateManualReviewPass {
    pub id: String,
    pub source_review_id: String,
    pub source_review_updated_at: String,
    pub task_id: String,
    pub candidate_execution_id: String,
    pub step_results_json: String,
    pub expected_task_version: i64,
    pub expected_task_status: String,
    pub expected_project_version: i64,
    pub expected_workflow_definition: String,
    pub occurred_at: String,
}
