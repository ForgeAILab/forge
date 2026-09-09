use super::*;
use api_types::{
    AccountUsageAnalyticsResponse, ActivityCounts, AgentUsageBreakdown, AnalyticsWindow,
    CostCoverage, CostCoverageReason, CostCoverageReasonCode as ApiReasonCode, CostKind,
    CostSourceFreshness, CostSourceKind, CostSourceRef, CostSummary, ModelUsageBreakdown,
    MoneyAmount, ProjectUsageBreakdown, SurfaceUsageBreakdown, TokenCounters as ApiTokenCounters,
    UsageAggregate, UsageAnalytics, UsageCostCoverage, UsageSurface as ApiUsageSurface,
};
use chrono::DateTime;
use serde::Deserialize;
use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    hash::Hash,
    str::FromStr,
};

#[derive(Debug, Deserialize)]
struct StepResult {
    command: String,
    exit_code: i64,
    #[serde(default)]
    started_at: Option<String>,
    #[serde(default)]
    finished_at: Option<String>,
}

#[derive(Debug, Deserialize)]
struct StepResultsObject {
    #[serde(default)]
    ci_steps: Vec<StepResult>,
}

#[derive(Debug, Default)]
struct StepAggregate {
    total_runs: i64,
    pass_count: i64,
    fail_count: i64,
    duration_ms: Vec<i64>,
    last_run_at: Option<String>,
}

fn parse_step_results(step_results_json: &str) -> Vec<StepResult> {
    if let Ok(steps) = serde_json::from_str::<Vec<StepResult>>(step_results_json) {
        return steps;
    }
    if let Ok(payload) = serde_json::from_str::<StepResultsObject>(step_results_json) {
        return payload.ci_steps;
    }
    Vec::new()
}

fn parse_duration_ms(started_at: &str, finished_at: &str) -> Option<i64> {
    let started = DateTime::parse_from_rfc3339(started_at).ok()?;
    let finished = DateTime::parse_from_rfc3339(finished_at).ok()?;
    let delta_ms = finished.signed_duration_since(started).num_milliseconds();
    if delta_ms < 0 {
        return None;
    }
    Some(delta_ms)
}

async fn list_review_step_results_json(
    db: &SqliteDb,
    project_id: &str,
    from: Option<&str>,
    to: Option<&str>,
) -> Result<Vec<String>> {
    let mut query = sqlx::QueryBuilder::<Sqlite>::new(
        "SELECT r.step_results_json \
         FROM review r \
         JOIN execution e ON r.execution_id = e.id \
         JOIN task t ON e.task_id = t.id \
         WHERE t.project_id = ",
    );
    query.push_bind(project_id);
    if let Some(from) = from {
        query
            .push(" AND julianday(r.started_at) >= julianday(")
            .push_bind(from)
            .push(")");
    }
    if let Some(to) = to {
        query
            .push(" AND julianday(r.started_at) < julianday(")
            .push_bind(to)
            .push(")");
    }

    let rows = query.build().fetch_all(db.pool()).await?;
    rows.into_iter()
        .map(|row| row.try_get("step_results_json").map_err(Into::into))
        .collect()
}

#[async_trait]
impl ProjectAnalyticsRepo for SqliteDb {
    async fn get_project_ci_analytics(
        &self,
        project_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<Vec<CiStepStats>> {
        let rows = list_review_step_results_json(self, project_id, from, to).await?;

        let mut by_command: BTreeMap<String, StepAggregate> = BTreeMap::new();
        for step_results_json in rows {
            let step_results = parse_step_results(&step_results_json);
            for step in step_results {
                let StepResult {
                    command,
                    exit_code,
                    started_at,
                    finished_at,
                } = step;
                let entry = by_command.entry(command).or_default();
                entry.total_runs = entry
                    .total_runs
                    .checked_add(1)
                    .ok_or(DbError::InvalidTransition)?;
                if exit_code == 0 {
                    entry.pass_count = entry
                        .pass_count
                        .checked_add(1)
                        .ok_or(DbError::InvalidTransition)?;
                } else {
                    entry.fail_count = entry
                        .fail_count
                        .checked_add(1)
                        .ok_or(DbError::InvalidTransition)?;
                }

                if let Some(finished_at) = finished_at.as_deref() {
                    let should_update = entry
                        .last_run_at
                        .as_deref()
                        .map(|current| finished_at > current)
                        .unwrap_or(true);
                    if should_update {
                        entry.last_run_at = Some(finished_at.to_owned());
                    }
                }

                if let (Some(started_at), Some(finished_at)) =
                    (started_at.as_deref(), finished_at.as_deref())
                {
                    if let Some(duration_ms) = parse_duration_ms(started_at, finished_at) {
                        entry.duration_ms.push(duration_ms);
                    }
                }
            }
        }

        by_command
            .into_iter()
            .map(|(command, mut aggregate)| -> Result<CiStepStats> {
                aggregate.duration_ms.sort_unstable();
                let len = aggregate.duration_ms.len();
                let avg_duration_ms = if len > 0 {
                    let total = aggregate
                        .duration_ms
                        .iter()
                        .try_fold(0_i64, |total, value| total.checked_add(*value))
                        .ok_or(DbError::InvalidTransition)?;
                    let len = i64::try_from(len).map_err(|_| DbError::InvalidTransition)?;
                    Some(total / len)
                } else {
                    None
                };
                let p50_duration_ms = if len > 0 {
                    Some(aggregate.duration_ms[len / 2])
                } else {
                    None
                };
                let p95_duration_ms = if len > 0 {
                    let p95_index = len
                        .checked_mul(95)
                        .and_then(|value| value.checked_div(100))
                        .ok_or(DbError::InvalidTransition)?;
                    Some(aggregate.duration_ms[p95_index])
                } else {
                    None
                };

                Ok(CiStepStats {
                    command,
                    total_runs: aggregate.total_runs,
                    pass_count: aggregate.pass_count,
                    fail_count: aggregate.fail_count,
                    avg_duration_ms,
                    p50_duration_ms,
                    p95_duration_ms,
                    last_run_at: aggregate.last_run_at,
                })
            })
            .collect()
    }

    async fn get_project_review_summary(
        &self,
        project_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<ProjectReviewSummary> {
        let mut query = sqlx::QueryBuilder::<Sqlite>::new(
            "SELECT \
                COUNT(*) AS total_reviews, \
                COALESCE(SUM(CASE WHEN r.status = 'passed' THEN 1 ELSE 0 END), 0) AS passed, \
                COALESCE(SUM(CASE WHEN r.status = 'failed' THEN 1 ELSE 0 END), 0) AS failed, \
                COALESCE(SUM(CASE WHEN r.status = 'cancelled' THEN 1 ELSE 0 END), 0) AS cancelled, \
                AVG(CASE \
                    WHEN r.started_at IS NOT NULL AND r.finished_at IS NOT NULL \
                    THEN CAST((JULIANDAY(r.finished_at) - JULIANDAY(r.started_at)) * 86400000 AS INTEGER) \
                    ELSE NULL \
                END) AS avg_duration_ms \
             FROM review r \
             JOIN execution e ON r.execution_id = e.id \
             JOIN task t ON e.task_id = t.id \
             WHERE t.project_id = ",
        );
        query.push_bind(project_id);
        if let Some(from) = from {
            query
                .push(" AND julianday(r.started_at) >= julianday(")
                .push_bind(from)
                .push(")");
        }
        if let Some(to) = to {
            query
                .push(" AND julianday(r.started_at) < julianday(")
                .push_bind(to)
                .push(")");
        }

        let row = query.build().fetch_one(self.pool()).await?;
        let total_reviews: i64 = row.try_get("total_reviews")?;
        let passed: i64 = row.try_get("passed")?;
        let failed: i64 = row.try_get("failed")?;
        let cancelled: i64 = row.try_get("cancelled")?;
        let avg_duration_ms: Option<i64> = row
            .try_get::<Option<f64>, _>("avg_duration_ms")?
            .map(|value| {
                if value.is_finite() && value >= i64::MIN as f64 && value <= i64::MAX as f64 {
                    Ok(value as i64)
                } else {
                    Err(DbError::InvalidTransition)
                }
            })
            .transpose()?;
        let pass_rate = if total_reviews > 0 {
            passed as f64 / total_reviews as f64
        } else {
            0.0
        };

        Ok(ProjectReviewSummary {
            total_reviews,
            passed,
            failed,
            cancelled,
            avg_duration_ms,
            pass_rate,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::WorkMode;

    async fn sqlite_db() -> SqliteDb {
        let pool = crate::create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        crate::run_migrations(&pool).await.expect("migrations run");
        SqliteDb::new(pool)
    }

    async fn seed_project_repo_task(db: &SqliteDb) -> (String, String, String) {
        let now = crate::now_rfc3339();
        let project_id = crate::new_uuid_v4();
        let repo_id = crate::new_uuid_v4();
        let task_id = crate::new_uuid_v4();

        ProjectRepo::create(
            db,
            CreateProject {
                id: project_id.clone(),
                name: "analytics-project".to_owned(),
                settings: "{}".to_owned(),
                workflow_definition: "{}".to_owned(),
                primary_repo_id: None,
                owner_id: None,
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("project creates");

        RepoRepo::create(
            db,
            CreateRepo {
                id: repo_id.clone(),
                project_id: project_id.clone(),
                name: "analytics-repo".to_owned(),
                remote_url: "https://example.com/forge-analytics.git".to_owned(),
                local_path: Some("/tmp/forge-analytics-test-repo".to_owned()),
                work_mode: WorkMode::DirectMerge,
                default_branch: "main".to_owned(),
                created_at: now.clone(),
                updated_at: now.clone(),
            },
        )
        .await
        .expect("repo creates");
        ProjectRepo::update_at_version(
            db,
            UpdateProject {
                id: project_id.clone(),
                name: None,
                settings: None,
                primary_repo_id: Some(Some(repo_id.clone())),
                paused_at: None,
                updated_at: crate::now_rfc3339(),
            },
            ProjectRepo::get_by_id(db, &project_id)
                .await
                .expect("fixture Project lookup")
                .expect("fixture Project exists")
                .version,
            None,
        )
        .await
        .expect("project primary repo updates");

        TaskRepo::create(
            db,
            CreateTask {
                id: task_id.clone(),
                project_id: project_id.clone(),
                repo_id: Some(repo_id.clone()),
                parent_task_id: None,
                assignee_type: None,
                assignee_id: None,
                title: "Task".to_owned(),
                description: None,
                task_type: "task".to_owned(),
                status: "todo".to_owned(),
                is_automation: false,
                priority: 0,
                subtask_order: None,
                task_state_config: None,
                merge_config: None,
                plan: None,
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .expect("task creates");

        (project_id, repo_id, task_id)
    }

    async fn seed_execution(db: &SqliteDb, task_id: &str, created_at: &str) -> String {
        let execution_id = crate::new_uuid_v4();
        ExecutionRepo::create(
            db,
            CreateExecution {
                id: execution_id.clone(),
                task_id: task_id.to_owned(),
                agent_id: None,
                role: "reviewer".to_owned(),
                status: ExecutionStatus::Running,
                stop_reason: None,
                stopped_by: None,
                resume_policy: None,
                stopped_at: None,
                parent_execution_id: None,
                agent_session_id: None,
                agent_message_id: None,
                last_activity_at: None,
                summary: None,
                logs_path: None,
                before_sha: None,
                after_sha: None,
                error: None,
                executor_config_snapshot_json: None,
                workspace_id: None,
                created_at: created_at.to_owned(),
                updated_at: created_at.to_owned(),
            },
        )
        .await
        .expect("execution creates");
        execution_id
    }

    async fn seed_review(
        db: &SqliteDb,
        task_id: &str,
        execution_id: &str,
        status: ReviewStatus,
        started_at: &str,
        step_results_json: &str,
    ) {
        let attempt_number = ReviewRepo::next_attempt_number(db, task_id)
            .await
            .expect("attempt number available");
        ReviewRepo::create(
            db,
            CreateReview {
                id: crate::new_uuid_v4(),
                task_id: task_id.to_owned(),
                execution_id: execution_id.to_owned(),
                attempt_number,
                status,
                step_results_json: step_results_json.to_owned(),
                started_at: started_at.to_owned(),
                created_at: started_at.to_owned(),
                updated_at: started_at.to_owned(),
            },
        )
        .await
        .expect("review creates");
    }

    #[allow(clippy::too_many_arguments)]
    #[tokio::test]
    async fn ci_analytics_happy_path() {
        let db = sqlite_db().await;
        let (project_id, _repo_id, task_id) = seed_project_repo_task(&db).await;
        let execution_one = seed_execution(&db, &task_id, "2026-04-10T00:00:00Z").await;
        let execution_two = seed_execution(&db, &task_id, "2026-04-11T00:00:00Z").await;

        seed_review(
            &db,
            &task_id,
            &execution_one,
            ReviewStatus::Passed,
            "2026-04-10T00:00:00Z",
            r#"[
                {"command":"cargo test","exit_code":0,"started_at":"2026-04-10T00:00:00Z","finished_at":"2026-04-10T00:00:10Z"},
                {"command":"cargo clippy","exit_code":1,"started_at":"2026-04-10T00:00:20Z","finished_at":"2026-04-10T00:00:25Z"}
            ]"#,
        )
        .await;
        seed_review(
            &db,
            &task_id,
            &execution_two,
            ReviewStatus::Failed,
            "2026-04-11T00:00:00Z",
            r#"{"ci_steps":[{"command":"cargo test","exit_code":1,"started_at":"2026-04-11T00:00:00Z","finished_at":"2026-04-11T00:00:20Z"}]}"#,
        )
        .await;

        let stats = ProjectAnalyticsRepo::get_project_ci_analytics(&db, &project_id, None, None)
            .await
            .expect("ci analytics computed");

        assert_eq!(stats.len(), 2);
        assert_eq!(stats[0].command, "cargo clippy");
        assert_eq!(stats[0].total_runs, 1);
        assert_eq!(stats[0].pass_count, 0);
        assert_eq!(stats[0].fail_count, 1);
        assert_eq!(stats[0].avg_duration_ms, Some(5000));
        assert_eq!(stats[0].p50_duration_ms, Some(5000));
        assert_eq!(stats[0].p95_duration_ms, Some(5000));
        assert_eq!(
            stats[0].last_run_at.as_deref(),
            Some("2026-04-10T00:00:25Z")
        );

        assert_eq!(stats[1].command, "cargo test");
        assert_eq!(stats[1].total_runs, 2);
        assert_eq!(stats[1].pass_count, 1);
        assert_eq!(stats[1].fail_count, 1);
        assert_eq!(stats[1].avg_duration_ms, Some(15000));
        assert_eq!(stats[1].p50_duration_ms, Some(20000));
        assert_eq!(stats[1].p95_duration_ms, Some(20000));
        assert_eq!(
            stats[1].last_run_at.as_deref(),
            Some("2026-04-11T00:00:20Z")
        );
    }

    #[tokio::test]
    async fn ci_analytics_empty_result() {
        let db = sqlite_db().await;
        let project_id = crate::new_uuid_v4();

        let stats = ProjectAnalyticsRepo::get_project_ci_analytics(&db, &project_id, None, None)
            .await
            .expect("ci analytics computed");

        assert!(stats.is_empty());
    }

    #[tokio::test]
    async fn ci_analytics_date_filter() {
        let db = sqlite_db().await;
        let (project_id, _repo_id, task_id) = seed_project_repo_task(&db).await;
        let execution_old = seed_execution(&db, &task_id, "2026-03-01T00:00:00Z").await;
        let execution_new = seed_execution(&db, &task_id, "2026-04-20T00:00:00Z").await;

        seed_review(
            &db,
            &task_id,
            &execution_old,
            ReviewStatus::Passed,
            "2026-03-01T00:00:00Z",
            r#"[{"command":"cargo test","exit_code":0}]"#,
        )
        .await;
        seed_review(
            &db,
            &task_id,
            &execution_new,
            ReviewStatus::Passed,
            "2026-04-20T00:00:00Z",
            r#"[{"command":"cargo clippy","exit_code":0}]"#,
        )
        .await;

        let stats = ProjectAnalyticsRepo::get_project_ci_analytics(
            &db,
            &project_id,
            Some("2026-04-01T00:00:00Z"),
            Some("2026-04-30T23:59:59Z"),
        )
        .await
        .expect("ci analytics filtered");

        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].command, "cargo clippy");
        assert_eq!(stats[0].total_runs, 1);
        assert_eq!(stats[0].avg_duration_ms, None);
        assert_eq!(stats[0].p50_duration_ms, None);
        assert_eq!(stats[0].p95_duration_ms, None);
        assert_eq!(stats[0].last_run_at, None);
    }

    #[tokio::test]
    async fn review_summary_happy_path() {
        let db = sqlite_db().await;
        let (project_id, _repo_id, task_id) = seed_project_repo_task(&db).await;
        let execution_one = seed_execution(&db, &task_id, "2026-04-10T00:00:00Z").await;
        let execution_two = seed_execution(&db, &task_id, "2026-04-11T00:00:00Z").await;
        let execution_three = seed_execution(&db, &task_id, "2026-04-12T00:00:00Z").await;

        seed_review(
            &db,
            &task_id,
            &execution_one,
            ReviewStatus::Passed,
            "2026-04-10T00:00:00Z",
            r#"[{"command":"cargo test","exit_code":0},{"command":"cargo clippy","exit_code":0}]"#,
        )
        .await;
        seed_review(
            &db,
            &task_id,
            &execution_two,
            ReviewStatus::Failed,
            "2026-04-11T00:00:00Z",
            r#"{"ci_steps":[{"command":"cargo test","exit_code":1}]}"#,
        )
        .await;
        seed_review(
            &db,
            &task_id,
            &execution_three,
            ReviewStatus::Passed,
            "2026-04-12T00:00:00Z",
            r#"[{"command":"cargo fmt","exit_code":0},{"command":"cargo test","exit_code":0},{"command":"cargo clippy","exit_code":0}]"#,
        )
        .await;

        let summary =
            ProjectAnalyticsRepo::get_project_review_summary(&db, &project_id, None, None)
                .await
                .expect("review summary computed");

        assert_eq!(summary.total_reviews, 3);
        assert_eq!(summary.passed, 2);
        assert_eq!(summary.failed, 1);
        assert_eq!(summary.cancelled, 0);
        assert_eq!(summary.avg_duration_ms, None);
        assert!((summary.pass_rate - (2.0 / 3.0)).abs() < f64::EPSILON);
    }

    #[tokio::test]
    async fn review_summary_empty_result() {
        let db = sqlite_db().await;
        let project_id = crate::new_uuid_v4();

        let summary =
            ProjectAnalyticsRepo::get_project_review_summary(&db, &project_id, None, None)
                .await
                .expect("review summary computed");

        assert_eq!(summary.total_reviews, 0);
        assert_eq!(summary.passed, 0);
        assert_eq!(summary.failed, 0);
        assert_eq!(summary.cancelled, 0);
        assert_eq!(summary.avg_duration_ms, None);
        assert_eq!(summary.pass_rate, 0.0);
    }

    #[tokio::test]
    async fn review_summary_date_filter() {
        let db = sqlite_db().await;
        let (project_id, _repo_id, task_id) = seed_project_repo_task(&db).await;
        let execution_old = seed_execution(&db, &task_id, "2026-03-01T00:00:00Z").await;
        let execution_new = seed_execution(&db, &task_id, "2026-04-20T00:00:00Z").await;

        seed_review(
            &db,
            &task_id,
            &execution_old,
            ReviewStatus::Failed,
            "2026-03-01T00:00:00Z",
            r#"[{"command":"cargo test","exit_code":1}]"#,
        )
        .await;
        seed_review(
            &db,
            &task_id,
            &execution_new,
            ReviewStatus::Passed,
            "2026-04-20T00:00:00Z",
            r#"[{"command":"cargo test","exit_code":0},{"command":"cargo clippy","exit_code":0}]"#,
        )
        .await;

        let summary = ProjectAnalyticsRepo::get_project_review_summary(
            &db,
            &project_id,
            Some("2026-04-01T00:00:00Z"),
            Some("2026-04-30T23:59:59Z"),
        )
        .await
        .expect("review summary filtered");

        assert_eq!(summary.total_reviews, 1);
        assert_eq!(summary.passed, 1);
        assert_eq!(summary.failed, 0);
        assert_eq!(summary.cancelled, 0);
        assert_eq!(summary.avg_duration_ms, None);
        assert_eq!(summary.pass_rate, 1.0);
    }
}

// -------------------------------------------------------------------------
// Ledger-backed usage analytics
// -------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct LedgerInvocationRow {
    id: String,
    project_id: Option<String>,
    surface: UsageSurface,
    source_id: String,
    admitted_provider_id: Option<String>,
    admitted_model_id: Option<String>,
    agent_id: Option<String>,
    profile_id: Option<String>,
    agent_name_snapshot: Option<String>,
    project_name_snapshot: Option<String>,
    executor_type: Option<String>,
    lifecycle: UsageInvocationLifecycle,
    telemetry_state: UsageTelemetryState,
    admitted_at: String,
    settled_at: Option<String>,
}

#[derive(Debug, Clone)]
struct LedgerEventRow {
    invocation_id: String,
    project_id: Option<String>,
    provider_id: Option<String>,
    model_id: Option<String>,
    runtime_model: Option<String>,
    agent_id: Option<String>,
    profile_id: Option<String>,
    agent_name_snapshot: Option<String>,
    project_name_snapshot: Option<String>,
    executor_type: Option<String>,
    counters: [Option<i64>; 4],
    provider_reported_nano_usd: Option<i64>,
    legacy_reported_cost_text: Option<String>,
    estimated_nano_usd: Option<i64>,
    cost_kind: UsageCostKind,
    provenance_kind: UsageEventProvenanceKind,
    rate_revision_id: Option<String>,
    catalog_snapshot_id: Option<String>,
    formula_revision: Option<String>,
    retrospective: bool,
    coverage_reason_code: Option<CostCoverageReasonCode>,
    occurred_at: String,
    rate_source_kind: Option<String>,
    rate_effective_at: Option<String>,
    rate_input: Option<i64>,
    rate_output: Option<i64>,
    rate_cache_read: Option<i64>,
    rate_cache_write: Option<i64>,
    catalog_digest: Option<String>,
    catalog_fetched_at: Option<String>,
    catalog_freshness: Option<String>,
}

#[derive(Debug, Clone)]
struct LedgerInvocationView {
    row: LedgerInvocationRow,
    events: Vec<LedgerEventRow>,
}

#[derive(Debug, Clone)]
struct DomainRun {
    surface: UsageSurface,
    source_id: String,
    project_id: Option<String>,
}

/// Immutable proof that an assistant response belongs to a handed-off
/// Product Genesis session.  The source aliases cover the message id and the
/// durable turn/source ids used by the historical and runtime chat writers.
#[derive(Debug, Clone)]
struct GenesisAttribution {
    message_id: String,
    source_id: Option<String>,
    source_message_id: Option<String>,
    source_turn_job_id: Option<String>,
    project_id: String,
    occurred_at: String,
}

/// Immutable Genesis handoff boundary. A control-transfer turn can have no
/// assistant message at all, so message-only attribution cannot count its
/// unmetered invocation or map its NULL-at-admission Project ID. The durable
/// delivery receipt is the occurrence boundary; unlike a turn's mutable
/// `updated_at`, its `created_at` is immutable.
#[derive(Debug, Clone)]
struct GenesisBoundary {
    project_id: String,
    source_turn_job_id: Option<String>,
    source_turn_occurred_at: Option<String>,
    source_message_ids: Vec<String>,
}

#[derive(Debug, Clone, Default)]
struct UsageDataset {
    invocations: Vec<LedgerInvocationView>,
    domain_runs: Vec<DomainRun>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RunKey {
    surface: UsageSurface,
    source_id: String,
}

impl std::hash::Hash for RunKey {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        surface_order(self.surface).hash(state);
        self.source_id.hash(state);
    }
}

impl Ord for RunKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        surface_order(self.surface)
            .cmp(&surface_order(other.surface))
            .then_with(|| self.source_id.cmp(&other.source_id))
    }
}

impl PartialOrd for RunKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct AgentGroupKey {
    agent_id: Option<String>,
    agent_name_snapshot: Option<String>,
    profile_id: Option<String>,
    executor_type: Option<String>,
}

impl Ord for AgentGroupKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.agent_id
            .cmp(&other.agent_id)
            .then_with(|| self.agent_name_snapshot.cmp(&other.agent_name_snapshot))
            .then_with(|| self.profile_id.cmp(&other.profile_id))
            .then_with(|| self.executor_type.cmp(&other.executor_type))
    }
}

impl PartialOrd for AgentGroupKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn surface_order(surface: UsageSurface) -> usize {
    match surface {
        UsageSurface::TaskExecution => 0,
        UsageSurface::ProjectChat => 1,
        UsageSurface::MainChat => 2,
        UsageSurface::GenesisChat => 3,
        UsageSurface::MainInquiry => 4,
    }
}

#[derive(Debug, Clone)]
enum GroupFilter {
    All,
    Surface(UsageSurface),
    Model(Option<String>, Option<String>),
    Agent(AgentGroupKey),
    Project(String),
}

#[derive(Debug, Clone, Default)]
struct AttemptSummary {
    costed: bool,
    metered: bool,
    pending: bool,
    unsettled: bool,
    reasons: Vec<ApiReasonCode>,
}

#[derive(Debug, Clone, Default)]
struct ReasonData {
    run_keys: std::collections::HashSet<RunKey>,
    invocation_ids: std::collections::HashSet<String>,
    tokens: [i64; 4],
}

#[derive(Debug, Clone, Default)]
struct CostAccumulator {
    provider_reported_nano_usd: Option<i128>,
    estimated_nano_usd: Option<i128>,
    tokens: [i64; 4],
    priced_tokens: [i64; 4],
    unpriced_tokens: [i64; 4],
    total_provider_attempts: i64,
    settled_provider_attempts: i64,
    pending_provider_attempts: i64,
    unsettled_provider_attempts: i64,
    metered_provider_attempts: i64,
    unmetered_provider_attempts: i64,
    costed_provider_attempts: i64,
    unpriced_provider_attempts: i64,
    costed_event_count: i64,
    unpriced_event_count: i64,
    sources: BTreeMap<String, CostSourceRef>,
    reasons: BTreeMap<String, ReasonData>,
}

impl CostAccumulator {
    fn add_amount(slot: &mut Option<i128>, amount: i128) -> Result<()> {
        if amount < 0 {
            return Err(DbError::InvalidTransition);
        }
        // `None` represents that no amount-bearing event has been seen yet;
        // using zero only as the additive identity preserves that presence
        // distinction because the slot is set to `Some` immediately.
        let current = (*slot).unwrap_or_default();
        *slot = Some(
            current
                .checked_add(amount)
                .ok_or(DbError::InvalidTransition)?,
        );
        Ok(())
    }

    fn add_counter(slot: &mut i64, value: Option<i64>) -> Result<()> {
        if let Some(value) = value {
            if value < 0 {
                return Err(DbError::InvalidTransition);
            }
            *slot = slot.checked_add(value).ok_or(DbError::InvalidTransition)?;
        }
        Ok(())
    }

    fn add_tokens(&mut self, counters: [Option<i64>; 4]) -> Result<()> {
        for (slot, value) in self.tokens.iter_mut().zip(counters) {
            Self::add_counter(slot, value)?;
        }
        Ok(())
    }

    fn add_reason(
        &mut self,
        code: ApiReasonCode,
        run_key: &RunKey,
        invocation_id: &str,
        tokens: [i64; 4],
    ) -> Result<()> {
        let key = reason_key(code);
        let reason = self.reasons.entry(key).or_default();
        reason.run_keys.insert(run_key.clone());
        reason.invocation_ids.insert(invocation_id.to_owned());
        for (slot, value) in reason.tokens.iter_mut().zip(tokens) {
            *slot = slot.checked_add(value).ok_or(DbError::InvalidTransition)?;
        }
        Ok(())
    }
}

fn reason_key(code: ApiReasonCode) -> String {
    match code {
        ApiReasonCode::Pending => "pending",
        ApiReasonCode::Unsettled => "unsettled",
        ApiReasonCode::Unmetered => "unmetered",
        ApiReasonCode::MissingProvider => "missing_provider",
        ApiReasonCode::MissingModel => "missing_model",
        ApiReasonCode::MissingBinding => "missing_binding",
        ApiReasonCode::MissingRate => "missing_rate",
        ApiReasonCode::UnresolvedTier => "unresolved_tier",
        ApiReasonCode::IdentityMismatch => "identity_mismatch",
        ApiReasonCode::InvalidLegacyUsage => "invalid_legacy_usage",
    }
    .to_owned()
}

fn reason_from_key(value: &str) -> Option<ApiReasonCode> {
    Some(match value {
        "pending" => ApiReasonCode::Pending,
        "unsettled" => ApiReasonCode::Unsettled,
        "unmetered" => ApiReasonCode::Unmetered,
        "missing_provider" => ApiReasonCode::MissingProvider,
        "missing_model" => ApiReasonCode::MissingModel,
        "missing_binding" => ApiReasonCode::MissingBinding,
        "missing_rate" => ApiReasonCode::MissingRate,
        "unresolved_tier" => ApiReasonCode::UnresolvedTier,
        "identity_mismatch" => ApiReasonCode::IdentityMismatch,
        "invalid_legacy_usage" => ApiReasonCode::InvalidLegacyUsage,
        _ => return None,
    })
}

fn api_reason_code(value: CostCoverageReasonCode) -> ApiReasonCode {
    match value {
        CostCoverageReasonCode::Pending => ApiReasonCode::Pending,
        CostCoverageReasonCode::Unsettled => ApiReasonCode::Unsettled,
        CostCoverageReasonCode::Unmetered => ApiReasonCode::Unmetered,
        CostCoverageReasonCode::MissingProvider => ApiReasonCode::MissingProvider,
        CostCoverageReasonCode::MissingModel => ApiReasonCode::MissingModel,
        CostCoverageReasonCode::MissingBinding => ApiReasonCode::MissingBinding,
        CostCoverageReasonCode::MissingRate => ApiReasonCode::MissingRate,
        CostCoverageReasonCode::UnresolvedTier => ApiReasonCode::UnresolvedTier,
        CostCoverageReasonCode::IdentityMismatch => ApiReasonCode::IdentityMismatch,
        CostCoverageReasonCode::InvalidLegacyUsage => ApiReasonCode::InvalidLegacyUsage,
    }
}

fn reason_order(code: ApiReasonCode) -> usize {
    match code {
        ApiReasonCode::Pending => 0,
        ApiReasonCode::Unsettled => 1,
        ApiReasonCode::Unmetered => 2,
        ApiReasonCode::MissingProvider => 3,
        ApiReasonCode::MissingModel => 4,
        ApiReasonCode::MissingBinding => 5,
        ApiReasonCode::MissingRate => 6,
        ApiReasonCode::UnresolvedTier => 7,
        ApiReasonCode::IdentityMismatch => 8,
        ApiReasonCode::InvalidLegacyUsage => 9,
    }
}

fn api_surface(surface: UsageSurface) -> ApiUsageSurface {
    match surface {
        UsageSurface::TaskExecution => ApiUsageSurface::TaskExecution,
        UsageSurface::ProjectChat => ApiUsageSurface::ProjectChat,
        UsageSurface::MainChat => ApiUsageSurface::MainChat,
        UsageSurface::GenesisChat => ApiUsageSurface::GenesisChat,
        UsageSurface::MainInquiry => ApiUsageSurface::MainInquiry,
    }
}

fn api_tokens(counters: [i64; 4]) -> ApiTokenCounters {
    ApiTokenCounters {
        input_tokens: counters[0],
        output_tokens: counters[1],
        cache_read_tokens: counters[2],
        cache_write_tokens: counters[3],
    }
}

fn run_key_for(invocation: &LedgerInvocationView) -> RunKey {
    RunKey {
        surface: invocation.row.surface,
        source_id: invocation.row.source_id.clone(),
    }
}

fn event_counters(event: &LedgerEventRow) -> Result<[i64; 4]> {
    // Aggregate wire counters are numeric totals.  Converting an absent
    // nullable bucket to zero here is only a summation convenience; telemetry
    // evidence and metered/unmetered coverage continue to use the invocation
    // state and never infer evidence from this value.
    let counters = event.counters.map(|value| value.unwrap_or(0));
    if counters.iter().any(|value| *value < 0) {
        return Err(DbError::InvalidTransition);
    }
    Ok(counters)
}

fn invocation_model(invocation: &LedgerInvocationView) -> (Option<String>, Option<String>) {
    invocation
        .events
        .first()
        .map(|event| {
            (
                event
                    .provider_id
                    .clone()
                    .or_else(|| invocation.row.admitted_provider_id.clone()),
                event
                    .model_id
                    .clone()
                    .or_else(|| event.runtime_model.clone())
                    .or_else(|| invocation.row.admitted_model_id.clone()),
            )
        })
        .unwrap_or_else(|| {
            (
                invocation.row.admitted_provider_id.clone(),
                invocation.row.admitted_model_id.clone(),
            )
        })
}

fn invocation_agent(invocation: &LedgerInvocationView) -> AgentGroupKey {
    invocation.events.first().map_or_else(
        || AgentGroupKey {
            agent_id: invocation.row.agent_id.clone(),
            agent_name_snapshot: invocation.row.agent_name_snapshot.clone(),
            profile_id: invocation.row.profile_id.clone(),
            executor_type: invocation.row.executor_type.clone(),
        },
        |event| AgentGroupKey {
            agent_id: event
                .agent_id
                .clone()
                .or_else(|| invocation.row.agent_id.clone()),
            agent_name_snapshot: event
                .agent_name_snapshot
                .clone()
                .or_else(|| invocation.row.agent_name_snapshot.clone()),
            profile_id: event
                .profile_id
                .clone()
                .or_else(|| invocation.row.profile_id.clone()),
            executor_type: event
                .executor_type
                .clone()
                .or_else(|| invocation.row.executor_type.clone()),
        },
    )
}

fn invocation_matches(invocation: &LedgerInvocationView, filter: &GroupFilter) -> bool {
    match filter {
        GroupFilter::All => true,
        GroupFilter::Surface(surface) => invocation.row.surface == *surface,
        GroupFilter::Model(provider, model) => {
            let (invocation_provider, invocation_model) = invocation_model(invocation);
            invocation_provider == *provider && invocation_model == *model
        }
        GroupFilter::Agent(agent) => invocation_agent(invocation) == *agent,
        GroupFilter::Project(project_id) => {
            invocation.row.project_id.as_deref() == Some(project_id)
        }
    }
}

fn parse_money_decimal(input: &str) -> Option<i128> {
    let input = input.trim().trim_matches('\'');
    if input.is_empty() || input.eq_ignore_ascii_case("null") || input.starts_with('-') {
        return None;
    }
    let (mantissa, exponent) = match input.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, exponent.parse::<i32>().ok()?),
        None => (input, 0_i32),
    };
    let (whole, fraction) = mantissa
        .split_once('.')
        .map_or((mantissa, ""), |(w, f)| (w, f));
    if whole.is_empty()
        || !whole.bytes().all(|byte| byte.is_ascii_digit())
        || !fraction.bytes().all(|byte| byte.is_ascii_digit())
    {
        return None;
    }
    let digits = format!("{whole}{fraction}");
    let digits = digits.trim_start_matches('0');
    let digits = if digits.is_empty() { "0" } else { digits };
    let integer = digits.parse::<i128>().ok()?;
    let scale = i32::try_from(fraction.len()).ok()?.checked_sub(exponent)?;
    if scale <= 0 {
        let multiplier = 10_i128.checked_pow(scale.unsigned_abs())?;
        return integer.checked_mul(multiplier)?.checked_mul(1_000_000_000);
    }
    let denominator = 10_i128.checked_pow(u32::try_from(scale).ok()?)?;
    let numerator = integer.checked_mul(1_000_000_000)?;
    let quotient = numerator / denominator;
    let remainder = numerator % denominator;
    // Round half away from zero without multiplying the remainder: the
    // latter can overflow for otherwise valid, very large decimal inputs.
    let round_up =
        remainder > denominator / 2 || (denominator % 2 == 0 && remainder == denominator / 2);
    let rounded = quotient.checked_add(i128::from(round_up))?;
    Some(rounded)
}

fn event_reported_amount(event: &LedgerEventRow) -> Option<i128> {
    event
        .provider_reported_nano_usd
        .map(i128::from)
        .or_else(|| {
            event
                .legacy_reported_cost_text
                .as_deref()
                .and_then(parse_money_decimal)
        })
}

fn event_unpriced_reason(event: &LedgerEventRow) -> ApiReasonCode {
    if let Some(reason) = event.coverage_reason_code.map(api_reason_code) {
        return reason;
    }
    // A legacy REAL provider amount is authoritative, but its original
    // lexical representation may still be unconvertible to fixed-point. Do
    // not mislabel that preserved data as a missing rate.
    if event.cost_kind == UsageCostKind::ProviderReported
        && event_reported_amount(event).is_none()
        && !matches!(
            event.provenance_kind,
            UsageEventProvenanceKind::RuntimeReport
        )
    {
        ApiReasonCode::InvalidLegacyUsage
    } else {
        ApiReasonCode::MissingRate
    }
}

fn event_source(event: &LedgerEventRow) -> Option<(String, CostSourceRef)> {
    if event_reported_amount(event).is_some() {
        let legacy = !matches!(
            event.provenance_kind,
            UsageEventProvenanceKind::RuntimeReport
        );
        let source_kind = if legacy {
            CostSourceKind::LegacyProviderReported
        } else {
            CostSourceKind::ProviderReported
        };
        let source = CostSourceRef {
            source_kind,
            rate_revision_id: None,
            catalog_snapshot_id: None,
            catalog_digest: None,
            effective_at: None,
            fetched_at: None,
            freshness: CostSourceFreshness::NotApplicable,
            retrospective: false,
            formula_revision: None,
        };
        return Some((
            format!("reported:{}", reason_key_for_source(source_kind)),
            source,
        ));
    }
    event.estimated_nano_usd?;
    let source_kind = match event.rate_source_kind.as_deref() {
        Some("manual_override") => CostSourceKind::ManualOverride,
        Some("models_dev_catalog") => CostSourceKind::ModelsDevCatalog,
        _ => return None,
    };
    let source = CostSourceRef {
        source_kind,
        rate_revision_id: event.rate_revision_id.clone(),
        catalog_snapshot_id: event.catalog_snapshot_id.clone(),
        catalog_digest: event.catalog_digest.clone(),
        effective_at: event.rate_effective_at.clone(),
        fetched_at: event.catalog_fetched_at.clone(),
        freshness: if source_kind == CostSourceKind::ModelsDevCatalog {
            match event.catalog_freshness.as_deref() {
                Some("fresh") => CostSourceFreshness::Fresh,
                Some("stale") => CostSourceFreshness::Stale,
                Some("refresh_failed") => CostSourceFreshness::RefreshFailed,
                Some("not_applicable") => CostSourceFreshness::NotApplicable,
                // Historical rows with an absent/unknown freshness must not
                // be presented as fresh provenance.  Preserve the usable
                // estimate while conservatively marking its source as a
                // refresh failure.
                _ => CostSourceFreshness::RefreshFailed,
            }
        } else {
            CostSourceFreshness::NotApplicable
        },
        retrospective: event.retrospective,
        formula_revision: event.formula_revision.clone(),
    };
    let key = format!(
        "estimated:{}:{}:{}",
        reason_key_for_source(source_kind),
        event.rate_revision_id.as_deref().unwrap_or_default(),
        event.catalog_snapshot_id.as_deref().unwrap_or_default()
    );
    Some((key, source))
}

fn reason_key_for_source(source_kind: CostSourceKind) -> &'static str {
    match source_kind {
        CostSourceKind::ProviderReported => "provider_reported",
        CostSourceKind::LegacyProviderReported => "legacy_provider_reported",
        CostSourceKind::ModelsDevCatalog => "models_dev_catalog",
        CostSourceKind::ManualOverride => "manual_override",
    }
}

fn event_accounting(
    event: &LedgerEventRow,
    accumulator: &mut CostAccumulator,
    run_key: &RunKey,
    blocked_reason: Option<ApiReasonCode>,
) -> Result<bool> {
    accumulator.add_tokens(event.counters)?;
    let counters = event_counters(event)?;
    if let Some(reason) = blocked_reason {
        accumulator.unpriced_event_count = accumulator
            .unpriced_event_count
            .checked_add(1)
            .ok_or(DbError::InvalidTransition)?;
        for (slot, value) in accumulator.unpriced_tokens.iter_mut().zip(counters) {
            *slot = slot.checked_add(value).ok_or(DbError::InvalidTransition)?;
        }
        accumulator.add_reason(reason, run_key, &event.invocation_id, counters)?;
        return Ok(false);
    }

    // A provider-reported amount wins for its event.  An estimate attached to
    // the same row (for example by a later retrospective revision) must not be
    // added on top of that reported amount.  Applied retrospective revisions
    // are exposed by the query as an effective `estimated` cost kind.
    let reported = (event.cost_kind == UsageCostKind::ProviderReported)
        .then(|| event_reported_amount(event))
        .flatten();
    let estimated = (event.cost_kind == UsageCostKind::Estimated)
        .then(|| event.estimated_nano_usd.map(i128::from))
        .flatten();
    let costed = reported.is_some() || estimated.is_some();

    if let Some(amount) = reported {
        CostAccumulator::add_amount(&mut accumulator.provider_reported_nano_usd, amount)?;
    } else if let Some(amount) = estimated {
        CostAccumulator::add_amount(&mut accumulator.estimated_nano_usd, amount)?;
    }
    if costed {
        accumulator.costed_event_count = accumulator
            .costed_event_count
            .checked_add(1)
            .ok_or(DbError::InvalidTransition)?;
        for (slot, value) in accumulator.priced_tokens.iter_mut().zip(counters) {
            *slot = slot.checked_add(value).ok_or(DbError::InvalidTransition)?;
        }
        if let Some((key, source)) = event_source(event) {
            accumulator.sources.insert(key, source);
        }
        return Ok(true);
    }

    accumulator.unpriced_event_count = accumulator
        .unpriced_event_count
        .checked_add(1)
        .ok_or(DbError::InvalidTransition)?;
    let reason = event_unpriced_reason(event);
    let rates = [
        event.rate_input,
        event.rate_output,
        event.rate_cache_read,
        event.rate_cache_write,
    ];
    let mut reason_tokens = [0_i64; 4];
    let reason_blocks_all_buckets = matches!(
        reason,
        ApiReasonCode::Unmetered
            | ApiReasonCode::MissingProvider
            | ApiReasonCode::MissingModel
            | ApiReasonCode::MissingBinding
            | ApiReasonCode::IdentityMismatch
            | ApiReasonCode::InvalidLegacyUsage
    );
    for (index, value) in counters.into_iter().enumerate() {
        let bucket_unpriced = reason_blocks_all_buckets
            || reason == ApiReasonCode::UnresolvedTier
            || rates[index].is_none();
        let target = if bucket_unpriced {
            &mut accumulator.unpriced_tokens[index]
        } else {
            &mut accumulator.priced_tokens[index]
        };
        *target = target
            .checked_add(value)
            .ok_or(DbError::InvalidTransition)?;
        if bucket_unpriced {
            reason_tokens[index] = value;
        }
    }
    accumulator.add_reason(reason, run_key, &event.invocation_id, reason_tokens)?;
    Ok(false)
}

fn attempt_summary(
    invocation: &LedgerInvocationView,
    accumulator: &mut CostAccumulator,
    run_key: &RunKey,
) -> Result<AttemptSummary> {
    let pending = !matches!(
        invocation.row.lifecycle,
        UsageInvocationLifecycle::Settled | UsageInvocationLifecycle::Unsettled
    );
    let unsettled = invocation.row.lifecycle == UsageInvocationLifecycle::Unsettled;
    let metered = invocation.row.telemetry_state == UsageTelemetryState::Metered;
    let mut summary = AttemptSummary {
        costed: !invocation.events.is_empty(),
        metered,
        pending,
        unsettled,
        reasons: Vec::new(),
    };
    accumulator.total_provider_attempts = accumulator
        .total_provider_attempts
        .checked_add(1)
        .ok_or(DbError::InvalidTransition)?;
    if pending {
        accumulator.pending_provider_attempts = accumulator
            .pending_provider_attempts
            .checked_add(1)
            .ok_or(DbError::InvalidTransition)?;
        let reason = ApiReasonCode::Pending;
        summary.reasons.push(reason);
        accumulator.add_reason(reason, run_key, &invocation.row.id, [0; 4])?;
    } else if unsettled {
        accumulator.unsettled_provider_attempts = accumulator
            .unsettled_provider_attempts
            .checked_add(1)
            .ok_or(DbError::InvalidTransition)?;
        let reason = ApiReasonCode::Unsettled;
        summary.reasons.push(reason);
        accumulator.add_reason(reason, run_key, &invocation.row.id, [0; 4])?;
    } else {
        accumulator.settled_provider_attempts = accumulator
            .settled_provider_attempts
            .checked_add(1)
            .ok_or(DbError::InvalidTransition)?;
    }
    if metered {
        accumulator.metered_provider_attempts = accumulator
            .metered_provider_attempts
            .checked_add(1)
            .ok_or(DbError::InvalidTransition)?;
    } else if !pending && !unsettled {
        accumulator.unmetered_provider_attempts = accumulator
            .unmetered_provider_attempts
            .checked_add(1)
            .ok_or(DbError::InvalidTransition)?;
        // Unmetered is an independent telemetry dimension.  It must remain
        // visible even when the provider supplied a reportable money amount;
        // the amount can make the attempt costed without proving counters.
        let reason = ApiReasonCode::Unmetered;
        summary.reasons.push(reason);
        accumulator.add_reason(reason, run_key, &invocation.row.id, [0; 4])?;
    }

    if invocation.events.is_empty() {
        summary.costed = false;
        accumulator.unpriced_provider_attempts = accumulator
            .unpriced_provider_attempts
            .checked_add(1)
            .ok_or(DbError::InvalidTransition)?;
        return Ok(summary);
    }

    summary.costed = true;
    let blocked_reason = if pending {
        Some(ApiReasonCode::Pending)
    } else if unsettled {
        Some(ApiReasonCode::Unsettled)
    } else {
        None
    };
    for event in &invocation.events {
        if !event_accounting(event, accumulator, run_key, blocked_reason)? {
            summary.costed = false;
            if blocked_reason.is_none() {
                let reason = event_unpriced_reason(event);
                summary.reasons.push(reason);
            }
        }
    }
    if summary.costed {
        accumulator.costed_provider_attempts = accumulator
            .costed_provider_attempts
            .checked_add(1)
            .ok_or(DbError::InvalidTransition)?;
    } else {
        accumulator.unpriced_provider_attempts = accumulator
            .unpriced_provider_attempts
            .checked_add(1)
            .ok_or(DbError::InvalidTransition)?;
    }
    Ok(summary)
}

fn source_refs(accumulator: &CostAccumulator) -> Vec<CostSourceRef> {
    let mut sources: Vec<_> = accumulator.sources.values().cloned().collect();
    sources.sort_by(|left, right| {
        source_order(left.source_kind)
            .cmp(&source_order(right.source_kind))
            .then_with(|| left.rate_revision_id.cmp(&right.rate_revision_id))
            .then_with(|| left.catalog_snapshot_id.cmp(&right.catalog_snapshot_id))
            .then_with(|| left.effective_at.cmp(&right.effective_at))
    });
    sources
}

fn source_order(source_kind: CostSourceKind) -> usize {
    match source_kind {
        CostSourceKind::ProviderReported => 0,
        CostSourceKind::LegacyProviderReported => 1,
        CostSourceKind::ModelsDevCatalog => 2,
        CostSourceKind::ManualOverride => 3,
    }
}

fn money(nanos: Option<i128>) -> Option<MoneyAmount> {
    let nanos = nanos?;
    if nanos < 0 {
        return None;
    }
    let whole = nanos / 1_000_000_000;
    let fraction = nanos % 1_000_000_000;
    let decimal = if fraction == 0 {
        whole.to_string()
    } else {
        let mut fraction = format!("{fraction:09}");
        while fraction.ends_with('0') {
            fraction.pop();
        }
        format!("{whole}.{fraction}")
    };
    Some(MoneyAmount {
        currency: "USD".to_owned(),
        decimal,
    })
}

fn cost_summary(
    accumulator: CostAccumulator,
    run_states: &BTreeMap<RunKey, Vec<AttemptSummary>>,
) -> Result<CostSummary> {
    let mut pending_runs = 0_i64;
    let mut no_provider_call_runs = 0_i64;
    let mut fully_metered_runs = 0_i64;
    let mut fully_costed_runs = 0_i64;
    let mut partially_costed_runs = 0_i64;
    let mut unavailable_cost_runs = 0_i64;

    let mut reasons = accumulator.reasons.clone();
    for (run_key, attempts) in run_states {
        if attempts.is_empty() {
            no_provider_call_runs = no_provider_call_runs
                .checked_add(1)
                .ok_or(DbError::InvalidTransition)?;
            continue;
        }
        let has_pending = attempts.iter().any(|attempt| attempt.pending);
        let has_unsettled = attempts.iter().any(|attempt| attempt.unsettled);
        let costed = attempts.iter().filter(|attempt| attempt.costed).count();
        let all_metered = attempts.iter().all(|attempt| attempt.metered);
        if has_pending {
            pending_runs = pending_runs
                .checked_add(1)
                .ok_or(DbError::InvalidTransition)?;
        } else if !has_unsettled && all_metered {
            fully_metered_runs = fully_metered_runs
                .checked_add(1)
                .ok_or(DbError::InvalidTransition)?;
        }
        if has_pending {
            // Pending has precedence over every settled status.
        } else if costed == attempts.len() && !has_unsettled {
            fully_costed_runs = fully_costed_runs
                .checked_add(1)
                .ok_or(DbError::InvalidTransition)?;
        } else if costed > 0 {
            partially_costed_runs = partially_costed_runs
                .checked_add(1)
                .ok_or(DbError::InvalidTransition)?;
        } else {
            unavailable_cost_runs = unavailable_cost_runs
                .checked_add(1)
                .ok_or(DbError::InvalidTransition)?;
        }

        for attempt in attempts {
            for reason in &attempt.reasons {
                let reason = reasons.entry(reason_key(*reason)).or_default();
                reason.run_keys.insert(run_key.clone());
            }
        }
    }

    let coverage = if accumulator.total_provider_attempts == 0 {
        CostCoverage::NoUsage
    } else if accumulator.pending_provider_attempts > 0 {
        CostCoverage::Pending
    } else if accumulator.unsettled_provider_attempts > 0 {
        if accumulator.costed_provider_attempts > 0 {
            CostCoverage::Partial
        } else {
            CostCoverage::Unavailable
        }
    } else if accumulator.costed_provider_attempts == accumulator.total_provider_attempts {
        CostCoverage::Complete
    } else if accumulator.costed_provider_attempts > 0 {
        CostCoverage::Partial
    } else {
        CostCoverage::Unavailable
    };

    let kind = if accumulator.provider_reported_nano_usd.is_some()
        && accumulator.estimated_nano_usd.is_some()
    {
        CostKind::Mixed
    } else if accumulator.provider_reported_nano_usd.is_some() {
        CostKind::ProviderReported
    } else if accumulator.estimated_nano_usd.is_some() {
        CostKind::Estimated
    } else if accumulator.total_provider_attempts == 0
        || (coverage == CostCoverage::Pending && accumulator.costed_provider_attempts == 0)
    {
        CostKind::None
    } else {
        CostKind::Unknown
    };

    // `None` means no amount-bearing event exists; `Some(0)` is an explicit
    // known zero and must remain distinguishable from that absence.
    let known_total = match (
        accumulator.provider_reported_nano_usd,
        accumulator.estimated_nano_usd,
    ) {
        (Some(reported), Some(estimated)) => Some(
            reported
                .checked_add(estimated)
                .ok_or(DbError::InvalidTransition)?,
        ),
        (Some(reported), None) => Some(reported),
        (None, Some(estimated)) => Some(estimated),
        (None, None) => None,
    };
    let complete_total = (coverage == CostCoverage::Complete)
        .then_some(known_total)
        .flatten();
    let total_runs = i64::try_from(run_states.len()).map_err(|_| DbError::InvalidTransition)?;
    let mut reason_items: Vec<_> = reasons
        .into_iter()
        .map(|(key, reason)| -> Result<_> {
            let code = reason_from_key(&key).ok_or(DbError::InvalidTransition)?;
            Ok((
                reason_order(code),
                CostCoverageReason {
                    code,
                    run_or_turn_count: i64::try_from(reason.run_keys.len())
                        .map_err(|_| DbError::InvalidTransition)?,
                    provider_attempt_count: i64::try_from(reason.invocation_ids.len())
                        .map_err(|_| DbError::InvalidTransition)?,
                    tokens: api_tokens(reason.tokens),
                },
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    reason_items.sort_by_key(|(order, _)| *order);

    Ok(CostSummary {
        kind,
        coverage,
        provider_reported: money(accumulator.provider_reported_nano_usd),
        estimated: money(accumulator.estimated_nano_usd),
        known_subtotal: (accumulator.provider_reported_nano_usd.is_some()
            || accumulator.estimated_nano_usd.is_some())
        .then(|| money(known_total))
        .flatten(),
        complete_total: money(complete_total),
        usage_coverage: UsageCostCoverage {
            total_runs_or_turns: total_runs,
            pending_runs_or_turns: pending_runs,
            no_provider_call_runs_or_turns: no_provider_call_runs,
            fully_metered_runs_or_turns: fully_metered_runs,
            fully_costed_runs_or_turns: fully_costed_runs,
            partially_costed_runs_or_turns: partially_costed_runs,
            unavailable_cost_runs_or_turns: unavailable_cost_runs,
            total_provider_attempts: accumulator.total_provider_attempts,
            settled_provider_attempts: accumulator.settled_provider_attempts,
            pending_provider_attempts: accumulator.pending_provider_attempts,
            unsettled_provider_attempts: accumulator.unsettled_provider_attempts,
            metered_provider_attempts: accumulator.metered_provider_attempts,
            unmetered_provider_attempts: accumulator.unmetered_provider_attempts,
            costed_provider_attempts: accumulator.costed_provider_attempts,
            unpriced_provider_attempts: accumulator.unpriced_provider_attempts,
            priced_tokens: api_tokens(accumulator.priced_tokens),
            unpriced_tokens: api_tokens(accumulator.unpriced_tokens),
            reasons: reason_items.into_iter().map(|(_, reason)| reason).collect(),
        },
        sources: source_refs(&accumulator),
    })
}

fn group_has_invocation(filter: &GroupFilter, invocation: &LedgerInvocationView) -> bool {
    invocation_matches(invocation, filter)
}

fn aggregate_for(
    invocations: &[LedgerInvocationView],
    domain_runs: &[DomainRun],
    filter: &GroupFilter,
) -> Result<UsageAggregate> {
    let selected: Vec<&LedgerInvocationView> = invocations
        .iter()
        .filter(|invocation| group_has_invocation(filter, invocation))
        .collect();
    let selected_source_ids: std::collections::HashSet<RunKey> = selected
        .iter()
        .map(|invocation| run_key_for(invocation))
        .collect();
    let mut run_states: BTreeMap<RunKey, Vec<AttemptSummary>> = BTreeMap::new();
    let mut accumulator = CostAccumulator::default();
    let mut counts = ActivityCounts {
        task_execution_count: 0,
        chat_turn_count: 0,
        inquiry_count: 0,
        provider_attempt_count: 0,
    };
    let mut tokens = [0_i64; 4];

    for invocation in selected {
        let run_key = run_key_for(invocation);
        let summary = attempt_summary(invocation, &mut accumulator, &run_key)?;
        run_states.entry(run_key).or_default().push(summary);
        counts.provider_attempt_count = counts
            .provider_attempt_count
            .checked_add(1)
            .ok_or(DbError::InvalidTransition)?;
        for event in &invocation.events {
            for (slot, value) in tokens.iter_mut().zip(event_counters(event)?) {
                *slot = slot.checked_add(value).ok_or(DbError::InvalidTransition)?;
            }
        }
    }

    for domain_run in domain_runs {
        let run_key = RunKey {
            surface: domain_run.surface,
            source_id: domain_run.source_id.clone(),
        };
        let filter_matches = match filter {
            GroupFilter::All => true,
            GroupFilter::Surface(surface) => domain_run.surface == *surface,
            GroupFilter::Project(project_id) => {
                domain_run.project_id.as_deref() == Some(project_id)
            }
            GroupFilter::Model(_, _) | GroupFilter::Agent(_) => {
                selected_source_ids.contains(&run_key)
            }
        };
        if filter_matches {
            run_states.entry(run_key).or_default();
        }
    }

    for key in run_states.keys() {
        match key.surface {
            UsageSurface::TaskExecution => {
                counts.task_execution_count = counts
                    .task_execution_count
                    .checked_add(1)
                    .ok_or(DbError::InvalidTransition)?;
            }
            UsageSurface::ProjectChat | UsageSurface::MainChat | UsageSurface::GenesisChat => {
                counts.chat_turn_count = counts
                    .chat_turn_count
                    .checked_add(1)
                    .ok_or(DbError::InvalidTransition)?;
            }
            UsageSurface::MainInquiry => {
                counts.inquiry_count = counts
                    .inquiry_count
                    .checked_add(1)
                    .ok_or(DbError::InvalidTransition)?;
            }
        }
    }
    let cost = cost_summary(accumulator, &run_states)?;
    Ok(UsageAggregate {
        counts,
        tokens: api_tokens(tokens),
        cost,
    })
}

fn usage_analytics(dataset: UsageDataset) -> Result<UsageAnalytics> {
    let all = aggregate_for(
        &dataset.invocations,
        &dataset.domain_runs,
        &GroupFilter::All,
    )?;
    let mut surfaces = Vec::new();
    for surface in [
        UsageSurface::TaskExecution,
        UsageSurface::ProjectChat,
        UsageSurface::MainChat,
        UsageSurface::GenesisChat,
        UsageSurface::MainInquiry,
    ] {
        let aggregate = aggregate_for(
            &dataset.invocations,
            &dataset.domain_runs,
            &GroupFilter::Surface(surface),
        )?;
        if aggregate.counts.task_execution_count > 0
            || aggregate.counts.chat_turn_count > 0
            || aggregate.counts.inquiry_count > 0
            || aggregate.counts.provider_attempt_count > 0
        {
            surfaces.push(SurfaceUsageBreakdown {
                surface: api_surface(surface),
                counts: aggregate.counts,
                tokens: aggregate.tokens,
                cost: aggregate.cost,
            });
        }
    }

    let mut model_keys = BTreeMap::<(Option<String>, Option<String>), ()>::new();
    let mut agent_keys = BTreeMap::<AgentGroupKey, ()>::new();
    for invocation in &dataset.invocations {
        model_keys.insert(invocation_model(invocation), ());
        agent_keys.insert(invocation_agent(invocation), ());
    }
    let mut by_model = Vec::new();
    for ((provider_id, model_id), _) in model_keys {
        let aggregate = aggregate_for(
            &dataset.invocations,
            &dataset.domain_runs,
            &GroupFilter::Model(provider_id.clone(), model_id.clone()),
        )?;
        by_model.push(ModelUsageBreakdown {
            provider_id,
            model_id,
            counts: aggregate.counts,
            tokens: aggregate.tokens,
            cost: aggregate.cost,
        });
    }
    let mut by_agent = Vec::new();
    for agent in agent_keys.into_keys() {
        let aggregate = aggregate_for(
            &dataset.invocations,
            &dataset.domain_runs,
            &GroupFilter::Agent(agent.clone()),
        )?;
        by_agent.push(AgentUsageBreakdown {
            agent_id: agent.agent_id,
            agent_name_snapshot: agent.agent_name_snapshot,
            profile_id: agent.profile_id,
            executor_type: agent.executor_type,
            counts: aggregate.counts,
            tokens: aggregate.tokens,
            cost: aggregate.cost,
        });
    }

    Ok(UsageAnalytics {
        counts: all.counts,
        tokens: all.tokens,
        cost: all.cost,
        by_surface: surfaces,
        by_model,
        by_agent,
    })
}

fn account_usage_analytics(
    dataset: UsageDataset,
    from: Option<&str>,
    to: Option<&str>,
) -> Result<AccountUsageAnalyticsResponse> {
    let token_usage = usage_analytics(dataset.clone())?;
    // Account `by_project` follows immutable Project attribution, not the
    // presence of a usage event. A successful unmetered Genesis handoff has no
    // usage event by contract, but its handoff boundary still proves the
    // Project and must appear exactly once in this grouping. Domain runs are
    // included as well so a Project activity that stopped before any provider
    // admission remains visible in the grouping's no-provider denominator.
    let project_invocations: Vec<LedgerInvocationView> = dataset
        .invocations
        .iter()
        .filter(|invocation| invocation.row.project_id.is_some())
        .cloned()
        .collect();
    let mut projects = BTreeMap::<String, Option<String>>::new();
    for invocation in &project_invocations {
        let Some(project_id) = invocation.row.project_id.as_ref() else {
            continue;
        };
        let name = invocation.row.project_name_snapshot.clone().or_else(|| {
            invocation
                .events
                .iter()
                .find_map(|event| event.project_name_snapshot.clone())
        });
        projects
            .entry(project_id.clone())
            .and_modify(|current| {
                if current.is_none() {
                    *current = name.clone();
                }
            })
            .or_insert(name);
    }
    for run in &dataset.domain_runs {
        let Some(project_id) = run.project_id.as_ref() else {
            continue;
        };
        projects.entry(project_id.clone()).or_insert(None);
    }

    let mut by_project = Vec::with_capacity(projects.len());
    for (project_id, project_name_snapshot) in projects {
        let aggregate = aggregate_for(
            &project_invocations,
            &dataset.domain_runs,
            &GroupFilter::Project(project_id.clone()),
        )?;
        by_project.push(ProjectUsageBreakdown {
            project_id: Some(project_id),
            project_name_snapshot,
            counts: aggregate.counts,
            tokens: aggregate.tokens,
            cost: aggregate.cost,
        });
    }

    Ok(AccountUsageAnalyticsResponse {
        window: AnalyticsWindow {
            from: from.map(str::to_owned),
            to: to.map(str::to_owned),
        },
        token_usage,
        by_project,
    })
}

fn parse_timestamp(value: &str) -> Option<DateTime<chrono::FixedOffset>> {
    DateTime::parse_from_rfc3339(value).ok()
}

fn validate_window(from: Option<&str>, to: Option<&str>) -> Result<()> {
    let from = from
        .map(|value| parse_timestamp(value).ok_or(DbError::InvalidTransition))
        .transpose()?;
    let to = to
        .map(|value| parse_timestamp(value).ok_or(DbError::InvalidTransition))
        .transpose()?;
    if let (Some(from), Some(to)) = (from, to) {
        if from >= to {
            return Err(DbError::InvalidTransition);
        }
    }
    Ok(())
}

/// SQL callers widen each bound by one second before this exact check. SQLite
/// `julianday` is a floating-point prefilter and can otherwise round a
/// sub-millisecond RFC3339 value onto a boundary; the Rust comparison below
/// is the final half-open `[from, to)` authority.
fn timestamp_in_window(value: &str, from: Option<&str>, to: Option<&str>) -> bool {
    let Some(value) = parse_timestamp(value) else {
        // An invalid historical occurrence cannot be assigned to a bounded
        // instant window, but an open-ended report must still expose the
        // preserved legacy row so its `invalid_legacy_usage` coverage is not
        // silently erased by parsing.
        return from.is_none() && to.is_none();
    };
    if let Some(from) = from.and_then(parse_timestamp) {
        if value < from {
            return false;
        }
    }
    if let Some(to) = to.and_then(parse_timestamp) {
        if value >= to {
            return false;
        }
    }
    true
}

fn parse_db_enum<T: FromStr<Err = String>>(value: String) -> Result<T> {
    value.parse().map_err(|_| DbError::InvalidTransition)
}

/// Resolve Genesis-to-Project attribution only through the immutable handoff
/// packet and its delivered receipt.  In particular, do not use a mutable
/// session `updated_at` (or a current Project relation) to classify old Main
/// Chat responses. Source message aliases are read from the immutable handoff
/// packet rather than the session's mutable working list. The query also
/// supplies no-provider domain runs, whose ledger invocation is legitimately
/// absent because no provider call was admitted.
async fn fetch_genesis_attributions(
    db: &SqliteDb,
    owner_user_id: Option<&str>,
    project_id: Option<&str>,
) -> Result<Vec<GenesisAttribution>> {
    let (sql, bind) = if let Some(project_id) = project_id {
        (
            "SELECT m.id AS message_id, m.source_id, m.source_message_id,
                    h.source_turn_job_id, g.project_id, m.created_at,
                    delivery.created_at AS delivery_created_at
             FROM agent_chat_message m
             JOIN agent_chat source_chat ON source_chat.id = m.chat_id
             JOIN product_genesis_session g
               ON g.main_chat_id = source_chat.id
              AND g.lifecycle = 'handed_off'
              AND g.project_id = ?1
              AND g.handoff_id IS NOT NULL
             JOIN project p
               ON p.id = g.project_id
              AND p.owner_id = source_chat.account_id
             JOIN project_admission_receipt receipt
               ON receipt.project_id = g.project_id
              AND receipt.source_kind = 'genesis_handoff'
              AND receipt.handoff_id = g.handoff_id
             JOIN agent_handoff h
               ON h.id = g.handoff_id
              AND h.source_chat_id = source_chat.id
              AND h.status = 'delivered'
             JOIN agent_chat target_chat
               ON target_chat.id = h.target_chat_id
              AND target_chat.kind = 'project'
              AND target_chat.project_id = g.project_id
             JOIN agent_handoff_delivery delivery
               ON delivery.handoff_id = h.id
              AND delivery.delivery_sequence = 1
              AND delivery.status = 'delivered'
              AND delivery.target_message_id IS h.target_message_id
              AND delivery.target_turn_job_id IS h.target_turn_job_id
             WHERE m.author_type = 'agent'
               AND m.status IN ('complete', 'failed', 'cancelled')
               AND m.source_type != 'handoff'
               AND json_valid(h.source_revisions_json)
               AND json_extract(
                       h.source_revisions_json,
                       '$.request.source_revisions_digest'
                   ) = receipt.payload_digest
               -- Keep the SQL prefilter conservative; Rust below compares
               -- parsed instants exactly, avoiding Julian-day precision loss.
               AND julianday(m.created_at) <= julianday(delivery.created_at)
                    + 1.0 / 86400.0
               AND (
                   (
                       h.source_turn_job_id IS NOT NULL
                       AND m.source_id = h.source_turn_job_id
                   )
                   OR EXISTS (
                       SELECT 1
                       FROM agent_chat_turn_job source_turn
                       WHERE source_turn.id = h.source_turn_job_id
                         AND source_turn.chat_id = source_chat.id
                         AND source_turn.response_message_id = m.id
                   )
                   OR EXISTS (
                       SELECT 1
                       FROM json_each(
                           json_extract(h.source_revisions_json, '$.source.message_ids')
                       ) source_message
                       WHERE source_message.value = m.id
                          OR source_message.value = m.source_message_id
                   )
               )
             ORDER BY m.created_at ASC, m.id ASC",
            project_id,
        )
    } else {
        let owner_user_id = owner_user_id.ok_or(DbError::InvalidTransition)?;
        (
            "SELECT m.id AS message_id, m.source_id, m.source_message_id,
                    h.source_turn_job_id, g.project_id, m.created_at,
                    delivery.created_at AS delivery_created_at
             FROM agent_chat_message m
             JOIN agent_chat source_chat ON source_chat.id = m.chat_id
             JOIN product_genesis_session g
               ON g.main_chat_id = source_chat.id
              AND g.lifecycle = 'handed_off'
              AND g.project_id IS NOT NULL
              AND g.handoff_id IS NOT NULL
             JOIN project p
               ON p.id = g.project_id
              AND p.owner_id = source_chat.account_id
             JOIN agent_handoff h
               ON h.id = g.handoff_id
              AND h.source_chat_id = source_chat.id
              AND h.status = 'delivered'
             JOIN project_admission_receipt receipt
               ON receipt.project_id = g.project_id
              AND receipt.source_kind = 'genesis_handoff'
              AND receipt.handoff_id = g.handoff_id
             JOIN agent_chat target_chat
               ON target_chat.id = h.target_chat_id
              AND target_chat.kind = 'project'
              AND target_chat.project_id = g.project_id
             JOIN agent_handoff_delivery delivery
               ON delivery.handoff_id = h.id
              AND delivery.delivery_sequence = 1
              AND delivery.status = 'delivered'
              AND delivery.target_message_id IS h.target_message_id
              AND delivery.target_turn_job_id IS h.target_turn_job_id
             WHERE p.owner_id = ?1
               AND m.author_type = 'agent'
               AND m.status IN ('complete', 'failed', 'cancelled')
               AND m.source_type != 'handoff'
               AND json_valid(h.source_revisions_json)
               AND json_extract(
                       h.source_revisions_json,
                       '$.request.source_revisions_digest'
                   ) = receipt.payload_digest
               -- Keep the SQL prefilter conservative; Rust below compares
               -- parsed instants exactly, avoiding Julian-day precision loss.
               AND julianday(m.created_at) <= julianday(delivery.created_at)
                    + 1.0 / 86400.0
               AND (
                   (
                       h.source_turn_job_id IS NOT NULL
                       AND m.source_id = h.source_turn_job_id
                   )
                   OR EXISTS (
                       SELECT 1
                       FROM agent_chat_turn_job source_turn
                       WHERE source_turn.id = h.source_turn_job_id
                         AND source_turn.chat_id = source_chat.id
                         AND source_turn.response_message_id = m.id
                   )
                   OR EXISTS (
                       SELECT 1
                       FROM json_each(
                           json_extract(h.source_revisions_json, '$.source.message_ids')
                       ) source_message
                       WHERE source_message.value = m.id
                          OR source_message.value = m.source_message_id
                   )
               )
             ORDER BY m.created_at ASC, m.id ASC",
            owner_user_id,
        )
    };
    let rows = sqlx::query(sql).bind(bind).fetch_all(db.pool()).await?;
    rows.into_iter()
        .map(|row| -> Result<Option<GenesisAttribution>> {
            let occurred_at: String = row.try_get("created_at")?;
            let delivery_created_at: String = row.try_get("delivery_created_at")?;
            let Some(message_at) = parse_timestamp(&occurred_at) else {
                return Ok(None);
            };
            let Some(delivery_at) = parse_timestamp(&delivery_created_at) else {
                return Ok(None);
            };
            if message_at > delivery_at {
                return Ok(None);
            }
            Ok(Some(GenesisAttribution {
                message_id: row.try_get("message_id")?,
                source_id: row.try_get("source_id")?,
                source_message_id: row.try_get("source_message_id")?,
                source_turn_job_id: row.try_get("source_turn_job_id")?,
                project_id: row.try_get("project_id")?,
                occurred_at,
            }))
        })
        .collect::<Result<Vec<_>>>()
        .map(|rows| rows.into_iter().flatten().collect())
}

/// Load immutable handoff boundaries even when the source turn has no
/// assistant message. Genesis control transfer intentionally completes the
/// source turn without writing a response, so an analytics join rooted only
/// at `agent_chat_message` would lose both its Project attribution and its
/// domain-run count. Source message references come from the immutable
/// handoff packet, not the mutable Genesis session working list.
async fn fetch_genesis_boundaries(
    db: &SqliteDb,
    owner_user_id: Option<&str>,
    project_id: Option<&str>,
) -> Result<Vec<GenesisBoundary>> {
    let (sql, bind) = if let Some(project_id) = project_id {
        (
            "SELECT g.project_id, h.source_turn_job_id,
                    delivery.created_at AS source_turn_occurred_at,
                    json_extract(h.source_revisions_json, '$.source.message_ids')
                        AS source_message_ids_json
             FROM product_genesis_session g
             JOIN agent_chat source_chat
               ON source_chat.id = g.main_chat_id
              AND source_chat.kind = 'account_main'
             JOIN project p
               ON p.id = g.project_id
              AND p.owner_id = source_chat.account_id
             JOIN project_admission_receipt receipt
               ON receipt.project_id = g.project_id
              AND receipt.source_kind = 'genesis_handoff'
              AND receipt.handoff_id = g.handoff_id
             JOIN agent_handoff h
               ON h.id = g.handoff_id
              AND h.source_chat_id = source_chat.id
              AND h.status = 'delivered'
             JOIN agent_chat target_chat
               ON target_chat.id = h.target_chat_id
              AND target_chat.kind = 'project'
              AND target_chat.project_id = g.project_id
             JOIN agent_handoff_delivery delivery
               ON delivery.handoff_id = h.id
              AND delivery.delivery_sequence = 1
              AND delivery.status = 'delivered'
              AND delivery.target_message_id IS h.target_message_id
              AND delivery.target_turn_job_id IS h.target_turn_job_id
             WHERE g.lifecycle = 'handed_off'
               AND g.project_id = ?1
               AND json_valid(h.source_revisions_json)
               AND json_extract(
                       h.source_revisions_json,
                       '$.request.source_revisions_digest'
                   ) = receipt.payload_digest
             ORDER BY delivery.created_at ASC, g.id ASC",
            project_id,
        )
    } else {
        let owner_user_id = owner_user_id.ok_or(DbError::InvalidTransition)?;
        (
            "SELECT g.project_id, h.source_turn_job_id,
                    delivery.created_at AS source_turn_occurred_at,
                    json_extract(h.source_revisions_json, '$.source.message_ids')
                        AS source_message_ids_json
             FROM product_genesis_session g
             JOIN agent_chat source_chat
               ON source_chat.id = g.main_chat_id
              AND source_chat.kind = 'account_main'
             JOIN project p
               ON p.id = g.project_id
              AND p.owner_id = source_chat.account_id
             JOIN agent_handoff h
               ON h.id = g.handoff_id
              AND h.source_chat_id = source_chat.id
              AND h.status = 'delivered'
             JOIN project_admission_receipt receipt
               ON receipt.project_id = g.project_id
              AND receipt.source_kind = 'genesis_handoff'
              AND receipt.handoff_id = g.handoff_id
             JOIN agent_chat target_chat
               ON target_chat.id = h.target_chat_id
              AND target_chat.kind = 'project'
              AND target_chat.project_id = g.project_id
             JOIN agent_handoff_delivery delivery
               ON delivery.handoff_id = h.id
              AND delivery.delivery_sequence = 1
              AND delivery.status = 'delivered'
              AND delivery.target_message_id IS h.target_message_id
              AND delivery.target_turn_job_id IS h.target_turn_job_id
             WHERE g.lifecycle = 'handed_off'
               AND p.owner_id = ?1
               AND json_valid(h.source_revisions_json)
               AND json_extract(
                       h.source_revisions_json,
                       '$.request.source_revisions_digest'
                   ) = receipt.payload_digest
             ORDER BY delivery.created_at ASC, g.id ASC",
            owner_user_id,
        )
    };
    let rows = sqlx::query(sql).bind(bind).fetch_all(db.pool()).await?;
    rows.into_iter()
        .map(|row| {
            let source_message_ids_json: Option<String> = row.try_get("source_message_ids_json")?;
            let source_message_ids = source_message_ids_json
                .map(|json| serde_json::from_str::<Vec<String>>(&json))
                .transpose()
                .map_err(|_| DbError::InvalidTransition)?
                .unwrap_or_default();
            Ok(GenesisBoundary {
                project_id: row.try_get("project_id")?,
                source_turn_job_id: row.try_get("source_turn_job_id")?,
                source_turn_occurred_at: row.try_get("source_turn_occurred_at")?,
                source_message_ids,
            })
        })
        .collect()
}

async fn fetch_ledger_invocations(
    db: &SqliteDb,
    owner_user_id: Option<&str>,
    project_id: Option<&str>,
    from: Option<&str>,
    to: Option<&str>,
    genesis_source_ids_json: Option<&str>,
) -> Result<Vec<LedgerInvocationRow>> {
    let (sql, bind) = if let Some(project_id) = project_id {
        (
            "SELECT id, owner_user_id, project_id, surface,
                    inv.source_id AS source_id,
                    candidate_key, attempt_ordinal, admitted_provider_id,
                    admitted_model_id, agent_id, profile_id,
                    agent_name_snapshot, project_name_snapshot, executor_type,
                    lifecycle, telemetry_state, admitted_at, settled_at
             FROM usage_invocation inv
             WHERE inv.surface IN ('task_execution', 'project_chat', 'genesis_chat')
               AND (inv.project_id = ?1
                    OR (inv.surface = 'genesis_chat'
                        AND inv.project_id IS NULL
                        AND inv.owner_user_id = (
                            SELECT owner_id FROM project WHERE id = ?1
                        )
                        AND EXISTS (
                            SELECT 1
                            FROM json_each(?4) genesis_source
                            WHERE genesis_source.value = inv.source_id
                        )))
               AND (
                    EXISTS (
                        SELECT 1 FROM usage_event e
                        WHERE e.invocation_id = inv.id
                          AND (?2 IS NULL OR julianday(e.occurred_at) >= julianday(?2) - 1.0 / 86400.0)
                          AND (?3 IS NULL OR julianday(e.occurred_at) < julianday(?3) + 1.0 / 86400.0)
                    )
                    OR (
                        NOT EXISTS (
                            SELECT 1 FROM usage_event e
                            WHERE e.invocation_id = inv.id
                        )
                        AND (?2 IS NULL OR julianday(COALESCE(inv.settled_at, inv.admitted_at)) >= julianday(?2) - 1.0 / 86400.0)
                        AND (?3 IS NULL OR julianday(COALESCE(inv.settled_at, inv.admitted_at)) < julianday(?3) + 1.0 / 86400.0)
                    )
               )
             ORDER BY inv.id ASC",
            project_id,
        )
    } else {
        let owner_user_id = owner_user_id.ok_or(DbError::InvalidTransition)?;
        (
            "SELECT id, owner_user_id, project_id, surface,
                    inv.source_id AS source_id,
                    candidate_key, attempt_ordinal, admitted_provider_id,
                    admitted_model_id, agent_id, profile_id,
                    agent_name_snapshot, project_name_snapshot, executor_type,
                    lifecycle, telemetry_state, admitted_at, settled_at
             FROM usage_invocation inv
             WHERE inv.owner_user_id = ?
               AND (
                    EXISTS (
                        SELECT 1 FROM usage_event e
                        WHERE e.invocation_id = inv.id
                          AND (?2 IS NULL OR julianday(e.occurred_at) >= julianday(?2) - 1.0 / 86400.0)
                          AND (?3 IS NULL OR julianday(e.occurred_at) < julianday(?3) + 1.0 / 86400.0)
                    )
                    OR (
                        NOT EXISTS (
                            SELECT 1 FROM usage_event e
                            WHERE e.invocation_id = inv.id
                        )
                        AND (?2 IS NULL OR julianday(COALESCE(inv.settled_at, inv.admitted_at)) >= julianday(?2) - 1.0 / 86400.0)
                        AND (?3 IS NULL OR julianday(COALESCE(inv.settled_at, inv.admitted_at)) < julianday(?3) + 1.0 / 86400.0)
                    )
               )
             ORDER BY inv.id ASC",
            owner_user_id,
        )
    };
    let rows = if project_id.is_some() {
        sqlx::query(sql)
            .bind(bind)
            .bind(from)
            .bind(to)
            .bind(genesis_source_ids_json.unwrap_or("[]"))
            .fetch_all(db.pool())
            .await?
    } else {
        sqlx::query(sql)
            .bind(bind)
            .bind(from)
            .bind(to)
            .fetch_all(db.pool())
            .await?
    };
    let mut invocations: Vec<LedgerInvocationRow> = rows
        .into_iter()
        .map(|row| {
            Ok(LedgerInvocationRow {
                id: row.try_get("id")?,
                project_id: row.try_get("project_id")?,
                surface: parse_db_enum(row.try_get("surface")?)?,
                source_id: row.try_get("source_id")?,
                admitted_provider_id: row.try_get("admitted_provider_id")?,
                admitted_model_id: row.try_get("admitted_model_id")?,
                agent_id: row.try_get("agent_id")?,
                profile_id: row.try_get("profile_id")?,
                agent_name_snapshot: row.try_get("agent_name_snapshot")?,
                project_name_snapshot: row.try_get("project_name_snapshot")?,
                executor_type: row.try_get("executor_type")?,
                lifecycle: parse_db_enum(row.try_get("lifecycle")?)?,
                telemetry_state: parse_db_enum(row.try_get("telemetry_state")?)?,
                admitted_at: row.try_get("admitted_at")?,
                settled_at: row.try_get("settled_at")?,
            })
        })
        .collect::<Result<_>>()?;

    // Chat producers historically used either the response message id or its
    // durable turn/source id as the ledger source.  Resolve those aliases in
    // one bounded query so a logical turn is counted once, while avoiding a
    // mutable chat/session join in the accounting projection.
    let source_ids: BTreeSet<String> = invocations
        .iter()
        .filter(|invocation| {
            matches!(
                invocation.surface,
                UsageSurface::ProjectChat | UsageSurface::MainChat | UsageSurface::GenesisChat
            )
        })
        .map(|invocation| invocation.source_id.clone())
        .collect();
    if !source_ids.is_empty() {
        let source_ids_json =
            serde_json::to_string(&source_ids).map_err(|_| DbError::InvalidTransition)?;
        let aliases = sqlx::query(
            "SELECT source.value AS raw_source_id, m.id AS message_id
             FROM json_each(?1) source
             JOIN agent_chat_message m
               ON m.author_type = 'agent'
              AND (m.id = source.value OR m.source_id = source.value)
             ORDER BY source.value ASC,
                      CASE WHEN m.id = source.value THEN 0 ELSE 1 END,
                      m.created_at DESC, m.id ASC",
        )
        .bind(source_ids_json)
        .fetch_all(db.pool())
        .await?;
        let mut canonical_ids = HashMap::<String, String>::new();
        for alias in aliases {
            let raw_source_id: String = alias.try_get("raw_source_id")?;
            canonical_ids
                .entry(raw_source_id)
                .or_insert(alias.try_get("message_id")?);
        }
        for invocation in &mut invocations {
            if let Some(message_id) = canonical_ids.get(&invocation.source_id) {
                invocation.source_id.clone_from(message_id);
            }
        }
    }
    Ok(invocations)
}

async fn fetch_ledger_events(
    db: &SqliteDb,
    owner_user_id: Option<&str>,
    project_id: Option<&str>,
    from: Option<&str>,
    to: Option<&str>,
    genesis_source_ids_json: Option<&str>,
) -> Result<Vec<LedgerEventRow>> {
    let (sql, bind) = if let Some(project_id) = project_id {
        (
            "SELECT e.id, e.invocation_id, e.project_id, e.surface, e.source_id,
                    e.provider_id, e.model_id, e.runtime_model, e.candidate_key,
                    e.attempt_ordinal, e.agent_id, e.profile_id,
                    e.agent_name_snapshot, e.project_name_snapshot,
                    e.executor_type, e.telemetry_state, e.input_tokens,
                    e.output_tokens, e.cache_read_tokens, e.cache_write_tokens,
                    e.context_tokens, e.selected_tier,
                    e.provider_reported_nano_usd,
                    e.legacy_reported_cost_usd,
                    e.legacy_cost_usd_raw,
                    CAST(e.legacy_reported_cost_usd AS TEXT) AS legacy_reported_cost_text,
                    COALESCE(er.estimated_nano_usd, e.estimated_nano_usd) AS estimated_nano_usd,
                    CASE WHEN er.id IS NOT NULL THEN 'estimated' ELSE e.cost_kind END AS cost_kind,
                    e.provenance_kind,
                    COALESCE(er.rate_revision_id, e.rate_revision_id) AS rate_revision_id,
                    COALESCE(er.catalog_snapshot_id, e.catalog_snapshot_id) AS catalog_snapshot_id,
                    COALESCE(er.formula_revision, e.formula_revision) AS formula_revision,
                    CASE WHEN er.id IS NOT NULL THEN 1 ELSE e.retrospective END AS retrospective,
                    e.coverage_reason_code, e.occurred_at,
                    r.source_kind AS rate_source_kind, r.effective_at AS rate_effective_at,
                    r.input_nano_usd_per_million AS rate_input,
                    r.output_nano_usd_per_million AS rate_output,
                    r.cache_read_nano_usd_per_million AS rate_cache_read,
                    r.cache_write_nano_usd_per_million AS rate_cache_write,
                    c.revision_digest AS catalog_digest,
                    c.fetched_at AS catalog_fetched_at,
                    CASE WHEN er.id IS NOT NULL
                         THEN ep.catalog_freshness
                         ELSE ps.catalog_freshness
                    END AS catalog_freshness
             FROM usage_event e
             JOIN usage_invocation i ON i.id = e.invocation_id
            LEFT JOIN cost_estimate_revision er
               ON er.usage_event_id = e.id
              AND e.provider_reported_nano_usd IS NULL
              AND e.legacy_reported_cost_usd IS NULL
              AND er.state = 'applied'
              AND er.revision = (
                  SELECT MAX(latest.revision)
                  FROM cost_estimate_revision latest
                  WHERE latest.usage_event_id = e.id
                    AND latest.state = 'applied'
              )
             LEFT JOIN cost_estimation_run erun ON erun.id = er.run_id
             LEFT JOIN cost_estimation_preview ep ON ep.id = erun.preview_id
             LEFT JOIN pricing_selection ps ON ps.id = i.pricing_selection_id
             LEFT JOIN pricing_rate_revision r
               ON r.id = COALESCE(er.rate_revision_id, e.rate_revision_id)
             LEFT JOIN pricing_catalog_snapshot c
               ON c.id = COALESCE(er.catalog_snapshot_id, e.catalog_snapshot_id, r.catalog_snapshot_id)
             WHERE e.surface IN ('task_execution', 'project_chat', 'genesis_chat')
               AND (e.project_id = ?1
                OR (e.surface = 'genesis_chat'
                    AND e.project_id IS NULL
                    AND i.project_id IS NULL
                    AND i.owner_user_id = (
                        SELECT owner_id FROM project WHERE id = ?1
                    )
                    AND EXISTS (
                        SELECT 1
                        FROM json_each(?4) genesis_source
                        WHERE genesis_source.value = i.source_id
                    )))
               AND (?2 IS NULL OR julianday(e.occurred_at) >= julianday(?2) - 1.0 / 86400.0)
               AND (?3 IS NULL OR julianday(e.occurred_at) < julianday(?3) + 1.0 / 86400.0)
             ORDER BY e.invocation_id ASC, e.occurred_at ASC, e.id ASC",
            project_id,
        )
    } else {
        let owner_user_id = owner_user_id.ok_or(DbError::InvalidTransition)?;
        (
            "SELECT e.id, e.invocation_id, e.project_id, e.surface, e.source_id,
                    e.provider_id, e.model_id, e.runtime_model, e.candidate_key,
                    e.attempt_ordinal, e.agent_id, e.profile_id,
                    e.agent_name_snapshot, e.project_name_snapshot,
                    e.executor_type, e.telemetry_state, e.input_tokens,
                    e.output_tokens, e.cache_read_tokens, e.cache_write_tokens,
                    e.context_tokens, e.selected_tier,
                    e.provider_reported_nano_usd,
                    e.legacy_reported_cost_usd,
                    e.legacy_cost_usd_raw,
                    CAST(e.legacy_reported_cost_usd AS TEXT) AS legacy_reported_cost_text,
                    COALESCE(er.estimated_nano_usd, e.estimated_nano_usd) AS estimated_nano_usd,
                    CASE WHEN er.id IS NOT NULL THEN 'estimated' ELSE e.cost_kind END AS cost_kind,
                    e.provenance_kind,
                    COALESCE(er.rate_revision_id, e.rate_revision_id) AS rate_revision_id,
                    COALESCE(er.catalog_snapshot_id, e.catalog_snapshot_id) AS catalog_snapshot_id,
                    COALESCE(er.formula_revision, e.formula_revision) AS formula_revision,
                    CASE WHEN er.id IS NOT NULL THEN 1 ELSE e.retrospective END AS retrospective,
                    e.coverage_reason_code, e.occurred_at,
                    r.source_kind AS rate_source_kind, r.effective_at AS rate_effective_at,
                    r.input_nano_usd_per_million AS rate_input,
                    r.output_nano_usd_per_million AS rate_output,
                    r.cache_read_nano_usd_per_million AS rate_cache_read,
                    r.cache_write_nano_usd_per_million AS rate_cache_write,
                    c.revision_digest AS catalog_digest,
                    c.fetched_at AS catalog_fetched_at,
                    CASE WHEN er.id IS NOT NULL
                         THEN ep.catalog_freshness
                         ELSE ps.catalog_freshness
                    END AS catalog_freshness
             FROM usage_event e
             JOIN usage_invocation i ON i.id = e.invocation_id
            LEFT JOIN cost_estimate_revision er
               ON er.usage_event_id = e.id
              AND e.provider_reported_nano_usd IS NULL
              AND e.legacy_reported_cost_usd IS NULL
              AND er.state = 'applied'
              AND er.revision = (
                  SELECT MAX(latest.revision)
                  FROM cost_estimate_revision latest
                  WHERE latest.usage_event_id = e.id
                    AND latest.state = 'applied'
              )
             LEFT JOIN cost_estimation_run erun ON erun.id = er.run_id
             LEFT JOIN cost_estimation_preview ep ON ep.id = erun.preview_id
             LEFT JOIN pricing_selection ps ON ps.id = i.pricing_selection_id
             LEFT JOIN pricing_rate_revision r
               ON r.id = COALESCE(er.rate_revision_id, e.rate_revision_id)
             LEFT JOIN pricing_catalog_snapshot c
               ON c.id = COALESCE(er.catalog_snapshot_id, e.catalog_snapshot_id, r.catalog_snapshot_id)
             WHERE i.owner_user_id = ?
               AND (?2 IS NULL OR julianday(e.occurred_at) >= julianday(?2) - 1.0 / 86400.0)
               AND (?3 IS NULL OR julianday(e.occurred_at) < julianday(?3) + 1.0 / 86400.0)
             ORDER BY e.invocation_id ASC, e.occurred_at ASC, e.id ASC",
            owner_user_id,
        )
    };
    let rows = if project_id.is_some() {
        sqlx::query(sql)
            .bind(bind)
            .bind(from)
            .bind(to)
            .bind(genesis_source_ids_json.unwrap_or("[]"))
            .fetch_all(db.pool())
            .await?
    } else {
        sqlx::query(sql)
            .bind(bind)
            .bind(from)
            .bind(to)
            .fetch_all(db.pool())
            .await?
    };
    rows.into_iter()
        .map(|row| {
            let legacy_reported_cost_text: Option<String> =
                row.try_get("legacy_reported_cost_text")?;
            let legacy_cost_usd_raw: Option<String> = row.try_get("legacy_cost_usd_raw")?;
            Ok(LedgerEventRow {
                invocation_id: row.try_get("invocation_id")?,
                project_id: row.try_get("project_id")?,
                provider_id: row.try_get("provider_id")?,
                model_id: row.try_get("model_id")?,
                runtime_model: row.try_get("runtime_model")?,
                agent_id: row.try_get("agent_id")?,
                profile_id: row.try_get("profile_id")?,
                agent_name_snapshot: row.try_get("agent_name_snapshot")?,
                project_name_snapshot: row.try_get("project_name_snapshot")?,
                executor_type: row.try_get("executor_type")?,
                counters: [
                    row.try_get("input_tokens")?,
                    row.try_get("output_tokens")?,
                    row.try_get("cache_read_tokens")?,
                    row.try_get("cache_write_tokens")?,
                ],
                provider_reported_nano_usd: row.try_get("provider_reported_nano_usd")?,
                legacy_reported_cost_text: legacy_cost_usd_raw.or(legacy_reported_cost_text),
                estimated_nano_usd: row.try_get("estimated_nano_usd")?,
                cost_kind: parse_db_enum(row.try_get("cost_kind")?)?,
                provenance_kind: parse_db_enum(row.try_get("provenance_kind")?)?,
                rate_revision_id: row.try_get("rate_revision_id")?,
                catalog_snapshot_id: row.try_get("catalog_snapshot_id")?,
                formula_revision: row.try_get("formula_revision")?,
                retrospective: row.try_get::<i64, _>("retrospective")? != 0,
                coverage_reason_code: row
                    .try_get::<Option<String>, _>("coverage_reason_code")?
                    .map(parse_db_enum)
                    .transpose()?,
                occurred_at: row.try_get("occurred_at")?,
                rate_source_kind: row.try_get("rate_source_kind")?,
                rate_effective_at: row.try_get("rate_effective_at")?,
                rate_input: row.try_get("rate_input")?,
                rate_output: row.try_get("rate_output")?,
                rate_cache_read: row.try_get("rate_cache_read")?,
                rate_cache_write: row.try_get("rate_cache_write")?,
                catalog_digest: row.try_get("catalog_digest")?,
                catalog_fetched_at: row.try_get("catalog_fetched_at")?,
                catalog_freshness: row.try_get("catalog_freshness")?,
            })
        })
        .collect()
}

async fn fetch_domain_runs(
    db: &SqliteDb,
    owner_user_id: Option<&str>,
    project_id: Option<&str>,
    from: Option<&str>,
    to: Option<&str>,
) -> Result<Vec<DomainRun>> {
    let mut runs = Vec::new();
    if let Some(project_id) = project_id {
        let rows = sqlx::query(
            "SELECT e.id, COALESCE(e.stopped_at, e.created_at) AS occurred_at, t.project_id
             FROM execution e JOIN task t ON t.id = e.task_id
             WHERE t.project_id = ?
               AND e.status IN ('completed', 'failed', 'cancelled')
               AND (?2 IS NULL OR julianday(COALESCE(e.stopped_at, e.created_at)) >= julianday(?2) - 1.0 / 86400.0)
               AND (?3 IS NULL OR julianday(COALESCE(e.stopped_at, e.created_at)) < julianday(?3) + 1.0 / 86400.0)",
        )
        .bind(project_id)
        .bind(from)
        .bind(to)
        .fetch_all(db.pool())
        .await?;
        for row in rows {
            let occurred_at: String = row.try_get("occurred_at")?;
            if timestamp_in_window(&occurred_at, from, to) {
                runs.push(DomainRun {
                    surface: UsageSurface::TaskExecution,
                    source_id: row.try_get("id")?,
                    project_id: Some(row.try_get("project_id")?),
                });
            }
        }
        let rows = sqlx::query(
            "SELECT m.id, m.created_at, c.project_id
             FROM agent_chat_message m JOIN agent_chat c ON c.id = m.chat_id
             WHERE c.kind = 'project' AND c.project_id = ?
               AND m.author_type = 'agent'
               AND m.status IN ('complete', 'failed', 'cancelled')
               AND m.source_type != 'handoff'
               AND (?2 IS NULL OR julianday(m.created_at) >= julianday(?2) - 1.0 / 86400.0)
               AND (?3 IS NULL OR julianday(m.created_at) < julianday(?3) + 1.0 / 86400.0)",
        )
        .bind(project_id)
        .bind(from)
        .bind(to)
        .fetch_all(db.pool())
        .await?;
        for row in rows {
            let occurred_at: String = row.try_get("created_at")?;
            if timestamp_in_window(&occurred_at, from, to) {
                runs.push(DomainRun {
                    surface: UsageSurface::ProjectChat,
                    source_id: row.try_get("id")?,
                    project_id: Some(row.try_get("project_id")?),
                });
            }
        }
        return Ok(runs);
    }

    let owner_user_id = owner_user_id.ok_or(DbError::InvalidTransition)?;
    let rows = sqlx::query(
        "SELECT e.id, COALESCE(e.stopped_at, e.created_at) AS occurred_at, t.project_id
         FROM execution e JOIN task t ON t.id = e.task_id
         JOIN project p ON p.id = t.project_id
         WHERE p.owner_id = ?
           AND e.status IN ('completed', 'failed', 'cancelled')
           AND (?2 IS NULL OR julianday(COALESCE(e.stopped_at, e.created_at)) >= julianday(?2) - 1.0 / 86400.0)
           AND (?3 IS NULL OR julianday(COALESCE(e.stopped_at, e.created_at)) < julianday(?3) + 1.0 / 86400.0)",
    )
    .bind(owner_user_id)
    .bind(from)
    .bind(to)
    .fetch_all(db.pool())
    .await?;
    for row in rows {
        let occurred_at: String = row.try_get("occurred_at")?;
        if timestamp_in_window(&occurred_at, from, to) {
            runs.push(DomainRun {
                surface: UsageSurface::TaskExecution,
                source_id: row.try_get("id")?,
                project_id: Some(row.try_get("project_id")?),
            });
        }
    }
    let rows = sqlx::query(
        "SELECT m.id, m.created_at, c.project_id
         FROM agent_chat_message m JOIN agent_chat c ON c.id = m.chat_id
         JOIN project p ON p.id = c.project_id
         WHERE c.kind = 'project' AND p.owner_id = ?
           AND m.author_type = 'agent'
           AND m.status IN ('complete', 'failed', 'cancelled')
           AND m.source_type != 'handoff'
           AND (?2 IS NULL OR julianday(m.created_at) >= julianday(?2) - 1.0 / 86400.0)
           AND (?3 IS NULL OR julianday(m.created_at) < julianday(?3) + 1.0 / 86400.0)",
    )
    .bind(owner_user_id)
    .bind(from)
    .bind(to)
    .fetch_all(db.pool())
    .await?;
    for row in rows {
        let occurred_at: String = row.try_get("created_at")?;
        if timestamp_in_window(&occurred_at, from, to) {
            runs.push(DomainRun {
                surface: UsageSurface::ProjectChat,
                source_id: row.try_get("id")?,
                project_id: Some(row.try_get("project_id")?),
            });
        }
    }
    let rows = sqlx::query(
        "SELECT m.id, m.created_at
         FROM agent_chat_message m JOIN agent_chat c ON c.id = m.chat_id
         WHERE c.kind = 'account_main' AND c.account_id = ?
           AND m.author_type = 'agent'
           AND m.status IN ('complete', 'failed', 'cancelled')
           AND m.source_type != 'handoff'
           AND (?2 IS NULL OR julianday(m.created_at) >= julianday(?2) - 1.0 / 86400.0)
           AND (?3 IS NULL OR julianday(m.created_at) < julianday(?3) + 1.0 / 86400.0)",
    )
    .bind(owner_user_id)
    .bind(from)
    .bind(to)
    .fetch_all(db.pool())
    .await?;
    for row in rows {
        let occurred_at: String = row.try_get("created_at")?;
        if timestamp_in_window(&occurred_at, from, to) {
            runs.push(DomainRun {
                surface: UsageSurface::MainChat,
                source_id: row.try_get("id")?,
                project_id: None,
            });
        }
    }
    let rows = sqlx::query(
        "SELECT id, finished_at
         FROM agent_inquiry
         WHERE owner_user_id = ?
           AND status IN ('succeeded', 'failed', 'cancelled')
           AND finished_at IS NOT NULL
           AND (?2 IS NULL OR julianday(finished_at) >= julianday(?2) - 1.0 / 86400.0)
           AND (?3 IS NULL OR julianday(finished_at) < julianday(?3) + 1.0 / 86400.0)",
    )
    .bind(owner_user_id)
    .bind(from)
    .bind(to)
    .fetch_all(db.pool())
    .await?;
    for row in rows {
        let occurred_at: String = row.try_get("finished_at")?;
        if timestamp_in_window(&occurred_at, from, to) {
            runs.push(DomainRun {
                surface: UsageSurface::MainInquiry,
                source_id: row.try_get("id")?,
                project_id: None,
            });
        }
    }
    Ok(runs)
}

async fn fetch_usage_dataset(
    db: &SqliteDb,
    owner_user_id: Option<&str>,
    project_id: Option<&str>,
    from: Option<&str>,
    to: Option<&str>,
) -> Result<UsageDataset> {
    validate_window(from, to)?;
    let genesis_attributions = fetch_genesis_attributions(db, owner_user_id, project_id).await?;
    let genesis_boundaries = fetch_genesis_boundaries(db, owner_user_id, project_id).await?;
    let mut genesis_sources = HashMap::<String, Option<String>>::new();
    for attribution in &genesis_attributions {
        let project = Some(attribution.project_id.clone());
        genesis_sources.insert(attribution.message_id.clone(), project.clone());
        if let Some(source_id) = attribution.source_id.as_deref() {
            genesis_sources.insert(source_id.to_owned(), project.clone());
        }
        if let Some(source_message_id) = attribution.source_message_id.as_deref() {
            genesis_sources.insert(source_message_id.to_owned(), project);
        }
        if let Some(source_turn_job_id) = attribution.source_turn_job_id.as_deref() {
            genesis_sources.insert(
                source_turn_job_id.to_owned(),
                Some(attribution.project_id.clone()),
            );
        }
    }
    for boundary in &genesis_boundaries {
        let project = Some(boundary.project_id.clone());
        if let Some(source_turn_job_id) = boundary.source_turn_job_id.as_deref() {
            genesis_sources.insert(source_turn_job_id.to_owned(), project.clone());
        }
        for source_message_id in &boundary.source_message_ids {
            genesis_sources.insert(source_message_id.clone(), project.clone());
        }
    }
    let genesis_source_ids: BTreeSet<String> = genesis_sources.keys().cloned().collect();
    let genesis_source_ids_json =
        serde_json::to_string(&genesis_source_ids).map_err(|_| DbError::InvalidTransition)?;
    let invocation_rows = fetch_ledger_invocations(
        db,
        owner_user_id,
        project_id,
        from,
        to,
        Some(&genesis_source_ids_json),
    )
    .await?;
    let event_rows = fetch_ledger_events(
        db,
        owner_user_id,
        project_id,
        from,
        to,
        Some(&genesis_source_ids_json),
    )
    .await?;
    let mut events_by_invocation: HashMap<String, Vec<LedgerEventRow>> = HashMap::new();
    for event in event_rows {
        events_by_invocation
            .entry(event.invocation_id.clone())
            .or_default()
            .push(event);
    }
    let mut invocations = Vec::new();
    for row in invocation_rows {
        let all_events = events_by_invocation.remove(&row.id).unwrap_or_default();
        let mut immutable_project_id = if row.surface == UsageSurface::GenesisChat {
            row.project_id
                .clone()
                .or_else(|| all_events.iter().find_map(|event| event.project_id.clone()))
                .or_else(|| genesis_sources.get(&row.source_id).cloned().flatten())
        } else {
            row.project_id.clone()
        };
        if let Some(project_id) = project_id {
            if row.surface == UsageSurface::GenesisChat
                && immutable_project_id.as_deref() != Some(project_id)
            {
                continue;
            }
        }
        let events_in_window: Vec<_> = all_events
            .iter()
            .filter(|event| timestamp_in_window(&event.occurred_at, from, to))
            .cloned()
            .collect();
        let has_events = !all_events.is_empty();
        let include = if has_events {
            !events_in_window.is_empty()
        } else {
            let occurrence = row.settled_at.as_deref().unwrap_or(&row.admitted_at);
            timestamp_in_window(occurrence, from, to)
        };
        if include {
            let mut row = row;
            // A historical Main Chat invocation may have been admitted before
            // the immutable Genesis handoff proof was materialized. Once the
            // proof binds that source to a Project, classify the invocation as
            // Genesis exactly once, rather than leaving provider attempts in
            // ordinary Main Chat while domain runs are reclassified below.
            if row.surface == UsageSurface::MainChat {
                if let Some(project_id) = genesis_sources.get(&row.source_id) {
                    row.surface = UsageSurface::GenesisChat;
                    immutable_project_id = project_id.clone();
                }
            }
            row.project_id = immutable_project_id.clone();
            invocations.push(LedgerInvocationView {
                row,
                events: events_in_window,
            });
        }
    }
    let mut domain_runs = fetch_domain_runs(db, owner_user_id, project_id, from, to).await?;
    if project_id.is_some() {
        domain_runs.extend(
            genesis_attributions
                .iter()
                .filter(|attribution| timestamp_in_window(&attribution.occurred_at, from, to))
                .map(|attribution| DomainRun {
                    surface: UsageSurface::GenesisChat,
                    source_id: attribution.message_id.clone(),
                    project_id: Some(attribution.project_id.clone()),
                }),
        );
    }
    // A successful Genesis handoff may terminalize its source turn through a
    // control-transfer event and intentionally write no assistant message.
    // Count that immutable turn job exactly once when its own terminal time is
    // in the requested window. If an assistant response exists, its immutable
    // message attribution already represents the same logical turn.
    let genesis_message_sources: HashSet<String> = genesis_attributions
        .iter()
        .flat_map(|attribution| {
            std::iter::once(attribution.message_id.clone())
                .chain(attribution.source_id.clone())
                .chain(attribution.source_message_id.clone())
                .chain(attribution.source_turn_job_id.clone())
        })
        .collect();
    domain_runs.extend(genesis_boundaries.iter().filter_map(|boundary| {
        let source_id = boundary.source_turn_job_id.clone()?;
        if genesis_message_sources.contains(&source_id) {
            return None;
        }
        let occurred_at = boundary.source_turn_occurred_at.as_deref()?;
        timestamp_in_window(occurred_at, from, to).then_some(DomainRun {
            surface: UsageSurface::GenesisChat,
            source_id,
            project_id: Some(boundary.project_id.clone()),
        })
    }));
    for run in &mut domain_runs {
        if run.surface == UsageSurface::MainChat {
            if let Some(project_id) = genesis_sources.get(&run.source_id) {
                run.surface = UsageSurface::GenesisChat;
                run.project_id = project_id.clone();
            }
        }
    }
    Ok(UsageDataset {
        invocations,
        domain_runs,
    })
}

#[async_trait]
impl UsageAnalyticsRepo for SqliteDb {
    async fn get_project_usage_analytics(
        &self,
        project_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<UsageAnalytics> {
        usage_analytics(fetch_usage_dataset(self, None, Some(project_id), from, to).await?)
    }

    async fn get_account_usage_analytics(
        &self,
        owner_user_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<AccountUsageAnalyticsResponse> {
        account_usage_analytics(
            fetch_usage_dataset(self, Some(owner_user_id), None, from, to).await?,
            from,
            to,
        )
    }

    async fn count_project_released_milestones(
        &self,
        project_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<i64> {
        validate_window(from, to)?;
        // Raw RFC3339 text is not ordered by instant when offsets differ.  Use
        // the parsed timestamp predicate shared by usage events so boundaries
        // remain correct for equivalent `Z` and offset-form timestamps.
        let rows = sqlx::query(
            "SELECT authorization_occurred_at FROM project_release
             WHERE project_id = ?
               AND (?2 IS NULL OR julianday(authorization_occurred_at) >= julianday(?2) - 1.0 / 86400.0)
               AND (?3 IS NULL OR julianday(authorization_occurred_at) < julianday(?3) + 1.0 / 86400.0)
             ORDER BY authorization_occurred_at ASC, id ASC",
        )
        .bind(project_id)
        .bind(from)
        .bind(to)
        .fetch_all(self.pool())
        .await?;
        let mut count = 0_i64;
        for row in rows {
            let occurred_at: String = row.try_get("authorization_occurred_at")?;
            if timestamp_in_window(&occurred_at, from, to) {
                count = count.checked_add(1).ok_or(DbError::InvalidTransition)?;
            }
        }
        Ok(count)
    }
}

#[cfg(test)]
mod ledger_analytics_tests {
    use super::*;

    fn run_key(source_id: &str) -> RunKey {
        RunKey {
            surface: UsageSurface::TaskExecution,
            source_id: source_id.to_owned(),
        }
    }

    fn attempt(costed: bool, pending: bool, unsettled: bool) -> AttemptSummary {
        AttemptSummary {
            costed,
            metered: !pending && !unsettled,
            pending,
            unsettled,
            reasons: Vec::new(),
        }
    }

    fn invocation(
        lifecycle: UsageInvocationLifecycle,
        telemetry_state: UsageTelemetryState,
        events: Vec<LedgerEventRow>,
    ) -> LedgerInvocationView {
        LedgerInvocationView {
            row: LedgerInvocationRow {
                id: "invocation".to_owned(),
                project_id: Some("project".to_owned()),
                surface: UsageSurface::TaskExecution,
                source_id: "run".to_owned(),
                admitted_provider_id: Some("provider".to_owned()),
                admitted_model_id: Some("model".to_owned()),
                agent_id: None,
                profile_id: None,
                agent_name_snapshot: None,
                project_name_snapshot: None,
                executor_type: None,
                lifecycle,
                telemetry_state,
                admitted_at: "2026-09-08T00:00:00Z".to_owned(),
                settled_at: Some("2026-09-08T00:00:01Z".to_owned()),
            },
            events,
        }
    }

    fn reported_event(counters: [Option<i64>; 4]) -> LedgerEventRow {
        LedgerEventRow {
            invocation_id: "invocation".to_owned(),
            project_id: Some("project".to_owned()),
            provider_id: Some("provider".to_owned()),
            model_id: Some("model".to_owned()),
            runtime_model: None,
            agent_id: None,
            profile_id: None,
            agent_name_snapshot: None,
            project_name_snapshot: None,
            executor_type: None,
            counters,
            provider_reported_nano_usd: Some(0),
            legacy_reported_cost_text: None,
            estimated_nano_usd: None,
            cost_kind: UsageCostKind::ProviderReported,
            provenance_kind: UsageEventProvenanceKind::RuntimeReport,
            rate_revision_id: None,
            catalog_snapshot_id: None,
            formula_revision: None,
            retrospective: false,
            coverage_reason_code: None,
            occurred_at: "2026-09-08T00:00:01Z".to_owned(),
            rate_source_kind: None,
            rate_effective_at: None,
            rate_input: None,
            rate_output: None,
            rate_cache_read: None,
            rate_cache_write: None,
            catalog_digest: None,
            catalog_fetched_at: None,
            catalog_freshness: None,
        }
    }

    #[test]
    fn decimal_money_is_fixed_point_and_preserves_explicit_zero() {
        assert_eq!(parse_money_decimal("0"), Some(0));
        assert_eq!(parse_money_decimal("0.000000001"), Some(1));
        assert_eq!(parse_money_decimal("1.2345678915"), Some(1_234_567_892));
        assert_eq!(parse_money_decimal("'0.25'"), Some(250_000_000));
        assert_eq!(parse_money_decimal("NULL"), None);
        assert_eq!(
            money(Some(0)),
            Some(MoneyAmount {
                currency: "USD".to_owned(),
                decimal: "0".to_owned(),
            })
        );
    }

    #[test]
    fn timestamp_window_uses_exact_rfc3339_instants_at_subsecond_boundaries() {
        let from = "2026-09-08T00:00:00.000000001Z";
        let to = "2026-09-08T00:00:00.000000003Z";
        assert!(timestamp_in_window(
            "2026-09-08T00:00:00.000000001Z",
            Some(from),
            Some(to)
        ));
        assert!(timestamp_in_window(
            "2026-09-08T05:30:00.000000002+05:30",
            Some(from),
            Some(to)
        ));
        assert!(!timestamp_in_window(to, Some(from), Some(to)));
    }

    #[test]
    fn coverage_is_no_usage_without_provider_attempts() {
        let summary = cost_summary(
            CostAccumulator::default(),
            &BTreeMap::from([(run_key("no-provider"), Vec::new())]),
        )
        .expect("coverage summary");
        assert_eq!(summary.kind, CostKind::None);
        assert_eq!(summary.coverage, CostCoverage::NoUsage);
        assert_eq!(summary.complete_total, None);
        assert_eq!(summary.usage_coverage.no_provider_call_runs_or_turns, 1);
    }

    #[test]
    fn pending_precedes_partial_and_keeps_known_subtotal_separate() {
        let accumulator = CostAccumulator {
            provider_reported_nano_usd: Some(1_000_000_000),
            total_provider_attempts: 2,
            settled_provider_attempts: 1,
            pending_provider_attempts: 1,
            costed_provider_attempts: 1,
            ..CostAccumulator::default()
        };
        let summary = cost_summary(
            accumulator,
            &BTreeMap::from([(
                run_key("pending"),
                vec![attempt(true, false, false), attempt(false, true, false)],
            )]),
        )
        .expect("coverage summary");
        assert_eq!(summary.coverage, CostCoverage::Pending);
        assert_eq!(summary.kind, CostKind::ProviderReported);
        assert_eq!(summary.known_subtotal.unwrap().decimal, "1");
        assert_eq!(summary.complete_total, None);
        assert_eq!(summary.usage_coverage.pending_runs_or_turns, 1);
    }

    #[test]
    fn terminal_unsettled_is_unavailable_without_another_costed_attempt() {
        let accumulator = CostAccumulator {
            total_provider_attempts: 1,
            unsettled_provider_attempts: 1,
            ..CostAccumulator::default()
        };
        let summary = cost_summary(
            accumulator,
            &BTreeMap::from([(run_key("unsettled"), vec![attempt(false, false, true)])]),
        )
        .expect("coverage summary");
        assert_eq!(summary.coverage, CostCoverage::Unavailable);
        assert_eq!(summary.kind, CostKind::Unknown);
        assert_eq!(summary.usage_coverage.unsettled_provider_attempts, 1);
    }

    #[test]
    fn mixed_costed_and_unpriced_attempts_are_partial() {
        let accumulator = CostAccumulator {
            provider_reported_nano_usd: Some(2_000_000_000),
            total_provider_attempts: 2,
            settled_provider_attempts: 2,
            costed_provider_attempts: 1,
            unpriced_provider_attempts: 1,
            ..CostAccumulator::default()
        };
        let summary = cost_summary(
            accumulator,
            &BTreeMap::from([(
                run_key("partial"),
                vec![attempt(true, false, false), attempt(false, false, false)],
            )]),
        )
        .expect("coverage summary");
        assert_eq!(summary.coverage, CostCoverage::Partial);
        assert_eq!(summary.known_subtotal.unwrap().decimal, "2");
        assert_eq!(summary.complete_total, None);
        assert_eq!(summary.usage_coverage.partially_costed_runs_or_turns, 1);
    }

    #[test]
    fn no_event_and_pending_attempts_stay_in_the_attempt_partition() {
        let no_event = invocation(
            UsageInvocationLifecycle::Settled,
            UsageTelemetryState::Unmetered,
            Vec::new(),
        );
        let no_event_key = run_key("no-event");
        let mut no_event_accumulator = CostAccumulator::default();
        let no_event_summary = attempt_summary(&no_event, &mut no_event_accumulator, &no_event_key)
            .expect("no-event attempt");
        assert!(!no_event_summary.costed);
        assert_eq!(no_event_accumulator.total_provider_attempts, 1);
        assert_eq!(no_event_accumulator.costed_provider_attempts, 0);
        assert_eq!(no_event_accumulator.unpriced_provider_attempts, 1);

        let pending = invocation(
            UsageInvocationLifecycle::PendingSettlement,
            UsageTelemetryState::Pending,
            Vec::new(),
        );
        let pending_key = run_key("pending-no-event");
        let mut pending_accumulator = CostAccumulator::default();
        let pending_summary = attempt_summary(&pending, &mut pending_accumulator, &pending_key)
            .expect("pending attempt");
        assert!(!pending_summary.costed);
        assert_eq!(pending_accumulator.total_provider_attempts, 1);
        assert_eq!(pending_accumulator.costed_provider_attempts, 0);
        assert_eq!(pending_accumulator.unpriced_provider_attempts, 1);
        assert_eq!(pending_accumulator.pending_provider_attempts, 1);
    }

    #[test]
    fn reported_money_without_counters_is_costed_but_unmetered() {
        let invocation = invocation(
            UsageInvocationLifecycle::Settled,
            UsageTelemetryState::Unmetered,
            vec![reported_event([None, None, None, None])],
        );
        let key = run_key("reported-only");
        let mut accumulator = CostAccumulator::default();
        let summary =
            attempt_summary(&invocation, &mut accumulator, &key).expect("reported-only attempt");
        assert!(summary.costed);
        assert_eq!(accumulator.total_provider_attempts, 1);
        assert_eq!(accumulator.costed_provider_attempts, 1);
        assert_eq!(accumulator.unpriced_provider_attempts, 0);
        assert_eq!(accumulator.unmetered_provider_attempts, 1);
        assert_eq!(accumulator.provider_reported_nano_usd, Some(0));
        assert_eq!(accumulator.tokens, [0; 4]);
    }

    #[test]
    fn missing_catalog_freshness_fails_closed_in_estimate_provenance() {
        let mut event = reported_event([Some(1), None, None, None]);
        event.provider_reported_nano_usd = None;
        event.estimated_nano_usd = Some(1);
        event.cost_kind = UsageCostKind::Estimated;
        event.rate_source_kind = Some("models_dev_catalog".to_owned());
        event.rate_revision_id = Some("rate-revision".to_owned());
        let (_, source) = event_source(&event).expect("estimate source");
        assert_eq!(source.freshness, CostSourceFreshness::RefreshFailed);
    }

    #[test]
    fn genesis_unmetered_invocation_keeps_immutable_project_attribution() {
        let project_id = "project-genesis".to_owned();
        let dataset = UsageDataset {
            invocations: vec![LedgerInvocationView {
                row: LedgerInvocationRow {
                    id: "genesis-invocation".to_owned(),
                    project_id: Some(project_id.clone()),
                    surface: UsageSurface::GenesisChat,
                    source_id: "genesis-source-turn".to_owned(),
                    admitted_provider_id: Some("openai".to_owned()),
                    admitted_model_id: Some("gpt-test".to_owned()),
                    agent_id: Some("main-agent".to_owned()),
                    profile_id: Some("main-profile".to_owned()),
                    agent_name_snapshot: Some("Main".to_owned()),
                    project_name_snapshot: Some("Genesis Project".to_owned()),
                    executor_type: Some("native".to_owned()),
                    lifecycle: UsageInvocationLifecycle::Settled,
                    telemetry_state: UsageTelemetryState::Unmetered,
                    admitted_at: "2026-09-08T00:00:00Z".to_owned(),
                    settled_at: Some("2026-09-08T00:00:01Z".to_owned()),
                },
                events: Vec::new(),
            }],
            domain_runs: Vec::new(),
        };

        let analytics = account_usage_analytics(dataset, None, None)
            .expect("Genesis usage analytics should accept a no-event invocation");
        assert_eq!(analytics.token_usage.counts.chat_turn_count, 1);
        assert_eq!(analytics.token_usage.by_surface.len(), 1);
        let genesis_surface = analytics
            .token_usage
            .by_surface
            .iter()
            .find(|surface| surface.surface == ApiUsageSurface::GenesisChat)
            .expect("Genesis surface is present");
        assert_eq!(genesis_surface.counts.chat_turn_count, 1);
        assert_eq!(analytics.by_project.len(), 1);
        assert_eq!(
            analytics.by_project[0].project_id.as_deref(),
            Some(project_id.as_str())
        );
        assert_eq!(analytics.by_project[0].counts.chat_turn_count, 1);
        assert_eq!(analytics.by_project[0].counts.provider_attempt_count, 1);
    }

    #[test]
    fn account_project_group_keeps_no_provider_domain_run() {
        let analytics = account_usage_analytics(
            UsageDataset {
                invocations: Vec::new(),
                domain_runs: vec![DomainRun {
                    surface: UsageSurface::GenesisChat,
                    source_id: "genesis-no-provider".to_owned(),
                    project_id: Some("project-genesis".to_owned()),
                }],
            },
            None,
            None,
        )
        .expect("account analytics should retain domain-only Project activity");
        assert_eq!(analytics.by_project.len(), 1);
        assert_eq!(analytics.by_project[0].counts.chat_turn_count, 1);
        assert_eq!(analytics.by_project[0].counts.provider_attempt_count, 0);
        assert_eq!(analytics.by_project[0].cost.coverage, CostCoverage::NoUsage);
    }

    #[tokio::test]
    async fn ledger_projection_queries_empty_project_and_account_scopes() {
        let pool = crate::create_sqlite_pool("sqlite::memory:")
            .await
            .expect("pool creates");
        crate::run_migrations(&pool).await.expect("migrations run");
        let db = SqliteDb::new(pool);
        let project_id = "missing-project";

        let project = UsageAnalyticsRepo::get_project_usage_analytics(&db, project_id, None, None)
            .await
            .expect("empty project projection");
        assert_eq!(project.cost.coverage, CostCoverage::NoUsage);
        assert_eq!(
            project.counts,
            ActivityCounts {
                task_execution_count: 0,
                chat_turn_count: 0,
                inquiry_count: 0,
                provider_attempt_count: 0,
            }
        );

        let account =
            UsageAnalyticsRepo::get_account_usage_analytics(&db, "missing-owner", None, None)
                .await
                .expect("empty account projection");
        assert_eq!(account.token_usage.cost.coverage, CostCoverage::NoUsage);
        assert!(account.by_project.is_empty());
    }
}
