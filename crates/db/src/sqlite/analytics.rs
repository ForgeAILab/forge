use super::*;
use chrono::DateTime;
use serde::Deserialize;
use std::collections::BTreeMap;

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
        query.push(" AND r.started_at >= ").push_bind(from);
    }
    if let Some(to) = to {
        query.push(" AND r.started_at <= ").push_bind(to);
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
                entry.total_runs += 1;
                if exit_code == 0 {
                    entry.pass_count += 1;
                } else {
                    entry.fail_count += 1;
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

        Ok(by_command
            .into_iter()
            .map(|(command, mut aggregate)| {
                aggregate.duration_ms.sort_unstable();
                let len = aggregate.duration_ms.len();
                let avg_duration_ms = if len > 0 {
                    Some(aggregate.duration_ms.iter().sum::<i64>() / len as i64)
                } else {
                    None
                };
                let p50_duration_ms = if len > 0 {
                    Some(aggregate.duration_ms[len / 2])
                } else {
                    None
                };
                let p95_duration_ms = if len > 0 {
                    Some(aggregate.duration_ms[(len * 95) / 100])
                } else {
                    None
                };

                CiStepStats {
                    command,
                    total_runs: aggregate.total_runs,
                    pass_count: aggregate.pass_count,
                    fail_count: aggregate.fail_count,
                    avg_duration_ms,
                    p50_duration_ms,
                    p95_duration_ms,
                    last_run_at: aggregate.last_run_at,
                }
            })
            .collect())
    }

    async fn get_project_token_analytics(
        &self,
        project_id: &str,
        from: Option<&str>,
        to: Option<&str>,
    ) -> Result<ProjectTokenStats> {
        let mut query = sqlx::QueryBuilder::<Sqlite>::new(
            "SELECT \
                eu.provider, \
                eu.model, \
                SUM(eu.input_tokens) AS input_tokens, \
                SUM(eu.output_tokens) AS output_tokens, \
                SUM(eu.cache_read_tokens) AS cache_read_tokens, \
                SUM(eu.cache_write_tokens) AS cache_write_tokens, \
                SUM(eu.cost_usd) AS cost_usd, \
                COUNT(DISTINCT eu.execution_id) AS execution_count \
             FROM execution_usage eu \
             JOIN execution e ON eu.execution_id = e.id \
             JOIN task t ON e.task_id = t.id \
             WHERE t.project_id = ",
        );
        query.push_bind(project_id);
        if let Some(from) = from {
            query.push(" AND eu.created_at >= ").push_bind(from);
        }
        if let Some(to) = to {
            query.push(" AND eu.created_at <= ").push_bind(to);
        }
        query.push(" GROUP BY eu.provider, eu.model ORDER BY eu.provider ASC, eu.model ASC");

        let rows = query.build().fetch_all(self.pool()).await?;
        let mut by_model = Vec::with_capacity(rows.len());

        let mut total_input_tokens = 0_i64;
        let mut total_output_tokens = 0_i64;
        let mut total_cache_read_tokens = 0_i64;
        let mut total_cache_write_tokens = 0_i64;
        let mut total_cost_usd: Option<f64> = None;

        for row in rows {
            let input_tokens = row.try_get::<Option<i64>, _>("input_tokens")?.unwrap_or(0);
            let output_tokens = row.try_get::<Option<i64>, _>("output_tokens")?.unwrap_or(0);
            let cache_read_tokens = row
                .try_get::<Option<i64>, _>("cache_read_tokens")?
                .unwrap_or(0);
            let cache_write_tokens = row
                .try_get::<Option<i64>, _>("cache_write_tokens")?
                .unwrap_or(0);
            let cost_usd = row.try_get::<Option<f64>, _>("cost_usd")?;
            let execution_count = row.try_get::<i64, _>("execution_count")?;

            total_input_tokens += input_tokens;
            total_output_tokens += output_tokens;
            total_cache_read_tokens += cache_read_tokens;
            total_cache_write_tokens += cache_write_tokens;

            if let Some(cost_usd) = cost_usd {
                total_cost_usd = Some(total_cost_usd.unwrap_or(0.0) + cost_usd);
            }

            by_model.push(ModelTokenBreakdown {
                provider: row.try_get("provider")?,
                model: row.try_get("model")?,
                input_tokens,
                output_tokens,
                cache_read_tokens,
                cache_write_tokens,
                cost_usd,
                execution_count,
            });
        }

        let mut total_count_query = sqlx::QueryBuilder::<Sqlite>::new(
            "SELECT COUNT(DISTINCT eu.execution_id) AS execution_count \
             FROM execution_usage eu \
             JOIN execution e ON eu.execution_id = e.id \
             JOIN task t ON e.task_id = t.id \
             WHERE t.project_id = ",
        );
        total_count_query.push_bind(project_id);
        if let Some(from) = from {
            total_count_query
                .push(" AND eu.created_at >= ")
                .push_bind(from);
        }
        if let Some(to) = to {
            total_count_query
                .push(" AND eu.created_at <= ")
                .push_bind(to);
        }
        let total_count_row = total_count_query.build().fetch_one(self.pool()).await?;
        let execution_count = total_count_row.try_get::<i64, _>("execution_count")?;

        let mut agent_query = sqlx::QueryBuilder::<Sqlite>::new(
            "SELECT \
                a.id AS agent_id, \
                a.name AS agent_name, \
                a.executor_type AS executor_type, \
                a.model AS model, \
                COUNT(DISTINCT e.id) AS execution_count, \
                COALESCE(SUM(CASE WHEN e.status = 'completed' THEN 1 ELSE 0 END), 0) AS completed_runs, \
                AVG(CASE \
                    WHEN e.status != 'running' \
                    THEN (JULIANDAY(e.updated_at) - JULIANDAY(e.created_at)) * 86400000 \
                    ELSE NULL \
                END) AS avg_duration_ms \
             FROM agent_current a \
             JOIN execution e ON e.agent_id = a.id \
             JOIN task t ON e.task_id = t.id \
             WHERE t.project_id = ",
        );
        agent_query.push_bind(project_id);
        if let Some(from) = from {
            agent_query.push(" AND e.created_at >= ").push_bind(from);
        }
        if let Some(to) = to {
            agent_query.push(" AND e.created_at <= ").push_bind(to);
        }
        agent_query.push(" GROUP BY a.id, a.name, a.executor_type, a.model ORDER BY a.name ASC");
        let agent_rows = agent_query.build().fetch_all(self.pool()).await?;

        let mut agent_usage_query = sqlx::QueryBuilder::<Sqlite>::new(
            "SELECT \
                e.agent_id AS agent_id, \
                COALESCE(SUM(eu.input_tokens), 0) AS input_tokens, \
                COALESCE(SUM(eu.output_tokens), 0) AS output_tokens, \
                COALESCE(SUM(eu.cache_read_tokens), 0) AS cache_read_tokens, \
                COALESCE(SUM(eu.cache_write_tokens), 0) AS cache_write_tokens, \
                SUM(eu.cost_usd) AS cost_usd \
             FROM execution_usage eu \
             JOIN execution e ON eu.execution_id = e.id \
             JOIN task t ON e.task_id = t.id \
             WHERE e.agent_id IS NOT NULL AND t.project_id = ",
        );
        agent_usage_query.push_bind(project_id);
        if let Some(from) = from {
            agent_usage_query
                .push(" AND eu.created_at >= ")
                .push_bind(from);
        }
        if let Some(to) = to {
            agent_usage_query
                .push(" AND eu.created_at <= ")
                .push_bind(to);
        }
        agent_usage_query.push(" GROUP BY e.agent_id");
        let agent_usage_rows = agent_usage_query.build().fetch_all(self.pool()).await?;

        struct AgentUsageRow {
            input_tokens: i64,
            output_tokens: i64,
            cache_read_tokens: i64,
            cache_write_tokens: i64,
            cost_usd: Option<f64>,
        }
        let mut usage_by_agent: std::collections::HashMap<String, AgentUsageRow> =
            std::collections::HashMap::new();
        for row in agent_usage_rows {
            let agent_id: String = row.try_get("agent_id")?;
            let input_tokens = row.try_get::<Option<i64>, _>("input_tokens")?.unwrap_or(0);
            let output_tokens = row.try_get::<Option<i64>, _>("output_tokens")?.unwrap_or(0);
            let cache_read_tokens = row
                .try_get::<Option<i64>, _>("cache_read_tokens")?
                .unwrap_or(0);
            let cache_write_tokens = row
                .try_get::<Option<i64>, _>("cache_write_tokens")?
                .unwrap_or(0);
            let cost_usd = row.try_get::<Option<f64>, _>("cost_usd")?;
            usage_by_agent.insert(
                agent_id,
                AgentUsageRow {
                    input_tokens,
                    output_tokens,
                    cache_read_tokens,
                    cache_write_tokens,
                    cost_usd,
                },
            );
        }

        let mut by_agent = Vec::with_capacity(agent_rows.len());
        for row in agent_rows {
            let agent_id: String = row.try_get("agent_id")?;
            let agent_name: String = row.try_get("agent_name")?;
            let executor_type: String = row.try_get("executor_type")?;
            let model: Option<String> = row.try_get("model")?;
            let exec_count: i64 = row.try_get("execution_count")?;
            let completed_runs: i64 = row.try_get("completed_runs")?;
            let avg_duration_ms = row
                .try_get::<Option<f64>, _>("avg_duration_ms")?
                .map(|d| d.round() as i64);
            let success_rate = if exec_count > 0 {
                Some(completed_runs as f64 / exec_count as f64)
            } else {
                None
            };

            let usage = usage_by_agent.remove(&agent_id);
            let (input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, cost_usd) =
                if let Some(u) = usage {
                    (
                        u.input_tokens,
                        u.output_tokens,
                        u.cache_read_tokens,
                        u.cache_write_tokens,
                        u.cost_usd,
                    )
                } else {
                    (0, 0, 0, 0, None)
                };

            by_agent.push(AgentTokenBreakdown {
                agent_id,
                agent_name,
                executor_type,
                model,
                input_tokens,
                output_tokens,
                cache_read_tokens,
                cache_write_tokens,
                cost_usd,
                execution_count: exec_count,
                success_rate,
                avg_duration_ms,
            });
        }

        Ok(ProjectTokenStats {
            total_input_tokens,
            total_output_tokens,
            total_cache_read_tokens,
            total_cache_write_tokens,
            total_cost_usd,
            execution_count,
            by_model,
            by_agent,
        })
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
            query.push(" AND r.started_at >= ").push_bind(from);
        }
        if let Some(to) = to {
            query.push(" AND r.started_at <= ").push_bind(to);
        }

        let row = query.build().fetch_one(self.pool()).await?;
        let total_reviews: i64 = row.try_get("total_reviews")?;
        let passed: i64 = row.try_get("passed")?;
        let failed: i64 = row.try_get("failed")?;
        let cancelled: i64 = row.try_get("cancelled")?;
        let avg_duration_ms: Option<i64> = row
            .try_get::<Option<f64>, _>("avg_duration_ms")?
            .map(|value| value as i64);
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
        ProjectRepo::update(
            db,
            UpdateProject {
                id: project_id.clone(),
                name: None,
                settings: None,
                primary_repo_id: Some(Some(repo_id.clone())),
                paused_at: None,
                updated_at: crate::now_rfc3339(),
            },
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

    async fn seed_agent(db: &SqliteDb, name: &str, model: Option<&str>) -> String {
        let agent = AgentRepo::create(
            db,
            CreateAgent {
                id: crate::new_uuid_v4(),
                name: name.to_owned(),
                description: None,
                executor_type: "claude-code".to_owned(),
                model: model.map(str::to_owned),
                reasoning_effort: None,
                permission_policy: None,
                prompt_template: None,
                capabilities_json: "[]".to_owned(),
                config_json: "{}".to_owned(),
                credential_ref: None,
                daemon_id: None,
                max_concurrent_tasks: 1,
                heartbeat_interval_seconds: 30,
                max_missed_heartbeats: 3,
                status: AgentStatus::Idle,
                last_heartbeat_at: None,
                is_default: false,
                paused: false,
                owner_id: None,
                visibility: "global".to_owned(),
                created_at: crate::now_rfc3339(),
                updated_at: crate::now_rfc3339(),
            },
        )
        .await
        .expect("agent creates");
        agent.id
    }

    async fn seed_execution_with_agent(
        db: &SqliteDb,
        task_id: &str,
        agent_id: Option<&str>,
        status: ExecutionStatus,
        created_at: &str,
    ) -> String {
        let execution_id = crate::new_uuid_v4();
        ExecutionRepo::create(
            db,
            CreateExecution {
                id: execution_id.clone(),
                task_id: task_id.to_owned(),
                agent_id: agent_id.map(str::to_owned),
                role: "reviewer".to_owned(),
                status,
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
    async fn seed_execution_usage(
        db: &SqliteDb,
        execution_id: &str,
        model: &str,
        input_tokens: i64,
        output_tokens: i64,
        cache_read_tokens: i64,
        cache_write_tokens: i64,
        cost_usd: Option<f64>,
        created_at: &str,
    ) {
        sqlx::query(
            "INSERT INTO execution_usage (id, execution_id, provider, model, input_tokens, output_tokens, cache_read_tokens, cache_write_tokens, cost_usd, created_at) \
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(crate::new_uuid_v4())
        .bind(execution_id)
        .bind("openai")
        .bind(model)
        .bind(input_tokens)
        .bind(output_tokens)
        .bind(cache_read_tokens)
        .bind(cache_write_tokens)
        .bind(cost_usd)
        .bind(created_at)
        .execute(db.pool())
        .await
        .expect("execution usage creates");
    }

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
    async fn token_analytics_happy_path() {
        let db = sqlite_db().await;
        let (project_id, _repo_id, task_id) = seed_project_repo_task(&db).await;
        let execution_one = seed_execution(&db, &task_id, "2026-04-10T00:00:00Z").await;
        let execution_two = seed_execution(&db, &task_id, "2026-04-11T00:00:00Z").await;

        seed_execution_usage(
            &db,
            &execution_one,
            "gpt-4.1",
            100,
            30,
            10,
            5,
            Some(0.15),
            "2026-04-10T00:00:00Z",
        )
        .await;
        seed_execution_usage(
            &db,
            &execution_two,
            "gpt-4.1-mini",
            50,
            20,
            0,
            0,
            Some(0.05),
            "2026-04-11T00:00:00Z",
        )
        .await;

        let stats = ProjectAnalyticsRepo::get_project_token_analytics(&db, &project_id, None, None)
            .await
            .expect("token analytics computed");

        assert_eq!(stats.total_input_tokens, 150);
        assert_eq!(stats.total_output_tokens, 50);
        assert_eq!(stats.total_cache_read_tokens, 10);
        assert_eq!(stats.total_cache_write_tokens, 5);
        assert_eq!(stats.total_cost_usd, Some(0.2));
        assert_eq!(stats.execution_count, 2);
        assert_eq!(stats.by_model.len(), 2);
        assert_eq!(stats.by_model[0].provider, "openai");
        assert_eq!(stats.by_model[0].model, "gpt-4.1");
        assert_eq!(stats.by_model[0].input_tokens, 100);
        assert_eq!(stats.by_model[0].execution_count, 1);
        assert_eq!(stats.by_model[1].provider, "openai");
        assert_eq!(stats.by_model[1].model, "gpt-4.1-mini");
        assert_eq!(stats.by_model[1].input_tokens, 50);
        assert_eq!(stats.by_model[1].execution_count, 1);
    }

    #[tokio::test]
    async fn token_analytics_empty_result() {
        let db = sqlite_db().await;
        let project_id = crate::new_uuid_v4();

        let stats = ProjectAnalyticsRepo::get_project_token_analytics(&db, &project_id, None, None)
            .await
            .expect("token analytics computed");

        assert_eq!(stats.total_input_tokens, 0);
        assert_eq!(stats.total_output_tokens, 0);
        assert_eq!(stats.total_cache_read_tokens, 0);
        assert_eq!(stats.total_cache_write_tokens, 0);
        assert_eq!(stats.total_cost_usd, None);
        assert_eq!(stats.execution_count, 0);
        assert!(stats.by_model.is_empty());
    }

    #[tokio::test]
    async fn token_analytics_date_filter() {
        let db = sqlite_db().await;
        let (project_id, _repo_id, task_id) = seed_project_repo_task(&db).await;
        let execution_old = seed_execution(&db, &task_id, "2026-03-01T00:00:00Z").await;
        let execution_new = seed_execution(&db, &task_id, "2026-04-20T00:00:00Z").await;

        seed_execution_usage(
            &db,
            &execution_old,
            "gpt-4.1",
            100,
            30,
            0,
            0,
            Some(0.1),
            "2026-03-01T00:00:00Z",
        )
        .await;
        seed_execution_usage(
            &db,
            &execution_new,
            "gpt-4.1",
            70,
            15,
            0,
            0,
            Some(0.07),
            "2026-04-20T00:00:00Z",
        )
        .await;

        let stats = ProjectAnalyticsRepo::get_project_token_analytics(
            &db,
            &project_id,
            Some("2026-04-01T00:00:00Z"),
            Some("2026-04-30T23:59:59Z"),
        )
        .await
        .expect("token analytics filtered");

        assert_eq!(stats.total_input_tokens, 70);
        assert_eq!(stats.total_output_tokens, 15);
        assert_eq!(stats.total_cost_usd, Some(0.07));
        assert_eq!(stats.execution_count, 1);
        assert_eq!(stats.by_model.len(), 1);
        assert_eq!(stats.by_model[0].provider, "openai");
        assert_eq!(stats.by_model[0].execution_count, 1);
    }

    #[tokio::test]
    async fn token_analytics_by_agent() {
        let db = sqlite_db().await;
        let (project_id, _repo_id, task_id) = seed_project_repo_task(&db).await;
        let agent_a = seed_agent(&db, "Agent Alpha", Some("gpt-4.1")).await;
        let agent_b = seed_agent(&db, "Agent Beta", Some("claude-3-7-sonnet")).await;

        let exec_a = seed_execution_with_agent(
            &db,
            &task_id,
            Some(&agent_a),
            ExecutionStatus::Completed,
            "2026-04-10T00:00:00Z",
        )
        .await;
        let exec_b = seed_execution_with_agent(
            &db,
            &task_id,
            Some(&agent_b),
            ExecutionStatus::Failed,
            "2026-04-11T00:00:00Z",
        )
        .await;

        seed_execution_usage(
            &db,
            &exec_a,
            "gpt-4.1",
            200,
            50,
            20,
            10,
            Some(0.25),
            "2026-04-10T00:00:00Z",
        )
        .await;
        seed_execution_usage(
            &db,
            &exec_b,
            "claude-3-7-sonnet",
            100,
            40,
            0,
            0,
            Some(0.12),
            "2026-04-11T00:00:00Z",
        )
        .await;

        let stats = ProjectAnalyticsRepo::get_project_token_analytics(&db, &project_id, None, None)
            .await
            .expect("token analytics computed");

        assert_eq!(stats.by_agent.len(), 2);
        assert_eq!(stats.by_agent[0].agent_id, agent_a);
        assert_eq!(stats.by_agent[0].agent_name, "Agent Alpha");
        assert_eq!(stats.by_agent[0].input_tokens, 200);
        assert_eq!(stats.by_agent[0].output_tokens, 50);
        assert_eq!(stats.by_agent[0].cache_read_tokens, 20);
        assert_eq!(stats.by_agent[0].cache_write_tokens, 10);
        assert_eq!(stats.by_agent[0].cost_usd, Some(0.25));
        assert_eq!(stats.by_agent[0].execution_count, 1);
        assert_eq!(stats.by_agent[0].success_rate, Some(1.0));

        assert_eq!(stats.by_agent[1].agent_id, agent_b);
        assert_eq!(stats.by_agent[1].agent_name, "Agent Beta");
        assert_eq!(stats.by_agent[1].input_tokens, 100);
        assert_eq!(stats.by_agent[1].output_tokens, 40);
        assert_eq!(stats.by_agent[1].cost_usd, Some(0.12));
        assert_eq!(stats.by_agent[1].execution_count, 1);
        assert_eq!(stats.by_agent[1].success_rate, Some(0.0));

        // Test stats_by_agent directly as well
        let stats_a = ExecutionRepo::stats_by_agent(&db, &agent_a)
            .await
            .expect("stats_by_agent");
        assert_eq!(stats_a.total_runs, 1);
        assert_eq!(stats_a.success_rate, Some(1.0));
        assert_eq!(stats_a.total_input_tokens, 200);
        assert_eq!(stats_a.total_output_tokens, 50);
        assert_eq!(stats_a.total_cache_read_tokens, 20);
        assert_eq!(stats_a.total_cache_write_tokens, 10);
        assert_eq!(stats_a.total_cost_usd, Some(0.25));
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
