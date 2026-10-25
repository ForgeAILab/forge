//! Execution creation, Task admission fences, capacity, and review binding.

use super::*;

impl SqliteDb {
    pub(super) async fn create_execution_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: &CreateExecution,
        admission: Option<&ExecutionAdmission>,
    ) -> Result<Execution> {
        if input.status == ExecutionStatus::Running {
            if !crate::task_writer::owns_task(&input.task_id) {
                return Err(DbError::Check(
                    "execution admission requires the Task step".to_owned(),
                ));
            }
            self.fence_current_step_in_tx(transaction).await?;
        }
        if input.status == ExecutionStatus::Running {
            if let Some(workspace_id) = &input.workspace_id {
                let pending: bool = sqlx::query_scalar(
                    "SELECT EXISTS(SELECT 1 FROM pending_remote_cancel WHERE workspace_id=?)",
                )
                .bind(workspace_id)
                .fetch_one(&mut **transaction)
                .await?;
                if pending {
                    return Err(DbError::Check(
                        "workspace has an unconfirmed remote cancellation".to_owned(),
                    ));
                }
            }
        }
        if input.status == ExecutionStatus::Running {
            if let Some(admission) = admission {
                Self::ensure_task_execution_admission_in_tx(transaction, input, admission).await?;
            }
        }
        if input.status == ExecutionStatus::Running {
            let paused_project_id: Option<String> = sqlx::query_scalar(
                "SELECT p.id
                 FROM task t
                 JOIN project p ON p.id = t.project_id
                 WHERE t.id = ? AND p.paused_at IS NOT NULL",
            )
            .bind(&input.task_id)
            .fetch_optional(&mut **transaction)
            .await?;
            if let Some(project_id) = paused_project_id {
                return Err(DbError::ProjectPaused { project_id });
            }
        }
        // The service performs a read-only admission check before preparing a
        // workspace. Recheck the authoritative Charter link in the same
        // transaction as the execution INSERT so a Charter supersession
        // racing that read cannot mint a stale Running execution.
        // The service removes a newly prepared workspace when this guard
        // rejects the execution, so no fresh lease remains behind.
        // Legacy/unverified Projects intentionally bypass this guard.
        if input.status == ExecutionStatus::Running {
            if let Some(workspace_id) = input.workspace_id.as_deref() {
                Self::ensure_execution_admission_in_tx(transaction, &input.task_id, workspace_id)
                    .await?;
            }
        }
        if input.status == ExecutionStatus::Running {
            // Child admission is rechecked under the same writer lock as the
            // execution insert. A root cancellation/review transition that
            // wins the race therefore prevents a stale child launcher from
            // minting a Running execution after the coordination boundary.
            let parent_row = sqlx::query(
                "SELECT parent.status,
                        parent.blocked_json,
                        parent.failed_json,
                        parent.error_annotation,
                        parent.entry_barrier_json,
                        project.workflow_definition
                 FROM task AS child
                 JOIN task AS parent ON parent.id = child.parent_task_id
                 JOIN project ON project.id = parent.project_id
                 WHERE child.id = ?
                   AND child.deleted_at IS NULL
                   AND parent.deleted_at IS NULL",
            )
            .bind(&input.task_id)
            .fetch_optional(&mut **transaction)
            .await?;
            if let Some(parent_row) = parent_row {
                let parent_status: String = parent_row.try_get("status")?;
                let workflow_definition: String = parent_row.try_get("workflow_definition")?;
                let parsed_workflow =
                    serde_json::from_str::<api_types::WorkflowDefinition>(&workflow_definition)
                        .ok();
                let parent_state_blocks_execution = parsed_workflow
                    .as_ref()
                    .and_then(|workflow| {
                        workflow
                            .states
                            .iter()
                            .find(|state| state.name == parent_status)
                    })
                    .is_some_and(|state| {
                        matches!(
                            state.kind,
                            api_types::StateKind::Backlog | api_types::StateKind::Terminal
                        ) || (state.kind == api_types::StateKind::Gate
                            && state.canonical_phase == Some(api_types::CanonicalPhase::Review))
                    })
                    || parsed_workflow
                        .as_ref()
                        .and_then(|workflow| workflow.cancellation_state.as_deref())
                        == Some(parent_status.as_str())
                    || matches!(
                        parent_status.as_str(),
                        "backlog" | "review" | "merging" | "done" | "cancelled"
                    );
                let parent_has_blocker = parent_row
                    .try_get::<Option<String>, _>("blocked_json")?
                    .is_some()
                    || parent_row
                        .try_get::<Option<String>, _>("failed_json")?
                        .is_some()
                    || parent_row
                        .try_get::<Option<String>, _>("error_annotation")?
                        .is_some()
                    || parent_row
                        .try_get::<Option<String>, _>("entry_barrier_json")?
                        .is_some();
                if parent_state_blocks_execution || parent_has_blocker {
                    return Err(DbError::InvalidTransition);
                }
            }

            // `executor` is the historical transport alias for the canonical
            // implementation role. Re-execution/recovery may still carry the
            // alias, but it must observe the same assignment CAS as `coder`.
            let assignment_role =
                canonical_execution_role(Some(input.role.as_str())).unwrap_or(input.role.as_str());
            let assignment_holder =
                task::assignment_holder_in_tx(transaction, &input.task_id, assignment_role).await?;
            let role_assignment = sqlx::query(
                "SELECT assignee_type, assignee_id
                 FROM task_role_assignment WHERE task_id = ? AND role_name = ?",
            )
            .bind(&assignment_holder)
            .bind(assignment_role)
            .fetch_optional(&mut **transaction)
            .await?;
            if let Some(role_assignment) = role_assignment {
                // An auditor runs under a separately selected Agent while the
                // reviewer assignment stays the authority, so its principal is
                // the reviewer execution's Agent -- which only the admission
                // snapshot names. Resolve that here, where the comparison
                // actually happens: demanding it for every auditor row
                // rejected an auditor execution created without an admission
                // as a bare version conflict, even with no assignment to
                // compare it against.
                let assignment_agent_id = if input.role == "auditor" {
                    let Some(reviewer_execution_id) = admission
                        .and_then(|admission| admission.expected_reviewer_execution_id.as_deref())
                    else {
                        return Err(DbError::Check(
                            "auditor execution requires the reviewer execution its admission \
                             selected before it can be matched to the reviewer assignment"
                                .to_owned(),
                        ));
                    };
                    Some(
                        sqlx::query_scalar::<_, String>(
                            "SELECT agent_id FROM execution
                         WHERE id = ? AND agent_id IS NOT NULL",
                        )
                        .bind(reviewer_execution_id)
                        .fetch_optional(&mut **transaction)
                        .await?
                        .ok_or(DbError::VersionConflict)?,
                    )
                } else {
                    input.agent_id.clone()
                };
                let assignee_type: Option<String> = role_assignment.try_get("assignee_type")?;
                let assignee_id: Option<String> = role_assignment.try_get("assignee_id")?;
                let matches_agent = assignee_type.as_deref() == Some("agent")
                    && assignee_id.as_deref() == assignment_agent_id.as_deref();
                let matches_user =
                    assignee_type.as_deref() == Some("user") && input.agent_id.is_none();
                if !matches_agent && !matches_user {
                    return Err(DbError::Check(format!(
                        "running execution principal does not match role assignment for {}",
                        assignment_role
                    )));
                }
            }
            if let Some(workspace_id) = input.workspace_id.as_deref() {
                let running_execution = sqlx::query_as::<_, (String, String)>(
                    "SELECT role, id FROM execution
                     WHERE workspace_id = ? AND status = 'running'
                     ORDER BY created_at DESC, id DESC LIMIT 1",
                )
                .bind(workspace_id)
                .fetch_optional(&mut **transaction)
                .await?;
                if let Some((running_role, running_execution_id)) = running_execution {
                    // The public scope names the occupied resource slot, not
                    // the incoming workflow role. A Workspace is the shared
                    // repository slot; an interactive occupant keeps the
                    // dedicated interactive scope.
                    let scope = if running_role == "interactive" {
                        "interactive"
                    } else {
                        "repository"
                    };
                    return Err(DbError::ExecutionAlreadyRunning {
                        scope: scope.to_owned(),
                        execution_id: running_execution_id,
                    });
                }
            }

            // Capacity is an insertion invariant, not a dispatcher hint.
            // Recheck the selected identity's pause/current max, its running
            // Task executions, and the effective machine run cap while
            // this BEGIN IMMEDIATE transaction still owns SQLite's writer lock.
            // This is intentionally outside the workspace branch so Task
            // claims, which may not have a Workspace row yet, use the same
            // authoritative rule as role/recovery launches.
            self.ensure_agent_execution_capacity_in_tx(transaction, input, admission)
                .await?;
        }
        if admission.and_then(|a| a.purpose)
            == Some(api_types::ExecutionPurpose::AutomaticReviewRecovery)
        {
            let settings: String = sqlx::query_scalar(
                "SELECT settings FROM project WHERE id=(SELECT project_id FROM task WHERE id=?)",
            )
            .bind(&input.task_id)
            .fetch_one(&mut **transaction)
            .await?;
            let maximum = crate::budget::recovery_limit(&settings);
            crate::budget::apply(
                transaction,
                &input.task_id,
                crate::budget::Mutation::Charge {
                    key: crate::budget::Kind::AutomaticReviewRecovery.key().into(),
                    limit: maximum,
                    step: input.id.clone(),
                },
            )
            .await?;
        }
        let stop_reason = input.stop_reason.as_ref().map(ToString::to_string);
        let resume_policy = input.resume_policy.as_ref().map(ToString::to_string);
        let prompt = input.summary.as_deref();
        sqlx::query(
            "INSERT INTO execution (id, task_id, agent_id, role, status, stop_reason, stopped_by, resume_policy, stopped_at, parent_execution_id, agent_session_id, agent_message_id, last_activity_at, prompt, summary, logs_path, before_sha, after_sha, error, executor_config_snapshot_json, workspace_id, created_at, updated_at, purpose) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&input.id)
        .bind(&input.task_id)
        .bind(input.agent_id.as_deref())
        .bind(&input.role)
        .bind(input.status.to_string())
        .bind(stop_reason.as_deref())
        .bind(input.stopped_by.as_deref())
        .bind(resume_policy.as_deref())
        .bind(input.stopped_at.as_deref())
        .bind(input.parent_execution_id.as_deref())
        .bind(input.agent_session_id.as_deref())
        .bind(input.agent_message_id.as_deref())
        .bind(input.last_activity_at.as_deref())
        .bind(prompt)
        .bind(input.summary.as_deref())
        .bind(input.logs_path.as_deref())
        .bind(input.before_sha.as_deref())
        .bind(input.after_sha.as_deref())
        .bind(input.error.as_deref())
        .bind(input.executor_config_snapshot_json.as_deref())
        .bind(input.workspace_id.as_deref())
        .bind(&input.created_at)
        .bind(&input.updated_at)
        .bind(admission.and_then(|a| a.purpose).map(api_types::ExecutionPurpose::as_str))
        .execute(&mut **transaction)
        .await?;
        if input.status == ExecutionStatus::Running {
            sqlx::query(
                "UPDATE task SET metadata_json = json_remove(metadata_json, '$.dispatch_disposition')
                 WHERE id = ? AND json_valid(metadata_json)
                   AND json_extract(metadata_json, '$.dispatch_disposition.capability') IN ('machine_capacity', 'project_capacity')",
            )
            .bind(&input.task_id)
            .execute(&mut **transaction)
            .await?;
            // Admission clears are SQL-computed: sync the condition shadow.
            // The recovery-intent consume below only runs for a Running
            // admission too, so one sync after both covers them.
        }

        if let Some(recovery_id) = admission
            .filter(|_| input.status == ExecutionStatus::Running)
            .and_then(|admission| admission.expected_queued_recovery_id.as_deref())
        {
            // Consume accepted recovery intent with admission, so a crash
            // after INSERT cannot replay it into a second execution. Ordinary
            // interactive/role launches carry no replay authority.
            sqlx::query(
                "UPDATE task
                 SET metadata_json = NULLIF(json_remove(metadata_json, '$.queued_recovery', '$.deferred_dispatch'), '{}')
                 WHERE id = ? AND json_valid(metadata_json)
                   AND json_extract(metadata_json, '$.queued_recovery.id') = ?",
            )
            .bind(&input.task_id)
            .bind(recovery_id)
            .execute(&mut **transaction)
            .await?;
        }
        // Only a Running admission changes a condition: it may become the
        // Task's running owner, and its metadata clears above are legacy
        // writes. An interactive session is never the witnessed execution,
        // and an execution inserted already settled is history.
        if input.status == ExecutionStatus::Running {
            let change = if input.role == "interactive" {
                crate::ConditionChange::Legacy
            } else {
                crate::ConditionChange::Execution
            };
            crate::task_condition::produce(transaction, &input.task_id, change).await?;
        }
        if let Some(workspace_id) = input.workspace_id.as_deref() {
            crate::task_condition::execution_joined_workspace(
                transaction,
                &input.task_id,
                workspace_id,
            )
            .await?;
        }

        let row = sqlx::query("SELECT * FROM execution WHERE id = ?")
            .bind(&input.id)
            .fetch_one(&mut **transaction)
            .await?;
        map_execution(row)
    }

    /// Re-check the current approved Charter in the transaction that mutates
    /// Task/Execution state. The service-level admission query is intentionally
    /// only an early side-effect filter; it cannot be the authority because a
    /// Charter may be superseded between that query and claim/launch.
    pub(super) async fn ensure_execution_admission_in_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        task_id: &str,
        workspace_id: &str,
    ) -> Result<()> {
        let blocked: Option<i64> = sqlx::query_scalar(
            "SELECT CASE
                        WHEN p.primary_repo_id IS NULL
                          OR NOT EXISTS (
                              SELECT 1 FROM repo r
                              WHERE r.id = p.primary_repo_id
                                AND r.project_id = p.id
                          )
                          OR NOT EXISTS (
                              SELECT 1 FROM workspace w
                              WHERE w.id = ? AND w.repo_id = p.primary_repo_id
                          )
                        THEN 1
                        WHEN p.charter_status = 'charter_backed'
                          AND p.charter_setup_required = 0
                          AND (
                              p.current_charter_revision_id IS NULL
                              OR g.charter_revision_id IS NULL
                              OR g.charter_revision_id != p.current_charter_revision_id
                          )
                        THEN 1
                        ELSE 0
                    END
             FROM task t
             JOIN project p ON p.id = t.project_id
             LEFT JOIN project_task_governance g
               ON g.task_id = t.id AND g.project_id = p.id
             WHERE t.id = ?",
        )
        .bind(workspace_id)
        .bind(task_id)
        .fetch_optional(&mut **transaction)
        .await?;
        if blocked == Some(1) {
            return Err(DbError::InvalidTransition);
        }
        Ok(())
    }

    /// Re-read the Task facts that selected a role execution while holding the
    /// same writer transaction that will insert the Running row.  The service
    /// reads are useful preflight filters, but only this compare-and-admit
    /// boundary can close the final-read-to-insert race.
    pub(super) async fn ensure_task_execution_admission_in_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        input: &CreateExecution,
        admission: &ExecutionAdmission,
    ) -> Result<()> {
        let task_id = &input.task_id;
        let row = sqlx::query(
            "SELECT t.version, t.status, t.parent_task_id, t.metadata_json,
                    p.version AS project_version,
                    p.workflow_definition
             FROM task AS t
             JOIN project AS p ON p.id = t.project_id
             WHERE t.id = ? AND t.deleted_at IS NULL",
        )
        .bind(task_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(DbError::NotFound)?;
        let actual_version: i64 = row.try_get("version")?;
        let actual_status: String = row.try_get("status")?;
        if (!crate::task_writer::owns_task(task_id)
            && actual_version != admission.expected_task_version)
            || actual_status != admission.expected_task_status
        {
            return Err(DbError::VersionConflict);
        }
        let metadata_json: Option<String> = row.try_get("metadata_json")?;
        let metadata = TaskMetadata::parse(metadata_json.as_deref())
            .map_err(|error| DbError::Check(format!("invalid task metadata: {error}")))?;
        if let Some(expected) = admission.expected_queued_recovery_id.as_deref() {
            if metadata
                .extra
                .get("queued_recovery")
                .and_then(|queued| queued.get("id"))
                .and_then(serde_json::Value::as_str)
                != Some(expected)
            {
                return Err(DbError::VersionConflict);
            }
        }
        if let Some(claim) = metadata.extra.get("plan_publication_claim") {
            let claim_state = claim
                .get("state")
                .and_then(serde_json::Value::as_str)
                .ok_or(DbError::VersionConflict)?;
            let claim_state_entry_token = match claim.get("state_entry_token") {
                None | Some(serde_json::Value::Null) => None,
                Some(serde_json::Value::String(token)) if !token.is_empty() => Some(token.as_str()),
                _ => return Err(DbError::VersionConflict),
            };
            if claim_state == actual_status {
                let current_state_entry_token = sqlx::query_scalar::<_, String>(
                    "SELECT id FROM transition_log
                     WHERE task_id = ? AND to_state = ?
                     ORDER BY created_at DESC, rowid DESC LIMIT 1",
                )
                .bind(task_id)
                .bind(&actual_status)
                .fetch_optional(&mut **transaction)
                .await?;
                if claim_state_entry_token == current_state_entry_token.as_deref() {
                    return Err(DbError::VersionConflict);
                }
            }
        }
        if let Some(expected_project_version) = admission.expected_project_version {
            let actual_project_version: i64 = row.try_get("project_version")?;
            if actual_project_version != expected_project_version {
                return Err(DbError::VersionConflict);
            }
        }

        let parent_task_id: Option<String> = row.try_get("parent_task_id")?;
        let workflow_is_inherited =
            parent_task_id.is_some() && inherited_subtask_workflow_state(&actual_status);
        let workflow_definition: String = row.try_get("workflow_definition")?;
        let actual_workflow_definition =
            (!workflow_is_inherited).then_some(workflow_definition.as_str());
        let synthetic_ci_review = canonical_execution_role(Some(input.role.as_str()))
            == Some("reviewer")
            && input.agent_id.is_none()
            && admission.expected_effective_role.is_none()
            && admission.expected_assignment_id.is_none();
        if admission.expected_effective_role.is_none()
            && input.role != "interactive"
            && !synthetic_ci_review
        {
            return Err(DbError::VersionConflict);
        }
        // The workflow definition is pinned by whoever let the workflow choose
        // the role. An interactive execution is the user reaching into the
        // workspace directly, so it owes no pin -- and requiring one rejected
        // every interactive execution in a Project that has a workflow
        // definition, which is every Project, as a bare version conflict. A
        // pin an admission does carry is still compared.
        let pin_required = !workflow_is_inherited && input.role != "interactive";
        match admission.expected_workflow_definition.as_deref() {
            Some(expected) => {
                if Some(expected) != actual_workflow_definition {
                    return Err(DbError::VersionConflict);
                }
            }
            None => {
                if pin_required {
                    return Err(DbError::VersionConflict);
                }
            }
        }

        // Dependency edges are independent rows and do not advance Task
        // version. Re-read them under this same writer transaction so a
        // dependency added after service preflight cannot admit stale work.
        let unsatisfied_dependencies =
            Self::unsatisfied_dependencies_in_tx(transaction, task_id).await?;
        if !unsatisfied_dependencies.is_empty() {
            let agent_id = input.agent_id.as_deref().ok_or(DbError::DependencyGate)?;
            let context_holder_match = sqlx::query_scalar::<_, i64>(
                "SELECT EXISTS(
                     SELECT 1
                     FROM execution
                     WHERE task_id IN (SELECT depends_on_id FROM task_dependency WHERE task_id = ?)
                       AND role = 'executor'
                       AND agent_id = ?
                 )",
            )
            .bind(task_id)
            .bind(agent_id)
            .fetch_one(&mut **transaction)
            .await?
                != 0;
            if !context_holder_match {
                return Err(DbError::DependencyGate);
            }
        }

        let Some(expected_role) = admission.expected_effective_role.as_deref() else {
            return Ok(());
        };
        if canonical_execution_role(Some(input.role.as_str()))
            != canonical_execution_role(Some(expected_role))
        {
            return Err(DbError::VersionConflict);
        }
        let actual_role = if workflow_is_inherited {
            inherited_subtask_workflow_role(&actual_status).map(str::to_owned)
        } else {
            let raw = workflow_definition.trim();
            if raw.is_empty() || raw == "{}" {
                default_workflow_role(&actual_status).map(str::to_owned)
            } else {
                let workflow = serde_json::from_str::<api_types::WorkflowDefinition>(raw)
                    .map_err(|_| DbError::VersionConflict)?;
                workflow
                    .states
                    .iter()
                    .find(|state| state.name == actual_status)
                    .and_then(|state| {
                        state.role.as_deref().or_else(|| {
                            (state.kind == api_types::StateKind::Active).then_some("assignee")
                        })
                    })
                    .map(str::to_owned)
            }
        };
        if canonical_execution_role(actual_role.as_deref())
            != canonical_execution_role(Some(expected_role))
        {
            return Err(DbError::VersionConflict);
        }

        // A role decision is inseparable from the principal assignment that
        // produced it.  Require the exact assignment row and its last-write
        // timestamp that the dispatcher observed; a missing, replaced, or
        // reassigned principal therefore fails closed inside the INSERT
        // transaction. `executor` remains the historical alias for coder and
        // `auditor` is a child of the reviewer assignment.
        let assignment_role =
            canonical_execution_role(Some(input.role.as_str())).unwrap_or(input.role.as_str());
        // Auditor work uses a separately selected Agent, while the reviewer
        // Task assignment remains the authority for the Review attempt. Bind
        // that assignment check to the reviewer execution recorded in the
        // admission snapshot instead of requiring reviewer_agent == auditor_agent.
        let assignment_agent_id = if input.role == "auditor" {
            let reviewer_execution_id = admission
                .expected_reviewer_execution_id
                .as_deref()
                .ok_or(DbError::VersionConflict)?;
            Some(
                sqlx::query_scalar::<_, String>(
                    "SELECT agent_id FROM execution
                 WHERE id = ? AND agent_id IS NOT NULL",
                )
                .bind(reviewer_execution_id)
                .fetch_optional(&mut **transaction)
                .await?
                .ok_or(DbError::VersionConflict)?,
            )
        } else {
            input.agent_id.clone()
        };
        let assignment =
            task::effective_execution_assignment_in_tx(transaction, task_id, assignment_role)
                .await?;
        if let Some(assignment) = assignment {
            let actual_assignment_id: String = assignment.try_get("id")?;
            let actual_assignment_updated_at: String = assignment.try_get("updated_at")?;
            if admission.expected_assignment_id.as_deref() != Some(actual_assignment_id.as_str())
                || admission.expected_assignment_updated_at.as_deref()
                    != Some(actual_assignment_updated_at.as_str())
            {
                return Err(DbError::VersionConflict);
            }
            let assignee_type: Option<String> = assignment.try_get("assignee_type")?;
            let assignee_id: Option<String> = assignment.try_get("assignee_id")?;
            let assignment_matches = (assignee_type.as_deref() == Some("agent")
                && assignee_id.as_deref() == assignment_agent_id.as_deref())
                || (assignee_type.as_deref() == Some("user") && input.agent_id.is_none());
            if !assignment_matches {
                return Err(DbError::VersionConflict);
            }
        } else {
            return Err(DbError::VersionConflict);
        }
        if canonical_execution_role(Some(input.role.as_str())) == Some("reviewer") {
            let latest_review = sqlx::query_as::<
                _,
                (
                    String,
                    String,
                    Option<String>,
                    Option<String>,
                    i64,
                    String,
                    String,
                ),
            >(
                "SELECT id, execution_id, reviewer_execution_id,
                        auditor_execution_id, attempt_number, status, updated_at
                 FROM review
                 WHERE task_id = ?
                 ORDER BY attempt_number DESC, id DESC
                 LIMIT 1",
            )
            .bind(task_id)
            .fetch_optional(&mut **transaction)
            .await?;
            let actual_review = latest_review;
            let expected_review = match (
                admission.expected_reviewer_id.as_ref(),
                admission
                    .expected_latest_review_candidate_execution_id
                    .as_deref(),
                admission.expected_reviewer_execution_id.as_deref(),
                admission.expected_auditor_execution_id.as_deref(),
                admission.expected_reviewer_attempt_number,
                admission.expected_reviewer_status.as_ref(),
                admission.expected_reviewer_updated_at.as_ref(),
            ) {
                (
                    Some(id),
                    Some(snapshot_candidate_execution_id),
                    expected_reviewer_execution_id,
                    expected_auditor_execution_id,
                    Some(attempt_number),
                    Some(status),
                    Some(updated_at),
                ) => Some((
                    id.as_str(),
                    snapshot_candidate_execution_id,
                    expected_reviewer_execution_id,
                    expected_auditor_execution_id,
                    attempt_number,
                    status.as_str(),
                    updated_at.as_str(),
                )),
                // A first review attempt has no prior Review row to
                // snapshot, but still binds the new child to its candidate
                // execution. The candidate is checked separately below.
                (None, None, None, None, None, None, None) => None,
                _ => return Err(DbError::VersionConflict),
            };
            let actual_review = actual_review.as_ref().map(
                |(
                    id,
                    snapshot_candidate_execution_id,
                    reviewer_execution_id,
                    auditor_execution_id,
                    attempt_number,
                    status,
                    updated_at,
                )| {
                    (
                        id.as_str(),
                        snapshot_candidate_execution_id.as_str(),
                        reviewer_execution_id.as_deref(),
                        auditor_execution_id.as_deref(),
                        *attempt_number,
                        status.as_str(),
                        updated_at.as_str(),
                    )
                },
            );
            let fresh_attempt_without_previous_review = expected_review.is_none()
                && admission
                    .expected_latest_review_candidate_execution_id
                    .is_none()
                && admission.expected_reviewer_parent_execution_id.is_some();
            if (fresh_attempt_without_previous_review && actual_review.is_some())
                || (!fresh_attempt_without_previous_review && actual_review != expected_review)
                || input.parent_execution_id != admission.expected_reviewer_parent_execution_id
            {
                return Err(DbError::VersionConflict);
            }
        }
        Ok(())
    }

    async fn ensure_agent_execution_capacity_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: &CreateExecution,
        admission: Option<&ExecutionAdmission>,
    ) -> Result<()> {
        let Some(agent_id) = input.agent_id.as_deref() else {
            return Ok(());
        };
        let agent_row = sqlx::query(
            "SELECT max_concurrent_tasks, daemon_id, paused, version
             FROM agent_current
             WHERE id = ?",
        )
        .bind(agent_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(DbError::NotFound)?;
        let paused: i64 = agent_row.try_get("paused")?;
        if paused != 0 {
            return Err(DbError::AgentPaused {
                agent_id: agent_id.to_owned(),
            });
        }
        let actual_version: i64 = agent_row.try_get("version")?;
        let actual_max: i64 = agent_row.try_get("max_concurrent_tasks")?;
        if let Some(admission) = admission {
            let expected_version = admission
                .expected_agent_version
                .ok_or(DbError::VersionConflict)?;
            if actual_version != expected_version {
                return Err(DbError::VersionConflict);
            }
            let expected_max = admission
                .expected_agent_max_concurrent_tasks
                .ok_or(DbError::VersionConflict)?;
            if actual_max != expected_max {
                return Err(DbError::VersionConflict);
            }
        }
        let occupied_slots = crate::machine_capacity::count_agent_capacity(transaction, agent_id)
            .await?
            .occupied_slots();
        if occupied_slots >= actual_max {
            return Err(DbError::AgentAtCapacity);
        }

        let daemon_id = match input.workspace_id.as_deref() {
            Some(workspace_id) => sqlx::query_scalar::<_, Option<String>>(
                "SELECT COALESCE(execution_daemon_id, daemon_id)
                 FROM workspace_placement WHERE workspace_id = ?",
            )
            .bind(workspace_id)
            .fetch_optional(&mut **transaction)
            .await?
            .flatten(),
            None => input
                .executor_config_snapshot_json
                .as_deref()
                .and_then(|raw| serde_json::from_str::<serde_json::Value>(raw).ok())
                .and_then(|snapshot| {
                    snapshot
                        .get("daemon_id")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                })
                .or(agent_row.try_get::<Option<String>, _>("daemon_id")?),
        };
        let daemon =
            match daemon_id.as_deref() {
                Some(id) => sqlx::query(
                    "SELECT machine_id, max_concurrent_runs, run_limit FROM daemon WHERE id = ?",
                )
                .bind(id)
                .fetch_optional(&mut **transaction)
                .await?,
                None => None,
            };
        let server_host = daemon_id.is_none()
            || daemon.as_ref().is_some_and(|row| {
                row.try_get::<String, _>("machine_id")
                    .is_ok_and(|id| id == self.server_run_cap.embedded_machine_id())
            });
        let cap = if server_host {
            crate::machine_capacity::server_machine_cap(self, transaction).await?
        } else {
            crate::machine_capacity::effective_machine_cap(
                daemon
                    .as_ref()
                    .map(|row| row.try_get("max_concurrent_runs"))
                    .transpose()?
                    .flatten(),
                daemon
                    .as_ref()
                    .map(|row| row.try_get("run_limit"))
                    .transpose()?
                    .flatten(),
            )
        };
        let machine = if server_host {
            None
        } else {
            daemon_id.as_deref()
        };
        let embedded = self.server_run_cap.embedded_machine_id();
        let capacity =
            crate::machine_capacity::count_machine_capacity(transaction, machine, cap, &embedded)
                .await?;
        // An aged run-slot waiter stays behind the checks queued before it
        // began to wait, and no longer behind the ones queued since.
        let capacity = crate::machine_capacity::capacity_for_task(
            transaction,
            capacity,
            machine,
            &input.task_id,
            &embedded,
        )
        .await?;
        if !capacity.admits_execution() {
            return Err(DbError::MachineAtCapacity);
        }
        Ok(())
    }

    /// Bind a reviewer or conformance-auditor execution to the exact Review
    /// attempt whose authority admitted it. The Review snapshot and this
    /// execution INSERT share the same BEGIN IMMEDIATE transaction, so a
    /// newer rerun cannot steal the slot between preflight and persistence.
    /// Existing bindings may only be replaced after their execution is
    /// terminal; a live execution remains the sole owner of its Review role.
    pub(super) async fn bind_role_execution_to_review_in_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        input: &CreateExecution,
        admission: Option<&ExecutionAdmission>,
    ) -> Result<()> {
        if !matches!(input.role.as_str(), "reviewer" | "auditor") {
            return Ok(());
        }
        let admission = admission.ok_or(DbError::VersionConflict)?;
        let review_id = admission
            .expected_reviewer_id
            .as_deref()
            .ok_or(DbError::VersionConflict)?;
        let row = sqlx::query(
            "SELECT task_id, execution_id, reviewer_execution_id,
                    auditor_execution_id, attempt_number, status, updated_at
             FROM review
             WHERE id = ?",
        )
        .bind(review_id)
        .fetch_optional(&mut **transaction)
        .await?
        .ok_or(DbError::VersionConflict)?;
        let task_id: String = row.try_get("task_id")?;
        let candidate_execution_id: String = row.try_get("execution_id")?;
        let reviewer_execution_id: Option<String> = row.try_get("reviewer_execution_id")?;
        let auditor_execution_id: Option<String> = row.try_get("auditor_execution_id")?;
        let attempt_number: i64 = row.try_get("attempt_number")?;
        let status: String = row.try_get("status")?;
        let updated_at: String = row.try_get("updated_at")?;
        validate_review_candidate_in_tx(transaction, &task_id, &candidate_execution_id).await?;
        let role_binding_shape_valid = if input.role == "reviewer" {
            // A reviewer may bind an unclaimed Review or replace a terminal
            // reviewer execution, but an already-owned auditor attempt means
            // this Review is no longer available for a new reviewer owner.
            auditor_execution_id.is_none()
        } else {
            // An auditor is subordinate to the reviewer execution for this
            // exact attempt. It may first-bind or replace only a terminal
            // auditor execution; the reviewer binding must remain present.
            reviewer_execution_id.is_some()
        };
        if task_id != input.task_id
            || candidate_execution_id
                != admission
                    .expected_reviewer_parent_execution_id
                    .as_deref()
                    .ok_or(DbError::VersionConflict)?
            || !role_binding_shape_valid
            || admission.expected_reviewer_attempt_number != Some(attempt_number)
            || admission.expected_reviewer_status.as_deref() != Some(status.as_str())
            || admission.expected_reviewer_updated_at.as_deref() != Some(updated_at.as_str())
            || admission.expected_reviewer_execution_id.as_deref()
                != reviewer_execution_id.as_deref()
            || admission.expected_auditor_execution_id.as_deref() != auditor_execution_id.as_deref()
            || status != "running"
        {
            return Err(DbError::VersionConflict);
        }
        let (current_binding, binding_column) = if input.role == "reviewer" {
            (reviewer_execution_id.as_deref(), "reviewer_execution_id")
        } else {
            (auditor_execution_id.as_deref(), "auditor_execution_id")
        };
        if let Some(existing_execution_id) = current_binding {
            let existing_status =
                sqlx::query_scalar::<_, String>("SELECT status FROM execution WHERE id = ?")
                    .bind(existing_execution_id)
                    .fetch_optional(&mut **transaction)
                    .await?
                    .ok_or(DbError::VersionConflict)?;
            if existing_status == "running" {
                return Err(DbError::VersionConflict);
            }
        }
        let result = if binding_column == "reviewer_execution_id" {
            sqlx::query(
                "UPDATE review
                 SET reviewer_execution_id = ?
                 WHERE id = ? AND attempt_number = ? AND status = ? AND updated_at = ?
                   AND ((reviewer_execution_id IS NULL AND ? IS NULL)
                        OR reviewer_execution_id = ?)",
            )
            .bind(&input.id)
            .bind(review_id)
            .bind(attempt_number)
            .bind(&status)
            .bind(&updated_at)
            .bind(current_binding)
            .bind(current_binding)
            .execute(&mut **transaction)
            .await?
        } else {
            if reviewer_execution_id.is_none() {
                return Err(DbError::VersionConflict);
            }
            sqlx::query(
                "UPDATE review
                 SET auditor_execution_id = ?
                 WHERE id = ? AND attempt_number = ? AND status = ? AND updated_at = ?
                   AND ((auditor_execution_id IS NULL AND ? IS NULL)
                        OR auditor_execution_id = ?)",
            )
            .bind(&input.id)
            .bind(review_id)
            .bind(attempt_number)
            .bind(&status)
            .bind(&updated_at)
            .bind(current_binding)
            .bind(current_binding)
            .execute(&mut **transaction)
            .await?
        };
        if result.rows_affected() != 1 {
            return Err(DbError::VersionConflict);
        }
        Ok(())
    }

    pub(super) async fn unsatisfied_dependencies_in_tx(
        transaction: &mut Transaction<'_, Sqlite>,
        task_id: &str,
    ) -> Result<Vec<String>> {
        let rows = sqlx::query_scalar::<_, String>(
            "SELECT depends_on_id FROM task_dependency WHERE task_id = ? AND depends_on_id NOT IN (SELECT id FROM task WHERE status = 'done')",
        )
        .bind(task_id)
        .fetch_all(&mut **transaction)
        .await?;
        Ok(rows)
    }
}
