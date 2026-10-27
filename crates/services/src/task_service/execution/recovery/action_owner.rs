//! Stored action normalization and queued recovery owner reconciliation.

use super::*;
use db::task_condition::channels;

impl TaskService {
    /// Read historical persisted commands at the storage boundary. No old
    /// action name is accepted by a command endpoint or transport schema.
    pub(super) async fn normalize_stored_task_action(&self, task: &Task) -> Result<Task> {
        if crate::deferred_dispatch::queued_recovery(task).is_some() {
            return Ok(task.clone());
        }
        let metadata = db::TaskMetadata::parse(task.metadata_json.as_deref())
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        let Some(raw) = metadata
            .extra
            .get(crate::deferred_dispatch::QUEUED_RECOVERY_KEY)
        else {
            return Ok(task.clone());
        };
        if raw
            .get("request")
            .and_then(|request| request.get("offer"))
            .is_some()
        {
            return Ok(task.clone());
        }
        raw.get("id")
            .and_then(Value::as_str)
            .ok_or_else(|| ServiceError::invalid_operation("stored queued intent has no id"))?;
        let request = &raw["request"];
        let saved_reason = request
            .get("reason")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let guidance = request
            .get("context")
            .or_else(|| request.get("resume_reason"))
            .and_then(Value::as_str)
            .map(str::to_owned);
        let name = request
            .get("action")
            .and_then(Value::as_str)
            .unwrap_or("resume_session");
        let actor = Actor::user(UserActionSource::Api);
        let mut snapshot = self.task_action_snapshot(&task.id, &actor).await?;
        snapshot.task.error_annotation = raw
            .get("error_annotation")
            .and_then(Value::as_str)
            .map(str::to_owned);
        snapshot.task.blocked_json = raw
            .get("blocked_json")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let mut projection = metadata.clone();
        projection
            .extra
            .remove(crate::deferred_dispatch::QUEUED_RECOVERY_KEY);
        snapshot.task.metadata_json = Some(
            serde_json::to_string(&projection)
                .map_err(|error| ServiceError::invalid_operation(error.to_string()))?,
        );
        // The offers below are read from the condition: project it from the
        // fields as they were before the intent was queued.
        snapshot.task.condition = db::compose_condition(&db::ConditionInput::from(&snapshot.task));
        let action = match name {
            "reset_to_initial" => Some(api_types::TaskAction::Restart {
                reason: saved_reason.clone(),
            }),
            "cancel_task" => Some(api_types::TaskAction::Cancel {
                reason: saved_reason.clone(),
            }),
            "mark_reviewed" | "skip_hook_once" => Some(api_types::TaskAction::Approve {
                reason: saved_reason.clone(),
                override_checks: Some(true),
            }),
            "defer_to_follow_up" => Some(api_types::TaskAction::Approve {
                reason: saved_reason.clone(),
                override_checks: Some(false),
            }),
            "resume_process"
                if crate::workflow::is_review_state(&snapshot.workflow, &snapshot.task.status) =>
            {
                Some(api_types::TaskAction::SendBack {
                    guidance: guidance
                        .clone()
                        .or_else(|| saved_reason.clone())
                        .unwrap_or_default(),
                })
            }
            "resume_session"
            | "reexecute"
            | "retry_hook"
            | "resume_process"
            | "update_workspace_and_retry_hook"
            | "reset_retry_window"
            | "proceed_once" => Some(api_types::TaskAction::Retry {
                reason: saved_reason.clone(),
                fresh_session: match name {
                    "resume_session" => Some(false),
                    "reexecute" => Some(true),
                    _ => None,
                },
                refresh_workspace: (name == "update_workspace_and_retry_hook").then_some(true),
                reset_budget: match name {
                    "reset_retry_window" => Some(true),
                    "proceed_once" => Some(false),
                    _ => None,
                },
                guidance: guidance.clone(),
            }),
            _ => None,
        };
        let offers = crate::available_actions(&snapshot);
        let selected = action.as_ref().and_then(|action| {
            offers
                .into_iter()
                .find(|offer| offer.action.verb() == action.verb())
        });
        // Keep the original bytes as evidence even when the state/owner has
        // changed and the old pending command can no longer be represented.
        let saved = TaskRepo::mutate_metadata_and_bump_version(
            &*self.db,
            &task.id,
            task.version,
            vec![TaskMetadataMutation::Set {
                key: "task_action_migrated_intent".to_owned(),
                value: raw.clone(),
            }],
            &now_rfc3339(),
        )
        .await?;
        let same_state =
            raw.get("target_state").and_then(Value::as_str) == Some(task.status.as_str());
        if !same_state || selected.is_none() {
            let error = ServiceError::invalid_operation(
                "stored queued intent no longer has a current apply plan",
            );
            self.restore_failed_task_action(&saved, raw, &error).await?;
            return Err(error);
        }
        snapshot.task.version = saved.version;
        let offer = selected.expect("selected above");
        let mut action = action.expect("selected action");
        // Missing legacy defaults now come from the offer. Parameters absent
        // from the state are not carried into a different apply plan.
        if let api_types::TaskAction::Retry {
            reason: _,
            fresh_session,
            refresh_workspace,
            reset_budget,
            guidance,
        } = &mut action
        {
            if let api_types::TaskAction::Retry {
                fresh_session: fresh,
                refresh_workspace: refresh,
                reset_budget: reset,
                ..
            } = &offer.action
            {
                if !offer
                    .parameters
                    .iter()
                    .any(|parameter| parameter.name == "fresh_session")
                {
                    *fresh_session = *fresh;
                }
                if !offer
                    .parameters
                    .iter()
                    .any(|parameter| parameter.name == "refresh_workspace")
                {
                    *refresh_workspace = *refresh;
                }
                if !offer
                    .parameters
                    .iter()
                    .any(|parameter| parameter.name == "reset_budget")
                {
                    *reset_budget = *reset;
                }
                if !offer
                    .parameters
                    .iter()
                    .any(|parameter| parameter.name == "guidance")
                {
                    *guidance = None;
                }
            }
        }
        self.queue_task_action(
            &snapshot,
            offer,
            action.clone(),
            Actor::user(UserActionSource::Action(action)),
        )
        .await
    }

    pub(crate) async fn reconcile_task_action_owner(
        &self,
        task: Task,
        action: api_types::TaskAction,
    ) -> Result<Task> {
        let Some(mut placement) =
            db::WorkspacePlacementRepo::get_for_task(&*self.db, &task.id).await?
        else {
            return Ok(task);
        };
        if placement.state == db::PlacementState::Failed
            && placement.failure_cause == Some(db::PlacementFailureCause::OwnerDisconnectedTimeout)
        {
            let online = match crate::recovery::placement_execution_daemon_id(&placement) {
                Some(daemon_id) => {
                    if self
                        .daemon_connections
                        .as_ref()
                        .and_then(|registry| registry.get(daemon_id))
                        .is_some_and(|connection| connection.needs_upgrade())
                    {
                        return Err(ServiceError::DaemonUpgradeRequired {
                            daemon_id: daemon_id.to_owned(),
                        });
                    }
                    db::DaemonRepo::get_by_id(&*self.db, daemon_id)
                        .await?
                        .is_some_and(|daemon| daemon.status == db::DaemonStatus::Online)
                        && self
                            .daemon_connections
                            .as_ref()
                            .and_then(|registry| registry.get(daemon_id))
                            .and_then(|connection| connection.snapshot())
                            .is_some_and(|facts| {
                                placement.owner_kind == db::PlacementOwnerKind::Server
                                    || !facts.workspace_incapable
                            })
                }
                None => false,
            };
            if !online {
                return Ok(task);
            }
            let mut update = crate::placement::admission::placement_update(&placement);
            update.state = Some(db::PlacementState::Disconnected);
            update.disconnected_at = Some(Some(now_rfc3339()));
            placement = db::WorkspacePlacementRepo::update(&*self.db, update).await?;
        }
        let reconciled = if let Some(registry) = self.daemon_connections.as_ref() {
            registry.reconciliation_notify().notify_one();
            crate::recovery::reconcile_workspace_placement(
                &self.db,
                &self.event_bus,
                registry,
                Some(self),
                &placement,
            )
            .await?
        } else {
            false
        };
        let current = TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
        if reconciled
            && ExecutionRepo::list_running_by_task(&*self.db, &task.id)
                .await?
                .is_empty()
            && self
                .parse_blocking_annotation(&current)
                .is_some_and(|annotation| {
                    matches!(
                        annotation.blocking_reason.as_str(),
                        "owner_lost_execution" | "owner_disconnected_timeout"
                    )
                })
        {
            let actor = TaskService::task_action_actor(Actor::user(UserActionSource::Api));
            let mut snapshot = self.task_action_snapshot(&task.id, &actor).await?;
            snapshot.owner_disconnected = false;
            if let Some(offer) = crate::available_actions(&snapshot)
                .into_iter()
                .find(|offer| offer.action.verb() == "retry")
            {
                return self
                    .queue_task_action(&snapshot, offer, action, actor)
                    .await;
            }
        }
        self.publish_recovery_applied(&current, "retry", Some(&current.status), None);
        Ok(current)
    }

    /// Settle a refused or corrupt durable intent under its exact Task/marker CAS.
    /// The interruption event is appended by the repository transaction.
    pub(super) async fn restore_failed_task_action(
        &self,
        task: &Task,
        raw: &Value,
        error: &ServiceError,
    ) -> Result<()> {
        let current = TaskRepo::get_by_id(&*self.db, &task.id, false)
            .await?
            .ok_or_else(|| ServiceError::not_found("task", task.id.clone()))?;
        let metadata = db::TaskMetadata::parse(current.metadata_json.as_deref())
            .map_err(|error| ServiceError::invalid_operation(error.to_string()))?;
        if metadata
            .extra
            .get(crate::deferred_dispatch::QUEUED_RECOVERY_KEY)
            != Some(raw)
        {
            return Ok(());
        }
        let saved = |key: &str| raw.get(key).and_then(Value::as_str).map(str::to_owned);
        let owns_condition = !matches!(current.status.as_str(), "done" | "cancelled")
            && current.error_annotation.is_none()
            && current.blocked_json.is_none()
            && current.failed_json.is_none()
            && current.archived_at.is_none();
        let (annotation, blocked, failed) = if owns_condition {
            let reason = error.to_string();
            let annotation =
                restore_action_annotation(saved("error_annotation").as_deref(), &reason)
                    .unwrap_or_else(|| {
                        let mut annotation: Value = saved("error_annotation")
                            .as_deref()
                            .and_then(|s| serde_json::from_str(s).ok())
                            .filter(Value::is_object)
                            .unwrap_or_else(|| json!({"type":"recovery_required"}));
                        annotation
                            .as_object_mut()
                            .expect("object above")
                            .remove("recovery_actions");
                        annotation["blocking_reason"] = json!(reason);
                        annotation["message"] = json!(reason);
                        annotation.to_string()
                    });
            let blocked = restore_action_interruption(
                saved("blocked_json").as_deref(),
                &reason,
                api_types::FailureKind::RecoveryRequired,
            )
            .unwrap_or_else(|| {
                let mut blocked: Value = saved("blocked_json")
                    .as_deref()
                    .and_then(|s| serde_json::from_str(s).ok())
                    .filter(Value::is_object)
                    .unwrap_or_else(|| json!({"kind":"recovery_required"}));
                blocked
                    .as_object_mut()
                    .expect("object above")
                    .remove("recovery_actions");
                blocked["reason"] = json!(reason);
                blocked.to_string()
            });
            let failed = saved("failed_json").map(|s| {
                restore_action_interruption(
                    Some(&s),
                    &reason,
                    api_types::FailureKind::ExecutorFailed,
                )
                .unwrap_or_else(|| {
                    let mut value: Value = serde_json::from_str(&s)
                        .ok()
                        .filter(Value::is_object)
                        .unwrap_or_else(|| json!({"kind":"executor_failed"}));
                    if let Some(object) = value.as_object_mut() {
                        object.remove("recovery_actions");
                    }
                    value["reason"] = json!(reason);
                    value.to_string()
                })
            });
            (
                Some(annotation),
                if failed.is_none() {
                    Some(blocked)
                } else {
                    None
                },
                failed,
            )
        } else {
            (
                current.error_annotation.clone(),
                current.blocked_json.clone(),
                current.failed_json.clone(),
            )
        };
        TaskRepo::restore_queued_recovery(
            &*self.db,
            db::RestoreQueuedRecovery {
                task_id: current.id,
                expected_version: current.version,
                queued_recovery_id: raw
                    .get("id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                error_annotation: annotation,
                blocked_json: blocked,
                failed_json: failed,
                updated_at: now_rfc3339(),
            },
        )
        .await?;
        Ok(())
    }
}

fn is_json_object(raw: &str) -> bool {
    serde_json::from_str::<Value>(raw).is_ok_and(|value| value.is_object())
}

// Restore known channels through their closed shapes; historical extensions keep
// the original Value edit at the caller.
fn restore_action_annotation(raw: Option<&str>, reason: &str) -> Option<String> {
    let mut annotation = if !raw.is_some_and(is_json_object) {
        channels::ErrorAnnotation::new(channels::AnnotationShape::Reason(
            channels::ReasonAnnotation {
                blocking_reason: reason.into(),
                message: reason.into(),
                kind: api_types::FailureKind::RecoveryRequired,
            },
        ))
    } else {
        serde_json::from_str::<channels::ErrorAnnotation>(raw?).ok()?
    };
    use channels::AnnotationShape as S;
    match &mut annotation.shape {
        S::Blocking(value) => {
            value.blocking_reason = reason.into();
            value.message = Some(reason.into());
        }
        S::Hold(value) => {
            annotation.shape = S::RecoveryExecution(channels::RecoveryExecutionAnnotation {
                blocked_at: value.blocked_at.clone(),
                blocked_by: value.blocked_by.clone(),
                blocked_execution_id: value.blocked_execution_id.clone(),
                blocking_reason: reason.into(),
                message: reason.into(),
                kind: value.kind,
            });
        }
        S::TimedBlock(value) => {
            value.blocking_reason = reason.into();
            value.message = reason.into();
        }
        S::RecoveryExecution(value) => {
            value.blocking_reason = reason.into();
            value.message = reason.into();
        }
        S::RemoteCancel(value) => {
            value.blocking_reason = reason.into();
            value.message = reason.into();
        }
        S::Message(value) => {
            annotation.shape = S::Reason(channels::ReasonAnnotation {
                blocking_reason: reason.into(),
                message: reason.into(),
                kind: value.kind,
            });
        }
        S::Reason(value) => {
            value.blocking_reason = reason.into();
            value.message = reason.into();
        }
        S::ReviewCandidate(value) => {
            value.blocking_reason = reason.into();
            value.message = reason.into();
        }
        S::ReviewPark(value) => {
            value.blocking_reason = reason.into();
            value.message = reason.into();
        }
        S::Detected(value) => {
            annotation.shape = S::Budget(channels::BudgetAnnotation {
                blocking_reason: reason.into(),
                detected_at: value.detected_at.clone(),
                message: reason.into(),
                kind: value.kind,
            });
        }
        S::Budget(value) => {
            value.blocking_reason = reason.into();
            value.message = reason.into();
        }
        S::StepFailure(value) => {
            value.blocking_reason = Some(Some(reason.into()));
            value.message = Some(reason.into());
        }
        // These main restore outputs add blocking_reason to a shape that the
        // frozen gate-1 vocabulary cannot represent.
        S::DetectedState(_) | S::PlacementRefusal(_) | S::PlanWait(_) | S::UpgradeDispatch(_) => {
            return None
        }
    }
    Some(
        serde_json::to_value(annotation)
            .expect("condition channel serializes")
            .to_string(),
    )
}

fn restore_action_interruption(
    raw: Option<&str>,
    reason: &str,
    fallback: api_types::FailureKind,
) -> Option<String> {
    let mut interruption = if !raw.is_some_and(is_json_object) {
        channels::Interruption {
            reason: Some(reason.into()),
            kind: Some(Some(fallback.into())),
            ..Default::default()
        }
    } else {
        serde_json::from_str::<channels::Interruption>(raw?).ok()?
    };
    interruption.reason = Some(reason.into());
    Some(serde_json::to_string(&interruption).expect("condition channel serializes"))
}

#[cfg(test)]
mod writer_parity_tests;
