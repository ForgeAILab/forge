//! Durable witness projection shared by producers and invariant repair.
use super::*;
use serde_json::Value;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum ConditionWitness {
    Entry {
        task_id: String,
        epoch: i64,
        transition_id: Option<String>,
    },
    Step {
        step_id: String,
        epoch: i64,
    },
    Execution {
        execution_id: String,
        status: String,
    },
    Budget {
        key: String,
        window_id: String,
        spent: i64,
    },
    Operation {
        operation_id: String,
        generation: i64,
    },
    Child {
        task_id: String,
        status: String,
    },
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MaterialBlocker {
    pub requires_intervention: bool,
    pub interruption: Option<Value>,
}
/// Exactly today's event/Attention inputs. Witnesses, observations, leases and
/// new condition tags never enter this projection. No Attention reader switches.
pub fn material_blocker(condition: &TaskCondition) -> MaterialBlocker {
    condition
        .evidence()
        .material
        .clone()
        .unwrap_or(MaterialBlocker {
            requires_intervention: false,
            interruption: None,
        })
}
pub(super) fn legacy_material_blocker(
    a: Option<&str>,
    b: Option<&str>,
    f: Option<&str>,
) -> Option<MaterialBlocker> {
    if a.is_none() && b.is_none() && f.is_none() {
        return None;
    }
    let selected = f
        .map(|r| ("failed", r))
        .or_else(|| b.map(|r| ("blocked", r)))
        .or_else(|| a.map(|r| ("annotation", r)));
    let interruption = selected.map(|(source, raw)| {
        let value = crate::repository::parse_event_json_object(raw);
        crate::repository::event_interruption_details(source, &value)
    });
    Some(MaterialBlocker {
        requires_intervention: crate::task_interruption_requires_intervention(a, b, f),
        interruption,
    })
}
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConditionFacts {
    pub version: i64,
    pub task_id: String,
    pub state: String,
    pub epoch: i64,
    pub transition_id: Option<String>,
    pub since: String,
    pub terminal: Option<TerminalOutcome>,
    pub hooks: Option<String>,
    pub running: Option<(String, String)>,
    pub children_pending: bool,
    pub children: Vec<(String, i64, String)>,
    pub witnesses: Vec<ConditionWitness>,
}
impl TaskCondition {
    pub fn evidence(&self) -> &ConditionEvidence {
        match self {
            Self::Clear { evidence }
            | Self::Entering { evidence, .. }
            | Self::Running { evidence, .. }
            | Self::Deferred { evidence, .. }
            | Self::Parked { evidence, .. }
            | Self::Failed { evidence, .. }
            | Self::Settled { evidence, .. } => evidence,
        }
    }
    pub(super) fn evidence_mut(&mut self) -> &mut ConditionEvidence {
        match self {
            Self::Clear { evidence }
            | Self::Entering { evidence, .. }
            | Self::Running { evidence, .. }
            | Self::Deferred { evidence, .. }
            | Self::Parked { evidence, .. }
            | Self::Failed { evidence, .. }
            | Self::Settled { evidence, .. } => evidence,
        }
    }
}
impl ConditionFacts {
    pub async fn load(c: &mut SqliteConnection, id: &str) -> Result<Self> {
        let row=sqlx::query("SELECT t.version,t.status,t.status_epoch,t.created_at,t.parent_task_id,p.workflow_definition FROM task t JOIN project p ON p.id=t.project_id WHERE t.id=?")
            .bind(id).fetch_one(&mut *c).await?;
        let state: String = row.get("status");
        let definition: String = row.get("workflow_definition");
        let workflow: Value = serde_json::from_str(&definition).unwrap_or_default();
        let states = workflow["states"].as_array();
        let subtask = row.get::<Option<String>, _>("parent_task_id").is_some();
        let config = states.and_then(|states| states.iter().find(|s| s["name"] == state));
        let inherited = subtask
            && matches!(
                state.as_str(),
                "todo" | "in_progress" | "done" | "cancelled"
            );
        let terminal = if inherited || states.is_none_or(|s| s.is_empty()) {
            matches!(state.as_str(), "done" | "cancelled")
        } else {
            config.is_some_and(|s| s["kind"] == "terminal")
        };
        let mut facts = Self {
            version: row.get("version"),
            task_id: id.into(),
            state: state.clone(),
            epoch: row.get("status_epoch"),
            since: row.get("created_at"),
            terminal: terminal.then(|| {
                if state
                    == workflow["cancellation_state"]
                        .as_str()
                        .unwrap_or("cancelled")
                {
                    TerminalOutcome::Cancelled
                } else {
                    TerminalOutcome::Completed
                }
            }),
            ..Default::default()
        };
        // Reorder/recovery audit receipts share the entry epoch. The first
        // receipt in that epoch owns its original time; later audit rows do
        // not replace the execution's entry binding.
        let entry = sqlx::query_as::<_, (String, String)>("SELECT id,created_at FROM transition_log WHERE task_id=? AND to_state=? AND status_epoch=? ORDER BY created_at,id LIMIT 1")
            .bind(id).bind(&state).bind(facts.epoch).fetch_optional(&mut *c).await?;
        if let Some((entry, time)) = entry {
            facts.transition_id = Some(entry);
            if facts.epoch != 0 { facts.since = time; }
        } else if let Some((entry, time)) = sqlx::query_as::<_, (String, String)>("SELECT id,created_at FROM transition_log WHERE task_id=? AND to_state=? AND status_epoch IS NULL ORDER BY created_at DESC,id DESC LIMIT 1")
            .bind(id).bind(&state).fetch_optional(&mut *c).await? {
            facts.transition_id = Some(entry); facts.since = time;
        }
        // Exact status/epoch prevents old entries from fabricating hook ownership.
        if let Some((step,epoch))=sqlx::query_as::<_,(String,i64)>("SELECT id,expected_epoch FROM task_step WHERE task_id=? AND expected_status=? AND expected_epoch=? AND kind='hooks' AND status IN ('pending','claimed') ORDER BY seq LIMIT 1")
            .bind(id).bind(&state).bind(facts.epoch).fetch_optional(&mut *c).await? {
            facts.hooks=Some(step.clone());facts.witnesses.push(ConditionWitness::Step{step_id:step,epoch});
        }
        let executions=sqlx::query("SELECT id,role,status,execution_version,created_at,executor_config_snapshot_json FROM execution WHERE task_id=? AND role!='interactive' ORDER BY created_at DESC,id DESC LIMIT 1")
            .bind(id).fetch_optional(&mut *c).await?;
        if let Some(e) = executions {
            let eid: String = e.get("id");
            let erole: String = e.get("role");
            let status: String = e.get("status");
            let snapshot: Value = e
                .get::<Option<String>, _>("executor_config_snapshot_json")
                .as_deref()
                .and_then(|raw| serde_json::from_str(raw).ok())
                .unwrap_or_default();
            let bound = if let Some(token) = snapshot["state_entry_token"].as_str() {
                snapshot["task_state"] == facts.state &&
                    (facts.transition_id.as_deref() == Some(token) ||
                     sqlx::query_scalar::<_, bool>("SELECT EXISTS(SELECT 1 FROM transition_log WHERE id=? AND task_id=? AND to_state=? AND status_epoch=?)")
                        .bind(token).bind(id).bind(&state).bind(facts.epoch).fetch_one(&mut *c).await?)
            } else {
                e.get::<String, _>("created_at") >= facts.since
            };
            if bound {
                // The entry-bound execution is the durable owner. Its role
                // is captured from admission, not re-guessed from today's
                // Project definition (subtasks and custom workflows differ).
                if status == "running" {
                    facts.running = Some((eid.clone(), erole));
                }
                facts.witnesses.push(ConditionWitness::Execution {
                    execution_id: eid,
                    status,
                });
            }
        }
        let budgets=sqlx::query("SELECT kind,window_id,spent FROM task_budget WHERE task_id=? AND spent>0 ORDER BY kind").bind(id).fetch_all(&mut *c).await?;
        for b in budgets {
            facts.witnesses.push(ConditionWitness::Budget {
                key: b.get("kind"),
                window_id: b.get("window_id"),
                spent: b.get("spent"),
            });
        }
        let ops=sqlx::query("SELECT r.operation_id,r.generation FROM pending_remote_cancel r WHERE r.step_id IN (SELECT id FROM task_step WHERE task_id=?) OR r.workspace_id IN (SELECT id FROM workspace WHERE task_id=?) ORDER BY r.operation_id")
            .bind(id).bind(id).fetch_all(&mut *c).await?;
        for op in ops {
            facts.witnesses.push(ConditionWitness::Operation {
                operation_id: op.get("operation_id"),
                generation: op.get("generation"),
            });
        }
        let pending:Option<i64>=sqlx::query_scalar("SELECT CASE WHEN json_valid(metadata_json) THEN CASE WHEN json_type(metadata_json,'$.coordination_review_pending')='true' THEN 1 ELSE 0 END ELSE 0 END FROM task WHERE id=?").bind(id).fetch_one(&mut *c).await?;
        facts.children_pending = pending == Some(1);
        if facts.children_pending {
            facts.children=sqlx::query_as("SELECT id,version,status FROM task WHERE parent_task_id=? AND deleted_at IS NULL ORDER BY subtask_order,id").bind(id).fetch_all(&mut *c).await?;
            for (id, _, status) in &facts.children {
                facts.witnesses.push(ConditionWitness::Child {
                    task_id: id.clone(),
                    status: status.clone(),
                });
            }
        }
        Ok(facts)
    }
    pub fn apply(&self, mut condition: TaskCondition) -> TaskCondition {
        // Evidence on ignored data is not an authorization or parking decision.
        let interesting = !matches!(&condition,TaskCondition::Clear{evidence} if evidence==&ConditionEvidence::default());
        let remote_cancel = self
            .witnesses
            .iter()
            .any(|w| matches!(w, ConditionWitness::Operation { .. }));
        let lifecycle = remote_cancel
            || self.terminal.is_some()
            || self.hooks.is_some()
            || self.running.is_some()
            || self.children_pending;
        if interesting || lifecycle || self.transition_id.is_some() {
            let evidence = condition.evidence_mut();
            evidence.witnesses = self.witnesses.clone();
            evidence.witnesses.insert(
                0,
                ConditionWitness::Entry {
                    task_id: self.task_id.clone(),
                    epoch: self.epoch,
                    transition_id: self.transition_id.clone(),
                },
            );
        }
        let exhausted = match &condition {
            TaskCondition::Parked {
                primary,
                additional,
                ..
            } => std::iter::once(primary)
                .chain(additional)
                .any(|r| matches!(r, ParkReason::BudgetExhausted { .. })),
            TaskCondition::Failed {
                failure,
                additional,
                ..
            } => std::iter::once(failure)
                .chain(additional)
                .any(|r| matches!(r, ParkReason::BudgetExhausted { .. })),
            _ => false,
        };
        if !exhausted {
            condition
                .evidence_mut()
                .witnesses
                .retain(|w| !matches!(w, ConditionWitness::Budget { .. }));
        }
        let evidence = condition.evidence().clone();
        if let Some(outcome) = &self.terminal {
            return TaskCondition::Settled {
                outcome: outcome.clone(),
                evidence,
            };
        }
        let remote_reason = ParkReason::RemoteCancelPending {
            source: source(&LegacyConditionField::PendingRemoteCancel, None),
        };
        if condition.is_blocked() {
            if remote_cancel {
                match &mut condition {
                    TaskCondition::Parked {
                        primary,
                        additional,
                        ..
                    }
                    | TaskCondition::Failed {
                        failure: primary,
                        additional,
                        ..
                    } => {
                        if !std::iter::once(&*primary)
                            .chain(additional.iter())
                            .any(|r| matches!(r, ParkReason::RemoteCancelPending { .. }))
                        {
                            additional.push(remote_reason.clone());
                        }
                    }
                    _ => unreachable!(),
                }
            }
            if self.children_pending {
                let remaining = self
                    .children
                    .iter()
                    .filter(|(_, _, s)| !matches!(s.as_str(), "done" | "cancelled"))
                    .map(|(id, _, _)| id.clone())
                    .collect::<Vec<_>>();
                if !remaining.is_empty() {
                    let reason = ParkReason::Children {
                        root_id: self.task_id.clone(),
                        remaining,
                    };
                    match &mut condition {
                        TaskCondition::Parked { additional, .. }
                        | TaskCondition::Failed { additional, .. } => additional.push(reason),
                        _ => unreachable!(),
                    }
                }
            }
            return condition;
        }
        if remote_cancel {
            return TaskCondition::Parked {
                primary: remote_reason,
                additional: Vec::new(),
                resume: ConditionContinuation::Reconcile,
                since: Some(self.since.clone()),
                evidence,
            };
        }
        if let Some(step) = &self.hooks {
            return TaskCondition::Entering {
                state: self.state.clone(),
                epoch: self.epoch,
                step_id: step.clone(),
                phase: "post_commit_hooks".into(),
                since: self.since.clone(),
                evidence,
            };
        }
        if let Some((execution, role)) = &self.running {
            return TaskCondition::Running {
                execution_id: execution.clone(),
                role: role.clone(),
                epoch: self.epoch,
                since: self.since.clone(),
                evidence,
            };
        }
        if self.children_pending && self.children.is_empty() {
            return TaskCondition::Parked {
                primary: unknown(
                    &LegacyConditionField::MetadataJson,
                    Some("coordination_review_pending"),
                    UnknownConditionProblem::UnownedEntry,
                ),
                additional: Vec::new(),
                resume: ConditionContinuation::Reconcile,
                since: Some(self.since.clone()),
                evidence,
            };
        }
        if self.children_pending {
            let remaining = self
                .children
                .iter()
                .filter(|(_, _, s)| !matches!(s.as_str(), "done" | "cancelled"))
                .map(|(id, _, _)| id.clone())
                .collect::<Vec<_>>();
            if remaining.is_empty() {
                return TaskCondition::Deferred {
                    until: None,
                    reason: RetryCause::ChildrenReady,
                    resume: ConditionContinuation::AdvanceAggregateReview {
                        child_ids: self.children.iter().map(|(id, _, _)| id.clone()).collect(),
                    },
                    evidence,
                };
            }
            return TaskCondition::Parked {
                primary: ParkReason::Children {
                    root_id: self.task_id.clone(),
                    remaining,
                },
                additional: Vec::new(),
                resume: ConditionContinuation::Reconcile,
                since: Some(self.since.clone()),
                evidence,
            };
        }
        condition
    }
}
