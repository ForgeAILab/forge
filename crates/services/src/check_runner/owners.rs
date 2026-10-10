//! Owner routing is pinned in check storage; a daemon path is never opened here.
use super::{worker::CheckOwnerPort, *};
use api_types::*;
use async_trait::async_trait;
use db::{
    CheckDispatchIntent, CheckDispatchTarget, CheckWorkerRecord, CheckWorkerRepo,
    PlacementOwnerKind, PlacementState, SqliteDb, WorkspacePlacementRepo,
};
use std::{
    collections::{BTreeMap, HashMap},
    path::Path,
    sync::Mutex,
    time::{Duration, Instant},
};
use tokio_util::sync::CancellationToken;

struct ServerOperation {
    cancel: CancellationToken,
    result: Option<DaemonCheckResult>,
}
type Operations = Arc<Mutex<HashMap<String, ServerOperation>>>;
struct OperationGuard {
    operations: Operations,
    id: String,
}
impl Drop for OperationGuard {
    fn drop(&mut self) {
        let mut operations = self.operations.lock().unwrap_or_else(|p| p.into_inner());
        if let Some(operation) = operations.get_mut(&self.id) {
            if operation.result.is_none() {
                operation.cancel.cancel();
                operation.result = Some(DaemonCheckResult::Interrupted {
                    operation_id: self.id.clone(),
                });
            }
        }
    }
}

pub struct WorkspaceCheckOwners {
    db: Arc<SqliteDb>,
    client: crate::daemon_transport::workspace_client::DaemonWorkspaceClient,
    registry: Arc<crate::daemon_transport::DaemonConnectionRegistry>,
    runtime_id: String,
    operations: Operations,
    disconnect_bound: Duration,
}
impl WorkspaceCheckOwners {
    pub fn new(
        db: Arc<SqliteDb>,
        registry: Arc<crate::daemon_transport::DaemonConnectionRegistry>,
        disconnect_bound: Duration,
    ) -> Self {
        Self {
            db,
            client: crate::daemon_transport::workspace_client::DaemonWorkspaceClient::new(
                registry.clone(),
            ),
            registry,
            runtime_id: format!("server:{}", db::new_uuid_v4()),
            operations: Default::default(),
            disconnect_bound,
        }
    }
    fn intent<'a>(&self, record: &'a CheckWorkerRecord) -> Result<&'a CheckDispatchIntent> {
        record
            .dispatch
            .as_ref()
            .ok_or_else(|| ServiceError::invalid_operation("check owner has no durable intent"))
    }
    async fn environment(&self, record: &CheckWorkerRecord) -> Result<BTreeMap<String, String>> {
        let environment = review::contract::project_environment(
            &self.db,
            &self.intent(record)?.environment_task_id,
        )
        .await
        .map_err(ServiceError::invalid_operation)?;
        // Secret bytes never enter check storage. Controlled values must still
        // equal the declared snapshot; volatile values make the key uncacheable.
        // The canonical policy states its Project values as revisions and
        // never refuses here: its owner compares the whole environment before
        // it runs, and one that moved makes the result not reusable.
        for (key, value) in &record.run.identity.inputs.environment {
            if let CheckEnvironmentValue::ControlledValue(expected) = value {
                if environment.env.get(key) != Some(expected) {
                    return Err(ServiceError::invalid_operation(
                        "check environment revision changed",
                    ));
                }
            }
        }
        Ok(environment.env)
    }
    fn deadline(&self, record: &CheckWorkerRecord) -> Result<Instant> {
        let time = chrono::DateTime::parse_from_rfc3339(
            record
                .deadline_at
                .as_deref()
                .ok_or_else(|| ServiceError::invalid_operation("check has no deadline"))?,
        )
        .map_err(|_| ServiceError::invalid_operation("invalid check deadline"))?
        .with_timezone(&chrono::Utc);
        Ok(Instant::now() + (time - chrono::Utc::now()).to_std().unwrap_or_default())
    }
    async fn server_run(
        &self,
        record: &CheckWorkerRecord,
        cancel: &CancellationToken,
    ) -> Result<DaemonCheckResult> {
        let intent = self.intent(record)?;
        let CheckDispatchTarget::Server {
            path,
            workspace_id,
            placement_id,
            generation,
        } = &intent.target
        else {
            return Err(ServiceError::invalid_operation(
                "foreign server check target",
            ));
        };
        let id = record.run.operation_id.clone();
        let token = cancel.child_token();
        {
            let mut operations = self.operations.lock().unwrap_or_else(|p| p.into_inner());
            if let Some(operation) = operations.get(&id) {
                return Ok(operation
                    .result
                    .clone()
                    .unwrap_or(DaemonCheckResult::Running { operation_id: id }));
            }
            operations.insert(
                id.clone(),
                ServerOperation {
                    cancel: token.clone(),
                    result: None,
                },
            );
        }
        let _operation = OperationGuard {
            operations: self.operations.clone(),
            id: id.clone(),
        };
        let deadline = self.deadline(record)?;
        let receipt=async {
            // This shares the integration owner's physical checkout lock.
            let _checkout=tokio::select! {
                guard=self.db.lock_server_check_checkout(workspace_id,path)=>guard?,
                _=token.cancelled()=>return Ok(check_executor::unstarted_receipt(&id,intent.owner.clone(),CheckExecutionOutcome::Cancelled,None)),
                _=tokio::time::sleep_until(deadline.into())=>return Ok(check_executor::unstarted_receipt(&id,intent.owner.clone(),CheckExecutionOutcome::TimedOut,None)),
            };
            let current=self.db.check_worker_record(&record.run.id).await?;
            if current.run.state!=db::CheckRunState::Running || current.run.lease_generation!=record.run.lease_generation || current.run.lease_owner!=record.run.lease_owner || current.run.lease_until.as_deref().and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok()).is_none_or(|until| until.with_timezone(&chrono::Utc)<=chrono::Utc::now()) {
                return Err(ServiceError::invalid_operation("check dispatch lease changed"));
            }
            let placement=WorkspacePlacementRepo::get_by_workspace_id(&*self.db,workspace_id).await?.ok_or(db::DbError::NotFound)?;
            if placement.id!=*placement_id || placement.generation!=*generation || placement.owner_kind!=PlacementOwnerKind::Server || placement.workspace_handle.as_deref()!=Some(path) || placement.state!=PlacementState::Ready { return Err(ServiceError::invalid_operation("check placement changed")); }
            let actual=tokio::time::timeout(Duration::from_secs(2),git::get_current_sha(Path::new(path))).await.map_err(|_| ServiceError::invalid_operation("check candidate witness timed out"))??;
            if actual!=record.run.identity.commit_sha { return Err(ServiceError::invalid_operation("check candidate changed")); }
            let environment=self.environment(record).await?;
            // The canonical policy is attested by this owner, now, from the
            // environment the steps are about to inherit. When that is not
            // the environment the request named (a Project value was edited,
            // the server restarted into another environment, the login shell
            // did not answer), the run still executes with what is in force,
            // as entry CI always did; the receipt then attests nothing and
            // the result is its consumers' verdict, never reused.
            let inputs=if record.run.identity.inputs.spec.execution_policy==CANONICAL_CI_POLICY {
                let stated=&record.run.identity.inputs.environment_identity;
                super::policy::attest(&self.db,&environment).await?.filter(|inputs| matches!(stated,CheckEnvironmentIdentity::Attested { .. }) && inputs.identity().as_ref()==Ok(stated))
            } else { None };
            crate::check_owner::ServerCheckOwner::run(check_executor::CheckExecution {
                operation_id:&id,spec:&record.run.identity.inputs.spec,target:check_executor::CheckoutTarget::Workspace(Path::new(path)),owner:intent.owner.clone(),input_revisions:inputs.as_ref(),environment:&environment,deadline:Some(deadline),cancel:&token,permit:&check_executor::CheckPermit::already_admitted(),cleanup:check_executor::CleanupPlan { commands:&[],timeout:check_executor::CLEANUP_TIMEOUT },output_limit:check_executor::OUTPUT_TAIL_BYTES,
            }).await
        }.await.unwrap_or_else(|_| check_executor::unstarted_receipt(&id,intent.owner.clone(),CheckExecutionOutcome::Infrastructure,Some("check checkout or execution inputs unavailable".into())));
        let result = DaemonCheckResult::Completed {
            receipt: Box::new(receipt.clone()),
        };
        // Retain in memory before the durable write too: a transient DB error
        // cannot erase a completed owner effect and cause another launch.
        self.operations
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get_mut(&id)
            .ok_or_else(|| ServiceError::invalid_operation("server operation disappeared"))?
            .result = Some(result.clone());
        self.db
            .record_check_owner_receipt(&record.run.id, &id, &receipt)
            .await?;
        Ok(result)
    }
}
#[async_trait]
impl CheckOwnerPort for WorkspaceCheckOwners {
    async fn prepare(&self, run: &StoredCheckRun) -> Result<CheckDispatchIntent> {
        let workspace_id = run
            .workspace_id
            .as_deref()
            .ok_or_else(|| ServiceError::invalid_operation("check needs an owner workspace"))?;
        let placement = WorkspacePlacementRepo::get_by_workspace_id(&*self.db, workspace_id)
            .await?
            .ok_or(db::DbError::NotFound)?;
        if placement.state != PlacementState::Ready {
            return Err(ServiceError::invalid_operation(
                "check placement is not ready",
            ));
        }
        // A daemon attests no inputs yet, so only the server owner executes
        // the canonical policy.
        let policy = run.identity.inputs.spec.execution_policy.as_str();
        let configured = match placement.owner_kind {
            PlacementOwnerKind::Server => {
                matches!(policy, "legacy-server/1" | CANONICAL_CI_POLICY)
            }
            PlacementOwnerKind::Daemon => policy == "legacy-daemon/1",
        };
        if !configured {
            return Err(ServiceError::invalid_operation(
                "check owner policy is not configured",
            ));
        }
        if let CheckScope::Workspace {
            workspace_id,
            generation,
        } = &run.identity.inputs.spec.scope
        {
            if workspace_id != &placement.workspace_id
                || i64::try_from(*generation).ok() != Some(placement.generation)
            {
                return Err(ServiceError::invalid_operation(
                    "check workspace generation changed",
                ));
            }
        }
        let consumers = self
            .db
            .active_check_consumers(&run.id, &db::now_rfc3339())
            .await?;
        let task_id = consumers
            .first()
            .and_then(|c| c.task_id.clone())
            .ok_or_else(|| ServiceError::invalid_operation("check has no current Task consumer"))?;
        let handle = placement
            .workspace_handle
            .clone()
            .ok_or_else(|| ServiceError::invalid_operation("check placement has no handle"))?;
        let (target, owner) = match placement.owner_kind {
            PlacementOwnerKind::Server => {
                if run.machine_id.is_some() {
                    return Err(ServiceError::invalid_operation(
                        "server check has foreign machine",
                    ));
                }
                (
                    CheckDispatchTarget::Server {
                        path: handle,
                        workspace_id: workspace_id.into(),
                        placement_id: placement.id,
                        generation: placement.generation,
                    },
                    CheckOwnerIdentity {
                        owner_kind: "server".into(),
                        machine_id: None,
                        runtime_id: self.runtime_id.clone(),
                    },
                )
            }
            PlacementOwnerKind::Daemon => {
                let daemon = placement.daemon_id.ok_or_else(|| {
                    ServiceError::invalid_operation("check placement has no daemon")
                })?;
                if run.machine_id.as_deref() != Some(&daemon) {
                    return Err(ServiceError::invalid_operation(
                        "check has a different physical owner",
                    ));
                }
                let runtime = placement.runtime_id.ok_or_else(|| {
                    ServiceError::invalid_operation("check placement has no runtime")
                })?;
                (
                    CheckDispatchTarget::Daemon {
                        workspace: WorkspaceHandleReference {
                            daemon_id: daemon.clone(),
                            runtime_id: runtime.clone(),
                            placement_id: placement.id,
                            workspace_handle: handle,
                            generation: u64::try_from(placement.generation).map_err(|_| {
                                ServiceError::invalid_operation("invalid check generation")
                            })?,
                        },
                    },
                    CheckOwnerIdentity {
                        owner_kind: "daemon".into(),
                        machine_id: Some(daemon),
                        runtime_id: runtime,
                    },
                )
            }
        };
        Ok(CheckDispatchIntent {
            target,
            owner,
            environment_task_id: task_id,
        })
    }
    async fn run(
        &self,
        record: &CheckWorkerRecord,
        cancel: &CancellationToken,
    ) -> Result<DaemonCheckResult> {
        let intent = self.intent(record)?;
        match &intent.target {
            CheckDispatchTarget::Server { .. } => self.server_run(record, cancel).await,
            CheckDispatchTarget::Daemon { workspace } => {
                let environment = self.environment(record).await?;
                let params = DaemonCheckRunParams {
                    operation_id: record.run.operation_id.clone(),
                    target: DaemonCheckTarget::Workspace {
                        workspace: workspace.clone(),
                    },
                    purpose: WorkspaceRunPurpose::CiStep,
                    spec: record.run.identity.inputs.spec.clone(),
                    env: environment.into_iter().collect(),
                    cleanup_commands: vec![],
                    cleanup_timeout_ms: 30_000,
                    deadline: record
                        .deadline_at
                        .clone()
                        .ok_or_else(|| ServiceError::invalid_operation("check has no deadline"))?,
                };
                self.client
                    .run_check(&workspace.daemon_id, params)
                    .await
                    .map_err(owner_error)
            }
        }
    }
    async fn lookup(&self, record: &CheckWorkerRecord) -> Result<DaemonCheckResult> {
        if let Some(receipt) = self.db.check_worker_record(&record.run.id).await?.receipt {
            return Ok(DaemonCheckResult::Completed {
                receipt: Box::new(receipt),
            });
        }
        match &self.intent(record)?.target {
            CheckDispatchTarget::Daemon { workspace } => self
                .client
                .lookup_check(&workspace.daemon_id, &record.run.operation_id)
                .await
                .map_err(owner_error),
            CheckDispatchTarget::Server { .. } => Ok(self
                .operations
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .get(&record.run.operation_id)
                .map(|op| {
                    op.result.clone().unwrap_or(DaemonCheckResult::Running {
                        operation_id: record.run.operation_id.clone(),
                    })
                })
                .unwrap_or(DaemonCheckResult::Interrupted {
                    operation_id: record.run.operation_id.clone(),
                })),
        }
    }
    async fn cancel(&self, record: &CheckWorkerRecord) -> Result<DaemonCheckResult> {
        match &self.intent(record)?.target {
            CheckDispatchTarget::Daemon { workspace } => self
                .client
                .cancel_check(&workspace.daemon_id, &record.run.operation_id)
                .await
                .map_err(owner_error),
            CheckDispatchTarget::Server { .. } => {
                {
                    let mut operations = self.operations.lock().unwrap_or_else(|p| p.into_inner());
                    let operation = operations
                        .entry(record.run.operation_id.clone())
                        .or_insert_with(|| ServerOperation {
                            cancel: CancellationToken::new(),
                            result: Some(DaemonCheckResult::Interrupted {
                                operation_id: record.run.operation_id.clone(),
                            }),
                        });
                    operation.cancel.cancel();
                }
                self.lookup(record).await
            }
        }
    }
    async fn owner_gone(&self, record: &CheckWorkerRecord) -> Result<bool> {
        let CheckDispatchTarget::Daemon { workspace } = &self.intent(record)?.target else {
            return Ok(false);
        };
        let row: Option<(Option<String>,Option<String>,Option<String>)>=sqlx::query_as("SELECT d.removed_at,p.disconnected_at,d.last_report_at FROM daemon d LEFT JOIN workspace_placement p ON p.id=? WHERE d.id=?").bind(&workspace.placement_id).bind(&workspace.daemon_id).fetch_optional(self.db.pool()).await?;
        let Some((removed, disconnected, reported)) = row else {
            return Ok(true);
        };
        if removed.is_some() {
            return Ok(true);
        }
        if self.registry.is_connected(&workspace.daemon_id) {
            return Ok(false);
        }
        let since = disconnected
            .or(reported)
            .or_else(|| record.admitted_at.clone());
        Ok(since
            .and_then(|s| chrono::DateTime::parse_from_rfc3339(&s).ok())
            .is_some_and(|since| {
                (chrono::Utc::now() - since.with_timezone(&chrono::Utc))
                    .to_std()
                    .unwrap_or_default()
                    >= self.disconnect_bound
            }))
    }
    async fn acknowledge(&self, record: &CheckWorkerRecord) -> Result<()> {
        match &self.intent(record)?.target {
            CheckDispatchTarget::Daemon { workspace } => {
                let entry = format!("forge:operation:{}", record.run.operation_id);
                let ack = self
                    .client
                    .acknowledge(&workspace.daemon_id, entry.clone())
                    .await
                    .map_err(owner_error)?;
                if ack.entry_id != entry || !ack.acknowledged {
                    return Err(ServiceError::invalid_operation(
                        "check journal acknowledgment not confirmed",
                    ));
                }
            }
            CheckDispatchTarget::Server { .. } => {
                self.operations
                    .lock()
                    .unwrap_or_else(|p| p.into_inner())
                    .remove(&record.run.operation_id);
            }
        }
        Ok(())
    }
}

fn owner_error(
    error: crate::daemon_transport::workspace_client::WorkspaceClientError,
) -> ServiceError {
    match error {
        crate::daemon_transport::workspace_client::WorkspaceClientError::Transport(error) => error,
        crate::daemon_transport::workspace_client::WorkspaceClientError::Daemon(error) => {
            ServiceError::invalid_operation(format!("check owner refused {}", error.code))
        }
    }
}
