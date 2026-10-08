//! Execute gate-authored checkpoint bytes against a real Forge provider.
use agent_runtime::{
    core::{
        approval::{ApprovalDecision, ApprovalPolicy, ApprovalRequest},
        cancel::Cancellation,
        catalog::{ModelLimits, ResolvedModelProfile},
        checkpoint::{CheckpointStore, TurnCheckpoint},
        grant::{
            GrantConstraints, SecurityCheck, SecurityCheckId, SecurityCheckMode,
            SecurityCheckOutcome, SecurityCheckRevision,
        },
        ids::SessionId,
        prelude::{
            ActionClass, InvocationContext, PreparationContext, PreparedToolCall, RuntimeError,
            Tool, ToolOutcome, ToolSpec,
        },
        provider::ModelId,
        security::{AuthorizationRequest, PermissionSet},
    },
    provider::fake::FakeProvider,
    runtime::{RuntimeBuilder, StartSession},
};
use async_trait::async_trait;
use forge_agent_host::ScopeToolComposition;
use serde_json::Value;
use std::sync::{Arc, Mutex};

#[derive(Debug)]
struct Store {
    saved: Mutex<TurnCheckpoint>,
    terminal: tokio::sync::Notify,
    outcomes: Mutex<Vec<Value>>,
}
#[async_trait]
impl CheckpointStore for Store {
    async fn load_latest(&self, _: &SessionId) -> Result<Option<TurnCheckpoint>, RuntimeError> {
        Ok(Some(
            serde_json::from_slice(&serde_json::to_vec(&*self.saved.lock().unwrap()).unwrap())
                .unwrap(),
        ))
    }
    async fn save(&self, checkpoint: &TurnCheckpoint) -> Result<(), RuntimeError> {
        checkpoint.validate()?;
        match &checkpoint.state {
            agent_runtime::core::checkpoint::TurnState::LocalActionOutcomeReady {
                outcome, ..
            }
            | agent_runtime::core::checkpoint::TurnState::ToolOutcomeReady { outcome, .. } => {
                assert!(
                    !outcome.is_error,
                    "real handler failed: {:?}",
                    outcome.value
                );
                self.outcomes.lock().unwrap().push(outcome.value.clone());
            }
            _ => {}
        }
        *self.saved.lock().unwrap() = checkpoint.clone();
        if checkpoint.state.is_terminal() {
            self.terminal.notify_one();
        }
        Ok(())
    }
}
#[derive(Debug)]
struct ExactOnly(Arc<dyn Tool>);
#[async_trait]
impl Tool for ExactOnly {
    fn spec(&self) -> ToolSpec {
        self.0.spec()
    }
    fn normalize_arguments(&self, _: Value) -> Result<Value, RuntimeError> {
        panic!("recorded call must not be normalized again")
    }
    async fn prepare(
        &self,
        _: Value,
        _: &PreparationContext,
    ) -> Result<PreparedToolCall, RuntimeError> {
        panic!("recorded call must not be prepared again")
    }
    async fn invoke(
        &self,
        prepared: PreparedToolCall,
        ctx: &InvocationContext,
    ) -> Result<ToolOutcome, RuntimeError> {
        self.0.invoke(prepared, ctx).await
    }
}
#[derive(Debug)]
struct RequireApproval;
#[async_trait]
impl SecurityCheck for RequireApproval {
    fn id(&self) -> &SecurityCheckId {
        static ID: std::sync::LazyLock<SecurityCheckId> =
            std::sync::LazyLock::new(|| SecurityCheckId::new("gate-approval"));
        &ID
    }
    fn revision(&self) -> &SecurityCheckRevision {
        static REV: std::sync::LazyLock<SecurityCheckRevision> =
            std::sync::LazyLock::new(|| SecurityCheckRevision::new("gate"));
        &REV
    }
    async fn evaluate(&self, _: &AuthorizationRequest, _: &Cancellation) -> SecurityCheckOutcome {
        SecurityCheckOutcome::RequireApproval {
            constraints: GrantConstraints::unconstrained(),
        }
    }
}
#[derive(Debug, Default)]
struct ApproveExact(Mutex<Vec<PreparedToolCall>>);
#[async_trait]
impl ApprovalPolicy for ApproveExact {
    async fn decide(&self, request: &ApprovalRequest) -> ApprovalDecision {
        self.0.lock().unwrap().push(request.prepared().clone());
        ApprovalDecision::Allow
    }
}
pub async fn resume(composition: ScopeToolComposition, fixture: &Value) {
    let checkpoint: TurnCheckpoint = serde_json::from_value(fixture.clone()).unwrap();
    checkpoint.validate().unwrap();
    let original = match &checkpoint.state {
        agent_runtime::core::checkpoint::TurnState::LocalActionPrepared { prepared, .. } => {
            prepared.clone()
        }
        agent_runtime::core::checkpoint::TurnState::AwaitingApproval { slots, .. } => {
            match &slots[0] {
                agent_runtime::core::checkpoint::ToolSlotCheckpoint::Prepared(prepared) => {
                    prepared.clone()
                }
                _ => unreachable!(),
            }
        }
        _ => unreachable!(),
    };
    let store = Arc::new(Store {
        saved: Mutex::new(checkpoint),
        terminal: Default::default(),
        outcomes: Mutex::new(Vec::new()),
    });
    let approval = Arc::new(ApproveExact::default());
    let coverage = PermissionSet::single(agent_runtime::registry::Permission::other(
        "forge.scope.propose",
    ));
    let tools = composition
        .tools()
        .into_iter()
        .filter(|t| t.spec().name == "forge_main_orchestration_propose")
        .map(|tool| Arc::new(ExactOnly(tool)) as Arc<dyn Tool>);
    let runtime = RuntimeBuilder::new(ModelId::new("fake"))
        .tools(tools)
        .security_check(
            Arc::new(RequireApproval),
            SecurityCheckMode::Authoritative,
            coverage,
            ActionClass::new("gate"),
        )
        .provider(Arc::new(FakeProvider::text_reply(
            "Recorded action completed.",
        )))
        .model_profile(ResolvedModelProfile::explicit(
            "fake",
            ModelId::new("fake"),
            ModelLimits::new(128_000, 128_000, 4_096),
        ))
        .checkpoint_store(store.clone())
        .approval(approval.clone())
        .build()
        .unwrap();
    let session = runtime
        .start_session(StartSession::resume(SessionId::new("session")))
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(5), store.terminal.notified())
        .await
        .unwrap();
    assert!(session.resumed());
    assert_eq!(
        store.outcomes.lock().unwrap().len(),
        1,
        "resume must invoke the real handler once"
    );
    let outcome = store.outcomes.lock().unwrap()[0].clone();
    assert_eq!(outcome["operation"], original.arguments()["operation"]);
    assert_eq!(
        outcome["code"],
        if original.arguments()["operation"] == "project.create" {
            "approval_required"
        } else {
            "ok"
        }
    );
    assert_eq!(approval.0.lock().unwrap().as_slice(), &[original]);
    assert!(store.saved.lock().unwrap().state.is_terminal());
    session.shutdown().await.unwrap();
}
