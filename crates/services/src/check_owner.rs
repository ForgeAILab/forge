//! Server check owner seam. Admission, durable intent and certification belong
//! to the caller; this owner never changes Task, Review or check storage rows.
use crate::{Result, ServiceError};
use api_types::CheckReceipt;

pub struct ServerCheckOwner;
impl ServerCheckOwner {
    pub async fn run(input: check_executor::CheckExecution<'_>) -> Result<CheckReceipt> {
        if input.owner.owner_kind != "server" || input.owner.machine_id.is_some() {
            return Err(ServiceError::invalid_operation("foreign check owner"));
        }
        Ok(check_executor::execute(input).await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use api_types::*;
    use std::{
        collections::BTreeMap,
        time::{Duration, Instant},
    };
    use tokio_util::sync::CancellationToken;
    #[tokio::test]
    async fn server_owner_refuses_a_foreign_owner_before_executing() {
        let temp = tempfile::tempdir().unwrap();
        let env = BTreeMap::new();
        let spec = check_executor::legacy_ci_spec("touch marker", &env, 1, false);
        let result = ServerCheckOwner::run(check_executor::CheckExecution {
            operation_id: "wrong-owner",
            spec: &spec,
            target: check_executor::CheckoutTarget::Workspace(temp.path()),
            owner: CheckOwnerIdentity {
                owner_kind: "daemon".into(),
                machine_id: Some("daemon".into()),
                runtime_id: "runtime".into(),
            },
            input_revisions: None,
            environment: &env,
            deadline: Some(Instant::now() + Duration::from_secs(1)),
            cancel: &CancellationToken::new(),
            permit: &check_executor::CheckPermit::already_admitted(),
            cleanup: check_executor::CleanupPlan {
                commands: &[],
                timeout: Duration::from_secs(1),
            },
            output_limit: 4096,
        })
        .await;
        assert!(result.is_err());
        assert!(!temp.path().join("marker").exists());
    }
}
