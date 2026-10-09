//! Durable mechanical-evidence requests. This port has no Task/Review writers.
pub mod consumer;
pub mod owners;
mod receipt;
pub mod worker;

use crate::{Result, ServiceError};
use db::{
    CheckConsumer, CheckRequestDisposition, CheckRunRepo, CheckRunRequest, StoredCheckResult,
    StoredCheckRun,
};
use std::sync::Arc;

#[derive(Debug)]
pub enum CheckRequestOutcome {
    Hit(StoredCheckResult),
    Joined(StoredCheckRun),
    Scheduled(StoredCheckRun),
}

#[derive(Debug)]
pub struct RequestedCheck {
    pub consumer: CheckConsumer,
    pub outcome: CheckRequestOutcome,
}

/// Only the check repository is available here. Application belongs to the
/// consuming Task step, which must revalidate its own authority.
pub struct CheckRunner {
    store: Arc<dyn CheckRunRepo>,
}
impl CheckRunner {
    pub fn new(store: Arc<dyn CheckRunRepo>) -> Self {
        Self { store }
    }

    pub async fn request(&self, request: CheckRunRequest) -> Result<RequestedCheck> {
        if request.identity.inputs.spec.commands.len() > 64 {
            return Err(ServiceError::invalid_operation(
                "check owner accepts at most 64 commands",
            ));
        }
        // Results are delivered as Task steps and the worker cancels a
        // consumer whose Task epoch moved, so a consumer without a Task
        // could never be woken: refuse it instead of scheduling a run that
        // is cancelled on first sight.
        if request.task_id.is_none() {
            return Err(ServiceError::invalid_operation(
                "check consumers are Task steps: a request needs its Task",
            ));
        }
        if request.request_key.len() > 512 {
            return Err(ServiceError::invalid_operation(
                "check consumer key exceeds 512 bytes",
            ));
        }
        if !(1..=86_400).contains(&request.wall_timeout_seconds) {
            return Err(ServiceError::invalid_operation(
                "check owner wall timeout must be 1 through 86400 seconds",
            ));
        }
        let requested = self.store.request_check_run(request).await?;
        let consumer = requested.consumer;
        let run = self
            .store
            .check_run(consumer.run_id.as_deref().ok_or_else(|| {
                ServiceError::invalid_operation("check consumer has no run identity")
            })?)
            .await?
            .ok_or_else(|| ServiceError::invalid_operation("check run disappeared"))?;
        if run.identity_key != consumer.identity_key {
            return Err(ServiceError::invalid_operation("foreign check run"));
        }
        let outcome = if matches!(requested.disposition, CheckRequestDisposition::Reused)
            || matches!(requested.disposition, CheckRequestDisposition::Idempotent)
                && run.state == db::CheckRunState::Succeeded
                && run.cacheable
                && consumer.result_id.is_some()
        {
            let result = self
                .store
                .check_result(consumer.result_id.as_deref().ok_or_else(|| {
                    ServiceError::invalid_operation("check hit has no result identity")
                })?)
                .await?
                .ok_or_else(|| ServiceError::invalid_operation("check result disappeared"))?;
            if result.identity_key != consumer.identity_key
                || result.run_id != run.id
                || !result.certified
                || !result.cacheable
                || result.outcome != db::CheckResultOutcome::Pass
            {
                return Err(ServiceError::invalid_operation("uncertified check hit"));
            }
            CheckRequestOutcome::Hit(result)
        } else if matches!(requested.disposition, CheckRequestDisposition::Scheduled) {
            CheckRequestOutcome::Scheduled(run)
        } else {
            // A repeated request joins its original run even when it is
            // terminal. Its consumer.result_id is the original verdict;
            // uncacheable or failed evidence is never labelled a cache hit.
            CheckRequestOutcome::Joined(run)
        };
        Ok(RequestedCheck { consumer, outcome })
    }
}

#[cfg(test)]
mod tests;
