//! Shared persistence values for Project environment pauses.

use api_types::ProjectEnvironmentPause;
use chrono::{DateTime, Duration, Utc};

use crate::{Result, ServiceError};

pub(crate) const ENVIRONMENT_NOT_READY: &str = "environment_not_ready";
/// Durable pre-dispatch tag. The original launch failure prefix also lets
/// migrated Tasks retry their legacy environment-failed executions.
pub(crate) const ENVIRONMENT_PRE_DISPATCH_ERROR_PREFIX: &str = "environment not ready: ";
/// Characters of check output kept in a pause detail or failure message.
pub(crate) const BLOCK_MESSAGE_OUTPUT_CHARS: usize = 1500;

pub(crate) fn is_environment_pre_dispatch_failure(execution: &db::Execution) -> bool {
    execution.status == db::ExecutionStatus::Failed
        && execution
            .error
            .as_deref()
            .is_some_and(|error| error.starts_with(ENVIRONMENT_PRE_DISPATCH_ERROR_PREFIX))
}

pub(crate) fn bounded_output_tail(output: &str) -> String {
    let output = output.trim();
    output
        .chars()
        .skip(
            output
                .chars()
                .count()
                .saturating_sub(BLOCK_MESSAGE_OUTPUT_CHARS),
        )
        .collect()
}

pub(crate) fn next_check_at(now: DateTime<Utc>, interval_seconds: u64) -> String {
    // Settings validation bounds this value; bounding old stored documents
    // also avoids an overflow while calculating a schedule.
    (now + Duration::seconds(interval_seconds.clamp(60, 86400) as i64)).to_rfc3339()
}

pub(crate) fn pause_detail(project: &db::Project) -> Result<Option<ProjectEnvironmentPause>> {
    project
        .environment_pause_json
        .as_deref()
        .map(serde_json::from_str)
        .transpose()
        .map_err(|error| {
            ServiceError::invalid_operation(format!("invalid environment pause: {error}"))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn environment_pause_output_retains_bounded_unicode_tail() {
        let output = format!("{}root free: 7G", "界".repeat(2000));
        let tail = bounded_output_tail(&output);
        assert_eq!(tail.chars().count(), BLOCK_MESSAGE_OUTPUT_CHARS);
        assert!(tail.ends_with("root free: 7G"));
    }
}
