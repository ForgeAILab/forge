#![forbid(unsafe_code)]

mod agent_chat_topic_repository;
pub mod budget;
mod chat_session_denials;
mod connection;
mod effective_authority;
mod environment_readiness;
mod error;
mod ids;
pub mod integration_queue;
pub use integration_queue::*;
pub mod machine_capacity;
mod migration;
mod models;
mod orchestration;
mod pagination;
mod repository;
mod review_conformance;
mod sqlite;
pub mod task_condition;
mod task_metadata;
pub use task_condition::{
    legacy_annotation_blocks, map_legacy_condition, material_blocker, ConditionCapacityScope,
    ConditionChange, ConditionCheckPass, ConditionCheckState, ConditionCheckStatus,
    ConditionContinuation, ConditionEnvironmentKind, ConditionEvidence, ConditionFacts,
    ConditionRead, ConditionRefusal, ConditionRetry, ConditionSource, ConditionStatement,
    ConditionWitness, HumanBoundary, LegacyConditionField, LegacyConditionInput, MaterialBlocker,
    ParkReason, RetryCause, TaskCondition, TerminalOutcome, UnknownConditionProblem,
    CONDITION_CHECK_PAGE, EVIDENCE_VALUE_LIMIT, LEGACY_BLOCKING_ANNOTATION_KINDS, MAPPING_REVISION,
    MAPPING_REVISION_KEY,
};
mod task_mutation;
mod task_step;
pub mod task_writer;
pub use task_mutation::*;
mod remote_cancel;
pub use remote_cancel::*;
pub use task_step::*;
#[cfg(test)]
mod tests;
mod time;
mod worker;
pub use worker::{
    clamp_worker_deferral, FailureState, HealthErrorKind, PoisonDecision, RetryPolicy, WorkItem,
    WorkerHealth, WorkerWaitState,
};

pub use agent_chat_topic_repository::*;
pub use chat_session_denials::*;
pub use connection::{
    begin_immediate, convert_sqlite_to_incremental, create_sqlite_pool, incremental_vacuum,
    sqlite_storage_status,
};
pub use environment_readiness::*;
pub use error::{DbError, Result};
pub use ids::{new_uuid_v4, validate_uuid_v4};
pub use migration::{run_migrations, run_migrations_from};
pub use models::*;
pub use orchestration::*;
pub use pagination::*;
pub use repository::*;
pub use review_conformance::*;
pub use sqlite::{
    supported_main_baseline_revision, DeadLetter, DeadLetterAction, DeadLetterPage,
    EventSubscription, ScheduleRead, SqliteDb, TaskListRead, WorkerDeadLetterIssue,
    WorkerDiagnostic,
};
pub use sqlx::{Sqlite, SqlitePool};
pub use task_metadata::TaskMetadata;
pub use time::now_rfc3339;

pub use models::{
    CreateOAuthAuthorizationCode, CreateOAuthClient, CreateOAuthRefreshToken,
    CreatePersonalAccessToken, CreateProjectIntegration, CreateProjectMember,
    CreateTaskExternalLink, CreateTaskRoleAssignment, CreateTerminalSession, CreateTransitionLog,
    IntegrationPlatform, OAuthAuthorizationCode, OAuthClient, OAuthRefreshToken,
    PersonalAccessToken, ProjectIntegration, ProjectMember, RefreshToken, TaskExternalLink,
    TaskRoleAssignment, TerminalSession, TerminalSessionStatus, TransitionLog,
    UpdateProjectIntegration, UpdateTerminalSessionStatus, User,
};
pub use repository::{
    CiStepStats, ExternalLinkRepo, IntegrationRepo, OAuthAuthorizationCodeRepo, OAuthClientRepo,
    OAuthRefreshTokenRepo, PersonalAccessTokenRepo, ProjectAnalyticsRepo, ProjectMemberRepo,
    ProjectReviewSummary, RefreshTokenRepo, SystemSettingRepo, TaskRoleAssignmentRepo,
    TerminalSessionRepo, TransitionLogRepo, UserRepo,
};

pub mod check_run;
pub use check_run::*;
