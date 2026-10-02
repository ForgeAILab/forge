#![forbid(unsafe_code)]

mod agent_chat_topic_repository;
mod chat_session_denials;
mod connection;
mod environment_readiness;
mod error;
mod ids;
mod migration;
mod models;
mod orchestration;
mod pagination;
mod repository;
mod review_conformance;
mod sqlite;
mod task_metadata;
#[cfg(test)]
mod tests;
mod time;

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
pub use sqlite::{supported_main_baseline_revision, SqliteDb, TaskListRead};
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
