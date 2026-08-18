#![forbid(unsafe_code)]

mod connection;
mod error;
mod ids;
mod migration;
mod models;
mod orchestration;
mod pagination;
mod repository;
mod sqlite;
mod task_metadata;
#[cfg(test)]
mod tests;
mod time;

pub use connection::create_sqlite_pool;
pub use error::{DbError, Result};
pub use ids::{new_uuid_v4, validate_uuid_v4};
pub use migration::{run_migrations, run_migrations_from};
pub use models::*;
pub use orchestration::*;
pub use pagination::*;
pub use repository::*;
pub use sqlite::SqliteDb;
pub use sqlx::{Sqlite, SqlitePool};
pub use task_metadata::TaskMetadata;
pub use time::now_rfc3339;

pub use models::{
    CreateOAuthAuthorizationCode, CreateOAuthClient, CreateOAuthRefreshToken,
    CreatePersonalAccessToken, CreateProjectIntegration, CreateProjectMember,
    CreateTaskExternalLink, CreateTaskRoleAssignment, CreateTerminalSession, CreateTransitionLog,
    ExecutionUsage, IntegrationPlatform, OAuthAuthorizationCode, OAuthClient, OAuthRefreshToken,
    PersonalAccessToken, ProjectIntegration, ProjectMember, RefreshToken, TaskExternalLink,
    TaskRoleAssignment, TerminalSession, TerminalSessionStatus, TransitionLog,
    UpdateProjectIntegration, UpdateTerminalSessionStatus, User,
};
pub use repository::{
    AgentTokenBreakdown, CiStepStats, ExecutionUsageRepo, ExternalLinkRepo, IntegrationRepo,
    ModelTokenBreakdown, OAuthAuthorizationCodeRepo, OAuthClientRepo, OAuthRefreshTokenRepo,
    PersonalAccessTokenRepo, ProjectAnalyticsRepo, ProjectMemberRepo, ProjectReviewSummary,
    ProjectTokenStats, RefreshTokenRepo, SystemSettingRepo, TaskRoleAssignmentRepo,
    TaskUsageSummary, TerminalSessionRepo, TransitionLogRepo, UpsertExecutionUsage, UserRepo,
};
