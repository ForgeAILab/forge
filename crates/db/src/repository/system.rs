//! Skills, notifications, credentials, membership, and system settings repositories.

use super::*;

#[async_trait]
pub trait SkillRepo: Send + Sync {
    async fn create(&self, input: CreateSkill) -> Result<Skill>;
    async fn get_by_id(&self, id: &str) -> Result<Option<Skill>>;
    async fn list_by_project(&self, project_id: &str, page: PageRequest) -> Result<Page<Skill>>;
    async fn update(&self, input: UpdateSkill) -> Result<Skill>;
    async fn delete(&self, id: &str) -> Result<()>;
}

#[async_trait]
pub trait NotificationRepo: Send + Sync {
    async fn create(&self, input: CreateNotification) -> Result<Notification>;
    async fn list(&self, query: NotificationListQuery) -> Result<Page<Notification>>;
    async fn unread_count(&self, project_id: Option<&str>) -> Result<i64>;
    async fn mark_read(&self, id: &str) -> Result<Notification>;
    async fn mark_all_read(&self, project_id: Option<&str>) -> Result<u64>;
    async fn delete(&self, id: &str) -> Result<()>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotificationListQuery {
    pub project_id: Option<String>,
    pub read: Option<bool>,
    pub page: PageRequest,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CreateSkill {
    pub id: String,
    pub project_id: String,
    pub name: String,
    pub content: String,
    pub created_at: String,
    pub updated_at: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateSkill {
    pub id: String,
    pub name: Option<String>,
    pub content: Option<String>,
    pub updated_at: String,
}

#[async_trait]
pub trait UserRepo: Send + Sync {
    async fn create_user(&self, user: &User) -> Result<()>;
    async fn get_user_by_id(&self, id: &str) -> Result<Option<User>>;
    async fn get_user_by_email(&self, email: &str) -> Result<Option<User>>;
    async fn search_users(&self, query: &str, limit: i64) -> Result<Vec<User>>;
    async fn list_users(&self, page: PageRequest) -> Result<Page<User>>;
    async fn set_admin(&self, id: &str, is_admin: bool) -> Result<()>;
    async fn update_profile(
        &self,
        id: &str,
        email: &str,
        display_name: Option<&str>,
        updated_at: &str,
    ) -> Result<()>;
    async fn count_admins(&self) -> Result<i64>;
    async fn delete_user(&self, id: &str) -> Result<bool>;
}

#[async_trait]
pub trait RefreshTokenRepo: Send + Sync {
    async fn create_refresh_token(&self, token: &RefreshToken) -> Result<()>;
    async fn delete_refresh_token_by_hash(&self, token_hash: &str) -> Result<Option<RefreshToken>>;
    async fn delete_refresh_tokens_by_family(&self, family_id: &str) -> Result<u64>;
    async fn delete_expired_refresh_tokens(&self) -> Result<u64>;
    async fn get_refresh_tokens_by_user(&self, user_id: &str) -> Result<Vec<RefreshToken>>;
}

#[async_trait]
pub trait PersonalAccessTokenRepo: Send + Sync {
    async fn create_pat(&self, input: CreatePersonalAccessToken) -> Result<PersonalAccessToken>;
    async fn get_pat_by_token_hash(&self, token_hash: &str) -> Result<Option<PersonalAccessToken>>;
    /// The PAT and its user joined, for the per-request authentication path.
    async fn get_pat_identity_by_token_hash(
        &self,
        token_hash: &str,
    ) -> Result<Option<PersonalAccessTokenIdentity>>;
    async fn list_pats_by_user(&self, user_id: &str) -> Result<Vec<PersonalAccessToken>>;
    async fn delete_pat(&self, id: &str, user_id: &str) -> Result<()>;
    async fn update_last_used(&self, id: &str, last_used_at: &str) -> Result<()>;
}

#[async_trait]
pub trait OAuthClientRepo: Send + Sync {
    async fn create_client(&self, input: CreateOAuthClient) -> Result<OAuthClient>;
    async fn get_client(&self, client_id: &str) -> Result<Option<OAuthClient>>;
    async fn touch_last_used(&self, client_id: &str, last_used_at: &str) -> Result<()>;
    async fn count_clients_created_since(&self, created_after_rfc3339: &str) -> Result<i64>;
}

#[async_trait]
pub trait OAuthAuthorizationCodeRepo: Send + Sync {
    async fn create_code(
        &self,
        input: CreateOAuthAuthorizationCode,
    ) -> Result<OAuthAuthorizationCode>;
    /// Returns the code row regardless of consumed_at / expires_at; service enforces semantics.
    async fn get_code_by_hash(&self, code_hash: &str) -> Result<Option<OAuthAuthorizationCode>>;
    /// Atomic single-use claim: UPDATE ... WHERE id = ? AND consumed_at IS NULL. Returns true if this caller won.
    async fn mark_code_consumed(&self, id: &str, consumed_at: &str) -> Result<bool>;
    async fn delete_expired_codes(&self, now_rfc3339: &str) -> Result<u64>;
}

#[async_trait]
pub trait OAuthRefreshTokenRepo: Send + Sync {
    async fn create_refresh_token(
        &self,
        input: CreateOAuthRefreshToken,
    ) -> Result<OAuthRefreshToken>;
    async fn create_refresh_token_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        input: CreateOAuthRefreshToken,
    ) -> Result<OAuthRefreshToken>;
    async fn get_refresh_token_by_hash(
        &self,
        token_hash: &str,
    ) -> Result<Option<OAuthRefreshToken>>;
    /// Atomic single-use claim for rotation. Returns true if this caller revoked the active row.
    async fn claim_refresh_token_for_rotation(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        id: &str,
        revoked_at: &str,
    ) -> Result<bool>;
    /// Set revoked_at on a single row by id. Idempotent.
    async fn revoke_refresh_token(&self, id: &str, revoked_at: &str) -> Result<()>;
    /// Set revoked_at on every non-revoked row in the family. Returns count of newly-revoked rows.
    async fn revoke_refresh_token_family(&self, family_id: &str, revoked_at: &str) -> Result<u64>;
    async fn revoke_refresh_token_family_in_tx(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        family_id: &str,
        revoked_at: &str,
    ) -> Result<u64>;
    async fn delete_expired_refresh_tokens(&self, now_rfc3339: &str) -> Result<u64>;
}

#[async_trait]
pub trait ProjectMemberRepo: Send + Sync {
    async fn add_member(&self, input: CreateProjectMember) -> Result<ProjectMember>;
    async fn get_member(&self, project_id: &str, user_id: &str) -> Result<Option<ProjectMember>>;
    async fn list_members(&self, project_id: &str) -> Result<Vec<ProjectMember>>;
    async fn update_member_role(
        &self,
        project_id: &str,
        user_id: &str,
        role: &str,
        updated_at: &str,
    ) -> Result<ProjectMember>;
    async fn remove_member(&self, project_id: &str, user_id: &str) -> Result<()>;
}

#[async_trait]
pub trait SystemSettingRepo: Send + Sync {
    async fn get_setting(&self, key: &str) -> Result<Option<String>>;
    async fn set_setting(&self, key: &str, value: &str, updated_at: &str) -> Result<()>;
    async fn list_settings(&self) -> Result<Vec<(String, String)>>;
    async fn delete_setting(&self, key: &str) -> Result<()>;
}
