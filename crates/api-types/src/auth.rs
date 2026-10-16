use serde::{Deserialize, Deserializer, Serialize};
use ts_rs::TS;

#[derive(Debug, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct RegisterRequest {
    pub email: String,
    pub password: String,
    pub display_name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct LoginRequest {
    pub email: String,
    pub password: String,
}

#[derive(Debug, Default, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct RefreshRequest {
    #[serde(default)]
    #[ts(optional)]
    pub refresh_token: Option<String>,
}

#[derive(Debug, Default, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct LogoutRequest {
    #[serde(default)]
    #[ts(optional)]
    pub refresh_token: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct AuthResponse {
    pub access_token: String,
    pub refresh_token: String,
    pub token_type: String,
    pub expires_in: u64,
}

#[derive(Debug, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct UserResponse {
    pub id: String,
    pub email: String,
    pub display_name: Option<String>,
    pub is_admin: bool,
    pub created_at: String,
}

#[derive(Debug, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct CreateTokenRequest {
    pub name: String,
    pub expires_at: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct TokenResponse {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub token: Option<String>,
    pub prefix: String,
    pub scopes: String,
    pub expires_at: Option<String>,
    pub last_used_at: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct AddMemberRequest {
    pub user_id: String,
    pub role: String,
}

#[derive(Debug, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct UpdateMemberRoleRequest {
    pub role: String,
}

#[derive(Debug, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct ProjectMemberResponse {
    pub id: String,
    pub user_id: String,
    pub email: String,
    pub display_name: Option<String>,
    pub role: String,
    pub created_at: String,
}

#[derive(Debug, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct UserSearchResult {
    pub id: String,
    pub email: String,
    pub display_name: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct AdminUserResponse {
    pub id: String,
    pub email: String,
    pub display_name: Option<String>,
    pub is_admin: bool,
    pub created_at: String,
}

#[derive(Debug, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct AdminUserListResponse {
    pub items: Vec<AdminUserResponse>,
    pub next_cursor: Option<String>,
    pub has_more: bool,
}

#[derive(Debug, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct UpdateAdminRequest {
    pub is_admin: bool,
}

#[derive(Debug, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct UpdateProfileRequest {
    #[ts(optional = nullable)]
    pub email: Option<String>,
    #[serde(default, deserialize_with = "deserialize_optional_update_field")]
    #[ts(type = "string | null")]
    #[ts(optional)]
    pub display_name: Option<Option<String>>,
}

fn deserialize_optional_update_field<'de, D, T>(
    deserializer: D,
) -> Result<Option<Option<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer).map(Some)
}

#[cfg(test)]
mod tests {
    use super::UpdateProfileRequest;

    #[test]
    fn update_profile_request_distinguishes_missing_display_name() {
        let request: UpdateProfileRequest =
            serde_json::from_str(r#"{"email":"user@example.com"}"#).expect("parse request");
        assert_eq!(request.email.as_deref(), Some("user@example.com"));
        assert_eq!(request.display_name, None);
    }

    #[test]
    fn update_profile_request_distinguishes_null_display_name() {
        let request: UpdateProfileRequest =
            serde_json::from_str(r#"{"display_name":null}"#).expect("parse request");
        assert_eq!(request.display_name, Some(None));
    }

    #[test]
    fn update_profile_request_deserializes_string_display_name() {
        let request: UpdateProfileRequest =
            serde_json::from_str(r#"{"display_name":"Forge"}"#).expect("parse request");
        assert_eq!(request.display_name, Some(Some("Forge".to_owned())));
    }
}

#[derive(Debug, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SettingResponse {
    pub key: String,
    pub value: String,
}

#[derive(Debug, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct SettingListResponse {
    pub items: Vec<SettingResponse>,
}

#[derive(Debug, Serialize, Deserialize, TS)]
#[ts(export)]
pub struct UpsertSettingRequest {
    pub value: String,
}
