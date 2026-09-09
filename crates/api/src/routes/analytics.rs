use api_types::AccountUsageAnalyticsResponse;
use axum::{
    extract::{Query, State},
    Json,
};
use db::UsageAnalyticsRepo;

use crate::{
    errors::ApiResult,
    routes::{
        auth::AuthenticatedUser,
        projects::{validate_analytics_window, AnalyticsQuery},
    },
    state::AppState,
};

/// Return usage and cost for every surface owned by the authenticated
/// account.  The repository applies the owner predicate to both invocation
/// and event reads, so this route never accepts an account id from the URL.
pub async fn get_usage_analytics(
    State(state): State<AppState>,
    user: AuthenticatedUser,
    Query(params): Query<AnalyticsQuery>,
) -> ApiResult<Json<AccountUsageAnalyticsResponse>> {
    let from = params.from.as_deref();
    let to = params.to.as_deref();
    validate_analytics_window(from, to)?;
    let response =
        UsageAnalyticsRepo::get_account_usage_analytics(&*state.db, &user.user_id, from, to)
            .await?;
    Ok(Json(response))
}
