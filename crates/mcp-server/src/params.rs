use db::{PageRequest, SortOrder};
use serde::Deserialize;
use serde_json::{json, Value};

use crate::error::McpToolError;

#[derive(Debug, Deserialize)]
pub(crate) struct ToolCallParams {
    pub(crate) name: String,
    #[serde(default)]
    pub(crate) arguments: Value,
}

pub(crate) fn parse_params<T>(params: Value) -> Result<T, McpToolError>
where
    T: for<'de> Deserialize<'de>,
{
    serde_json::from_value(params).map_err(|error| {
        McpToolError::new(-32602, "invalid params").with_data(json!({
            "details": error.to_string()
        }))
    })
}

pub(crate) fn page_request(
    cursor: Option<String>,
    limit: Option<i64>,
    sort_by: Option<String>,
) -> Result<PageRequest, McpToolError> {
    Ok(PageRequest {
        cursor,
        limit: db::clamp_page_limit(limit),
        include_total: false,
        sort_by: db::sort_by_from_name(sort_by.as_deref())
            .map_err(|error| McpToolError::new(-32602, error.to_string()))?,
        sort_order: SortOrder::Desc,
    })
}

pub(crate) fn task_page_request(
    cursor: Option<String>,
    limit: Option<i64>,
    sort_by: Option<String>,
) -> Result<PageRequest, McpToolError> {
    if sort_by.is_none() {
        let (sort_by, sort_order) = db::task_sort_defaults();
        return Ok(PageRequest {
            cursor,
            limit: db::clamp_page_limit(limit),
            include_total: false,
            sort_by,
            sort_order,
        });
    }

    page_request(cursor, limit, sort_by)
}
