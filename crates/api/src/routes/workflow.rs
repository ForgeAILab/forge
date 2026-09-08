use api_types::PromptBuilderRegistryEntry;
use axum::Json;

use crate::errors::ApiResult;

pub async fn list_prompt_builders() -> ApiResult<Json<Vec<PromptBuilderRegistryEntry>>> {
    let entries = services::workflow::dispatch::prompt_builder_registry_entries()
        .into_iter()
        .map(|entry| PromptBuilderRegistryEntry {
            id: entry.id.to_string(),
            label: entry.label.to_string(),
            compatible_role_hints: entry
                .compatible_role_hints
                .iter()
                .map(|hint| hint.to_string())
                .collect(),
            description: entry.description.to_string(),
        })
        .collect();
    Ok(Json(entries))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn list_prompt_builders_returns_registry_entries() {
        let Json(entries) = list_prompt_builders()
            .await
            .expect("prompt builder registry loads");

        assert!(entries
            .iter()
            .any(|entry| entry.id == "reviewer.conformance.v1"));
        assert!(entries
            .iter()
            .any(|entry| entry.id == "coder.review_fix.v2"));
    }
}
