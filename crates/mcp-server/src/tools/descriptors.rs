use serde_json::Value;

#[cfg(test)]
pub(crate) fn tool_descriptors(scoped_project: bool) -> Value {
    Value::Array(
        operation_registry::mcp::CATALOG
            .iter()
            .map(|spec| spec.descriptor(scoped_project, true))
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialized_mcp_tool_definitions() {
        let baseline: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/mcp_h_base_definitions.json"
        ))
        .unwrap();
        let schemas: Value = serde_json::from_str(include_str!(
            "../../tests/fixtures/mcp_h_expected_schemas.json"
        ))
        .unwrap();
        for (surface, scoped) in [("mcp_account", false), ("mcp_project", true)] {
            let definitions = tool_descriptors(scoped);
            let before = baseline[surface].as_array().unwrap();
            let after = definitions.as_array().unwrap();
            assert_eq!(before.len(), after.len());
            for (before, after) in before.iter().zip(after) {
                let name = after["name"].as_str().unwrap();
                assert_eq!(before["name"], after["name"], "base names/order");
                assert_eq!(
                    schemas[surface][name], after["inputSchema"],
                    "only explicit H schema corrections: {surface}/{name}"
                );
                assert!(after["description"].as_str().unwrap().len() <= 200);
            }
            let authority =
                operation_registry::authority::mcp_authority("u", scoped.then_some("p"));
            let before = before
                .iter()
                .filter(|tool| {
                    !scoped
                        || operation_registry::mcp::lookup(tool["name"].as_str().unwrap())
                            .unwrap()
                            .scope
                            != operation_registry::authority::McpScopeRule::AccountInspection
                })
                .cloned()
                .collect::<Vec<_>>();
            // Compare a fully admitted owner prefix for both grants. Other
            // role surfaces are measured by the live parity table.
            let mut authority = authority;
            for fact in [
                "mcp_project_visible",
                "mcp_project_member",
                "mcp_project_admin",
                "mcp_project_owner",
            ] {
                authority.ceiling.insert(fact.into());
            }
            let after = operation_registry::mcp::CATALOG
                .iter()
                .filter(|spec| authority.evaluate(*spec).is_ok())
                .map(|spec| spec.descriptor(scoped, true))
                .collect::<Vec<_>>();
            let before_bytes = serde_json::to_vec(&before).unwrap().len();
            let after_bytes = serde_json::to_vec(&after).unwrap().len();
            assert!(after_bytes < before_bytes, "smaller {surface} prefix");
            println!("MCP_PREFIX {surface} before_bytes={before_bytes} after_bytes={after_bytes} before_token_estimate={} after_token_estimate={} (ceil bytes/4)",before_bytes.div_ceil(4),after_bytes.div_ceil(4));
            println!(
                "TOOL_DEFINITIONS {surface} {}",
                serde_json::to_string(&definitions).unwrap()
            );
        }
    }
}
