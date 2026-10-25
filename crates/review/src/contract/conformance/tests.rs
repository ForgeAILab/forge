use super::super::tests::source;
use super::*;
use serde_json::json;

#[test]
fn findings_unknown_requirement_ids_are_dropped_before_freezing() {
    let context = context_from_source(&source()).unwrap();
    let known = context.requirements[0].id.clone();
    let contract = ReviewContract {
        execution_id: "e".into(),
        policy: REVIEW_CONFORMANCE_POLICY.into(),
        commit_sha: "c".into(),
        base_sha: "b".into(),
        candidate_changed_paths: vec![],
        context,
        check_results: vec![],
        digest: "d".into(),
    };
    let mut report = parse_assessment(&json!({"result":"fail","findings":[{"summary":"missed", "requirement_ids":[known, "R99", known]}]}).to_string()).unwrap();
    retain_contract_requirement_ids(&mut report, &contract);
    assert_eq!(report.findings[0].requirement_ids, [known]);
    assert!(!serde_json::to_string(&report).unwrap().contains("R99"));
    let prompt = contract_prompt(&contract);
    assert!(prompt.contains(&format!(
        "Frozen requirement ids: {}",
        contract
            .context
            .requirements
            .iter()
            .map(|requirement| requirement.id.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    )));
    assert!(prompt.contains("After the first review in a window"));
}
