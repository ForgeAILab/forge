use super::*;
use serde_json::Value;

fn fixture() -> CheckDigestInput {
    serde_json::from_str(include_str!("fixtures/input.json")).unwrap()
}
#[test]
fn golden_execution_identity_pins_bytes_and_sha256() {
    let input = fixture();
    assert_eq!(
        input.encoding().unwrap(),
        include_str!("fixtures/encoding.json").trim_end()
    );
    assert_eq!(
        input.digest().unwrap(),
        include_str!("fixtures/digest.txt").trim()
    );
}
fn permutations(values: &mut [String], start: usize, visit: &mut impl FnMut(&[String])) {
    if start == values.len() {
        visit(values);
        return;
    }
    for index in start..values.len() {
        values.swap(start, index);
        permutations(values, start + 1, visit);
        values.swap(start, index);
    }
}
#[test]
fn map_key_permutations_preserve_identity() {
    let input = fixture();
    let digest = input.digest().unwrap();
    let value = serde_json::to_value(&input).unwrap();
    let mut keys: Vec<_> = value.as_object().unwrap().keys().cloned().collect();
    let mut tested = 0;
    // Deserialize every permutation, rather than relying on BTreeMap insertion.
    permutations(&mut keys, 0, &mut |keys| {
        let json = format!(
            "{{{}}}",
            keys.iter()
                .map(|k| format!("{}:{}", serde_json::to_string(k).unwrap(), value[k]))
                .collect::<Vec<_>>()
                .join(",")
        );
        let reordered: CheckDigestInput = serde_json::from_str(&json).unwrap();
        assert_eq!(reordered.digest().unwrap(), digest);
        let mut swapped = reordered.clone();
        swapped.environment = reordered.environment.into_iter().rev().collect();
        assert_eq!(swapped.digest().unwrap(), digest);
        tested += 1;
    });
    assert_eq!(tested, 24);
}
fn leaf_paths(value: &Value, prefix: &str, paths: &mut Vec<String>) {
    match value {
        Value::Object(map) => {
            for (key, child) in map {
                leaf_paths(child, &format!("{prefix}/{key}"), paths);
            }
        }
        Value::Array(items) => {
            for (index, child) in items.iter().enumerate() {
                leaf_paths(child, &format!("{prefix}/{index}"), paths);
            }
        }
        _ => paths.push(prefix.to_owned()),
    }
}
#[test]
fn every_semantic_field_changes_the_encoding_and_digest() {
    let input = fixture();
    let value: Value = serde_json::from_str(&input.encoding().unwrap()).unwrap();
    let mut paths = Vec::new();
    leaf_paths(&value, "", &mut paths);
    // Exhaustively mutate every encoded scalar, including enum tags and names.
    // Invalid revisions remain distinct encodings; admission rejects them.
    for path in paths {
        let mut changed = value.clone();
        let leaf = changed.pointer_mut(&path).unwrap();
        *leaf = match leaf {
            Value::String(s) => Value::String(format!("{s}!")),
            Value::Number(n) => serde_json::json!(n.as_u64().unwrap() + 1),
            Value::Bool(b) => Value::Bool(!*b),
            Value::Null => serde_json::json!(1),
            _ => unreachable!(),
        };
        let bytes = crate::canonical_json(&changed).unwrap();
        assert_ne!(bytes, input.encoding().unwrap(), "{path}");
        use sha2::{Digest, Sha256};
        assert_ne!(
            Sha256::digest(bytes.as_bytes()),
            Sha256::digest(input.encoding().unwrap().as_bytes()),
            "{path}"
        );
    }
    let mut changed = input.clone();
    changed.spec.commands.reverse();
    assert_ne!(changed.digest().unwrap(), input.digest().unwrap());
    changed = input.clone();
    changed.environment.remove("LANG");
    assert!(
        changed.digest().is_err(),
        "missing declared value must be refused"
    );
    changed = input.clone();
    changed.execution_revision.audit_ref = Some("another audit location".into());
    assert_eq!(changed.digest().unwrap(), input.digest().unwrap());
}
#[test]
fn cacheability_requires_controlled_inputs_and_attestation() {
    let input = fixture();
    assert!(input.reusable_inputs());
    let mut changed = input.clone();
    changed.environment_identity = CheckEnvironmentIdentity::NotAttested;
    assert!(!changed.reusable_inputs());
    changed = input.clone();
    changed.spec.commands[0].cacheability = CheckCacheability::Uncacheable;
    assert!(!changed.reusable_inputs());
    changed = input.clone();
    changed
        .environment
        .insert("LANG".into(), CheckEnvironmentValue::Volatile);
    assert!(!changed.reusable_inputs());
    changed = input.clone();
    changed.execution_revision.audit_ref = None;
    assert!(changed.validate().is_err());
}
#[test]
fn server_input_identity_binds_tools_assets_secrets_and_environment_revisions() {
    let input = ServerCheckExecutionInputs {
        toolchain_revision: "toolchain-1".into(),
        environment_revision: "env-1".into(),
        shell_revision: "bash-1".into(),
        runner_revision: "runner-1".into(),
        asset_revisions: BTreeMap::from([("config".into(), "asset-1".into())]),
        secret_revisions: BTreeMap::from([("TOKEN".into(), "opaque-1".into())]),
    };
    let identity = input.identity().unwrap();
    for mutate in [
        |i: &mut ServerCheckExecutionInputs| i.toolchain_revision.push('2'),
        |i: &mut ServerCheckExecutionInputs| i.environment_revision.push('2'),
        |i: &mut ServerCheckExecutionInputs| i.shell_revision.push('2'),
        |i: &mut ServerCheckExecutionInputs| i.runner_revision.push('2'),
        |i: &mut ServerCheckExecutionInputs| {
            i.asset_revisions.insert("config".into(), "asset-2".into());
        },
        |i: &mut ServerCheckExecutionInputs| {
            i.secret_revisions.insert("TOKEN".into(), "opaque-2".into());
        },
    ] {
        let mut changed = input.clone();
        mutate(&mut changed);
        assert_ne!(changed.identity().unwrap(), identity);
    }
    let mut changed = input;
    changed.toolchain_revision.clear();
    assert!(changed.identity().is_err());
}
