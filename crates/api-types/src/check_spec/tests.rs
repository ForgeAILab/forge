use super::*;

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
#[test]
fn every_semantic_field_changes_the_digest_and_audit_metadata_does_not() {
    let input = fixture();
    let digest = input.digest().unwrap();
    type Change = fn(&mut CheckDigestInput);
    // One typed field per entry: a field `encoding()` dropped would not move
    // the digest and fails here by name.
    let changes: &[(&str, Change)] = &[
        ("scope commit", |i| i.spec.scope = CheckScope::Commit),
        ("scope workspace id", |i| {
            i.spec.scope = CheckScope::Workspace {
                workspace_id: "workspace-2".into(),
                generation: 3,
            }
        }),
        ("scope workspace generation", |i| {
            i.spec.scope = CheckScope::Workspace {
                workspace_id: "workspace-1".into(),
                generation: 4,
            }
        }),
        ("scope task", |i| {
            i.spec.scope = CheckScope::Task {
                task_id: "workspace-1".into(),
            }
        }),
        ("declares_cleanup", |i| i.spec.declares_cleanup = true),
        ("execution_policy", |i| i.spec.execution_policy.push('!')),
        ("command order", |i| i.spec.commands.reverse()),
        ("command removed", |i| {
            i.spec.commands.pop();
            i.spec.configured_commands -= 1;
        }),
        ("command id", |i| i.spec.commands[0].id.push('!')),
        ("shell_text", |i| i.spec.commands[0].shell_text.push(' ')),
        ("shell", |i| i.spec.commands[0].shell = "sh -c".into()),
        ("working_directory", |i| {
            i.spec.commands[0].working_directory = CheckWorkingDirectory::SuppliedDirectory
        }),
        ("timeout_seconds", |i| {
            i.spec.commands[0].timeout_seconds = Some(1801)
        }),
        ("timeout unbounded", |i| {
            i.spec.commands[0].timeout_seconds = None
        }),
        ("failure_policy", |i| {
            i.spec.commands[0].failure_policy = CheckFailurePolicy::Continue
        }),
        ("cacheability", |i| {
            i.spec.commands[0].cacheability = CheckCacheability::Uncacheable
        }),
        ("requirement_ids", |i| {
            i.spec.commands[0].requirement_ids.insert("scope:r2".into());
        }),
        ("environment key", |i| {
            for command in &mut i.spec.commands {
                command.environment_keys.insert("PATH".into());
            }
            i.environment
                .insert("PATH".into(), CheckEnvironmentValue::Removed);
        }),
        ("controlled value", |i| {
            i.environment.insert(
                "LANG".into(),
                CheckEnvironmentValue::ControlledValue("C".into()),
            );
        }),
        ("secret revision", |i| {
            i.environment.insert(
                "TOKEN".into(),
                CheckEnvironmentValue::SecretRevision("opaque-secret-r4".into()),
            );
        }),
        ("value kind, same text", |i| {
            i.environment.insert(
                "LANG".into(),
                CheckEnvironmentValue::SecretRevision("C.UTF-8".into()),
            );
        }),
        ("removed value", |i| {
            i.environment
                .insert("LANG".into(), CheckEnvironmentValue::Removed);
        }),
        ("volatile value", |i| {
            i.environment
                .insert("LANG".into(), CheckEnvironmentValue::Volatile);
        }),
        ("attested input digest", |i| {
            i.environment_identity = CheckEnvironmentIdentity::Attested {
                input_digest: "b".repeat(64),
            }
        }),
        ("not attested", |i| {
            i.environment_identity = CheckEnvironmentIdentity::NotAttested
        }),
        ("execution revision", |i| i.execution_revision.number += 1),
    ];
    // Exhaustive by construction: a field added to the spec does not compile
    // here until it is listed above (semantic) or below (not semantic).
    let CheckSpec {
        schema_revision: _,
        scope: _,
        commands: _,
        declares_cleanup: _,
        execution_policy: _,
        configured_commands: _,
        blank_commands: _,
    } = &input.spec;
    let CheckCommandSpec {
        id: _,
        shell_text: _,
        shell: _,
        working_directory: _,
        environment_keys: _,
        timeout_seconds: _,
        failure_policy: _,
        cacheability: _,
        requirement_ids: _,
    } = &input.spec.commands[0];
    let CheckDigestInput {
        spec: _,
        environment: _,
        environment_identity: _,
        execution_revision: _,
    } = &input;
    let mut seen = std::collections::BTreeSet::from([digest.clone()]);
    for (name, change) in changes {
        let mut changed = input.clone();
        change(&mut changed);
        assert!(
            seen.insert(changed.digest().unwrap()),
            "{name} did not produce a new digest"
        );
    }
    // The schema revision is semantic too; admission refuses any other value,
    // so compare the encoded envelopes without validation.
    let encode = |revision: u32| {
        let mut spec = input.spec.clone();
        spec.schema_revision = revision;
        crate::canonical_json(&spec).unwrap()
    };
    assert_ne!(encode(CHECK_SPEC_REVISION), encode(CHECK_SPEC_REVISION + 1));
    assert!(input.encoding().unwrap().contains("\"schema_revision\":2"));

    // Not semantic: where a blank step sat changes no command that runs.
    let unchanged: &[(&str, Change)] = &[
        ("one more blank step", |i| {
            i.spec.configured_commands += 1;
            i.spec.blank_commands.push(3);
        }),
        ("no blank step", |i| {
            i.spec.configured_commands = 2;
            i.spec.blank_commands.clear();
        }),
        ("blank step elsewhere", |i| i.spec.blank_commands = vec![0]),
    ];
    for (name, change) in unchanged {
        let mut changed = input.clone();
        change(&mut changed);
        assert_eq!(changed.digest().unwrap(), digest, "{name} moved the digest");
    }
    // Purpose and the whole-run wall timeout are not identity inputs at all:
    // the digest input has nowhere to put them, and nothing encodes them.
    let encoding = input.encoding().unwrap();
    for absent in ["purpose", "whole_run", "wall_timeout", "blank_commands"] {
        assert!(!encoding.contains(absent), "{absent} reached the digest");
    }
    assert!(serde_json::to_string(&input)
        .unwrap()
        .contains("blank_commands"));
    // The record of blank steps must still account for every configured step.
    let mut changed = input.clone();
    changed.spec.configured_commands += 1;
    assert!(changed.digest().is_err());

    let mut changed = input.clone();
    changed.environment.remove("LANG");
    assert!(
        changed.digest().is_err(),
        "missing declared value must be refused"
    );
    changed = input.clone();
    changed.execution_revision.audit_ref = Some("another audit location".into());
    assert_eq!(changed.digest().unwrap(), digest);
    assert!(!input.encoding().unwrap().contains("owner-action-7"));
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
