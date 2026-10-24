//! Shared typed tool fixtures and workspace, outcome, and denial tests.

/// A Project Agent proposes this Project's Tasks from its Chat scope, so
/// it has to be able to list them. Without this read it saw only aggregate
/// counts, could not tell its own Tasks apart, and duplicated the set it
/// had already created.
#[test]
fn a_project_agent_chat_can_read_the_projects_work() {
    let (reads, _) = super::non_task_operations(CanonicalScopeType::AgentChat, true, false);
    assert!(
        reads.contains(&"work.read".to_owned()),
        "project agent chat reads: {reads:?}"
    );
    assert_eq!(
        crate::operation_catalog::descriptor(CanonicalScopeType::AgentChat, "work.read", None)
            .required_permission,
        Some("read_agent_chat")
    );

    // A Main Chat is not bound to one Project, so it keeps the portfolio
    // surface instead.
    let (main_reads, _) = super::non_task_operations(CanonicalScopeType::AgentChat, false, false);
    assert!(!main_reads.contains(&"work.read".to_owned()));
}

use super::*;
use crate::operation_catalog::{
    MAIN_CHARTER_READ_OPERATION, MAIN_PROJECT_CREATE_OPERATION, PROJECT_DECISION_OPERATION,
    PROJECT_DOCUMENT_OPERATION, PROJECT_EVIDENCE_OPERATION, PROJECT_MILESTONE_OPERATION,
    PROJECT_READINESS_OPERATION, PROJECT_RELEASE_OPERATION, PROJECT_VALIDATION_OPERATION,
};
use crate::operation_contract::{
    orchestration_payload_schema, orchestration_read_arguments_schema,
};
use agent_runtime::core::{
    clock::SystemClock,
    ids::{RequestId, SessionId, ToolCallId},
    tool::Tool,
};
use api_types::{CanonicalScopeRef, OrchestrationOutcome, OutcomeCode, OutcomeScopeType};

fn command_preparation_context(workspace: Arc<dyn Workspace>) -> PreparationContext {
    PreparationContext {
        session: SessionId::new("command-session"),
        turn: None,
        call_id: ToolCallId::new("command-call"),
        request: RequestId::new("command-request"),
        workspace,
        clock: Arc::new(SystemClock),
        cancel: Cancellation::new(),
        deadline: Deadline::never(),
    }
}

fn command_invocation_context(workspace: Arc<dyn Workspace>) -> InvocationContext {
    InvocationContext {
        session: SessionId::new("command-session"),
        turn: None,
        call_id: ToolCallId::new("command-call"),
        request: RequestId::new("command-request"),
        workspace,
        clock: Arc::new(SystemClock),
        cancel: Cancellation::new(),
        deadline: Deadline::never(),
        output_limit: 16_384,
    }
}

/// A separate process keeps the irreversible production policy from
/// affecting any normal-mode tests running in this unit-test binary.
#[cfg(unix)]
#[test]
fn demo_native_command_guard_refuses_spawn() {
    const CHILD: &str = "FORGE_TEST_DEMO_NATIVE_COMMAND_CHILD";
    if std::env::var_os(CHILD).is_none() {
        let output = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "typed_tools::tests::demo_native_command_guard_refuses_spawn",
                "--nocapture",
            ])
            .env(CHILD, "1")
            .output()
            .unwrap();
        let stdout = String::from_utf8_lossy(&output.stdout);
        assert!(
            output.status.success() && stdout.contains("demo native command refusal verified"),
            "{}\n{}",
            stdout,
            String::from_utf8_lossy(&output.stderr)
        );
        return;
    }
    executors::server_commands::disable();
    let dir = tempfile::tempdir().unwrap();
    let workspace: Arc<dyn Workspace> = Arc::new(TestWorkspace {
        root: dir.path().to_string_lossy().into_owned(),
    });
    let ctx = command_invocation_context(workspace);
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let result = runtime.block_on(super::execute_workspace_command(
        "sh",
        &["-c".into(), "touch native-command-marker".into()],
        None,
        &Default::default(),
        &ctx,
    ));
    assert!(
        matches!(result, Err(ref error) if error.to_string().contains("server_commands_disabled"))
    );
    assert!(!dir.path().join("native-command-marker").exists());
    println!("demo native command refusal verified");
}

#[cfg(unix)]
#[tokio::test]
async fn native_cleared_commands_receive_operator_proxy_variables() {
    const MARKER: &str = "FORGE_NATIVE_PROXY_CHILD";
    let keys = executors::run_process::PROXY_ENV_KEYS;
    if std::env::var_os(MARKER).is_none() {
        let output = Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "typed_tools::tests::native_cleared_commands_receive_operator_proxy_variables",
            ])
            .env(MARKER, "1")
            .envs(keys.map(|key| (key, format!("sentinel-{key}"))))
            .output()
            .await
            .unwrap();
        assert!(output.status.success(), "native proxy child fixture failed");
        assert!(String::from_utf8_lossy(&output.stdout).contains("1 passed"));
        return;
    }
    let dir = tempfile::tempdir().unwrap();
    let workspace: Arc<dyn Workspace> = Arc::new(TestWorkspace {
        root: dir.path().to_string_lossy().into_owned(),
    });
    let script = keys
        .map(|key| format!("test \"${key}\" = 'sentinel-{key}'"))
        .join(" && ");
    let output = execute_workspace_command(
        "sh",
        &["-c".into(), script],
        None,
        &Default::default(),
        &command_invocation_context(workspace),
    )
    .await
    .unwrap();
    assert!(output.success);
}

#[cfg(unix)]
#[tokio::test]
async fn native_commands_receive_project_and_machine_build_environment() {
    let dir = tempfile::tempdir().unwrap();
    let workspace: Arc<dyn Workspace> = Arc::new(TestWorkspace {
        root: dir.path().to_string_lossy().into_owned(),
    });
    let env = std::collections::BTreeMap::from([("CARGO_BUILD_JOBS".into(), "project".into())]);
    let ctx = command_invocation_context(workspace);
    let output = super::execute_workspace_command(
        "sh",
        &[
            "-c".into(),
            "printf '%s\\n' \"$CARGO_BUILD_JOBS\" \"$MAKEFLAGS\"".into(),
        ],
        None,
        &env,
        &ctx,
    )
    .await
    .unwrap();
    assert!(output.success);
    let text = String::from_utf8(output.stdout).unwrap();
    let lines: Vec<_> = text.lines().collect();
    assert_eq!(lines[0], "project");
    let expected = std::env::var("MAKEFLAGS").unwrap_or_else(|_| {
        format!(
            "-j{}",
            executors::run_process::machine_policy().get().build_jobs()
        )
    });
    assert_eq!(lines[1], expected);
}

/// What a run printed for `$RUSTC_WRAPPER|$KACHE_CACHE_DIR` when Forge
/// offered `wrapper` and `store`. The operator's own environment wins
/// over Forge's, so on a machine whose environment names a wrapper (or a
/// kache directory) this asserts that rule instead.
#[cfg(unix)]
fn assert_saw_compiler_cache(seen: &str, wrapper: &std::path::Path, store: &std::path::Path) {
    let operator = |key: &str| std::env::var_os(key).is_some_and(|value| !value.is_empty());
    let (wrapper, store) = (wrapper.to_str().unwrap(), store.to_str().unwrap());
    if operator("RUSTC_WRAPPER") {
        assert!(!seen.starts_with(wrapper), "{seen}");
    } else if operator("KACHE_CACHE_DIR") {
        assert!(seen.starts_with(&format!("{wrapper}|")), "{seen}");
    } else {
        assert_eq!(seen, format!("{wrapper}|{store}"));
    }
}

/// Plan 3.4 F: a native tool command in a Task worktree is handed the
/// machine's shared compiler cache.
#[cfg(unix)]
#[tokio::test]
async fn native_command_gets_the_shared_compiler_cache() {
    let temp = tempfile::tempdir().unwrap();
    let root = temp.path().join("ws");
    let worktree = root.join("t").join("repo");
    std::fs::create_dir_all(&worktree).unwrap();
    executors::sandbox::TaskRoot::reserve(worktree.parent().unwrap()).unwrap();
    // A linked worktree of the repository `r1`, as the server lays it out.
    std::fs::write(
        worktree.join(".git"),
        format!("gitdir: {}\n", root.join(".repos/r1/worktrees/t").display()),
    )
    .unwrap();
    // The repository names the worktree back, as Git does; without it the
    // worktree's own `.git` file claims nothing.
    std::fs::create_dir_all(root.join(".repos/r1/worktrees/t")).unwrap();
    std::fs::write(
        root.join(".repos/r1/worktrees/t/gitdir"),
        format!("{}\n", worktree.join(".git").display()),
    )
    .unwrap();
    let wrapper = temp.path().join("kache");
    process_supervisor::executable::write_executable(&wrapper, "#!/bin/sh\nexec \"$@\"\n").unwrap();
    executors::compiler_cache::install(
        &root,
        Some(executors::compiler_cache::CompilerCache {
            kind: executors::compiler_cache::WrapperKind::of(&wrapper),
            wrapper: wrapper.clone(),
            dir: root.join(executors::compiler_cache::CACHE_DIR),
            max_bytes: 1 << 30,
        }),
    );
    let store = root.join(executors::compiler_cache::CACHE_DIR).join("r1");
    let seen_file = temp.path().join("seen");
    let script = format!(
        "printf '%s|%s' \"$RUSTC_WRAPPER\" \"$KACHE_CACHE_DIR\" > '{}'",
        seen_file.display()
    );
    let workspace: Arc<dyn Workspace> = Arc::new(TestWorkspace {
        root: worktree.to_string_lossy().into_owned(),
    });
    let ctx = command_invocation_context(workspace);
    let output = super::execute_workspace_command(
        "sh",
        &["-c".into(), script],
        None,
        &std::collections::BTreeMap::new(),
        &ctx,
    )
    .await
    .unwrap();
    executors::compiler_cache::install(&root, None);
    assert!(output.success);
    assert_saw_compiler_cache(
        &std::fs::read_to_string(&seen_file).unwrap(),
        &wrapper,
        &store,
    );
}

/// The tool clears the child's environment, so the server's own
/// `CARGO_TARGET_DIR` is not inherited and must not hold Forge's back: the
/// command builds into its Task root's build directory, never inside the
/// worktree. (A test run under Cargo with a configured target directory
/// has the variable in its own environment, which is the case that
/// regressed.) A value the Project declares still wins.
#[cfg(unix)]
#[tokio::test]
async fn native_command_builds_into_the_task_root_whatever_the_server_environment_has() {
    let dir = tempfile::tempdir().unwrap();
    let worktree = dir.path().join("t").join("repo");
    std::fs::create_dir_all(&worktree).unwrap();
    executors::sandbox::TaskRoot::reserve(worktree.parent().unwrap()).unwrap();
    let build = worktree.parent().unwrap().join(".forge-task/build/cargo");
    let run = |environment: std::collections::BTreeMap<String, String>| {
        let workspace: Arc<dyn Workspace> = Arc::new(TestWorkspace {
            root: worktree.to_string_lossy().into_owned(),
        });
        async move {
            let ctx = command_invocation_context(workspace);
            let output = super::execute_workspace_command(
                "sh",
                &[
                    "-c".into(),
                    "printf '%s' \"${CARGO_TARGET_DIR-unset}\"".into(),
                ],
                None,
                &environment,
                &ctx,
            )
            .await
            .unwrap();
            String::from_utf8(output.stdout).unwrap()
        }
    };
    assert_eq!(
        std::path::PathBuf::from(run(std::collections::BTreeMap::new()).await),
        build
    );
    // A key the Project declares is the Project's: the command gets
    // that value (not Forge's directory, and not nothing, which would
    // build inside the worktree).
    let project = std::collections::BTreeMap::from([(
        "CARGO_TARGET_DIR".to_owned(),
        "/project/target".to_owned(),
    )]);
    assert_eq!(run(project).await, "/project/target");
}

/// The native command tool clears the child's environment, so without the
/// Task root's per-run directory a command falls back to the shared
/// platform temp directory. Each command gets its own, removed when the
/// command returns (pass or fail); a workspace whose parent Forge did not
/// reserve gets nothing and nothing is created beside it.
#[cfg(unix)]
#[tokio::test]
async fn native_command_gets_a_task_root_tmpdir_removed_when_it_returns() {
    let dir = tempfile::tempdir().unwrap();
    let worktree = dir.path().join("t").join("repo");
    std::fs::create_dir_all(&worktree).unwrap();
    executors::sandbox::TaskRoot::reserve(worktree.parent().unwrap()).unwrap();
    let tmp_root = worktree.parent().unwrap().join(".forge-task/tmp");
    let env = std::collections::BTreeMap::new();
    let run = |root: &Path, script: &'static str| {
        let workspace: Arc<dyn Workspace> = Arc::new(TestWorkspace {
            root: root.to_string_lossy().into_owned(),
        });
        let env = env.clone();
        async move {
            let ctx = command_invocation_context(workspace);
            super::execute_workspace_command("sh", &["-c".into(), script.into()], None, &env, &ctx)
                .await
                .unwrap()
        }
    };
    let mut seen = Vec::new();
    for (script, success) in [
        (
            "touch \"$TMPDIR/made\" && printf '%s|%s|%s' \"$TMPDIR\" \"$TMP\" \"$TEMP\"",
            true,
        ),
        (
            "touch \"$TMPDIR/made\" && printf '%s|%s|%s' \"$TMPDIR\" \"$TMP\" \"$TEMP\"; exit 4",
            false,
        ),
    ] {
        let output = run(&worktree, script).await;
        assert_eq!(output.success, success);
        let text = String::from_utf8(output.stdout).unwrap();
        let parts: Vec<_> = text.split('|').collect();
        assert!(parts[0] == parts[1] && parts[1] == parts[2], "{text}");
        let tmp = std::path::PathBuf::from(parts[0]);
        assert_eq!(tmp.parent(), Some(tmp_root.as_path()));
        assert!(!tmp.exists(), "removed when the command returns");
        seen.push(tmp);
    }
    assert_ne!(seen[0], seen[1], "one directory per command");
    assert_eq!(std::fs::read_dir(&tmp_root).unwrap().count(), 0);

    let plain = dir.path().join("user").join("repo");
    std::fs::create_dir_all(&plain).unwrap();
    let output = run(&plain, "printf '%s' \"${TMPDIR-unset}\"").await;
    assert_eq!(String::from_utf8(output.stdout).unwrap(), "unset");
    assert_eq!(
        std::fs::read_dir(dir.path().join("user")).unwrap().count(),
        1
    );
}

#[tokio::test]
async fn verification_commands_run_inside_the_checkout_never_at_the_workspace_root() {
    // The verification workspace root is the parent of the checkout. A
    // command run there would let a build tool's upward manifest search
    // escape into whatever repository holds the data directory.
    let root = std::env::temp_dir().join(format!(
        "forge-verify-cwd-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or_default()
    ));
    let checkout = root.join(PROJECT_VERIFICATION_CHECKOUT_DIR);
    std::fs::create_dir_all(&checkout).expect("create checkout");
    std::fs::write(root.join("marker.txt"), "at the workspace root\n").expect("root marker");
    std::fs::write(checkout.join("marker.txt"), "inside the checkout\n").expect("checkout marker");
    let workspace: Arc<dyn Workspace> = Arc::new(TestWorkspace {
        root: root.to_string_lossy().into_owned(),
    });
    let tool = TaskCommandTool {
        environment: Default::default(),
        allowlist: std::sync::Arc::new(CommandAllowlist::builtin()),
        observer: None,
        command_dir: Some(PROJECT_VERIFICATION_CHECKOUT_DIR),
    };
    let arguments = json!({"program": "cat", "args": ["marker.txt"]});

    let prepared = tool
        .prepare(
            arguments.clone(),
            &command_preparation_context(workspace.clone()),
        )
        .await
        .expect("prepare inside checkout");
    let outcome = tool
        .invoke(prepared, &command_invocation_context(workspace.clone()))
        .await
        .expect("invoke inside checkout");
    assert_eq!(outcome.value["success"], true);
    assert_eq!(outcome.value["stdout"], "inside the checkout\n");

    std::fs::remove_dir_all(&checkout).expect("remove checkout");
    let refused = tool
        .prepare(arguments, &command_preparation_context(workspace))
        .await
        .expect_err("a Project without a checkout has nowhere to run commands");
    assert!(
        refused.to_string().contains("no repository checkout"),
        "unexpected refusal: {refused}"
    );
    let _ = std::fs::remove_dir_all(&root);
}

#[tokio::test]
async fn verification_commands_are_recorded_as_observations_the_agent_can_cite() {
    let root = std::env::temp_dir().join(format!(
        "forge-verify-observe-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or_default()
    ));
    let checkout = root.join(PROJECT_VERIFICATION_CHECKOUT_DIR);
    std::fs::create_dir_all(&checkout).expect("create checkout");
    std::fs::write(checkout.join("marker.txt"), "observed output\n").expect("marker");
    let workspace: Arc<dyn Workspace> = Arc::new(TestWorkspace {
        root: root.to_string_lossy().into_owned(),
    });
    let provider = Arc::new(TestProvider::default());
    let tool = TaskCommandTool {
        environment: Default::default(),
        allowlist: std::sync::Arc::new(CommandAllowlist::builtin()),
        observer: Some(CommandObserver {
            actor_identity_id: "agent-1".to_owned(),
            scope: CanonicalScope {
                scope_type: CanonicalScopeType::AgentChat,
                scope_id: "chat-1".to_owned(),
                workspace_access: WorkspaceAccess::ProjectVerify,
            },
            provider: provider.clone(),
        }),
        command_dir: Some(PROJECT_VERIFICATION_CHECKOUT_DIR),
    };
    let arguments = json!({"program": "cat", "args": ["marker.txt"]});
    let prepared = tool
        .prepare(arguments, &command_preparation_context(workspace.clone()))
        .await
        .expect("prepare");
    let outcome = tool
        .invoke(prepared, &command_invocation_context(workspace))
        .await
        .expect("invoke");
    assert_eq!(outcome.value["observation_id"], "observation-1");
    assert_eq!(outcome.value["stdout"], "observed output\n");

    let observations = provider.observations.lock().expect("observation log");
    let observation = observations.first().expect("one observation recorded");
    assert_eq!(observation.program, "cat");
    assert_eq!(observation.args, vec!["marker.txt".to_owned()]);
    assert_eq!(observation.exit_code, Some(0));
    assert!(observation.success);
    assert_eq!(observation.stdout_excerpt, "observed output\n");
    // The digest covers the complete stdout and stderr, so a later
    // inline evidence claim can be checked against what really printed.
    let expected = {
        use sha2::{Digest, Sha256};
        let mut hasher = Sha256::new();
        hasher.update(b"observed output\n");
        hasher.update(b"\n--stderr--\n");
        hex::encode(hasher.finalize())
    };
    assert_eq!(observation.output_digest, expected);
    let _ = std::fs::remove_dir_all(&root);
}

#[test]
fn portable_const_schema_adds_type_and_enum_for_provider_compatibility() {
    let schema = portable_const_schema(json!({
        "oneOf":[{"properties":{
            "action":{"const":"save_revision"},
            "kind":{"enum":["a","b"]},
            "count":{"type":"integer","minimum":1}
        }}]
    }));
    let action = &schema["oneOf"][0]["properties"]["action"];
    assert_eq!(action["const"], "save_revision");
    assert_eq!(action["type"], "string");
    assert_eq!(action["enum"], json!(["save_revision"]));
    let kind = &schema["oneOf"][0]["properties"]["kind"];
    assert_eq!(kind["type"], "string");
    let count = &schema["oneOf"][0]["properties"]["count"];
    assert!(count.get("enum").is_none());
}

#[test]
fn only_the_effective_allowlist_decides_which_program_may_spawn() {
    let builtin = CommandAllowlist::builtin();
    validate_command_program(&builtin, "cargo").expect("cargo is built in");
    let refused =
        validate_command_program(&builtin, "docker").expect_err("docker needs owner configuration");
    assert!(
        refused.to_string().contains("command allowlist"),
        "the refusal must name the allowlist so the model stops guessing: {refused}"
    );

    let (configured, _) =
        CommandAllowlist::resolve(Some(&["docker".to_owned()]), &["podman".to_owned()]);
    validate_command_program(&configured, "docker").expect("configured program runs");
    validate_command_program(&configured, "podman").expect("added program runs");
    validate_command_program(&configured, "cargo")
        .expect_err("`only` drops the built-in set, cargo included");
}

#[test]
fn a_program_name_carrying_a_path_is_refused_even_if_the_list_holds_it() {
    // The list is keyed on bare names, so a path that ends in an allowed
    // name must not slip through on a substring match.
    let (allowlist, rejected) = CommandAllowlist::resolve(None, &[]);
    assert!(rejected.is_empty());
    for smuggled in ["/bin/sh", "../sh", "usr/bin/cargo", "..\\sh"] {
        validate_command_program(&allowlist, smuggled).expect_err("a path is not a program name");
    }
}

#[test]
fn the_authorization_gate_binds_fetch_to_a_public_https_origin() {
    assert!(public_https_origin("https://docs.rs"));
    assert!(public_https_origin("https://example.com:8443"));
    for refused in [
        "http://example.com",
        "https://localhost",
        "https://127.0.0.1",
        "https://10.1.2.3",
        "https://169.254.169.254",
        "https://[::1]",
        "https://forge.internal",
        "ftp://example.com",
        "not a url",
    ] {
        assert!(
            !public_https_origin(refused),
            "{refused} must not be an authorizable fetch origin"
        );
    }
}

#[test]
fn hostname_refusal_fetch_authorization() {
    for reserved in [
        "localhost",
        "localhost.localdomain",
        "internal",
        "home.arpa",
        "local",
    ] {
        for host in [
            reserved.to_owned(),
            format!("search.{reserved}"),
            format!("nested.search.{reserved}"),
        ] {
            for host in [
                host.clone(),
                host.to_ascii_uppercase(),
                format!("{host}."),
                format!("{}.", host.to_ascii_uppercase()),
            ] {
                assert!(!public_https_origin(&format!("https://{host}")), "{host}");
            }
        }
    }
    for host in [
        "example.internal.com",
        "internal.example.com",
        "home.arpa.example.com",
        "local.example.com",
        "localhost.example.com",
        "localhost.localdomain.example.com",
        "notinternal",
        "notlocal",
    ] {
        assert!(public_https_origin(&format!("https://{host}")), "{host}");
    }
}

pub(super) fn scope(scope_type: CanonicalScopeType, access: WorkspaceAccess) -> CanonicalScope {
    CanonicalScope {
        scope_type,
        scope_id: "scope-1".to_owned(),
        workspace_access: access,
    }
}

pub(super) fn all_permissions() -> BTreeSet<String> {
    BTreeSet::from([
        "read_account".to_owned(),
        "read_project".to_owned(),
        "read_agent_chat".to_owned(),
        "read_task".to_owned(),
        "read_memory".to_owned(),
        "propose_task".to_owned(),
        "propose_message".to_owned(),
        "propose_review".to_owned(),
        "propose_commitment".to_owned(),
        "propose_memory".to_owned(),
        "propose_decision".to_owned(),
        "propose_session".to_owned(),
        "task_read".to_owned(),
        "task_write".to_owned(),
    ])
}

#[test]
fn artifact_scope_content_is_not_mistaken_for_authority_override() {
    assert!(
        reject_authority_overrides(&json!({
            "action": "draft_revision",
            "content": {"scope": {"included": ["checkout"]}},
        }))
        .is_ok()
    );
    assert!(
        reject_authority_overrides(&json!({
            "scope": {"scope_type": "project", "scope_id": "forged"},
        }))
        .is_err()
    );
}

#[tokio::test]
async fn task_read_preparation_rejects_sibling_and_parent_paths() {
    let tool = TaskReadTool;
    let workspace: Arc<dyn Workspace> = Arc::new(TestWorkspace {
        root: "/tmp/forge/task-1".to_owned(),
    });
    let context = PreparationContext {
        session: agent_runtime::core::ids::SessionId::new("session"),
        turn: None,
        call_id: ToolCallId::new("call"),
        request: agent_runtime::core::ids::RequestId::new("request"),
        workspace,
        clock: Arc::new(agent_runtime::core::clock::SystemClock),
        cancel: agent_runtime::core::cancel::Cancellation::new(),
        deadline: agent_runtime::core::clock::Deadline::never(),
    };
    let sibling = tool
        .prepare(json!({"path":"/tmp/forge/task-10/file"}), &context)
        .await;
    assert!(sibling.is_err());
    let parent = tool
        .prepare(json!({"path":"../task-2/file"}), &context)
        .await;
    assert!(parent.is_err());
}

#[tokio::test]
async fn task_list_discovers_real_files_with_a_bounded_globbed_result() {
    let root = tempfile::tempdir().expect("temporary Task Workspace");
    let tests = root.path().join("src/test");
    std::fs::create_dir_all(&tests).expect("test directory");
    std::fs::write(tests.join("profile.test.ts"), "profile\n").expect("profile test");
    std::fs::write(tests.join("cli.test.ts"), "cli\n").expect("cli test");
    std::fs::write(tests.join("fixture.csv"), "a,b\n").expect("fixture");
    let workspace: Arc<dyn Workspace> = Arc::new(TestWorkspace {
        root: root.path().to_string_lossy().into_owned(),
    });
    let tool = TaskListTool;
    let prepared = tool
        .prepare(
            json!({"path":"src/test","glob":"*.test.ts"}),
            &command_preparation_context(workspace.clone()),
        )
        .await
        .expect("list prepares");
    let outcome = tool
        .invoke(prepared, &command_invocation_context(workspace))
        .await
        .expect("list runs");

    assert!(!outcome.is_error);
    assert_eq!(outcome.value["path"], "src/test");
    assert_eq!(outcome.value["truncated"], false);
    assert_eq!(
        outcome.value["entries"],
        json!([
            {"name":"cli.test.ts","path":"src/test/cli.test.ts","kind":"file"},
            {"name":"profile.test.ts","path":"src/test/profile.test.ts","kind":"file"}
        ])
    );
}

#[tokio::test]
async fn task_list_caps_entries_and_task_read_explains_directory_and_missing_paths() {
    let root = tempfile::tempdir().expect("temporary Task Workspace");
    let many = root.path().join("many");
    std::fs::create_dir(&many).expect("many directory");
    std::fs::create_dir_all(root.path().join("src/test")).expect("nested directory");
    for index in 0..(MAX_DIRECTORY_ENTRIES + 2) {
        std::fs::write(many.join(format!("{index:03}.txt")), "x").expect("bounded fixture");
    }
    let workspace: Arc<dyn Workspace> = Arc::new(TestWorkspace {
        root: root.path().to_string_lossy().into_owned(),
    });

    let list = TaskListTool;
    let prepared = list
        .prepare(
            json!({"path":"many"}),
            &command_preparation_context(workspace.clone()),
        )
        .await
        .expect("list prepares");
    let outcome = list
        .invoke(prepared, &command_invocation_context(workspace.clone()))
        .await
        .expect("list runs");
    assert_eq!(
        outcome.value["entries"].as_array().map(Vec::len),
        Some(MAX_DIRECTORY_ENTRIES)
    );
    assert_eq!(outcome.value["truncated"], true);

    let read = TaskReadTool;
    let directory = read
        .prepare(
            json!({"path":"src"}),
            &command_preparation_context(workspace.clone()),
        )
        .await
        .expect("directory read prepares");
    let directory_outcome = read
        .invoke(directory, &command_invocation_context(workspace.clone()))
        .await
        .expect("directory read stays in-band");
    assert!(directory_outcome.is_error);
    assert_eq!(directory_outcome.value["code"], "is_directory");
    assert_eq!(directory_outcome.value["entries"][0]["path"], "src/test");

    let missing = read
        .prepare(
            json!({"path":"src/test/missing.test.ts"}),
            &command_preparation_context(workspace.clone()),
        )
        .await
        .expect("missing read prepares");
    let missing_outcome = read
        .invoke(missing, &command_invocation_context(workspace))
        .await
        .expect("missing read stays in-band");
    assert!(missing_outcome.is_error);
    assert_eq!(missing_outcome.value["code"], "not_found");
    assert_eq!(missing_outcome.value["path"], "src/test/missing.test.ts");
}

#[test]
fn task_list_glob_matches_only_direct_entry_names() {
    assert!(glob_matches("*.test.ts", "profile.test.ts"));
    assert!(glob_matches("cl?.test.ts", "cli.test.ts"));
    assert!(!glob_matches("*.test.ts", "fixture.csv"));
    assert!(!glob_matches("src/*.ts", "profile.test.ts"));
}

#[tokio::test]
async fn forge_scope_read_preserves_structured_provider_errors_in_band() {
    let tool = ForgeScopeReadTool::new(
        "identity-1".to_owned(),
        scope(CanonicalScopeType::Account, WorkspaceAccess::Deny),
        vec!["account.summary".to_owned()],
        Arc::new(StructuredOutcomeProvider),
    );
    let prepared = tool
        .prepare(
            json!({"operation":"account.summary"}),
            &test_preparation_context("read-call"),
        )
        .await
        .expect("read call prepares");
    let expected = structured_version_conflict();
    let outcome = tool
        .invoke(prepared, &test_invocation_context("read-call"))
        .await
        .expect("domain errors remain in-band tool outcomes");

    assert!(outcome.is_error);
    assert_eq!(outcome.value, serde_json::to_value(expected).unwrap());
    assert_eq!(outcome.value["code"], "version_conflict");
    assert_eq!(outcome.value["current_version_or_revision"]["version"], 7);
    assert!(!outcome.value.to_string().contains("sqlx::Error"));
    assert!(!outcome.value.to_string().contains("secret-provider-detail"));
}

#[tokio::test]
async fn forge_scope_propose_preserves_structured_provider_errors_in_band() {
    let tool = ForgeScopeProposeTool::new(
        "identity-1".to_owned(),
        scope(CanonicalScopeType::Project, WorkspaceAccess::Deny),
        vec!["message.send".to_owned()],
        Arc::new(StructuredOutcomeProvider),
    );
    let prepared = tool
        .prepare(
            json!({
                "operation":"message.send",
                "payload": null,
                "dedupe_key":"dedupe-1",
                "correlation_id":"corr-1"
            }),
            &test_preparation_context("propose-call"),
        )
        .await
        .expect("proposal prepares");
    let expected = structured_version_conflict();
    let outcome = tool
        .invoke(prepared, &test_invocation_context("propose-call"))
        .await
        .expect("domain errors remain in-band tool outcomes");

    assert!(outcome.is_error);
    assert_eq!(outcome.value, serde_json::to_value(expected).unwrap());
    assert_eq!(outcome.value["code"], "version_conflict");
    assert_eq!(outcome.value["retry"]["action"], "refresh_and_retry");
    assert!(!outcome.value.to_string().contains("secret-provider-detail"));
}

#[tokio::test]
async fn forge_scope_propose_forwards_the_exact_runtime_session() {
    let provider = Arc::new(TestProvider::default());
    let tool = ForgeScopeProposeTool::new(
        "identity-1".to_owned(),
        scope(CanonicalScopeType::Project, WorkspaceAccess::Deny),
        vec!["message.send".to_owned()],
        provider.clone(),
    );
    let prepared = tool
        .prepare(
            json!({
                "operation":"message.send",
                "payload": null,
                "dedupe_key":"dedupe-session",
                "correlation_id":"corr-session"
            }),
            &test_preparation_context("session-call"),
        )
        .await
        .expect("proposal prepares");
    assert!(prepared.arguments().get("runtime_session_id").is_none());
    let mut invocation = test_invocation_context("session-call");
    invocation.session = agent_runtime::core::ids::SessionId::new("runtime-session-42");

    tool.invoke(prepared, &invocation)
        .await
        .expect("proposal reaches provider");

    assert_eq!(
        *provider
            .proposal_sessions
            .lock()
            .expect("proposal session log"),
        vec!["runtime-session-42".to_owned()]
    );
}

#[tokio::test]
async fn forge_scope_read_keeps_already_structured_success_json_unchanged() {
    let tool = ForgeScopeReadTool::new(
        "identity-1".to_owned(),
        scope(CanonicalScopeType::Account, WorkspaceAccess::Deny),
        vec!["account.summary".to_owned()],
        Arc::new(StructuredSuccessProvider),
    );
    let prepared = tool
        .prepare(
            json!({"operation":"account.summary"}),
            &test_preparation_context("success-call"),
        )
        .await
        .expect("read call prepares");
    let expected = serde_json::to_value(OrchestrationOutcome::succeeded(
        "account.summary",
        CanonicalScopeRef::new(OutcomeScopeType::Account, "scope-1"),
        "corr-success",
        Some(json!({"account_id":"account-1"})),
    ))
    .unwrap();
    let outcome = tool
        .invoke(prepared, &test_invocation_context("success-call"))
        .await
        .expect("structured success is a normal tool result");

    assert!(!outcome.is_error);
    assert_eq!(outcome.value, expected);
}

fn structured_version_conflict() -> OrchestrationOutcome {
    let mut outcome = OrchestrationOutcome::failed(
        OutcomeCode::VersionConflict,
        "account.summary",
        CanonicalScopeRef::new(OutcomeScopeType::Account, "scope-1"),
        "corr-structured",
        "The account changed; refresh and retry.",
    );
    outcome.current_version_or_revision = Some(api_types::CurrentVersionOrRevision {
        resource_type: "account".to_owned(),
        resource_id: "scope-1".to_owned(),
        version: Some(7),
        revision_id: None,
        revision: None,
        content_digest: None,
        rendered_digest: None,
    });
    outcome.retry = Some(api_types::RetryInstruction::new(
        api_types::RetryAction::RefreshAndRetry,
        true,
    ));
    outcome
}

#[derive(Debug, Default)]
struct TerminalDenialProvider {
    cause: Option<DeniedBy>,
    record_failure: bool,
    barrier: Option<tokio::sync::Barrier>,
    succeeds: std::sync::atomic::AtomicBool,
    evaluations: std::sync::atomic::AtomicUsize,
    records: std::sync::atomic::AtomicUsize,
}

#[async_trait]
impl ForgeToolProvider for TerminalDenialProvider {
    async fn read(
        &self,
        _: &str,
        _: &CanonicalScope,
        operation: &str,
        _: Value,
    ) -> Result<Value, AgentHostError> {
        self.evaluations
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if let Some(barrier) = &self.barrier {
            barrier.wait().await;
            return Ok(json!({"done": true}));
        }
        if self.succeeds.load(std::sync::atomic::Ordering::SeqCst) {
            return Ok(json!({"done": true}));
        }
        Err(AgentHostError::StructuredOutcome(Box::new(
            OrchestrationOutcome::terminal_denial(
                operation,
                CanonicalScopeRef::new(OutcomeScopeType::Account, "scope-1"),
                "first-correlation",
                self.cause
                    .clone()
                    .unwrap_or_else(|| DeniedBy::PermissionMissing("read_account".to_owned())),
            ),
        )))
    }

    async fn propose(
        &self,
        _: &str,
        _: &CanonicalScope,
        _: &str,
        _: &str,
        _: Value,
    ) -> Result<Value, AgentHostError> {
        unreachable!("read test")
    }

    async fn record_terminal_denial(
        &self,
        _: &str,
        _: &CanonicalScope,
        _: &str,
        _: &str,
        _: &DeniedBy,
    ) -> Result<(), AgentHostError> {
        self.records
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        if self.record_failure {
            return Err(AgentHostError::ProtectedPersistence);
        }
        Ok(())
    }
}

fn terminal_test_tool(provider: Arc<TerminalDenialProvider>) -> Arc<dyn Tool> {
    ScopeToolComposition::for_scope_with_permissions(
        "actor",
        scope(CanonicalScopeType::Account, WorkspaceAccess::Deny),
        None,
        None,
        &BTreeSet::from(["read_account".to_owned()]),
        Some(provider),
    )
    .unwrap()
    .tools()
    .into_iter()
    .find(|tool| tool.spec().name == "forge_scope_read")
    .unwrap()
}

async fn invoke_terminal_test_tool(tool: &dyn Tool, call_id: &str) -> ToolOutcome {
    let mut preparation = test_preparation_context(call_id);
    preparation.turn = Some(TurnId::new("same-turn"));
    let mut invocation = test_invocation_context(call_id);
    invocation.turn = preparation.turn.clone();
    tool.invoke(
        tool.prepare(json!({"operation":"account.summary"}), &preparation)
            .await
            .unwrap(),
        &invocation,
    )
    .await
    .unwrap()
}

#[tokio::test]
async fn terminal_denial_cache_ignores_request_specific_and_turn_only_causes() {
    use std::sync::atomic::Ordering;
    for cause in [
        DeniedBy::Unspecified,
        DeniedBy::TargetAgentPaused,
        DeniedBy::TaskTerminal,
        DeniedBy::ReviewerReadOnly,
        DeniedBy::IndependentApprovalRequired,
        DeniedBy::UserRequestRequired,
        DeniedBy::LeasedTurnRequired,
        DeniedBy::CharterAdoptionNotApplicable,
        DeniedBy::ReadBoundaryRequired,
        DeniedBy::DirectCommandNotAdmitted,
        DeniedBy::ReviewAssignmentRequired,
        DeniedBy::ProfileNotSelected,
    ] {
        let provider = Arc::new(TerminalDenialProvider {
            cause: Some(cause),
            ..Default::default()
        });
        let tool = terminal_test_tool(provider.clone());
        assert!(invoke_terminal_test_tool(&*tool, "first").await.is_error);
        assert!(invoke_terminal_test_tool(&*tool, "second").await.is_error);
        assert_eq!(provider.evaluations.load(Ordering::SeqCst), 2);
        assert_eq!(provider.records.load(Ordering::SeqCst), 0);
        provider.succeeds.store(true, Ordering::SeqCst);
        assert!(
            !invoke_terminal_test_tool(&*tool, "corrected")
                .await
                .is_error
        );
    }
}

#[tokio::test]
async fn terminal_denial_record_failure_preserves_denial_and_cache() {
    use std::sync::atomic::Ordering;
    let provider = Arc::new(TerminalDenialProvider {
        record_failure: true,
        ..Default::default()
    });
    let tool = terminal_test_tool(provider.clone());
    let first = invoke_terminal_test_tool(&*tool, "first").await;
    let second = invoke_terminal_test_tool(&*tool, "second").await;
    assert!(first.is_error);
    assert_eq!(first.value, second.value);
    assert_eq!(provider.evaluations.load(Ordering::SeqCst), 1);
    assert_eq!(provider.records.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn terminal_denial_cache_allows_parallel_same_operation_evaluations() {
    let provider = Arc::new(TerminalDenialProvider {
        barrier: Some(tokio::sync::Barrier::new(2)),
        ..Default::default()
    });
    let tool = terminal_test_tool(provider);
    let (first, second) = tokio::time::timeout(std::time::Duration::from_secs(5), async {
        tokio::join!(
            invoke_terminal_test_tool(&*tool, "first"),
            invoke_terminal_test_tool(&*tool, "second")
        )
    })
    .await
    .expect("same-operation provider calls must execute concurrently");
    assert!(!first.is_error);
    assert!(!second.is_error);
}

#[tokio::test]
async fn terminal_denial_composition_skips_repeat_evaluation_and_resets_next_turn() {
    use std::sync::atomic::Ordering;
    let provider = Arc::new(TerminalDenialProvider::default());
    let composition = ScopeToolComposition::for_scope_with_permissions(
        "actor",
        scope(CanonicalScopeType::Account, WorkspaceAccess::Deny),
        None,
        None,
        &BTreeSet::from(["read_account".to_owned()]),
        Some(provider.clone()),
    )
    .unwrap();
    let tool = composition
        .tools()
        .into_iter()
        .find(|tool| tool.spec().name == "forge_scope_read")
        .unwrap();
    let catalog = tool.spec().input_schema;
    let mut preparation = test_preparation_context("first");
    preparation.turn = Some(TurnId::new("first-turn"));
    let mut invocation = test_invocation_context("first");
    invocation.turn = preparation.turn.clone();
    let args = json!({"operation":"account.summary"});
    let first = tool
        .invoke(
            tool.prepare(args.clone(), &preparation).await.unwrap(),
            &invocation,
        )
        .await
        .unwrap();
    assert!(first.is_error);
    // A second call has a new call id and still receives the exact original denial.
    preparation.call_id = ToolCallId::new("second");
    invocation.call_id = preparation.call_id.clone();
    let second = tool
        .invoke(
            tool.prepare(args.clone(), &preparation).await.unwrap(),
            &invocation,
        )
        .await
        .unwrap();
    assert_eq!(first.value, second.value);
    assert_eq!(provider.evaluations.load(Ordering::SeqCst), 1);
    assert_eq!(provider.records.load(Ordering::SeqCst), 1);
    // The runtime caches this catalog; Forge deliberately keeps its schema stable.
    assert_eq!(catalog, tool.spec().input_schema);
    preparation.turn = Some(TurnId::new("next-turn"));
    invocation.turn = preparation.turn.clone();
    tool.invoke(tool.prepare(args, &preparation).await.unwrap(), &invocation)
        .await
        .unwrap();
    assert_eq!(provider.evaluations.load(Ordering::SeqCst), 2);
}

pub(super) fn test_preparation_context(call_id: &str) -> PreparationContext {
    PreparationContext {
        session: agent_runtime::core::ids::SessionId::new("session"),
        turn: None,
        call_id: ToolCallId::new(call_id),
        request: agent_runtime::core::ids::RequestId::new("request"),
        workspace: Arc::new(agent_runtime::core::workspace::DenyAllWorkspace),
        clock: Arc::new(agent_runtime::core::clock::SystemClock),
        cancel: agent_runtime::core::cancel::Cancellation::new(),
        deadline: agent_runtime::core::clock::Deadline::never(),
    }
}

pub(super) fn test_invocation_context(call_id: &str) -> InvocationContext {
    InvocationContext {
        session: agent_runtime::core::ids::SessionId::new("session"),
        turn: None,
        call_id: ToolCallId::new(call_id),
        request: agent_runtime::core::ids::RequestId::new("request"),
        workspace: Arc::new(agent_runtime::core::workspace::DenyAllWorkspace),
        clock: Arc::new(agent_runtime::core::clock::SystemClock),
        cancel: agent_runtime::core::cancel::Cancellation::new(),
        deadline: agent_runtime::core::clock::Deadline::never(),
        output_limit: 4096,
    }
}

#[derive(Debug)]
struct StructuredOutcomeProvider;

#[async_trait]
impl ForgeToolProvider for StructuredOutcomeProvider {
    async fn read(
        &self,
        _actor_identity_id: &str,
        _scope: &CanonicalScope,
        _operation: &str,
        _arguments: Value,
    ) -> Result<Value, AgentHostError> {
        Err(AgentHostError::StructuredOutcome(Box::new(
            structured_version_conflict(),
        )))
    }

    async fn propose(
        &self,
        _actor_identity_id: &str,
        _scope: &CanonicalScope,
        _runtime_session_id: &str,
        _operation: &str,
        _arguments: Value,
    ) -> Result<Value, AgentHostError> {
        Err(AgentHostError::StructuredOutcome(Box::new(
            structured_version_conflict(),
        )))
    }
}

#[derive(Debug)]
struct StructuredSuccessProvider;

#[async_trait]
impl ForgeToolProvider for StructuredSuccessProvider {
    async fn read(
        &self,
        _actor_identity_id: &str,
        _scope: &CanonicalScope,
        _operation: &str,
        _arguments: Value,
    ) -> Result<Value, AgentHostError> {
        Ok(serde_json::to_value(OrchestrationOutcome::succeeded(
            "account.summary",
            CanonicalScopeRef::new(OutcomeScopeType::Account, "scope-1"),
            "corr-success",
            Some(json!({"account_id":"account-1"})),
        ))
        .unwrap())
    }

    async fn propose(
        &self,
        _actor_identity_id: &str,
        _scope: &CanonicalScope,
        _runtime_session_id: &str,
        _operation: &str,
        _arguments: Value,
    ) -> Result<Value, AgentHostError> {
        Ok(Value::Null)
    }
}

#[derive(Debug, Default)]
struct TestProvider {
    observations: std::sync::Mutex<Vec<CommandObservation>>,
    proposal_sessions: std::sync::Mutex<Vec<String>>,
}

#[async_trait]
impl ForgeToolProvider for TestProvider {
    async fn observe_command(
        &self,
        _actor_identity_id: &str,
        _scope: &CanonicalScope,
        observation: CommandObservation,
    ) -> Result<Value, AgentHostError> {
        self.observations
            .lock()
            .expect("observation log")
            .push(observation);
        Ok(json!({"observation_id": "observation-1"}))
    }

    async fn read(
        &self,
        _actor_identity_id: &str,
        scope: &CanonicalScope,
        operation: &str,
        _arguments: Value,
    ) -> Result<Value, AgentHostError> {
        Ok(json!({"scope": scope.scope_id, "operation": operation}))
    }

    async fn propose(
        &self,
        _actor_identity_id: &str,
        scope: &CanonicalScope,
        runtime_session_id: &str,
        operation: &str,
        _arguments: Value,
    ) -> Result<Value, AgentHostError> {
        self.proposal_sessions
            .lock()
            .expect("proposal session log")
            .push(runtime_session_id.to_owned());
        Ok(json!({"scope": scope.scope_id, "operation": operation}))
    }
}

#[derive(Debug)]
pub(super) struct ConfiguredSearchProvider;

#[async_trait]
impl ForgeToolProvider for ConfiguredSearchProvider {
    async fn read(
        &self,
        _actor_identity_id: &str,
        _scope: &CanonicalScope,
        _operation: &str,
        _arguments: Value,
    ) -> Result<Value, AgentHostError> {
        Ok(Value::Null)
    }

    async fn propose(
        &self,
        _actor_identity_id: &str,
        _scope: &CanonicalScope,
        _runtime_session_id: &str,
        _operation: &str,
        _arguments: Value,
    ) -> Result<Value, AgentHostError> {
        Ok(Value::Null)
    }

    fn public_search_configured(&self) -> bool {
        true
    }
}

#[derive(Debug)]
struct TestWorkspace {
    root: String,
}

impl Workspace for TestWorkspace {
    fn root(&self) -> &str {
        &self.root
    }

    fn contains(&self, path: &str) -> bool {
        Path::new(path) == Path::new(&self.root)
            || Path::new(path).starts_with(Path::new(&self.root))
    }
}

mod arguments;
mod composition;
