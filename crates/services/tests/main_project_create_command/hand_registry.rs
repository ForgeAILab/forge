//! Literal hand-path receipts from 33e2a974, reached via the real approved
//! Charter/create fixture above and the native milestone-definition command.
use super::*;
use forge_agent_host::{CanonicalScope, CanonicalScopeType, ForgeToolProvider, WorkspaceAccess};
use services::CoordinationToolProvider;

const POLICY: &str = r#"{"permissions":["read_project","read_agent_chat","propose_project","propose_discovery","read_account","handoff"]}"#;

fn provider(db: Arc<SqliteDb>) -> CoordinationToolProvider {
    CoordinationToolProvider::new(db)
}
fn scope(id: &str, scope_type: CanonicalScopeType) -> CanonicalScope {
    CanonicalScope {
        scope_type,
        scope_id: id.into(),
        workspace_access: WorkspaceAccess::Deny,
    }
}
fn envelope(operation: &str, payload: Value) -> Value {
    json!({"operation":operation,"payload":payload,"dedupe_key":format!("e2-{operation}"),"correlation_id":format!("e2-{operation}")})
}

async fn new_real_setup(path: &std::path::Path) -> (Arc<SqliteDb>, String, String) {
    let db = database_with_url(&format!("sqlite://{}?mode=rwc", path.display())).await;
    let f = fixture_with_agent_policies(db.clone(), "native", POLICY, POLICY).await;
    // Identity ceilings are mutable; Profile policies were selected at creation.
    sqlx::query("UPDATE agent_identity SET account_permission_ceiling = ? WHERE id IN (?, ?)")
        .bind(POLICY)
        .bind(MAIN_IDENTITY_ID)
        .bind(PROJECT_IDENTITY_ID)
        .execute(db.pool())
        .await
        .unwrap();
    let execution = MainOrchestrationActionService::new(db.clone())
        .execute(command_input(&f))
        .await
        .unwrap();
    let result: Value = serde_json::from_str(execution.result_json.as_deref().unwrap()).unwrap();
    let project_id = result["project_id"].as_str().unwrap().to_owned();
    let p = provider(db.clone());
    let definition = p.propose(PROJECT_IDENTITY_ID, &scope(&project_id, CanonicalScopeType::Project), "e2", "project.milestone", envelope("project.milestone", json!({
        "action":"define", "lifecycle":"proposed", "display_label":"Delivery",
        "content":{"name":"Delivery","outcome":"One durable Project handoff", "acceptance_checks":[{"id":"delivery","description":"Observe delivery","source_kind":"task_validation","expected_result":"Delivered","required":true}], "evidence_requirements":[{"id":"delivery","description":"Delivery report","required":true,"evidence_kind":"report"}]}
    }))).await.unwrap();
    assert_eq!(definition["code"], "approval_required", "{definition}");
    let milestone_id = definition["result"]["domain_result"]["milestone_id"]
        .as_str()
        .unwrap()
        .to_owned();
    (db, project_id, milestone_id)
}

async fn capture_evidence_case() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("checkpoint.sqlite");
    if let Some(dir) = std::env::var_os("FORGE_E2_CAPTURE_DIR").map(PathBuf::from) {
        assert!(
            operation_registry::PROPOSAL_CATALOG
                .lookup("project.evidence")
                .is_none(),
            "capture the hand path before moving it"
        );
        let (db, project_id, milestone_id) = new_real_setup(&path).await;
        let milestone_version: i64 =
            sqlx::query_scalar("SELECT version FROM project_milestone WHERE id = ?")
                .bind(&milestone_id)
                .fetch_one(db.pool())
                .await
                .unwrap();
        let arguments = envelope(
            "project.evidence",
            json!({"action":"capture","milestone_id":milestone_id,"expected_milestone_version":milestone_version,"acceptance_check_ids":["delivery"],"caption":"Observed delivery","kind":"report","content":"Delivered","filename":"proof.txt"}),
        );
        let call_scope = scope(&project_id, CanonicalScopeType::Project);
        let p = provider(db.clone());
        p.set_media_root(temp.path().join("media"));
        let first = p
            .propose(
                PROJECT_IDENTITY_ID,
                &call_scope,
                "e2",
                "project.evidence",
                arguments.clone(),
            )
            .await
            .unwrap();
        assert_eq!(first["code"], "ok", "{first}");
        assert!(first["receipt_id"].is_string(), "{first}");
        let checkpoint = json!({"base":"33e2a9740adfd69e29850dcde9c9cde28575d874","project_id":project_id,"milestone_id":milestone_id,"project.evidence":{"actor":PROJECT_IDENTITY_ID,"scope_type":"project","scope_id":call_scope.scope_id,"arguments":arguments,"first":first}});
        std::fs::write(
            dir.join("hand_registry_base.json"),
            serde_json::to_vec_pretty(&checkpoint).unwrap(),
        )
        .unwrap();
        db.pool().close().await;
        std::fs::copy(path, dir.join("hand_registry_base.sqlite")).unwrap();
        return;
    }
    replay_case("project.evidence").await;
}

async fn replay_case(operation: &str) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("checkpoint.sqlite");
    std::fs::write(
        &path,
        include_bytes!("../fixtures/hand_registry_base.sqlite"),
    )
    .unwrap();
    let db = Arc::new(SqliteDb::new(
        create_sqlite_pool(&format!("sqlite://{}", path.display()))
            .await
            .unwrap(),
    ));
    let checkpoint: Value =
        serde_json::from_str(include_str!("../fixtures/hand_registry_base.json")).unwrap();
    let case = &checkpoint[operation];
    let call_scope = scope(
        case["scope_id"].as_str().unwrap(),
        if case["scope_type"] == "project" {
            CanonicalScopeType::Project
        } else {
            CanonicalScopeType::AgentChat
        },
    );
    let p = provider(db.clone());
    p.set_media_root(temp.path().join("media"));
    let receipt_before: (String, String, String) = sqlx::query_as(
        "SELECT input_digest, outcome_json, event_id FROM command_receipt WHERE id = ?",
    )
    .bind(case["first"]["receipt_id"].as_str().unwrap())
    .fetch_one(db.pool())
    .await
    .unwrap();
    let replay = p
        .propose(
            case["actor"].as_str().unwrap(),
            &call_scope,
            "e2",
            case["arguments"]["operation"].as_str().unwrap(),
            case["arguments"].clone(),
        )
        .await
        .unwrap();
    let mut expected = case["replay"]
        .as_object()
        .map(|_| case["replay"].clone())
        .unwrap_or_else(|| case["first"].clone());
    if case["replay"].is_null() {
        expected["replayed"] = json!(true);
        expected["result"]["replayed"] = json!(true);
    }
    assert_eq!(
        replay, expected,
        "every base result byte except replay marker"
    );
    let receipt_after: (String, String, String) = sqlx::query_as(
        "SELECT input_digest, outcome_json, event_id FROM command_receipt WHERE id = ?",
    )
    .bind(case["first"]["receipt_id"].as_str().unwrap())
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(receipt_before, receipt_after);
}
#[tokio::test]
async fn evidence_replays_literal_hand_receipt() {
    capture_evidence_case().await;
}

async fn open_checkpoint(path: &std::path::Path) -> Arc<SqliteDb> {
    std::fs::write(
        path,
        include_bytes!("../fixtures/hand_registry_base.sqlite"),
    )
    .unwrap();
    Arc::new(SqliteDb::new(
        create_sqlite_pool(&format!("sqlite://{}", path.display()))
            .await
            .unwrap(),
    ))
}
async fn authority_table(db: Arc<SqliteDb>, case: &Value, media_root: PathBuf) -> Value {
    let actor = case["actor"].as_str().unwrap();
    let project_id: String = sqlx::query_scalar("SELECT id FROM project LIMIT 1")
        .fetch_one(db.pool())
        .await
        .unwrap();
    let main_chat: String =
        sqlx::query_scalar("SELECT id FROM agent_chat WHERE kind='account_main'")
            .fetch_one(db.pool())
            .await
            .unwrap();
    let project_chat: String = sqlx::query_scalar("SELECT id FROM agent_chat WHERE kind='project'")
        .fetch_one(db.pool())
        .await
        .unwrap();
    create_identity(
        &db,
        "e2-unbound",
        "e2-unbound-profile",
        "Owned unbound identity",
        POLICY,
        "native",
    )
    .await;
    sqlx::query("UPDATE agent_identity SET account_permission_ceiling=? WHERE id='e2-unbound'")
        .bind(POLICY)
        .execute(db.pool())
        .await
        .unwrap();
    let p = provider(db.clone());
    p.set_media_root(media_root);
    let mut table = serde_json::Map::new();
    for profile in ["granted", "restricted"] {
        // Replace the identity's effective ceiling; never edit an immutable Profile.
        let permissions = if profile == "granted" { POLICY } else { "{}" };
        sqlx::query("UPDATE agent_identity SET account_permission_ceiling=?")
            .bind(permissions)
            .execute(db.pool())
            .await
            .unwrap();
        for state in ["ready", "paused"] {
            sqlx::query("UPDATE agent_identity SET paused=?")
                .bind(i64::from(state == "paused"))
                .execute(db.pool())
                .await
                .unwrap();
            for caller in [
                actor,
                "e2-unbound",
                if actor == PROJECT_IDENTITY_ID {
                    MAIN_IDENTITY_ID
                } else {
                    PROJECT_IDENTITY_ID
                },
            ] {
                for (name, call_scope) in [
                    ("account", scope(ACCOUNT_ID, CanonicalScopeType::Account)),
                    (
                        "main_chat",
                        scope(&main_chat, CanonicalScopeType::AgentChat),
                    ),
                    ("project", scope(&project_id, CanonicalScopeType::Project)),
                    (
                        "project_chat",
                        scope(&project_chat, CanonicalScopeType::AgentChat),
                    ),
                    ("task", scope("unassigned", CanonicalScopeType::Task)),
                ] {
                    let outcome = p
                        .propose(
                            caller,
                            &call_scope,
                            "e2",
                            case["arguments"]["operation"].as_str().unwrap(),
                            case["arguments"].clone(),
                        )
                        .await;
                    table.insert(
                        format!("{caller}/{profile}/{state}/{name}"),
                        json!(outcome.is_ok_and(|value| matches!(
                            value["code"].as_str(),
                            Some("ok" | "approval_required")
                        ))),
                    );
                }
            }
        }
    }
    table.into()
}

#[tokio::test]
async fn evidence_attach_and_authority_match_hand_base() {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("checkpoint.sqlite");
    let db = open_checkpoint(&path).await;
    let mut checkpoint: Value =
        serde_json::from_str(include_str!("../fixtures/hand_registry_base.json")).unwrap();
    if let Some(dir) = std::env::var_os("FORGE_E2_CAPTURE_DIR").map(PathBuf::from) {
        let original = &checkpoint["project.evidence"];
        let mut arguments = original["arguments"].clone();
        let payload = arguments["payload"].as_object_mut().unwrap();
        payload.remove("content");
        payload.remove("filename");
        payload.insert("action".into(), json!("attach"));
        let asset: (String, String) =
            sqlx::query_as("SELECT id,checksum FROM media_asset WHERE project_id=?")
                .bind(checkpoint["project_id"].as_str().unwrap())
                .fetch_one(db.pool())
                .await
                .unwrap();
        payload.insert("asset_id".into(), json!(asset.0));
        payload.insert("checksum".into(), json!(asset.1));
        arguments["dedupe_key"] = json!("e2-evidence-attach");
        let version: i64 = sqlx::query_scalar("SELECT version FROM project_milestone WHERE id=?")
            .bind(checkpoint["milestone_id"].as_str().unwrap())
            .fetch_one(db.pool())
            .await
            .unwrap();
        arguments["payload"]["expected_milestone_version"] = json!(version);
        let call_scope = scope(
            checkpoint["project_id"].as_str().unwrap(),
            CanonicalScopeType::Project,
        );
        let first = provider(db.clone())
            .propose(
                PROJECT_IDENTITY_ID,
                &call_scope,
                "e2",
                "project.evidence",
                arguments.clone(),
            )
            .await
            .unwrap();
        checkpoint["project.evidence.attach"] = json!({"actor":PROJECT_IDENTITY_ID,"scope_type":"project","scope_id":call_scope.scope_id,"arguments":arguments,"first":first});
        // Capture the completed domain checkpoint before policy-only mutations.
        sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(db.pool())
            .await
            .unwrap();
        std::fs::copy(&path, dir.join("hand_registry_base.sqlite")).unwrap();
        checkpoint["project.evidence"]["authority"] = authority_table(
            db.clone(),
            &checkpoint["project.evidence"],
            temp.path().join("media"),
        )
        .await;
        std::fs::write(
            dir.join("hand_registry_base.json"),
            serde_json::to_vec_pretty(&checkpoint).unwrap(),
        )
        .unwrap();
    } else {
        let actual = authority_table(
            db.clone(),
            &checkpoint["project.evidence"],
            temp.path().join("media"),
        )
        .await;
        assert_eq!(
            actual, checkpoint["project.evidence"]["authority"],
            "no widening or narrowing on principal/profile/state/scope"
        );
        replay_case("project.evidence.attach").await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn capture_case(
    db: Arc<SqliteDb>,
    path: &std::path::Path,
    dir: &std::path::Path,
    checkpoint: &mut Value,
    key: &str,
    actor: &str,
    call_scope: CanonicalScope,
    arguments: Value,
) {
    let operation = arguments["operation"].as_str().unwrap();
    assert!(
        operation_registry::PROPOSAL_CATALOG
            .lookup(operation)
            .is_none(),
        "capture the hand path first"
    );
    let p = provider(db.clone());
    let first = p
        .propose(actor, &call_scope, "e2", operation, arguments.clone())
        .await
        .unwrap();
    assert!(first["receipt_id"].is_string(), "{first}");
    let replay = p
        .propose(actor, &call_scope, "e2", operation, arguments.clone())
        .await
        .unwrap();
    assert_eq!(first["receipt_id"], replay["receipt_id"]);
    assert_eq!(first["event_id"], replay["event_id"]);
    let mut domain = first["result"]["domain_result"].clone();
    if domain.get("replayed").is_some() {
        domain["replayed"] = replay["result"]["domain_result"]["replayed"].clone();
    }
    assert_eq!(domain, replay["result"]["domain_result"]);
    checkpoint[key] = json!({"actor":actor,"scope_type":if call_scope.scope_type==CanonicalScopeType::Project {"project"}else{"agent_chat"},"scope_id":call_scope.scope_id,"arguments":arguments,"first":first,"replay":replay});
    sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
        .execute(db.pool())
        .await
        .unwrap();
    std::fs::copy(path, dir.join("hand_registry_base.sqlite")).unwrap();
    std::fs::write(
        dir.join("hand_registry_base.json"),
        serde_json::to_vec_pretty(checkpoint).unwrap(),
    )
    .unwrap();
}

#[tokio::test]
async fn adoption_and_amendment_replay_literal_hand_receipts() {
    let Some(dir) = std::env::var_os("FORGE_E2_CAPTURE_DIR").map(PathBuf::from) else {
        replay_case("project.charter.adoption").await;
        replay_case("project.charter.adoption.setup").await;
        let temp = tempfile::tempdir().unwrap();
        let db = open_checkpoint(&temp.path().join("checkpoint.sqlite")).await;
        let checkpoint: Value =
            serde_json::from_str(include_str!("../fixtures/hand_registry_base.json")).unwrap();
        let actual = authority_table(
            db,
            &checkpoint["project.charter.adoption"],
            temp.path().join("media"),
        )
        .await;
        assert_eq!(actual, checkpoint["project.charter.adoption"]["authority"]);
        return;
    };
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("checkpoint.sqlite");
    let db = open_checkpoint(&path).await;
    let mut checkpoint: Value =
        serde_json::from_str(include_str!("../fixtures/hand_registry_base.json")).unwrap();
    let project_id = checkpoint["project_id"].as_str().unwrap().to_owned();
    let version: i64 = sqlx::query_scalar("SELECT version FROM project_charter WHERE id=?")
        .bind(CHARTER_ID)
        .fetch_one(db.pool())
        .await
        .unwrap();
    let mut content = serde_json::to_value(charter_content()).unwrap();
    content["identity"]["value_proposition"] =
        json!("Retain verified evidence for the durable Main-to-Project handoff.");
    let payload = json!({"action":"draft_revision","charter_id":CHARTER_ID,"base_revision_id":CHARTER_REVISION_ID,"expected_charter_version":version,"project_mode":"compact","maturity":"mvp","content":content,"provenance":{"author":{"kind":"agent","id":PROJECT_IDENTITY_ID},"change_summary":"Draft evidence-retention amendment"}});
    capture_case(
        db.clone(),
        &path,
        &dir,
        &mut checkpoint,
        "project.charter.adoption",
        PROJECT_IDENTITY_ID,
        scope(&project_id, CanonicalScopeType::Project),
        envelope("project.charter.adoption", payload),
    )
    .await;
    db::ProjectRepo::create_with_agent_binding(
        &*db,
        db::CreateProject {
            id: "e2-legacy".into(),
            name: "Legacy Project".into(),
            settings: "{}".into(),
            workflow_definition: "{}".into(),
            primary_repo_id: None,
            owner_id: Some(ACCOUNT_ID.into()),
            created_at: NOW.into(),
            updated_at: NOW.into(),
        },
        Some(PROJECT_IDENTITY_ID.into()),
        Some(PROJECT_PROFILE_ID.into()),
    )
    .await
    .unwrap();
    let payload = json!({"action":"draft_revision","expected_charter_version":0,"project_mode":"compact","maturity":"mvp","content":charter_content(),"provenance":{"author":{"kind":"agent","id":PROJECT_IDENTITY_ID},"change_summary":"Draft legacy adoption"}});
    let mut arguments = envelope("project.charter.adoption", payload);
    arguments["dedupe_key"] = json!("e2-adoption-setup");
    capture_case(
        db.clone(),
        &path,
        &dir,
        &mut checkpoint,
        "project.charter.adoption.setup",
        PROJECT_IDENTITY_ID,
        scope("e2-legacy", CanonicalScopeType::Project),
        arguments,
    )
    .await;
    checkpoint["project.charter.adoption"]["authority"] = authority_table(
        db.clone(),
        &checkpoint["project.charter.adoption"],
        temp.path().join("media"),
    )
    .await;
    std::fs::write(
        dir.join("hand_registry_base.json"),
        serde_json::to_vec_pretty(&checkpoint).unwrap(),
    )
    .unwrap();
}

#[tokio::test]
async fn main_draft_replays_literal_hand_receipt() {
    let Some(dir) = std::env::var_os("FORGE_E2_CAPTURE_DIR").map(PathBuf::from) else {
        replay_case("charter.draft").await;
        let temp = tempfile::tempdir().unwrap();
        let db = open_checkpoint(&temp.path().join("checkpoint.sqlite")).await;
        let checkpoint: Value =
            serde_json::from_str(include_str!("../fixtures/hand_registry_base.json")).unwrap();
        assert_eq!(
            authority_table(db, &checkpoint["charter.draft"], temp.path().join("media")).await,
            checkpoint["charter.draft"]["authority"]
        );
        return;
    };
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("checkpoint.sqlite");
    let db = open_checkpoint(&path).await;
    let mut checkpoint: Value =
        serde_json::from_str(include_str!("../fixtures/hand_registry_base.json")).unwrap();
    let main_chat: String =
        sqlx::query_scalar("SELECT id FROM agent_chat WHERE kind='account_main'")
            .fetch_one(db.pool())
            .await
            .unwrap();
    let started = ProductGenesisService::for_sqlite(db.clone())
        .start(
            ACCOUNT_ID,
            Some(&main_chat),
            ProductMaturity::Mvp,
            Some("Create another durable Project".into()),
            Some(PROJECT_IDENTITY_ID.into()),
            GenesisPromptContext::default(),
        )
        .await
        .unwrap();
    let payload = json!({"action":"save_revision","genesis_session_id":started.session.id,"charter_id":"e2-main-charter","expected_charter_version":1,"project_mode":"compact","maturity":"mvp","content":charter_content(),"provenance":{"author":{"kind":"agent","id":MAIN_IDENTITY_ID},"change_summary":"Draft the next Project Charter"}});
    capture_case(
        db.clone(),
        &path,
        &dir,
        &mut checkpoint,
        "charter.draft",
        MAIN_IDENTITY_ID,
        scope(&main_chat, CanonicalScopeType::AgentChat),
        envelope("charter.draft", payload),
    )
    .await;
    checkpoint["charter.draft"]["authority"] = authority_table(
        db.clone(),
        &checkpoint["charter.draft"],
        temp.path().join("media"),
    )
    .await;
    std::fs::write(
        dir.join("hand_registry_base.json"),
        serde_json::to_vec_pretty(&checkpoint).unwrap(),
    )
    .unwrap();
}

async fn replaced_main_binding_replay(operation: &str) {
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("checkpoint.sqlite");
    let db = open_checkpoint(&path).await;
    let checkpoint: Value =
        serde_json::from_str(include_str!("../fixtures/hand_registry_base.json")).unwrap();
    let case = &checkpoint[operation];
    create_identity(
        &db,
        "e2-replacement-main",
        "e2-replacement-profile",
        "Replacement Main",
        POLICY,
        "native",
    )
    .await;
    sqlx::query(
        "UPDATE agent_identity SET account_permission_ceiling=? WHERE id='e2-replacement-main'",
    )
    .bind(POLICY)
    .execute(db.pool())
    .await
    .unwrap();
    sqlx::query("UPDATE account_main_agent_binding SET identity_id='e2-replacement-main', profile_id='e2-replacement-profile', version=version+1 WHERE state='active'").execute(db.pool()).await.unwrap();
    let call_scope = scope(
        case["scope_id"].as_str().unwrap(),
        CanonicalScopeType::AgentChat,
    );
    let p = provider(db);
    let replay = p
        .propose(
            MAIN_IDENTITY_ID,
            &call_scope,
            "e2",
            operation,
            case["arguments"].clone(),
        )
        .await
        .unwrap();
    assert_eq!(
        replay, case["replay"],
        "the base returns this immutable receipt after replacement of its Main binding"
    );
    let mut fresh = case["arguments"].clone();
    fresh["dedupe_key"] = json!("e2-unbound-fresh");
    assert!(
        p.propose(MAIN_IDENTITY_ID, &call_scope, "e2", operation, fresh)
            .await
            .is_err(),
        "replacement never grants a new effect to the unbound caller"
    );
}
#[tokio::test]
async fn main_draft_owned_unbound_replay_preserves_base() {
    replaced_main_binding_replay("charter.draft").await;
}

#[tokio::test]
async fn main_draft_contract_matches_domain_fields_and_refuses_unknowns() {
    let checkpoint: Value =
        serde_json::from_str(include_str!("../fixtures/hand_registry_base.json")).unwrap();
    let case = &checkpoint["charter.draft"];
    let mut payload = case["arguments"]["payload"].clone();
    payload.as_object_mut().unwrap().remove("action");
    let request: services::MainGenesisCharterDraftRequest =
        serde_json::from_value(payload).unwrap();
    let fields = serde_json::to_value(request).unwrap();
    let spec = operation_registry::PROPOSAL_CATALOG
        .lookup("charter.draft")
        .unwrap();
    assert_eq!(
        fields.as_object().unwrap().keys().collect::<Vec<_>>(),
        spec.input.schema["properties"]
            .as_object()
            .unwrap()
            .keys()
            .collect::<Vec<_>>()
    );
    let temp = tempfile::tempdir().unwrap();
    let db = open_checkpoint(&temp.path().join("checkpoint.sqlite")).await;
    let p = provider(db);
    let call_scope = scope(
        case["scope_id"].as_str().unwrap(),
        CanonicalScopeType::AgentChat,
    );
    for root in [false, true] {
        let mut arguments = case["arguments"].clone();
        if root {
            arguments["unexpected"] = json!(true);
        } else {
            arguments["payload"]["unexpected"] = json!(true);
        }
        let error = p
            .propose(
                MAIN_IDENTITY_ID,
                &call_scope,
                "e2",
                "charter.draft",
                arguments,
            )
            .await
            .unwrap_err();
        let text = format!("{error:?}");
        assert!(
            text.contains("unexpected") && text.contains(&spec.contract_line()),
            "{text}"
        );
        assert_eq!(text.matches("expected charter.draft:").count(), 1, "{text}");
    }
}

#[tokio::test]
async fn genesis_start_replays_literal_hand_receipt() {
    let Some(dir) = std::env::var_os("FORGE_E2_CAPTURE_DIR").map(PathBuf::from) else {
        replay_case("genesis.start").await;
        let temp = tempfile::tempdir().unwrap();
        let db = open_checkpoint(&temp.path().join("checkpoint.sqlite")).await;
        let checkpoint: Value =
            serde_json::from_str(include_str!("../fixtures/hand_registry_base.json")).unwrap();
        assert_eq!(
            authority_table(db, &checkpoint["genesis.start"], temp.path().join("media")).await,
            checkpoint["genesis.start"]["authority"]
        );
        return;
    };
    let temp = tempfile::tempdir().unwrap();
    let path = temp.path().join("checkpoint.sqlite");
    let db = open_checkpoint(&path).await;
    let mut checkpoint: Value =
        serde_json::from_str(include_str!("../fixtures/hand_registry_base.json")).unwrap();
    let genesis = ProductGenesisService::for_sqlite(db.clone());
    let id = checkpoint["charter.draft"]["arguments"]["payload"]["genesis_session_id"]
        .as_str()
        .unwrap();
    let current = genesis.get(id).await.unwrap();
    genesis
        .cancel(
            id,
            current.version,
            Some("Start the next explicit Project request".into()),
        )
        .await
        .unwrap();
    sqlx::query("UPDATE agent_chat_turn_job SET status='leased',version=version+1 WHERE id='main-command-source-turn'").execute(db.pool()).await.unwrap();
    let main_chat: String =
        sqlx::query_scalar("SELECT id FROM agent_chat WHERE kind='account_main'")
            .fetch_one(db.pool())
            .await
            .unwrap();
    let arguments = envelope(
        "genesis.start",
        json!({"action":"start","maturity":"mvp","preferred_project_agent_identity_id":PROJECT_IDENTITY_ID}),
    );
    capture_case(
        db.clone(),
        &path,
        &dir,
        &mut checkpoint,
        "genesis.start",
        MAIN_IDENTITY_ID,
        scope(&main_chat, CanonicalScopeType::AgentChat),
        arguments,
    )
    .await;
    checkpoint["genesis.start"]["authority"] = authority_table(
        db.clone(),
        &checkpoint["genesis.start"],
        temp.path().join("media"),
    )
    .await;
    std::fs::write(
        dir.join("hand_registry_base.json"),
        serde_json::to_vec_pretty(&checkpoint).unwrap(),
    )
    .unwrap();
}
#[tokio::test]
async fn genesis_start_owned_unbound_replay_preserves_base() {
    replaced_main_binding_replay("genesis.start").await;
}

#[tokio::test]
async fn genesis_start_contract_is_native_only_and_refuses_unknowns() {
    let spec = operation_registry::PROPOSAL_CATALOG
        .lookup("genesis.start")
        .unwrap();
    assert_eq!(
        spec.input.schema["properties"]
            .as_object()
            .unwrap()
            .keys()
            .map(String::as_str)
            .collect::<Vec<_>>(),
        ["maturity", "preferred_project_agent_identity_id"]
    );
    spec.validate_arguments(&json!({})).unwrap();
    let checkpoint: Value =
        serde_json::from_str(include_str!("../fixtures/hand_registry_base.json")).unwrap();
    let case = &checkpoint["genesis.start"];
    let temp = tempfile::tempdir().unwrap();
    let db = open_checkpoint(&temp.path().join("checkpoint.sqlite")).await;
    let p = provider(db);
    let call_scope = scope(
        case["scope_id"].as_str().unwrap(),
        CanonicalScopeType::AgentChat,
    );
    for root in [false, true] {
        let mut arguments = case["arguments"].clone();
        if root {
            arguments["unexpected"] = json!(true);
        } else {
            arguments["payload"]["unexpected"] = json!(true);
        }
        let error = p
            .propose(
                MAIN_IDENTITY_ID,
                &call_scope,
                "e2",
                "genesis.start",
                arguments,
            )
            .await
            .unwrap_err();
        let text = format!("{error:?}");
        assert!(
            text.contains("unexpected") && text.contains(&spec.contract_line()),
            "{text}"
        );
        assert_eq!(text.matches("expected genesis.start:").count(), 1, "{text}");
    }
}
