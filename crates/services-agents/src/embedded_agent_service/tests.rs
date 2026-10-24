//! Tests for embedded providers, native capabilities, sessions, and authority.

#[test]
fn non_chat_host_errors_keep_their_service_variants() {
    assert!(matches!(
        redacted_host_error(forge_agent_host::AgentHostError::CredentialNotFound),
        ServiceError::NotFound {
            entity: "protected_runtime_resource",
            ..
        }
    ));
    assert!(matches!(
        redacted_host_error(forge_agent_host::AgentHostError::Configuration(
            "invalid".into()
        )),
        ServiceError::InvalidOperation { .. }
    ));
}

#[test]
fn native_task_capabilities_keep_sessions_but_disable_lcm() {
    let profile = AgentProfile {
        id: "profile-1".to_owned(),
        identity_id: "agent-1".to_owned(),
        backend_kind: "native".to_owned(),
        executor_type: NATIVE_EXECUTOR_TYPE.to_owned(),
        provider: Some("test".to_owned()),
        model: Some("model".to_owned()),
        reasoning_effort: None,
        permission_policy: None,
        prompt_template: None,
        capabilities_json: "{}".to_owned(),
        tool_policy_json: "{}".to_owned(),
        config_json: "{}".to_owned(),
        credential_ref: None,
        daemon_id: None,
        version: 1,
        created_at: "2026-09-25T00:00:00Z".to_owned(),
        updated_at: "2026-09-25T00:00:00Z".to_owned(),
    };
    let capabilities = capabilities_for_profile(
        &profile,
        &CanonicalScope {
            scope_type: CanonicalScopeType::Task,
            scope_id: "task-1".to_owned(),
            workspace_access: WorkspaceAccess::TaskWrite,
        },
    );
    assert!(capabilities.persistent_session);
    assert!(capabilities.protected_checkpoints);
    assert!(!capabilities.lcm);
}

#[test]
fn generic_input_budget_uses_the_whole_context_window() {
    // A Project Agent turn failed `budget_exceeded` at 97,747 of 96,000
    // while its own profile advertised a 128k context window, because the
    // input budget was a flat constant unrelated to that window. The
    // budget must follow the window, so the turn that failed now fits.
    let (context, max_input, max_output) = native_model_limits("openai");
    assert_eq!(context, DEFAULT_CONTEXT_TOKENS);
    assert_eq!(max_output, DEFAULT_MAX_OUTPUT_TOKENS);
    assert_eq!(max_input, context - max_output);
    assert!(
        max_input > 97_747,
        "the turn that failed at 97,747 tokens must now fit, got {max_input}"
    );

    // Gemini keeps its bespoke arm.
    assert_eq!(native_model_limits("gemini"), (1_048_576, 800_000, 64_000));
}

#[test]
fn a_profile_carrying_the_baked_default_triple_is_relifted() {
    // Profiles are immutable, so agents created before this carry the old
    // triple verbatim. That exact triple can only be a baked default, and
    // must resolve to the provider limits rather than cap the model.
    let stored = (
        DEFAULT_CONTEXT_TOKENS,
        DEFAULT_MAX_INPUT_TOKENS,
        DEFAULT_MAX_OUTPUT_TOKENS,
    );
    let (_, max_input, _) = effective_native_limits("openai", stored.0, stored.1, stored.2);
    assert_eq!(
        max_input,
        DEFAULT_CONTEXT_TOKENS - DEFAULT_MAX_OUTPUT_TOKENS
    );

    // A deliberate user choice is still honoured verbatim.
    assert_eq!(
        effective_native_limits("openai", 200_000, 50_000, 8_000),
        (200_000, 50_000, 8_000)
    );
}
use super::*;

#[tokio::test]
async fn provider_base_url_rejects_credential_bearing_query_before_resolution() {
    let error = provider_url_addresses("https://example.invalid/v1?api_key=SENTINEL")
        .await
        .expect_err("query parameters are not safe provider endpoint identity");
    assert_eq!(
        error,
        "base_url must not contain userinfo, a query, or a fragment"
    );
}

#[tokio::test]
async fn frozen_chat_session_rotates_after_a_new_profile_is_admitted() {
    let (service, owner) = test_service_with_owner().await;
    let identity_id = new_uuid_v4();
    let first_profile_id = new_uuid_v4();
    let now = now_rfc3339();
    let tool_policy = json!({"allowed": ["read_account"]}).to_string();
    AgentRepo::create_identity_with_profile(
        &*service.db,
        CreateAgentIdentity {
            id: identity_id.clone(),
            name: "profile-rotation-main".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some(owner.clone()),
            visibility: "account".to_owned(),
            account_permission_ceiling: json!({"permissions": ["read_account"]}).to_string(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: first_profile_id.clone(),
            identity_id: identity_id.clone(),
            backend_kind: "native".to_owned(),
            executor_type: NATIVE_EXECUTOR_TYPE.to_owned(),
            provider: Some("test".to_owned()),
            model: Some("model-one".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: tool_policy.clone(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("identity creates");
    let binding = crate::AgentChatService::new(Arc::clone(&service.db))
        .set_main_binding(crate::SetMainAgentBindingInput {
            actor_user_id: owner.clone(),
            account_id: owner.clone(),
            identity_id: identity_id.clone(),
            autonomy_policy_json: "{}".to_owned(),
            tool_policy_revision: "profile-rotation-policy".to_owned(),
            expected_version: None,
            replacement_reason: None,
        })
        .await
        .expect("Main binding creates");
    let chat = AgentChatRepo::get_main_chat(&*service.db, &owner)
        .await
        .expect("Main Chat lookup")
        .expect("Main Chat exists");
    let policy_digest = crate::agent_turn_admission::policy_digest(&binding.autonomy_policy_json)
        .expect("policy digest");
    let tool_policy_digest =
        crate::agent_turn_admission::policy_digest(&tool_policy).expect("tool policy digest");

    let first = service
        .create_or_resume_frozen_chat_session(CreateFrozenAgentChatSession {
            actor_user_id: owner.clone(),
            identity_id: identity_id.clone(),
            profile_id: first_profile_id,
            profile_version: 1,
            binding_id: binding.id.clone(),
            binding_version: binding.version,
            policy_revision: binding.tool_policy_revision.clone(),
            policy_digest: policy_digest.clone(),
            permission_policy_digest: "permission-digest".to_owned(),
            tool_policy_digest: tool_policy_digest.clone(),
            scope: RequestedCanonicalScope::AgentChat {
                chat_id: chat.id.clone(),
            },
        })
        .await
        .expect("first frozen session creates");

    let selected_identity = AgentRepo::get_by_id(&*service.db, &identity_id)
        .await
        .expect("identity lookup")
        .expect("identity exists");
    let second_profile_id = new_uuid_v4();
    let later = now_rfc3339();
    let (second_profile, _) = AgentProfileRepo::create_and_select_profile(
        &*service.db,
        CreateAgentProfile {
            id: second_profile_id.clone(),
            identity_id: identity_id.clone(),
            backend_kind: "native".to_owned(),
            executor_type: NATIVE_EXECUTOR_TYPE.to_owned(),
            provider: Some("test".to_owned()),
            model: Some("model-two".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: tool_policy,
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: later.clone(),
            updated_at: later.clone(),
        },
        SelectAgentProfile {
            identity_id: identity_id.clone(),
            profile_id: second_profile_id.clone(),
            expected_version: selected_identity.version,
            updated_at: later,
        },
    )
    .await
    .expect("new Profile is selected");

    let second = service
        .create_or_resume_frozen_chat_session(CreateFrozenAgentChatSession {
            actor_user_id: owner,
            identity_id,
            profile_id: second_profile_id.clone(),
            profile_version: second_profile.version,
            binding_id: binding.id,
            binding_version: binding.version,
            policy_revision: binding.tool_policy_revision,
            policy_digest,
            permission_policy_digest: "permission-digest".to_owned(),
            tool_policy_digest,
            scope: RequestedCanonicalScope::AgentChat { chat_id: chat.id },
        })
        .await
        .expect("newly admitted Profile rotates the chat session");

    assert_eq!(second.profile_id, second_profile_id);
    assert_eq!(
        second.predecessor_session_id.as_deref(),
        Some(first.id.as_str())
    );
    let previous = AgentSessionRepo::get_agent_session(&*service.db, &first.id)
        .await
        .expect("previous session lookup")
        .expect("previous session exists");
    assert_eq!(previous.status, "replaced");
    let scopes = AgentContextScopeRepo::list_context_scopes(&*service.db, &previous.identity_id)
        .await
        .expect("context scopes list");
    assert_eq!(scopes.len(), 1);
    assert!(!scopes[0].authority_json.contains("frozen_profile_id"));
    assert!(!scopes[0].authority_json.contains("frozen_binding_id"));
}

struct NativeChatFixture {
    service: EmbeddedAgentService,
    owner: String,
    identity_id: String,
    chat_id: String,
}

impl NativeChatFixture {
    async fn resume(&self) -> Result<AgentSession> {
        self.service
            .create_or_resume_session(CreateScopedSession {
                actor_user_id: self.owner.clone(),
                identity_id: self.identity_id.clone(),
                profile_id: None,
                scope: RequestedCanonicalScope::AgentChat {
                    chat_id: self.chat_id.clone(),
                },
            })
            .await
    }

    async fn session(&self, id: &str) -> AgentSession {
        AgentSessionRepo::get_agent_session(&*self.service.db, id)
            .await
            .expect("session lookup")
            .expect("session exists")
    }

    /// Rewrites a row the way a pre-3.6 restart left it: suspended, with
    /// no replacement link, and an explicit age.
    async fn suspend_at(&self, id: &str, created_at: &str) -> AgentSession {
        sqlx::query("UPDATE agent_session SET status = 'suspended', created_at = ? WHERE id = ?")
            .bind(created_at)
            .bind(id)
            .execute(self.service.db.pool())
            .await
            .expect("session suspends");
        self.session(id).await
    }

    /// Inserts a sibling of `template` in the same scope.
    async fn insert_sibling(
        &self,
        template: &AgentSession,
        status: &str,
        created_at: &str,
    ) -> AgentSession {
        AgentSessionRepo::create_agent_session(
            &*self.service.db,
            CreateAgentSession {
                id: new_uuid_v4(),
                identity_id: template.identity_id.clone(),
                profile_id: template.profile_id.clone(),
                context_scope_id: template.context_scope_id.clone(),
                backend_kind: template.backend_kind.clone(),
                runtime_session_id: Some(new_uuid_v4()),
                status: status.to_owned(),
                capabilities_json: template.capabilities_json.clone(),
                connection_status: template.connection_status.clone(),
                predecessor_session_id: None,
                last_activity_at: None,
                created_at: created_at.to_owned(),
                updated_at: created_at.to_owned(),
            },
        )
        .await
        .expect("sibling session inserts")
    }
}

const OLDER: &str = "2026-01-01T00:00:00+00:00";
const NEWER: &str = "2026-02-01T00:00:00+00:00";

async fn native_chat_fixture() -> NativeChatFixture {
    let (service, owner) = test_service_with_owner().await;
    let identity_id = new_uuid_v4();
    let now = now_rfc3339();
    AgentRepo::create_identity_with_profile(
        &*service.db,
        CreateAgentIdentity {
            id: identity_id.clone(),
            name: "dormant-resume-main".to_owned(),
            description: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: None,
            is_default: false,
            paused: false,
            owner_id: Some(owner.clone()),
            visibility: "account".to_owned(),
            account_permission_ceiling: json!({"permissions": ["read_account"]}).to_string(),
            created_at: now.clone(),
            updated_at: now.clone(),
        },
        CreateAgentProfile {
            id: new_uuid_v4(),
            identity_id: identity_id.clone(),
            backend_kind: "native".to_owned(),
            executor_type: NATIVE_EXECUTOR_TYPE.to_owned(),
            provider: Some("test".to_owned()),
            model: Some("model-one".to_owned()),
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "{}".to_owned(),
            tool_policy_json: json!({"allowed": ["read_account"]}).to_string(),
            config_json: "{}".to_owned(),
            credential_ref: None,
            daemon_id: None,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("identity creates");
    crate::AgentChatService::new(Arc::clone(&service.db))
        .set_main_binding(crate::SetMainAgentBindingInput {
            actor_user_id: owner.clone(),
            account_id: owner.clone(),
            identity_id: identity_id.clone(),
            autonomy_policy_json: "{}".to_owned(),
            tool_policy_revision: "dormant-resume-policy".to_owned(),
            expected_version: None,
            replacement_reason: None,
        })
        .await
        .expect("Main binding creates");
    let chat_id = AgentChatRepo::get_main_chat(&*service.db, &owner)
        .await
        .expect("Main Chat lookup")
        .expect("Main Chat exists")
        .id;
    NativeChatFixture {
        service,
        owner,
        identity_id,
        chat_id,
    }
}

#[tokio::test]
async fn active_chat_session_wins_over_a_leftover_suspended_one() {
    let fixture = native_chat_fixture().await;
    let first = fixture.resume().await.expect("first session creates");
    let leftover = fixture.suspend_at(&first.id, OLDER).await;
    let ready = fixture.insert_sibling(&leftover, "ready", NEWER).await;

    let resumed = fixture
        .resume()
        .await
        .expect("an active session is returned despite the leftover suspended row");

    assert_eq!(resumed.id, ready.id);
    assert_eq!(resumed.version, ready.version);
    let leftover_after = fixture.session(&leftover.id).await;
    assert_eq!(leftover_after.status, "suspended");
    assert_eq!(leftover_after.version, leftover.version);
}

#[tokio::test]
async fn newest_suspended_chat_session_resumes_once_when_none_is_active() {
    let fixture = native_chat_fixture().await;
    let first = fixture.resume().await.expect("first session creates");
    let older = fixture.suspend_at(&first.id, OLDER).await;
    let newest = fixture.insert_sibling(&older, "suspended", NEWER).await;

    let resumed = fixture.resume().await.expect("newest session resumes");
    assert_eq!(resumed.id, newest.id);
    assert_eq!(resumed.status, "ready");
    assert_eq!(resumed.runtime_session_id, newest.runtime_session_id);

    let again = fixture.resume().await.expect("resumed session is reused");
    assert_eq!(again.id, newest.id);
    assert_eq!(again.version, resumed.version);

    let older_after = fixture.session(&older.id).await;
    assert_eq!(older_after.status, "suspended");
    assert_eq!(older_after.version, older.version);
}

#[tokio::test]
async fn pending_topic_rotation_successor_is_not_resumed() {
    let fixture = native_chat_fixture().await;
    let first = fixture.resume().await.expect("first session creates");
    let successor = fixture.suspend_at(&first.id, OLDER).await;
    sqlx::query(
        "INSERT INTO agent_chat_topic_rotation
                (chat_id, id, successor_session_id, label, cause, created_at)
             VALUES (?, ?, ?, 'Next topic', 'test', ?)",
    )
    .bind(&fixture.chat_id)
    .bind(new_uuid_v4())
    .bind(&successor.id)
    .bind(OLDER)
    .execute(fixture.service.db.pool())
    .await
    .expect("rotation intent inserts");

    let session = fixture.resume().await.expect("a fresh session creates");

    assert_ne!(session.id, successor.id);
    let successor_after = fixture.session(&successor.id).await;
    assert_eq!(successor_after.status, "suspended");
    assert_eq!(successor_after.version, successor.version);
}

#[tokio::test]
async fn stale_dormant_resume_adopts_the_session_a_concurrent_call_activated() {
    let fixture = native_chat_fixture().await;
    let first = fixture.resume().await.expect("first session creates");
    let older = fixture.suspend_at(&first.id, OLDER).await;
    let newest = fixture.insert_sibling(&older, "suspended", NEWER).await;
    let now = now_rfc3339();

    // Both callers read the scope before either wrote; the winner then
    // resumes the newest row.
    let winner = fixture.resume().await.expect("winner resumes");
    assert_eq!(winner.id, newest.id);

    // A loser holding the same row loses on its version.
    let same_row = fixture
        .service
        .resume_dormant_session(&newest, &now)
        .await
        .expect("version conflict resolves to the winner");
    assert_eq!(same_row.id, winner.id);
    assert_eq!(same_row.version, winner.version);

    // A loser that read after the winner's write picked the older row and
    // loses on the active-scope unique index.
    let other_row = fixture
        .service
        .resume_dormant_session(&older, &now)
        .await
        .expect("unique violation resolves to the winner");
    assert_eq!(other_row.id, winner.id);
    assert_eq!(other_row.version, winner.version);
    let older_after = fixture.session(&older.id).await;
    assert_eq!(older_after.status, "suspended");
    assert_eq!(older_after.version, older.version);
}

#[tokio::test]
async fn stale_dormant_resume_keeps_its_conflict_when_nothing_is_active() {
    let fixture = native_chat_fixture().await;
    let first = fixture.resume().await.expect("first session creates");
    let mut stale = fixture.suspend_at(&first.id, OLDER).await;
    stale.version += 1;

    let error = fixture
        .service
        .resume_dormant_session(&stale, &now_rfc3339())
        .await
        .expect_err("no active session exists to adopt");

    assert!(matches!(
        error,
        ServiceError::Db(db::DbError::VersionConflict)
    ));
}

#[tokio::test]
async fn concurrent_dormant_resumes_return_one_session() {
    let fixture = native_chat_fixture().await;
    let first = fixture.resume().await.expect("first session creates");
    let older = fixture.suspend_at(&first.id, OLDER).await;
    let newest = fixture.insert_sibling(&older, "suspended", NEWER).await;

    let (left, right) = tokio::join!(fixture.resume(), fixture.resume());

    let left = left.expect("left call resolves");
    let right = right.expect("right call resolves");
    assert_eq!(left.id, newest.id);
    assert_eq!(right.id, newest.id);
    assert_eq!(fixture.session(&older.id).await.status, "suspended");
}

#[test]
fn frozen_binding_accepts_the_retained_replacement_version_only() {
    assert!(validate_frozen_binding_state("active", 3, 3).is_ok());
    assert!(validate_frozen_binding_state("replaced", 4, 3).is_ok());
    assert!(validate_frozen_binding_state("replaced", 3, 3).is_ok());
    assert!(validate_frozen_binding_state("replaced", 5, 3).is_err());
    assert!(validate_frozen_binding_state("active", 4, 3).is_err());
}

#[test]
fn frozen_binding_rejects_withdrawn_authority_even_with_matching_version() {
    for state in ["paused", "revoked", "agent_setup_required"] {
        assert!(validate_frozen_binding_state(state, 3, 3).is_err());
    }
}

#[test]
fn permission_intersection_never_expands_authority() {
    let account = permission_set(r#"["read_project","read_memory","propose_task"]"#);
    let profile = permission_set(r#"{"allowed":["read_project","propose_task"]}"#);
    let scope: BTreeSet<String> = ["read_project", "read_memory"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    let effective = intersect_non_empty_layers(&[account, profile, scope]);
    assert_eq!(effective, BTreeSet::from(["read_project".to_owned()]));
}

#[test]
fn task_is_the_only_scope_that_can_admit_workspace_access() {
    for scope_type in [
        CanonicalScopeType::Account,
        CanonicalScopeType::Project,
        CanonicalScopeType::AgentChat,
    ] {
        let scope = CanonicalScope {
            scope_type,
            scope_id: "scope".to_owned(),
            workspace_access: WorkspaceAccess::TaskRead,
        };
        assert!(scope.validate().is_err());
    }
}

#[test]
fn non_task_scope_tool_permissions_never_include_task_workspace_authority() {
    for scope_type in [
        CanonicalScopeType::Account,
        CanonicalScopeType::Project,
        CanonicalScopeType::AgentChat,
    ] {
        let permissions = scope_permission_set(
            &CanonicalScope {
                scope_type,
                scope_id: "scope".to_owned(),
                workspace_access: WorkspaceAccess::Deny,
            },
            false,
            false,
        );
        assert!(!permissions.contains("task_read"));
        assert!(!permissions.contains("task_write"));
        assert_eq!(
            permissions.contains("propose_review"),
            scope_type == CanonicalScopeType::Project,
            "a review request is a Project-scoped proposal, not Task workspace authority"
        );
    }
}

#[test]
fn malformed_or_empty_required_layer_denies_all_permissions() {
    let account = permission_set(r#"["read_project","read_memory"]"#);
    let malformed = permission_set("not-json");
    let scope = BTreeSet::from(["read_project".to_owned()]);
    assert!(intersect_non_empty_layers(&[account.clone(), malformed, scope]).is_empty());
    assert!(intersect_non_empty_layers(&[account, BTreeSet::new()]).is_empty());
}

#[test]
fn the_reviewer_role_is_served_by_its_auditor_execution() {
    // The review runner's conformance pass runs as an `auditor` execution
    // under the reviewer role assignment; refusing it stranded every
    // native reviewer at session authorization.
    assert!(execution_role_serves_task_role(
        default_roles::AUDITOR,
        "reviewer"
    ));
    assert!(execution_role_serves_task_role(
        default_roles::REVIEWER,
        "reviewer"
    ));
    assert!(!execution_role_serves_task_role(
        default_roles::AUDITOR,
        "worker"
    ));
    assert!(!execution_role_serves_task_role(
        default_roles::CODER,
        "reviewer"
    ));
    assert!(execution_role_serves_task_role(
        default_roles::CODER,
        "worker"
    ));
    assert!(execution_role_serves_task_role(
        default_roles::PLANNER,
        "planner"
    ));
}

#[test]
fn task_role_requires_the_current_workflow_state() {
    assert!(task_role_admitted_by_workflow(
        Some(default_roles::CODER),
        "worker"
    ));
    assert!(task_role_admitted_by_workflow(
        Some(default_roles::WORKER),
        "worker"
    ));
    assert!(task_role_admitted_by_workflow(
        Some(default_roles::REVIEWER),
        "reviewer"
    ));
    assert!(task_role_admitted_by_workflow(
        Some(default_roles::PLANNER),
        "planner"
    ));
    assert!(!task_role_admitted_by_workflow(
        Some(default_roles::CODER),
        "planner"
    ));
    assert!(!task_role_admitted_by_workflow(None, "worker"));
    assert!(!task_role_admitted_by_workflow(
        Some(default_roles::PLANNER),
        "worker"
    ));
    assert!(!task_role_admitted_by_workflow(
        Some(default_roles::CODER),
        "reviewer"
    ));
}

#[test]
fn entry_base_url_prefers_metadata_then_method_default() {
    let handle = |method: &str, metadata: &str| CredentialHandle {
        id: "entry-1".to_owned(),
        owner_user_id: "user-1".to_owned(),
        provider: "openai".to_owned(),
        label: "entry".to_owned(),
        status: "configured".to_owned(),
        enabled: true,
        credential_method: method.to_owned(),
        metadata_json: metadata.to_owned(),
        version: 1,
        created_at: "2026-08-15T00:00:00Z".to_owned(),
        updated_at: "2026-08-15T00:00:00Z".to_owned(),
    };
    assert_eq!(
        entry_base_url(&handle(
            "api_key",
            r#"{"base_url":"https://proxy.example/v1"}"#
        ))
        .expect("metadata wins"),
        "https://proxy.example/v1"
    );
    assert_eq!(
        entry_base_url(&handle("api_key", "{}")).expect("api-key default"),
        "https://api.openai.com/v1"
    );
    assert_eq!(
        entry_base_url(&handle("oauth_bundle", "{}")).expect("oauth default"),
        "https://chatgpt.com/backend-api/codex"
    );
    let malformed = handle(
        "api_key",
        r#"{"base_url":"https://proxy.example/v1?api_key=SENTINEL"}"#,
    );
    let error = entry_base_url(&malformed).expect_err("stored query must be rejected");
    assert!(matches!(
        error,
        ServiceError::InvalidOperation { ref message }
            if message == "base_url must not contain userinfo, a query, or a fragment"
    ));
    assert!(!error.to_string().contains("SENTINEL"));
    let mut compatible = handle("api_key", "{}");
    compatible.provider = "openai_compatible".to_owned();
    assert!(entry_base_url(&compatible).is_err());
}

#[test]
fn usage_payload_maps_windows_and_prefers_absolute_reset_over_a_zero_filler_delay() {
    let now = 1_700_000_000;
    let body = serde_json::json!({
        "plan_type": "pro",
        "rate_limit": {
            "primary_window": {
                "used_percent": 42,
                "limit_window_seconds": 300,
                "reset_after_seconds": 0,
                "reset_at": 1_704_069_000,
            },
            "secondary_window": {
                "used_percent": 84.5,
                "limit_window_seconds": 3600,
                "reset_after_seconds": 1800,
            },
        },
    })
    .to_string();
    let (plan_type, windows) = usage_snapshot_from_wham_json(&body, now).expect("payload parses");
    assert_eq!(plan_type.as_deref(), Some("pro"));
    assert_eq!(windows.len(), 2);
    assert_eq!(windows[0].id, "primary");
    assert_eq!(windows[0].used_percent, 42.0);
    assert_eq!(windows[0].window_minutes, Some(5));
    // The zero delay beside an absolute reset is filler, not "resets now".
    assert_eq!(
        windows[0].resets_at,
        rfc3339_from_unix_seconds(1_704_069_000)
    );
    assert_eq!(windows[1].id, "secondary");
    assert_eq!(windows[1].used_percent, 84.5);
    assert_eq!(windows[1].window_minutes, Some(60));
    assert_eq!(windows[1].resets_at, rfc3339_from_unix_seconds(now + 1800));
}

#[test]
fn usage_payload_without_rate_limit_reports_no_windows_not_zeroes() {
    let body = serde_json::json!({ "plan_type": "plus" }).to_string();
    let (plan_type, windows) =
        usage_snapshot_from_wham_json(&body, 1_700_000_000).expect("payload parses");
    assert_eq!(plan_type.as_deref(), Some("plus"));
    assert!(windows.is_empty());
}

#[test]
fn usage_window_with_no_reset_signal_reports_no_resets_at() {
    let body = serde_json::json!({
        "rate_limit": {
            "primary_window": {
                "used_percent": 10,
                "limit_window_seconds": 300,
            },
        },
    })
    .to_string();
    let (_, windows) = usage_snapshot_from_wham_json(&body, 1_700_000_000).expect("parses");
    assert_eq!(windows.len(), 1);
    assert_eq!(windows[0].resets_at, None);
}

#[test]
fn usage_window_missing_used_percent_is_skipped() {
    let body = serde_json::json!({
        "rate_limit": {
            "primary_window": { "limit_window_seconds": 300 },
            "secondary_window": { "used_percent": 12.5, "limit_window_seconds": 60 },
        },
    })
    .to_string();
    let (_, windows) = usage_snapshot_from_wham_json(&body, 1_700_000_000).expect("parses");
    assert_eq!(windows.len(), 1);
    assert_eq!(windows[0].id, "secondary");
}

#[test]
fn usage_payload_malformed_json_is_an_error() {
    assert!(usage_snapshot_from_wham_json("not json", 1_700_000_000).is_err());
}

#[test]
fn direct_chatgpt_reasoning_effort_is_bounded_to_runtime_values() {
    let base_url = "https://chatgpt.com/backend-api/codex";
    for effort in CODEX_REASONING_EFFORTS {
        assert_eq!(
            validate_native_reasoning_effort(
                "openai",
                "oauth_bundle",
                base_url,
                "gpt-5.6-terra",
                Some(effort)
            )
            .expect("supported effort validates")
            .as_deref(),
            Some(*effort)
        );
    }
    assert!(validate_native_reasoning_effort(
        "openai",
        "oauth_bundle",
        base_url,
        "gpt-5.5",
        Some("max")
    )
    .is_err());
    assert!(validate_native_reasoning_effort(
        "openai",
        "api_key",
        "https://api.openai.com/v1",
        "gpt-5.6-terra",
        Some("high")
    )
    .is_err());
}

async fn test_service_with_owner() -> (EmbeddedAgentService, String) {
    let pool = db::create_sqlite_pool("sqlite::memory:")
        .await
        .expect("pool creates");
    db::run_migrations(&pool).await.expect("migrations run");
    let db = Arc::new(SqliteDb::new(pool));
    let owner = new_uuid_v4();
    let now = now_rfc3339();
    db::UserRepo::create_user(
        &*db,
        &db::User {
            id: owner.clone(),
            email: "owner@example.com".to_owned(),
            password_hash: "test".to_owned(),
            display_name: Some("Owner".to_owned()),
            is_admin: true,
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("owner creates");
    (
        EmbeddedAgentService::new(db, b"entry-split-test-key"),
        owner,
    )
}

#[tokio::test]
async fn api_key_entry_publishes_no_agent_and_stores_base_url() {
    let (service, owner) = test_service_with_owner().await;
    let entry = service
        .connect_api_key_credential(ConnectApiKeyCredential {
            owner_user_id: owner.clone(),
            provider: "openai_compatible".to_owned(),
            label: "work key".to_owned(),
            credential: Secret::new("provider-secret-value".to_owned()),
            base_url: Some("https://8.8.8.8".to_owned()),
        })
        .await
        .expect("entry creates");
    assert_eq!(entry.status, "configured");
    assert_eq!(entry.credential_method, "api_key");
    assert_eq!(
        entry_base_url(&entry).expect("stored endpoint"),
        "https://8.8.8.8"
    );
    let agents = AgentRepo::list(
        &*service.db,
        db::AgentListQuery {
            status: None,
            executor_type: None,
            capabilities: Vec::new(),
            page: PageRequest {
                cursor: None,
                limit: 10,
                include_total: false,
                sort_by: SortBy::CreatedAt,
                sort_order: SortOrder::Desc,
            },
        },
    )
    .await
    .expect("agents list");
    assert!(agents.items.is_empty(), "connecting must not create agents");
    assert!(!entry.metadata_json.contains("provider-secret-value"));
}

#[tokio::test]
async fn malformed_legacy_entry_metadata_never_enters_profile_config() {
    let (service, owner) = test_service_with_owner().await;
    let entry = service
        .protected_store()
        .create_credential(
            &new_uuid_v4(),
            &owner,
            "openai",
            "legacy entry",
            Secret::new("provider-secret-value".to_owned()),
            &now_rfc3339(),
        )
        .await
        .expect("legacy entry creates");
    let sentinel = "https://proxy.example/v1?api_key=SENTINEL";
    sqlx::query("UPDATE credential_handle SET metadata_json = ? WHERE id = ?")
        .bind(json!({ "base_url": sentinel }).to_string())
        .bind(&entry.id)
        .execute(service.db.pool())
        .await
        .expect("legacy metadata tampers for test");

    let error = service
        .create_agent_from_entry(CreateEmbeddedAgent {
            owner_user_id: owner,
            name: "legacy agent".to_owned(),
            description: None,
            credential_id: entry.id.clone(),
            model: "gpt-4o".to_owned(),
            reasoning_effort: None,
            system_prompt: None,
            account_permission_ceiling: json!({}),
            tool_policy: json!({}),
            context_tokens: None,
            max_input_tokens: None,
            max_output_tokens: None,
        })
        .await
        .expect_err("malformed legacy endpoint must be rejected before persistence");
    assert!(!error.to_string().contains("SENTINEL"));
    let profile_count =
        sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM agent_profile WHERE credential_ref = ?")
            .bind(&entry.id)
            .fetch_one(service.db.pool())
            .await
            .expect("profile count reads");
    assert_eq!(profile_count, 0);
}

#[tokio::test]
async fn provider_entry_lookup_is_owner_scoped() {
    let (service, owner) = test_service_with_owner().await;
    let other_owner = new_uuid_v4();
    let now = now_rfc3339();
    db::UserRepo::create_user(
        &*service.db,
        &db::User {
            id: other_owner.clone(),
            email: "other-owner@example.com".to_owned(),
            password_hash: "test".to_owned(),
            display_name: Some("Other Owner".to_owned()),
            is_admin: false,
            created_at: now.clone(),
            updated_at: now.clone(),
        },
    )
    .await
    .expect("second owner creates");
    let entry = service
        .protected_store()
        .create_credential(
            &new_uuid_v4(),
            &other_owner,
            "openai",
            "other entry",
            Secret::new("provider-secret-value".to_owned()),
            &now,
        )
        .await
        .expect("other owner's entry creates");

    let denied = service.require_owned_entry(&owner, &entry.id).await;
    assert!(matches!(denied, Err(ServiceError::NotFound { .. })));
    assert_eq!(
        service
            .require_owned_entry(&other_owner, &entry.id)
            .await
            .expect("entry remains visible to its owner")
            .id,
        entry.id
    );
}

#[tokio::test]
async fn provider_env_injection_is_in_memory_only_and_refuses_oauth() {
    let (service, owner) = test_service_with_owner().await;
    let entry = service
        .connect_api_key_credential(ConnectApiKeyCredential {
            owner_user_id: owner.clone(),
            provider: "openai_compatible".to_owned(),
            label: "harness key".to_owned(),
            credential: Secret::new("injected-secret-value".to_owned()),
            base_url: Some("https://8.8.8.8".to_owned()),
        })
        .await
        .expect("entry creates");
    let now = now_rfc3339();
    let agent = AgentRepo::create(
        &*service.db,
        db::CreateAgent {
            id: new_uuid_v4(),
            name: "codex-worker".to_owned(),
            description: None,
            executor_type: "codex".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: Some(entry.id.clone()),
            daemon_id: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: Some(now.clone()),
            is_default: false,
            paused: false,
            owner_id: Some(owner.clone()),
            visibility: "account".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("harness agent creates");

    let mut snapshot = json!({
        "executor_type": "codex",
        "agent_id": agent.id,
        "profile_id": agent.profile_id,
        "config": {}
    });
    service
        .inject_provider_env(&mut snapshot)
        .await
        .expect("injection succeeds");
    // `CommandOverrides` is flattened into executor configs, so the env
    // map must land at `config.env` for deserialization to see it.
    assert_eq!(
        snapshot["config"]["env"]["OPENAI_API_KEY"],
        Value::String("injected-secret-value".to_owned())
    );
    // The stored agent row never gains the secret: injection mutates only
    // the in-memory dispatch snapshot.
    let stored = AgentRepo::get_by_id(&*service.db, &agent.id)
        .await
        .expect("agent reads")
        .expect("agent exists");
    assert!(!stored.config_json.contains("injected-secret-value"));

    // Embedded agents resolve credentials inside the native runtime; the
    // injector must leave their snapshots untouched.
    let mut embedded_snapshot = json!({
        "executor_type": "embedded",
        "agent_id": agent.id,
        "config": {}
    });
    service
        .inject_provider_env(&mut embedded_snapshot)
        .await
        .expect("embedded snapshots pass through");
    assert_eq!(embedded_snapshot["config"], json!({}));

    // An OAuth-backed entry cannot drive a CLI harness.
    let oauth_entry = service
        .connect_oauth_credential(ConnectOAuthCredential {
            owner_user_id: owner.clone(),
            provider: "openai".to_owned(),
            base_url: "https://8.8.8.8".to_owned(),
            credential_label: "chatgpt login".to_owned(),
            credential: OAuthCredentialBundle {
                schema_version: 1,
                access_token: "oauth-access-secret".to_owned(),
                refresh_token: "oauth-refresh-secret".to_owned(),
                expires_at_ms: u64::MAX,
                token_endpoint: "https://auth.openai.com/oauth/token".to_owned(),
                client_id: "client".to_owned(),
                client_secret: None,
                scopes: vec!["openid".to_owned()],
                provider_account_id: Some("acct".to_owned()),
            },
        })
        .await
        .expect("oauth entry creates");
    let now = now_rfc3339();
    let oauth_agent = AgentRepo::create(
        &*service.db,
        db::CreateAgent {
            id: new_uuid_v4(),
            name: "oauth-harness".to_owned(),
            description: None,
            executor_type: "codex".to_owned(),
            model: None,
            reasoning_effort: None,
            permission_policy: None,
            prompt_template: None,
            capabilities_json: "[]".to_owned(),
            config_json: "{}".to_owned(),
            credential_ref: Some(oauth_entry.id.clone()),
            daemon_id: None,
            max_concurrent_tasks: 1,
            heartbeat_interval_seconds: 30,
            max_missed_heartbeats: 3,
            status: AgentStatus::Idle,
            last_heartbeat_at: Some(now.clone()),
            is_default: false,
            paused: false,
            owner_id: Some(owner.clone()),
            visibility: "account".to_owned(),
            created_at: now.clone(),
            updated_at: now,
        },
    )
    .await
    .expect("oauth harness agent creates");
    let mut oauth_snapshot = json!({
        "executor_type": "codex",
        "agent_id": oauth_agent.id,
        "config": {}
    });
    let error = service
        .inject_provider_env(&mut oauth_snapshot)
        .await
        .expect_err("oauth entries cannot drive a harness");
    assert!(!error.to_string().contains("oauth-access-secret"));
}
