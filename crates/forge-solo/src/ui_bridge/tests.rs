//! Snapshot projection, controller effects, activity cursors, and review input tests.

use super::*;
use crate::app::{FocusTarget, PrimaryView};
use crate::backend::{
    ActivityCursor, ActivityEntry, ActivityTarget, ChatSnapshot, CommitEvidence, ConflictTarget,
    FailureSnapshot, InteractionField, MessageStatus, ProjectSnapshot, SnapshotRequest,
    TurnSnapshot,
};

fn scope() -> SoloScope {
    SoloScope::new("owner", "project", "repo", "chat")
}

fn snapshot() -> SoloSnapshot {
    let target = ActivityTarget {
        project_id: "project".into(),
        turn_id: Some("turn".into()),
        task_id: None,
        execution_id: "execution".into(),
        attempt: 2,
    };
    SoloSnapshot {
        scope: scope(),
        repository: RepositorySnapshot {
            id: "repo".into(),
            name: "repo".into(),
            root: "/repo".into(),
            default_branch: "main".into(),
        },
        project: ProjectSnapshot {
            id: "project".into(),
            name: "Project".into(),
            readiness: BackendProjectReadiness::AwaitingApproval,
            runtime: BackendRuntimeState::Ready,
            workflow: "autonomous_v1".into(),
            selected_agent: Some(AgentSnapshot {
                id: "agent".into(),
                name: "Codex".into(),
                harness: "codex".into(),
            }),
        },
        chat: ChatSnapshot {
            id: "chat".into(),
            messages: vec![MessageSnapshot {
                id: "message".into(),
                sequence: 1,
                role: MessageRole::User,
                content: "hello".into(),
                created_at: "now".into(),
                turn_id: None,
                status: MessageStatus::Complete,
            }],
            has_older_messages: false,
            active_turns: Vec::new(),
            interactions: vec![InteractionSnapshot {
                id: "question".into(),
                turn_id: "turn".into(),
                prompt: "Choose one".into(),
                fields: vec![InteractionField {
                    id: "choice".into(),
                    label: "Choice".into(),
                    required: true,
                    choices: vec!["yes".into(), "no".into()],
                }],
                expected_version: 7,
            }],
        },
        setup_agents: Vec::new(),
        live_activity: vec![LiveActivitySnapshot {
            target: target.clone(),
            state: BackendTurnState::Leased,
            summary: "working".into(),
            worker: None,
            reviewer: None,
            entries: vec![ActivityEntry {
                sequence: 0,
                attempt: 2,
                kind: ActivityKind::ToolCall,
                summary: "run tests".into(),
                preview: Some("cargo test".into()),
            }],
            cursor: ActivityCursor::beginning(&target),
        }],
        tasks: vec![TaskSnapshot {
            review_actions: vec![api_types::Offer {
                action: api_types::TaskAction::Approve {
                    override_checks: Some(false),
                    reason: None,
                },
                parameters: Vec::new(),
                authority: vec![api_types::ActionAuthority::Owner],
                reason: "human_review".into(),
                label: "Accept".into(),
                target_execution_id: None,
                propagates: false,
            }],
            id: "task".into(),
            title: "Review me".into(),
            state: BackendTaskState::Review,
            version: 4,
            worker: None,
            reviewer: None,
            checks: vec![CheckSnapshot {
                name: "tests".into(),
                status: CheckStatus::Passed,
                summary: Some("passed".into()),
            }],
            commit: Some(CommitEvidence {
                commit: Some("abc".into()),
                changed_files: vec!["src/lib.rs".into()],
                merged: false,
                summary: None,
            }),
            blocker: Some(FailureSnapshot {
                kind: "review".into(),
                headline: "needs review".into(),
                detail: "review target".into(),
                retryable: false,
            }),
            retryable: false,
        }],
        attention: vec![crate::backend::AttentionSnapshot {
            id: "attention".into(),
            kind: crate::backend::AttentionKind::Approval,
            headline: "Approve".into(),
            detail: "exact target".into(),
            affected_id: Some("approval".into()),
            permitted_action: Some(crate::backend::AttentionAction::Approve),
        }],
        approvals: vec![ApprovalSnapshot {
            id: "approval".into(),
            kind: ApprovalKind::CharterAdoption,
            title: "Adopt".into(),
            summary: "adopt charter".into(),
            revision: "rev-1".into(),
            digest: "sha256:abc".into(),
            operating_skill_revision: Some("skill-1".into()),
            expected_version: 3,
            selected_agent: None,
            permitted_actions: vec![ApprovalAction::Approve],
        }],
        refreshed_at: "now".into(),
    }
}

fn turn(id: &str, state: BackendTurnState, version: i64, retryable: bool) -> TurnSnapshot {
    TurnSnapshot {
        id: id.into(),
        triggering_message_id: format!("message-{id}"),
        state,
        version,
        attempt: 2,
        reply: None,
        assistant_message_id: None,
        failure: (state == BackendTurnState::Failed).then(|| FailureSnapshot {
            kind: "executor".into(),
            headline: format!("{id} failed"),
            detail: "durable failure".into(),
            retryable,
        }),
        retryable,
    }
}

#[test]
fn snapshot_mapping_preserves_typed_setup_chat_activity_tasks_and_approval() {
    let projection = to_projection_snapshot(&snapshot());
    assert_eq!(projection.header.project, "Project");
    assert_eq!(projection.header.agent, "Codex");
    assert!(matches!(projection.setup, SetupState::Adoption { .. }));
    assert_eq!(projection.timeline[0].content, "hello");
    assert_eq!(
        projection.approvals[0].expected_digest.as_deref(),
        Some("sha256:abc")
    );
    assert!(matches!(projection.modal, Some(ModalState::Question(_))));
    assert_eq!(projection.tasks[0].version, 4);
    assert_eq!(
        projection.live_activity.as_ref().map(|a| a.attempt),
        Some(2)
    );
}

#[test]
fn failed_turn_snapshot_remains_visible_and_supplies_canonical_retry() {
    let mut snapshot = snapshot();
    snapshot.project.readiness = BackendProjectReadiness::Operational;
    snapshot.chat.interactions.clear();
    snapshot.chat.active_turns = vec![turn("failed", BackendTurnState::Failed, 9, true)];
    snapshot.live_activity.clear();

    let projection = to_projection_snapshot(&snapshot);
    let activity = projection
        .live_activity
        .expect("durable failure should remain visible in activity");
    assert_eq!(activity.turn_id, "failed");
    assert_eq!(activity.state, AppTurnState::Failed);
    assert_eq!(activity.summary, "failed failed");
    assert_eq!(
        projection.retryable_turn,
        Some(TurnReference {
            turn_id: "failed".into(),
            attempt: 2,
            expected_version: 9,
        })
    );
}

#[test]
fn newer_running_or_succeeded_turn_fences_an_older_failure() {
    let mut snapshot = snapshot();
    snapshot.project.readiness = BackendProjectReadiness::Operational;
    snapshot.chat.interactions.clear();
    snapshot.chat.active_turns = vec![
        turn("old-failure", BackendTurnState::Failed, 3, true),
        turn("new-running", BackendTurnState::Leased, 4, false),
    ];
    snapshot.live_activity.clear();

    let running = to_projection_snapshot(&snapshot);
    assert_eq!(
        running
            .live_activity
            .as_ref()
            .map(|activity| activity.turn_id.as_str()),
        Some("new-running")
    );
    assert_eq!(
        running
            .live_activity
            .as_ref()
            .map(|activity| activity.state),
        Some(AppTurnState::Running)
    );
    assert!(running.retryable_turn.is_none());

    snapshot.chat.active_turns = vec![
        turn("old-failure", BackendTurnState::Failed, 3, true),
        turn("new-success", BackendTurnState::Succeeded, 5, false),
    ];
    snapshot.live_activity = vec![LiveActivitySnapshot {
        target: ActivityTarget {
            project_id: "project".into(),
            turn_id: Some("old-failure".into()),
            task_id: None,
            execution_id: "old-failure".into(),
            attempt: 2,
        },
        state: BackendTurnState::Failed,
        summary: "old failure".into(),
        worker: None,
        reviewer: None,
        entries: Vec::new(),
        cursor: ActivityCursor::beginning(&ActivityTarget {
            project_id: "project".into(),
            turn_id: Some("old-failure".into()),
            task_id: None,
            execution_id: "old-failure".into(),
            attempt: 2,
        }),
    }];
    let succeeded = to_projection_snapshot(&snapshot);
    assert!(succeeded.live_activity.is_none());
    assert!(succeeded.retryable_turn.is_none());
}

#[test]
fn failed_snapshot_reaches_retry_command_without_becoming_cancellable() {
    let mut snapshot = snapshot();
    snapshot.project.readiness = BackendProjectReadiness::Operational;
    snapshot.chat.interactions.clear();
    snapshot.chat.active_turns = vec![turn("failed", BackendTurnState::Failed, 9, true)];
    snapshot.live_activity.clear();
    let mut reducer = AppReducer::new(AppState::new());
    let initial = reducer.reduce(ControllerEvent::SnapshotUpdated {
        request: SnapshotRequest::default(),
        result: Ok(snapshot),
    });
    assert!(initial
        .iter()
        .all(|effect| !matches!(effect, ControllerEffect::ReadActivity(_))));
    assert_eq!(reducer.state().live_turn_id(), None);
    assert_eq!(
        reducer.state().live_activity.as_ref().map(|a| a.state),
        Some(AppTurnState::Failed)
    );

    reducer.reduce(ControllerEvent::Input(InputEvent::Key(
        ControllerKeyEvent::new(ControllerKeyCode::Tab),
    )));
    let effects = reducer.reduce(ControllerEvent::Input(InputEvent::Key(
        ControllerKeyEvent::new(ControllerKeyCode::Char('r')),
    )));
    assert!(effects.iter().any(|effect| matches!(
        effect,
        ControllerEffect::Command(BackendCommand::RetryTurn(request))
            if request.turn_id == "failed" && request.expected_version == 9
    )));
}

#[test]
fn cancelled_snapshot_reaches_retry_command() {
    let mut snapshot = snapshot();
    snapshot.project.readiness = BackendProjectReadiness::Operational;
    snapshot.chat.interactions.clear();
    snapshot.chat.active_turns = vec![turn("cancelled", BackendTurnState::Cancelled, 9, true)];
    snapshot.live_activity.clear();
    let mut reducer = AppReducer::new(AppState::new());
    reducer.reduce(ControllerEvent::SnapshotUpdated {
        request: SnapshotRequest::default(),
        result: Ok(snapshot),
    });
    assert_eq!(reducer.state().live_turn_id(), None);
    assert_eq!(
        reducer.state().retryable_turn.as_ref().unwrap().turn_id,
        "cancelled"
    );
    reducer.reduce(ControllerEvent::Input(InputEvent::Key(
        ControllerKeyEvent::new(ControllerKeyCode::Tab),
    )));
    let effects = reducer.reduce(ControllerEvent::Input(InputEvent::Key(
        ControllerKeyEvent::new(ControllerKeyCode::Char('r')),
    )));
    assert!(effects.iter().any(|effect| matches!(
        effect,
        ControllerEffect::Command(BackendCommand::RetryTurn(request))
            if request.turn_id == "cancelled" && request.expected_version == 9
    )));
}

#[test]
fn live_snapshot_refreshes_exact_version_for_cancel_command() {
    let mut snapshot = snapshot();
    snapshot.project.readiness = BackendProjectReadiness::Operational;
    snapshot.chat.interactions.clear();
    snapshot.chat.active_turns = vec![turn("live", BackendTurnState::Leased, 7, false)];
    snapshot.live_activity.clear();

    let mut reducer = AppReducer::new(AppState::new());
    reducer.reduce(ControllerEvent::SnapshotUpdated {
        request: SnapshotRequest::default(),
        result: Ok(snapshot),
    });
    assert_eq!(reducer.state().active_turn_version, 7);

    reducer.reduce(ControllerEvent::Input(InputEvent::Key(
        ControllerKeyEvent::ctrl(ControllerKeyCode::Char('c')),
    )));
    let effects = reducer.reduce(ControllerEvent::Input(InputEvent::Key(
        ControllerKeyEvent::new(ControllerKeyCode::Enter),
    )));
    assert!(effects.iter().any(|effect| matches!(
        effect,
        ControllerEffect::Command(BackendCommand::CancelTurn(request))
            if request.turn_id == "live" && request.expected_version == 7
    )));
}

#[test]
fn wide_project_rail_enters_exact_approval_then_requires_modal_accept() {
    let mut snapshot = snapshot();
    snapshot.project.readiness = BackendProjectReadiness::Operational;
    snapshot.chat.interactions.clear();
    let mut reducer = AppReducer::new(AppState::new());
    reducer.reduce(ControllerEvent::SnapshotUpdated {
        request: SnapshotRequest::default(),
        result: Ok(snapshot),
    });
    reducer.state_mut().focus = crate::app::FocusTarget::ProjectRail;
    reducer.state_mut().rail.tab = crate::app::ProjectTab::Approvals;

    let opened = reducer.reduce(ControllerEvent::Input(InputEvent::Key(
        ControllerKeyEvent::new(ControllerKeyCode::Enter),
    )));
    assert!(opened.is_empty(), "opening a card must not execute it");
    assert!(matches!(
        &reducer.state().modal,
        Some(ModalState::Approval(card))
            if card.id == "approval"
                && card.expected_version == Some(3)
                && card.expected_digest.as_deref() == Some("sha256:abc")
    ));

    let effects = reducer.reduce(ControllerEvent::Input(InputEvent::Key(
        ControllerKeyEvent::new(ControllerKeyCode::Enter),
    )));
    assert!(effects.iter().any(|effect| matches!(
        effect,
        ControllerEffect::Command(BackendCommand::DecideApproval(request))
            if request.approval_id == "approval"
                && request.expected_version == 3
                && request.target_digest == "sha256:abc"
                && request.action == crate::backend::ApprovalAction::Approve
    )));
}

#[test]
fn narrow_task_entry_opens_authoritative_review_then_emits_exact_decision() {
    let mut snapshot = snapshot();
    snapshot.project.readiness = BackendProjectReadiness::Operational;
    snapshot.chat.interactions.clear();
    let mut reducer = AppReducer::new(AppState::new());
    reducer.reduce(ControllerEvent::SnapshotUpdated {
        request: SnapshotRequest::default(),
        result: Ok(snapshot),
    });
    reducer.state_mut().focus = crate::app::FocusTarget::ProjectRail;
    reducer.state_mut().layout = crate::app::LayoutMode::Narrow;
    reducer.state_mut().rail.tab = crate::app::ProjectTab::Tasks;

    let opened = reducer.reduce(ControllerEvent::Input(InputEvent::Key(
        ControllerKeyEvent::new(ControllerKeyCode::Enter),
    )));
    assert!(opened.is_empty(), "opening a card must not execute it");
    assert!(matches!(
        &reducer.state().modal,
        Some(ModalState::Review(card))
            if card.id == "task"
                && card.task_id == "task"
                && card.expected_version == 4
                && card.checks.len() == 1
                && card.changed_files == vec!["src/lib.rs"]
    ));

    let effects = reducer.reduce(ControllerEvent::Input(InputEvent::Key(
        ControllerKeyEvent::new(ControllerKeyCode::Enter),
    )));
    assert!(effects.iter().any(|effect| matches!(
        effect,
        ControllerEffect::Command(BackendCommand::DecideReview(request))
            if request.task_id == "task"
                && request.expected_version == 4
                && request.decision == crate::backend::ReviewDecision::Accept
    )));
}

#[test]
fn charter_drafting_admits_chat() {
    let readiness = to_app_readiness(BackendProjectReadiness::AwaitingCharter);
    assert!(readiness.allows_chat());
}

#[test]
fn setup_snapshot_candidates_replace_the_picker_projection() {
    let mut snapshot = snapshot();
    snapshot.project.readiness = BackendProjectReadiness::AwaitingAgent;
    snapshot.setup_agents = vec![SetupAgentSnapshot {
        id: "new-agent".into(),
        name: "New Codex".into(),
        harness: "codex".into(),
        available: true,
        authenticated: true,
        detail: "authenticated after launch".into(),
    }];

    let projection = to_projection_snapshot(&snapshot);
    let SetupState::AgentPicker { candidates, .. } = projection.setup else {
        panic!("awaiting-agent snapshot should render the setup picker");
    };
    assert_eq!(candidates.len(), 1);
    assert_eq!(candidates[0].id, "new-agent");
    assert_eq!(candidates[0].detail, "authenticated after launch");
}

#[test]
fn refresh_keeps_activity_file_size_cursor_across_snapshots() {
    let mut snapshot = snapshot();
    snapshot.live_activity[0].cursor.file_size = 42;
    let target = snapshot.live_activity[0].target.clone();
    let mut reducer = AppReducer::default();
    reducer.reduce(ControllerEvent::SnapshotUpdated {
        request: SnapshotRequest::default(),
        result: Ok(snapshot.clone()),
    });

    let key = ActivityKey::from(&target);
    assert_eq!(reducer.activities[&key].cursor.file_size, 42);

    snapshot.live_activity[0].cursor.file_size = 7;
    reducer.reduce(ControllerEvent::SnapshotUpdated {
        request: SnapshotRequest::default(),
        result: Ok(snapshot),
    });
    assert_eq!(
        reducer.activities[&key].cursor.file_size, 42,
        "an authoritative refresh must not erase an incremental log cursor"
    );
}

#[test]
fn key_mapping_uses_app_context_and_resize_is_pure() {
    let mut state = AppState::new();
    state.header.runtime = AppRuntimeState::Ready;
    state.header.readiness = ProjectReadiness::Ready;
    state.primary_view = PrimaryView::MainChat;
    state.focus = FocusTarget::Composer;
    assert_eq!(
        app_action_from_input(
            &state,
            Keymap::default(),
            InputEvent::Key(ControllerKeyEvent::new(ControllerKeyCode::Char('x'))),
        ),
        Some(AppAction::Input(AppInput::Insert('x')))
    );
    assert_eq!(
        app_action_from_input(
            &state,
            Keymap::default(),
            InputEvent::Resize {
                width: 80,
                height: 20,
            },
        ),
        Some(AppAction::Resize {
            width: 80,
            height: 20,
        })
    );
}

#[test]
fn send_and_select_agent_commands_cross_typed_backend_boundary() {
    let mut reducer = AppReducer::new({
        let mut state = AppState::new();
        state.header.runtime = AppRuntimeState::Ready;
        state.header.readiness = ProjectReadiness::Ready;
        state.primary_view = PrimaryView::MainChat;
        state.focus = FocusTarget::Composer;
        state
    });
    reducer
        .state_mut()
        .reduce(AppAction::Input(AppInput::Insert('h')));
    let send_effects = reducer.reduce(ControllerEvent::Input(InputEvent::Key(
        ControllerKeyEvent::new(ControllerKeyCode::Enter),
    )));
    assert!(matches!(
        send_effects.as_slice(),
        [ControllerEffect::Command(BackendCommand::SendMessage(_))]
    ));
    let mut setup = AppState::new();
    setup.setup = SetupState::AgentPicker {
        candidates: vec![AgentCandidate {
            id: "agent".into(),
            label: "Codex".into(),
            kind: "codex".into(),
            available: true,
            authenticated: true,
            detail: String::new(),
        }],
        selected: 0,
        detail: String::new(),
    };
    let mut setup_reducer = AppReducer::new(setup);
    let effects = setup_reducer.reduce(ControllerEvent::Input(InputEvent::Key(
        ControllerKeyEvent::new(ControllerKeyCode::Enter),
    )));
    assert!(matches!(
        effects.as_slice(),
        [ControllerEffect::Command(BackendCommand::SelectAgent(_))]
    ));
}

#[test]
fn interaction_answer_uses_authoritative_turn_version_and_idempotency() {
    let mut reducer = AppReducer::new(AppState::new());
    let mut snap = snapshot();
    snap.project.readiness = BackendProjectReadiness::Operational;
    reducer.reduce(ControllerEvent::SnapshotUpdated {
        request: SnapshotRequest::default(),
        result: Ok(snap),
    });
    let effects = reducer.reduce(ControllerEvent::Input(InputEvent::Key(
        ControllerKeyEvent::new(ControllerKeyCode::Enter),
    )));
    assert!(matches!(
        effects.as_slice(),
        [ControllerEffect::Command(BackendCommand::AnswerInteraction(request))]
            if request.interaction_id == "question"
                && request.turn_id == "turn"
                && request.expected_version == 7
                && request.answers[0].value == "yes"
    ));
}

#[test]
fn conflict_preserves_draft_and_requests_authoritative_refresh() {
    let mut state = AppState::new();
    state.header.runtime = AppRuntimeState::Ready;
    state.header.readiness = ProjectReadiness::Ready;
    state.primary_view = PrimaryView::MainChat;
    state.focus = FocusTarget::Composer;
    let mut reducer = AppReducer::new(state);
    reducer.reduce(ControllerEvent::Input(InputEvent::Key(
        ControllerKeyEvent::new(ControllerKeyCode::Char('d')),
    )));
    let effects = reducer.reduce(ControllerEvent::Input(InputEvent::Key(
        ControllerKeyEvent::new(ControllerKeyCode::Enter),
    )));
    let ControllerEffect::Command(command) = effects[0].clone() else {
        panic!("send command expected");
    };
    let effects = reducer.reduce(ControllerEvent::CommandCompleted {
        id: 42,
        command,
        result: Err(BackendError::conflict(
            "stale target",
            Some(ConflictTarget {
                kind: "turn".into(),
                id: "turn".into(),
                expected_version: Some(1),
                expected_digest: None,
            }),
        )),
    });
    assert_eq!(reducer.state().composer.text, "d");
    assert!(effects
        .iter()
        .any(|effect| matches!(effect, ControllerEffect::Refresh)));
}

#[test]
fn activity_poll_uses_latest_cursor_and_bounded_page() {
    let mut reducer = AppReducer::new(AppState::new());
    let snap = snapshot();
    reducer.reduce(ControllerEvent::SnapshotUpdated {
        request: SnapshotRequest::default(),
        result: Ok(snap),
    });
    let effects = reducer.reduce(ControllerEvent::ActivityPollDue);
    assert!(effects.iter().any(|effect| matches!(
        effect,
        ControllerEffect::ReadActivity(request)
            if request.limit == DEFAULT_ACTIVITY_PAGE && request.cursor.next_sequence == 0
    )));
}
#[test]
fn review_card_uses_only_offered_decisions() {
    let mut task = snapshot().tasks[0].clone();
    task.review_actions.clear();
    assert!(to_review_card(&task).permitted_actions.is_empty());
    task.review_actions.push(api_types::Offer {
        action: api_types::TaskAction::SendBack {
            guidance: String::new(),
        },
        parameters: Vec::new(),
        authority: vec![api_types::ActionAuthority::Owner],
        reason: "review_failed".into(),
        label: "Request changes".into(),
        target_execution_id: None,
        propagates: false,
    });
    assert_eq!(
        to_review_card(&task).permitted_actions,
        vec![AppApprovalAction::RequestChanges]
    );
}
#[test]
fn physical_review_keys_collect_guidance_and_forward_it_to_backend() {
    let mut snapshot = snapshot();
    snapshot.project.readiness = BackendProjectReadiness::Operational;
    snapshot.chat.interactions.clear();
    snapshot.tasks[0].review_actions = vec![api_types::Offer {
        action: api_types::TaskAction::SendBack {
            guidance: String::new(),
        },
        parameters: Vec::new(),
        authority: vec![api_types::ActionAuthority::Owner],
        reason: "human_review".into(),
        label: "Request changes".into(),
        target_execution_id: None,
        propagates: false,
    }];
    let mut reducer = AppReducer::new(AppState::new());
    reducer.reduce(ControllerEvent::SnapshotUpdated {
        request: SnapshotRequest::default(),
        result: Ok(snapshot),
    });
    reducer.state_mut().focus = crate::app::FocusTarget::ProjectRail;
    reducer.state_mut().rail.tab = crate::app::ProjectTab::Tasks;
    let input = |code| ControllerEvent::Input(InputEvent::Key(ControllerKeyEvent::new(code)));
    assert!(reducer.reduce(input(ControllerKeyCode::Enter)).is_empty());
    assert!(reducer.reduce(input(ControllerKeyCode::Enter)).is_empty());
    let guidance = "Add coverage for retry? yes.";
    for character in guidance.chars() {
        assert!(reducer
            .reduce(input(ControllerKeyCode::Char(character)))
            .is_empty());
    }
    let effects = reducer.reduce(input(ControllerKeyCode::Enter));
    assert!(effects.iter().any(|effect| matches!(effect, ControllerEffect::Command(BackendCommand::DecideReview(request)) if request.guidance.as_deref() == Some(guidance) && request.expected_version == 4 && request.decision == crate::backend::ReviewDecision::Reject)));
}
