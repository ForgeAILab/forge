//! Solo app input, bounded state, command identity, and review interaction tests.

use super::*;

fn ready_state() -> AppState {
    let mut state = AppState::new();
    state.header.runtime = RuntimeState::Ready;
    state.header.readiness = ProjectReadiness::Ready;
    state.primary_view = PrimaryView::MainChat;
    state.focus = FocusTarget::Composer;
    state
}

#[test]
fn solo_opens_on_kanban_and_switches_to_one_chat_view() {
    let mut state = AppState::new();
    assert_eq!(state.primary_view, PrimaryView::Kanban);
    assert_eq!(state.focus, FocusTarget::ProjectRail);

    state.reduce(AppAction::Input(AppInput::SelectPrimaryView(
        PrimaryView::MainChat,
    )));
    assert_eq!(state.primary_view, PrimaryView::MainChat);
    assert_eq!(state.focus, FocusTarget::Composer);

    state.reduce(AppAction::Input(AppInput::PreviousPrimaryView));
    assert_eq!(state.primary_view, PrimaryView::Kanban);
    assert_eq!(state.focus, FocusTarget::ProjectRail);
}

#[test]
fn kanban_arrows_move_within_and_between_nonempty_lanes() {
    let mut state = AppState::new();
    state.tasks = vec![
        TaskSummary::new("q1", "Queued one", TaskState::Queued),
        TaskSummary::new("q2", "Queued two", TaskState::Queued),
        TaskSummary::new("a1", "Active one", TaskState::Running),
        TaskSummary::new("d1", "Done one", TaskState::Succeeded),
    ];
    state.clamp_selections();

    state.reduce(AppAction::Input(AppInput::Down));
    assert_eq!(
        state.selected_task().map(|task| task.id.as_str()),
        Some("q2")
    );

    state.reduce(AppAction::Input(AppInput::MoveRight));
    assert_eq!(state.rail.lane, KanbanLane::Active);
    assert_eq!(
        state.selected_task().map(|task| task.id.as_str()),
        Some("a1")
    );

    state.reduce(AppAction::Input(AppInput::MoveRight));
    assert_eq!(state.rail.lane, KanbanLane::Done);
    assert_eq!(
        state.selected_task().map(|task| task.id.as_str()),
        Some("d1")
    );
}

#[test]
fn composer_moves_and_edits_unicode_without_splitting_bytes() {
    let mut composer = ComposerState::default();
    composer.insert('a', 20);
    composer.insert('é', 20);
    composer.insert('中', 20);
    composer.move_left();
    composer.backspace();
    assert_eq!(composer.text, "a中");
    assert_eq!(composer.cursor, "a".len());
    composer.move_home();
    composer.delete();
    assert_eq!(composer.text, "中");
}

#[test]
fn timeline_and_activity_are_bounded() {
    let limits = AppLimits {
        timeline_messages: 2,
        activity_items: 2,
        ..AppLimits::default()
    };
    let mut state = AppState::new().with_limits(limits);
    for number in 0..4 {
        state.reduce(AppAction::AddMessage(ChatMessage::new(
            number.to_string(),
            MessageRole::User,
            number.to_string(),
            "now",
        )));
    }
    state.reduce(AppAction::TurnStarted {
        turn_id: "turn".to_owned(),
        attempt: 1,
        summary: "working".to_owned(),
    });
    for number in 0..4 {
        state.reduce(AppAction::Activity(ActivityItem::new(
            number,
            ActivityKind::Tool,
            ActivityStatus::Complete,
            "tool",
            number.to_string(),
        )));
    }
    assert_eq!(state.timeline.len(), 2);
    assert_eq!(state.timeline[0].id, "2");
    assert_eq!(
        state
            .live_activity
            .as_ref()
            .expect("live activity")
            .items
            .len(),
        2
    );
    assert_eq!(
        state.live_activity.as_ref().expect("live activity").items[0].sequence,
        2
    );
}

#[test]
fn duplicate_submit_is_deduplicated_until_command_finishes() {
    let mut state = ready_state();
    state.reduce(AppAction::Input(AppInput::Insert('h')));
    state.reduce(AppAction::Input(AppInput::Insert('i')));
    let first = state.reduce(AppAction::Input(AppInput::Submit));
    let second = state.reduce(AppAction::Input(AppInput::Submit));
    assert_eq!(first.commands.len(), 1);
    assert!(second.is_empty());
    assert_eq!(state.pending_commands.len(), 1);
    assert!(state.composer.submitting);
}

#[test]
fn independent_instances_namespace_commands_and_failed_send_replays_identity() {
    let mut first = ready_state();
    first.reduce(AppAction::Input(AppInput::Insert('h')));
    let first_command = first.reduce(AppAction::Input(AppInput::Submit)).commands[0].clone();

    let mut second = ready_state();
    second.reduce(AppAction::Input(AppInput::Insert('h')));
    let second_command = second.reduce(AppAction::Input(AppInput::Submit)).commands[0].clone();

    assert_ne!(
        first_command.idempotency_key, second_command.idempotency_key,
        "separate Solo launches must not reuse durable message keys"
    );

    first.reduce(AppAction::CommandFinished(CommandResult {
        command_id: first_command.id,
        outcome: CommandOutcome::Failed {
            error: FailureNotice::public("temporary transport failure", true),
        },
    }));
    let replay = first.reduce(AppAction::Input(AppInput::Submit));
    assert_eq!(replay.commands.len(), 1);
    assert_eq!(replay.commands[0].id, first_command.id);
    assert_eq!(
        replay.commands[0].idempotency_key, first_command.idempotency_key,
        "retrying the unchanged draft must replay the same command identity"
    );
}

#[test]
fn failed_send_preserves_draft_and_conflict_requests_refresh() {
    let mut state = ready_state();
    state.reduce(AppAction::Input(AppInput::Insert('x')));
    let reduction = state.reduce(AppAction::Input(AppInput::Submit));
    let command_id = reduction.commands[0].id;
    let result = state.reduce(AppAction::CommandFinished(CommandResult {
        command_id,
        outcome: CommandOutcome::Failed {
            error: FailureNotice::conflict("state changed; refresh required"),
        },
    }));
    assert_eq!(state.composer.text, "x");
    assert!(!state.composer.submitting);
    assert_eq!(result.commands.len(), 1);
    assert!(matches!(
        result.commands[0].request,
        CommandRequest::Refresh
    ));
}

#[test]
fn ordinary_cancel_or_quit_never_executes_an_approval() {
    let mut state = ready_state();
    state.reduce(AppAction::OpenModal(ModalState::Approval(ApprovalCard {
        id: "approval".to_owned(),
        kind: ApprovalKind::CharterAdoption,
        title: "Adopt".to_owned(),
        target: "project".to_owned(),
        impact: "enables task admission".to_owned(),
        expected_version: Some(4),
        expected_digest: Some("digest".to_owned()),
        details: vec!["exact target".to_owned()],
        permitted_actions: vec![ApprovalAction::Approve],
        selected_action: 0,
        visibility: ContentVisibility::Public,
    })));
    let cancel = state.reduce(AppAction::Input(AppInput::Cancel));
    assert!(cancel.is_empty());
    assert!(state.modal.is_none());
}

#[test]
fn snapshot_replaces_transient_activity_and_dedupes_messages() {
    let mut state = ready_state();
    state.reduce(AppAction::TurnStarted {
        turn_id: "live".to_owned(),
        attempt: 1,
        summary: "draft".to_owned(),
    });
    state.apply_snapshot(ProjectionSnapshot {
        header: state.header.clone(),
        timeline: vec![
            ChatMessage::new("m", MessageRole::Assistant, "first", "t1"),
            ChatMessage::new("m", MessageRole::Assistant, "authoritative", "t2"),
        ],
        ..ProjectionSnapshot::default()
    });
    assert!(state.live_activity.is_none());
    assert_eq!(state.timeline.len(), 1);
    assert_eq!(state.timeline[0].content, "authoritative");
}

#[test]
fn snapshot_refresh_preserves_local_error_notification() {
    let mut state = ready_state();
    state.reduce(AppAction::Notice(Notification::public(
        "turn version changed; refresh and retry",
    )));
    state.apply_snapshot(ProjectionSnapshot {
        header: state.header.clone(),
        ..ProjectionSnapshot::default()
    });
    assert_eq!(
        state
            .notifications
            .last()
            .map(|notice| notice.message.as_str()),
        Some("turn version changed; refresh and retry")
    );
}

#[test]
fn opening_review_uses_authoritative_card_and_otherwise_shows_task_details() {
    let mut state = ready_state();
    let mut task = TaskSummary::new("task", "Review task", TaskState::AwaitingReview);
    task.selected = true;
    state.tasks.push(task);
    state.primary_view = PrimaryView::Kanban;
    state.rail.tab = ProjectTab::Tasks;
    state.rail.lane = KanbanLane::Review;
    state.focus = FocusTarget::ProjectRail;
    state.reduce(AppAction::Input(AppInput::OpenSelected));
    assert!(matches!(state.modal, Some(ModalState::Task(_))));
    state.reduce(AppAction::Input(AppInput::Cancel));

    let task = state.tasks[0].clone();
    state
        .review_cards
        .push(ReviewCard::from_task_summary(&task));
    state.reduce(AppAction::Input(AppInput::OpenSelected));
    assert!(matches!(state.modal, Some(ModalState::Review(card)) if card.task_id == "task"));
}
#[test]
fn request_changes_collects_guidance_before_dispatch() {
    let mut state = ready_state();
    let mut task = TaskSummary::new("task", "Review", TaskState::AwaitingReview);
    task.review_actions = vec![ApprovalAction::RequestChanges];
    task.version = 8;
    state.modal = Some(ModalState::Review(ReviewCard::from_task_summary(&task)));
    assert!(state
        .reduce(AppAction::Input(AppInput::RequestChanges))
        .is_empty());
    assert!(state.reduce(AppAction::Input(AppInput::Confirm)).is_empty());
    for character in "Fix the missing coverage".chars() {
        state.reduce(AppAction::Input(AppInput::Insert(character)));
    }
    let commands = state.reduce(AppAction::Input(AppInput::Confirm));
    assert!(commands.commands.iter().any(|command| matches!(&command.request, CommandRequest::Review { guidance: Some(value), expected_version: 8, .. } if value == "Fix the missing coverage")));
}
