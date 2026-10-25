//! App state construction, input reduction, command handling, and navigation.

use super::*;

impl AppState {
    pub fn new() -> Self {
        Self {
            header: HeaderState::default(),
            setup: SetupState::default(),
            timeline: Vec::new(),
            live_activity: None,
            retryable_turn: None,
            tasks: Vec::new(),
            review_cards: Vec::new(),
            attention: Vec::new(),
            approvals: Vec::new(),
            notifications: Vec::new(),
            composer: ComposerState::default(),
            modal: None,
            primary_view: PrimaryView::Kanban,
            focus: FocusTarget::ProjectRail,
            layout: LayoutMode::Wide,
            terminal_width: 120,
            terminal_height: 40,
            rail: RailState::default(),
            scroll: ScrollState::default(),
            active_turn_version: 0,
            shutdown_requested: false,
            force_shutdown: false,
            limits: AppLimits::default(),
            pending_commands: BTreeMap::new(),
            next_command_id: 1,
            idempotency_prefix: format!("solo-ui-{}", Uuid::new_v4()),
            retryable_send: None,
        }
    }

    pub fn with_limits(mut self, limits: AppLimits) -> Self {
        self.limits = limits;
        self
    }

    pub fn composer_enabled(&self) -> bool {
        self.header.runtime.accepts_input()
            && self.header.readiness.allows_chat()
            && !self.composer.submitting
            && self.modal.is_none()
            && !self.shutdown_requested
    }

    pub fn live_turn_id(&self) -> Option<&str> {
        self.live_activity
            .as_ref()
            .filter(|activity| activity.state.is_live())
            .map(|activity| activity.turn_id.as_str())
    }

    pub fn selected_task(&self) -> Option<&TaskSummary> {
        self.tasks
            .get(self.rail.selected_task)
            .filter(|task| task.state.kanban_lane() == self.rail.lane)
    }

    pub fn reduce(&mut self, action: AppAction) -> Reduction {
        let mut commands = Vec::new();
        match action {
            AppAction::Input(input) => self.reduce_input(input, &mut commands),
            AppAction::Resize { width, height } => {
                self.set_size(width, height);
            }
            AppAction::ReplaceSnapshot(snapshot) => self.replace_snapshot(snapshot),
            AppAction::SetRuntime(runtime) => {
                self.header.runtime = runtime;
            }
            AppAction::SetReadiness(readiness) => {
                self.header.readiness = readiness;
            }
            AppAction::SetSetup(setup) => {
                self.setup = setup;
                normalize_setup(&mut self.setup);
                self.clamp_selections();
                if matches!(
                    self.setup,
                    SetupState::AgentPicker { .. } | SetupState::Unavailable { .. }
                ) && self.modal.is_none()
                {
                    self.focus = FocusTarget::SetupPicker;
                }
            }
            AppAction::AddMessage(message) => {
                self.upsert_message(message);
            }
            AppAction::TurnStarted {
                turn_id,
                attempt,
                summary,
            } => {
                self.header.runtime = RuntimeState::Busy;
                self.live_activity = Some(LiveActivity::new(turn_id, attempt, summary));
                self.active_turn_version = 0;
                self.scroll.activity_offset = 0;
            }
            AppAction::SetTurnVersion {
                turn_id,
                expected_version,
            } => {
                if self
                    .live_activity
                    .as_ref()
                    .is_some_and(|activity| activity.turn_id == turn_id)
                {
                    self.active_turn_version = expected_version;
                }
            }
            AppAction::Activity(item) => {
                if let Some(activity) = self.live_activity.as_mut() {
                    activity.append(item, self.limits.activity_items);
                }
            }
            AppAction::SetLiveActivity(activity) => {
                self.live_activity = activity.map(|mut activity| {
                    retain_latest(&mut activity.items, self.limits.activity_items);
                    activity
                });
            }
            AppAction::TurnStateChanged { turn_id, state } => {
                if let Some(activity) = self.live_activity.as_mut() {
                    if activity.turn_id == turn_id {
                        activity.state = state;
                    }
                }
                self.header.runtime = if state.is_live() {
                    RuntimeState::Busy
                } else {
                    RuntimeState::Ready
                };
            }
            AppAction::TurnFinished {
                turn_id,
                state,
                assistant_message,
                retryable,
                expected_version,
            } => {
                // A delayed terminal event for an older turn must not replace
                // the current turn's live/failure presentation or expose an
                // obsolete retry target. Authoritative snapshots still
                // reconcile the complete projection.
                if self
                    .live_activity
                    .as_ref()
                    .is_none_or(|activity| activity.turn_id == turn_id || !activity.state.is_live())
                {
                    let attempt = self
                        .live_activity
                        .as_ref()
                        .filter(|activity| activity.turn_id == turn_id)
                        .map_or(0, |activity| activity.attempt);
                    let matches_live = self
                        .live_activity
                        .as_ref()
                        .is_some_and(|activity| activity.turn_id == turn_id);
                    if matches_live {
                        self.live_activity = None;
                    }
                    if let Some(message) = assistant_message {
                        self.upsert_message(message);
                    }
                    self.retryable_turn = retryable.then_some(TurnReference {
                        turn_id,
                        attempt,
                        expected_version,
                    });
                    self.active_turn_version = 0;
                    self.header.runtime = RuntimeState::Ready;
                    if !state.is_retryable() {
                        self.retryable_turn = None;
                    }
                }
            }
            AppAction::OpenModal(modal) => {
                let mut modal = modal;
                normalize_modal(&mut modal, &self.limits);
                self.modal = Some(modal);
                self.focus = FocusTarget::Modal;
            }
            AppAction::CloseModal => self.close_modal(),
            AppAction::Notice(notification) => self.push_notification(notification),
            AppAction::CommandFinished(result) => {
                self.finish_command(result, &mut commands);
            }
            AppAction::ShutdownComplete => {
                self.header.runtime = if self.force_shutdown {
                    RuntimeState::ForcedShutdown
                } else {
                    RuntimeState::Stopped
                };
                self.shutdown_requested = true;
            }
            AppAction::Tick => {
                if self.scroll.follow_tail {
                    self.scroll.timeline_offset = 0;
                }
            }
        }
        Reduction { commands }
    }

    pub fn apply_snapshot(&mut self, snapshot: ProjectionSnapshot) {
        self.replace_snapshot(snapshot);
    }

    fn replace_snapshot(&mut self, snapshot: ProjectionSnapshot) {
        let old_live_turn = self.live_turn_id().map(str::to_owned);
        self.header = snapshot.header;
        self.setup = snapshot.setup;
        normalize_setup(&mut self.setup);
        if matches!(
            self.setup,
            SetupState::AgentPicker { .. } | SetupState::Unavailable { .. }
        ) && self.modal.is_none()
        {
            self.focus = FocusTarget::SetupPicker;
        }
        self.timeline = bounded_messages(snapshot.timeline, self.limits.timeline_messages);
        self.live_activity = snapshot.live_activity.map(|mut activity| {
            retain_latest(&mut activity.items, self.limits.activity_items);
            activity
        });
        self.retryable_turn = snapshot.retryable_turn;
        self.tasks = snapshot.tasks;
        for task in &mut self.tasks {
            retain_latest(&mut task.checks, self.limits.checks_per_task);
            retain_latest(&mut task.changed_files, 128);
        }
        retain_latest(&mut self.tasks, self.limits.tasks);
        self.review_cards = snapshot.review_cards;
        retain_latest(&mut self.review_cards, self.limits.tasks);
        self.attention = snapshot.attention;
        retain_latest(&mut self.attention, self.limits.attention_items);
        self.approvals = snapshot.approvals;
        retain_latest(&mut self.approvals, self.limits.attention_items);
        // Notifications are transient local feedback.  Backend projections
        // currently carry no notifications, so an ordinary refresh must not
        // erase a conflict/error before the user can read it.  If a future
        // authoritative projection supplies notifications, replace the local
        // list with that non-empty projection.
        if !snapshot.notifications.is_empty() {
            self.notifications = snapshot.notifications;
        }
        retain_latest(&mut self.notifications, self.limits.notifications);
        // Durable refreshes can arrive while a local confirmation/help card
        // is open.  Preserve those transient cards unless the authoritative
        // projection supplies a replacement interaction/approval/review.
        match snapshot.modal {
            Some(modal) => self.modal = Some(modal),
            None if matches!(
                self.modal,
                Some(
                    ModalState::Approval(_)
                        | ModalState::Review(_)
                        | ModalState::Task(_)
                        | ModalState::Cancel(_)
                        | ModalState::Help
                )
            ) => {}
            None => self.modal = None,
        }
        if let Some(modal) = self.modal.as_mut() {
            normalize_modal(modal, &self.limits);
        }
        if self.modal.is_some() {
            self.focus = FocusTarget::Modal;
        } else if matches!(self.focus, FocusTarget::Modal) {
            self.focus = self.primary_view.default_focus();
        }
        if old_live_turn != self.live_turn_id().map(str::to_owned) {
            self.scroll.activity_offset = 0;
        }
        self.clamp_selections();
    }

    fn reduce_input(&mut self, input: AppInput, commands: &mut Vec<Command>) {
        if self.force_shutdown {
            return;
        }

        if self.modal.is_some() {
            self.reduce_modal_input(input, commands);
            return;
        }

        match input {
            AppInput::Insert(character) if self.focus == FocusTarget::Composer => {
                self.composer.insert(character, self.limits.composer_chars);
            }
            AppInput::NewLine if self.focus == FocusTarget::Composer => {
                self.composer.newline(self.limits.composer_chars);
            }
            AppInput::Backspace if self.focus == FocusTarget::Composer => {
                self.composer.backspace();
            }
            AppInput::Delete if self.focus == FocusTarget::Composer => {
                self.composer.delete();
            }
            AppInput::MoveLeft if self.focus == FocusTarget::Composer => self.composer.move_left(),
            AppInput::MoveLeft
                if self.primary_view == PrimaryView::Kanban
                    && self.focus == FocusTarget::ProjectRail
                    && self.rail.tab == ProjectTab::Tasks =>
            {
                self.select_lane(-1);
            }
            AppInput::MoveRight if self.focus == FocusTarget::Composer => {
                self.composer.move_right();
            }
            AppInput::MoveRight
                if self.primary_view == PrimaryView::Kanban
                    && self.focus == FocusTarget::ProjectRail
                    && self.rail.tab == ProjectTab::Tasks =>
            {
                self.select_lane(1);
            }
            AppInput::Home if self.focus == FocusTarget::Composer => self.composer.move_home(),
            AppInput::End if self.focus == FocusTarget::Composer => self.composer.move_end(),
            AppInput::Submit => self.submit_message(commands),
            AppInput::Cancel => {
                self.close_modal();
            }
            AppInput::RequestQuit => self.request_quit(commands),
            AppInput::ForceQuit => {
                self.force_shutdown = true;
                self.shutdown_requested = true;
                self.header.runtime = RuntimeState::ForcedShutdown;
                self.push_notification(Notification::public(
                    "Forced quit requested; durable recovery will run on next launch.",
                ));
                self.queue_command(CommandRequest::ForceShutdown, commands);
            }
            AppInput::RequestCancel => self.open_cancel_for_live_turn(),
            AppInput::Retry => {
                if matches!(
                    self.setup,
                    SetupState::Unavailable {
                        retryable: true,
                        ..
                    }
                ) {
                    self.queue_command(CommandRequest::Refresh, commands);
                } else {
                    self.retry_turn(commands);
                }
            }
            AppInput::ToggleHelp => {
                self.modal = Some(ModalState::Help);
                self.focus = FocusTarget::Help;
            }
            AppInput::ToggleActivity => {
                if let Some(activity) = self.live_activity.as_mut() {
                    activity.expanded = !activity.expanded;
                    self.focus = FocusTarget::Activity;
                }
            }
            AppInput::ToggleReasoning => {
                if let Some(activity) = self.live_activity.as_mut() {
                    activity.reasoning_expanded = !activity.reasoning_expanded;
                }
            }
            AppInput::FocusNext => {
                self.focus = self.focus.next(self.primary_view);
            }
            AppInput::FocusPrevious => {
                self.focus = self.focus.previous(self.primary_view);
            }
            AppInput::Up => self.move_up(),
            AppInput::Down => self.move_down(),
            AppInput::PageUp => self.scroll_page(-1),
            AppInput::PageDown => self.scroll_page(1),
            AppInput::TimelineTop => {
                // The renderer clamps this sentinel against the number of
                // wrapped rows available at the current terminal width.
                self.scroll.timeline_offset = usize::MAX;
                self.scroll.follow_tail = false;
            }
            AppInput::TimelineBottom => {
                self.scroll.timeline_offset = 0;
                self.scroll.follow_tail = true;
            }
            AppInput::NextPrimaryView => self.select_primary_view(self.primary_view.next()),
            AppInput::PreviousPrimaryView => {
                self.select_primary_view(self.primary_view.previous());
            }
            AppInput::SelectPrimaryView(view) => self.select_primary_view(view),
            AppInput::NextProjectTab => self.select_project_tab(self.rail.tab.next()),
            AppInput::PreviousProjectTab => self.select_project_tab(self.rail.tab.previous()),
            AppInput::SelectProjectTab(tab) => self.select_project_tab(tab),
            AppInput::SelectNext => self.select_task(1),
            AppInput::SelectPrevious => self.select_task(-1),
            AppInput::SelectSetupNext => {
                self.setup.move_selection(1);
            }
            AppInput::SelectSetupPrevious => {
                self.setup.move_selection(-1);
            }
            AppInput::Confirm => self.confirm_setup(commands),
            AppInput::OpenSelected => self.open_selected_card(),
            AppInput::Reject => {}
            // Modal-only actions are ignored when no modal is open.  Keeping
            // them in the enum lets keymap/controller code stay explicit.
            AppInput::Approve | AppInput::Accept | AppInput::RequestChanges => {}
            _ => {}
        }
    }

    fn reduce_modal_input(&mut self, input: AppInput, commands: &mut Vec<Command>) {
        let Some(modal) = self.modal.take() else {
            return;
        };
        match modal {
            ModalState::Help => match input {
                AppInput::Cancel | AppInput::ToggleHelp | AppInput::Confirm => self.close_modal(),
                _ => {
                    self.modal = Some(ModalState::Help);
                    self.focus = FocusTarget::Help;
                }
            },
            ModalState::Cancel(card) => match input {
                AppInput::Cancel | AppInput::Reject => self.close_modal(),
                AppInput::Confirm | AppInput::Accept | AppInput::Approve => {
                    let request = CommandRequest::CancelTurn {
                        turn_id: card.turn_id.clone(),
                        expected_version: card.expected_version,
                    };
                    self.close_modal();
                    self.queue_command(request, commands);
                    if let Some(activity) = self.live_activity.as_mut() {
                        if activity.turn_id == card.turn_id {
                            activity.state = TurnState::Cancelling;
                        }
                    }
                }
                _ => {
                    self.modal = Some(ModalState::Cancel(card));
                    self.focus = FocusTarget::Modal;
                }
            },
            ModalState::Approval(mut card) => match input {
                AppInput::Cancel => self.close_modal(),
                AppInput::Up | AppInput::SelectPrevious => {
                    card.move_action(-1);
                    self.modal = Some(ModalState::Approval(card));
                    self.focus = FocusTarget::Modal;
                }
                AppInput::Down | AppInput::SelectNext => {
                    card.move_action(1);
                    self.modal = Some(ModalState::Approval(card));
                    self.focus = FocusTarget::Modal;
                }
                AppInput::Confirm | AppInput::Approve | AppInput::Accept => {
                    if let Some(action) = card.selected_action() {
                        let request = CommandRequest::Approval {
                            approval_id: card.id.clone(),
                            action,
                            expected_version: card.expected_version,
                            expected_digest: card.expected_digest.clone(),
                        };
                        self.close_modal();
                        self.queue_command(request, commands);
                    } else {
                        self.modal = Some(ModalState::Approval(card));
                        self.focus = FocusTarget::Modal;
                    }
                }
                AppInput::Reject | AppInput::RequestChanges
                    if card.permitted_actions.contains(&ApprovalAction::Reject)
                        || card
                            .permitted_actions
                            .contains(&ApprovalAction::RequestChanges) =>
                {
                    let action = if card.permitted_actions.contains(&ApprovalAction::Reject) {
                        ApprovalAction::Reject
                    } else {
                        ApprovalAction::RequestChanges
                    };
                    let request = CommandRequest::Approval {
                        approval_id: card.id.clone(),
                        action,
                        expected_version: card.expected_version,
                        expected_digest: card.expected_digest.clone(),
                    };
                    self.close_modal();
                    self.queue_command(request, commands);
                }
                _ => {
                    self.modal = Some(ModalState::Approval(card));
                    self.focus = FocusTarget::Modal;
                }
            },
            ModalState::Review(mut card) => match input {
                AppInput::Cancel => self.close_modal(),
                AppInput::Insert(character) if card.editing_guidance => {
                    card.guidance.push(character);
                    self.modal = Some(ModalState::Review(card));
                }
                AppInput::Backspace if card.editing_guidance => {
                    card.guidance.pop();
                    self.modal = Some(ModalState::Review(card));
                }
                AppInput::NewLine if card.editing_guidance => {
                    card.guidance.push('\n');
                    self.modal = Some(ModalState::Review(card));
                }
                AppInput::Up | AppInput::SelectPrevious => {
                    card.move_action(-1);
                    card.editing_guidance = false;
                    self.modal = Some(ModalState::Review(card));
                }
                AppInput::Down | AppInput::SelectNext => {
                    card.move_action(1);
                    card.editing_guidance = false;
                    self.modal = Some(ModalState::Review(card));
                }
                AppInput::Confirm | AppInput::Approve | AppInput::Accept => {
                    if let Some(action) = card.selected_action() {
                        let send_back = matches!(
                            action,
                            ApprovalAction::Reject | ApprovalAction::RequestChanges
                        );
                        if (send_back || card.requires_reason)
                            && (!card.editing_guidance || card.guidance.trim().is_empty())
                        {
                            card.editing_guidance = true;
                            self.modal = Some(ModalState::Review(card));
                        } else {
                            let request = CommandRequest::Review {
                                review_id: card.id.clone(),
                                task_id: card.task_id.clone(),
                                action,
                                expected_version: card.expected_version,
                                guidance: (send_back || card.requires_reason)
                                    .then(|| card.guidance.clone()),
                            };
                            self.close_modal();
                            self.queue_command(request, commands);
                        }
                    } else {
                        self.modal = Some(ModalState::Review(card));
                    }
                }
                AppInput::Reject | AppInput::RequestChanges => {
                    if let Some(index) = card.permitted_actions.iter().position(|action| {
                        matches!(
                            action,
                            ApprovalAction::Reject | ApprovalAction::RequestChanges
                        )
                    }) {
                        card.selected_action = index;
                        card.editing_guidance = true;
                    }
                    self.modal = Some(ModalState::Review(card));
                }
                _ => {
                    self.modal = Some(ModalState::Review(card));
                }
            },
            ModalState::Question(mut card) => match input {
                AppInput::Cancel => self.close_modal(),
                AppInput::Up | AppInput::SelectPrevious => {
                    card.move_option(-1);
                    self.modal = Some(ModalState::Question(card));
                    self.focus = FocusTarget::Modal;
                }
                AppInput::Down | AppInput::SelectNext => {
                    card.move_option(1);
                    self.modal = Some(ModalState::Question(card));
                    self.focus = FocusTarget::Modal;
                }
                AppInput::Confirm => {
                    if let Some(option) = card.selected_option() {
                        let request = CommandRequest::AnswerQuestion {
                            question_id: card.id.clone(),
                            option_id: option.id.clone(),
                        };
                        self.close_modal();
                        self.queue_command(request, commands);
                    } else {
                        self.modal = Some(ModalState::Question(card));
                        self.focus = FocusTarget::Modal;
                    }
                }
                _ => {
                    self.modal = Some(ModalState::Question(card));
                    self.focus = FocusTarget::Modal;
                }
            },
            ModalState::Task(card) => match input {
                AppInput::Cancel | AppInput::Confirm => self.close_modal(),
                _ => {
                    self.modal = Some(ModalState::Task(card));
                    self.focus = FocusTarget::Modal;
                }
            },
            ModalState::Error(notice) => match input {
                AppInput::Cancel | AppInput::Confirm => self.close_modal(),
                AppInput::Retry if notice.retryable => {
                    self.close_modal();
                    self.retry_turn(commands);
                }
                _ => {
                    self.modal = Some(ModalState::Error(notice));
                    self.focus = FocusTarget::Modal;
                }
            },
        }
    }

    fn submit_message(&mut self, commands: &mut Vec<Command>) {
        if !self.composer_enabled() || self.composer.is_blank() || self.has_pending_send() {
            return;
        }
        let text = self.composer.text.clone();
        self.composer.submitting = true;
        self.composer.error = None;

        // A retry of a send is the same logical admission when the draft is
        // unchanged. Reuse the original command id and idempotency key so a
        // response that was committed before a transport failure is replayed
        // by the service instead of admitted a second time.
        if let Some(command) = self.retryable_send.clone() {
            let same_draft = matches!(
                &command.request,
                CommandRequest::SendMessage { text: command_text }
                    if command_text == &text
            );
            if same_draft && !self.pending_commands.contains_key(&command.id) {
                self.pending_commands.insert(
                    command.id,
                    PendingCommand::new(command.id, command.request.clone()),
                );
                commands.push(command);
                return;
            }
            if !same_draft {
                self.retryable_send = None;
            }
        }
        self.queue_command(CommandRequest::SendMessage { text }, commands);
    }

    fn request_quit(&mut self, commands: &mut Vec<Command>) {
        if self.shutdown_requested {
            return;
        }
        if let Some(activity) = self
            .live_activity
            .as_ref()
            .filter(|activity| activity.state.is_live())
        {
            self.modal = Some(ModalState::Cancel(CancelCard {
                turn_id: activity.turn_id.clone(),
                attempt: activity.attempt,
                expected_version: self.active_turn_version,
                summary: activity.summary.clone(),
            }));
            self.focus = FocusTarget::Modal;
            return;
        }
        self.shutdown_requested = true;
        self.header.runtime = RuntimeState::ShuttingDown;
        self.queue_command(CommandRequest::Shutdown, commands);
    }

    fn open_cancel_for_live_turn(&mut self) {
        let Some(activity) = self
            .live_activity
            .as_ref()
            .filter(|activity| activity.state.is_live())
        else {
            return;
        };
        self.modal = Some(ModalState::Cancel(CancelCard {
            turn_id: activity.turn_id.clone(),
            attempt: activity.attempt,
            expected_version: self.active_turn_version,
            summary: activity.summary.clone(),
        }));
        self.focus = FocusTarget::Modal;
    }

    fn retry_turn(&mut self, commands: &mut Vec<Command>) {
        let Some(turn) = self.retryable_turn.clone() else {
            return;
        };
        if !turn.turn_id.is_empty() && !self.has_pending_retry() {
            self.queue_command(
                CommandRequest::RetryTurn {
                    turn_id: turn.turn_id,
                    expected_version: turn.expected_version,
                },
                commands,
            );
        }
    }

    fn confirm_setup(&mut self, commands: &mut Vec<Command>) {
        let Some(candidate) = self.setup.selected_candidate() else {
            return;
        };
        if !candidate.eligible() {
            self.push_notification(Notification::public(
                "That Agent is unavailable or not authenticated; choose an eligible Agent.",
            ));
            return;
        }
        self.queue_command(
            CommandRequest::SelectAgent {
                agent_id: candidate.id.clone(),
            },
            commands,
        );
    }

    /// Open the card represented by the currently selected Project row.
    ///
    /// This action only changes local modal state. The existing modal reducer
    /// is the sole path that emits an approval/review mutation, so opening a
    /// card can never approve or accept it implicitly.
    fn open_selected_card(&mut self) {
        let modal = match self.rail.tab {
            ProjectTab::Approvals => self
                .approvals
                .get(self.rail.selected_approval)
                .cloned()
                .filter(|card| card.visibility.is_public())
                .map(ModalState::Approval),
            ProjectTab::Tasks => self.selected_task().cloned().map(|task| {
                if task.state == TaskState::AwaitingReview {
                    self.review_cards
                        .iter()
                        .find(|card| card.task_id == task.id)
                        .cloned()
                        .filter(|card| card.visibility.is_public())
                        .map_or_else(|| ModalState::Task(task), ModalState::Review)
                } else {
                    ModalState::Task(task)
                }
            }),
            ProjectTab::Attention => None,
        };
        if let Some(modal) = modal {
            self.modal = Some(modal);
            self.focus = FocusTarget::Modal;
        }
    }

    fn queue_command(&mut self, request: CommandRequest, commands: &mut Vec<Command>) {
        let id = self.next_command_id;
        self.next_command_id = self.next_command_id.saturating_add(1);
        let idempotency_key = format!("{}-{id}", self.idempotency_prefix);
        let command = Command {
            id,
            idempotency_key,
            request,
        };
        let pending = PendingCommand::new(command.id, command.request.clone());
        self.pending_commands.insert(id, pending);
        if matches!(&command.request, CommandRequest::SendMessage { .. }) {
            self.retryable_send = Some(command.clone());
        }
        commands.push(command);
    }

    fn finish_command(&mut self, result: CommandResult, commands: &mut Vec<Command>) {
        let Some(pending) = self.pending_commands.remove(&result.command_id) else {
            return;
        };
        match result.outcome {
            CommandOutcome::Succeeded { .. } => match pending.request {
                CommandRequest::SendMessage { text } => {
                    if self
                        .retryable_send
                        .as_ref()
                        .is_some_and(|command| command.id == pending.command_id)
                    {
                        self.retryable_send = None;
                    }
                    if self.composer.text == text {
                        self.composer.clear();
                    } else {
                        self.composer.submitting = false;
                    }
                    self.push_notification(Notification::public("Message sent."));
                }
                CommandRequest::CancelTurn { turn_id, .. } => {
                    if let Some(activity) = self.live_activity.as_mut() {
                        if activity.turn_id == turn_id {
                            activity.state = TurnState::Cancelled;
                        }
                    }
                    self.push_notification(Notification::public("Turn cancellation requested."));
                }
                CommandRequest::RetryTurn { turn_id, .. } => {
                    self.retryable_turn = None;
                    self.push_notification(Notification::public(format!(
                        "Retry requested for turn {turn_id}."
                    )));
                }
                CommandRequest::Approval { .. }
                | CommandRequest::Review { .. }
                | CommandRequest::AnswerQuestion { .. }
                | CommandRequest::SelectAgent { .. } => {
                    self.push_notification(Notification::public("Action accepted."));
                }
                CommandRequest::Refresh
                | CommandRequest::Shutdown
                | CommandRequest::ForceShutdown => {}
            },
            CommandOutcome::Failed { error } => {
                let retryable = error.retryable;
                if let CommandRequest::SendMessage { .. } = pending.request {
                    self.composer.submitting = false;
                    self.composer.error = Some(error.clone());
                    if !retryable
                        && self
                            .retryable_send
                            .as_ref()
                            .is_some_and(|command| command.id == pending.command_id)
                    {
                        self.retryable_send = None;
                    }
                }
                let mut failure = error;
                if failure.visibility == ContentVisibility::Protected {
                    // Keep the state useful without retaining an internal
                    // provider error in a notification rendered later.
                    failure.message.clear();
                }
                self.push_notification(if failure.visibility.is_public() {
                    Notification::public(if failure.message.is_empty() {
                        "Action failed; no details were provided.".to_owned()
                    } else {
                        failure.message.clone()
                    })
                } else {
                    Notification::protected()
                });
                if failure.conflict {
                    commands.push(self.make_refresh_command());
                } else if retryable {
                    self.retryable_turn = self.retryable_turn.take();
                }
            }
        }
    }

    fn make_refresh_command(&mut self) -> Command {
        let id = self.next_command_id;
        self.next_command_id = self.next_command_id.saturating_add(1);
        let request = CommandRequest::Refresh;
        let idempotency_key = format!("{}-{id}", self.idempotency_prefix);
        self.pending_commands
            .insert(id, PendingCommand::new(id, request.clone()));
        Command {
            id,
            idempotency_key,
            request,
        }
    }

    fn close_modal(&mut self) {
        self.modal = None;
        self.focus = if matches!(
            self.setup,
            SetupState::AgentPicker { .. } | SetupState::Unavailable { .. }
        ) {
            FocusTarget::SetupPicker
        } else {
            self.primary_view.default_focus()
        };
    }

    fn push_notification(&mut self, notification: Notification) {
        self.notifications.push(notification);
        retain_latest(&mut self.notifications, self.limits.notifications);
    }

    fn upsert_message(&mut self, message: ChatMessage) {
        if let Some(existing) = self.timeline.iter_mut().find(|item| item.id == message.id) {
            *existing = message;
        } else {
            self.timeline.push(message);
        }
        retain_latest(&mut self.timeline, self.limits.timeline_messages);
        if self.scroll.follow_tail {
            self.scroll.timeline_offset = 0;
        }
    }

    fn set_size(&mut self, width: u16, height: u16) {
        self.terminal_width = width;
        self.terminal_height = height;
        self.layout = LayoutMode::for_size(width, height);
        self.clamp_selections();
    }

    fn select_primary_view(&mut self, view: PrimaryView) {
        self.primary_view = view;
        self.focus = if matches!(
            self.setup,
            SetupState::AgentPicker { .. } | SetupState::Unavailable { .. }
        ) {
            FocusTarget::SetupPicker
        } else {
            view.default_focus()
        };
    }

    fn select_project_tab(&mut self, tab: ProjectTab) {
        self.primary_view = PrimaryView::Kanban;
        self.rail.tab = tab;
        self.focus = FocusTarget::ProjectRail;
    }

    pub(super) fn clamp_selections(&mut self) {
        if self.tasks.is_empty() {
            self.rail.selected_task = 0;
            self.rail.lane = KanbanLane::Queued;
        } else {
            self.rail.selected_task = self.rail.selected_task.min(self.tasks.len() - 1);
            if !self
                .tasks
                .iter()
                .any(|task| task.state.kanban_lane() == self.rail.lane)
            {
                self.rail.lane = self.tasks[self.rail.selected_task].state.kanban_lane();
            }
            if self.tasks[self.rail.selected_task].state.kanban_lane() != self.rail.lane {
                self.rail.selected_task = self
                    .tasks
                    .iter()
                    .position(|task| task.state.kanban_lane() == self.rail.lane)
                    .unwrap_or(0);
            }
            for (index, task) in self.tasks.iter_mut().enumerate() {
                task.selected = index == self.rail.selected_task;
            }
        }
        if self.approvals.is_empty() {
            self.rail.selected_approval = 0;
        } else {
            self.rail.selected_approval = self.rail.selected_approval.min(self.approvals.len() - 1);
        }
        if let SetupState::AgentPicker {
            candidates,
            selected,
            ..
        } = &mut self.setup
        {
            *selected = if candidates.is_empty() {
                0
            } else {
                (*selected).min(candidates.len() - 1)
            };
        }
    }

    fn move_up(&mut self) {
        match self.focus {
            FocusTarget::Timeline => self.scroll_page(-1),
            FocusTarget::Activity => self.scroll_activity(-1),
            FocusTarget::ProjectRail => self.select_task(-1),
            FocusTarget::SetupPicker => self.setup.move_selection(-1),
            _ => {}
        }
    }

    fn move_down(&mut self) {
        match self.focus {
            FocusTarget::Timeline => self.scroll_page(1),
            FocusTarget::Activity => self.scroll_activity(1),
            FocusTarget::ProjectRail => self.select_task(1),
            FocusTarget::SetupPicker => self.setup.move_selection(1),
            _ => {}
        }
    }

    fn scroll_page(&mut self, direction: i8) {
        let amount = 8;
        if direction < 0 {
            self.scroll.timeline_offset = self.scroll.timeline_offset.saturating_add(amount);
            self.scroll.follow_tail = false;
        } else {
            self.scroll.timeline_offset = self.scroll.timeline_offset.saturating_sub(amount);
            if self.scroll.timeline_offset == 0 {
                self.scroll.follow_tail = true;
            }
        }
    }

    fn scroll_activity(&mut self, direction: i8) {
        if direction < 0 {
            self.scroll.activity_offset = self.scroll.activity_offset.saturating_add(4);
        } else {
            self.scroll.activity_offset = self.scroll.activity_offset.saturating_sub(4);
        }
    }

    fn select_task(&mut self, delta: isize) {
        if self.rail.tab == ProjectTab::Approvals {
            if self.approvals.is_empty() {
                self.rail.selected_approval = 0;
                return;
            }
            let len = self.approvals.len();
            self.rail.selected_approval =
                (self.rail.selected_approval as isize + delta).rem_euclid(len as isize) as usize;
            return;
        }
        if self.rail.tab != ProjectTab::Tasks {
            return;
        }

        let lane_tasks = self
            .tasks
            .iter()
            .enumerate()
            .filter_map(|(index, task)| {
                (task.state.kanban_lane() == self.rail.lane).then_some(index)
            })
            .collect::<Vec<_>>();
        if lane_tasks.is_empty() {
            return;
        }
        let current = lane_tasks
            .iter()
            .position(|index| *index == self.rail.selected_task)
            .unwrap_or(0);
        let next = (current as isize + delta).rem_euclid(lane_tasks.len() as isize) as usize;
        self.set_selected_task(lane_tasks[next]);
    }

    fn select_lane(&mut self, delta: isize) {
        let current_row = self
            .tasks
            .iter()
            .enumerate()
            .filter(|(_, task)| task.state.kanban_lane() == self.rail.lane)
            .position(|(index, _)| index == self.rail.selected_task)
            .unwrap_or(0);

        let mut lane = self.rail.lane;
        for _ in 0..KanbanLane::ALL.len() {
            lane = if delta < 0 {
                lane.previous()
            } else {
                lane.next()
            };
            let lane_tasks = self
                .tasks
                .iter()
                .enumerate()
                .filter_map(|(index, task)| (task.state.kanban_lane() == lane).then_some(index))
                .collect::<Vec<_>>();
            if !lane_tasks.is_empty() {
                let next = lane_tasks[current_row.min(lane_tasks.len() - 1)];
                self.rail.lane = lane;
                self.set_selected_task(next);
                return;
            }
        }

        self.rail.lane = if delta < 0 {
            self.rail.lane.previous()
        } else {
            self.rail.lane.next()
        };
        for task in &mut self.tasks {
            task.selected = false;
        }
    }

    fn set_selected_task(&mut self, selected: usize) {
        self.rail.selected_task = selected.min(self.tasks.len().saturating_sub(1));
        if let Some(task) = self.tasks.get(self.rail.selected_task) {
            self.rail.lane = task.state.kanban_lane();
        }
        for (index, task) in self.tasks.iter_mut().enumerate() {
            task.selected = index == self.rail.selected_task;
        }
    }

    fn has_pending_send(&self) -> bool {
        self.pending_commands
            .values()
            .any(|pending| matches!(pending.request, CommandRequest::SendMessage { .. }))
    }

    fn has_pending_retry(&self) -> bool {
        self.pending_commands
            .values()
            .any(|pending| matches!(pending.request, CommandRequest::RetryTurn { .. }))
    }
}

fn bounded_messages(mut messages: Vec<ChatMessage>, max: usize) -> Vec<ChatMessage> {
    let mut result = Vec::with_capacity(messages.len().min(max));
    for message in messages.drain(..) {
        if let Some(existing) = result
            .iter_mut()
            .find(|item: &&mut ChatMessage| item.id == message.id)
        {
            *existing = message;
        } else {
            result.push(message);
        }
    }
    retain_latest(&mut result, max);
    result
}

fn normalize_setup(setup: &mut SetupState) {
    if let SetupState::AgentPicker {
        candidates,
        selected,
        ..
    } = setup
    {
        // Harness discovery is expected to return a small set.  Keep a hard
        // ceiling here as a last line of defence for a malformed adapter.
        candidates.truncate(32);
        if *selected >= candidates.len() {
            *selected = candidates.len().saturating_sub(1);
        }
    }
}

fn normalize_modal(modal: &mut ModalState, limits: &AppLimits) {
    match modal {
        ModalState::Question(card) => {
            card.options.truncate(32);
            card.selected_option = card
                .selected_option
                .min(card.options.len().saturating_sub(1));
        }
        ModalState::Approval(card) => {
            card.details.truncate(48);
            card.permitted_actions.truncate(8);
            card.selected_action = card
                .selected_action
                .min(card.permitted_actions.len().saturating_sub(1));
        }
        ModalState::Review(card) => {
            card.checks.truncate(limits.checks_per_task);
            card.changed_files.truncate(128);
            card.permitted_actions.truncate(8);
            card.selected_action = card
                .selected_action
                .min(card.permitted_actions.len().saturating_sub(1));
        }
        ModalState::Task(task) => {
            task.checks.truncate(limits.checks_per_task);
            task.changed_files.truncate(128);
        }
        ModalState::Cancel(_) | ModalState::Help | ModalState::Error(_) => {}
    }
}
