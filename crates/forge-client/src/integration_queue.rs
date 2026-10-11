//! Text for the merge queue reads: `forge-ctl task show` and
//! `forge-ctl repo queue`. The words are the ones the web shows.
use api_types::{
    IntegrationMemberState, IntegrationPhase, IntegrationQueueResponse, IntegrationQueueState,
    IntegrationWait, IntegrationWaitCode, TaskIntegrationSummary,
};
use tabled::Table;

fn phase_text(phase: &IntegrationPhase) -> &'static str {
    match phase {
        IntegrationPhase::Validating => "validating",
        IntegrationPhase::Rebasing => "rebasing",
        IntegrationPhase::Checking => "checking",
        IntegrationPhase::AwaitingCarry => "carrying its review",
        IntegrationPhase::AwaitingAuthorization => "authorizing",
        IntegrationPhase::FastForwarding => "fast-forwarding",
        IntegrationPhase::Reconciling => "confirming the result",
    }
}

/// The cause of a wait, as the short phrase after "waiting".
pub fn wait_text(wait: &IntegrationWait) -> String {
    let machine = || {
        wait.machine_name
            .clone()
            .or_else(|| wait.machine_id.clone())
            .unwrap_or_else(|| "its machine".to_owned())
    };
    match wait.code {
        IntegrationWaitCode::Queue => "others are ahead".to_owned(),
        IntegrationWaitCode::QueueSuspended => "the target checkout is not ready".to_owned(),
        IntegrationWaitCode::QueueQuarantined => "another merge has an unknown result".to_owned(),
        IntegrationWaitCode::TaskHeld => "held".to_owned(),
        IntegrationWaitCode::TaskBlocked => "blocked".to_owned(),
        IntegrationWaitCode::TaskFailed => "failed".to_owned(),
        IntegrationWaitCode::TaskEntryBlocked => "entry blocked".to_owned(),
        IntegrationWaitCode::TaskAwaitingHuman => "a person's decision".to_owned(),
        IntegrationWaitCode::ProjectPaused => "project paused".to_owned(),
        IntegrationWaitCode::OwnerOffline => format!("for {}", machine()),
        IntegrationWaitCode::TargetDirty => "the target checkout has local changes".to_owned(),
        IntegrationWaitCode::Infrastructure => "retrying after a failure".to_owned(),
        IntegrationWaitCode::BudgetExhausted => "retries spent".to_owned(),
        IntegrationWaitCode::OwnerRequired => "the owner must decide".to_owned(),
        IntegrationWaitCode::UnresolvedResult => {
            format!("merge result unknown on {}", machine())
        }
        IntegrationWaitCode::CancelRequested => "cancelling".to_owned(),
        // A cause a newer server names: the server's own words say it.
        IntegrationWaitCode::Unknown => "see the message".to_owned(),
    }
}

/// One line for where a Task is in the merge of its change.
pub fn integration_label(summary: &TaskIntegrationSummary) -> String {
    match summary.state {
        IntegrationMemberState::Waiting => match (&summary.wait, summary.position) {
            (Some(wait), _) if wait.code != IntegrationWaitCode::Queue => {
                format!("waiting: {}", wait_text(wait))
            }
            (_, Some(0)) | (None, None) => "in merge queue (next)".to_owned(),
            (_, Some(ahead)) => format!("in merge queue ({ahead} ahead)"),
            (Some(_), None) => "in merge queue".to_owned(),
        },
        IntegrationMemberState::Owned => format!(
            "merging: {}",
            summary.phase.as_ref().map_or("starting", phase_text)
        ),
        IntegrationMemberState::Repair => "fix merge: sent back to its coder".to_owned(),
        IntegrationMemberState::ReviewRequired => "sent back for a fresh review".to_owned(),
        IntegrationMemberState::CandidateCheckFailed => "merge check failed".to_owned(),
        IntegrationMemberState::Deferred => match &summary.wait {
            Some(wait) => format!("merge stopped: {}", wait_text(wait)),
            None => "merge stopped".to_owned(),
        },
        IntegrationMemberState::Applied => "merged; closing the Task".to_owned(),
        IntegrationMemberState::Unknown => "in merge queue".to_owned(),
    }
}

fn queue_state_text(state: IntegrationQueueState) -> &'static str {
    match state {
        IntegrationQueueState::Open => "open",
        IntegrationQueueState::Suspended => "suspended",
        IntegrationQueueState::Quarantined => "quarantined",
        IntegrationQueueState::Closed => "closed",
        IntegrationQueueState::Unknown => "not open",
    }
}

fn position_text(position: Option<u32>) -> String {
    match position {
        Some(0) => "head".to_owned(),
        Some(ahead) => format!("{ahead} ahead"),
        None => "not in line".to_owned(),
    }
}

/// The merge-queue section `task show` prints under the Task's row.
pub fn task_integration_text(summary: &TaskIntegrationSummary) -> String {
    let mut text = format!(
        "Merge queue: {}{}\n",
        summary.target_branch.as_deref().unwrap_or("(no target)"),
        match summary.queue_state {
            Some(IntegrationQueueState::Open) | None => String::new(),
            Some(state) => format!(" (queue {})", queue_state_text(state)),
        }
    );
    text.push_str(&format!("  state     {}\n", integration_label(summary)));
    text.push_str(&format!(
        "  position  {}\n",
        position_text(summary.position)
    ));
    if let Some(wait) = &summary.wait {
        text.push_str(&format!("  wait      {}", wait_text(wait)));
        if let Some(message) = &wait.message {
            text.push_str(&format!(": {message}"));
        }
        if let Some(retry_at) = &wait.retry_at {
            text.push_str(&format!(" (retry at {retry_at})"));
        }
        text.push('\n');
    }
    text.push_str(&format!(
        "  queued    {} (round {})\n",
        summary.timings.enqueued_at, summary.timings.rounds
    ));
    text
}

/// A Task title as one table cell: one line, at most 60 characters.
fn title_cell(title: Option<&str>) -> String {
    const WIDTH: usize = 60;
    let Some(title) = title else {
        return "(deleted)".to_owned();
    };
    let line = title
        .chars()
        .map(|c| if c.is_control() { ' ' } else { c })
        .collect::<String>();
    if line.chars().count() <= WIDTH {
        return line;
    }
    let mut cut = line.chars().take(WIDTH - 1).collect::<String>();
    cut.push('\u{2026}');
    cut
}

/// `repo queue`: each queue's line, then its members in queue order.
pub fn queues_text(queues: &[IntegrationQueueResponse]) -> String {
    if queues.is_empty() {
        return "No merge queue: nothing has been queued for this repository.\n".to_owned();
    }
    let mut text = String::new();
    for queue in queues {
        text.push_str(&format!(
            "Queue {}: {}",
            queue.target_branch,
            queue_state_text(queue.state)
        ));
        if let Some(reason) = &queue.state_reason {
            text.push_str(&format!(" ({}", reason.code));
            if let Some(message) = &reason.message {
                text.push_str(&format!(": {message}"));
            }
            text.push(')');
        }
        text.push_str(&format!("  members {}", queue.member_count));
        if queue.members_truncated {
            text.push_str(&format!(" (first {} shown)", queue.members.len()));
        }
        text.push('\n');
        if queue.members.is_empty() {
            continue;
        }
        let rows = queue
            .members
            .iter()
            .map(|member| {
                vec![
                    position_text(member.integration.position),
                    member.task_id.chars().take(8).collect(),
                    title_cell(member.task_title.as_deref()),
                    integration_label(&member.integration),
                    member.integration.timings.enqueued_at.clone(),
                ]
            })
            .collect::<Vec<_>>();
        text.push_str(&format!(
            "{}\n",
            Table::from_rows(&["Position", "Task", "Title", "State", "Queued"], rows)
        ));
    }
    text
}

#[cfg(test)]
mod tests {
    use super::*;
    use api_types::{
        IntegrationQueueMemberResponse, IntegrationQueueStateReason, IntegrationTimings,
    };

    fn summary(
        state: IntegrationMemberState,
        phase: Option<IntegrationPhase>,
        position: Option<u32>,
        wait: Option<(IntegrationWaitCode, Option<&str>)>,
    ) -> TaskIntegrationSummary {
        TaskIntegrationSummary {
            attempt_id: "attempt".into(),
            queue_id: Some("queue".into()),
            repo_id: Some("repo".into()),
            target_branch: Some("main".into()),
            queue_state: Some(IntegrationQueueState::Open),
            state,
            phase,
            position,
            wait: wait.map(|(code, message)| IntegrationWait {
                code,
                message: message.map(str::to_owned),
                machine_id: matches!(
                    code,
                    IntegrationWaitCode::OwnerOffline | IntegrationWaitCode::UnresolvedResult
                )
                .then(|| "daemon-1".to_owned()),
                machine_name: matches!(code, IntegrationWaitCode::OwnerOffline)
                    .then(|| "build-box".to_owned()),
                retry_at: matches!(code, IntegrationWaitCode::Infrastructure)
                    .then(|| "2026-10-01T00:05:00Z".to_owned()),
            }),
            action_hint: None,
            timings: IntegrationTimings {
                enqueued_at: "2026-10-01T00:00:00Z".into(),
                rounds: 1,
                ..Default::default()
            },
        }
    }

    #[test]
    fn labels_cover_every_state_of_a_member() {
        use IntegrationMemberState as M;
        use IntegrationWaitCode as W;
        let label = |state, phase, position, wait| {
            integration_label(&summary(state, phase, position, wait))
        };
        assert_eq!(
            label(M::Waiting, None, Some(2), Some((W::Queue, None))),
            "in merge queue (2 ahead)"
        );
        assert_eq!(
            label(M::Waiting, None, Some(0), None),
            "in merge queue (next)"
        );
        assert_eq!(
            label(M::Waiting, None, None, Some((W::ProjectPaused, None))),
            "waiting: project paused"
        );
        assert_eq!(
            label(M::Waiting, None, None, Some((W::TaskHeld, None))),
            "waiting: held"
        );
        assert_eq!(
            label(M::Owned, Some(IntegrationPhase::Rebasing), Some(0), None),
            "merging: rebasing"
        );
        assert_eq!(
            label(M::Owned, Some(IntegrationPhase::Checking), Some(0), None),
            "merging: checking"
        );
        assert_eq!(
            label(
                M::Owned,
                Some(IntegrationPhase::FastForwarding),
                Some(0),
                None
            ),
            "merging: fast-forwarding"
        );
        assert_eq!(
            label(M::Repair, None, None, None),
            "fix merge: sent back to its coder"
        );
        assert_eq!(
            label(M::ReviewRequired, None, None, None),
            "sent back for a fresh review"
        );
        assert_eq!(
            label(M::CandidateCheckFailed, None, None, None),
            "merge check failed"
        );
        assert_eq!(
            label(M::Deferred, None, None, Some((W::OwnerOffline, None))),
            "merge stopped: for build-box"
        );
        assert_eq!(
            label(M::Deferred, None, None, Some((W::UnresolvedResult, None))),
            "merge stopped: merge result unknown on daemon-1"
        );
        assert_eq!(
            label(M::Applied, None, None, None),
            "merged; closing the Task"
        );
    }

    #[test]
    fn task_show_prints_target_phase_position_and_wait() {
        assert_eq!(
            task_integration_text(&summary(
                IntegrationMemberState::Owned,
                Some(IntegrationPhase::Checking),
                Some(0),
                None
            )),
            "Merge queue: main\n  state     merging: checking\n  position  head\n  queued    2026-10-01T00:00:00Z (round 1)\n"
        );
        let mut parked = summary(
            IntegrationMemberState::Deferred,
            None,
            None,
            Some((
                IntegrationWaitCode::Infrastructure,
                Some("the owner did not answer"),
            )),
        );
        parked.queue_state = Some(IntegrationQueueState::Suspended);
        assert_eq!(
            task_integration_text(&parked),
            "Merge queue: main (queue suspended)\n  state     merge stopped: retrying after a failure\n  position  not in line\n  wait      retrying after a failure: the owner did not answer (retry at 2026-10-01T00:05:00Z)\n  queued    2026-10-01T00:00:00Z (round 1)\n"
        );
    }

    fn queue() -> IntegrationQueueResponse {
        let member = |task: &str, title: &str, integration| IntegrationQueueMemberResponse {
            task_id: task.into(),
            task_title: Some(title.into()),
            task_status: Some("merging".into()),
            integration,
        };
        IntegrationQueueResponse {
            id: "queue".into(),
            repo_id: "repo".into(),
            target_branch: "main".into(),
            state: IntegrationQueueState::Open,
            state_reason: None,
            head_task_id: Some("11111111-aaaa".into()),
            head_attempt_id: Some("attempt".into()),
            head_since: Some("2026-10-01T00:01:00Z".into()),
            member_count: 3,
            members: vec![
                member(
                    "11111111-aaaa",
                    "Add login",
                    summary(
                        IntegrationMemberState::Owned,
                        Some(IntegrationPhase::Rebasing),
                        Some(0),
                        None,
                    ),
                ),
                member(
                    "22222222-bbbb",
                    "Fix logout",
                    summary(
                        IntegrationMemberState::Waiting,
                        None,
                        Some(1),
                        Some((IntegrationWaitCode::Queue, None)),
                    ),
                ),
                member(
                    "33333333-cccc",
                    "Docs",
                    summary(
                        IntegrationMemberState::Deferred,
                        None,
                        None,
                        Some((IntegrationWaitCode::TargetDirty, None)),
                    ),
                ),
            ],
            members_truncated: false,
        }
    }

    #[test]
    fn repo_queue_table_lists_members_in_queue_order() {
        assert_eq!(
            queues_text(&[]),
            "No merge queue: nothing has been queued for this repository.\n"
        );
        let mut suspended = queue();
        suspended.target_branch = "release".into();
        suspended.state = IntegrationQueueState::Suspended;
        suspended.state_reason = Some(IntegrationQueueStateReason {
            code: "target_unavailable".into(),
            message: Some("the default checkout is not ready".into()),
        });
        suspended.member_count = 0;
        suspended.members.clear();
        let text = queues_text(&[queue(), suspended]);
        let lines = text.lines().collect::<Vec<_>>();
        assert_eq!(lines[0], "Queue main: open  members 3");
        let row = |needle: &str| {
            lines
                .iter()
                .find(|line| line.contains(needle))
                .unwrap_or_else(|| panic!("{needle} in {text}"))
                .split('|')
                .map(str::trim)
                .filter(|cell| !cell.is_empty())
                .map(str::to_owned)
                .collect::<Vec<_>>()
        };
        assert_eq!(
            row("Position"),
            ["Position", "Task", "Title", "State", "Queued"]
        );
        assert_eq!(
            row("11111111"),
            [
                "head",
                "11111111",
                "Add login",
                "merging: rebasing",
                "2026-10-01T00:00:00Z"
            ]
        );
        assert_eq!(
            row("22222222"),
            [
                "1 ahead",
                "22222222",
                "Fix logout",
                "in merge queue (1 ahead)",
                "2026-10-01T00:00:00Z"
            ]
        );
        assert_eq!(
            row("33333333"),
            [
                "not in line",
                "33333333",
                "Docs",
                "merge stopped: the target checkout has local changes",
                "2026-10-01T00:00:00Z"
            ]
        );
        let order = ["11111111", "22222222", "33333333"].map(|task| text.find(task).unwrap());
        assert!(order[0] < order[1] && order[1] < order[2]);
        assert_eq!(
            *lines.last().unwrap(),
            "Queue release: suspended (target_unavailable: the default checkout is not ready)  members 0"
        );
    }

    #[test]
    fn a_long_or_deleted_title_and_an_unknown_state_keep_the_table_readable() {
        let mut queue = queue();
        queue.members[0].task_title = Some(format!("{}\nsecond line", "x".repeat(200)));
        queue.members[1].task_title = None;
        queue.members[2].integration.state = IntegrationMemberState::Unknown;
        queue.members[2].integration.wait = Some(IntegrationWait {
            code: IntegrationWaitCode::Unknown,
            message: Some("the merge train is forming".into()),
            machine_id: None,
            machine_name: None,
            retry_at: None,
        });
        queue.state = IntegrationQueueState::Unknown;
        let text = queues_text(&[queue.clone()]);
        // One line a member, whatever its title holds; no line runs away.
        assert_eq!(text.lines().count(), 9, "{text}");
        assert!(
            text.lines().all(|line| line.chars().count() < 200),
            "{text}"
        );
        assert!(
            text.contains(&format!("{}\u{2026}", "x".repeat(59))),
            "{text}"
        );
        assert!(text.contains("(deleted)"), "{text}");
        assert!(
            text.starts_with("Queue main: not open  members 3"),
            "{text}"
        );
        assert!(text.contains("in merge queue"), "{text}");
        // `task show` says what the server said about a wait it cannot name.
        assert!(task_integration_text(&queue.members[2].integration)
            .contains("wait      see the message: the merge train is forming"),);
    }

    #[test]
    fn repo_queue_json_is_the_route_response() {
        let json = serde_json::to_value(api_types::IntegrationQueuesResponse {
            items: vec![queue()],
            next_cursor: None,
            has_more: false,
        })
        .unwrap();
        assert_eq!(json["items"][0]["target_branch"], "main");
        assert_eq!(
            json["items"][0]["members"][0]["integration"]["phase"],
            "rebasing"
        );
        assert_eq!(json["items"][0]["members"][1]["integration"]["position"], 1);
        assert_eq!(
            json["items"][0]["members"][2]["integration"]["wait"]["code"],
            "target_dirty"
        );
        assert_eq!(json["has_more"], false);
    }
}
