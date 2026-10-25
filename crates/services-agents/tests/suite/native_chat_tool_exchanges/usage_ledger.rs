//! Per-turn provider call accounting and topic summary settlement.

use super::*;

/// The host boundary every native scope shares (chat, Task worker, inquiry):
/// a turn on a restored session reports its own provider calls, in both the
/// per-attempt reports and the aggregate counters.
#[tokio::test]
async fn a_native_turn_reports_only_the_provider_calls_it_made() {
    let fixture = chat_fixture().await;
    let backend = NativeAgentRuntimeBackend::new(fixture.service.protected_store())
        .with_provider_override(scripted_provider(vec![
            metered_text_step("first reply", MEASURED_CALLS[0]),
            metered_tool_call_step(REUSED_ID, "forge_scope_propose", MEASURED_CALLS[1]),
            metered_text_step("second reply", MEASURED_CALLS[2]),
            metered_text_step("third reply", MEASURED_CALLS[3]),
        ]));

    let mut report_ids = std::collections::BTreeSet::new();
    for (turn, calls) in [
        &MEASURED_CALLS[0..1],
        &MEASURED_CALLS[1..3],
        &MEASURED_CALLS[3..4],
    ]
    .into_iter()
    .enumerate()
    {
        let output = backend
            .run_turn(fixture.turn(&format!("turn {turn}")), Arc::new(NoopSink))
            .await
            .unwrap_or_else(|error| panic!("turn {turn} must complete: {error}"));
        let reported: Vec<(u64, u64)> = output
            .usage_reports
            .iter()
            .map(|report| {
                (
                    report.input_tokens.expect("metered input"),
                    report.output_tokens.expect("metered output"),
                )
            })
            .collect();
        assert_eq!(reported, calls, "turn {turn} reports its own calls only");
        assert_eq!(
            (output.input_tokens, output.output_tokens),
            (
                calls.iter().map(|call| call.0).sum::<u64>(),
                calls.iter().map(|call| call.1).sum::<u64>()
            ),
            "turn {turn}'s aggregate counters cover that turn, not the session"
        );
        for report in &output.usage_reports {
            assert!(
                report_ids.insert(report.report_id.clone()),
                "a provider call is reported under one id, once: {}",
                report.report_id
            );
        }
    }
    assert_eq!(report_ids.len(), MEASURED_CALLS.len());
}

/// The reported shape: several turns, one provider call each. Turn n's
/// recorded usage is call n's, and every total is the sum of the calls.
#[cfg(feature = "test-support")]
#[tokio::test]
async fn each_chat_turn_records_its_own_provider_call_and_totals_are_the_sum_of_the_calls() {
    let chat = ledger_chat(
        MEASURED_CALLS
            .iter()
            .enumerate()
            .map(|(turn, call)| metered_text_step(&format!("reply {turn}"), *call))
            .collect(),
    )
    .await;

    for (turn, call) in MEASURED_CALLS.iter().enumerate() {
        let job = chat.turn(&format!("message {turn}")).await;
        assert_eq!(job.status, db::AgentChatTurnState::Succeeded, "turn {turn}");
        assert_eq!(
            chat.recorded_calls(&job.id).await,
            [recorded(0, 0, *call)],
            "turn {turn} records its own provider call, not the session so far"
        );

        // What the chat shows for this reply, and the turn's own aggregate.
        let reply = services::usage_breakdowns_for_source(&chat.db, &job.id)
            .await
            .expect("reply usage");
        assert_eq!(reply.len(), 1, "turn {turn}");
        let expected_tokens = api_types::TokenCounters {
            input_tokens: i64::try_from(call.0).unwrap(),
            output_tokens: i64::try_from(call.1).unwrap(),
            cache_read_tokens: 0,
            cache_write_tokens: 0,
        };
        assert_eq!(reply[0].counters, Some(expected_tokens.clone()));
        let aggregate = services::usage_aggregate_for_source(&chat.db, &job.id)
            .await
            .expect("turn aggregate");
        assert_eq!(aggregate.tokens, expected_tokens);
        assert_eq!(aggregate.counts.provider_attempt_count, 1);
        assert_eq!(
            aggregate.cost.complete_total,
            Some(api_types::MoneyAmount {
                currency: "USD".to_owned(),
                decimal: nano_decimal(nano_usd(*call)),
            })
        );

        let turns = i64::try_from(turn + 1).unwrap();
        chat.assert_totals_count_each_call_once(&MEASURED_CALLS[..=turn], turns, turns)
            .await;
    }
    assert_eq!(chat.provider.requests().len(), MEASURED_CALLS.len());
}

/// A tool loop makes several provider calls in one turn. Each is one event
/// under that turn, and the following turn does not record them again.
#[cfg(feature = "test-support")]
#[tokio::test]
async fn a_tool_loop_records_each_of_its_provider_calls_once() {
    let chat = ledger_chat(vec![
        metered_tool_call_step(REUSED_ID, "forge_scope_propose", MEASURED_CALLS[0]),
        metered_tool_call_step("call_2", "forge_task_command", MEASURED_CALLS[1]),
        metered_text_step("both tools answered.", MEASURED_CALLS[2]),
        metered_text_step("a plain follow-up.", MEASURED_CALLS[3]),
    ])
    .await;

    let tool_loop = chat.turn("run two tools").await;
    assert_eq!(tool_loop.status, db::AgentChatTurnState::Succeeded);
    assert_eq!(
        chat.recorded_calls(&tool_loop.id).await,
        [
            recorded(0, 0, MEASURED_CALLS[0]),
            recorded(0, 1, MEASURED_CALLS[1]),
            recorded(0, 2, MEASURED_CALLS[2]),
        ]
    );
    chat.assert_totals_count_each_call_once(&MEASURED_CALLS[..3], 1, 1)
        .await;

    let follow_up = chat.turn("and then").await;
    assert_eq!(follow_up.status, db::AgentChatTurnState::Succeeded);
    assert_eq!(
        chat.recorded_calls(&follow_up.id).await,
        [recorded(0, 0, MEASURED_CALLS[3])]
    );
    chat.assert_totals_count_each_call_once(&MEASURED_CALLS, 2, 2)
        .await;
    assert_eq!(chat.provider.requests().len(), MEASURED_CALLS.len());
}

/// A turn attempt that fails after the provider metered it is retried on the
/// same restored session. The runtime gives one turn attempt three provider
/// attempts; when all three fail the turn waits and Forge runs it again. The
/// failed calls stay on the failed attempt, and the retry records only the
/// call it made.
#[cfg(feature = "test-support")]
#[tokio::test]
async fn a_retried_turn_does_not_record_the_failed_attempts_calls_again() {
    use agent_runtime::core::provider::{ProviderError, ProviderErrorKind};

    let earlier = MEASURED_CALLS[0];
    let failed = [(5_679, 11), (5_680, 12), (5_681, 13)];
    let retried = MEASURED_CALLS[2];
    let failed_step = |(input, output): (u64, u64)| {
        ScriptedStream::new(vec![
            usage_event(input, output),
            ProviderStreamEvent::Error {
                error: ProviderError::new(ProviderErrorKind::Server, "provider returned HTTP 503"),
            },
        ])
    };
    let chat = ledger_chat(vec![
        metered_text_step("an earlier reply.", earlier),
        failed_step(failed[0]),
        failed_step(failed[1]),
        failed_step(failed[2]),
        metered_text_step("the retry answered.", retried),
    ])
    .await;

    let first = chat.turn("an earlier turn").await;
    assert_eq!(first.status, db::AgentChatTurnState::Succeeded);

    let waiting = chat.turn("this turn's first attempt fails").await;
    assert_eq!(waiting.status, db::AgentChatTurnState::RetryWait);
    let failed_attempt = [
        recorded(0, 0, failed[0]),
        recorded(0, 1, failed[1]),
        recorded(0, 2, failed[2]),
    ];
    assert_eq!(
        chat.recorded_calls(&waiting.id).await,
        failed_attempt,
        "the failed attempt keeps the calls the provider metered"
    );

    // Past the turn's retry cooldown and the provider entry's backoff.
    let later = chrono::Utc::now() + chrono::Duration::hours(1);
    assert_eq!(chat.worker.run_once_at(later).await.unwrap(), 1);
    let retried_job = chat.job(&waiting.id).await;
    assert_eq!(retried_job.status, db::AgentChatTurnState::Succeeded);
    assert_eq!(retried_job.attempt_count, 2);
    let mut both_attempts = failed_attempt.to_vec();
    both_attempts.push(recorded(1, 0, retried));
    assert_eq!(
        chat.recorded_calls(&waiting.id).await,
        both_attempts,
        "the retry records its own call, not the session's earlier ones"
    );

    let mut calls = vec![earlier];
    calls.extend(failed);
    calls.push(retried);
    chat.assert_totals_count_each_call_once(&calls, 2, 3).await;
    assert_eq!(chat.provider.requests().len(), calls.len());
}

/// Rotation-summary usage is charged once (an outbox row settled by id), not
/// on every turn of the successor topic.
#[cfg(feature = "test-support")]
#[tokio::test]
async fn topic_summary_usage_is_settled_once() {
    let chat = ledger_chat(
        MEASURED_CALLS[..3]
            .iter()
            .enumerate()
            .map(|(turn, call)| metered_text_step(&format!("reply {turn}"), *call))
            .collect(),
    )
    .await;
    let first = chat.turn("message 0").await;
    assert_eq!(first.status, db::AgentChatTurnState::Succeeded);
    let runtime_id: String = sqlx::query_scalar(
        "SELECT runtime_session_id FROM agent_session WHERE runtime_session_id IS NOT NULL AND backend_kind = 'native' ORDER BY created_at DESC LIMIT 1",
    )
    .fetch_one(chat.db.pool())
    .await
    .unwrap();
    // Exactly the row fork_topic writes for a completed rotation summary.
    sqlx::query("INSERT INTO agent_topic_summary_usage (id, runtime_session_id, provider, model, input_tokens, output_tokens, failed, purpose) VALUES ('topic-summary:audit', ?, 'openai', 'fake', 7777, 55, 0, 'topic_summary')")
        .bind(&runtime_id).execute(chat.db.pool()).await.unwrap();
    println!(
        "summary usage: runtime={runtime_id} sessions={:?}",
        sqlx::query_as::<_, (String, Option<String>, String)>(
            "SELECT id, runtime_session_id, status FROM agent_session"
        )
        .fetch_all(chat.db.pool())
        .await
        .unwrap()
    );
    for turn in 1..3 {
        let job = chat.turn(&format!("message {turn}")).await;
        assert_eq!(job.status, db::AgentChatTurnState::Succeeded, "turn {turn}");
    }
    let charged: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM usage_event WHERE input_tokens = 7777")
            .fetch_one(chat.db.pool())
            .await
            .unwrap();
    assert_eq!(charged, 1, "rotation summary usage charged {charged} times");
}
