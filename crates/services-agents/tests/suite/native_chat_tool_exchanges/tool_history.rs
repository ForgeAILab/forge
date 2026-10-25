//! Durable tool call pairings, LCM pressure, and oversized tool loop retries.

use super::*;

/// Asserts the shape the context planner requires, and that the second
/// exchange really did land under its own id rather than being dropped.
fn assert_one_call_and_result_per_id(pairings: &BTreeMap<String, (usize, usize)>) {
    assert_eq!(
        pairings.len(),
        2,
        "two tool exchanges must occupy two distinct ids on the durable timeline: {pairings:?}"
    );
    assert!(
        pairings.contains_key(REUSED_ID),
        "the first exchange keeps the id the provider sent: {pairings:?}"
    );
    for (id, (calls, results)) in pairings {
        assert_eq!(
            (*calls, *results),
            (1, 1),
            "tool call `{id}` must have exactly one call and one result fragment: {pairings:?}"
        );
    }
}

/// The live shape: the same id answers two different tools in one chat
/// session. Every turn after the second exchange used to fail
/// `invalid_pairing`; here the chat keeps answering and the timeline stays
/// well-formed.
#[tokio::test]
async fn a_reused_tool_call_id_does_not_poison_the_chat_timeline() {
    let fixture = chat_fixture().await;
    let backend = NativeAgentRuntimeBackend::new(fixture.service.protected_store())
        .with_provider_override(scripted_provider(vec![
            tool_call_step(REUSED_ID, "forge_scope_propose"),
            text_step("scope proposed; continuing the plan."),
            tool_call_step(REUSED_ID, "forge_task_command"),
            text_step("task command attempted; continuing the plan."),
            text_step("the chat is still answering after the collision."),
        ]));

    let mut timeline_id = None;
    for (turn, input) in [
        "turn 0: propose the scope",
        "turn 1: run the task command",
        "turn 2: summarize where we are",
    ]
    .into_iter()
    .enumerate()
    {
        let output = backend
            .run_turn(fixture.turn(input), Arc::new(NoopSink))
            .await
            .unwrap_or_else(|error| panic!("turn {turn} must complete: {error}"));
        assert!(
            !output.text.trim().is_empty(),
            "turn {turn} returns assistant text"
        );
        timeline_id = output
            .context_manifest
            .expect("native turn links a runtime context manifest")
            .lcm_timeline_id;
    }

    let timeline_id = timeline_id.expect("the chat turns link an LCM timeline");
    let pairings = durable_pairings(&fixture.db, &timeline_id).await;
    assert_one_call_and_result_per_id(&pairings);
}

/// The worker shape: the colliding exchange arrives in a later process, so
/// the earlier id is only knowable from the protected session store Forge
/// restores the runtime session from. A restore that lost it would let the
/// collision through and wedge the timeline exactly as before.
#[tokio::test]
async fn a_reused_tool_call_id_is_caught_after_the_session_is_restored() {
    let fixture = chat_fixture().await;

    let first_backend = NativeAgentRuntimeBackend::new(fixture.service.protected_store())
        .with_provider_override(scripted_provider(vec![
            tool_call_step(REUSED_ID, "forge_scope_propose"),
            text_step("scope proposed in the first process."),
        ]));
    first_backend
        .run_turn(
            fixture.turn("turn 0: propose the scope"),
            Arc::new(NoopSink),
        )
        .await
        .expect("the first process' turn completes");
    drop(first_backend);

    // A fresh backend: nothing in memory remembers the first exchange.
    let second_backend = NativeAgentRuntimeBackend::new(fixture.service.protected_store())
        .with_provider_override(scripted_provider(vec![
            tool_call_step(REUSED_ID, "forge_task_command"),
            text_step("task command attempted in the second process."),
            text_step("the restored chat is still answering."),
        ]));
    let colliding = second_backend
        .run_turn(
            fixture.turn("turn 1: run the task command"),
            Arc::new(NoopSink),
        )
        .await
        .expect("the restored session's colliding turn completes");
    let timeline_id = colliding
        .context_manifest
        .expect("native turn links a runtime context manifest")
        .lcm_timeline_id
        .expect("the restored chat links an LCM timeline");
    second_backend
        .run_turn(
            fixture.turn("turn 2: summarize where we are"),
            Arc::new(NoopSink),
        )
        .await
        .expect("the turn after the collision completes");

    let pairings = durable_pairings(&fixture.db, &timeline_id).await;
    assert_one_call_and_result_per_id(&pairings);
}

async fn lcm_node_counts(db: &SqliteDb, timeline_id: &str) -> (i64, i64) {
    let leaf: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_lcm_node WHERE timeline_id = ? AND kind = 'leaf'",
    )
    .bind(timeline_id)
    .fetch_one(db.pool())
    .await
    .expect("leaf node count");
    let condensed: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM agent_lcm_node WHERE timeline_id = ? AND kind = 'condensed'",
    )
    .bind(timeline_id)
    .fetch_one(db.pool())
    .await
    .expect("condensed node count");
    (leaf, condensed)
}

/// Hard compaction over a history made of tool exchanges — the shape of a
/// worker or Project Agent timeline, and the one the stock sizer could not
/// see.
///
/// `CharRatioSizer::entry_tokens` charges an entry for `joined_text()` plus
/// one token per tool part, but a tool call's arguments and a tool result's
/// body sit *inside* their content part where `joined_text()` cannot reach
/// them. Measured on this exact fixture (24 exchanges, 12,288-token input
/// budget, no composed tools): it estimated 8,582 tokens against an 11,243
/// conversation budget — 76%, under the 85% hard threshold, so LCM never
/// compacted — while the planner charged 13,244 for the same history and
/// refused the turn with `budget_exceeded: input budget exceeded by 956
/// tokens ... largest category is `history` at 8750 tokens`. The serialized
/// history is ~14.5k tokens, so the estimate ran ~41% under: far past the
/// 15% of headroom `forge-lcm-pressure-3` left for estimator error. Canonical
/// history is durable, so every retry replayed it and the session never
/// recovered.
///
/// `ForgeLcmSizer` charges the serialized entry instead, which puts this
/// history over the hard threshold and compacts it.
#[tokio::test]
async fn tool_exchange_history_reaches_hard_pressure_before_the_planner_refuses_the_turn() {
    use agent_runtime::core::content::{ToolCall, ToolResultBlock};
    use agent_runtime::core::ids::ToolCallId;
    use forge_agent_host::Role;

    let fixture = chat_fixture().await;
    let backend = NativeAgentRuntimeBackend::new(fixture.service.protected_store())
        .with_provider_override(text_only_provider(6));

    // Each round is one canonical exchange: the user asks, the assistant
    // calls a tool, the runtime answers it, the assistant reports.
    let mut seed_history = Vec::new();
    for index in 0..24 {
        let call_id = ToolCallId::new(format!("call-{index}"));
        seed_history.push(Message::user(format!(
            "user message {index}: {}",
            "considered planning detail. ".repeat(24)
        )));
        seed_history.push(Message::assistant(vec![ContentPart::ToolCall(ToolCall {
            id: call_id.clone(),
            name: "forge_task_read".to_owned(),
            arguments: serde_json::json!({ "round": index }),
        })]));
        seed_history.push(Message::tool_result(ToolResultBlock {
            call_id,
            name: "forge_task_read".to_owned(),
            content: vec![ContentPart::text(format!(
                "tool result {index}: {}",
                "retrieved workspace detail. ".repeat(24)
            ))],
            is_error: false,
        }));
        seed_history.push(Message::text(
            Role::Assistant,
            format!(
                "assistant reply {index}: {}",
                "prior assistant reasoning. ".repeat(24)
            ),
        ));
    }

    let mut compacted = None;
    for turn in 0..4 {
        let history = if turn == 0 {
            seed_history.clone()
        } else {
            Vec::new()
        };
        let output = backend
            .run_turn(
                fixture.turn_with_history(&format!("turn {turn}: continue the plan"), history),
                Arc::new(NoopSink),
            )
            .await
            .unwrap_or_else(|error| {
                panic!("turn {turn} over a tool-exchange history must complete: {error}")
            });
        let timeline_id = output
            .context_manifest
            .expect("native turn links a runtime context manifest")
            .lcm_timeline_id
            .expect("manifest links the chat LCM timeline");
        let (leaf, condensed) = lcm_node_counts(&fixture.db, &timeline_id).await;
        if leaf + condensed > 0 {
            compacted = Some((turn, timeline_id));
            break;
        }
    }

    let (compaction_turn, timeline_id) = compacted.expect(
        "a tool-exchange history over the input budget must reach hard pressure and condense \
         rather than be refused by the planner",
    );

    // The turn after compaction plans against a projection that mixes summary
    // nodes with the raw suffix. It is the one that fails `invalid_pairing` if
    // a leaf span split an exchange.
    let output = backend
        .run_turn(
            fixture.turn(&format!(
                "turn {}: continue after compaction",
                compaction_turn + 1
            )),
            Arc::new(NoopSink),
        )
        .await
        .expect("the turn after compacting a tool-exchange history completes");
    assert!(
        !output.text.trim().is_empty(),
        "the turn after compaction returns assistant text"
    );

    // No half-pair may survive on the durable timeline either: LCM derives the
    // planner's pairings straight off these entries.
    let pairings = durable_pairings(&fixture.db, &timeline_id).await;
    assert_eq!(
        pairings.len(),
        24,
        "every seeded exchange is admitted under its own id: {pairings:?}"
    );
    for (id, (calls, results)) in &pairings {
        assert_eq!(
            (*calls, *results),
            (1, 1),
            "tool call `{id}` must keep exactly one call and one result entry: {pairings:?}"
        );
    }
}

/// A Forge-side LCM policy change must not wedge existing sessions.
///
/// The runtime folds Forge's LCM tunables (input budget, working set, sizer,
/// pressure and summary policy) into one LCM component revision. Before U6,
/// `decode_state` refused state written under a different revision; that
/// conflict failed the turn and was replayed on every attempt, so shipping any
/// change to those tunables — the sizer fix above included — would have failed
/// every turn of every live chat with no recovery path. The runtime (U6) now
/// tolerates superseded tuning revisions and rebuilds the metadata from the
/// durable timeline, so the host keeps the state and no longer drops it.
///
/// The superseded state here is real: an earlier backend ("the old binary")
/// runs turns under a different Main working set and input budget, so the
/// runtime itself publishes both journal domains under the old revision. A
/// fresh backend with the current tunables then resumes the chat.
#[tokio::test]
async fn a_superseded_lcm_policy_revision_rebuilds_instead_of_failing_every_turn() {
    use agent_runtime::core::ids::SessionId;
    use agent_runtime::harness::LCM_COMPONENT_ID;
    use agent_runtime::registry::RegistryRevision;

    /// The LCM component revision each native domain last published.
    async fn published_lcm_revisions(
        store: &forge_agent_host::SqliteProtectedRuntimeStore,
        session: &SessionId,
    ) -> (RegistryRevision, RegistryRevision) {
        let flags = store
            .persistence_flags(session)
            .await
            .expect("native publication flags");
        assert!(
            flags.ordinary_published && flags.protected_published,
            "both native domains are published: {flags:?}"
        );
        let ordinary = store
            .persisted_extension(session, flags, LCM_COMPONENT_ID)
            .await
            .expect("ordinary LCM state reads")
            .expect("the ordinary domain carries LCM state")
            .revision;
        // Without ordinary publication the reader falls back to protected.
        let protected_only = db::SessionPersistenceFlags {
            ordinary_published: false,
            ..flags
        };
        let protected = store
            .persisted_extension(session, protected_only, LCM_COMPONENT_ID)
            .await
            .expect("protected LCM state reads")
            .expect("the protected domain carries LCM state")
            .revision;
        (ordinary, protected)
    }

    let fixture = chat_fixture().await;
    let store = fixture.service.protected_store();
    let runtime_session = SessionId::new(&fixture.runtime_session_id);
    let provider = text_only_provider(4);

    // The old binary: a smaller Main working set and input budget. Both are
    // inputs to the LCM component revision.
    let old_binary =
        NativeAgentRuntimeBackend::new(Arc::clone(&store)).with_provider_override(provider.clone());
    old_binary.set_working_sets(8_000, 10_000, 96_000, 128_000);
    let old_turn = |input: &str| {
        let mut request = fixture.turn(input);
        request.provider.max_input_tokens = 10_240;
        request
    };
    let first = old_binary
        .run_turn(old_turn("turn 0: start the chat"), Arc::new(NoopSink))
        .await
        .expect("the first turn completes and writes LCM component state");
    old_binary
        .run_turn(
            old_turn("turn 1: continue under the old tunables"),
            Arc::new(NoopSink),
        )
        .await
        .expect("a second turn under the old tunables completes");
    let (old_ordinary, old_protected) = published_lcm_revisions(&store, &runtime_session).await;
    assert_eq!(
        old_ordinary, old_protected,
        "the old binary published one LCM revision to both domains"
    );
    drop(old_binary);

    // The current binary: default working sets and the fixture's budget.
    let backend =
        NativeAgentRuntimeBackend::new(Arc::clone(&store)).with_provider_override(provider.clone());
    let output = backend
        .run_turn(
            fixture.turn("turn 2: continue after the policy change"),
            Arc::new(NoopSink),
        )
        .await
        .expect("a turn after a Forge LCM policy change rebuilds instead of failing");
    assert!(
        !output.text.trim().is_empty(),
        "the rebuilt session answers normally"
    );
    let (rebuilt_ordinary, rebuilt_protected) =
        published_lcm_revisions(&store, &runtime_session).await;
    assert_ne!(
        rebuilt_ordinary, old_ordinary,
        "the resumed state was published under a superseded LCM revision"
    );
    assert_eq!(
        rebuilt_ordinary, rebuilt_protected,
        "the rebuild republished both domains under the current revision"
    );

    // Not just one lucky turn: the next turn resumes the rebuilt state.
    backend
        .run_turn(
            fixture.turn("turn 3: the rebuilt chat keeps going"),
            Arc::new(NoopSink),
        )
        .await
        .expect("the turn after the rebuild completes");
    assert_eq!(
        published_lcm_revisions(&store, &runtime_session).await,
        (rebuilt_ordinary.clone(), rebuilt_ordinary),
        "unchanged tunables keep the rebuilt revision"
    );

    let timeline_id = output
        .context_manifest
        .expect("native turn links a runtime context manifest")
        .lcm_timeline_id
        .expect("the rebuilt session links the same chat LCM timeline");
    assert_eq!(
        Some(timeline_id.as_str()),
        first
            .context_manifest
            .as_ref()
            .and_then(|manifest| manifest.lcm_timeline_id.as_deref()),
        "the rebuild keeps the chat's timeline"
    );
    let entries: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM agent_lcm_entry WHERE timeline_id = ?")
            .bind(&timeline_id)
            .fetch_one(fixture.db.pool())
            .await
            .expect("entry count");
    assert!(
        entries > 0,
        "the durable timeline the state was rebuilt from is intact"
    );
}

/// The live wedge from a Project Agent chat: one turn ran ~50 tool rounds
/// (guessing a board revision) and died without replying. That single turn
/// held no user boundary and far outgrew the leaf target, so the leaf
/// planner could never commit a span and every retry failed with "LCM
/// context cannot fit after bounded hard compaction". Each retry also
/// re-sent the same message, stacking identical user turns on the timeline.
///
/// The retry must now compact the oversized turn whole and complete, and the
/// durable timeline must carry a continuation instead of a second copy.
#[tokio::test]
async fn a_retry_after_an_oversized_unanswered_tool_loop_compacts_and_does_not_repeat_the_message()
{
    use agent_runtime::core::content::{ToolCall, ToolResultBlock};
    use agent_runtime::core::ids::ToolCallId;

    const ASK: &str = "this task is too big; break it down with good dependencies";

    let fixture = chat_fixture().await;
    let backend = NativeAgentRuntimeBackend::new(fixture.service.protected_store())
        .with_provider_override(text_only_provider(2));

    let mut seed_history = vec![Message::user(ASK)];
    for index in 0..40 {
        let call_id = ToolCallId::new(format!("loop-call-{index}"));
        seed_history.push(Message::assistant(vec![ContentPart::ToolCall(ToolCall {
            id: call_id.clone(),
            name: "forge_scope_propose".to_owned(),
            arguments: serde_json::json!({ "expected_board_revision": index }),
        })]));
        seed_history.push(Message::tool_result(ToolResultBlock {
            call_id,
            name: "forge_scope_propose".to_owned(),
            content: vec![ContentPart::text(format!(
                "version_conflict {index}: {}",
                "the authorized resource changed; refresh and retry. ".repeat(20)
            ))],
            is_error: true,
        }));
    }

    let output = backend
        .run_turn(
            fixture.turn_with_history(ASK, seed_history),
            Arc::new(NoopSink),
        )
        .await
        .expect("a retry behind an oversized tool-loop turn must compact and complete");
    assert!(!output.text.trim().is_empty());
    let timeline_id = output
        .context_manifest
        .expect("native turn links a runtime context manifest")
        .lcm_timeline_id
        .expect("manifest links the chat LCM timeline");
    let (leaf, _) = lcm_node_counts(&fixture.db, &timeline_id).await;
    assert!(leaf > 0, "the oversized turn must be compacted into a leaf");

    let user_texts: Vec<String> = sqlx::query_as::<_, (String,)>(
        "SELECT content_json FROM agent_lcm_entry WHERE timeline_id = ? ORDER BY sequence",
    )
    .bind(&timeline_id)
    .fetch_all(fixture.db.pool())
    .await
    .expect("timeline entries")
    .into_iter()
    .map(|(json,)| serde_json::from_str::<Message>(&json).expect("canonical message"))
    .filter(|message| message.role == forge_agent_host::Role::User)
    .map(|message| message.joined_text())
    .collect();
    assert_eq!(
        user_texts
            .iter()
            .filter(|text| text.as_str() == ASK)
            .count(),
        1,
        "the retried message must not be appended a second time: {user_texts:?}"
    );
    assert_eq!(user_texts.len(), 2, "user entries: {user_texts:?}");
}
