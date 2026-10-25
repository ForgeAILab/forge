//! Topic read references, compaction, and marked state-card tests.

use super::*;

#[tokio::test]
async fn unchanged_reads_are_scoped_to_one_topic_and_changed_bodies_are_full() {
    use crate::typed_tools::ToolResultFilter;
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let database = Arc::new(db::SqliteDb::new(pool));
    let filter = |runtime_session_id: &str| TopicReadFilter {
        db: database.clone(),
        runtime_session_id: runtime_session_id.into(),
        turn: Mutex::new(TopicReadTurn::default()),
    };
    let topic = filter("topic-one");
    let args = serde_json::json!({"operation": "project.current_state"});
    let outcome = |version| ToolOutcome {
        value: serde_json::json!({"version": version, "body": "current authoritative state"}),
        content: Default::default(),
        is_error: false,
    };
    let full = |value: &serde_json::Value| value["read_ref"].as_str().unwrap().to_owned();
    topic.begin_turn(0);
    let first = topic
        .filter(&ToolCallId::new("call-1"), &args, outcome(1))
        .await
        .unwrap()
        .value;
    assert_eq!(first["version"], 1);
    let reference = full(&first);
    // Within the running turn the staged result is visible.
    assert_eq!(
        topic
            .filter(&ToolCallId::new("call-1"), &args, outcome(1))
            .await
            .unwrap()
            .value,
        serde_json::json!({"unchanged_since_call": reference})
    );
    // The completed turn's history holds that result at index 2.
    let history = vec![
        Message::user("read the state"),
        Message::assistant(vec![ContentPart::text("reading")]),
        Message::tool_result(agent_runtime::core::content::ToolResultBlock {
            call_id: ToolCallId::new("call-1"),
            name: "forge_scope_read".to_owned(),
            content: vec![ContentPart::text(first.to_string())],
            is_error: false,
        }),
    ];
    topic.commit(&history).await.unwrap();
    // A provider call id is reused by the next turn; the reference is not.
    topic.begin_turn(3);
    assert_eq!(
        topic
            .filter(&ToolCallId::new("call-1"), &args, outcome(1))
            .await
            .unwrap()
            .value,
        serde_json::json!({"unchanged_since_call": reference})
    );
    let changed = topic
        .filter(&ToolCallId::new("call-3"), &args, outcome(2))
        .await
        .unwrap()
        .value;
    assert_eq!(changed["version"], 2);
    assert_ne!(full(&changed), reference);
    // Another topic (runtime session) never sees this one's references.
    let next = filter("topic-two");
    next.begin_turn(0);
    assert_eq!(
        next.filter(&ToolCallId::new("call-4"), &args, outcome(1))
            .await
            .unwrap()
            .value["version"],
        1
    );

    // LCM compaction past the referenced call: the body is sent again.
    let compacted = filter("topic-three");
    compacted.begin_turn(0);
    let body = compacted
        .filter(&ToolCallId::new("call-5"), &args, outcome(1))
        .await
        .unwrap()
        .value;
    compacted.commit(&history_with(&body)).await.unwrap();
    sqlx::query("PRAGMA foreign_keys = OFF")
        .execute(database.pool())
        .await
        .unwrap();
    sqlx::query(
        "INSERT INTO agent_runtime_lcm_binding (runtime_session_id, timeline_id)
             VALUES ('topic-three', 'timeline-three')",
    )
    .execute(database.pool())
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO agent_lcm_node (timeline_id, node_id, kind, range_start, range_end,
                 edges_json, source_fingerprint, summary_revision, summary, policy_revision,
                 algorithm_revision, sizer_revision, provenance_json, token_count,
                 source_token_count, classification_json, revision, superseded_by,
                 operation_id, operation_fingerprint, created_at)
             VALUES ('timeline-three', 'leaf-0', 'leaf', 0, 2, '[]', 'f', 'r', 's', 'p', 'a',
                 'z', '{}', 1, 9, '{}', 1, NULL, 'op', 'opf', '2026-10-05T00:00:00Z')",
    )
    .execute(database.pool())
    .await
    .unwrap();
    compacted.begin_turn(3);
    assert_eq!(
        compacted
            .filter(&ToolCallId::new("call-6"), &args, outcome(1))
            .await
            .unwrap()
            .value["version"],
        1,
        "a reference compacted into a summary is never returned"
    );
}

#[test]
fn only_the_marked_state_card_is_sent_as_user_input() {
    let marker = "[forge-state-card:test]\n";
    let card = "Project state: 3 Tasks open; review before acting.";
    // The user repeats the card verbatim and sends an edited copy.
    let mut messages = vec![
        Message::system("system prompt"),
        Message::user(card),
        Message::user(format!("{card} (edited)")),
        Message::system(format!("{marker}{card}")),
    ];
    unmark_state_card(&mut messages, marker).unwrap();
    assert_eq!(
        messages,
        vec![
            Message::system("system prompt"),
            Message::user(card),
            Message::user(format!("{card} (edited)")),
            Message::user(card),
        ]
    );
    // Card text without the marker, on any role, is never the card.
    let mut unmarked = vec![Message::user(card), Message::system(card)];
    assert!(unmark_state_card(&mut unmarked, marker).is_err());
    assert_eq!(unmarked, vec![Message::user(card), Message::system(card)]);
}

fn history_with(result: &serde_json::Value) -> Vec<Message> {
    vec![
        Message::user("read the state"),
        Message::assistant(vec![ContentPart::text("reading")]),
        Message::tool_result(agent_runtime::core::content::ToolResultBlock {
            call_id: ToolCallId::new("call-5"),
            name: "forge_scope_read".to_owned(),
            content: vec![ContentPart::text(result.to_string())],
            is_error: false,
        }),
    ]
}
