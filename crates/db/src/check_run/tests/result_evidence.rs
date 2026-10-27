//! Result evidence repository tests.
use super::*;

#[tokio::test]
async fn a_long_bundle_shrinks_its_tails_to_fit_the_row_instead_of_refusing_to_settle() {
    let db = fixture().await;
    let mut req = request("many");
    let template = req.identity.inputs.spec.commands[0].clone();
    req.identity.inputs.spec.commands = (0..64)
        .map(|index| CheckCommandSpec {
            id: format!("ci:{index}"),
            ..template.clone()
        })
        .collect();
    req.identity.inputs.spec.configured_commands = 64;
    let run = cleaning(&db, req).await;
    let mut output = evidence(CheckResultOutcome::Pass, CheckCleanup::Success);
    let step = output.commands[0].clone();
    output.commands = (0..64)
        .map(|index| CheckCommandOutcome {
            failures: vec![],
            failures_truncated: false,
            index,
            output_tail: "o".repeat(CHECK_OUTPUT_TAIL_BYTES),
            stderr_tail: format!("{}end-{index}", "e".repeat(CHECK_OUTPUT_TAIL_BYTES)),
            ..step.clone()
        })
        .collect();
    let result = db
        .finish_check_run(&fence(&run), output, NOW)
        .await
        .unwrap();
    assert!(result.output_truncated && result.certified);
    assert_eq!(result.commands.len(), 64);
    assert!(serde_json::to_string(&result.commands).unwrap().len() <= CHECK_STEPS_BYTES);
    for (index, step) in result.commands.iter().enumerate() {
        assert_eq!(step.output_tail.len(), CHECK_OUTPUT_TAIL_BYTES / 4);
        assert!(step.stderr_tail.ends_with(&format!("end-{index}")));
    }
}

#[tokio::test]
async fn sql_check_enums_equal_rust_enums() {
    let db = fixture().await;
    async fn values(db: &SqliteDb, table: &str, column: &str) -> Vec<String> {
        let sql: String =
            sqlx::query_scalar("SELECT sql FROM sqlite_master WHERE type='table' AND name=?")
                .bind(table)
                .fetch_one(db.pool())
                .await
                .unwrap();
        let needle = format!("{column} IN (");
        sql.split(&needle)
            .nth(1)
            .unwrap()
            .split(')')
            .next()
            .unwrap()
            .split(',')
            .map(|s| s.trim().trim_matches('\'').into())
            .collect()
    }
    macro_rules! compare {
        ($table:literal,$column:literal,$enum:ident) => {
            assert_eq!(
                values(&db, $table, $column).await,
                $enum::ALL
                    .iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            );
            for state in $enum::ALL {
                assert_eq!(state.to_string().parse::<$enum>().unwrap(), *state);
            }
            assert!("future".parse::<$enum>().is_err());
        };
    }
    compare!("check_run", "state", CheckRunState);
    compare!("check_result", "outcome", CheckResultOutcome);
    compare!("check_result", "cleanup", CheckCleanup);
    compare!("check_consumer", "origin", CheckConsumerOrigin);
}

#[tokio::test]
async fn output_is_redacted_bounded_utf8_tail_and_flagged_and_json_caps_are_enforced() {
    let db = fixture().await;
    let run = cleaning(&db, request("one")).await;
    let mut output = evidence(CheckResultOutcome::Pass, CheckCleanup::Success);
    output.commands[0].output_tail = format!("{}long-secret", "é".repeat(5000));
    output.commands[0].stderr_tail = "long-secret".into();
    output.redaction_values = vec!["secret".into(), "long-secret".into()];
    let result = db
        .finish_check_run(&fence(&run), output, NOW)
        .await
        .unwrap();
    assert!(result.output_truncated);
    assert!(result.commands[0].output_tail.len() <= CHECK_OUTPUT_TAIL_BYTES);
    assert!(result.commands[0].output_tail.ends_with("[REDACTED]"));
    assert_eq!(result.commands[0].stderr_tail, "[REDACTED]");
    let serialized = serde_json::to_string(&result.commands).unwrap();
    assert!(!serialized.contains("long-secret"));
    let mut oversized = request("big");
    oversized.identity.inputs.spec.commands[0].shell_text = "x".repeat(CHECK_INPUT_BYTES);
    assert!(matches!(
        db.request_check_run(oversized).await,
        Err(DbError::Check(_))
    ));
    assert!(sqlx::query("UPDATE check_run SET input_json=? WHERE id=?")
        .bind(format!("\"{}\"", "x".repeat(CHECK_INPUT_BYTES)))
        .bind(&run.id)
        .execute(db.pool())
        .await
        .is_err());
    assert!(
        sqlx::query("UPDATE check_result SET steps_json=? WHERE id=?")
            .bind(format!("\"{}\"", "x".repeat(CHECK_STEPS_BYTES)))
            .bind(&result.id)
            .execute(db.pool())
            .await
            .is_err()
    );
    assert!(json(&"x".repeat(CHECK_STEPS_BYTES), CHECK_STEPS_BYTES).is_err());
}

#[tokio::test]
async fn pass_requires_complete_ordered_successful_commands_and_atomic_cas() {
    let db = fixture().await;
    let run = cleaning(&db, request("one")).await;
    for malformed in [0, 1, 2] {
        let mut output = evidence(CheckResultOutcome::Pass, CheckCleanup::Success);
        match malformed {
            0 => output.commands.clear(),
            1 => output.commands[0].command = "different".into(),
            _ => output.commands[0].exit_code = 1,
        }
        assert!(matches!(
            db.finish_check_run(&fence(&run), output, NOW).await,
            Err(DbError::Check(_))
        ));
        assert_eq!(
            db.check_run(&run.id).await.unwrap().unwrap().version,
            run.version
        );
    }
    db.finish_check_run(
        &fence(&run),
        evidence(CheckResultOutcome::Pass, CheckCleanup::Success),
        NOW,
    )
    .await
    .unwrap();
    assert!(matches!(
        db.finish_check_run(
            &fence(&run),
            evidence(CheckResultOutcome::Pass, CheckCleanup::Success),
            NOW
        )
        .await,
        Err(DbError::VersionConflict)
    ));
}

#[tokio::test]
async fn failure_signature_lists_are_capped_redacted_and_fit_aggregate_row_budget() {
    let db = fixture().await;
    let mut req = request("signatures");
    let template = req.identity.inputs.spec.commands[0].clone();
    req.identity.inputs.spec.commands = (0..32)
        .map(|index| CheckCommandSpec {
            id: format!("ci:{index}"),
            ..template.clone()
        })
        .collect();
    req.identity.inputs.spec.configured_commands = 32;
    let run = cleaning(&db, req).await;
    let mut output = evidence(CheckResultOutcome::Fail, CheckCleanup::Success);
    let template = output.commands[0].clone();
    output.commands = (0..32)
        .map(|index| CheckCommandOutcome {
            index,
            failures: (0..201)
                .map(|test| format!("private-value/{test:03}/{}", "猫".repeat(301)))
                .collect(),
            ..template.clone()
        })
        .collect();
    output.redaction_values = vec!["private-value".into()];
    let result = db
        .finish_check_run(&fence(&run), output, NOW)
        .await
        .unwrap();
    assert_eq!(result.outcome, CheckResultOutcome::Fail);
    assert_eq!(result.commands.len(), 32);
    let json = serde_json::to_string(&result.commands).unwrap();
    assert!(json.len() <= CHECK_STEPS_BYTES);
    assert!(!json.contains("private-value"));
    for step in result.commands {
        assert!(step.failures_truncated);
        assert!(!step.failures.is_empty() && step.failures.len() <= 200);
        assert!(step
            .failures
            .iter()
            .all(|signature| signature.chars().count() <= 300));
        assert!(step.failures[0].contains("[REDACTED]"));
    }
}
