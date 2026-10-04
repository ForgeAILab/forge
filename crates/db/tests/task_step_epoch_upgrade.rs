//! Upgrade through the real migration runner with Task steps in flight: they
//! fence on epoch 0, keep their status, and are re-laned (merge/CI only).
use db::{create_sqlite_pool, new_uuid_v4, now_rfc3339, run_migrations_from, STEP_FENCE};
use std::path::PathBuf;

fn scratch(name: &str) -> PathBuf {
    let dir = std::env::temp_dir()
        .join("forge-task-step-epoch-upgrade")
        .join(format!("{name}-{}", new_uuid_v4()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test]
async fn in_flight_steps_fence_on_epoch_zero_after_upgrade() {
    let full = PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/migrations"));
    let base = scratch("base-migrations");
    for entry in std::fs::read_dir(&full).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name().into_string().unwrap();
        if name.as_str() < "V202610031934" {
            std::fs::copy(entry.path(), base.join(&name)).unwrap();
        }
    }
    let db_dir = scratch("upgrade-db");
    let pool = create_sqlite_pool(&format!(
        "sqlite://{}",
        db_dir.join("forge.sqlite").display()
    ))
    .await
    .unwrap();
    run_migrations_from(&pool, &base).await.unwrap();

    let now = now_rfc3339();
    sqlx::query("INSERT INTO project(id,name,created_at,updated_at) VALUES ('p','p',?,?)")
        .bind(&now)
        .bind(&now)
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES ('t','p','t','review',?,?)")
        .bind(&now).bind(&now).execute(&pool).await.unwrap();
    let workflow = serde_json::json!({"states":[
        {"name":"merging","hooks":{"on_enter":[{"action":"run_merge"}]}},
        {"name":"in_progress","hooks":{"before_enter":[{"action":"run_before_work_hooks"}],"on_enter":[{"action":"dispatch_role_agent"}]}}
    ]});
    for (seq, to) in [(1, "merging"), (2, "in_progress")] {
        let payload = serde_json::json!({"to":to,"reason":"automatic","rejection":false,"skip_before_exit":false,"workflow":workflow});
        sqlx::query("INSERT INTO task_step(id,task_id,seq,kind,payload_json,causation_key,chain_id,chain_position,expected_status,expected_version,status,available_at,created_at,updated_at) VALUES (?,'t',?,'cascade',?,?,'chain',?,'review',1,'pending',?,?,?)")
            .bind(to).bind(seq).bind(payload.to_string()).bind(to).bind(seq).bind(&now).bind(&now).bind(&now)
            .execute(&pool).await.unwrap();
    }

    run_migrations_from(&pool, &full).await.unwrap();

    let rows: Vec<(String, i64, String, Option<String>, String)> = sqlx::query_as(
        "SELECT id,expected_epoch,lane,workflow_ref_id,status FROM task_step ORDER BY seq",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(rows[0].0, "merging");
    assert_eq!((rows[0].1, rows[0].2.as_str()), (0, "long"));
    assert_eq!((rows[1].1, rows[1].2.as_str()), (0, "fast"));
    assert!(rows.iter().all(|row| row.3.is_some() && row.4 == "pending"));
    let fence = |epoch: i64| {
        let pool = pool.clone();
        async move {
            sqlx::query_scalar::<_, bool>(STEP_FENCE)
                .bind("t")
                .bind("review")
                .bind(epoch)
                .fetch_one(&pool)
                .await
                .unwrap()
        }
    };
    assert!(fence(0).await, "an untouched in-flight step stays valid");
    sqlx::query("UPDATE task SET status='in_progress' WHERE id='t'")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("UPDATE task SET status='review' WHERE id='t'")
        .execute(&pool)
        .await
        .unwrap();
    assert!(!fence(0).await, "leaving and returning supersedes it");
    assert!(fence(2).await);
}
