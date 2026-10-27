//! Cancellation while a daemon merge push is in flight.

use super::*;

#[cfg(unix)]
#[tokio::test]
async fn cancel_during_remote_merge_push_finishes_integration_and_reports_done() {
    use db::TaskStepRepo;

    let fixture = Fixture::new("forge-merge-push-cancel").await;
    let candidate = fixture.candidate().await;
    fixture.approve_candidate().await;
    let root = fixture._daemon_root.path();
    let remote = root.join("push-target.git");
    let started = root.join("push-started");
    let release = root.join("push-release");
    let output = tokio::process::Command::new("git")
        .args(["init", "--bare"])
        .arg(&remote)
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    let hook = remote.join("hooks/pre-receive");
    process_supervisor::executable::write_executable(
        &hook,
        format!(
            "#!/bin/sh\ntouch {}\nwhile [ ! -f {} ]; do sleep 0.02; done\n",
            quote(&started.to_string_lossy()),
            quote(&release.to_string_lossy())
        ),
    )
    .unwrap();
    let post_merge = fixture.checkout.join(".git/hooks/post-merge");
    process_supervisor::executable::write_executable(
        &post_merge,
        format!(
            "#!/bin/sh\ngit push {} HEAD:refs/heads/main\n",
            quote(&remote.to_string_lossy())
        ),
    )
    .unwrap();
    let db = fixture.harness.state.db.clone();
    let service = fixture.harness.state.task_service.clone();
    let task_id = fixture.resolved.placement.task_id.clone();
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    service
        .transition(
            &task_id,
            "merging".into(),
            services::task_service::TransitionOptions {
                bridge: Default::default(),
                version: task.version,
                triggered_by: Actor::system(SystemComponent::Workflow),
                reason: Some("approved remote merge".into()),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await
        .unwrap();
    let drain = {
        let service = service.clone();
        let task_id = task_id.clone();
        tokio::spawn(async move { service.drain(&task_id).await })
    };
    tokio::time::timeout(Duration::from_secs(15), async {
        while !started.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("merge reaches the push process");
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    let mut cancel = {
        let service = service.clone();
        let task_id = task_id.clone();
        tokio::spawn(async move {
            service
                .perform_task_action(&task_id, TaskAction::Cancel { reason: None }, task.version)
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        while !db.task_steps(&task_id).await.unwrap().iter().any(|s| {
            s.kind == "command"
                && s.status == "pending"
                && serde_json::from_str::<Value>(&s.payload_json).unwrap()["preempt"] == true
        }) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut cancel)
            .await
            .is_err(),
        "Cancel waits for the protected push"
    );
    std::fs::write(&release, "release").unwrap();
    let cancelled = tokio::time::timeout(Duration::from_secs(15), cancel)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(cancelled.task.status, "done");
    // The queued Cancel ran after the merge landed and records that it was moot.
    let moot: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM task_comment WHERE task_id=? AND content LIKE 'Cancel had no effect%'",
    )
    .bind(&task_id)
    .fetch_one(db.pool())
    .await
    .unwrap();
    assert_eq!(moot, 1);
    drain.await.unwrap().unwrap();
    let output = tokio::process::Command::new("git")
        .arg("--git-dir")
        .arg(&remote)
        .args(["rev-parse", "refs/heads/main"])
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), candidate);
    assert!(db
        .pending_remote_cancels(None, None)
        .await
        .unwrap()
        .is_empty());
}

#[cfg(unix)]
#[tokio::test]
async fn cancel_during_slow_remote_merge_push_reports_done() {
    use db::TaskStepRepo;

    let fixture = Fixture::new("forge-merge-push-cancel-slow").await;
    let candidate = fixture.candidate().await;
    fixture.approve_candidate().await;
    let root = fixture._daemon_root.path();
    let remote = root.join("push-target.git");
    let started = root.join("push-started");
    let release = root.join("push-release");
    let output = tokio::process::Command::new("git")
        .args(["init", "--bare"])
        .arg(&remote)
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    let hook = remote.join("hooks/pre-receive");
    process_supervisor::executable::write_executable(
        &hook,
        format!(
            "#!/bin/sh\ntouch {}\nwhile [ ! -f {} ]; do sleep 0.02; done\n",
            quote(&started.to_string_lossy()),
            quote(&release.to_string_lossy())
        ),
    )
    .unwrap();
    let post_merge = fixture.checkout.join(".git/hooks/post-merge");
    process_supervisor::executable::write_executable(
        &post_merge,
        format!(
            "#!/bin/sh\ngit push {} HEAD:refs/heads/main\n",
            quote(&remote.to_string_lossy())
        ),
    )
    .unwrap();
    let db = fixture.harness.state.db.clone();
    let service = fixture.harness.state.task_service.clone();
    let task_id = fixture.resolved.placement.task_id.clone();
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    service
        .transition(
            &task_id,
            "merging".into(),
            services::task_service::TransitionOptions {
                bridge: Default::default(),
                version: task.version,
                triggered_by: Actor::system(SystemComponent::Workflow),
                reason: Some("approved remote merge".into()),
                rejection: false,
                defer_dispatch_seconds: None,
            },
        )
        .await
        .unwrap();
    let drain = {
        let service = service.clone();
        let task_id = task_id.clone();
        tokio::spawn(async move { service.drain(&task_id).await })
    };
    tokio::time::timeout(Duration::from_secs(15), async {
        while !started.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("merge reaches the push process");
    let task = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    let mut cancel = {
        let service = service.clone();
        let task_id = task_id.clone();
        tokio::spawn(async move {
            service
                .perform_task_action(&task_id, TaskAction::Cancel { reason: None }, task.version)
                .await
        })
    };
    tokio::time::timeout(Duration::from_secs(5), async {
        while !db.task_steps(&task_id).await.unwrap().iter().any(|s| {
            s.kind == "command"
                && s.status == "pending"
                && serde_json::from_str::<Value>(&s.payload_json).unwrap()["preempt"] == true
        }) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(50), &mut cancel)
            .await
            .is_err(),
        "Cancel waits for the protected push"
    );
    // The push is still running 10 s after the Cancel was asked for. The
    // Cancel waits at most 15 s from its own enqueue, so the hold is measured
    // on the command's own clock (`created_at`), not from when this test saw
    // it: a slow machine then spends none of the remaining margin on the
    // detection above.
    let enqueued = db
        .task_steps(&task_id)
        .await
        .unwrap()
        .into_iter()
        .find(|s| {
            s.kind == "command"
                && serde_json::from_str::<Value>(&s.payload_json).unwrap()["preempt"] == true
        })
        .map(|s| {
            chrono::DateTime::parse_from_rfc3339(&s.created_at)
                .unwrap()
                .with_timezone(&chrono::Utc)
        })
        .expect("the preempting Cancel is enqueued");
    let hold_until = enqueued + chrono::Duration::milliseconds(10_200);
    while chrono::Utc::now() < hold_until {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(!cancel.is_finished(), "Cancel still waits for the push");
    assert!(
        chrono::Utc::now() < enqueued + chrono::Duration::seconds(14),
        "this machine was too slow to release the push inside the Cancel's own 15 s wait"
    );
    std::fs::write(&release, "release").unwrap();
    let cancelled = tokio::time::timeout(Duration::from_secs(15), cancel)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    let pushed = {
        let mut sha = String::new();
        for _ in 0..200 {
            let output = tokio::process::Command::new("git")
                .arg("--git-dir")
                .arg(&remote)
                .args(["rev-parse", "refs/heads/main"])
                .output()
                .await
                .unwrap();
            sha = String::from_utf8(output.stdout).unwrap().trim().to_owned();
            if sha == candidate {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        sha
    };
    let now = TaskRepo::get_by_id(&*db, &task_id, false)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        (cancelled.task.status.as_str(), now.status.as_str()),
        ("done", "done"),
        "integration landed on the target (pushed={}, candidate={candidate}) but Cancel reported {} and the Task is now {}",
        pushed, cancelled.task.status, now.status
    );
    assert_eq!(cancelled.task.status, "done");
    drain.await.unwrap().unwrap();
    let output = tokio::process::Command::new("git")
        .arg("--git-dir")
        .arg(&remote)
        .args(["rev-parse", "refs/heads/main"])
        .output()
        .await
        .unwrap();
    assert!(output.status.success());
    assert_eq!(String::from_utf8(output.stdout).unwrap().trim(), candidate);
    assert!(db
        .pending_remote_cancels(None, None)
        .await
        .unwrap()
        .is_empty());
}
