//! Tests for recorded outcomes in scheduler equivalence.

use super::*;
/// The spawn count waits for the started run to reach the executor. With
/// the first poll of the execution future delayed past the three ticks
/// (load did this twice on 2026-10-13: `deferred_future_todo` and
/// `publication_claim_orphan` read `spawned=0` with every persisted line
/// equal), the old sampling misses the run and the new one sees it.
#[tokio::test]
async fn shape_spawn_count_waits_for_a_delayed_executor_poll() {
    let delay = Duration::from_millis(600);
    let pass = |lines: &[String]| {
        lines
            .iter()
            .find(|line| line.contains("(pass)"))
            .cloned()
            .unwrap_or_default()
    };
    let old = run_shape_observed(
        "steady",
        "deferred_future_todo",
        true,
        delay,
        Observe::Immediate,
    )
    .await;
    assert!(pass(&old).contains("spawned=0"), "{}", pass(&old));
    let new = run_shape_observed(
        "steady",
        "deferred_future_todo",
        true,
        delay,
        Observe::UntilStarted,
    )
    .await;
    assert!(pass(&new).contains("spawned=1"), "{}", pass(&new));
    let expected = FIXTURE
        .lines()
        .find(|line| line.starts_with("steady deferred_future_todo (pass)"))
        .unwrap();
    assert_eq!(pass(&new), expected);
}

/// The cases run every shape once, and together they expect every recorded
/// line of the two sections: none is left to no case.
#[test]
fn scheduler_equivalence_shape_cases_cover_the_shapes_and_the_recording() {
    let mut owned: Vec<&str> = (0..SHAPE_CASES).flat_map(shapes_of_case).collect();
    assert_eq!(owned.len(), SHAPES.len());
    for case in 0..SHAPE_CASES {
        assert!(!shapes_of_case(case).is_empty(), "case {case} is empty");
    }
    owned.sort_unstable();
    let mut all = SHAPES.to_vec();
    all.sort_unstable();
    assert_eq!(owned, all);
    all.dedup();
    assert_eq!(all.len(), SHAPES.len(), "a shape is listed twice");
    for line in FIXTURE
        .lines()
        .filter(|l| !l.is_empty() && !l.starts_with('#') && !l.starts_with('@'))
    {
        for section in ["steady", "startup"] {
            assert!(
                !line.starts_with(&format!("{section}/")),
                "{line} is expected by no shape case"
            );
            if line.starts_with(&format!("{section} ")) {
                assert!(
                    SHAPES.contains(&shape_of(line)),
                    "{line} is expected by no shape case"
                );
            }
        }
    }
}

#[test]
fn scheduler_equivalence_failed_review() {
    on_runtime(|| async {
        let mut lines = Vec::new();
        for (label, minutes) in [("failed_review_old", 5), ("failed_review_in_grace", 0)] {
            let f = failed_review_fixture(chrono::Duration::minutes(minutes), 3).await;
            let pid = f.task.project_id.clone();
            let agents = HashMap::new();
            let before = snapshot(&f.db, &pid, &agents).await;
            for _ in 0..3 {
                tick(&f.dispatcher, 20).await;
            }
            let after = snapshot(&f.db, &pid, &agents).await;
            for ((title, a), (_, b)) in after.iter().zip(before.iter()) {
                lines.push(format!(
                    "review {label} {title}: from={} dv={} {}",
                    b.status,
                    a.version - b.version,
                    a.line
                ));
            }
        }
        settle("review", lines);
    });
}

/// 30 Tasks, two Agents, a machine run cap of four and a Project limit of six:
/// admission order, target Agents and slot counts per round.
#[test]
fn scheduler_equivalence_under_load() {
    on_runtime(|| async {
        let mut lines = Vec::new();
        for (mode, prime) in [("load/steady", true), ("load/startup", false)] {
            let mut c = ctx(false).await;
            c.db.server_run_cap.set(
                Some(4),
                config::resolved_run_cap(Some(4)),
                &config::embedded_machine_id(),
            );
            set_project_active_task_limit(&c.db, &c.pid, 6).await;
            if prime {
                let _ = c.dispatcher.check_once_and_drain().await;
            }
            let mut titles: HashMap<String, String> = HashMap::new();
            for i in 0..18 {
                let t = seed_task(
                    &c.db,
                    &c.pid,
                    &format!("S{i:02}"),
                    "todo",
                    [0, 5, 10, 5, 0, 10][i % 6],
                )
                .await;
                assign_role(
                    &c.db,
                    &t.id,
                    "coder",
                    if i % 2 == 0 { &c.a1 } else { &c.a2 },
                )
                .await;
                titles.insert(t.id.clone(), format!("S{i:02}"));
                tokio::time::sleep(Duration::from_millis(3)).await;
            }
            for r in 0..3 {
                let root = seed_task(&c.db, &c.pid, &format!("R{r}"), "todo", [10, 0, 5][r]).await;
                assign_role(&c.db, &root.id, "coder", &c.a1).await;
                titles.insert(root.id.clone(), format!("R{r}"));
                for k in 0..3 {
                    let child = seed_subtask(&c.db, &root, &format!("R{r}C{k}"), "todo", k).await;
                    assign_role(
                        &c.db,
                        &child.id,
                        "coder",
                        if k % 2 == 0 { &c.a2 } else { &c.a1 },
                    )
                    .await;
                    titles.insert(child.id.clone(), format!("R{r}C{k}"));
                }
            }
            let agents = agent_labels(&c).await;
            for round in 0..5 {
                for _ in 0..2 {
                    tick(&c.dispatcher, 60).await;
                    tokio::time::sleep(Duration::from_millis(60)).await;
                }
                let mut spawned = Vec::new();
                while let Ok(x) = c.rx.try_recv() {
                    spawned.push(
                        titles
                            .get(&x.task_id)
                            .cloned()
                            .unwrap_or_else(|| "?".into()),
                    );
                }
                spawned.sort();
                let created: Vec<(String, String, String)> = sqlx::query_as(
                "SELECT t.title, COALESCE(e.agent_id,''), e.status FROM execution e JOIN task t ON t.id=e.task_id ORDER BY e.created_at,e.rowid",
            )
            .fetch_all(c.db.pool())
            .await
            .unwrap();
                let mut running: Vec<String> = created
                    .iter()
                    .filter(|x| x.2 == "running")
                    .map(|x| format!("{}@{}", x.0, agents.get(&x.1).cloned().unwrap_or_default()))
                    .collect();
                running.sort();
                let admitted: Vec<String> = sqlx::query_scalar(
                "SELECT t.title FROM transition_log l JOIN task t ON t.id=l.task_id WHERE l.from_state='todo' AND l.to_state!='todo' ORDER BY l.created_at,l.rowid",
            )
            .fetch_all(c.db.pool())
            .await
            .unwrap();
                lines.push(format!(
                    "{mode} round{round} (admission order): {admitted:?}"
                ));
                lines.push(format!("{mode} round{round} (spawned): {spawned:?}"));
                lines.push(format!(
                    "{mode} round{round} (running): {running:?} slots=[{}]",
                    slots_line(&c.db, &c.pid).await
                ));
                for (title, s) in snapshot(&c.db, &c.pid, &agents).await {
                    lines.push(format!("{mode} round{round} {title}: {}", s.line));
                }
                // Two finish per round, chosen by title: creation order between
                // the two Agents is not stable within a millisecond.
                sqlx::query("UPDATE execution SET status='completed', updated_at=? WHERE id IN (SELECT e.id FROM execution e JOIN task t ON t.id=e.task_id WHERE e.status='running' ORDER BY t.title LIMIT 2)")
                .bind(now_rfc3339()).execute(c.db.pool()).await.unwrap();
            }
        }
        settle("load", lines);
    });
}

/// A wake against a stored deferral and a stored refusal: what a Project-wide
/// wake and a single-Task wake leave in the legacy metadata, and whether the
/// Task is then dispatched.
#[test]
fn scheduler_equivalence_wake_clears_what_the_base_cleared() {
    on_runtime(|| async {
        let mut lines = Vec::new();
        for stored in ["deferral", "refusal"] {
            for wake in ["project", "task", "none"] {
                for status in ["in_progress", "todo"] {
                    let c = ctx(true).await;
                    let t = task(&c, "T", status).await;
                    if stored == "deferral" {
                        deferred_dispatch::set(
                            &c.db,
                            &t,
                            "in_progress",
                            &(chrono::Utc::now() + chrono::Duration::minutes(10)).to_rfc3339(),
                            "backoff",
                        )
                        .await
                        .unwrap();
                    } else {
                        let capability = if status == "todo" {
                            "coder"
                        } else {
                            "in_progress"
                        };
                        deferred_dispatch::record_dispatch_disposition(
                            &c.db,
                            &t,
                            capability,
                            "refused: probe",
                        )
                        .await
                        .unwrap();
                    }
                    let agents = agent_labels(&c).await;
                    for _ in 0..2 {
                        tick(&c.dispatcher, 20).await;
                    }
                    let before = snapshot(&c.db, &c.pid, &agents).await;
                    match wake {
                        "project" => {
                            TaskRepo::wake_dispatch_for_project(&*c.db, &c.pid, &now_rfc3339())
                                .await
                                .unwrap();
                        }
                        "task" => crate::wake_task_dispatch(&c.db, &t.id, "probe")
                            .await
                            .unwrap(),
                        _ => {}
                    }
                    c.dispatcher.drain_steps().await.unwrap();
                    let woken = snapshot(&c.db, &c.pid, &agents).await;
                    for _ in 0..3 {
                        tick(&c.dispatcher, 20).await;
                    }
                    let after = snapshot(&c.db, &c.pid, &agents).await;
                    let name = format!("wake {stored}_{wake}_{status}");
                    lines.push(format!("{name} (parked): {}", before[0].1.line));
                    lines.push(format!(
                        "{name} (woken): dv={} {}",
                        woken[0].1.version - before[0].1.version,
                        woken[0].1.line
                    ));
                    lines.push(format!(
                        "{name} (after): dv={} {}",
                        after[0].1.version - before[0].1.version,
                        after[0].1.line
                    ));
                }
            }
        }
        settle("wake", lines);
    });
}
