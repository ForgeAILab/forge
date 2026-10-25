//! Randomized ledger deltas and batched attribution match reference aggregates.

use super::*;

#[tokio::test]
async fn randomized_differential_ledger_mutations() {
    // Each seed executes a few hundred real repository mutations. Attribution
    // tolerance is tested through append_usage_event with only its attribution
    // guard relaxed; every remaining V135 identity/lifecycle guard stays live.
    for seed in [0x1234_5678, 0xabc0_0191, 0xfade_7013] {
        let db = fixture().await;
        let index = UsageLedgerIndex::new(db.clone());
        let mut rng = seed;
        let mut projects = Vec::<String>::new();
        let mut executions = Vec::<String>::new();
        let mut invocations = Vec::<String>::new();
        let mut events = Vec::<String>::new();
        project(&db, "prelude").await;
        projects.push("prelude".to_owned());
        let exec = execution(
            &db,
            "prelude-run",
            "prelude",
            "a",
            db::ExecutionStatus::Running,
        )
        .await;
        executions.push(exec.id.clone());
        let inv = admit(&db, "prelude-inv", "prelude", &exec.id, "b").await;
        invocations.push(inv.id.clone());
        assert_reference(&db, &index, seed, 1000).await;
        let inv = start(&db, &inv).await;
        assert_reference(&db, &index, seed, 1001).await;
        let inv = UsageLedgerRepo::mark_usage_invocation_pending_settlement(
            &*db,
            db::MarkUsageInvocationPendingSettlement {
                id: inv.id,
                expected_version: inv.version,
                updated_at: AT.into(),
            },
        )
        .await
        .unwrap();
        assert_reference(&db, &index, seed, 1002).await;
        let inv = settle(&db, &inv).await;
        assert_reference(&db, &index, seed, 1003).await;
        let e = event(&db, &inv, "prelude-e", "a", false, 0).await;
        events.push(e.id.clone());
        assert_reference(&db, &index, seed, 1004).await;
        reprice(&db, &e, 5000).await;
        assert_reference(&db, &index, seed, 1005).await;
        reprice(&db, &e, 5001).await;
        assert_reference(&db, &index, seed, 1006).await;
        finish(&db, &exec, true).await;
        assert_reference(&db, &index, seed, 1007).await;
        let mut serial = 0;
        for step in 0..360 {
            if step % 60 == 0 {
                index.set_budget_mb(match (step / 60) % 3 {
                    0 => Some(0),
                    1 => Some(1),
                    _ => None,
                });
            }
            let choice = (next(&mut rng) % 13) as usize;
            serial += 1;
            if projects.is_empty() || choice == 0 {
                let id = format!("p-{serial}");
                project(&db, &id).await;
                projects.push(id);
            } else if executions.is_empty() || choice == 1 || choice == 2 {
                let p = &projects[next(&mut rng) as usize % projects.len()];
                let id = format!("exec-{serial}");
                execution(
                    &db,
                    &id,
                    p,
                    AGENTS[next(&mut rng) as usize % 3],
                    if choice == 2 {
                        db::ExecutionStatus::Completed
                    } else {
                        db::ExecutionStatus::Running
                    },
                )
                .await;
                executions.push(id);
            } else if invocations.is_empty() || choice == 3 {
                let id = &executions[next(&mut rng) as usize % executions.len()];
                if let Some(e) = ExecutionRepo::get_by_id(&*db, id).await.unwrap() {
                    let task = TaskRepo::get_by_id(&*db, &e.task_id, false)
                        .await
                        .unwrap()
                        .unwrap();
                    let id = format!("inv-{serial}");
                    admit(
                        &db,
                        &id,
                        &task.project_id,
                        &e.id,
                        AGENTS[next(&mut rng) as usize % 3],
                    )
                    .await;
                    invocations.push(id);
                }
            } else if choice <= 6 {
                let id = &invocations[next(&mut rng) as usize % invocations.len()];
                if let Some(i) = UsageLedgerRepo::get_usage_invocation(&*db, id)
                    .await
                    .unwrap()
                {
                    match i.lifecycle {
                        UsageInvocationLifecycle::Admitted => {
                            start(&db, &i).await;
                        }
                        UsageInvocationLifecycle::Started
                        | UsageInvocationLifecycle::PendingSettlement => {
                            if choice == 4 && i.lifecycle == UsageInvocationLifecycle::Started {
                                UsageLedgerRepo::mark_usage_invocation_pending_settlement(
                                    &*db,
                                    db::MarkUsageInvocationPendingSettlement {
                                        id: i.id.clone(),
                                        expected_version: i.version,
                                        updated_at: AT.into(),
                                    },
                                )
                                .await
                                .unwrap();
                            } else if choice == 5 {
                                UsageLedgerRepo::mark_usage_invocation_unsettled(
                                    &*db,
                                    db::MarkUsageInvocationUnsettled {
                                        id: i.id.clone(),
                                        expected_version: i.version,
                                        terminal_reason: "fixture failure".into(),
                                        settled_at: AT.into(),
                                        updated_at: AT.into(),
                                    },
                                )
                                .await
                                .unwrap();
                            } else {
                                settle(&db, &i).await;
                            }
                        }
                        _ => {}
                    }
                }
            } else if choice == 7 || choice == 8 {
                let id = &invocations[next(&mut rng) as usize % invocations.len()];
                if let Some(i) = UsageLedgerRepo::get_usage_invocation(&*db, id)
                    .await
                    .unwrap()
                {
                    if i.lifecycle == UsageInvocationLifecycle::Settled {
                        let id = format!("event-{serial}");
                        event(
                            &db,
                            &i,
                            &id,
                            AGENTS[next(&mut rng) as usize % 3],
                            choice == 7,
                            serial,
                        )
                        .await;
                        events.push(id);
                    }
                }
            } else if choice == 9 && !events.is_empty() {
                let id = &events[next(&mut rng) as usize % events.len()];
                if let Some(e) = UsageLedgerRepo::get_usage_event(&*db, id).await.unwrap() {
                    if e.cost_kind != UsageCostKind::ProviderReported {
                        reprice(&db, &e, step).await;
                    }
                }
            } else if choice == 10 {
                let id = &executions[next(&mut rng) as usize % executions.len()];
                if let Some(e) = ExecutionRepo::get_by_id(&*db, id).await.unwrap() {
                    if e.status == db::ExecutionStatus::Running {
                        finish(&db, &e, next(&mut rng).is_multiple_of(2)).await;
                    }
                }
            } else if choice == 11 {
                // There is no execution-delete repository API; this is the
                // exact SQL boundary used by task_service/workspace.rs.
                let n = next(&mut rng) as usize % executions.len();
                let id = executions.remove(n);
                sqlx::query("DELETE FROM execution WHERE id=?")
                    .bind(id)
                    .execute(db.pool())
                    .await
                    .unwrap();
            } else if choice == 12 && step % 7 == 0 {
                let n = next(&mut rng) as usize % projects.len();
                let id = projects.remove(n);
                for e in ExecutionRepo::list_running_for_project(&*db, &id)
                    .await
                    .unwrap()
                {
                    finish(&db, &e, true).await;
                }
                ProjectRepo::delete(&*db, &id).await.unwrap();
                executions = sqlx::query_scalar("SELECT id FROM execution ORDER BY id")
                    .fetch_all(db.pool())
                    .await
                    .unwrap();
                invocations = sqlx::query_scalar("SELECT id FROM usage_invocation ORDER BY id")
                    .fetch_all(db.pool())
                    .await
                    .unwrap();
                events = sqlx::query_scalar("SELECT id FROM usage_event ORDER BY id")
                    .fetch_all(db.pool())
                    .await
                    .unwrap();
            }
            assert_reference(&db, &index, seed, step).await;
        }
    }
}

#[tokio::test]
async fn randomized_batched_deltas() {
    // Each seed executes a few hundred real repository mutations. Attribution
    // tolerance is tested through append_usage_event with only its attribution
    // guard relaxed; every remaining V135 identity/lifecycle guard stays live.
    for seed in [
        0x1234_5678u64,
        0xabc0_0191,
        0xfade_7013,
        0x51ed_270b,
        0x0bad_cafe,
        0x7777_1234,
    ] {
        let mut check_rng = seed ^ 0x9e37_79b9;
        let mut until_read = 1 + next(&mut check_rng) % 8;
        let db = fixture().await;
        let index = UsageLedgerIndex::new(db.clone());
        let mut rng = seed;
        let mut projects = Vec::<String>::new();
        let mut executions = Vec::<String>::new();
        let mut invocations = Vec::<String>::new();
        let mut events = Vec::<String>::new();
        project(&db, "prelude").await;
        projects.push("prelude".to_owned());
        let exec = execution(
            &db,
            "prelude-run",
            "prelude",
            "a",
            db::ExecutionStatus::Running,
        )
        .await;
        executions.push(exec.id.clone());
        let inv = admit(&db, "prelude-inv", "prelude", &exec.id, "b").await;
        invocations.push(inv.id.clone());
        assert_reference(&db, &index, seed, 1000).await;
        let inv = start(&db, &inv).await;
        assert_reference(&db, &index, seed, 1001).await;
        let inv = UsageLedgerRepo::mark_usage_invocation_pending_settlement(
            &*db,
            db::MarkUsageInvocationPendingSettlement {
                id: inv.id,
                expected_version: inv.version,
                updated_at: AT.into(),
            },
        )
        .await
        .unwrap();
        assert_reference(&db, &index, seed, 1002).await;
        let inv = settle(&db, &inv).await;
        assert_reference(&db, &index, seed, 1003).await;
        let e = event(&db, &inv, "prelude-e", "a", false, 0).await;
        events.push(e.id.clone());
        assert_reference(&db, &index, seed, 1004).await;
        reprice(&db, &e, 5000).await;
        assert_reference(&db, &index, seed, 1005).await;
        reprice(&db, &e, 5001).await;
        assert_reference(&db, &index, seed, 1006).await;
        finish(&db, &exec, true).await;
        assert_reference(&db, &index, seed, 1007).await;
        admission_binding(&db).await;
        let mut serial = 0;
        for step in 0..360 {
            let choice = (next(&mut rng) % 19) as usize;
            serial += 1;
            if projects.is_empty() || choice == 0 {
                let id = format!("p-{serial}");
                project(&db, &id).await;
                projects.push(id);
            } else if executions.is_empty() || choice == 1 || choice == 2 {
                let p = &projects[next(&mut rng) as usize % projects.len()];
                let id = format!("exec-{serial}");
                execution(
                    &db,
                    &id,
                    p,
                    AGENTS[next(&mut rng) as usize % 3],
                    if choice == 2 {
                        db::ExecutionStatus::Completed
                    } else {
                        db::ExecutionStatus::Running
                    },
                )
                .await;
                executions.push(id);
            } else if invocations.is_empty() || choice == 3 {
                let id = &executions[next(&mut rng) as usize % executions.len()];
                if let Some(e) = ExecutionRepo::get_by_id(&*db, id).await.unwrap() {
                    let task = TaskRepo::get_by_id(&*db, &e.task_id, false)
                        .await
                        .unwrap()
                        .unwrap();
                    let id = format!("inv-{serial}");
                    let surface = [
                        DbUsageSurface::TaskExecution,
                        DbUsageSurface::ProjectChat,
                        DbUsageSurface::MainChat,
                        DbUsageSurface::GenesisChat,
                        DbUsageSurface::MainInquiry,
                    ][next(&mut rng) as usize % 5];
                    let project = matches!(
                        surface,
                        DbUsageSurface::TaskExecution | DbUsageSurface::ProjectChat
                    )
                    .then_some(task.project_id.as_str());
                    let owner = (!next(&mut rng).is_multiple_of(4))
                        .then_some(AGENTS[next(&mut rng) as usize % 3]);
                    let ordinal = (next(&mut rng) % 4) as i64;
                    let priced = next(&mut rng).is_multiple_of(2);
                    scoped_admit(
                        &db,
                        &id,
                        project,
                        &format!("{}-{surface}", e.id),
                        owner,
                        surface,
                        ordinal,
                        priced,
                    )
                    .await;
                    invocations.push(id);
                }
            } else if choice <= 6 {
                let id = &invocations[next(&mut rng) as usize % invocations.len()];
                if let Some(i) = UsageLedgerRepo::get_usage_invocation(&*db, id)
                    .await
                    .unwrap()
                {
                    match i.lifecycle {
                        UsageInvocationLifecycle::Admitted => {
                            start(&db, &i).await;
                        }
                        UsageInvocationLifecycle::Started
                        | UsageInvocationLifecycle::PendingSettlement => {
                            if choice == 4 && i.lifecycle == UsageInvocationLifecycle::Started {
                                UsageLedgerRepo::mark_usage_invocation_pending_settlement(
                                    &*db,
                                    db::MarkUsageInvocationPendingSettlement {
                                        id: i.id.clone(),
                                        expected_version: i.version,
                                        updated_at: AT.into(),
                                    },
                                )
                                .await
                                .unwrap();
                            } else if choice == 5 {
                                UsageLedgerRepo::mark_usage_invocation_unsettled(
                                    &*db,
                                    db::MarkUsageInvocationUnsettled {
                                        id: i.id.clone(),
                                        expected_version: i.version,
                                        terminal_reason: "fixture failure".into(),
                                        settled_at: AT.into(),
                                        updated_at: AT.into(),
                                    },
                                )
                                .await
                                .unwrap();
                            } else {
                                if next(&mut rng).is_multiple_of(4) {
                                    UsageLedgerRepo::settle_usage_invocation(
                                        &*db,
                                        db::SettleUsageInvocation {
                                            id: i.id.clone(),
                                            expected_version: i.version,
                                            telemetry_state: DbUsageTelemetryState::Unmetered,
                                            terminal_reason: Some("provider did not meter".into()),
                                            settled_at: timestamp(next(&mut rng)),
                                            updated_at: timestamp(next(&mut rng)),
                                        },
                                    )
                                    .await
                                    .unwrap();
                                } else {
                                    settle(&db, &i).await;
                                }
                            }
                        }
                        _ => {}
                    }
                }
            } else if choice == 7 || choice == 8 {
                let id = &invocations[next(&mut rng) as usize % invocations.len()];
                if let Some(i) = UsageLedgerRepo::get_usage_invocation(&*db, id)
                    .await
                    .unwrap()
                {
                    if i.lifecycle == UsageInvocationLifecycle::Settled {
                        let id = format!("event-{serial}");
                        event(
                            &db,
                            &i,
                            &id,
                            AGENTS[next(&mut rng) as usize % 3],
                            choice == 7,
                            serial,
                        )
                        .await;
                        events.push(id);
                    }
                }
            } else if choice == 9 && !events.is_empty() {
                let id = &events[next(&mut rng) as usize % events.len()];
                if let Some(e) = UsageLedgerRepo::get_usage_event(&*db, id).await.unwrap() {
                    if e.cost_kind != UsageCostKind::ProviderReported
                        && e.project_id.is_some()
                        && e.input_tokens.is_some()
                    {
                        reprice(&db, &e, step).await;
                    }
                }
            } else if choice == 10 {
                let id = &executions[next(&mut rng) as usize % executions.len()];
                if let Some(e) = ExecutionRepo::get_by_id(&*db, id).await.unwrap() {
                    if e.status == db::ExecutionStatus::Running {
                        {
                            let failed = next(&mut rng).is_multiple_of(2);
                            let at = timestamp(next(&mut rng));
                            finish_at(&db, &e, failed, &at).await;
                        }
                    }
                }
            } else if choice == 11 {
                // There is no execution-delete repository API; this is the
                // exact SQL boundary used by task_service/workspace.rs.
                let n = next(&mut rng) as usize % executions.len();
                let id = executions.remove(n);
                sqlx::query("DELETE FROM execution WHERE id=?")
                    .bind(id)
                    .execute(db.pool())
                    .await
                    .unwrap();
            } else if choice == 12 && step % 7 == 0 {
                let n = next(&mut rng) as usize % projects.len();
                let id = projects.remove(n);
                for e in ExecutionRepo::list_running_for_project(&*db, &id)
                    .await
                    .unwrap()
                {
                    finish(&db, &e, true).await;
                }
                ProjectRepo::delete(&*db, &id).await.unwrap();
                executions = sqlx::query_scalar("SELECT id FROM execution ORDER BY id")
                    .fetch_all(db.pool())
                    .await
                    .unwrap();
                invocations = sqlx::query_scalar("SELECT id FROM usage_invocation ORDER BY id")
                    .fetch_all(db.pool())
                    .await
                    .unwrap();
                events = sqlx::query_scalar("SELECT id FROM usage_event ORDER BY id")
                    .fetch_all(db.pool())
                    .await
                    .unwrap();
            } else if choice == 13 {
                let id = &executions[next(&mut rng) as usize % executions.len()];
                let at = timestamp(next(&mut rng));
                sqlx::query("UPDATE execution SET updated_at=?, last_activity_at=? WHERE id=? AND status='running'")
                    .bind(&at).bind(&at).bind(id).execute(db.pool()).await.unwrap();
            } else if choice == 14 {
                let id = &executions[next(&mut rng) as usize % executions.len()];
                let at = timestamp(next(&mut rng));
                sqlx::query("UPDATE execution SET updated_at=? WHERE id=? AND status!='running'")
                    .bind(&at)
                    .bind(id)
                    .execute(db.pool())
                    .await
                    .unwrap();
            } else if choice == 15 {
                let id = &executions[next(&mut rng) as usize % executions.len()];
                let agent = AGENTS[next(&mut rng) as usize % 3];
                sqlx::query("UPDATE execution SET agent_id=? WHERE id=?")
                    .bind(agent)
                    .bind(id)
                    .execute(db.pool())
                    .await
                    .unwrap();
            } else if choice == 16 {
                let id = &executions[next(&mut rng) as usize % executions.len()];
                let at = format!("1970-01-01T00:00:{:02}Z", next(&mut rng) % 60);
                sqlx::query("UPDATE execution SET created_at=? WHERE id=?")
                    .bind(&at)
                    .bind(id)
                    .execute(db.pool())
                    .await
                    .unwrap();
            }
            until_read -= 1;
            if step == 359 || until_read == 0 {
                until_read = 1 + next(&mut check_rng) % 8;
                assert_reference_full(&db, &index, seed, step).await;
            }
        }
        let nonempty_cold = UsageLedgerIndex::new(db.clone());
        assert_reference_full(&db, &nonempty_cold, seed, 9997).await;
        // Remove Project scopes through their guarded repository boundary;
        // NULL-Project usage then exercises the final account-row cascade.
        for project in &projects {
            for execution in ExecutionRepo::list_running_for_project(&*db, project)
                .await
                .unwrap()
            {
                finish(&db, &execution, true).await;
            }
            ProjectRepo::delete(&*db, project).await.unwrap();
        }
        db::UserRepo::delete_user(&*db, "pricing-owner")
            .await
            .unwrap();
        assert_reference_full(&db, &index, seed, 9998).await;
        let cold = UsageLedgerIndex::new(db.clone());
        assert_reference_full(&db, &cold, seed, 9999).await;
    }
}

#[tokio::test]
async fn batched_agent_reference_equals_per_invocation_reference() {
    // Reuse the batched random generator indirectly: build a mixed ledger, then compare.
    for seed in [0x1234_5678u64, 0xabc0_0191, 0xfade_7013] {
        let db = fixture().await;
        let mut rng = seed;
        project(&db, "p").await;
        let mut invs = Vec::new();
        let mut evs = Vec::new();
        for n in 0..60 {
            let id = format!("run-{n}");
            let status = if next(&mut rng).is_multiple_of(3) {
                db::ExecutionStatus::Running
            } else {
                db::ExecutionStatus::Completed
            };
            execution(&db, &id, "p", AGENTS[next(&mut rng) as usize % 3], status).await;
            for k in 0..(next(&mut rng) % 3) {
                let i = admit(
                    &db,
                    &format!("inv-{n}-{k}"),
                    "p",
                    &id,
                    AGENTS[next(&mut rng) as usize % 3],
                )
                .await;
                let i = start(&db, &i).await;
                let i = match next(&mut rng) % 4 {
                    0 => i,
                    1 => UsageLedgerRepo::mark_usage_invocation_unsettled(
                        &*db,
                        db::MarkUsageInvocationUnsettled {
                            id: i.id.clone(),
                            expected_version: i.version,
                            terminal_reason: "x".into(),
                            settled_at: AT.into(),
                            updated_at: AT.into(),
                        },
                    )
                    .await
                    .unwrap(),
                    _ => settle(&db, &i).await,
                };
                if i.lifecycle == UsageInvocationLifecycle::Settled {
                    for j in 0..(next(&mut rng) % 3) {
                        let e = event(
                            &db,
                            &i,
                            &format!("ev-{n}-{k}-{j}"),
                            AGENTS[next(&mut rng) as usize % 3],
                            next(&mut rng).is_multiple_of(2),
                            j as i64,
                        )
                        .await;
                        evs.push(e);
                    }
                }
                invs.push(i);
            }
        }
        let mut step = 0;
        for e in &evs {
            if e.cost_kind != UsageCostKind::ProviderReported && next(&mut rng).is_multiple_of(3) {
                step += 1;
                reprice(&db, e, 7000 + step).await;
            }
        }
        for id in AGENTS {
            let new = usage_aggregate_for_agent(&db, id).await.unwrap();
            let old = old_agent_reference(&db, id).await.unwrap();
            assert_eq!(
                serde_json::to_value(new).unwrap(),
                serde_json::to_value(old).unwrap(),
                "agent {id} seed {seed}"
            );
        }
    }
}
