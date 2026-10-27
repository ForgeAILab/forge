//! Seeded liveness, lead planning, invariants and workflow recovery cases.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_root_with_a_lead_plans_and_has_every_step_checked() {
    log_to_stderr();
    if let Err(error) = root_with_a_lead().await {
        panic!("{error}");
    }
}

/// Exhaust the real step gate, then the replanning lead creates a fix,
/// recovers the parked step and cancels the untouched suffix via native tools.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replanning_lead_uses_real_operations() {
    let mut world = World::new().await;
    world.create(None).await.unwrap();
    world.create(Some(0)).await.unwrap();
    world.create(Some(0)).await.unwrap();
    for action in [
        Action::AssignLead(0),
        Action::Finish(0, Outcome::Success),
        Action::Verdict(1, false),
    ] {
        world.lead_step(action).await.unwrap();
    }
    for _ in 0..3 {
        world
            .lead_step(Action::Finish(1, Outcome::Success))
            .await
            .unwrap();
        if world.waiting(1) {
            world
                .lead_step(Action::Finish(1, Outcome::Success))
                .await
                .unwrap();
        }
    }
    assert_eq!(
        world.task(0).await.unwrap()["status"],
        "planning",
        "{:?}",
        world.trace
    );
    assert_eq!(
        world
            .lead_count("SELECT SUM(spent) FROM task_budget WHERE kind='replan'")
            .await
            .unwrap(),
        1
    );
    world
        .lead_step(Action::Finish(0, Outcome::Success))
        .await
        .unwrap();
    assert_eq!(
        *world.shared.lead_operations.lock().unwrap(),
        [
            "lead.subtask:create",
            "lead.recover:retry",
            "lead.subtask:cancel"
        ]
    );
    assert_eq!(world.tasks.len(), 4);
    assert_eq!(world.task(2).await.unwrap()["status"], "cancelled");
    assert_eq!(world.task(0).await.unwrap()["status"], "in_progress");
    world.lead_step(Action::Verdict(1, true)).await.unwrap();
    world.drive_to_settlement().await.unwrap();
    assert_eq!(world.task(0).await.unwrap()["status"], "done");
    assert_eq!(world.task(3).await.unwrap()["status"], "done");
}

/// Different submitted steps consume one root's full re-plan window. Each
/// allowed re-plan performs native lead operations; the sixth trigger parks
/// the root instead of admitting another planning run or another debit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replanning_lead_operations_stop_at_the_root_budget() {
    let mut world = World::new().await;
    world.create(None).await.unwrap();
    world.create(Some(0)).await.unwrap();
    world.create(Some(0)).await.unwrap();
    world.lead_step(Action::AssignLead(0)).await.unwrap();
    world
        .lead_step(Action::Finish(0, Outcome::Success))
        .await
        .unwrap();
    let mut child = 1;
    for round in 1..=6 {
        world
            .lead_step(Action::Verdict(child, false))
            .await
            .unwrap();
        // Five actions from a worker (or four from a recovered check)
        // exhaust the gate. Stop before Finish's fallback could open the
        // root's planning run instead of this child's run.
        for _ in 0..6 {
            let root = world.task(0).await.unwrap();
            if root["status"] == "planning"
                || root["condition"]["primary"]["budget_kind"] == "replan"
            {
                break;
            }
            assert!(world.waiting(child), "the submitted step owns the next run");
            world
                .lead_step(Action::Finish(child, Outcome::Success))
                .await
                .unwrap();
        }
        let root = world.task(0).await.unwrap();
        if round == 6 {
            assert_eq!(root["status"], "in_progress");
            assert_eq!(
                root["condition"]["primary"]["kind"], "budget_exhausted",
                "{root}"
            );
            assert_eq!(
                root["condition"]["primary"]["budget_kind"], "replan",
                "{root}"
            );
            assert!(!world.waiting(0));
            break;
        }
        assert_eq!(root["status"], "planning", "{root}");
        world
            .lead_step(Action::Finish(0, Outcome::Success))
            .await
            .unwrap();
        assert_eq!(world.task(0).await.unwrap()["status"], "in_progress");
        let next = world.tasks.len() - 1;
        world.lead_step(Action::Verdict(child, true)).await.unwrap();
        // Retry validates the existing work first. If the product instead
        // needs an implementation run, finish that and its check too.
        for _ in 0..2 {
            if world.task(child).await.unwrap()["status"] == "done" {
                break;
            }
            assert!(world.waiting(child));
            world
                .lead_step(Action::Finish(child, Outcome::Success))
                .await
                .unwrap();
        }
        assert_eq!(world.task(child).await.unwrap()["status"], "done");
        child = next;
    }
    assert_eq!(
        world
            .lead_count("SELECT SUM(spent) FROM task_budget WHERE kind='replan'")
            .await
            .unwrap(),
        5
    );
    assert_eq!(
        world
            .lead_count("SELECT COUNT(*) FROM task_budget_charge WHERE kind='replan'")
            .await
            .unwrap(),
        5
    );
    assert_eq!(
        world
            .lead_count("SELECT COUNT(*) FROM transition_log WHERE bridge_kind='replan'")
            .await
            .unwrap(),
        5
    );
    assert_eq!(
        world
            .shared
            .lead_operations
            .lock()
            .unwrap()
            .iter()
            .filter(|op| op.as_str() == "lead.subtask:create")
            .count(),
        5
    );
    world.check_quiescent().await.unwrap();
}

/// A lead retries the exhausted gate, which validates the existing work.
/// Another failing verdict must stay within the gate's current window.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_validation_after_lead_recovery_respects_the_step_budget() {
    let mut world = World::new().await;
    world.create(None).await.unwrap();
    world.create(Some(0)).await.unwrap();
    world.create(Some(0)).await.unwrap();
    world.lead_step(Action::AssignLead(0)).await.unwrap();
    world
        .lead_step(Action::Finish(0, Outcome::Success))
        .await
        .unwrap();
    world.lead_step(Action::Verdict(1, false)).await.unwrap();
    for _ in 0..3 {
        world
            .lead_step(Action::Finish(1, Outcome::Success))
            .await
            .unwrap();
        if world.waiting(1) {
            world
                .lead_step(Action::Finish(1, Outcome::Success))
                .await
                .unwrap();
        }
    }
    assert_eq!(world.task(0).await.unwrap()["status"], "planning");
    world
        .lead_step(Action::Finish(0, Outcome::Success))
        .await
        .unwrap();
    assert!(world.waiting(1), "recovery starts the validation check");
    // Keep the failed verdict instead of passing the recovered validation.
    world
        .lead_step(Action::Finish(1, Outcome::Success))
        .await
        .unwrap();
    world.check_quiescent().await.unwrap();
}

/// The audit of (l) on rows written directly: what a product that re-plans
/// a root with no lead would write is caught as a charge alone, as an entry
/// alone, and as both; the history of an authorized re-plan is not.
#[tokio::test]
async fn replan_authority_audit_retains_history_and_catches_later_illegal_effects() {
    let pool = db::create_sqlite_pool("sqlite::memory:").await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    install_replan_audit(&pool).await;
    let entry = |id: &str, receipt: &str| {
        format!("INSERT INTO transition_log(id,task_id,from_state,to_state,triggered_by,trigger_reason,created_at,bridge_kind,bridge_payload) VALUES('{id}','root','in_progress','planning','system:workflow','model','now','replan','{{\"receipt\":\"{receipt}\"}}');")
    };
    let charge = |receipt: &str| {
        format!("INSERT INTO task_budget_charge(task_id,kind,window_id,step_id) VALUES('root','replan','w','{receipt}');")
    };
    let flagged = || async {
        sqlx::query_scalar::<_, String>(
            "SELECT kind || ' ' || receipt FROM model_replan_authority WHERE eligible=0 ORDER BY rowid",
        )
        .fetch_all(&pool)
        .await
        .unwrap()
    };
    sqlx::raw_sql(&format!("INSERT INTO project(id,name,created_at,updated_at) VALUES('p','p','now','now');
        INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('root','p','root','planning','now','now');
        INSERT INTO task(id,project_id,parent_task_id,title,status,created_at,updated_at) VALUES('child','p','root','child','todo','now','now');
        INSERT INTO task_role_assignment(id,task_id,role_name,assignee_type,assignee_id,created_at,updated_at) VALUES('a','root','planner','agent','lead','now','now');
        INSERT INTO task_budget(task_id,kind,window_id,spent) VALUES('root','replan','w',2);
        {} {} {}
        DELETE FROM task_role_assignment WHERE id='a';
        {}",
        charge("with-lead"), entry("e1", "with-lead"), charge("decided-with-lead"),
        // The designed race: decided and charged under the lead, entered
        // after the owner removed it.
        entry("e2", "decided-with-lead")))
        .execute(&pool).await.unwrap();
    assert_eq!(
        flagged().await,
        Vec::<String>::new(),
        "an authorized re-plan stays authorized after the lead is removed"
    );
    // A product that re-plans a root with no lead: the charge alone, the
    // entry alone (no charge, and a receipt charged without a lead), both.
    sqlx::raw_sql(&format!(
        "{} {} {}",
        charge("no-lead"),
        entry("e3", "never-charged"),
        entry("e4", "no-lead")
    ))
    .execute(&pool)
    .await
    .unwrap();
    assert_eq!(
        flagged().await,
        ["charge no-lead", "entry never-charged", "entry no-lead"],
        "every effect decided without a lead is a violation"
    );
    // Assigning a lead again does not launder them.
    sqlx::raw_sql("INSERT INTO task_role_assignment(id,task_id,role_name,assignee_type,assignee_id,created_at,updated_at) VALUES('b','root','planner','agent','lead','now','now');")
        .execute(&pool).await.unwrap();
    assert_eq!(flagged().await.len(), 3);
    pool.close().await;
}

#[test]
fn settlement_scheduling_does_not_change_the_action_trace_digest() {
    let base = vec![
        "0 Create -> task running".to_owned(),
        "pauses lifted: task running".to_owned(),
    ];
    let mut isolated = base.clone();
    isolated.extend(
        [
            "settle 0 []: task review/check",
            "settle 1 []: task done",
            "settled: task done",
        ]
        .map(str::to_owned),
    );
    let mut concurrent = base;
    concurrent.extend(["settle 0 []: task done", "settled: task done"].map(str::to_owned));
    assert_eq!(digest(&isolated), digest(&concurrent));
    concurrent[0] = "0 Create -> task parked".to_owned();
    assert_ne!(digest(&isolated), digest(&concurrent));
    concurrent[0] = isolated[0].clone();
    *concurrent.last_mut().unwrap() = "settled: task cancelled".to_owned();
    assert_ne!(digest(&isolated), digest(&concurrent));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn random_sequences_keep_every_task_live() {
    let long = std::env::var("FORGE_MODEL_LONG").is_ok_and(|value| value != "0");
    let steps = env_number("FORGE_MODEL_STEPS").unwrap_or(if long { 40 } else { 16 }) as usize;
    let chosen: Vec<u64> = std::env::var("FORGE_MODEL_SEED")
        .map(|list| {
            list.split(',')
                .filter_map(|seed| seed.trim().parse().ok())
                .collect()
        })
        .unwrap_or_default();
    let seeds = if chosen.is_empty() {
        // Random seeds only on request: see the header.
        let extra = env_number("FORGE_MODEL_CASES").unwrap_or(0);
        let mut clock = Rng(std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| since.as_nanos() as u64));
        let mut seeds = FIXED_SEEDS.to_vec();
        seeds.extend((0..extra).map(|_| clock.next() % 1_000_000_000));
        seeds
    } else {
        chosen
    };
    println!("model seeds (reproduce one with FORGE_MODEL_SEED=<n>, steps {steps}): {seeds:?}");
    run_all(
        seeds
            .into_iter()
            .map(|seed| (format!("FORGE_MODEL_SEED={seed}"), generate(seed, steps)))
            .collect(),
        true,
    )
    .await;
}

/// A machine with `max_concurrent_runs: 1`. Before the fix a single Task
/// parked on a machine slot that its own running execution held
/// (`placement::machine_precheck::task_blocked` counted it), and nothing but
/// another Task's dispatch retired that wait.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequences_stay_live_at_run_cap_1() {
    PINNED_RUN_CAP
        .scope(Some(1), run_cases(cap_matrix_cases()))
        .await;
}

/// With one slot the whole lifecycle still happens, each stage in that slot:
/// the coder run, the review-entry check, the reviewer run and the merge.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_slot_carries_a_task_from_run_to_merge() {
    PINNED_RUN_CAP
        .scope(Some(1), async {
            let mut world = World::new().await;
            let outcome: Result<(), String> = async {
                for action in [Action::Create, Action::Finish(0, Outcome::Success)] {
                    let applied = world.apply(&action).await?;
                    world.quiesce().await?;
                    let state = world.check_quiescent().await?;
                    world
                        .trace
                        .push(format!("{action:?} -> {applied}: {state}"));
                }
                world.drive_to_settlement().await?;
                let task = world.task(0).await?;
                if task["status"] != "done" {
                    return Err(format!("the Task ended in {}", task["status"]));
                }
                let pool = &world.live.as_ref().expect("the stack is up").pool;
                let checks: Vec<(String,)> = sqlx::query_as("SELECT state FROM check_run")
                    .fetch_all(pool)
                    .await
                    .map_err(|error| error.to_string())?;
                if checks.is_empty() || checks.iter().any(|(state,)| state != "succeeded") {
                    return Err(format!("review-entry check runs: {checks:?}"));
                }
                let runs: Vec<(String, String)> =
                    sqlx::query_as("SELECT role, status FROM execution ORDER BY created_at, rowid")
                        .fetch_all(pool)
                        .await
                        .map_err(|error| error.to_string())?;
                for role in ["coder", "reviewer"] {
                    if !runs
                        .iter()
                        .any(|(ran, status)| ran == role && status == "completed")
                    {
                        return Err(format!("no completed {role} run in {runs:?}"));
                    }
                }
                Ok(())
            }
            .await;
            if let Err(violation) = outcome {
                panic!("{violation}\ntrace:\n{}", world.trace.join("\n"));
            }
        })
        .await;
}

/// No pinned cap: the server resolves its own from the host's cores.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sequences_stay_live_without_a_pinned_run_cap() {
    PINNED_RUN_CAP
        .scope(None, run_cases(cap_matrix_cases()))
        .await;
}

/// Sequences for wedges this project has shipped, as far as the action set
/// can express them. Each must end with every Task settled.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn known_wedges_stay_fixed() {
    use Action::{Cancel, Claim, Crash, Create, Depend, Finish};
    run_cases(vec![
        // v0.13.10: the merge landed but the cascade stopped short and the
        // Task stayed in `merging`. The plain path must reach `done`.
        (
            "merge cascade reaches done".to_owned(),
            vec![
                step(Create),
                step(Claim(0)),
                step(Finish(0, Outcome::Success)),
            ],
        ),
        // 3.2 C2b: a merge intent orphaned by a crash. The coder's result is
        // committed, the server dies before the review / merge steps run.
        (
            "crash before the review and merge steps".to_owned(),
            vec![
                step(Create),
                step(Claim(0)),
                crash_after(Finish(0, Outcome::Success)),
            ],
        ),
        // A crash with a coder run in flight: recovery must reclaim it.
        (
            "crash with a run in flight".to_owned(),
            vec![step(Create), step(Claim(0)), step(Crash)],
        ),
        // Review-refresh livelock (stress pass 3) and conflict handoff: two
        // Tasks edit the same file, the second conflicts at merge, is handed
        // back, reconciled and must merge after a fresh review.
        (
            "second merge conflicts and is handed back".to_owned(),
            vec![
                step(Create),
                step(Create),
                step(Claim(0)),
                step(Claim(1)),
                step(Finish(0, Outcome::Conflict)),
                step(Finish(1, Outcome::Conflict)),
            ],
        ),
        // Cancelled-dependency stranding: the dependant must keep a way out.
        (
            "dependency cancelled under a waiting Task".to_owned(),
            vec![
                step(Create),
                step(Create),
                step(Depend(1, 0)),
                step(Cancel(0)),
            ],
        ),
        // Found by a random run of this model and fixed with it: linking
        // the same dependency twice answered 500.
        (
            "the same dependency twice".to_owned(),
            vec![
                step(Create),
                step(Create),
                step(Depend(1, 0)),
                step(Depend(1, 0)),
                step(Finish(0, Outcome::Success)),
            ],
        ),
        // A dependant of a finished Task is released.
        (
            "dependency completes under a waiting Task".to_owned(),
            vec![
                step(Create),
                step(Create),
                step(Depend(1, 0)),
                step(Claim(0)),
                step(Finish(0, Outcome::Success)),
            ],
        ),
        // A failed review and a failed check go back to the coder and recover.
        (
            "review rejection and check failure recover".to_owned(),
            vec![
                step(Create),
                step(Action::Verdict(0, false)),
                step(Claim(0)),
                step(Finish(0, Outcome::CiFail)),
                step(Finish(0, Outcome::Success)),
            ],
        ),
    ])
    .await;
}

/// Pause with no resume: every hold the API offers must offer its release.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_task_always_offers_its_release() {
    let mut world = World::new().await;
    let outcome: Result<(), String> = async {
        world.apply(&Action::Create).await?;
        world.quiesce().await?;
        let mut held = 0;
        for stage in ["initial", "claimed"] {
            let (version, offers) = world.offers(0).await?;
            if let Some(hold) = offers
                .iter()
                .find(|offer| offer["action"]["verb"] == "hold")
            {
                held += 1;
                world.apply_offer(0, version, hold).await?;
                world.quiesce().await?;
                world.check_quiescent().await?;
                let (version, offers) = world.offers(0).await?;
                let release = offers
                    .iter()
                    .find(|offer| offer["action"]["verb"] == "release")
                    .ok_or_else(|| {
                        format!("(c) {stage}: a held Task offers no release: {offers:?}")
                    })?;
                world.apply_offer(0, version, release).await?;
                world.quiesce().await?;
                world.check_quiescent().await?;
            }
            world.apply(&Action::Claim(0)).await?;
            world.quiesce().await?;
        }
        if held == 0 {
            return Err("`hold` was never offered, so this test checked nothing".to_owned());
        }
        world.drive_to_settlement().await
    }
    .await;
    if let Err(violation) = outcome {
        panic!("{violation}\ntrace:\n{}", world.trace.join("\n"));
    }
}

/// Found by a random run of this model and fixed with it; minimized below.
///
/// `start` was offered on a Task waiting in `todo` (for capacity, or because
/// its coder was paused) for the Agent of the planning gate's role, which
/// with no planner assigned fell back to any available Agent, while the
/// claim it then made entered `in_progress` for the coder role. The claim
/// was refused ("role 'coder' is assigned to a different agent"), the Task
/// was parked as `recovery_required`, and the `retry` then offered restored
/// the same park every time.
///
/// The offer, the queued action and its replay now resolve the role the way
/// the claim does (`services::workflow::action_role`). So `start` on a Task
/// queued for capacity is accepted and waits for that capacity, and `start`
/// is not offered while the Agent it would run on is paused: resuming the
/// Agent is that Task's exit.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn start_on_a_queued_task_does_not_park_it_as_failed() {
    // Pinned: the third Task must queue behind the Agent's two runs. With one
    // machine slot it waits for the machine instead and `start` is not offered.
    PINNED_RUN_CAP
        .scope(Some(MODEL_RUN_CAP), start_on_a_queued_task())
        .await;
}

/// Found by this model, fixed. A held Task used to lose its hold when one of
/// its dependencies was cancelled: the `dependency_cancelled` blocker
/// (`services/src/task_service/dependencies.rs`) overwrote it, and removing
/// the cancelled dependency then left the Task in `in_progress` with a
/// `clear` condition, no run and no park. The blocker now carries the
/// condition it displaces and removing the dependency restores it: the Task
/// is held again and offers `release`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_held_task_survives_a_cancelled_dependency() {
    use Action::{Cancel, Create, Depend, Take};
    run_cases(vec![(
        "held dependant, dependency cancelled".to_owned(),
        vec![
            step(Create),
            step(Create),
            step(Depend(0, 1)),
            step(Take(0, "hold")),
            step(Cancel(1)),
        ],
    )])
    .await;
}

/// Found by a random run of this model, fixed; minimized below. A dependency
/// is cancelled while its dependant is already parked for another reason
/// (here: an owner approval with no implementation candidate). The typed
/// `dependency_cancelled` blocker used to be skipped, so `send_back`, `retry`
/// and `restart` stayed on offer, each accepted and then refused by the
/// dependency gate into an untyped `recovery_required: "dependency gate"`
/// park. The Task now shows the typed blocker naming the dependency, offers
/// cancellation only, and gets its original park back when the dependency is
/// removed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parked_task_names_its_cancelled_dependency() {
    use Action::{Cancel, Create, Depend, Take};
    run_cases(vec![(
        "parked dependant, dependency cancelled".to_owned(),
        vec![
            step(Create),
            step(Take(0, "approve")),
            step(Create),
            step(Depend(0, 1)),
            step(Cancel(1)),
        ],
    )])
    .await;
}

/// The adjacent gap: the dependency is unfinished, not cancelled. An accepted
/// `send_back` or `retry` used to be refused by the dependency gate into the
/// same untyped `recovery_required: "dependency gate"` park, which offered
/// the refused action again. The action now stays accepted behind the
/// dependency wait and runs when the dependency finishes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_accepted_action_waits_for_an_unfinished_dependency() {
    use Action::{Create, Depend, Finish, Take};
    let sequence = vec![
        step(Create),
        step(Take(0, "approve")),
        step(Create),
        step(Depend(0, 1)),
        step(Take(0, "send_back")),
        step(Take(0, "retry")),
        step(Finish(1, Outcome::Success)),
    ];
    let name = "parked dependant, dependency unfinished";
    let trace = run(&sequence, false)
        .await
        .unwrap_or_else(|failure| panic!("{}", report(name, &sequence, &failure)));
    assert!(
        !trace.iter().any(|line| line.contains("recovery_required")),
        "{name}: the wait became an untyped park\n{}",
        trace.join("\n")
    );
    run_cases(vec![(name.to_owned(), sequence)]).await;
}

/// Found by a random run of this model and fixed with it; minimized below.
///
/// A coder run that failed after the Project's version changed (a pause, a
/// resume, any settings edit) was dropped: the Task stayed in `in_progress`
/// with a `clear` condition, no retry deadline, no blocker and no park, and
/// nothing relaunched it.
///
/// The failure path and the dispatcher's healer both refused to act on a run
/// dispatched under an older Project version. That fence is for completions,
/// which advance the workflow. A failure is now retried (or blocks the Task)
/// under the current Project whichever version dispatched the run.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_run_that_fails_after_a_project_pause_is_retried() {
    use Action::{Create, EditProject, Finish, PauseProject, ResumeProject};
    let mut cases = Vec::new();
    for outcome in [Outcome::Fail, Outcome::UsageLimit] {
        for (name, edits) in [
            ("while the Project is paused", vec![PauseProject]),
            (
                "after the Project was paused and resumed",
                vec![PauseProject, ResumeProject],
            ),
            ("after the Project was edited", vec![EditProject]),
        ] {
            let mut sequence = vec![step(Create)];
            sequence.extend(edits.into_iter().map(step));
            sequence.push(step(Finish(0, outcome)));
            cases.push((format!("run ends {outcome:?} {name}"), sequence));
        }
    }
    // The failed run must leave a pending retry behind, not only settle
    // because the final drive pokes the Task.
    for (name, sequence) in &cases {
        if name.contains("paused") && !name.contains("resumed") {
            continue; // a paused Project legitimately waits.
        }
        let trace = run(sequence, false)
            .await
            .unwrap_or_else(|failure| panic!("{}", report(name, sequence, &failure)));
        let after_failure = &trace[sequence.len() - 1];
        assert!(
            after_failure.contains("0:in_progress/deferred/")
                || after_failure.contains("0:in_progress/running/"),
            "{name}: the failed run left no pending retry and no new run: {after_failure}\n{}",
            trace.join("\n")
        );
    }
    run_cases(cases).await;
}

/// Found by a random run of this model (`FORGE_MODEL_SEED=45233331
/// FORGE_MODEL_LONG=1`) and fixed with it; minimized below. The coder Agent
/// is paused, the running Task is held and then released. The hold stopped
/// the run with a `manual` resume policy, and a release taken while no Agent
/// could run the Task (`release_to_dispatch_queue`,
/// `services/src/task_service/actions.rs`) cleared the hold but left that
/// policy, so the dispatcher's stopped-run park
/// (`task_dispatcher/snapshot.rs`, `stopped_execution`) kept the Task in
/// `in_progress` with a `clear` condition and no run once the Agent resumed.
/// The release is the owner's decision on that run: it now marks it
/// resumable and the Task runs again when its Agent can take it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_task_released_after_a_crash_under_a_paused_agent_runs_again() {
    use Action::{Create, PauseAgent, Take};
    run_cases(vec![(
        "hold, crash and release under a paused Agent".to_owned(),
        vec![
            step(Create),
            step(PauseAgent),
            crash_after(Take(0, "hold")),
            step(Take(0, "release")),
        ],
    )])
    .await;
}

/// Reached by `FORGE_MODEL_SEED=974622785` on a slow CI runner and fixed;
/// the sequence below is that seed's without its crash. The crash only chose
/// the history: on the runner the lead's planning run was committed before
/// the crash, so the root went on to coordinate and subtask 2 started; on a
/// fast host the crash wins and the seed never gets there.
///
/// The owner approves the running subtask twice. A user acts under the
/// Project workflow, so the first approval moves the subtask to `review`
/// (a state the inherited subtask workflow does not have), where it parks
/// with nothing to review; the second passes that gate into `merging`. A
/// subtask does not merge: the `run_merge` entry answered `Skipped`, and the
/// dispatcher's admission sweep takes root Tasks only, so the subtask rested
/// in `merging` with a `clear` condition, no step and no park. It now ends
/// `done` there (its work lands with the root's merge), from the entry and
/// from any later dispatcher pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_subtask_approved_into_the_merge_state_finishes() {
    use Action::{AdvanceClock, AssignLead, CreateLeadRoot, Finish, Take};
    let name = "the owner approves a subtask through review into merging";
    let sequence = vec![
        step(CreateLeadRoot),
        step(AssignLead(0)),
        step(Finish(2, Outcome::UsageLimit)),
        step(Take(1, "cancel")),
        step(AdvanceClock),
        step(Finish(2, Outcome::Success)),
        step(Take(2, "approve")),
        step(Take(2, "approve")),
    ];
    let trace = run(&sequence, false)
        .await
        .unwrap_or_else(|failure| panic!("{}", report(name, &sequence, &failure)));
    // The sequence must walk the history it names, or it proves nothing.
    assert!(
        trace[5].contains("2:in_progress/running/")
            && trace[6].contains("-> approve:")
            && trace[6].contains("2:review/parked/failure(review_candidate_missing)")
            && trace[7].contains("-> approve:"),
        "{name}: the subtask did not reach its merge state\n{}",
        trace.join("\n")
    );
    assert!(
        trace[7].contains(" 2:done/settled/"),
        "{name}: the subtask did not finish\n{}",
        trace.join("\n")
    );
}

/// Reached by `FORGE_MODEL_LONG=1 FORGE_MODEL_SEED=951857209` and fixed;
/// minimized below. Task 3's review is rejected (scripted) while other Tasks
/// hold the coder's runs; it goes back to `in_progress` and stayed there with
/// a `clear` condition, no run, no deferral and no park. It was waiting for a
/// free run of its busy Agent: the dispatcher's capacity park
/// (`Reason::Capacity`, `task_dispatcher/next_step.rs`, `agent_full`) was a
/// scheduling decision nothing stored. For a Task that already started it is
/// now the visible schedule park `AgentCapacity`: the Task is `parked` on the
/// typed reason `capacity` with scope `agent`, and runs when a run ends. The
/// capacity park is code the parent, Agent and dependency waits did not
/// touch, so the stall predates them (read, not re-run on the older build).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_task_sent_back_by_review_under_a_busy_agent_shows_why_it_waits() {
    use Action::{Create, Finish, Offer, Verdict};
    run_cases(vec![(
        "review rejection while the coder is busy".to_owned(),
        vec![
            step(Create),
            step(Offer(0, 0)),
            step(Create),
            step(Create),
            step(Finish(2, Outcome::Fail)),
            step(Finish(2, Outcome::Success)),
            step(Create),
            step(Create),
            step(Create),
            step(Verdict(3, false)),
            step(Finish(1, Outcome::Conflict)),
        ],
    )])
    .await;
}

/// The case above, as CI minimized it when it failed there (next/v0.14
/// `a3a08e50`, a 4-core runner: run cap 2): "task step drain timed out
/// waiting for a check". Task 1 passes review, loses the merge race to a
/// moved target, is rebased and enters `review` a second time. By then the
/// dispatcher has given the freed run slot to the next Task, so the second
/// entry's CI run is queued behind a full machine. That is a wait the Task
/// states (`check`, phase `slot`) and the check worker ends when a run does;
/// it was the drain helper that kept sweeping for a slot nothing in a drain
/// can free (`TaskStepWorker::drain`, `services/src/worker_runtime/queue.rs`).
/// The drain now returns on a stated slot wait, and the settle phase proves
/// the second entry's CI runs and the Task merges once a run ends.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_task_rebased_after_merge_contention_runs_its_review_entry_again() {
    use Action::{Create, Finish, Verdict};
    run_cases(vec![(
        "second review entry after a mechanical rebase".to_owned(),
        vec![
            step(Create),
            step(Create),
            step(Create),
            step(Finish(2, Outcome::Fail)),
            step(Finish(2, Outcome::Success)),
            step(Create),
            step(Create),
            step(Create),
            step(Verdict(3, false)),
            step(Finish(1, Outcome::Conflict)),
        ],
    )])
    .await;
}

/// The check of a rebased commit, directly. Two Tasks branch from the same
/// target. The first merges; the second passed its review on the old target,
/// and the merge queue rebases it because the target moved. A check runs on
/// the rebased commit and settles before it merges (the queue's head check,
/// or the entry check of a second review when the review cannot carry), and
/// the Task merges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_second_review_entry_after_a_rebase_requests_runs_and_settles_its_check() {
    // Pinned: both Tasks run side by side and branch from the same target.
    PINNED_RUN_CAP
        .scope(Some(MODEL_RUN_CAP), second_review_entry_after_a_rebase())
        .await;
}

/// Found by this model, fixed. A subtask used to be accepted under a parent
/// that was already done, cancelled, in review or integrating; it then sat in
/// `todo` with a `clear` condition, was never scheduled and offered only
/// `start` and `cancel`. Creating it is now refused with
/// `SUBTASK_PARENT_CLOSED`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_subtask_of_a_settled_parent_is_scheduled_or_refused() {
    use Action::{Create, CreateChild, Finish};
    run_cases(vec![(
        "subtask under a finished parent".to_owned(),
        vec![
            step(Create),
            step(Finish(0, Outcome::Success)),
            step(CreateChild(0)),
        ],
    )])
    .await;
}

/// Found by a random run with `CreateChild` generated and fixed; minimized
/// below. A coordination root whose aggregate review check fails ends in
/// `review` parked on `review retry budget exhausted`. It used to have no
/// exit but `cancel`:
///
/// - `retry` moved the root to `in_progress`, parked it on "coordination root
///   <id> is not in its aggregate review state"
///   (`services/src/task_service/execution/guards.rs`), and `restart` put it
///   back, for ever. `retry` on a root is now review-only: it resets the
///   budget and re-runs the aggregate review in place
///   (`reset_task_retry_budget`, `services/src/task_service/execution/recovery.rs`).
/// - The owner's `approve` (override) moved the root to `merging`, where
///   integration sent it back: "fresh conformance review required before
///   integration". A manual pass was not review authority while a reviewer
///   Agent was assigned, for a root or for any other Task. It now is
///   (`lock_review_integration`, `db/src/review_conformance.rs`).
/// - A corrective subtask could not be added (`SUBTASK_PARENT_CLOSED` in
///   `review`) and `send_back` was not offered on an exhausted budget. It is
///   now (`root_review_reopen`, `services/src/task_actions.rs`): the root
///   goes back to its working state with a fresh budget, held, and accepts
///   the subtask; its release runs the subtask and the review after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_coordination_root_that_fails_aggregate_review_can_recover() {
    use Action::{Create, CreateChild, Finish};
    run_cases(vec![(
        "root fails aggregate review".to_owned(),
        vec![
            step(Create),
            step(Create),
            crash_after(Finish(1, Outcome::Fail)),
            step(CreateChild(1)),
            step(Finish(2, Outcome::CiFail)),
        ],
    )])
    .await;
}

/// Found by a random run with `CreateChild` generated and fixed with it;
/// minimized below. `cancel` was offered for a coordination root and refused
/// with `400 validation_error`: "the Task is settling a completed execution's
/// plan artifact; retry after publication". The refusal was
/// `ensure_plan_publication_transition_authority`
/// (`services/src/task_service/execution.rs`) in `cancel_task_with_options`
/// and in the subtask cancel cascade
/// (`services/src/workflow/actions/common.rs`), and the claim outlived
/// quiescence, so waiting did not clear it. Cancel now abandons the claim
/// (the prior plan is restored, staged files are removed) and proceeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_offered_cancel_is_accepted_while_a_plan_artifact_settles() {
    use Action::{Create, CreateChild, Finish, Offer, Verdict};
    run_cases(vec![(
        "cancel offered and refused on a root".to_owned(),
        vec![
            step(Create),
            step(Finish(0, Outcome::Success)),
            step(Create),
            step(Create),
            crash_after(Verdict(3, false)),
            step(CreateChild(2)),
            step(Finish(4, Outcome::Success)),
            step(Finish(4, Outcome::Success)),
            step(Offer(2, 0)),
        ],
    )])
    .await;
}

/// Found by a random run with `CreateChild` generated and fixed with it. A
/// subtask created under a held parent is accepted and never dispatched while
/// the hold lasts (`task_hierarchy::coordination_root_allows_child_dispatch`),
/// which is right, but it said nothing: it sat in `todo` with a `clear`
/// condition and offered `start` and `cancel`. The dispatcher's park for it
/// (`services/src/task_dispatcher/next_step.rs`) was a scheduling decision
/// nothing stored. It is now the visible schedule park `ParentWait`: the
/// subtask is `parked` on the typed reason `parent`, naming the parent and
/// the cause, and offers no `start`. The same run found that releasing the
/// held parent queued a role the root may not run and put the hold back;
/// the release now clears the hold and the subtask runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_subtask_of_a_held_parent_shows_why_it_waits() {
    use Action::{Create, CreateChild, Take};
    run_cases(vec![(
        "subtask under a held parent".to_owned(),
        vec![step(Create), step(Take(0, "hold")), step(CreateChild(0))],
    )])
    .await;
}
