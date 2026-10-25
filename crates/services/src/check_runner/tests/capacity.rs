//! Check admission, capacity reservations, and aged run slot waiters.

use super::*;

#[tokio::test]
async fn a_queued_check_gets_the_next_freed_slot_and_executions_keep_one() {
    let (_temp, store, runner) = fixture().await;
    store.server_run_cap.set(Some(2), 2, "server-machine");
    let mut stream = SlotStream {
        store: store.clone(),
        started: 0,
    };
    // The machine is full of new Tasks' runs and more are waiting.
    assert!(stream.dispatch().await);
    assert!(stream.dispatch().await);
    assert!(!stream.dispatch().await);
    // A Task reaches review: its entry check is queued behind the full machine.
    let first = queue_check(&runner, "first", CheckConsumerOrigin::Entry).await;
    assert!(stream.try_admit(&first).await.is_none());
    assert_eq!(stream.capacity().await.queued_checks, 1);

    // One slot release. The dispatcher asks first and is refused: the slot
    // is kept for the queued check, which takes it.
    assert!(stream.finish_oldest_execution().await);
    assert!(stream.capacity().await.has_capacity());
    assert!(!stream.dispatch().await, "a new run took the check's slot");
    let first = stream
        .try_admit(&first)
        .await
        .expect("the queued check runs after one release");

    // Two more checks queue while the first runs: an integration head check
    // that is older than a review-entry check. Entry goes first.
    let head = queue_check(
        &runner,
        "integration-head",
        CheckConsumerOrigin::Integration,
    )
    .await;
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let entry = queue_check(&runner, "second-entry", CheckConsumerOrigin::Entry).await;
    assert!(!stream.dispatch().await);
    let before = stream.started;

    // Release 1 (a run ends). Checks already hold their share (all but one
    // slot) and a Task waits: the slot goes to an execution.
    assert!(stream.finish_oldest_execution().await);
    assert!(stream.try_admit(&head).await.is_none());
    assert!(stream.try_admit(&entry).await.is_none());
    assert!(stream.dispatch().await, "executions lost their one slot");
    assert!(!stream.dispatch().await);

    // Release 2 (the running check ends). The slot is kept from the stream
    // and goes to the queue's head, which is the entry check although the
    // sweep reaches the integration check first.
    finish_check(&store, &first).await;
    assert!(!stream.dispatch().await, "a new run took the check's slot");
    assert!(stream.try_admit(&head).await.is_none());
    let entry = stream.try_admit(&entry).await.expect("entry check first");
    assert!(stream.try_admit(&head).await.is_none());

    // Release 3 (a run ends): an execution again.
    assert!(stream.finish_oldest_execution().await);
    assert!(stream.try_admit(&head).await.is_none());
    assert!(stream.dispatch().await);
    assert!(!stream.dispatch().await);

    // Release 4 (the entry check ends): the integration head check.
    finish_check(&store, &entry).await;
    assert!(!stream.dispatch().await);
    let head = stream.try_admit(&head).await.expect("then the head check");

    // No check queued: every freed slot is an execution's again.
    finish_check(&store, &head).await;
    assert!(stream.dispatch().await);
    assert!(stream.finish_oldest_execution().await);
    assert!(stream.dispatch().await);
    assert_eq!(stream.started, before + 4);
    let capacity = stream.capacity().await;
    assert_eq!((capacity.active_runs(), capacity.queued_checks), (2, 0));
}

/// One slot: the queued check takes it before any new run, and the run that
/// follows the check is admitted as soon as no check is queued.
#[tokio::test]
async fn with_one_slot_the_queued_check_runs_before_the_next_new_run() {
    let (_temp, store, runner) = fixture().await;
    store.server_run_cap.set(Some(1), 1, "server-machine");
    let stream = SlotStream {
        store: store.clone(),
        started: 0,
    };
    let one =
        |capacity: db::machine_capacity::MachineCapacity| db::machine_capacity::MachineCapacity {
            max_concurrent_runs: Some(1),
            ..capacity
        };
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('run','p','run','in_progress',?,?)").bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES('run','run','coder','running',?,?)").bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    let check = queue_check(&runner, "entry", CheckConsumerOrigin::Entry).await;
    assert!(stream.try_admit(&check).await.is_none());
    assert!(stream.finish_oldest_execution().await);
    let free = one(stream.capacity().await);
    assert!(free.has_capacity() && !free.admits_execution());
    let check = stream
        .try_admit(&check)
        .await
        .expect("the check takes the one slot");
    assert!(!one(stream.capacity().await).admits_execution());
    finish_check(&store, &check).await;
    assert!(one(stream.capacity().await).admits_execution());
}

/// A queued check reserves a slot only while somebody can still use its
/// result. The consumer of a Task that was cancelled or left `review` is
/// cancelled when the worker next looks at the run; until then the run must
/// not keep a free slot from executions.
#[tokio::test]
async fn a_queued_check_whose_task_left_review_reserves_no_slot() {
    let (_temp, store, runner) = fixture().await;
    store.server_run_cap.set(Some(2), 2, "server-machine");
    let stream = SlotStream {
        store: store.clone(),
        started: 0,
    };
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('run','p','run','in_progress',?,?)").bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES('run','run','coder','running',?,?)").bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    queue_check(&runner, "left-review", CheckConsumerOrigin::Entry).await;
    let reserved = stream.capacity().await;
    assert_eq!(reserved.queued_checks, 1);
    assert!(reserved.has_capacity() && !reserved.admits_execution());
    // The Task leaves the status it asked from (cancel, hold, a new entry).
    sqlx::query("UPDATE task SET status_epoch=status_epoch+1 WHERE id='t'")
        .execute(store.pool())
        .await
        .unwrap();
    let released = stream.capacity().await;
    assert_eq!(released.queued_checks, 0);
    assert!(released.admits_execution());
    // The same for a deleted Task.
    sqlx::query("UPDATE task SET status_epoch=status_epoch-1 WHERE id='t'")
        .execute(store.pool())
        .await
        .unwrap();
    assert_eq!(stream.capacity().await.queued_checks, 1);
    sqlx::query("UPDATE task SET deleted_at=? WHERE id='t'")
        .bind(&now)
        .execute(store.pool())
        .await
        .unwrap();
    assert!(stream.capacity().await.admits_execution());
}

/// Rule 3 (checks stay within their share while a Task waits for a run
/// slot) counts only Tasks that really wait for a slot this machine could
/// give: not a readiness or reconnect wait that names the machine, not a
/// Task that holds a slot, not a Task whose worktree is on another machine.
#[tokio::test]
async fn only_real_run_slot_waiters_hold_checks_to_their_share() {
    let (temp, store, _runner) = fixture().await;
    let now = db::now_rfc3339();
    let waiters = |machine: Option<&'static str>| {
        let store = store.clone();
        async move {
            let mut tx = db::begin_immediate(store.pool()).await.unwrap();
            db::machine_capacity::run_slot_waiters(&mut tx, machine, "server-machine")
                .await
                .unwrap()
        }
    };
    for id in ["named", "holder", "placed", "waiter"] {
        sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES(?,'p','w','in_progress',?,?)").bind(id).bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    }
    // Waits for the machine's readiness, not for a slot.
    store
        .schedule_wait("named", ("p", false), None, Some("server-daemon"), None)
        .await
        .unwrap();
    assert!(!waiters(None).await);
    assert!(!waiters(Some("server-daemon")).await);
    // Holds a slot itself.
    sqlx::query("INSERT INTO execution(id,task_id,role,status,created_at,updated_at) VALUES('holder','holder','interactive','running',?,?)").bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    store
        .schedule_wait("holder", ("p", false), None, Some("*"), None)
        .await
        .unwrap();
    assert!(!waiters(None).await);
    // Its worktree is on the server: a slot on another machine is no use.
    sqlx::query("INSERT INTO workspace(id,task_id,repo_id,worktree_path,branch,status,created_at,updated_at) VALUES('w','placed','r',?,'task/branch','ready',?,?)").bind(temp.path().to_str()).bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,is_default,status,created_at,updated_at) VALUES('l','r','server',?,'primary_checkout',1,'ready',?,?)").bind(temp.path().to_str()).bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    sqlx::query("INSERT INTO workspace_placement(id,workspace_id,task_id,owner_kind,repo_location_id,workspace_handle,generation,state,selected_by,selection_reason,created_at,updated_at) VALUES('pl','w','placed','server','l',?,1,'ready','scheduler','{}',?,?)").bind(temp.path().to_str()).bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    store
        .schedule_wait("placed", ("p", false), None, Some("*"), None)
        .await
        .unwrap();
    assert!(!waiters(Some("another-machine")).await);
    assert!(waiters(None).await);
    sqlx::query("DELETE FROM task_schedule_wait WHERE task_id='placed'")
        .execute(store.pool())
        .await
        .unwrap();
    // No worktree yet: whichever machine has room.
    store
        .schedule_wait("waiter", ("p", false), None, Some("*"), None)
        .await
        .unwrap();
    assert!(waiters(None).await && waiters(Some("another-machine")).await);
}

/// Slot-wait aging. Without it the Task below is refused at every release
/// for as long as checks keep queuing (rule 1 keeps the freed slot for the
/// queued check). After three releases it was woken for and refused, it is
/// admitted ahead of the check queued since; the cap holds throughout and
/// the check runs as soon as a slot is free again.
#[tokio::test]
async fn a_task_waiting_for_a_second_slot_is_admitted_ahead_of_newer_checks_once_aged() {
    let (temp, store, runner) = fixture().await;
    store.server_run_cap.set(Some(2), 2, "server-machine");
    let stream = SlotStream {
        store: store.clone(),
        started: 0,
    };
    let holder = SecondSlotTask::new(&store, &temp).await;
    let mut running = admit(
        &store,
        &queue_check(&runner, "check-0", CheckConsumerOrigin::Entry).await,
    )
    .await;
    assert!(!holder.dispatch().await, "the machine is full");
    // The Task cannot age its own wait: marks it receives for its own
    // writes are not releases, and neither is a refused attempt.
    for _ in 0..5 {
        sqlx::query("INSERT INTO task_schedule_dirty(task_id,external) VALUES('holder',1) ON CONFLICT(task_id) DO UPDATE SET generation=generation+1,dirty=1,external=1").execute(store.pool()).await.unwrap();
        assert!(!holder.dispatch().await);
    }
    assert!(!holder.aged().await, "a Task aged its own wait");
    // Releases 1 and 2: a check queued meanwhile takes the freed slot.
    for round in 1..=2 {
        tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        let next = queue_check(
            &runner,
            &format!("check-{round}"),
            CheckConsumerOrigin::Entry,
        )
        .await;
        finish_check(&store, &running).await;
        assert!(!holder.aged().await, "aged after {round} release(s)");
        assert!(
            !holder.dispatch().await,
            "release {round}: the slot is kept for the queued check"
        );
        running = stream
            .try_admit(&next)
            .await
            .expect("the queued check takes the slot");
    }
    // Release 3: the wait is aged. The check queued since stands back even
    // when the check runner asks first, and the Task is admitted.
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let newer = queue_check(&runner, "check-3", CheckConsumerOrigin::Entry).await;
    finish_check(&store, &running).await;
    assert!(holder.aged().await);
    assert!(
        stream.try_admit(&newer).await.is_none(),
        "a newly queued check took the aged waiter's slot"
    );
    assert!(holder.dispatch().await, "the aged waiter is admitted");
    assert_eq!(stream.capacity().await.active_runs(), 2);
    assert!(stream.try_admit(&newer).await.is_none(), "cap exceeded");
    // The check is not starved: it runs at the next release.
    sqlx::query("UPDATE execution SET status='completed' WHERE id='coder-run'")
        .execute(store.pool())
        .await
        .unwrap();
    assert!(stream.try_admit(&newer).await.is_some());
    assert_eq!(stream.capacity().await.active_runs(), 2);
}

/// What aging does not do: an aged waiter stays behind a check that was
/// queued before it began to wait, and a check stands back for an aged
/// waiter for a bounded time only. The age survives the dispatch attempt's
/// gap and starts afresh after it.
#[tokio::test]
async fn an_aged_waiter_stays_behind_older_checks_and_holds_newer_ones_for_a_bounded_time() {
    let (temp, store, runner) = fixture().await;
    store.server_run_cap.set(Some(2), 2, "server-machine");
    let stream = SlotStream {
        store: store.clone(),
        started: 0,
    };
    let holder = SecondSlotTask::new(&store, &temp).await;
    let running = admit(
        &store,
        &queue_check(&runner, "running", CheckConsumerOrigin::Entry).await,
    )
    .await;
    let older = queue_check(&runner, "older", CheckConsumerOrigin::Entry).await;
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    assert!(!holder.dispatch().await);
    assert!(!holder.aged().await);
    // Two minutes of waiting age it as three releases do.
    sqlx::query("UPDATE task_slot_wait_age SET since=strftime('%Y-%m-%dT%H:%M:%fZ','now','-119 seconds') WHERE task_id='holder'").execute(store.pool()).await.unwrap();
    assert!(!holder.aged().await);
    sqlx::query("UPDATE task_slot_wait_age SET since=strftime('%Y-%m-%dT%H:%M:%fZ','now','-121 seconds'), releases=0 WHERE task_id='holder'").execute(store.pool()).await.unwrap();
    assert!(holder.aged().await);
    // Put the start of the wait back after the older check, aged by count.
    sqlx::query("UPDATE task_slot_wait_age SET since=strftime('%Y-%m-%dT%H:%M:%fZ','now'), releases=3 WHERE task_id='holder'").execute(store.pool()).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(5)).await;
    let newer = queue_check(&runner, "newer", CheckConsumerOrigin::Entry).await;

    // A release: the check that was queued first goes first.
    finish_check(&store, &running).await;
    assert!(
        !holder.dispatch().await,
        "an aged waiter jumped a check older than its wait"
    );
    assert!(stream.try_admit(&newer).await.is_none());
    let older = stream.try_admit(&older).await.expect("older check first");

    // The next release is the waiter's; the newer check stands back.
    finish_check(&store, &older).await;
    assert!(stream.try_admit(&newer).await.is_none());
    let yielded: Option<String> =
        sqlx::query_scalar("SELECT yielded_since FROM check_run WHERE id=?")
            .bind(&newer.id)
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert!(yielded.is_some());
    // The bound is on the check, not per waiter. Nine more aged waiters on
    // this machine (subtasks that share the holder's worktree), each ahead
    // of the check: it stands back for them too, and the moment it first
    // stood back does not move, so ten waiters cost it the same 90 seconds
    // as one.
    for n in 0..9 {
        let id = format!("waiter-{n}");
        sqlx::query("INSERT INTO task(id,project_id,title,status,parent_task_id,created_at,updated_at) VALUES(?,'p',?,'in_progress','holder',?,?)")
            .bind(&id).bind(&id).bind(db::now_rfc3339()).bind(db::now_rfc3339())
            .execute(store.pool()).await.unwrap();
        store
            .schedule_wait(&id, ("p", false), None, Some("*"), None)
            .await
            .unwrap();
    }
    sqlx::query("UPDATE task_slot_wait_age SET releases=3, since=(SELECT since FROM task_slot_wait_age WHERE task_id='holder') WHERE task_id LIKE 'waiter-%'").execute(store.pool()).await.unwrap();
    {
        let mut tx = db::begin_immediate(store.pool()).await.unwrap();
        assert_eq!(
            db::machine_capacity::aged_waiters_ahead(&mut tx, &newer.id, None, "server-machine")
                .await
                .unwrap(),
            10
        );
    }
    for _ in 0..3 {
        assert!(stream.try_admit(&newer).await.is_none());
    }
    let still: Option<String> =
        sqlx::query_scalar("SELECT yielded_since FROM check_run WHERE id=?")
            .bind(&newer.id)
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(still, yielded, "another waiter restarted the check's bound");
    // The waiter does not take the slot. After 90 seconds of standing back
    // the check takes it: a waiter that cannot run never starves a check.
    sqlx::query("UPDATE check_run SET yielded_since=? WHERE id=?")
        .bind((chrono::Utc::now() - chrono::Duration::seconds(91)).to_rfc3339())
        .bind(&newer.id)
        .execute(store.pool())
        .await
        .unwrap();
    assert!(stream.try_admit(&newer).await.is_some());
    assert_eq!(stream.capacity().await.active_runs(), 2);

    // The wait row dropped by a dispatch attempt keeps its age for 30 s.
    store
        .schedule_wait("holder", ("p", false), None, None, None)
        .await
        .unwrap();
    assert!(holder.aged().await);
    store
        .schedule_wait("holder", ("p", false), None, Some("*"), None)
        .await
        .unwrap();
    assert!(holder.aged().await, "the gap reset the age");
    // A wait that ended longer ago starts afresh.
    store
        .schedule_wait("holder", ("p", false), None, None, None)
        .await
        .unwrap();
    sqlx::query("UPDATE task_slot_wait_age SET lapsed_at=strftime('%Y-%m-%dT%H:%M:%fZ','now','-31 seconds') WHERE task_id='holder'").execute(store.pool()).await.unwrap();
    assert!(!holder.aged().await);
    store
        .schedule_wait("holder", ("p", false), None, Some("*"), None)
        .await
        .unwrap();
    assert!(!holder.aged().await);
    let releases: i64 =
        sqlx::query_scalar("SELECT releases FROM task_slot_wait_age WHERE task_id='holder'")
            .fetch_one(store.pool())
            .await
            .unwrap();
    assert_eq!(releases, 0);
}

/// A check that stops holding or reserving a slot is a release for the Tasks
/// that wait for a run slot: its end, the cancellation of a queued one, and
/// its last consumer giving up each mark those waiters. Before, only the end
/// of an execution did, and a slot a check freed could stay unused.
#[tokio::test]
async fn a_check_that_frees_a_slot_marks_the_run_slot_waiters() {
    let (_temp, store, runner) = fixture().await;
    store.server_run_cap.set(Some(1), 1, "server-machine");
    let now = db::now_rfc3339();
    for id in ["slot", "named", "agent"] {
        sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES(?,'p','waiter','todo',?,?)").bind(id).bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    }
    store
        .schedule_wait("slot", ("p", false), None, Some("*"), None)
        .await
        .unwrap();
    store
        .schedule_wait("named", ("p", false), None, Some("another-machine"), None)
        .await
        .unwrap();
    store
        .schedule_wait("agent", ("p", false), Some("someone"), None, None)
        .await
        .unwrap();
    let marked = || async {
        let ids = store.dirty_schedule_tasks(100).await.unwrap();
        sqlx::query("DELETE FROM task_schedule_dirty")
            .execute(store.pool())
            .await
            .unwrap();
        ids.into_iter()
            .filter(|id| ["slot", "named", "agent"].contains(&id.as_str()))
            .collect::<Vec<_>>()
    };
    marked().await;

    // Queued, then admitted: it takes a slot, which frees nothing.
    let running = queue_check(&runner, "runs", CheckConsumerOrigin::Entry).await;
    let running = admit(&store, &running).await;
    assert!(marked().await.is_empty());
    // It ends.
    finish_check(&store, &running).await;
    assert_eq!(marked().await, ["slot"]);

    // A queued check keeps the slot from executions. Its consumer gives up.
    let queued = queue_check(&runner, "queued", CheckConsumerOrigin::Entry).await;
    assert!(marked().await.is_empty());
    sqlx::query("UPDATE check_consumer SET cancelled_at=? WHERE run_id=?")
        .bind(db::now_rfc3339())
        .bind(&queued.id)
        .execute(store.pool())
        .await
        .unwrap();
    assert_eq!(marked().await, ["slot"]);
    // The queued run itself is cancelled, and later deleted with its Project.
    sqlx::query(
        "UPDATE check_run SET state='cancelled',finished_at=?,version=version+1 WHERE id=?",
    )
    .bind(db::now_rfc3339())
    .bind(&queued.id)
    .execute(store.pool())
    .await
    .unwrap();
    assert_eq!(marked().await, ["slot"]);
    sqlx::query("DELETE FROM check_run WHERE id=?")
        .bind(&queued.id)
        .execute(store.pool())
        .await
        .unwrap();
    assert!(marked().await.is_empty(), "a settled run frees nothing");
    let last = queue_check(&runner, "deleted", CheckConsumerOrigin::Entry).await;
    marked().await;
    sqlx::query("DELETE FROM check_run WHERE id=?")
        .bind(&last.id)
        .execute(store.pool())
        .await
        .unwrap();
    assert_eq!(marked().await, ["slot"]);
}

/// Every run this worker dispatches executes in its Task's existing
/// worktree, so the disk floor never holds one back: a wait here would keep
/// a reviewed Task from finishing (which is how its worktree is given back)
/// and would end, after the 30-minute queued-run expiry, as an
/// infrastructure failure. Under the floor the check is admitted at once,
/// never waits for capacity, and runs.
#[tokio::test]
async fn a_check_in_an_existing_worktree_is_admitted_under_the_disk_floor() {
    let (_temp, store, runner) = fixture().await;
    store.disk_admission.configure(
        api_types::DiskFloor::of_bytes(100, 0),
        Arc::new(|| {
            Some(api_types::MachineDiskFacts {
                free_bytes: 10,
                total_bytes: 1_000,
                free_inodes: None,
                total_inodes: None,
                measured_at: db::now_rfc3339(),
                gc_state: None,
                compiler_cache_bytes: None,
            })
        }),
    );
    assert_eq!(
        store.disk_admission.server_pressure(),
        Some(api_types::DiskPressureKind::Bytes)
    );
    let run = scheduled(runner.request(cacheable_request("disk-a")).await.unwrap());
    let admitted = admit(&store, &run).await;
    let record = store.check_worker_record(&run.id).await.unwrap();
    assert!(record.admitted_at.is_some(), "admitted, not expired");
    assert_eq!(
        store.check_run_counts().await.unwrap().waiting_for_capacity,
        0
    );
    let owner = Arc::new(TestOwner::default());
    CheckRunWorker::new(store.clone(), owner.clone())
        .drive(admitted)
        .await
        .unwrap();
    assert_eq!(owner.runs.load(Ordering::SeqCst), 1);
    let finished = store.check_run(&run.id).await.unwrap().unwrap().state;
    assert!(
        !matches!(
            finished,
            CheckRunState::Failed | CheckRunState::Cancelled | CheckRunState::Queued
        ),
        "{finished:?}"
    );
}

#[tokio::test]
async fn a_check_borrows_the_same_tasks_reservation_and_keeps_its_slot_when_it_expires() {
    let (temp, store, runner) = fixture().await;
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO workspace(id,task_id,repo_id,worktree_path,branch,status,created_at,updated_at) VALUES('w','t','r',?,'task/branch','ready',?,?)").bind(temp.path().to_str()).bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,is_default,status,created_at,updated_at) VALUES('l','r','server',?,'primary_checkout',1,'ready',?,?)").bind(temp.path().to_str()).bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    sqlx::query("INSERT INTO workspace_placement(id,workspace_id,task_id,owner_kind,repo_location_id,workspace_handle,generation,state,selected_by,selection_reason,reserved_until,created_at,updated_at) VALUES('pl','w','t','server','l',?,1,'reserved','scheduler','{}','2099-01-01T00:00:00Z',?,?)").bind(temp.path().to_str()).bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    store.server_run_cap.set(Some(1), 1, "server-machine");
    let run = scheduled(runner.request(request("borrow")).await.unwrap());
    admit(&store, &run).await;
    let mut tx = db::begin_immediate(store.pool()).await.unwrap();
    let count =
        db::machine_capacity::count_machine_capacity(&mut tx, None, Some(1), "server-machine")
            .await
            .unwrap();
    assert_eq!(count.reservations, 1);
    assert_eq!(count.borrowed_check_runs, 1);
    assert_eq!(count.check_runs, 0);
    assert_eq!(count.active_runs(), 1);
    tx.commit().await.unwrap();
    sqlx::query(
        "UPDATE workspace_placement SET reserved_until='2000-01-01T00:00:00Z' WHERE id='pl'",
    )
    .execute(store.pool())
    .await
    .unwrap();
    let mut tx = db::begin_immediate(store.pool()).await.unwrap();
    let count =
        db::machine_capacity::count_machine_capacity(&mut tx, None, Some(1), "server-machine")
            .await
            .unwrap();
    assert_eq!(count.borrowed_check_runs, 0);
    assert_eq!(count.check_runs, 1);
    assert_eq!(count.active_runs(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn real_concurrent_capacity_admission_on_a_file_backed_database_has_one_winner() {
    let (_temp, store, runner) = fixture().await;
    store.server_run_cap.set(Some(1), 1, "server-machine");
    let mut candidates = Vec::new();
    for i in 1..=8 {
        let mut req = request(&format!("admission-{i}"));
        req.identity.commit_sha = format!("{i:040x}");
        candidates.push(scheduled(runner.request(req).await.unwrap()));
    }
    let barrier = Arc::new(tokio::sync::Barrier::new(8));
    let mut claims = Vec::new();
    for run in candidates {
        let (db, barrier) = (store.clone(), barrier.clone());
        claims.push(tokio::spawn(async move {
            barrier.wait().await;
            let now = db::now_rfc3339();
            let until = (chrono::Utc::now() + chrono::Duration::minutes(1)).to_rfc3339();
            db.admit_check_run(&run, "concurrent", &now, &until)
                .await
                .unwrap()
        }));
    }
    let mut winners = 0;
    for claim in claims {
        if matches!(claim.await.unwrap(), CheckAdmission::Admitted(_)) {
            winners += 1;
        }
    }
    assert_eq!(winners, 1);
    let counts = store.check_run_counts().await.unwrap();
    assert_eq!(counts.admitted_runs, 1);
    assert_eq!(counts.waiting_for_capacity, 7);
}

#[tokio::test]
async fn capacity_is_charged_to_the_owner_machine_and_not_to_an_agent_or_a_join() {
    let (_temp, store, runner) = fixture().await;
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO daemon(id,machine_id,hostname,os,arch,status,max_concurrent_runs,created_at,updated_at) VALUES('m','physical-m','owner','linux','aarch64','online',1,?,?)").bind(&now).bind(&now).execute(store.pool()).await.unwrap();
    let mut req = request("owner-capacity");
    req.machine_id = Some("m".into());
    let run = scheduled(runner.request(req.clone()).await.unwrap());
    admit(&store, &run).await;
    req.request_key = "joined-owner-capacity".into();
    assert!(matches!(
        runner.request(req).await.unwrap().outcome,
        CheckRequestOutcome::Joined(_)
    ));
    let mut tx = db::begin_immediate(store.pool()).await.unwrap();
    let remote =
        db::machine_capacity::count_machine_capacity(&mut tx, Some("m"), Some(1), "server-machine")
            .await
            .unwrap();
    let server =
        db::machine_capacity::count_machine_capacity(&mut tx, None, Some(1), "server-machine")
            .await
            .unwrap();
    let agent = db::machine_capacity::count_agent_capacity(&mut tx, "some-agent")
        .await
        .unwrap();
    assert_eq!(remote.active_runs(), 1);
    assert_eq!(remote.check_runs, 1);
    assert_eq!(server.active_runs(), 0);
    assert_eq!(agent.occupied_slots(), 0);
}
