//! The production object transfer over two server-owned checkouts of one
//! repository: real SQLite, real Git. The facts port, the routing owner and
//! `build_integration_worker` are exercised end to end with the real step
//! worker and check runner in `integration_steps::tests` (server-owned
//! target) and `crates/api/tests/daemon_workspace_roundtrip.rs` (daemon-owned
//! target, through the fake-daemon harness).
use super::*;
use crate::{
    integration_effects::EffectOwner,
    integration_worker::{
        ObjectTransferDirection, ObjectTransferEndpoint, ObjectTransferOutcome, ObjectTransferPort,
        ObjectTransferRelease, ObjectTransferRequest,
    },
};
use db::{IntegrationAttempt, IntegrationOwnerFence, IntegrationQueueRepo};
use std::path::{Path, PathBuf};

async fn git_at(path: &Path, args: &[&str]) -> String {
    let out = tokio::process::Command::new("git")
        .args(args)
        .current_dir(path)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .await
        .unwrap();
    assert!(
        out.status.success(),
        "{args:?}: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    String::from_utf8(out.stdout).unwrap().trim().to_owned()
}
async fn has(path: &Path, args: &[&str]) -> bool {
    git::command_output(path, args)
        .await
        .unwrap()
        .status
        .success()
}
async fn integration_refs(path: &Path) -> Vec<String> {
    git_at(
        path,
        &["for-each-ref", "--format=%(refname)", "refs/forge/"],
    )
    .await
    .lines()
    .map(str::to_owned)
    .collect()
}

struct Fixture {
    temp: tempfile::TempDir,
    db: Arc<SqliteDb>,
    transfer: OwnerObjectTransfer,
    fence: IntegrationOwnerFence,
    attempt: IntegrationAttempt,
    /// The default checkout (location `l`).
    repo: PathBuf,
    /// Another clone of the repository, where the Task works (location `c`).
    clone: PathBuf,
}
fn endpoint(location: &str) -> ObjectTransferEndpoint {
    ObjectTransferEndpoint {
        repo_location_id: location.into(),
        owner: EffectOwner::Server,
    }
}

async fn fixture() -> Fixture {
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    std::fs::create_dir(&repo).unwrap();
    git::init(&repo).await.unwrap();
    std::fs::write(repo.join("base"), "base\n").unwrap();
    git::commit_all(&repo, "initial").await.unwrap();
    git::checkout_branch(&repo, "main").await.unwrap();
    let clone = temp.path().join("clone");
    git_at(
        temp.path(),
        &[
            "clone",
            "-q",
            repo.to_str().unwrap(),
            clone.to_str().unwrap(),
        ],
    )
    .await;
    for (key, value) in [
        ("user.email", "test@forge.dev"),
        ("user.name", "Forge Test"),
    ] {
        git_at(&clone, &["config", key, value]).await;
    }
    let url = format!(
        "sqlite://{}?mode=rwc",
        temp.path().join("ports.sqlite").display()
    );
    let pool = db::create_sqlite_pool(&url).await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = Arc::new(SqliteDb::new(pool));
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO project(id,name,settings,workflow_definition,created_at,updated_at) VALUES('p','p','{}','{}',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO repo(id,project_id,name,local_path,default_branch,created_at,updated_at) VALUES('r','p','r',?,'main',?,?)").bind(repo.to_str()).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('t','p','t','merging',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,is_default,status,created_at,updated_at) VALUES('l','r','server',?,'primary_checkout',1,'ready',?,?)").bind(repo.to_str()).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,is_default,status,created_at,updated_at) VALUES('c','r','server',?,'managed_clone',0,'ready',?,?)").bind(clone.to_str()).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    let queue = db
        .create_or_get_integration_queue("r", "main")
        .await
        .unwrap();
    let attempt = db
        .admit_integration_attempt(IntegrationAttempt::new(
            Some(queue.id.clone()),
            "t".into(),
            "p".into(),
            "admit-t".into(),
            "merging".into(),
            0,
            1,
        ))
        .await
        .unwrap();
    let queue = db.integration_queue(&queue.id).await.unwrap().unwrap();
    db.claim_integration_queue(
        &queue.id,
        queue.revision,
        "ports-test",
        &now,
        "2099-01-01T00:00:00Z",
    )
    .await
    .unwrap();
    let fence = db
        .integration_owner_fence(&attempt.id)
        .await
        .unwrap()
        .unwrap();
    let bus = Arc::new(events::EventBus::default());
    let sink = Arc::new(crate::daemon_transport::ServerExecutionEventSink::new(
        db.clone(),
        bus.clone(),
        temp.path().join("events"),
    ));
    let daemons = Arc::new(DaemonConnectionRegistry::new(bus, sink));
    let server = Arc::new(ServerIntegrationOwner::new(db.clone()));
    let client = DaemonWorkspaceClient::new(daemons).with_receipts(db.clone());
    let fences = Arc::new(DaemonFences::new(db.clone(), client.clone()));
    let transfer = OwnerObjectTransfer::new(
        db.clone(),
        server,
        client,
        fences,
        &temp.path().join("staging"),
    );
    Fixture {
        db,
        transfer,
        fence,
        attempt,
        repo,
        clone,
        temp,
    }
}

impl Fixture {
    fn request(
        &self,
        direction: ObjectTransferDirection,
        have: &str,
        want: &str,
    ) -> ObjectTransferRequest {
        ObjectTransferRequest {
            fence: self.fence.clone(),
            direction,
            task: endpoint("c"),
            target: endpoint("l"),
            target_branch: "main".into(),
            have: vec![have.to_owned()],
            want: want.to_owned(),
            max_bytes: 256 * 1024 * 1024,
        }
    }
    fn key(&self, direction: ObjectTransferDirection) -> String {
        api_types::object_transfer_key(&self.attempt.id, self.fence.generation, direction)
    }
    fn staged(&self) -> usize {
        fn count(path: &Path) -> usize {
            std::fs::read_dir(path)
                .map(|entries| {
                    entries
                        .flatten()
                        .map(|entry| {
                            if entry.path().is_dir() {
                                count(&entry.path())
                            } else {
                                1
                            }
                        })
                        .sum()
                })
                .unwrap_or(0)
        }
        count(&self.temp.path().join("staging"))
    }
}

/// D2 checklist "Production `ObjectTransferPort`": both directions move the
/// exact commit between two clones in the owner-operation order, a repeat of
/// a key moves nothing, a transfer over the cap moves nothing, `release`
/// deletes the attempt's refs on both ends, and the start sweep removes the
/// leftovers of a crashed transfer and the refs of an attempt off the slot.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn objects_move_between_server_checkouts_and_release_and_sweep_leave_no_refs() {
    let f = fixture().await;
    let target_tip = git::get_current_sha(&f.repo).await.unwrap();
    std::fs::write(f.clone.join("candidate"), "candidate\n").unwrap();
    git::commit_all(&f.clone, "candidate").await.unwrap();
    let candidate = git::get_current_sha(&f.clone).await.unwrap();
    assert!(
        !has(
            &f.repo,
            &["cat-file", "-e", &format!("{candidate}^{{commit}}")]
        )
        .await
    );

    // Outbound: the candidate reaches the default checkout, under the key's
    // ref and nowhere else.
    let outbound = f.request(ObjectTransferDirection::Outbound, &target_tip, &candidate);
    let moved = f.transfer.transfer(outbound.clone()).await.unwrap();
    assert!(
        matches!(moved, ObjectTransferOutcome::Transferred { bytes } if bytes > 0),
        "{moved:?}"
    );
    let out_ref = format!(
        "refs/forge/integration/{}",
        f.key(ObjectTransferDirection::Outbound)
    );
    assert_eq!(git_at(&f.repo, &["rev-parse", &out_ref]).await, candidate);
    assert_eq!(git::get_current_sha(&f.repo).await.unwrap(), target_tip);
    assert_eq!(integration_refs(&f.repo).await, vec![out_ref.clone()]);
    assert_eq!(f.staged(), 0, "no bundle is left on the server");
    // The same key again: found on the receiver, nothing exported.
    assert_eq!(
        f.transfer.transfer(outbound.clone()).await.unwrap(),
        ObjectTransferOutcome::Transferred { bytes: 0 }
    );
    // Over the caller's cap: refused before the receiver sees a byte.
    std::fs::write(f.repo.join("moved"), "moved\n").unwrap();
    git::commit_all(&f.repo, "moved").await.unwrap();
    let moved_tip = git::get_current_sha(&f.repo).await.unwrap();
    let mut capped = f.request(ObjectTransferDirection::Inbound, &candidate, &moved_tip);
    capped.max_bytes = 1;
    assert!(matches!(
        f.transfer.transfer(capped).await.unwrap(),
        ObjectTransferOutcome::TooLarge { bytes } if bytes > 1
    ));
    assert!(integration_refs(&f.clone).await.is_empty());
    assert_eq!(f.staged(), 0);
    // Inbound: the moved target tip reaches the Task's clone.
    let inbound = f.request(ObjectTransferDirection::Inbound, &candidate, &moved_tip);
    assert!(matches!(
        f.transfer.transfer(inbound).await.unwrap(),
        ObjectTransferOutcome::Transferred { bytes } if bytes > 0
    ));
    let in_ref = format!(
        "refs/forge/integration/{}",
        f.key(ObjectTransferDirection::Inbound)
    );
    assert_eq!(git_at(&f.clone, &["rev-parse", &in_ref]).await, moved_tip);
    // A claim that is no longer the attempt's fence moves nothing.
    let mut stale = outbound.clone();
    stale.fence.generation += 1;
    assert!(f.transfer.transfer(stale).await.is_err());

    // Release: every ref of the attempt, on both ends. Idempotent.
    let release = ObjectTransferRelease {
        attempt_id: f.attempt.id.clone(),
        task: endpoint("c"),
        target: endpoint("l"),
    };
    f.transfer.release(release.clone()).await.unwrap();
    assert!(integration_refs(&f.repo).await.is_empty());
    assert!(integration_refs(&f.clone).await.is_empty());
    f.transfer.release(release).await.unwrap();
    assert_eq!(git::get_current_sha(&f.repo).await.unwrap(), moved_tip);
    assert_eq!(git::get_current_sha(&f.clone).await.unwrap(), candidate);

    // The start sweep. The attempt holds the slot: its import is kept (a
    // resumed head finds it by key). Leftovers of a crashed transfer go.
    f.transfer.transfer(outbound).await.unwrap();
    git_at(
        &f.repo,
        &["update-ref", "refs/forge/export/crashed", &moved_tip],
    )
    .await;
    let quarantine = f.repo.join(".git").join("forge-incoming-crashed");
    std::fs::create_dir_all(quarantine.join("pack")).unwrap();
    // A ref of an attempt that no longer exists.
    git_at(
        &f.repo,
        &[
            "update-ref",
            "refs/forge/integration/gone-attempt-3-in",
            &moved_tip,
        ],
    )
    .await;
    f.transfer.sweep_at_start().await.unwrap();
    assert_eq!(integration_refs(&f.repo).await, vec![out_ref.clone()]);
    assert!(!quarantine.exists());
    // Off the slot: its refs go at the next start.
    sqlx::query("UPDATE integration_queue SET head_attempt_id=NULL")
        .execute(f.db.pool())
        .await
        .unwrap();
    f.transfer.sweep_at_start().await.unwrap();
    assert!(integration_refs(&f.repo).await.is_empty());
    assert_eq!(
        git_at(&f.repo, &["rev-parse", "refs/heads/main"]).await,
        moved_tip
    );
    assert!(
        has(
            &f.repo,
            &["cat-file", "-e", &format!("{candidate}^{{commit}}")]
        )
        .await
    );
}

/// The daemon fence announcement names every queue the daemon may still be
/// asked about: those that target it and those with an outstanding intent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_live_queue_list_covers_targets_and_outstanding_intents() {
    let f = fixture().await;
    let now = db::now_rfc3339();
    // A queue whose target is a daemon-owned checkout, and a closed one.
    for (id, state) in [
        ("q-live", "open"),
        ("q-suspended", "suspended"),
        ("q-closed", "closed"),
    ] {
        sqlx::query("INSERT INTO repo(id,project_id,name,default_branch,created_at,updated_at) VALUES(?,'p',?,'main',?,?)")
            .bind(format!("repo-{id}")).bind(id).bind(&now).bind(&now).execute(f.db.pool()).await.unwrap();
        let queue =
            f.db.create_or_get_integration_queue(&format!("repo-{id}"), "main")
                .await
                .unwrap();
        sqlx::query("UPDATE integration_queue SET id=?,state=?,target_owner_json=? WHERE id=?")
            .bind(id)
            .bind(state)
            .bind(serde_json::json!({"owner_kind":"daemon","daemon_id":"d-1","runtime_id":"rt-1","location_id":"x"}).to_string())
            .bind(&queue.id)
            .execute(f.db.pool())
            .await
            .unwrap();
    }
    let bus = Arc::new(events::EventBus::default());
    let sink = Arc::new(crate::daemon_transport::ServerExecutionEventSink::new(
        f.db.clone(),
        bus.clone(),
        f.temp.path().join("events-2"),
    ));
    let client = DaemonWorkspaceClient::new(Arc::new(DaemonConnectionRegistry::new(bus, sink)))
        .with_receipts(f.db.clone());
    let fences = DaemonFences::new(f.db.clone(), client);
    assert_eq!(
        fences.live_queue_ids("d-1", "q-asking").await.unwrap(),
        vec![
            "q-asking".to_owned(),
            "q-live".to_owned(),
            "q-suspended".to_owned()
        ]
    );
    assert_eq!(
        fences.live_queue_ids("d-other", "q-asking").await.unwrap(),
        vec!["q-asking".to_owned()]
    );
    // An unreachable daemon: the announcement fails and nothing is recorded
    // as announced, so the effect that needed it is never begun.
    assert!(fences.announce("d-1", "rt-1", &f.fence).await.is_err());
}
