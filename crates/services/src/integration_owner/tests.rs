//! Server owner fences, Git effects, object transfer, and recovery tests.
use super::*;
use db::{IntegrationAttempt, IntegrationQueueRepo};
struct Fixture {
    _temp: tempfile::TempDir,
    db: Arc<SqliteDb>,
    owner: ServerIntegrationOwner,
    workspace: EffectWorkspace,
    fence: IntegrationOwnerFence,
    repo: std::path::PathBuf,
    tree: std::path::PathBuf,
    head: String,
    target: String,
    cancel: CancellationToken,
}
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
async fn fixture() -> Fixture {
    fixture_with_storage(false).await
}
async fn fixture_with_storage(durable: bool) -> Fixture {
    let temp = tempfile::tempdir().unwrap();
    let repo = temp.path().join("repo");
    std::fs::create_dir(&repo).unwrap();
    git::init(&repo).await.unwrap();
    std::fs::write(repo.join("base"), "base\n").unwrap();
    git::commit_all(&repo, "initial").await.unwrap();
    let target = git::get_current_sha(&repo).await.unwrap();
    let tree = temp.path().join("tree");
    git_at(
        &repo,
        &["worktree", "add", "-b", "candidate", tree.to_str().unwrap()],
    )
    .await;
    std::fs::write(tree.join("candidate"), "candidate\n").unwrap();
    git::commit_all(&tree, "candidate").await.unwrap();
    let head = git::get_current_sha(&tree).await.unwrap();
    let url = if durable {
        format!(
            "sqlite://{}?mode=rwc",
            temp.path().join("owner.sqlite").display()
        )
    } else {
        "sqlite::memory:".to_owned()
    };
    let pool = db::create_sqlite_pool(&url).await.unwrap();
    db::run_migrations(&pool).await.unwrap();
    let db = Arc::new(SqliteDb::new(pool));
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO project(id,name,settings,workflow_definition,created_at,updated_at) VALUES('p','p','{}','{}',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO repo(id,project_id,name,local_path,default_branch,created_at,updated_at) VALUES('r','p','r',?,'main',?,?)").bind(repo.to_str()).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO task(id,project_id,title,status,created_at,updated_at) VALUES('t','p','t','merging',?,?)").bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,is_default,status,created_at,updated_at) VALUES('l','r','server',?,'primary_checkout',1,'ready',?,?)").bind(repo.to_str()).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO workspace(id,task_id,repo_id,worktree_path,branch,status,created_at,updated_at) VALUES('w','t','r',?,'candidate','ready',?,?)").bind(tree.to_str()).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    sqlx::query("INSERT INTO workspace_placement(id,workspace_id,task_id,owner_kind,repo_location_id,workspace_handle,generation,state,selected_by,selection_reason,created_at,updated_at) VALUES('pl','w','t','server','l',?,1,'ready','scheduler','{}',?,?)").bind(tree.to_str()).bind(&now).bind(&now).execute(db.pool()).await.unwrap();
    let q = db
        .create_or_get_integration_queue("r", "main")
        .await
        .unwrap();
    let a = db
        .admit_integration_attempt(IntegrationAttempt::new(
            Some(q.id.clone()),
            "t".into(),
            "p".into(),
            "owner-test".into(),
            "merging".into(),
            0,
            1,
        ))
        .await
        .unwrap();
    let q = db.integration_queue(&q.id).await.unwrap().unwrap();
    db.claim_integration_queue(
        &q.id,
        q.revision,
        "test-owner",
        &now,
        "2099-01-01T00:00:00Z",
    )
    .await
    .unwrap();
    let fence = db.integration_owner_fence(&a.id).await.unwrap().unwrap();
    let workspace = EffectWorkspace {
        workspace_id: "w".into(),
        placement_id: "pl".into(),
        generation: 1,
        owner: EffectOwner::Server,
        handle: tree.to_string_lossy().into_owned(),
    };
    let owner = ServerIntegrationOwner::new(db.clone());
    Fixture {
        _temp: temp,
        db,
        owner,
        workspace,
        fence,
        repo,
        tree,
        head,
        target,
        cancel: CancellationToken::new(),
    }
}
fn merge_request(f: &Fixture) -> ServerMergeRequest<'_> {
    ServerMergeRequest {
        cancel: &f.cancel,
        deadline: None,
        fence: &f.fence,
        workspace: &f.workspace,
        target_branch: "main",
        task_branch: "candidate",
        expected_head_sha: &f.head,
        expected_target_sha: &f.target,
        reviewed: Some(integration_effects::merge::ReviewedMergeObject {
            commit_sha: f.head.clone(),
            base_sha: f.target.clone(),
        }),
    }
}
/// A second server-owned clone of the fixture repository, one commit
/// (the candidate) behind the Task's checkout.
async fn second_location(f: &Fixture) -> std::path::PathBuf {
    let clone = f._temp.path().join("clone");
    git_at(
        f._temp.path(),
        &[
            "clone",
            "-q",
            "--branch",
            "main",
            f.repo.to_str().unwrap(),
            "clone",
        ],
    )
    .await;
    // A clone does not copy the source's local identity.
    for (key, value) in [
        ("user.email", "test@forge.dev"),
        ("user.name", "Forge Test"),
    ] {
        git_at(&clone, &["config", key, value]).await;
    }
    let now = db::now_rfc3339();
    sqlx::query("INSERT INTO repo_location(id,repo_id,owner_kind,path,kind,is_default,status,created_at,updated_at) VALUES('l2','r','server',?,'managed_clone',0,'ready',?,?)").bind(clone.to_str()).bind(&now).bind(&now).execute(f.db.pool()).await.unwrap();
    clone
}
async fn untouched(path: &Path) -> String {
    format!(
        "{}\n{}\n{}\n{}",
        git_at(path, &["for-each-ref"]).await,
        git_at(path, &["rev-parse", "HEAD"]).await,
        git_at(path, &["status", "--porcelain"]).await,
        git_at(path, &["count-objects", "-v"]).await
    )
}
#[tokio::test]
async fn server_owner_moves_exact_objects_between_checkouts_once_per_key() {
    let f = fixture().await;
    let clone = second_location(&f).await;
    let key = api_types::object_transfer_key(
        &f.fence.attempt_id,
        f.fence.generation,
        api_types::ObjectTransferDirection::Outbound,
    );
    let bundle = f._temp.path().join("out.bundle");
    let have = vec![f.target.clone()];
    let export_input = || ServerObjectExport {
        fence: &f.fence,
        key: &key,
        repo_location_id: "l",
        have: &have,
        want: &f.head,
        dest: &bundle,
        cancel: &f.cancel,
    };
    let before = untouched(&clone).await;
    assert_eq!(
        f.owner
            .imported_objects(&f.fence, "l2", &key, &f.head)
            .await
            .unwrap(),
        ObjectTransferOutcome::Done(None)
    );
    let ObjectTransferOutcome::Done(export) = f.owner.export_objects(export_input()).await.unwrap()
    else {
        panic!("export refused")
    };
    assert_eq!(export.tip_sha, f.head);
    assert!(export.total_bytes > 0);
    let import_input = |export, bundle| ServerObjectImport {
        fence: &f.fence,
        export,
        repo_location_id: "l2",
        bundle,
        cancel: &f.cancel,
    };
    // A bundle that does not match its receipt, an oversized receipt and a
    // corrupt bundle are refused with the target untouched.
    let mut wrong = export.clone();
    wrong.sha256 = "0".repeat(64);
    let mut huge = export.clone();
    huge.total_bytes = api_types::MAX_OBJECT_TRANSFER_BYTES + 1;
    let corrupt = f._temp.path().join("corrupt.bundle");
    let mut bytes = std::fs::read(&bundle).unwrap();
    let at = bytes.len() - 30;
    bytes[at] ^= 0xff;
    std::fs::write(&corrupt, &bytes).unwrap();
    let mut relabelled = export.clone();
    relabelled.sha256 = {
        use sha2::Digest;
        hex::encode(sha2::Sha256::digest(&bytes))
    };
    let missing = f._temp.path().join("missing.bundle");
    for (name, receipt, path) in [
        ("digest", &wrong, &bundle),
        ("size", &huge, &missing),
        ("corrupt", &relabelled, &corrupt),
    ] {
        let outcome = f
            .owner
            .import_objects(import_input(receipt, path))
            .await
            .unwrap();
        assert!(
            matches!(outcome, ObjectTransferOutcome::Refused(_)),
            "{name}: {outcome:?}"
        );
        assert_eq!(untouched(&clone).await, before, "{name}");
    }
    let cancelled = CancellationToken::new();
    cancelled.cancel();
    assert_eq!(
        f.owner
            .import_objects(ServerObjectImport {
                cancel: &cancelled,
                ..import_input(&export, &bundle)
            })
            .await
            .unwrap(),
        ObjectTransferOutcome::Cancelled
    );
    assert_eq!(untouched(&clone).await, before);
    // The sound transfer binds the Forge ref only.
    let head = git_at(&clone, &["rev-parse", "HEAD"]).await;
    let imported = f
        .owner
        .import_objects(import_input(&export, &bundle))
        .await
        .unwrap();
    let reference = format!("refs/forge/integration/{key}");
    assert_eq!(
        imported,
        ObjectTransferOutcome::Done(api_types::ObjectImportReceipt {
            key: key.clone(),
            tip_sha: f.head.clone(),
            ref_name: reference.clone(),
            replayed: false,
        })
    );
    assert_eq!(git_at(&clone, &["rev-parse", &reference]).await, f.head);
    assert_eq!(git_at(&clone, &["rev-parse", "HEAD"]).await, head);
    assert_eq!(
        git_at(&clone, &["rev-parse", "refs/heads/main"]).await,
        head
    );
    assert!(!clone.join("candidate").exists());
    // The duplicate key replays the receipt without the bundle.
    std::fs::remove_file(&bundle).unwrap();
    let after = untouched(&clone).await;
    let replay = f
        .owner
        .import_objects(import_input(&export, &bundle))
        .await
        .unwrap();
    assert!(
        matches!(&replay, ObjectTransferOutcome::Done(receipt) if receipt.replayed && receipt.tip_sha == f.head)
    );
    assert!(matches!(
        f.owner.imported_objects(&f.fence, "l2", &key, &f.head).await.unwrap(),
        ObjectTransferOutcome::Done(Some(receipt)) if receipt.replayed
    ));
    assert_eq!(untouched(&clone).await, after);
    // A cancelled export keeps no file; an old claim and a checkout this
    // server does not own are refused before Git.
    assert_eq!(
        f.owner
            .export_objects(ServerObjectExport {
                cancel: &cancelled,
                ..export_input()
            })
            .await
            .unwrap(),
        ObjectTransferOutcome::Cancelled
    );
    assert!(!bundle.exists());
    let mut stale = f.fence.clone();
    stale.generation += 1;
    assert_eq!(
        f.owner
            .export_objects(ServerObjectExport {
                fence: &stale,
                ..export_input()
            })
            .await
            .unwrap(),
        ObjectTransferOutcome::Refused(api_types::ObjectTransferRefusal::StaleFence)
    );
    assert!(matches!(
        f.owner
            .export_objects(ServerObjectExport {
                repo_location_id: "elsewhere",
                ..export_input()
            })
            .await
            .unwrap(),
        ObjectTransferOutcome::Refused(api_types::ObjectTransferRefusal::Invalid { .. })
    ));
    assert!(!bundle.exists());
}

#[test]
fn owner_receipt_validation_refuses_wrong_object_and_false_success() {
    let request = IntegrationEffectRequest {
        fence: IntegrationOwnerFence {
            queue_id: "q".into(),
            attempt_id: "a".into(),
            generation: 1,
            lease_owner: "worker".into(),
            target_owner: json!({}),
        },
        kind: IntegrationOperationKind::FastForward,
        witness: json!({"target_branch":"main","expected_head_sha":"approved"}),
    };
    let mut receipt = db::IntegrationEffectReceipt {
        request,
        result: json!({"kind":"completed","outcome":crate::MergeOutcome::Done { before_sha: "before".into(), after_sha: "foreign".into(), branch: "main".into() }}),
        operation_state: IntegrationOperationState::Succeeded,
        recorded_at: db::now_rfc3339(),
    };
    assert!(validate_attempt_receipt(&receipt).is_err());
    receipt.result["outcome"]["Done"]["after_sha"] = json!("approved");
    validate_attempt_receipt(&receipt).unwrap();
    receipt.operation_state = IntegrationOperationState::Failed;
    assert!(validate_attempt_receipt(&receipt).is_err());
}

/// The default checkout is on this server and the Task's checkout on
/// another machine: the candidate reaches the server only as the ref
/// this claim generation's outbound transfer binds. The fast-forward
/// takes exactly that commit, from exactly the witnessed tip, once.
#[tokio::test]
async fn a_server_owned_target_fast_forwards_only_the_commit_its_transfer_ref_binds() {
    #[derive(Clone, Copy, Debug, PartialEq)]
    enum Case {
        Green,
        WrongCommit,
        MissingRef,
        OlderGeneration,
        OtherAttempt,
        MovedTarget,
        DirtyTarget,
    }
    for case in [
        Case::Green,
        Case::WrongCommit,
        Case::MissingRef,
        Case::OlderGeneration,
        Case::OtherAttempt,
        Case::MovedTarget,
        Case::DirtyTarget,
    ] {
        let f = fixture().await;
        let away = EffectWorkspace {
            owner: EffectOwner::Daemon {
                daemon_id: "d".into(),
                runtime_id: "rt".into(),
            },
            ..f.workspace.clone()
        };
        let key = |attempt: &str, generation: i64| {
            api_types::object_transfer_key(
                attempt,
                generation,
                api_types::ObjectTransferDirection::Outbound,
            )
        };
        let own = key(&f.fence.attempt_id, f.fence.generation);
        // Another commit than the permitted candidate, also held here.
        std::fs::write(f.tree.join("unreviewed"), "unreviewed\n").unwrap();
        let unreviewed = git::commit_all(&f.tree, "unreviewed").await.unwrap();
        let bind = |key: String, sha: String| {
            let repo = f.repo.clone();
            async move {
                git_at(
                    &repo,
                    &["update-ref", &format!("refs/forge/integration/{key}"), &sha],
                )
                .await;
            }
        };
        match case {
            Case::WrongCommit => bind(own.clone(), unreviewed.clone()).await,
            Case::MissingRef => {}
            Case::OlderGeneration => {
                bind(
                    key(&f.fence.attempt_id, f.fence.generation - 1),
                    f.head.clone(),
                )
                .await
            }
            Case::OtherAttempt => {
                bind(key("another-attempt", f.fence.generation), f.head.clone()).await
            }
            _ => bind(own.clone(), f.head.clone()).await,
        }
        let mut tip = f.target.clone();
        if case == Case::MovedTarget {
            std::fs::write(f.repo.join("outside"), "outside\n").unwrap();
            tip = git::commit_all(&f.repo, "outside").await.unwrap();
        }
        if case == Case::DirtyTarget {
            std::fs::write(f.repo.join("base"), "edited by hand\n").unwrap();
        }
        let request = || ServerMergeRequest {
            workspace: &away,
            ..merge_request(&f)
        };
        let before = untouched(&f.repo).await;
        let receipt = f.owner.merge(request()).await.unwrap();
        match case {
            Case::Green => {
                assert!(
                    matches!(&receipt, OwnerMergeReceipt::Completed { outcome: crate::MergeOutcome::Done { after_sha, .. } } if *after_sha == f.head),
                    "{case:?}: {receipt:?}"
                );
                assert_eq!(git::get_current_sha(&f.repo).await.unwrap(), f.head);
                assert!(git::is_worktree_clean(&f.repo).await.unwrap());
                // A lost reply: the same request is answered by its
                // receipt and the branch is not written again.
                let reflog = git_at(&f.repo, &["reflog", "--all"]).await;
                let again = ServerIntegrationOwner::new(f.db.clone())
                    .merge(request())
                    .await
                    .unwrap();
                assert_eq!(json!(again), json!(receipt));
                assert_eq!(git_at(&f.repo, &["reflog", "--all"]).await, reflog);
                let moves = git_at(&f.repo, &["reflog", "show", "--format=%gs", "main"]).await;
                assert_eq!(
                    moves
                        .lines()
                        .filter(|line| line.contains("forge: fast-forward"))
                        .count(),
                    1,
                    "{moves}"
                );
            }
            Case::DirtyTarget => {
                assert!(
                    matches!(
                        &receipt,
                        OwnerMergeReceipt::Completed {
                            outcome: crate::MergeOutcome::TargetDirty { .. }
                        }
                    ),
                    "{case:?}: {receipt:?}"
                );
                assert_eq!(untouched(&f.repo).await, before);
            }
            _ => {
                assert!(
                    matches!(
                        &receipt,
                        OwnerMergeReceipt::Refused {
                            reason: OwnerEffectRefusal::WitnessMismatch
                        }
                    ),
                    "{case:?}: {receipt:?}"
                );
                assert_eq!(untouched(&f.repo).await, before);
                assert_eq!(
                    git_at(&f.repo, &["rev-parse", "refs/heads/main"]).await,
                    tip
                );
            }
        }
    }
}
/// The server stops in the middle of a transferred fast-forward (the
/// Task's checkout is on another machine). At the next start the intent
/// is settled from the default checkout alone: a branch that never moved
/// is "not performed", a branch at the candidate is the merge, proven
/// and not repeated.
#[tokio::test]
async fn a_server_restart_during_a_transferred_fast_forward_is_settled_from_the_checkout() {
    for landed in [false, true] {
        let f = fixture_with_storage(true).await;
        let away = EffectWorkspace {
            owner: EffectOwner::Daemon {
                daemon_id: "d".into(),
                runtime_id: "rt".into(),
            },
            ..f.workspace.clone()
        };
        let request = IntegrationEffectRequest {
            fence: f.fence.clone(),
            kind: IntegrationOperationKind::FastForward,
            witness: json!({"workspace":away,"target_branch":"main","task_branch":"candidate","expected_head_sha":f.head,"expected_target_sha":f.target,"reviewed":{"commit_sha":f.head,"base_sha":f.target}}),
        };
        let IntegrationEffectAdmission::Started(mut guard) =
            f.db.begin_integration_effect(request.clone())
                .await
                .unwrap()
        else {
            panic!("not admitted");
        };
        assert!(guard.start().await.unwrap().is_none());
        // The process dies here, before or after the branch moved.
        drop(guard);
        if landed {
            let applied = git::integration::fast_forward_exact(
                &f.repo,
                "main",
                &f.head,
                &f.target,
                git::integration::FastForwardLimits {
                    deadline: Duration::from_secs(30),
                    output_bytes: 64 * 1024,
                },
            )
            .await
            .unwrap();
            assert!(matches!(
                applied,
                git::integration::MergeApplyOutcome::Applied
            ));
        }
        let reflog = git_at(&f.repo, &["reflog", "--all"]).await;
        ServerIntegrationOwner::new(f.db.clone())
            .reconcile_outstanding()
            .await
            .unwrap();
        let receipt =
            f.db.integration_effect_receipt(&request)
                .await
                .unwrap()
                .expect("settled at start");
        if landed {
            assert_eq!(
                receipt.operation_state,
                IntegrationOperationState::Succeeded
            );
            assert_eq!(
                receipt.result["outcome"]["Done"]["after_sha"],
                json!(f.head)
            );
        } else {
            assert_eq!(receipt.operation_state, IntegrationOperationState::Failed);
            assert_eq!(receipt.result["kind"], "not_performed");
        }
        assert_eq!(git_at(&f.repo, &["reflog", "--all"]).await, reflog);
        assert!(f
            .db
            .integration_attempt(&f.fence.attempt_id)
            .await
            .unwrap()
            .unwrap()
            .effect_intent_json
            .is_none());
    }
}
/// A Task's checkout that is not on this server can never be merged
/// here without a reviewed candidate (there is no worktree to merge).
#[tokio::test]
async fn a_checkout_on_another_machine_is_not_merged_without_a_reviewed_candidate() {
    let f = fixture().await;
    let away = EffectWorkspace {
        owner: EffectOwner::Daemon {
            daemon_id: "d".into(),
            runtime_id: "rt".into(),
        },
        ..f.workspace.clone()
    };
    let before = untouched(&f.repo).await;
    let receipt = f
        .owner
        .merge(ServerMergeRequest {
            workspace: &away,
            reviewed: None,
            ..merge_request(&f)
        })
        .await
        .unwrap();
    assert!(matches!(
        receipt,
        OwnerMergeReceipt::Refused {
            reason: OwnerEffectRefusal::ForeignOwner
        }
    ));
    assert_eq!(untouched(&f.repo).await, before);
}
#[tokio::test]
async fn lost_merge_reply_replays_one_receipt_without_git_changes() {
    let f = fixture().await;
    let result = f.owner.merge(merge_request(&f)).await.unwrap();
    assert!(matches!(
        result,
        OwnerMergeReceipt::Completed {
            outcome: crate::MergeOutcome::Done { .. }
        }
    ));
    let reflog = git_at(&f.repo, &["reflog", "--all"]).await;
    let objects = git_at(&f.repo, &["count-objects", "-v"]).await;
    let reconnect = ServerIntegrationOwner::new(f.db.clone());
    let replay = reconnect.merge(merge_request(&f)).await.unwrap();
    assert_eq!(json!(result), json!(replay));
    assert_eq!(git_at(&f.repo, &["reflog", "--all"]).await, reflog);
    assert_eq!(git_at(&f.repo, &["count-objects", "-v"]).await, objects);
    let n: i64 = sqlx::query_scalar(
        "SELECT json_array_length(effect_receipts_json) FROM integration_attempt WHERE id=?",
    )
    .bind(&f.fence.attempt_id)
    .fetch_one(f.db.pool())
    .await
    .unwrap();
    assert_eq!(n, 1);
    assert_eq!(
        f.db.integration_attempt(&f.fence.attempt_id)
            .await
            .unwrap()
            .unwrap()
            .current_operation_state,
        Some(IntegrationOperationState::Succeeded)
    );
}
#[tokio::test]
async fn restart_reconciles_outstanding_intent_before_a_new_effect() {
    let mut f = fixture_with_storage(true).await;
    let request = IntegrationEffectRequest {
        fence: f.fence.clone(),
        kind: IntegrationOperationKind::FastForward,
        witness: json!({"workspace":f.workspace,"target_branch":"main","task_branch":"candidate","expected_head_sha":f.head,"expected_target_sha":f.target,"reviewed":{"commit_sha":f.head,"base_sha":f.target},"deadline_nanos":null}),
    };
    let IntegrationEffectAdmission::Started(mut guard) =
        f.db.begin_integration_effect(request.clone())
            .await
            .unwrap()
    else {
        panic!("not admitted");
    };
    assert!(guard.start().await.unwrap().is_none());
    git::checkout_branch(&f.repo, "main").await.unwrap();
    git_at(&f.repo, &["merge", "--ff-only", &f.head]).await;
    // Crash after Git but before receipt commit; reopen durable storage.
    drop(guard);
    f.db.pool().close().await;
    let url = format!(
        "sqlite://{}?mode=rwc",
        f._temp.path().join("owner.sqlite").display()
    );
    f.db = Arc::new(SqliteDb::new(db::create_sqlite_pool(&url).await.unwrap()));
    f.owner = ServerIntegrationOwner::new(f.db.clone());
    let reflog = git_at(&f.repo, &["reflog", "--all"]).await;
    let result = f.owner.merge(merge_request(&f)).await.unwrap();
    assert!(matches!(
        result,
        OwnerMergeReceipt::Completed {
            outcome: crate::MergeOutcome::Done { .. }
        }
    ));
    assert_eq!(git_at(&f.repo, &["reflog", "--all"]).await, reflog);
    let attempt =
        f.db.integration_attempt(&f.fence.attempt_id)
            .await
            .unwrap()
            .unwrap();
    assert!(attempt.effect_intent_json.is_none());
    assert_eq!(
        attempt.current_operation_state,
        Some(IntegrationOperationState::Succeeded)
    );
}

#[tokio::test]
async fn stale_and_foreign_server_owner_are_refused_before_git_and_witness_is_checked() {
    for case in ["stale", "foreign", "placement", "head", "target"] {
        let mut f = fixture().await;
        let before = git_at(&f.repo, &["reflog", "--all"]).await;
        match case {
            "stale" => f.fence.generation += 1,
            "foreign" => f.fence.target_owner["generation"] = json!(999),
            "placement" => f.workspace.generation += 1,
            "head" => f.head = "bad-head".into(),
            "target" => f.target = "bad-target".into(),
            _ => unreachable!(),
        }
        if matches!(case, "stale" | "foreign" | "placement") {
            std::fs::rename(f.repo.join(".git"), f.repo.join("hidden-git")).unwrap();
        }
        let outcome = f.owner.merge(merge_request(&f)).await.unwrap();
        assert!(
            matches!(outcome, OwnerMergeReceipt::Refused { .. }),
            "case {case}: {outcome:?}"
        );
        if matches!(case, "stale" | "foreign" | "placement") {
            std::fs::rename(f.repo.join("hidden-git"), f.repo.join(".git")).unwrap();
        }
        assert_eq!(git_at(&f.repo, &["reflog", "--all"]).await, before);
    }
}
#[tokio::test]
async fn duplicate_rebase_returns_receipt_and_preserves_objects_and_reflog() {
    let mut f = fixture().await;
    std::fs::write(f.repo.join("target"), "target\n").unwrap();
    git::commit_all(&f.repo, "advance target").await.unwrap();
    f.target = git::get_current_sha(&f.repo).await.unwrap();
    let cancel = CancellationToken::new();
    let request = || ServerRebaseRequest {
        fence: &f.fence,
        workspace: &f.workspace,
        target_branch: "main",
        expected_head_sha: &f.head,
        expected_target_sha: &f.target,
        handoff_conflicts: false,
        deadline: Duration::from_secs(5),
        cancel: &cancel,
    };
    let first = f.owner.rebase(request()).await.unwrap();
    assert!(matches!(first, OwnerRebaseReceipt::Completed { .. }));
    assert_ne!(git::get_current_sha(&f.tree).await.unwrap(), f.head);
    let reflog = git_at(&f.repo, &["reflog", "--all"]).await;
    let objects = git_at(&f.repo, &["count-objects", "-v"]).await;
    let second = f.owner.rebase(request()).await.unwrap();
    assert_eq!(json!(first), json!(second));
    assert_eq!(git_at(&f.repo, &["reflog", "--all"]).await, reflog);
    assert_eq!(git_at(&f.repo, &["count-objects", "-v"]).await, objects);
}
#[cfg(unix)]
#[tokio::test]
async fn cancel_mid_rebase_kills_git_and_hook_and_retains_recovery_facts() {
    let mut f = fixture().await;
    std::fs::write(f.repo.join("target"), "target\n").unwrap();
    git::commit_all(&f.repo, "advance target").await.unwrap();
    f.target = git::get_current_sha(&f.repo).await.unwrap();
    let hook = f.repo.join(".git/hooks/pre-rebase");
    process_supervisor::executable::write_executable(
        &hook,
        "#!/bin/sh\necho $$ > hook.pid\necho $PPID > git.pid\nexec sleep 60\n",
    )
    .unwrap();
    let cancel = CancellationToken::new();
    let operation = f.owner.rebase(ServerRebaseRequest {
        fence: &f.fence,
        workspace: &f.workspace,
        target_branch: "main",
        expected_head_sha: &f.head,
        expected_target_sha: &f.target,
        handoff_conflicts: false,
        deadline: Duration::from_secs(20),
        cancel: &cancel,
    });
    let cancel_midway = async {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !f.tree.join("git.pid").exists() {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        tokio::time::timeout(Duration::from_millis(250), async {
            let mut tx = db::begin_immediate(f.db.pool()).await.unwrap();
            sqlx::query("UPDATE repo SET name='writer during Git' WHERE id='r'")
                .execute(&mut *tx)
                .await
                .unwrap();
            tx.commit().await.unwrap();
        })
        .await
        .expect("Git effect held the SQLite writer");
        cancel.cancel();
    };
    let (outcome, _) = tokio::join!(operation, cancel_midway);
    let outcome = outcome.unwrap();
    assert!(matches!(outcome, OwnerRebaseReceipt::Cancelled { .. }));
    for file in ["git.pid", "hook.pid"] {
        let pid = std::fs::read_to_string(f.tree.join(file)).unwrap();
        tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                let alive = std::process::Command::new("kill")
                    .args(["-0", pid.trim()])
                    .stdout(std::process::Stdio::null())
                    .stderr(std::process::Stdio::null())
                    .status()
                    .unwrap()
                    .success();
                #[cfg(target_os = "linux")]
                let alive = alive
                    && !(file == "hook.pid"
                        && std::fs::read_to_string(format!("/proc/{}/stat", pid.trim()))
                            .is_ok_and(|stat| {
                                stat.rsplit_once(") ")
                                    .is_some_and(|(_, tail)| tail.starts_with('Z'))
                            }));
                if !alive {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
    assert_eq!(git::get_current_sha(&f.tree).await.unwrap(), f.head);
    assert!(!git::detect_rebase_in_progress(&f.tree).await.unwrap());
    let raw: String =
        sqlx::query_scalar("SELECT effect_receipts_json FROM integration_attempt WHERE id=?")
            .bind(&f.fence.attempt_id)
            .fetch_one(f.db.pool())
            .await
            .unwrap();
    assert!(raw.contains("cancelled"));
}
