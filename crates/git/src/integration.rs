//! Shared owner-local integration effects. No storage or Task authority.
use crate::{GitError, Result};
use std::path::Path;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum RebaseOutcome {
    Rebased,
    Conflict {
        details: String,
        conflict_paths: Vec<String>,
    },
    UnsupportedConflict {
        details: String,
    },
    Dirty {
        files: Vec<String>,
    },
}

pub async fn rebase(
    worktree: &Path,
    target: &str,
    handoff_conflicts: bool,
) -> Result<RebaseOutcome> {
    let in_progress = crate::detect_rebase_in_progress(worktree).await?;
    if in_progress && !handoff_conflicts {
        // Resume an interrupted rebase the way a fresh conflicting one
        // ends without handoff: abort, restoring the branch.
        crate::abort_rebase(worktree).await?;
        return Ok(RebaseOutcome::Conflict {
            details: "aborted interrupted rebase".into(),
            conflict_paths: Vec::new(),
        });
    }
    if in_progress {
        return match crate::continue_rebase_keeping_conflicts(worktree).await {
            Ok(conflict_paths) => Ok(RebaseOutcome::Conflict {
                details: "resumed interrupted rebase".into(),
                conflict_paths,
            }),
            Err(crate::GitError::UnsupportedRebaseConflict { details }) => {
                Ok(RebaseOutcome::UnsupportedConflict { details })
            }
            Err(error) => Err(error),
        };
    }
    if !crate::is_worktree_clean(worktree).await? {
        return Ok(RebaseOutcome::Dirty {
            files: crate::status_porcelain(worktree).await?,
        });
    }
    match crate::rebase(worktree, target).await {
        Ok(()) => Ok(RebaseOutcome::Rebased),
        Err(crate::GitError::MergeConflict { stderr, .. }) => {
            if !handoff_conflicts {
                crate::abort_rebase(worktree).await?;
                return Ok(RebaseOutcome::Conflict {
                    details: stderr,
                    conflict_paths: Vec::new(),
                });
            }
            match crate::continue_rebase_keeping_conflicts(worktree).await {
                Ok(conflict_paths) => Ok(RebaseOutcome::Conflict {
                    details: stderr,
                    conflict_paths,
                }),
                Err(crate::GitError::UnsupportedRebaseConflict { details }) => {
                    Ok(RebaseOutcome::UnsupportedConflict { details })
                }
                Err(error) => Err(error),
            }
        }
        Err(error) => Err(error),
    }
}

/// The same reviewed fast-forward algorithm serves both owners. Manual mode
/// retains the caller's merge-commit candidate. Witness checks belong to gates.
#[derive(Debug)]
pub enum MergeApplyOutcome {
    Applied,
    ManualFailed(GitError),
    ReviewRequired { reason: String },
    ExactObjectMismatch,
    TargetMoved,
}

pub struct FastForwardLimits {
    pub deadline: std::time::Duration,
    pub output_bytes: usize,
}

pub async fn apply_merge(
    repo: &Path,
    target_branch: &str,
    candidate: &str,
    reviewed: bool,
    already_merged: bool,
    expected_target: Option<&str>,
    limits: FastForwardLimits,
) -> Result<MergeApplyOutcome> {
    if !already_merged {
        crate::checkout_branch(repo, target_branch).await?;
    }
    if already_merged {
        return Ok(MergeApplyOutcome::Applied);
    }
    if let Some(expected) = expected_target {
        if crate::get_current_sha(repo).await? != expected {
            return Ok(MergeApplyOutcome::TargetMoved);
        }
    }
    if reviewed {
        let output = match tokio::time::timeout(
            limits.deadline,
            crate::command_output_bounded(
                repo,
                &["merge", "--ff-only", candidate],
                limits.output_bytes,
            ),
        )
        .await
        {
            Ok(Ok(output)) => output,
            Ok(Err(error)) => {
                return Ok(MergeApplyOutcome::ReviewRequired {
                    reason: match error {
                        GitError::Io(error) => error.to_string(),
                        error => error.to_string(),
                    },
                })
            }
            Err(_) => {
                return Ok(MergeApplyOutcome::ReviewRequired {
                    reason: "review command timed out".into(),
                })
            }
        };
        if !output.status.success() {
            return Ok(MergeApplyOutcome::ReviewRequired {
                reason: format!(
                    "git evidence unavailable: {}",
                    String::from_utf8_lossy(&output.stderr)
                ),
            });
        }
        if crate::get_current_sha(repo).await? != candidate {
            return Ok(MergeApplyOutcome::ExactObjectMismatch);
        }
        Ok(MergeApplyOutcome::Applied)
    } else {
        match crate::merge_branch_into(repo, candidate).await {
            Ok(()) => Ok(MergeApplyOutcome::Applied),
            Err(error) => Ok(MergeApplyOutcome::ManualFailed(error)),
        }
    }
}

/// Largest Git object transfer between two checkouts of one repository.
pub const MAX_OBJECT_TRANSFER_BYTES: u64 = 256 * 1024 * 1024;
/// The only ref namespace an object import may write.
pub const INTEGRATION_REF_PREFIX: &str = "refs/forge/integration/";
const EXPORT_REF_PREFIX: &str = "refs/forge/export/";

/// Why an object transfer was refused. Every refusal leaves the repository's
/// refs, object store and work tree as they were.
#[derive(Debug, thiserror::Error)]
pub enum ObjectTransferError {
    #[error("object transfer of {bytes} bytes exceeds the {max_bytes} byte limit")]
    TooLarge { bytes: u64, max_bytes: u64 },
    #[error("object transfer refused: {reason}")]
    Invalid { reason: String },
    #[error("commit {sha} is not present in the source repository")]
    MissingObject { sha: String },
    #[error("transfer key is already bound to commit {existing_sha}")]
    KeyConflict { existing_sha: String },
    #[error(transparent)]
    Git(#[from] GitError),
}

impl From<std::io::Error> for ObjectTransferError {
    fn from(error: std::io::Error) -> Self {
        Self::Git(GitError::Io(error))
    }
}

pub type TransferResult<T> = std::result::Result<T, ObjectTransferError>;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ObjectExport {
    pub tip_sha: String,
    /// Zero when the receiver's `have` commits already contain the tip: no
    /// bundle file is written and the import only binds its ref.
    pub total_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ObjectImport {
    pub tip_sha: String,
    pub ref_name: String,
    /// The key was already imported: nothing was read or written this time.
    pub replayed: bool,
}

/// `refs/forge/integration/<key>`. A key is one ref path component.
pub fn transfer_ref(key: &str) -> TransferResult<String> {
    transfer_ref_in(INTEGRATION_REF_PREFIX, key)
}

fn transfer_ref_in(prefix: &str, key: &str) -> TransferResult<String> {
    let valid = !key.is_empty()
        && key.len() <= 128
        && !key.starts_with('.')
        && !key.ends_with('.')
        && !key.ends_with(".lock")
        && !key.contains("..")
        && key
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    if !valid {
        return Err(ObjectTransferError::Invalid {
            reason: "transfer key must be 1-128 characters of [A-Za-z0-9._-]".into(),
        });
    }
    Ok(format!("{prefix}{key}"))
}

fn is_object_id(value: &str) -> bool {
    matches!(value.len(), 40 | 64) && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

async fn commit_exists(repo: &Path, sha: &str) -> Result<bool> {
    Ok(is_object_id(sha)
        && crate::command_output(repo, &["cat-file", "-e", &format!("{sha}^{{commit}}")])
            .await?
            .status
            .success())
}

async fn resolved_ref(repo: &Path, name: &str) -> Result<Option<String>> {
    let output = crate::command_output(
        repo,
        &["rev-parse", "--verify", "--quiet", "--end-of-options", name],
    )
    .await?;
    Ok(output
        .status
        .success()
        .then(|| String::from_utf8_lossy(&output.stdout).trim().to_owned()))
}

fn invalid(context: &str, output: &std::process::Output) -> ObjectTransferError {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let tail: String = stderr
        .chars()
        .rev()
        .take(1024)
        .collect::<Vec<_>>()
        .into_iter()
        .rev()
        .collect();
    ObjectTransferError::Invalid {
        reason: format!("{context}: {}", tail.trim()),
    }
}

/// Removes a path when dropped, so a cancelled transfer leaves nothing behind.
struct RemoveOnDrop {
    path: std::path::PathBuf,
    armed: bool,
}
impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if self.path.is_dir() {
            let _ = std::fs::remove_dir_all(&self.path);
        } else {
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Deletes the export's temporary ref even when the export future is dropped.
struct ExportRef<'a> {
    repo: &'a Path,
    name: String,
}
impl Drop for ExportRef<'_> {
    fn drop(&mut self) {
        let _ = std::process::Command::new("git")
            .args(["update-ref", "-d", &self.name])
            .current_dir(self.repo)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status();
    }
}

/// The receipt of an import that already happened for this key, if any.
pub async fn imported_objects(
    repo: &Path,
    key: &str,
    expected_tip: &str,
) -> TransferResult<Option<ObjectImport>> {
    let ref_name = transfer_ref(key)?;
    match resolved_ref(repo, &ref_name).await? {
        Some(existing) if existing == expected_tip => Ok(Some(ObjectImport {
            tip_sha: existing,
            ref_name,
            replayed: true,
        })),
        Some(existing_sha) => Err(ObjectTransferError::KeyConflict { existing_sha }),
        None => Ok(None),
    }
}

/// Write the commits reachable from `want` and from none of `have` to a Git
/// bundle at `dest`. Both owners use this one algorithm. The source repository
/// is left unchanged; a refused, failed or dropped export removes `dest`.
pub async fn export_objects(
    repo: &Path,
    key: &str,
    have: &[String],
    want: &str,
    dest: &Path,
    max_bytes: u64,
) -> TransferResult<ObjectExport> {
    let export_ref = transfer_ref_in(EXPORT_REF_PREFIX, key)?;
    if !commit_exists(repo, want).await? {
        return Err(ObjectTransferError::MissingObject { sha: want.into() });
    }
    // A commit the source does not hold cannot be a bundle prerequisite.
    let mut exclusions = Vec::new();
    for sha in have {
        if commit_exists(repo, sha).await? {
            exclusions.push(format!("^{sha}"));
        }
    }
    let mut count = vec!["rev-list", "--count", want];
    count.extend(exclusions.iter().map(String::as_str));
    let output = crate::command_output(repo, &count).await?;
    if !output.status.success() {
        return Err(invalid("could not size the transfer", &output));
    }
    if String::from_utf8_lossy(&output.stdout).trim() == "0" {
        return Ok(ObjectExport {
            tip_sha: want.into(),
            total_bytes: 0,
        });
    }
    let mut file = RemoveOnDrop {
        path: dest.to_path_buf(),
        armed: true,
    };
    let pinned = ExportRef {
        repo,
        name: export_ref.clone(),
    };
    let output = crate::command_output(repo, &["update-ref", &pinned.name, want]).await?;
    if !output.status.success() {
        return Err(invalid("could not pin the exported commit", &output));
    }
    let dest_arg = dest.to_string_lossy().into_owned();
    let mut create = vec!["bundle", "create", "--quiet", &dest_arg, &export_ref];
    create.extend(exclusions.iter().map(String::as_str));
    let output = crate::command_output(repo, &create).await?;
    drop(pinned);
    if !output.status.success() {
        return Err(invalid("could not write the bundle", &output));
    }
    let total_bytes = tokio::fs::metadata(dest).await?.len();
    if total_bytes > max_bytes {
        return Err(ObjectTransferError::TooLarge {
            bytes: total_bytes,
            max_bytes,
        });
    }
    file.armed = false;
    Ok(ObjectExport {
        tip_sha: want.into(),
        total_bytes,
    })
}

async fn quarantined(
    repo: &Path,
    quarantine: &Path,
    objects: &Path,
    args: &[&str],
) -> Result<std::process::Output> {
    let mut command = tokio::process::Command::new("git");
    command
        .args(args)
        .current_dir(repo)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .env("GIT_OBJECT_DIRECTORY", quarantine)
        .env("GIT_ALTERNATE_OBJECT_DIRECTORIES", objects);
    Ok(process_supervisor::output_bounded(&mut command, Some(64 * 1024)).await?)
}

/// Verify a bundle and store its objects under `refs/forge/integration/<key>`.
///
/// `bundle` is `None` for an export of zero bytes. The objects are unpacked
/// into a quarantine directory, checked (`index-pack --strict`, the expected
/// tip present and fully connected) and only then moved into the object
/// store. The single ref written is the transfer ref: ref names inside the
/// bundle are never used, no other ref moves, nothing is checked out. A
/// refused, failed or dropped import leaves the repository as it was. A key
/// that was already imported returns its receipt without reading the bundle.
pub async fn import_objects(
    repo: &Path,
    key: &str,
    bundle: Option<&Path>,
    expected_tip: &str,
    max_bytes: u64,
) -> TransferResult<ObjectImport> {
    if !is_object_id(expected_tip) {
        return Err(ObjectTransferError::Invalid {
            reason: "expected tip is not a full object id".into(),
        });
    }
    if let Some(replay) = imported_objects(repo, key, expected_tip).await? {
        return Ok(replay);
    }
    let ref_name = transfer_ref(key)?;
    if let Some(bundle) = bundle {
        let bytes = tokio::fs::metadata(bundle).await?.len();
        if bytes > max_bytes {
            return Err(ObjectTransferError::TooLarge { bytes, max_bytes });
        }
        let bundle_arg = bundle.to_string_lossy().into_owned();
        let output =
            crate::command_output_bounded(repo, &["bundle", "verify", &bundle_arg], 64 * 1024)
                .await?;
        if !output.status.success() {
            return Err(invalid("bundle failed verification", &output));
        }
        let output =
            crate::command_output_bounded(repo, &["bundle", "list-heads", &bundle_arg], 64 * 1024)
                .await?;
        if !output.status.success()
            || !String::from_utf8_lossy(&output.stdout)
                .lines()
                .any(|line| line.split_whitespace().next() == Some(expected_tip))
        {
            return Err(ObjectTransferError::Invalid {
                reason: "bundle does not carry the expected tip".into(),
            });
        }
        let common = crate::command_output(repo, &["rev-parse", "--git-common-dir"]).await?;
        if !common.status.success() {
            return Err(invalid("target is not a Git repository", &common));
        }
        let common = repo
            .join(String::from_utf8_lossy(&common.stdout).trim())
            .canonicalize()?;
        let objects = common.join("objects");
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_nanos())
            .unwrap_or_default();
        let quarantine = RemoveOnDrop {
            path: common.join(format!(
                "forge-incoming-{key}-{}-{nonce}",
                std::process::id()
            )),
            armed: true,
        };
        tokio::fs::create_dir_all(quarantine.path.join("pack")).await?;
        let output = quarantined(
            repo,
            &quarantine.path,
            &objects,
            &["bundle", "unbundle", &bundle_arg],
        )
        .await?;
        if !output.status.success() {
            return Err(invalid("bundle could not be unpacked", &output));
        }
        let mut packs = Vec::new();
        let mut entries = tokio::fs::read_dir(quarantine.path.join("pack")).await?;
        while let Some(entry) = entries.next_entry().await? {
            packs.push(entry.path());
        }
        packs.sort();
        for (index, pack) in packs
            .iter()
            .filter(|path| path.extension().is_some_and(|ext| ext == "pack"))
            .enumerate()
        {
            let check = quarantine.path.join(format!("check-{index}.idx"));
            let output = quarantined(
                repo,
                &quarantine.path,
                &objects,
                &[
                    "index-pack",
                    "--strict",
                    "-o",
                    &check.to_string_lossy(),
                    &pack.to_string_lossy(),
                ],
            )
            .await?;
            if !output.status.success() {
                return Err(invalid("bundle objects failed integrity checks", &output));
            }
        }
        let tip = format!("{expected_tip}^{{commit}}");
        let output = quarantined(
            repo,
            &quarantine.path,
            &objects,
            &["rev-parse", "--verify", "--quiet", "--end-of-options", &tip],
        )
        .await?;
        if !output.status.success() {
            return Err(ObjectTransferError::Invalid {
                reason: "bundle did not deliver the expected tip".into(),
            });
        }
        let output = quarantined(
            repo,
            &quarantine.path,
            &objects,
            &[
                "rev-list",
                "--objects",
                "--quiet",
                expected_tip,
                "--not",
                "--all",
            ],
        )
        .await?;
        if !output.status.success() {
            return Err(invalid("imported history is not fully connected", &output));
        }
        // Publish: an index last, so Git never sees an index without its pack.
        tokio::fs::create_dir_all(objects.join("pack")).await?;
        packs.sort_by_key(|path| path.extension().is_some_and(|ext| ext == "idx"));
        for path in &packs {
            let Some(name) = path.file_name() else {
                continue;
            };
            let dest = objects.join("pack").join(name);
            if tokio::fs::try_exists(&dest).await? {
                continue;
            }
            tokio::fs::rename(path, &dest).await?;
        }
    } else if !commit_exists(repo, expected_tip).await? {
        return Err(ObjectTransferError::Invalid {
            reason: "target does not hold the expected tip and no objects were sent".into(),
        });
    }
    if !commit_exists(repo, expected_tip).await? {
        return Err(ObjectTransferError::Invalid {
            reason: "expected tip is missing after the import".into(),
        });
    }
    // Create-only: a concurrent import of another object under this key loses.
    let zero = "0".repeat(expected_tip.len());
    let output =
        crate::command_output(repo, &["update-ref", &ref_name, expected_tip, &zero]).await?;
    if !output.status.success() {
        return match imported_objects(repo, key, expected_tip).await? {
            Some(replay) => Ok(replay),
            None => Err(invalid("could not bind the transfer ref", &output)),
        };
    }
    Ok(ObjectImport {
        tip_sha: expected_tip.into(),
        ref_name,
        replayed: false,
    })
}

#[cfg(test)]
mod transfer_tests {
    use super::*;
    use std::path::PathBuf;

    fn git(cwd: &Path, args: &[&str]) -> String {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(cwd)
            .env("GIT_AUTHOR_NAME", "Forge")
            .env("GIT_AUTHOR_EMAIL", "forge@example.invalid")
            .env("GIT_COMMITTER_NAME", "Forge")
            .env("GIT_COMMITTER_EMAIL", "forge@example.invalid")
            .output()
            .expect("git runs");
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        String::from_utf8_lossy(&output.stdout).trim().to_owned()
    }

    fn commit(repo: &Path, file: &str, body: &str) -> String {
        std::fs::write(repo.join(file), body).unwrap();
        git(repo, &["add", "."]);
        git(repo, &["commit", "-q", "-m", file]);
        git(repo, &["rev-parse", "HEAD"])
    }

    /// Two clones of one history; the source is one commit ahead.
    struct Pair {
        _dir: tempfile::TempDir,
        source: PathBuf,
        target: PathBuf,
        staging: PathBuf,
        base: String,
        tip: String,
    }

    fn pair() -> Pair {
        let dir = tempfile::tempdir().unwrap();
        let source = dir.path().join("source");
        let target = dir.path().join("target");
        let staging = dir.path().join("staging");
        std::fs::create_dir_all(&source).unwrap();
        std::fs::create_dir_all(&staging).unwrap();
        git(&source, &["init", "-q", "-b", "main"]);
        let base = commit(&source, "base.txt", "base\n");
        git(
            dir.path(),
            &["clone", "-q", source.to_str().unwrap(), "target"],
        );
        let tip = commit(&source, "work.txt", &"work\n".repeat(64));
        Pair {
            _dir: dir,
            source,
            target,
            staging,
            base,
            tip,
        }
    }

    /// Everything an import may not disturb: refs, HEAD, index, work tree,
    /// packs and leftover directories in the Git directory.
    fn snapshot(repo: &Path) -> String {
        let mut entries: Vec<String> = std::fs::read_dir(repo.join(".git"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        entries.sort();
        let mut packs: Vec<String> = std::fs::read_dir(repo.join(".git/objects/pack"))
            .map(|dir| {
                dir.map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
                    .collect()
            })
            .unwrap_or_default();
        packs.sort();
        format!(
            "{}\n{}\n{}\n{entries:?}\n{packs:?}",
            git(repo, &["for-each-ref"]),
            git(repo, &["rev-parse", "HEAD"]),
            git(repo, &["status", "--porcelain"]),
        )
    }

    #[tokio::test]
    async fn round_trip_binds_only_the_forge_ref_and_checks_nothing_out() {
        let pair = pair();
        let bundle = pair.staging.join("a.bundle");
        let export = export_objects(
            &pair.source,
            "attempt-1-out",
            std::slice::from_ref(&pair.base),
            &pair.tip,
            &bundle,
            MAX_OBJECT_TRANSFER_BYTES,
        )
        .await
        .unwrap();
        assert_eq!(export.tip_sha, pair.tip);
        assert!(export.total_bytes > 0);
        assert_eq!(
            git(&pair.source, &["for-each-ref", "refs/forge"]),
            "",
            "the export leaves no ref in the source"
        );
        let head = git(&pair.target, &["rev-parse", "HEAD"]);
        let import = import_objects(
            &pair.target,
            "attempt-1-out",
            Some(&bundle),
            &pair.tip,
            MAX_OBJECT_TRANSFER_BYTES,
        )
        .await
        .unwrap();
        assert_eq!(
            import,
            ObjectImport {
                tip_sha: pair.tip.clone(),
                ref_name: "refs/forge/integration/attempt-1-out".into(),
                replayed: false
            }
        );
        assert_eq!(
            git(
                &pair.target,
                &["rev-parse", "refs/forge/integration/attempt-1-out"]
            ),
            pair.tip
        );
        assert_eq!(git(&pair.target, &["rev-parse", "HEAD"]), head);
        assert_eq!(git(&pair.target, &["rev-parse", "refs/heads/main"]), head);
        assert_eq!(git(&pair.target, &["status", "--porcelain"]), "");
        assert!(!pair.target.join("work.txt").exists());
        git(&pair.target, &["fsck", "--strict", "--no-dangling"]);
        // The exact object can now be fast-forwarded where it landed.
        git(&pair.target, &["merge", "-q", "--ff-only", &pair.tip]);
        assert_eq!(git(&pair.target, &["rev-parse", "HEAD"]), pair.tip);
    }

    #[tokio::test]
    async fn duplicate_key_replays_the_receipt_and_reads_no_bundle() {
        let pair = pair();
        let bundle = pair.staging.join("a.bundle");
        export_objects(&pair.source, "k", &[], &pair.tip, &bundle, u64::MAX)
            .await
            .unwrap();
        import_objects(&pair.target, "k", Some(&bundle), &pair.tip, u64::MAX)
            .await
            .unwrap();
        let before = snapshot(&pair.target);
        std::fs::remove_file(&bundle).unwrap();
        let replay = import_objects(&pair.target, "k", Some(&bundle), &pair.tip, 1)
            .await
            .unwrap();
        assert!(replay.replayed);
        assert_eq!(replay.tip_sha, pair.tip);
        assert_eq!(snapshot(&pair.target), before);
        assert!(matches!(
            import_objects(&pair.target, "k", None, &pair.base, u64::MAX).await,
            Err(ObjectTransferError::KeyConflict { existing_sha }) if existing_sha == pair.tip
        ));
    }

    #[tokio::test]
    async fn an_export_the_receiver_already_holds_sends_nothing() {
        let pair = pair();
        let bundle = pair.staging.join("a.bundle");
        let export = export_objects(
            &pair.source,
            "k",
            std::slice::from_ref(&pair.base),
            &pair.base,
            &bundle,
            u64::MAX,
        )
        .await
        .unwrap();
        assert_eq!(export.total_bytes, 0);
        assert!(!bundle.exists());
        let import = import_objects(&pair.target, "k", None, &pair.base, u64::MAX)
            .await
            .unwrap();
        assert!(!import.replayed);
        assert!(matches!(
            import_objects(&pair.target, "k2", None, &pair.tip, u64::MAX).await,
            Err(ObjectTransferError::Invalid { .. })
        ));
    }

    #[tokio::test]
    async fn over_cap_is_refused_on_both_sides_before_the_target_changes() {
        let pair = pair();
        let bundle = pair.staging.join("a.bundle");
        assert!(matches!(
            export_objects(&pair.source, "k", &[], &pair.tip, &bundle, 16).await,
            Err(ObjectTransferError::TooLarge { max_bytes: 16, .. })
        ));
        assert!(!bundle.exists(), "a refused export keeps no file");
        assert_eq!(git(&pair.source, &["for-each-ref", "refs/forge"]), "");
        export_objects(&pair.source, "k", &[], &pair.tip, &bundle, u64::MAX)
            .await
            .unwrap();
        let before = snapshot(&pair.target);
        assert!(matches!(
            import_objects(&pair.target, "k", Some(&bundle), &pair.tip, 16).await,
            Err(ObjectTransferError::TooLarge { max_bytes: 16, .. })
        ));
        assert_eq!(snapshot(&pair.target), before);
    }

    #[tokio::test]
    async fn corrupt_and_truncated_bundles_leave_the_target_untouched() {
        let pair = pair();
        let bundle = pair.staging.join("a.bundle");
        export_objects(
            &pair.source,
            "k",
            std::slice::from_ref(&pair.base),
            &pair.tip,
            &bundle,
            u64::MAX,
        )
        .await
        .unwrap();
        let bytes = std::fs::read(&bundle).unwrap();
        let before = snapshot(&pair.target);
        let mut corrupt = bytes.clone();
        let middle = corrupt.len() - 30;
        for byte in &mut corrupt[middle..middle + 8] {
            *byte ^= 0xff;
        }
        let cases: Vec<(&str, Vec<u8>)> = vec![
            ("corrupt pack", corrupt),
            ("truncated pack", bytes[..bytes.len() - 40].to_vec()),
            ("truncated header", bytes[..20].to_vec()),
            ("not a bundle", b"hello".to_vec()),
            ("empty", Vec::new()),
        ];
        for (name, payload) in cases {
            let path = pair.staging.join("bad.bundle");
            std::fs::write(&path, payload).unwrap();
            let result = import_objects(&pair.target, "k", Some(&path), &pair.tip, u64::MAX).await;
            assert!(
                matches!(result, Err(ObjectTransferError::Invalid { .. })),
                "{name}: {result:?}"
            );
            assert_eq!(snapshot(&pair.target), before, "{name}");
        }
        // A sound bundle for another tip is refused as well.
        assert!(matches!(
            import_objects(&pair.target, "k", Some(&bundle), &pair.base, u64::MAX).await,
            Err(ObjectTransferError::Invalid { .. })
        ));
        assert_eq!(snapshot(&pair.target), before);
        // The prerequisite is missing from an unrelated repository.
        let other = pair.staging.join("other");
        std::fs::create_dir_all(&other).unwrap();
        git(&other, &["init", "-q", "-b", "main"]);
        commit(&other, "x.txt", "x\n");
        let other_before = snapshot(&other);
        assert!(matches!(
            import_objects(&other, "k", Some(&bundle), &pair.tip, u64::MAX).await,
            Err(ObjectTransferError::Invalid { .. })
        ));
        assert_eq!(snapshot(&other), other_before);
    }

    #[tokio::test]
    async fn a_bundle_naming_user_refs_cannot_move_them() {
        let pair = pair();
        // Hand-made bundle: it names the user's branch and a tag.
        git(&pair.source, &["tag", "v9"]);
        let bundle = pair.staging.join("hostile.bundle");
        git(
            &pair.source,
            &[
                "bundle",
                "create",
                "-q",
                bundle.to_str().unwrap(),
                "refs/heads/main",
                "refs/tags/v9",
                &format!("^{}", pair.base),
            ],
        );
        let main = git(&pair.target, &["rev-parse", "refs/heads/main"]);
        let remote = git(&pair.target, &["rev-parse", "refs/remotes/origin/main"]);
        import_objects(&pair.target, "k", Some(&bundle), &pair.tip, u64::MAX)
            .await
            .unwrap();
        assert_eq!(git(&pair.target, &["rev-parse", "refs/heads/main"]), main);
        assert_eq!(
            git(&pair.target, &["rev-parse", "refs/remotes/origin/main"]),
            remote
        );
        assert_eq!(git(&pair.target, &["tag", "--list"]), "");
        assert_eq!(
            git(&pair.target, &["for-each-ref", "--format=%(refname)"]),
            "refs/forge/integration/k\nrefs/heads/main\nrefs/remotes/origin/HEAD\nrefs/remotes/origin/main"
        );
        for key in ["../heads/main", "a/b", "", ".x", "x.lock", "a..b", "a b"] {
            assert!(
                matches!(transfer_ref(key), Err(ObjectTransferError::Invalid { .. })),
                "{key}"
            );
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_cancelled_transfer_leaves_no_temporary_files_or_refs() {
        let pair = pair();
        let bundle = pair.staging.join("a.bundle");
        let source_before = snapshot(&pair.source);
        let mut cancelled = 0;
        for millis in [0, 1, 2, 4, 8, 16, 32] {
            let export = export_objects(&pair.source, "k", &[], &pair.tip, &bundle, u64::MAX);
            if tokio::time::timeout(std::time::Duration::from_millis(millis), export)
                .await
                .is_err()
            {
                cancelled += 1;
                assert!(!bundle.exists(), "export cancelled after {millis} ms");
                assert_eq!(snapshot(&pair.source), source_before, "{millis} ms");
            } else {
                std::fs::remove_file(&bundle).unwrap();
            }
        }
        export_objects(&pair.source, "k", &[], &pair.tip, &bundle, u64::MAX)
            .await
            .unwrap();
        let target_before = snapshot(&pair.target);
        for millis in [0, 1, 2, 4, 8, 16, 32, 64] {
            let import = import_objects(&pair.target, "k", Some(&bundle), &pair.tip, u64::MAX);
            if tokio::time::timeout(std::time::Duration::from_millis(millis), import)
                .await
                .is_err()
            {
                cancelled += 1;
                assert_eq!(
                    snapshot(&pair.target),
                    target_before,
                    "import cancelled after {millis} ms"
                );
            } else {
                // It finished inside the window: undo it for the next round.
                git(
                    &pair.target,
                    &["update-ref", "-d", "refs/forge/integration/k"],
                );
                break;
            }
        }
        assert!(cancelled > 0, "no run was cancelled");
    }
}
