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
const QUARANTINE_PREFIX: &str = "forge-incoming-";
/// A ref write runs the repository's `reference-transaction` hook. A transfer
/// must not run repository code, so its ref writes disable hooks.
const NO_HOOKS: [&str; 2] = ["-c", "core.hooksPath=/dev/null"];

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

// Cancelling a transfer means dropping its future, and a dropped transfer
// must have left nothing behind by the time the drop returns. Three rules
// keep that true:
//
// 1. Every file system effect of a transfer is a synchronous `std::fs` call.
//    A `tokio::fs` call runs on the blocking pool and is not stopped when the
//    future that asked for it is dropped: it would create the quarantine
//    directory, or publish a pack, after the guards below had already run.
// 2. A guard is declared before the command that writes what it removes. A
//    dropped future drops the running command first, and the process
//    supervisor stops and reaps that command's process group before its drop
//    returns, so no child is left to write when the guard removes its path.
// 3. Publishing (the step that changes the repository) has no await point:
//    see `bind_imported`.

/// Removes a path when dropped, so a cancelled transfer leaves nothing behind.
/// For a file, Git's `<path>.lock` goes with it: a killed `git bundle create`
/// cannot remove its own lock file.
struct RemoveOnDrop {
    path: std::path::PathBuf,
    armed: bool,
}
impl Drop for RemoveOnDrop {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut lock = self.path.clone().into_os_string();
        lock.push(".lock");
        // A process that outlived the supervisor's stop sequence (it ignored
        // TERM and was then killed without a wait) can add an entry while the
        // tree is removed. Retry until the path is gone, for a bounded time.
        for _ in 0..REMOVE_ATTEMPTS {
            let _ = std::fs::remove_file(&lock);
            let removed = match std::fs::symlink_metadata(&self.path) {
                Ok(found) if found.is_dir() => std::fs::remove_dir_all(&self.path),
                Ok(_) => std::fs::remove_file(&self.path),
                Err(error) => Err(error),
            };
            match removed {
                Ok(()) => return,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => return,
                Err(_) => std::thread::sleep(std::time::Duration::from_millis(5)),
            }
        }
    }
}
const REMOVE_ATTEMPTS: usize = 200;

/// Run Git to completion on the calling thread. A transfer uses it where a
/// step must not be interrupted by its future being dropped.
fn git_blocking(repo: &Path, args: &[&str]) -> std::io::Result<std::process::Output> {
    std::process::Command::new("git")
        .args(args)
        .current_dir(repo)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .stdin(std::process::Stdio::null())
        .output()
}

/// Deletes the export's temporary ref even when the export future is dropped.
struct ExportRef<'a> {
    repo: &'a Path,
    name: String,
}
impl Drop for ExportRef<'_> {
    fn drop(&mut self) {
        // The command that pins the ref is stopped before this runs. If it
        // was stopped while it held the ref's lock file and could not remove
        // it, the lock would refuse this delete and every later export of the
        // key. Only this export writes the ref, so the lock is its own.
        let lock = format!("{}.lock", self.name);
        if let Ok(output) = git_blocking(self.repo, &["rev-parse", "--git-path", &lock]) {
            if output.status.success() {
                let path = String::from_utf8_lossy(&output.stdout);
                let _ = std::fs::remove_file(self.repo.join(path.trim()));
            }
        }
        let _ = git_blocking(
            self.repo,
            &[NO_HOOKS[0], NO_HOOKS[1], "update-ref", "-d", &self.name],
        );
    }
}

/// Remove what a crashed transfer can leave in a repository: export pin refs
/// (`refs/forge/export/*`) and import quarantine directories. Call it only
/// when no transfer is running on the repository (owner start). It touches
/// no branch, tag, imported `refs/forge/integration/*` ref or work tree.
/// Returns how many leftovers were removed.
pub fn sweep_transfer_leftovers(repo: &Path) -> usize {
    let git = |args: &[&str]| {
        std::process::Command::new("git")
            .args(args)
            .current_dir(repo)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .stdin(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .output()
            .ok()
            .filter(|output| output.status.success())
            .map(|output| String::from_utf8_lossy(&output.stdout).into_owned())
    };
    let mut removed = 0;
    if let Some(refs) = git(&["for-each-ref", "--format=%(refname)", EXPORT_REF_PREFIX]) {
        for name in refs
            .lines()
            .filter(|name| name.starts_with(EXPORT_REF_PREFIX))
        {
            if git(&[NO_HOOKS[0], NO_HOOKS[1], "update-ref", "-d", name]).is_some() {
                removed += 1;
            }
        }
    }
    let Some(common) = git(&["rev-parse", "--git-common-dir"]) else {
        return removed;
    };
    let common = repo.join(common.trim());
    if let Ok(entries) = std::fs::read_dir(&common) {
        for entry in entries.flatten() {
            if entry
                .file_name()
                .to_string_lossy()
                .starts_with(QUARANTINE_PREFIX)
                && entry.file_type().is_ok_and(|kind| kind.is_dir())
                && std::fs::remove_dir_all(entry.path()).is_ok()
            {
                removed += 1;
            }
        }
    }
    removed
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
    if let Some(bad) = have.iter().find(|sha| !is_object_id(sha)) {
        return Err(ObjectTransferError::Invalid {
            reason: format!("`have` entry is not a full object id: {bad:.80}"),
        });
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
    let output = crate::command_output(
        repo,
        &[NO_HOOKS[0], NO_HOOKS[1], "update-ref", &pinned.name, want],
    )
    .await?;
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
    let total_bytes = std::fs::metadata(dest)?.len();
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
///
/// Dropping the returned future cancels the import. Until the objects have
/// passed every check nothing outside the quarantine directory is written,
/// and the drop removes that directory after its Git processes are gone.
/// Publishing the checked objects and binding the ref then runs without an
/// await point, so a cancelled import either changed nothing or completed.
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
        let bytes = std::fs::metadata(bundle)?.len();
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
                "{QUARANTINE_PREFIX}{key}-{}-{nonce}",
                std::process::id()
            )),
            armed: true,
        };
        std::fs::create_dir_all(quarantine.path.join("pack"))?;
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
        for entry in std::fs::read_dir(quarantine.path.join("pack"))? {
            packs.push(entry?.path());
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
        packs.sort_by_key(|path| path.extension().is_some_and(|ext| ext == "idx"));
        return bind_imported(repo, &ref_name, expected_tip, Some((&objects, &packs)));
    }
    if !commit_exists(repo, expected_tip).await? {
        return Err(ObjectTransferError::Invalid {
            reason: "target does not hold the expected tip and no objects were sent".into(),
        });
    }
    bind_imported(repo, &ref_name, expected_tip, None)
}

/// The only step of an import that changes the repository: move the checked
/// packs into the object store and bind the transfer ref.
///
/// It is synchronous on purpose. With no await point it cannot be interrupted
/// by a dropped future, so cancellation never leaves published packs without
/// their ref, or a ref the caller was told had been cancelled. It is short:
/// renames inside one file system and two Git commands that run no hook.
fn bind_imported(
    repo: &Path,
    ref_name: &str,
    expected_tip: &str,
    publish: Option<(&Path, &[std::path::PathBuf])>,
) -> TransferResult<ObjectImport> {
    if let Some((objects, packs)) = publish {
        std::fs::create_dir_all(objects.join("pack"))?;
        for path in packs {
            let Some(name) = path.file_name() else {
                continue;
            };
            let dest = objects.join("pack").join(name);
            if dest.try_exists()? {
                continue;
            }
            std::fs::rename(path, &dest)?;
        }
    }
    let tip = format!("{expected_tip}^{{commit}}");
    if !git_blocking(repo, &["cat-file", "-e", &tip])?
        .status
        .success()
    {
        return Err(ObjectTransferError::Invalid {
            reason: "expected tip is missing after the import".into(),
        });
    }
    // Create-only: a concurrent import of another object under this key loses.
    let zero = "0".repeat(expected_tip.len());
    let output = git_blocking(
        repo,
        &[
            NO_HOOKS[0],
            NO_HOOKS[1],
            "update-ref",
            ref_name,
            expected_tip,
            &zero,
        ],
    )?;
    if output.status.success() {
        return Ok(ObjectImport {
            tip_sha: expected_tip.into(),
            ref_name: ref_name.into(),
            replayed: false,
        });
    }
    let bound = git_blocking(
        repo,
        &[
            "rev-parse",
            "--verify",
            "--quiet",
            "--end-of-options",
            ref_name,
        ],
    )?;
    let existing = String::from_utf8_lossy(&bound.stdout).trim().to_owned();
    match (bound.status.success(), existing == expected_tip) {
        (true, true) => Ok(ObjectImport {
            tip_sha: existing,
            ref_name: ref_name.into(),
            replayed: true,
        }),
        (true, false) => Err(ObjectTransferError::KeyConflict {
            existing_sha: existing,
        }),
        (false, _) => Err(invalid("could not bind the transfer ref", &output)),
    }
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

    /// Poll `transfer` at most `polls` times, a millisecond apart, and drop it
    /// immediately after the last poll. `None` when it was dropped unfinished.
    async fn drop_after_polls<T>(
        transfer: impl std::future::Future<Output = T>,
        polls: usize,
    ) -> Option<T> {
        use std::task::Poll;
        let mut transfer = Box::pin(transfer);
        for poll in 0..polls {
            if poll > 0 {
                tokio::time::sleep(std::time::Duration::from_millis(1)).await;
            }
            let polled =
                std::future::poll_fn(|context| Poll::Ready(transfer.as_mut().poll(context))).await;
            if let Poll::Ready(output) = polled {
                return Some(output);
            }
        }
        // Dropped right after a poll: whatever that poll started is under way.
        None
    }

    /// How many polls each run of the sweep below gets: every count up to 48,
    /// then steps of an eighth, so a slow machine costs time in proportion.
    fn poll_counts() -> impl Iterator<Item = usize> {
        std::iter::successors(Some(0usize), |polls| {
            Some(polls + (polls / 8).saturating_sub(5).max(1))
        })
        .take_while(|polls| *polls < 20_000)
    }

    /// Cancellation is a dropped future, and a future can be dropped at any
    /// of its await points. Drop one transfer after 0 polls, the next after
    /// 1, and so on until one finishes, and compare the repository the
    /// moment each drop returns.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_transfer_dropped_at_any_await_point_leaves_nothing_behind() {
        let pair = pair();
        let bundle = pair.staging.join("a.bundle");
        let lock = pair.staging.join("a.bundle.lock");
        let ref_lock = pair.source.join(".git/refs/forge/export/k.lock");
        let source_before = snapshot(&pair.source);
        let mut finished = false;
        for polls in poll_counts() {
            let export = export_objects(&pair.source, "k", &[], &pair.tip, &bundle, u64::MAX);
            if let Some(result) = drop_after_polls(export, polls).await {
                result.unwrap();
                finished = true;
                break;
            }
            assert!(!bundle.exists(), "export dropped after {polls} polls");
            assert!(!lock.exists(), "export dropped after {polls} polls");
            assert!(!ref_lock.exists(), "export dropped after {polls} polls");
            assert_eq!(snapshot(&pair.source), source_before, "{polls} polls");
        }
        assert!(finished, "the export never finished");
        assert_eq!(snapshot(&pair.source), source_before);

        let target_before = snapshot(&pair.target);
        let mut finished = false;
        for polls in poll_counts() {
            let import = import_objects(&pair.target, "k", Some(&bundle), &pair.tip, u64::MAX);
            if let Some(result) = drop_after_polls(import, polls).await {
                assert!(!result.unwrap().replayed);
                finished = true;
                break;
            }
            assert_eq!(
                snapshot(&pair.target),
                target_before,
                "import dropped after {polls} polls"
            );
        }
        assert!(finished, "the import never finished");
        assert_eq!(
            git(&pair.target, &["rev-parse", "refs/forge/integration/k"]),
            pair.tip
        );
        git(&pair.target, &["fsck", "--strict", "--no-dangling"]);
    }

    /// Bytes Git cannot compress, so the bundle is larger than a pipe buffer.
    fn noise(len: usize) -> Vec<u8> {
        let mut state = 0x9e37_79b9_7f4a_7c15_u64;
        (0..len)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                (state >> 32) as u8
            })
            .collect()
    }

    /// The import is cancelled while its Git child is stalled in the middle
    /// of unpacking, holding an open pack file inside the quarantine
    /// directory. The bundle is a FIFO fed by the test, so the child stalls
    /// exactly there. When the drop returns the child must be gone (the FIFO
    /// has no reader left) and the quarantine directory with it.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_import_dropped_while_its_child_holds_the_quarantine_leaves_nothing() {
        use std::io::Write;
        let pair = pair();
        std::fs::write(pair.source.join("big.bin"), noise(1024 * 1024)).unwrap();
        git(&pair.source, &["add", "."]);
        git(&pair.source, &["commit", "-q", "-m", "big"]);
        let tip = git(&pair.source, &["rev-parse", "HEAD"]);
        let bundle = pair.staging.join("a.bundle");
        export_objects(&pair.source, "k", &[], &tip, &bundle, u64::MAX)
            .await
            .unwrap();
        let bytes = std::fs::read(&bundle).unwrap();
        assert!(bytes.len() > 512 * 1024, "{} bytes", bytes.len());
        // The import opens the bundle three times: verify, list-heads, unbundle.
        // `slow.bundle` is a link the feeder points at a fresh FIFO before it
        // answers each open, so every Git command reads its own pipe. The
        // first two read the header and close, which ends that write with a
        // broken pipe. The third is fed half the pack and then nothing.
        let slow = pair.staging.join("slow.bundle");
        let fifos: Vec<PathBuf> = (0..3)
            .map(|index| pair.staging.join(format!("fifo-{index}")))
            .collect();
        for fifo in &fifos {
            assert!(std::process::Command::new("mkfifo")
                .arg(fifo)
                .status()
                .unwrap()
                .success());
        }
        std::os::unix::fs::symlink(&fifos[0], &slow).unwrap();
        let (stalled_tx, stalled_rx) = std::sync::mpsc::channel::<()>();
        let (resume_tx, resume_rx) = std::sync::mpsc::channel::<()>();
        let (link, staging, half) = (slow.clone(), pair.staging.clone(), bytes.len() / 2);
        let feeder = std::thread::spawn(move || {
            // Blocks until a Git command has opened this FIFO for reading.
            let open = |fifo: &Path| std::fs::OpenOptions::new().write(true).open(fifo).unwrap();
            for index in 0..2 {
                let mut pipe = open(&fifos[index]);
                // The reader cannot finish before it is fed, so the next
                // command finds the link already pointing at the next FIFO.
                let next = staging.join("next");
                std::os::unix::fs::symlink(&fifos[index + 1], &next).unwrap();
                std::fs::rename(&next, &link).unwrap();
                let _ = pipe.write_all(&bytes);
            }
            let mut pipe = open(&fifos[2]);
            pipe.write_all(&bytes[..half]).unwrap();
            stalled_tx.send(()).unwrap();
            resume_rx.recv().unwrap();
            // With no reader left the rest cannot be written.
            pipe.write_all(&bytes[half..]).map_err(|error| error.kind())
        });

        let target_before = snapshot(&pair.target);
        let git_dir = pair.target.join(".git");
        let child_holds_quarantine = async {
            loop {
                let holding = stalled_rx.try_recv().is_ok()
                    && std::fs::read_dir(&git_dir).unwrap().flatten().any(|entry| {
                        entry
                            .file_name()
                            .to_string_lossy()
                            .starts_with("forge-incoming-")
                            && std::fs::read_dir(entry.path().join("pack"))
                                .is_ok_and(|pack| pack.count() > 0)
                    });
                if holding {
                    return;
                }
                tokio::time::sleep(std::time::Duration::from_millis(2)).await;
            }
        };
        // `select!` drops the unfinished import when the other branch wins.
        tokio::select! {
            result = import_objects(&pair.target, "k", Some(&slow), &tip, u64::MAX) => {
                panic!("the import finished on half a bundle: {result:?}")
            }
            _ = child_holds_quarantine => {}
            _ = tokio::time::sleep(std::time::Duration::from_secs(60)) => {
                panic!("the import never reached its unpack step")
            }
        }
        assert_eq!(snapshot(&pair.target), target_before);
        resume_tx.send(()).unwrap();
        assert_eq!(
            feeder.join().unwrap(),
            Err(std::io::ErrorKind::BrokenPipe),
            "a Git child still had the bundle open after the drop returned"
        );
        assert_eq!(snapshot(&pair.target), target_before);
        assert_eq!(sweep_transfer_leftovers(&pair.target), 0);
    }

    /// A hook that would fire on any ref write or object arrival.
    fn arm_hooks(repo: &Path, marker: &Path) {
        use std::os::unix::fs::PermissionsExt;
        let hooks = repo.join(".git/hooks");
        std::fs::create_dir_all(&hooks).unwrap();
        for name in [
            "reference-transaction",
            "post-update",
            "pre-receive",
            "update",
            "post-receive",
            "post-checkout",
            "post-merge",
        ] {
            let path = hooks.join(name);
            std::fs::write(
                &path,
                format!("#!/bin/sh\necho {name} >> '{}'\n", marker.display()),
            )
            .unwrap();
            std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
        }
    }

    #[tokio::test]
    async fn a_transfer_runs_no_repository_hook_or_configured_program() {
        let pair = pair();
        let marker = pair.staging.join("ran");
        for repo in [&pair.source, &pair.target] {
            arm_hooks(repo, &marker);
            // Configuration that names a program: none of it may be started.
            for (key, value) in [
                ("core.fsmonitor", "touch"),
                ("core.sshCommand", "touch"),
                ("uploadpack.packObjectsHook", "touch"),
                ("filter.x.clean", "touch"),
                ("filter.x.smudge", "touch"),
            ] {
                let value = format!("{value} '{}' #", marker.display());
                git(repo, &["config", key, &value]);
            }
        }
        let bundle = pair.staging.join("a.bundle");
        let have = vec![pair.base.clone()];
        export_objects(&pair.source, "k", &have, &pair.tip, &bundle, u64::MAX)
            .await
            .unwrap();
        import_objects(&pair.target, "k", Some(&bundle), &pair.tip, u64::MAX)
            .await
            .unwrap();
        import_objects(&pair.target, "k", Some(&bundle), &pair.tip, u64::MAX)
            .await
            .unwrap();
        assert_eq!(sweep_transfer_leftovers(&pair.source), 0);
        assert!(
            !marker.exists(),
            "ran: {}",
            std::fs::read_to_string(&marker).unwrap_or_default()
        );
    }

    #[tokio::test]
    async fn malformed_have_and_want_are_refused_before_git_reads_them() {
        let pair = pair();
        let bundle = pair.staging.join("a.bundle");
        for have in ["--all", "main", "HEAD~1", "../x", &pair.base[..39]] {
            let result = export_objects(
                &pair.source,
                "k",
                &[have.to_owned()],
                &pair.tip,
                &bundle,
                u64::MAX,
            )
            .await;
            assert!(
                matches!(result, Err(ObjectTransferError::Invalid { .. })),
                "{have}: {result:?}"
            );
        }
        for want in ["--all", "main", "refs/heads/main", ""] {
            let result = export_objects(&pair.source, "k", &[], want, &bundle, u64::MAX).await;
            assert!(
                matches!(result, Err(ObjectTransferError::MissingObject { .. })),
                "{want}: {result:?}"
            );
        }
        assert!(!bundle.exists());
        assert_eq!(git(&pair.source, &["for-each-ref", "refs/forge"]), "");
    }

    #[tokio::test]
    async fn the_start_sweep_removes_crash_leftovers_and_nothing_else() {
        let pair = pair();
        let bundle = pair.staging.join("a.bundle");
        export_objects(&pair.source, "k", &[], &pair.tip, &bundle, u64::MAX)
            .await
            .unwrap();
        import_objects(&pair.target, "k", Some(&bundle), &pair.tip, u64::MAX)
            .await
            .unwrap();
        let clean = snapshot(&pair.target);
        // What a process killed mid-export and mid-import leaves behind.
        git(
            &pair.target,
            &["update-ref", "refs/forge/export/dead", &pair.base],
        );
        let quarantine = pair.target.join(".git/forge-incoming-dead-1-2");
        std::fs::create_dir_all(quarantine.join("pack")).unwrap();
        std::fs::write(quarantine.join("pack/x.pack"), b"partial").unwrap();
        assert_ne!(snapshot(&pair.target), clean);
        assert_eq!(sweep_transfer_leftovers(&pair.target), 2);
        assert_eq!(snapshot(&pair.target), clean);
        assert_eq!(sweep_transfer_leftovers(&pair.target), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_imports_of_one_key_bind_one_ref_and_the_rest_replay() {
        let pair = pair();
        let bundle = pair.staging.join("a.bundle");
        export_objects(&pair.source, "k", &[], &pair.tip, &bundle, u64::MAX)
            .await
            .unwrap();
        let mut runs = Vec::new();
        for _ in 0..4 {
            let (target, bundle, tip) = (pair.target.clone(), bundle.clone(), pair.tip.clone());
            runs.push(tokio::spawn(async move {
                import_objects(&target, "k", Some(&bundle), &tip, u64::MAX).await
            }));
        }
        let mut fresh = 0;
        for run in runs {
            let import = run.await.unwrap().unwrap();
            assert_eq!(import.tip_sha, pair.tip);
            fresh += usize::from(!import.replayed);
        }
        assert_eq!(fresh, 1, "exactly one import binds the ref");
        git(&pair.target, &["fsck", "--strict", "--no-dangling"]);
        assert_eq!(
            git(
                &pair.target,
                &["for-each-ref", "--format=%(refname)", "refs/forge"]
            ),
            "refs/forge/integration/k"
        );
        // Another object under the same key is a conflict, never a move.
        assert!(matches!(
            import_objects(&pair.target, "k", None, &pair.base, u64::MAX).await,
            Err(ObjectTransferError::KeyConflict { .. })
        ));
        let leftovers: Vec<_> = std::fs::read_dir(pair.target.join(".git"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .filter(|name| name.starts_with("forge-incoming-"))
            .collect();
        assert!(leftovers.is_empty(), "{leftovers:?}");
    }
}
