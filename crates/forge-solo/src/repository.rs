//! Git repository identity and Solo marker handling.
//!
//! Repository discovery is intentionally kept separate from the terminal and
//! database layers. A successful [`resolve_git_repository`] call is read-only;
//! [`read_or_create_marker`] is the first operation in this crate that may
//! write anything, and callers should run CLI terminal preflight first.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, SystemTime};

use uuid::Uuid;

/// Stable marker filename stored in the Git common directory.
pub const SOLO_ID_MARKER_FILE: &str = "forge-solo-id";
/// Current on-disk marker format.
pub const SOLO_ID_MARKER_VERSION: u32 = 1;
const MAX_MARKER_BYTES: u64 = 4096;
const MARKER_RETRY_ATTEMPTS: usize = 12;
const MARKER_RETRY_DELAY: Duration = Duration::from_millis(5);

/// A primary Git worktree and the metadata needed to bind Solo state to it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GitRepository {
    /// Canonical path of the primary worktree.
    pub worktree_root: PathBuf,
    /// Canonical Git common directory (normally `<worktree>/.git`).
    pub git_common_dir: PathBuf,
    /// The branch checked out in the primary worktree, if HEAD is attached.
    /// A detached HEAD is left to the caller to handle because the repository
    /// itself is still unambiguously identified.
    pub default_branch: Option<String>,
}

impl GitRepository {
    #[must_use]
    pub fn marker_path(&self) -> PathBuf {
        marker_path(&self.git_common_dir)
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.worktree_root
    }

    #[must_use]
    pub fn common_dir(&self) -> &Path {
        &self.git_common_dir
    }
}

/// A primary worktree together with its stable Solo repository identifier.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SoloRepository {
    pub git: GitRepository,
    pub repository_id: Uuid,
}

impl SoloRepository {
    #[must_use]
    pub fn worktree_root(&self) -> &Path {
        self.git.root()
    }

    #[must_use]
    pub fn git_common_dir(&self) -> &Path {
        self.git.common_dir()
    }

    #[must_use]
    pub fn default_branch(&self) -> Option<&str> {
        self.git.default_branch.as_deref()
    }

    #[must_use]
    pub fn marker_path(&self) -> PathBuf {
        self.git.marker_path()
    }
}

/// Versioned content written to the Git common directory.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SoloIdMarker {
    pub version: u32,
    pub repository_id: Uuid,
}

impl SoloIdMarker {
    #[must_use]
    pub const fn new(repository_id: Uuid) -> Self {
        Self {
            version: SOLO_ID_MARKER_VERSION,
            repository_id,
        }
    }

    /// The exact two-line format is part of the Solo on-disk contract.
    #[must_use]
    pub fn encode(self) -> String {
        format!(
            "version = {}\nrepository_id = \"{}\"\n",
            self.version, self.repository_id
        )
    }
}

/// Errors returned while resolving the source repository or its marker.
#[derive(Debug, thiserror::Error)]
pub enum RepositoryError {
    #[error("launch path does not exist: {path}")]
    PathNotFound { path: PathBuf },

    #[error("launch path is not a directory: {path}")]
    NotDirectory { path: PathBuf },

    #[error(
        "forge-solo requires an existing Git repository; no worktree was found at or above {path}"
    )]
    NotGitRepository { path: PathBuf },

    #[error("Git repository at {path} is bare and has no primary worktree")]
    BareRepository { path: PathBuf },

    #[error(
        "forge-solo cannot launch from linked worktree {worktree}; launch from the primary worktree {primary_worktree}"
    )]
    LinkedWorktree {
        worktree: PathBuf,
        primary_worktree: PathBuf,
    },

    #[error(
        "forge-solo cannot launch from Forge-managed worktree {worktree}; launch from the primary worktree {primary_worktree}"
    )]
    ManagedWorktree {
        worktree: PathBuf,
        primary_worktree: PathBuf,
    },

    #[error("Git metadata directory is unavailable: {path}")]
    InvalidCommonDirectory { path: PathBuf },

    #[error("Git command failed: {command}: {stderr}")]
    GitCommand { command: String, stderr: String },

    #[error("failed to run Git: {0}")]
    GitIo(#[source] io::Error),

    #[error("failed to inspect repository marker {path}: {source}")]
    MarkerIo { path: PathBuf, source: io::Error },

    #[error("repository marker is missing: {path}")]
    MarkerMissing { path: PathBuf },

    #[error("repository marker is a symlink, which is not allowed: {path}")]
    MarkerSymlink { path: PathBuf },

    #[error("repository marker is not a regular file: {path}")]
    MarkerNotRegular { path: PathBuf },

    #[error("repository marker has insecure permissions (must be owner-only): {path}")]
    MarkerPermissions { path: PathBuf },

    #[error("repository marker is malformed at {path}: {reason}")]
    MarkerMalformed { path: PathBuf, reason: String },

    #[error(
        "repository marker format version {version} is unsupported at {path}; expected version {expected}"
    )]
    MarkerVersion {
        path: PathBuf,
        version: u32,
        expected: u32,
    },

    #[error("repository marker identifier is not valid at {path}: {reason}")]
    MarkerIdentifier { path: PathBuf, reason: String },
}

/// Result type returned by repository discovery and marker operations.
pub type Result<T> = std::result::Result<T, RepositoryError>;

/// Resolve a path to the primary Git worktree without creating a marker.
pub fn resolve_git_repository(path: &Path) -> Result<GitRepository> {
    let launch_path = canonical_directory(path)?;

    let inside = match run_git(&launch_path, &["rev-parse", "--is-inside-work-tree"]) {
        Ok(value) => value,
        Err(RepositoryError::GitCommand { .. }) => {
            return Err(RepositoryError::NotGitRepository { path: launch_path });
        }
        Err(error) => return Err(error),
    };
    if inside.trim() != "true" {
        return Err(RepositoryError::NotGitRepository { path: launch_path });
    }

    let bare = run_git(&launch_path, &["rev-parse", "--is-bare-repository"])?;
    if bare.trim() == "true" {
        return Err(RepositoryError::BareRepository { path: launch_path });
    }

    let worktree_root = canonical_git_path(
        &launch_path,
        &run_git(&launch_path, &["rev-parse", "--show-toplevel"])?,
    )?;
    let git_common_dir = canonical_git_path(
        &launch_path,
        &run_git(&launch_path, &["rev-parse", "--git-common-dir"])?,
    )?;
    let git_common_metadata = fs::symlink_metadata(&git_common_dir).map_err(|source| {
        if source.kind() == io::ErrorKind::NotFound {
            RepositoryError::InvalidCommonDirectory {
                path: git_common_dir.clone(),
            }
        } else {
            RepositoryError::MarkerIo {
                path: git_common_dir.clone(),
                source,
            }
        }
    })?;
    if !git_common_metadata.is_dir() {
        return Err(RepositoryError::InvalidCommonDirectory {
            path: git_common_dir,
        });
    }

    let primary_worktree =
        primary_worktree_from_list(&launch_path)?.unwrap_or_else(|| worktree_root.clone());
    let primary_worktree = fs::canonicalize(&primary_worktree).map_err(|source| {
        if source.kind() == io::ErrorKind::NotFound {
            RepositoryError::InvalidCommonDirectory {
                path: primary_worktree.clone(),
            }
        } else {
            RepositoryError::GitIo(source)
        }
    })?;

    if managed_worktree_root(&worktree_root).is_some() {
        return Err(RepositoryError::ManagedWorktree {
            worktree: worktree_root,
            primary_worktree,
        });
    }

    if worktree_root != primary_worktree {
        return Err(RepositoryError::LinkedWorktree {
            worktree: worktree_root,
            primary_worktree,
        });
    }

    // A linked worktree normally has a `.git` file and a git-dir below
    // `<common>/worktrees`. The worktree-list comparison above is the
    // authoritative check; this extra guard handles repositories where Git
    // cannot enumerate a partially-created linked worktree.
    let git_dir = canonical_git_path(
        &launch_path,
        &run_git(&launch_path, &["rev-parse", "--git-dir"])?,
    )?;
    if git_dir != git_common_dir
        && git_dir
            .components()
            .any(|component| component.as_os_str() == "worktrees")
    {
        return Err(RepositoryError::LinkedWorktree {
            worktree: worktree_root,
            primary_worktree,
        });
    }

    let default_branch = match run_git(
        &launch_path,
        &["symbolic-ref", "--quiet", "--short", "HEAD"],
    ) {
        Ok(branch) if !branch.trim().is_empty() => Some(branch.trim().to_owned()),
        Err(RepositoryError::GitCommand { .. }) => None,
        Err(error) => return Err(error),
        _ => None,
    };

    Ok(GitRepository {
        worktree_root,
        git_common_dir,
        default_branch,
    })
}

/// Resolve a repository and read/create its stable Solo identifier.
pub fn resolve_repository(path: &Path) -> Result<SoloRepository> {
    let git = resolve_git_repository(path)?;
    let marker = read_or_create_marker(&git.git_common_dir)?;
    Ok(SoloRepository {
        git,
        repository_id: marker.repository_id,
    })
}

/// Return the marker path for a canonical Git common directory.
#[must_use]
pub fn marker_path(git_common_dir: &Path) -> PathBuf {
    git_common_dir.join(SOLO_ID_MARKER_FILE)
}

/// Read an existing marker without creating one.
pub fn read_marker(git_common_dir: &Path) -> Result<SoloIdMarker> {
    validate_common_directory(git_common_dir)?;
    let path = marker_path(git_common_dir);
    validate_marker_metadata(&path)?;

    // Re-check the path after opening it, then use the opened descriptor for
    // all subsequent metadata and reads.  The descriptor keeps the bounded
    // read tied to one inode instead of a path that can be replaced between
    // `metadata` and `open`.
    let file = File::open(&path).map_err(|source| RepositoryError::MarkerIo {
        path: path.clone(),
        source,
    })?;
    validate_marker_metadata(&path)?;
    let metadata = file
        .metadata()
        .map_err(|source| RepositoryError::MarkerIo {
            path: path.clone(),
            source,
        })?;
    validate_marker_file_metadata(&path, &metadata)?;
    let mut bytes = Vec::new();
    file.take(MAX_MARKER_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|source| RepositoryError::MarkerIo {
            path: path.clone(),
            source,
        })?;
    if bytes.len() as u64 > MAX_MARKER_BYTES {
        return Err(RepositoryError::MarkerMalformed {
            path: path.clone(),
            reason: format!("file exceeds {MAX_MARKER_BYTES} bytes"),
        });
    }

    let contents = String::from_utf8(bytes).map_err(|error| RepositoryError::MarkerMalformed {
        path: path.clone(),
        reason: format!("marker is not valid UTF-8: {error}"),
    })?;
    parse_marker(&path, &contents)
}

/// Read the existing marker or atomically claim the marker path with a fresh
/// UUID.  The marker is first completed and synced under a private temporary
/// name, then published with same-directory hard-link creation.  Hard-link
/// creation is an atomic no-replace operation on the supported local filesystems;
/// it also avoids exposing partially-written marker contents to readers.
pub fn read_or_create_marker(git_common_dir: &Path) -> Result<SoloIdMarker> {
    validate_common_directory(git_common_dir)?;
    let path = marker_path(git_common_dir);

    for attempt in 0..=MARKER_RETRY_ATTEMPTS {
        match read_marker(git_common_dir) {
            Ok(marker) => return Ok(marker),
            Err(RepositoryError::MarkerMissing { .. }) if attempt < MARKER_RETRY_ATTEMPTS => {
                let marker = SoloIdMarker::new(Uuid::new_v4());
                match write_marker_atomically(git_common_dir, &path, marker) {
                    Ok(()) => return Ok(marker),
                    Err(source) if source.kind() == io::ErrorKind::AlreadyExists => {
                        // Another process won the no-replace publication race.
                        // Re-read its complete marker on the next iteration.
                    }
                    Err(source) => {
                        return Err(RepositoryError::MarkerIo {
                            path: path.clone(),
                            source,
                        });
                    }
                }
            }
            Err(error) if should_retry_marker(&path, &error, attempt) => {
                std::thread::sleep(MARKER_RETRY_DELAY);
            }
            Err(error) => return Err(error),
        }
    }

    read_marker(git_common_dir)
}

fn write_marker_atomically(
    git_common_dir: &Path,
    path: &Path,
    marker: SoloIdMarker,
) -> io::Result<()> {
    let temporary_path =
        git_common_dir.join(format!(".{SOLO_ID_MARKER_FILE}.{}.tmp", Uuid::new_v4()));
    let result = (|| {
        let mut options = OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);

        let mut file = options.open(&temporary_path)?;
        file.write_all(marker.encode().as_bytes())?;
        file.sync_all()?;
        set_private_permissions(&file)?;
        file.sync_all()?;
        drop(file);

        // Unlike rename, hard_link never replaces an existing destination.
        // That preserves the first successfully published repository ID.
        fs::hard_link(&temporary_path, path)?;
        sync_directory(git_common_dir)
    })();

    match result {
        Ok(()) => fs::remove_file(&temporary_path),
        Err(error) => {
            let _ = fs::remove_file(&temporary_path);
            Err(error)
        }
    }
}

/*
 * Keep this helper close to the path-based validator: the latter rejects
 * symlinks before opening, while this one validates the descriptor actually
 * used for the bounded read.  On Unix, permissions are read from that same
 * descriptor rather than from a raceable pathname.
 */
fn validate_marker_file_metadata(path: &Path, metadata: &fs::Metadata) -> Result<()> {
    if !metadata.is_file() {
        return Err(RepositoryError::MarkerNotRegular {
            path: path.to_path_buf(),
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        if metadata.permissions().mode() & 0o077 != 0 {
            return Err(RepositoryError::MarkerPermissions {
                path: path.to_path_buf(),
            });
        }
    }
    Ok(())
}

/// Read/create the stable repository identifier when callers do not need the
/// marker version in the result.
pub fn read_or_create_repository_id(git_common_dir: &Path) -> Result<Uuid> {
    Ok(read_or_create_marker(git_common_dir)?.repository_id)
}

fn parse_marker(path: &Path, contents: &str) -> Result<SoloIdMarker> {
    if !contents.ends_with('\n') {
        return Err(RepositoryError::MarkerMalformed {
            path: path.to_path_buf(),
            reason: "marker must end with a newline".to_owned(),
        });
    }
    let contents = contents.strip_suffix('\n').unwrap_or(contents);
    let mut lines = contents.split('\n');
    let version_line = lines.next().unwrap_or_default();
    let id_line = lines.next().unwrap_or_default();
    if lines.next().is_some() || version_line.is_empty() || id_line.is_empty() {
        return Err(RepositoryError::MarkerMalformed {
            path: path.to_path_buf(),
            reason: "expected exactly version and repository_id lines".to_owned(),
        });
    }

    let version = version_line
        .strip_prefix("version = ")
        .ok_or_else(|| RepositoryError::MarkerMalformed {
            path: path.to_path_buf(),
            reason: "first line must be `version = <number>`".to_owned(),
        })?
        .parse::<u32>()
        .map_err(|_| RepositoryError::MarkerMalformed {
            path: path.to_path_buf(),
            reason: "marker version is not an unsigned integer".to_owned(),
        })?;
    if version != SOLO_ID_MARKER_VERSION {
        return Err(RepositoryError::MarkerVersion {
            path: path.to_path_buf(),
            version,
            expected: SOLO_ID_MARKER_VERSION,
        });
    }

    let id_text = id_line
        .strip_prefix("repository_id = \"")
        .and_then(|value| value.strip_suffix('"'))
        .ok_or_else(|| RepositoryError::MarkerMalformed {
            path: path.to_path_buf(),
            reason: "second line must be `repository_id = \"<uuid>\"`".to_owned(),
        })?;
    let repository_id =
        Uuid::parse_str(id_text).map_err(|error| RepositoryError::MarkerIdentifier {
            path: path.to_path_buf(),
            reason: error.to_string(),
        })?;
    if repository_id.is_nil() {
        return Err(RepositoryError::MarkerIdentifier {
            path: path.to_path_buf(),
            reason: "identifier must not be the nil UUID".to_owned(),
        });
    }
    if repository_id.to_string() != id_text {
        return Err(RepositoryError::MarkerIdentifier {
            path: path.to_path_buf(),
            reason: "identifier must use canonical lowercase UUID spelling".to_owned(),
        });
    }

    Ok(SoloIdMarker {
        version,
        repository_id,
    })
}

fn canonical_directory(path: &Path) -> Result<PathBuf> {
    let metadata = fs::metadata(path).map_err(|source| {
        if source.kind() == io::ErrorKind::NotFound {
            RepositoryError::PathNotFound {
                path: path.to_path_buf(),
            }
        } else {
            RepositoryError::GitIo(source)
        }
    })?;
    if !metadata.is_dir() {
        return Err(RepositoryError::NotDirectory {
            path: path.to_path_buf(),
        });
    }
    fs::canonicalize(path).map_err(RepositoryError::GitIo)
}

fn canonical_git_path(base: &Path, raw: &str) -> Result<PathBuf> {
    let raw_path = Path::new(raw.trim());
    let candidate = if raw_path.is_absolute() {
        raw_path.to_path_buf()
    } else {
        base.join(raw_path)
    };
    fs::canonicalize(&candidate).map_err(|source| {
        if source.kind() == io::ErrorKind::NotFound {
            RepositoryError::InvalidCommonDirectory { path: candidate }
        } else {
            RepositoryError::GitIo(source)
        }
    })
}

fn run_git(cwd: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .env_remove("GIT_DIR")
        .env_remove("GIT_WORK_TREE")
        .env_remove("GIT_INDEX_FILE")
        .output()
        .map_err(RepositoryError::GitIo)?;
    if !output.status.success() {
        return Err(RepositoryError::GitCommand {
            command: format!("git {}", args.join(" ")),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }
    Ok(String::from_utf8_lossy(&output.stdout).trim().to_owned())
}

fn primary_worktree_from_list(path: &Path) -> Result<Option<PathBuf>> {
    let output = run_git(path, &["worktree", "list", "--porcelain"])?;
    Ok(output.lines().find_map(|line| {
        line.strip_prefix("worktree ")
            .filter(|value| !value.trim().is_empty())
            .map(|value| PathBuf::from(value.trim()))
    }))
}

fn managed_worktree_root(worktree: &Path) -> Option<PathBuf> {
    if let Some(configured_root) = std::env::var_os("FORGE_WORKSPACE_ROOT") {
        let configured_root = PathBuf::from(configured_root);
        if let Ok(configured_root) = fs::canonicalize(configured_root) {
            if worktree.starts_with(&configured_root) && worktree != configured_root {
                return Some(configured_root);
            }
        }
    }

    // WorkspaceManager places `.forge.lock` beside each task worktree. Walk
    // only ancestors of the resolved root so a tracked `.forge.lock` inside
    // the source checkout cannot trigger this check accidentally.
    let mut cursor = worktree.parent();
    while let Some(path) = cursor {
        let lock = path.join(".forge.lock");
        if fs::symlink_metadata(&lock)
            .map(|metadata| metadata.is_file())
            .unwrap_or(false)
        {
            return Some(path.to_path_buf());
        }
        cursor = path.parent();
    }
    None
}

fn validate_common_directory(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(|source| {
        if source.kind() == io::ErrorKind::NotFound {
            RepositoryError::InvalidCommonDirectory {
                path: path.to_path_buf(),
            }
        } else {
            RepositoryError::MarkerIo {
                path: path.to_path_buf(),
                source,
            }
        }
    })?;
    if metadata.file_type().is_symlink() || !metadata.is_dir() {
        return Err(RepositoryError::InvalidCommonDirectory {
            path: path.to_path_buf(),
        });
    }
    Ok(())
}

fn validate_marker_metadata(path: &Path) -> Result<()> {
    let metadata = fs::symlink_metadata(path).map_err(|source| {
        if source.kind() == io::ErrorKind::NotFound {
            RepositoryError::MarkerMissing {
                path: path.to_path_buf(),
            }
        } else {
            RepositoryError::MarkerIo {
                path: path.to_path_buf(),
                source,
            }
        }
    })?;
    if metadata.file_type().is_symlink() {
        return Err(RepositoryError::MarkerSymlink {
            path: path.to_path_buf(),
        });
    }
    validate_marker_file_metadata(path, &metadata)
}

fn set_private_permissions(file: &File) -> io::Result<()> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        file.set_permissions(fs::Permissions::from_mode(0o600))?;
    }
    Ok(())
}

fn sync_directory(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        File::open(path).and_then(|file| file.sync_all())?;
    }
    Ok(())
}

fn should_retry_marker(path: &Path, error: &RepositoryError, attempt: usize) -> bool {
    if attempt >= MARKER_RETRY_ATTEMPTS {
        return false;
    }
    if !matches!(error, RepositoryError::MarkerMalformed { .. }) {
        return false;
    }
    fs::metadata(path)
        .and_then(|metadata| metadata.modified())
        .ok()
        .and_then(|modified| SystemTime::now().duration_since(modified).ok())
        .is_some_and(|age| age <= Duration::from_secs(1))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Stdio;
    use tempfile::TempDir;

    fn git(cwd: &Path, args: &[&str]) {
        let output = Command::new("git")
            .args(args)
            .current_dir(cwd)
            .env_remove("GIT_DIR")
            .env_remove("GIT_WORK_TREE")
            .env_remove("GIT_INDEX_FILE")
            .stdin(Stdio::null())
            .output()
            .expect("git should be installed");
        assert!(
            output.status.success(),
            "git {:?} failed: {}",
            args,
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn repository() -> (TempDir, PathBuf) {
        let root = TempDir::new().expect("temporary root");
        let repo = root.path().join("repo");
        fs::create_dir(&repo).expect("repo directory");
        git(&repo, &["init", "-b", "main"]);
        git(&repo, &["config", "user.email", "solo@example.test"]);
        git(&repo, &["config", "user.name", "Solo Test"]);
        fs::write(repo.join("README.md"), "# Solo\n").expect("seed file");
        git(&repo, &["add", "README.md"]);
        git(&repo, &["commit", "-m", "initial"]);
        (root, repo)
    }

    #[test]
    fn resolves_nested_path_to_primary_worktree_and_branch() {
        let (_root, repo) = repository();
        let nested = repo.join("src");
        fs::create_dir(&nested).expect("nested directory");

        let resolved = resolve_git_repository(&nested).expect("repository resolves");
        assert_eq!(resolved.worktree_root, fs::canonicalize(repo).unwrap());
        assert_eq!(resolved.default_branch.as_deref(), Some("main"));
        assert!(resolved.git_common_dir.ends_with(".git"));
    }

    #[test]
    fn creates_exact_versioned_marker_once() {
        let (_root, repo) = repository();
        let resolved = resolve_git_repository(&repo).expect("repository resolves");
        let first = read_or_create_marker(&resolved.git_common_dir).expect("marker creates");
        let marker_file = resolved.marker_path();
        assert_eq!(
            fs::read_to_string(&marker_file).unwrap(),
            format!("version = 1\nrepository_id = \"{}\"\n", first.repository_id)
        );
        let second = read_or_create_marker(&resolved.git_common_dir).expect("marker resumes");
        assert_eq!(first, second);
    }

    #[test]
    fn resolve_repository_returns_stable_id() {
        let (_root, repo) = repository();
        let first = resolve_repository(&repo).expect("first resolve");
        let second = resolve_repository(&repo).expect("second resolve");
        assert_eq!(first.repository_id, second.repository_id);
        assert_eq!(first.git, second.git);
    }

    #[test]
    fn non_git_path_fails_before_marker_creation() {
        let root = TempDir::new().expect("temporary root");
        let error = resolve_repository(root.path()).expect_err("not a git repository");
        assert!(matches!(error, RepositoryError::NotGitRepository { .. }));
        assert!(!root.path().join(SOLO_ID_MARKER_FILE).exists());
    }

    #[test]
    fn malformed_marker_fails_closed() {
        let (_root, repo) = repository();
        let resolved = resolve_git_repository(&repo).expect("repository resolves");
        let marker_file = resolved.marker_path();
        fs::write(
            &marker_file,
            "version = 1\nrepository_id = \"not-a-uuid\"\n",
        )
        .expect("marker write");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&marker_file, fs::Permissions::from_mode(0o600))
                .expect("marker permissions");
        }
        let error = read_or_create_marker(&resolved.git_common_dir)
            .expect_err("malformed marker must not be replaced");
        assert!(matches!(error, RepositoryError::MarkerIdentifier { .. }));
    }

    #[test]
    fn marker_without_required_trailing_newline_fails_closed() {
        let (_root, repo) = repository();
        let resolved = resolve_git_repository(&repo).expect("repository resolves");
        let marker_file = resolved.marker_path();
        fs::write(
            &marker_file,
            "version = 1\nrepository_id = \"00000000-0000-4000-8000-000000000001\"",
        )
        .expect("marker write");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&marker_file, fs::Permissions::from_mode(0o600))
                .expect("marker permissions");
        }
        let error = read_marker(&resolved.git_common_dir)
            .expect_err("exact marker format requires trailing newline");
        assert!(matches!(error, RepositoryError::MarkerMalformed { .. }));
    }

    #[cfg(unix)]
    #[test]
    fn marker_symlink_is_rejected_without_following_it() {
        let (_root, repo) = repository();
        let resolved = resolve_git_repository(&repo).expect("repository resolves");
        let target = resolved.git_common_dir.join("real-marker");
        fs::write(
            &target,
            "version = 1\nrepository_id = \"00000000-0000-4000-8000-000000000001\"\n",
        )
        .expect("target write");
        std::os::unix::fs::symlink(&target, resolved.marker_path()).expect("symlink");
        let error = read_or_create_marker(&resolved.git_common_dir)
            .expect_err("symlink marker must be rejected");
        assert!(matches!(error, RepositoryError::MarkerSymlink { .. }));
    }

    #[cfg(unix)]
    #[test]
    fn marker_permissions_are_owner_only() {
        use std::os::unix::fs::PermissionsExt;

        let (_root, repo) = repository();
        let resolved = resolve_git_repository(&repo).expect("repository resolves");
        let marker = read_or_create_marker(&resolved.git_common_dir).expect("marker creates");
        fs::set_permissions(resolved.marker_path(), fs::Permissions::from_mode(0o644))
            .expect("permissions update");
        let error = read_marker(&resolved.git_common_dir).expect_err("insecure marker");
        assert!(matches!(error, RepositoryError::MarkerPermissions { .. }));
        assert_eq!(marker.version, SOLO_ID_MARKER_VERSION);
    }

    #[test]
    fn marker_publication_does_not_leave_temporary_files() {
        let (_root, repo) = repository();
        let resolved = resolve_git_repository(&repo).expect("repository resolves");
        let marker = SoloIdMarker::new(Uuid::new_v4());
        let error =
            write_marker_atomically(&resolved.git_common_dir, &resolved.marker_path(), marker);
        assert!(error.is_ok());
        let temporary_files = fs::read_dir(&resolved.git_common_dir)
            .unwrap()
            .filter_map(std::result::Result::ok)
            .filter(|entry| {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                name.starts_with(".forge-solo-id.") && name.ends_with(".tmp")
            })
            .count();
        assert_eq!(temporary_files, 0);
        assert_eq!(read_marker(&resolved.git_common_dir).unwrap(), marker);
    }

    #[test]
    fn marker_publication_never_replaces_an_existing_marker() {
        let (_root, repo) = repository();
        let resolved = resolve_git_repository(&repo).expect("repository resolves");
        let existing = read_or_create_marker(&resolved.git_common_dir).expect("marker creates");
        let replacement = SoloIdMarker::new(Uuid::new_v4());
        let error = write_marker_atomically(
            &resolved.git_common_dir,
            &resolved.marker_path(),
            replacement,
        )
        .expect_err("no-replace publication must report contention");
        assert_eq!(error.kind(), io::ErrorKind::AlreadyExists);
        assert_eq!(read_marker(&resolved.git_common_dir).unwrap(), existing);
    }

    #[test]
    fn linked_worktree_is_rejected() {
        let (root, repo) = repository();
        let linked = root.path().join("linked");
        git(&repo, &["branch", "linked-branch"]);
        git(
            &repo,
            &[
                "worktree",
                "add",
                linked.to_str().expect("UTF-8 path"),
                "linked-branch",
            ],
        );

        let error = resolve_git_repository(&linked).expect_err("linked worktree rejected");
        assert!(matches!(error, RepositoryError::LinkedWorktree { .. }));
    }

    #[test]
    fn detached_head_still_resolves_without_inventing_a_branch() {
        let (_root, repo) = repository();
        git(&repo, &["checkout", "--detach", "HEAD"]);
        let resolved = resolve_git_repository(&repo).expect("repository resolves");
        assert_eq!(resolved.default_branch, None);
    }
}
