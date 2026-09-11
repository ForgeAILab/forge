//! OS-backed exclusivity for one live Solo runtime.
//!
//! The lock is an advisory filesystem lock, not a stale sentinel. Dropping
//! the process (including a crash) releases the kernel lock automatically;
//! the small `runtime.lock` file remains as durable, non-secret owner context
//! and is overwritten only after a later process successfully acquires it.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use fs2::FileExt;
use uuid::Uuid;

/// Lock filename under each Solo data root.
pub const RUNTIME_LOCK_FILE: &str = "runtime.lock";
const MAX_OWNER_BYTES: u64 = 4096;

/// Non-secret context written into the lock file for actionable contention
/// errors. The data root path itself is carried by [`RuntimeLockError`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockOwnerContext {
    pub process_id: u32,
    pub repository_id: Option<Uuid>,
    pub project_id: Option<String>,
}

impl Default for LockOwnerContext {
    fn default() -> Self {
        Self {
            process_id: std::process::id(),
            repository_id: None,
            project_id: None,
        }
    }
}

impl std::fmt::Display for LockOwnerContext {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "process {}", self.process_id)?;
        if let Some(repository_id) = self.repository_id {
            write!(formatter, ", repository {repository_id}")?;
        }
        if let Some(project_id) = &self.project_id {
            write!(formatter, ", project {project_id}")?;
        }
        Ok(())
    }
}

/// Errors returned while acquiring or writing the runtime lock.
#[derive(Debug, thiserror::Error)]
pub enum RuntimeLockError {
    #[error("Solo data root is a symlink, which is not allowed: {path}")]
    DataRootSymlink { path: PathBuf },

    #[error("Solo data root is not a directory: {path}")]
    DataRootNotDirectory { path: PathBuf },

    #[error("Solo runtime lock is a symlink, which is not allowed: {path}")]
    LockSymlink { path: PathBuf },

    #[error("failed to access Solo runtime lock {path}: {source}")]
    Io { path: PathBuf, source: io::Error },

    #[error(
        "Solo runtime is already running for data root {data_root} (lock {lock_path}; owner: {owner}); close that process before starting another"
    )]
    AlreadyHeld {
        data_root: PathBuf,
        lock_path: PathBuf,
        owner: LockOwnerContext,
    },
}

type Result<T> = std::result::Result<T, RuntimeLockError>;

/// The held kernel lock. It is intentionally non-cloneable: one process owns
/// exactly one guard and releasing it is tied to dropping this value.
pub struct RuntimeLock {
    file: File,
    data_root: PathBuf,
    lock_path: PathBuf,
    owner: LockOwnerContext,
}

impl std::fmt::Debug for RuntimeLock {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("RuntimeLock")
            .field("data_root", &self.data_root)
            .field("lock_path", &self.lock_path)
            .field("owner", &self.owner)
            .finish_non_exhaustive()
    }
}

impl RuntimeLock {
    /// Acquire `runtime.lock` under `data_root` with process-only owner
    /// context.
    pub fn acquire(data_root: impl AsRef<Path>) -> Result<Self> {
        Self::acquire_with_owner(data_root, LockOwnerContext::default())
    }

    /// Acquire `runtime.lock` and persist repository/Project context for any
    /// later contention diagnostic.
    pub fn acquire_with_owner(
        data_root: impl AsRef<Path>,
        owner: LockOwnerContext,
    ) -> Result<Self> {
        let requested_root = absolute_data_root(data_root.as_ref())?;
        reject_symlink_components(&requested_root)?;
        ensure_data_root(&requested_root)?;
        reject_symlink_components(&requested_root)?;
        let data_root =
            fs::canonicalize(&requested_root).map_err(|source| RuntimeLockError::Io {
                path: requested_root.clone(),
                source,
            })?;
        let lock_path = data_root.join(RUNTIME_LOCK_FILE);
        if let Ok(metadata) = fs::symlink_metadata(&lock_path) {
            if metadata.file_type().is_symlink() {
                return Err(RuntimeLockError::LockSymlink { path: lock_path });
            }
        }

        let mut options = OpenOptions::new();
        options.read(true).write(true).create(true);
        #[cfg(unix)]
        std::os::unix::fs::OpenOptionsExt::mode(&mut options, 0o600);
        let mut file = options
            .open(&lock_path)
            .map_err(|source| RuntimeLockError::Io {
                path: lock_path.clone(),
                source,
            })?;
        // Validate the descriptor and the pathname again after opening. This
        // closes ordinary check/use races; std has no portable O_NOFOLLOW
        // equivalent, so an attacker racing the final open is still handled
        // conservatively by the post-open check whenever observable.
        if fs::symlink_metadata(&lock_path)
            .map(|metadata| metadata.file_type().is_symlink())
            .unwrap_or(true)
        {
            return Err(RuntimeLockError::LockSymlink { path: lock_path });
        }
        let metadata = file.metadata().map_err(|source| RuntimeLockError::Io {
            path: lock_path.clone(),
            source,
        })?;
        if !metadata.is_file() {
            return Err(RuntimeLockError::Io {
                path: lock_path.clone(),
                source: io::Error::new(
                    io::ErrorKind::InvalidData,
                    "Solo runtime lock must be a regular file",
                ),
            });
        }
        #[cfg(unix)]
        validate_lock_file_permissions(&file).map_err(|source| RuntimeLockError::Io {
            path: lock_path.clone(),
            source,
        })?;

        if let Err(error) = file.try_lock_exclusive() {
            if is_lock_contended(&error) {
                let held_owner = read_owner(&lock_path).unwrap_or_default();
                return Err(RuntimeLockError::AlreadyHeld {
                    data_root,
                    lock_path,
                    owner: held_owner,
                });
            }
            return Err(RuntimeLockError::Io {
                path: lock_path,
                source: error,
            });
        }

        if let Err(error) = write_owner(&mut file, &owner) {
            let _ = file.unlock();
            return Err(RuntimeLockError::Io {
                path: lock_path,
                source: error,
            });
        }

        Ok(Self {
            file,
            data_root,
            lock_path,
            owner,
        })
    }

    #[must_use]
    pub fn data_root(&self) -> &Path {
        &self.data_root
    }

    #[must_use]
    pub fn lock_path(&self) -> &Path {
        &self.lock_path
    }

    #[must_use]
    pub fn owner(&self) -> &LockOwnerContext {
        &self.owner
    }

    /// Update the non-secret contention context while retaining this exact
    /// kernel lock. Callers use this after bootstrap has materialized the
    /// Project ID; dropping and reacquiring here would leave a race in which
    /// another Solo process could enter before migrations/session setup is
    /// complete.
    pub fn update_owner_context(&mut self, owner: LockOwnerContext) -> Result<()> {
        if owner.process_id != self.owner.process_id
            || owner.repository_id != self.owner.repository_id
        {
            return Err(RuntimeLockError::Io {
                path: self.lock_path.clone(),
                source: io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "runtime lock owner process/repository identity cannot change",
                ),
            });
        }
        write_owner(&mut self.file, &owner).map_err(|source| RuntimeLockError::Io {
            path: self.lock_path.clone(),
            source,
        })?;
        self.owner = owner;
        Ok(())
    }
}

impl Drop for RuntimeLock {
    fn drop(&mut self) {
        // The kernel releases the advisory lock with the file descriptor. A
        // drop-time error cannot be reported safely (especially during panic),
        // so best-effort release is intentionally silent.
        let _ = self.file.unlock();
    }
}

fn absolute_data_root(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        let current_dir = std::env::current_dir().map_err(|source| RuntimeLockError::Io {
            path: path.to_path_buf(),
            source,
        })?;
        Ok(current_dir.join(path))
    }
}

fn ensure_data_root(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                return Err(RuntimeLockError::DataRootSymlink {
                    path: path.to_path_buf(),
                });
            }
            if !metadata.is_dir() {
                return Err(RuntimeLockError::DataRootNotDirectory {
                    path: path.to_path_buf(),
                });
            }
            #[cfg(unix)]
            validate_private_data_root(path)?;
            Ok(())
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            fs::create_dir_all(path).map_err(|source| RuntimeLockError::Io {
                path: path.to_path_buf(),
                source,
            })?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                fs::set_permissions(path, fs::Permissions::from_mode(0o700)).map_err(|source| {
                    RuntimeLockError::Io {
                        path: path.to_path_buf(),
                        source,
                    }
                })?;
            }
            Ok(())
        }
        Err(source) => Err(RuntimeLockError::Io {
            path: path.to_path_buf(),
            source,
        }),
    }
}

/// Reject symlink components before recursive creation/opening.  The standard
/// library does not provide a portable no-follow directory-handle API, so the
/// caller also rechecks the path after creation and uses the canonical root.
fn reject_symlink_components(path: &Path) -> Result<()> {
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::Prefix(prefix) => current.push(prefix.as_os_str()),
            std::path::Component::RootDir => current.push(component.as_os_str()),
            std::path::Component::CurDir => {}
            std::path::Component::ParentDir => current.push(component.as_os_str()),
            std::path::Component::Normal(name) => {
                current.push(name);
                match fs::symlink_metadata(&current) {
                    Ok(metadata)
                        if metadata.file_type().is_symlink()
                            && !is_trusted_platform_alias(&current) =>
                    {
                        return Err(RuntimeLockError::DataRootSymlink { path: current });
                    }
                    Ok(_) => {}
                    Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
                    Err(source) => {
                        return Err(RuntimeLockError::Io {
                            path: current,
                            source,
                        });
                    }
                }
            }
        }
    }
    Ok(())
}

fn is_trusted_platform_alias(path: &Path) -> bool {
    #[cfg(target_os = "macos")]
    {
        matches!(path.to_str(), Some("/var" | "/tmp" | "/etc"))
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = path;
        false
    }
}

#[cfg(unix)]
fn validate_private_data_root(path: &Path) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let metadata = fs::symlink_metadata(path).map_err(|source| RuntimeLockError::Io {
        path: path.to_path_buf(),
        source,
    })?;
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(RuntimeLockError::Io {
            path: path.to_path_buf(),
            source: io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Solo data root must be owner-only",
            ),
        });
    }
    Ok(())
}

fn write_owner(file: &mut File, owner: &LockOwnerContext) -> io::Result<()> {
    let mut contents = format!("process_id = {}\n", owner.process_id);
    if let Some(repository_id) = owner.repository_id {
        contents.push_str(&format!("repository_id = \"{repository_id}\"\n"));
    }
    if let Some(project_id) = owner.project_id.as_deref() {
        // Project IDs are UUIDs in Forge. Refuse line breaks even if an
        // embedding caller constructs context manually, so the lock file
        // remains bounded and parseable.
        if project_id.contains(['\n', '\r']) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "project_id must not contain a newline",
            ));
        }
        contents.push_str(&format!("project_id = \"{project_id}\"\n"));
    }
    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(contents.as_bytes())?;
    file.sync_all()
}

fn read_owner(path: &Path) -> io::Result<LockOwnerContext> {
    let path_metadata = fs::symlink_metadata(path)?;
    if path_metadata.file_type().is_symlink() || !path_metadata.is_file() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "runtime lock metadata is not a regular bounded file",
        ));
    }
    let file = File::open(path)?;
    if fs::symlink_metadata(path)
        .map(|metadata| metadata.file_type().is_symlink())
        .unwrap_or(true)
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "runtime lock metadata path changed while opening",
        ));
    }
    let metadata = file.metadata()?;
    if !metadata.is_file() || metadata.len() > MAX_OWNER_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "runtime lock metadata is not a regular bounded file",
        ));
    }
    let mut bytes = Vec::new();
    file.take(MAX_OWNER_BYTES.saturating_add(1))
        .read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_OWNER_BYTES {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "runtime lock metadata is not a regular bounded file",
        ));
    }
    let contents = String::from_utf8(bytes).map_err(|_| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            "runtime lock metadata is not valid UTF-8",
        )
    })?;
    let mut owner = LockOwnerContext {
        process_id: 0,
        repository_id: None,
        project_id: None,
    };
    for line in contents.lines() {
        let Some((key, value)) = line.split_once(" = ") else {
            continue;
        };
        match key {
            "process_id" => owner.process_id = value.parse().unwrap_or(0),
            "repository_id" => {
                owner.repository_id = value
                    .strip_prefix('"')
                    .and_then(|value| value.strip_suffix('"'))
                    .and_then(|value| Uuid::parse_str(value).ok());
            }
            "project_id" => {
                owner.project_id = value
                    .strip_prefix('"')
                    .and_then(|value| value.strip_suffix('"'))
                    .map(str::to_owned);
            }
            _ => {}
        }
    }
    Ok(owner)
}

fn is_lock_contended(error: &io::Error) -> bool {
    if error.kind() == io::ErrorKind::WouldBlock {
        return true;
    }
    // macOS uses EAGAIN/EWOULDBLOCK (35) while some libc surfaces report 11;
    // keep this fallback for platforms that don't map it to WouldBlock.
    if matches!(error.raw_os_error(), Some(11 | 35)) {
        return true;
    }
    #[cfg(windows)]
    if error.raw_os_error() == Some(33) {
        // Windows ERROR_LOCK_VIOLATION from fs2::FileExt::try_lock_exclusive.
        return true;
    }
    false
}

#[cfg(unix)]
fn validate_lock_file_permissions(file: &File) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;
    let metadata = file.metadata()?;
    if metadata.permissions().mode() & 0o077 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "runtime lock must be owner-only",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn repository_id() -> Uuid {
        Uuid::parse_str("00000000-0000-4000-8000-000000000001").unwrap()
    }

    fn private_root(temporary: &TempDir) -> PathBuf {
        let root = temporary.path().join("state");
        fs::create_dir(&root).unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            fs::set_permissions(&root, fs::Permissions::from_mode(0o700)).unwrap();
        }
        fs::canonicalize(root).unwrap()
    }

    #[test]
    fn lock_is_exclusive_and_reports_owner_context() {
        let temporary = TempDir::new().unwrap();
        let root = private_root(&temporary);
        let owner = LockOwnerContext {
            process_id: 1234,
            repository_id: Some(repository_id()),
            project_id: Some("project-1".to_owned()),
        };
        let first = RuntimeLock::acquire_with_owner(&root, owner.clone()).unwrap();
        assert_eq!(first.lock_path(), root.join(RUNTIME_LOCK_FILE));
        assert_eq!(first.owner(), &owner);

        let error = RuntimeLock::acquire(&root).expect_err("second runtime must conflict");
        match error {
            RuntimeLockError::AlreadyHeld {
                data_root,
                lock_path,
                owner: observed,
            } => {
                assert_eq!(data_root, root);
                assert_eq!(lock_path, root.join(RUNTIME_LOCK_FILE));
                assert_eq!(observed, owner);
            }
            other => panic!("unexpected error: {other:?}"),
        }
    }

    #[test]
    fn owner_context_can_add_project_without_releasing_lock() {
        let temporary = TempDir::new().unwrap();
        let root = private_root(&temporary);
        let repository_id = repository_id();
        let owner = LockOwnerContext {
            process_id: 1234,
            repository_id: Some(repository_id),
            project_id: None,
        };
        let mut first = RuntimeLock::acquire_with_owner(&root, owner).unwrap();
        first
            .update_owner_context(LockOwnerContext {
                process_id: 1234,
                repository_id: Some(repository_id),
                project_id: Some("project-1".to_owned()),
            })
            .unwrap();
        assert_eq!(first.owner().project_id.as_deref(), Some("project-1"));

        let error = RuntimeLock::acquire(&root).expect_err("lock must remain held");
        assert!(matches!(error, RuntimeLockError::AlreadyHeld { owner, .. }
            if owner.project_id.as_deref() == Some("project-1")));
    }

    #[test]
    fn drop_releases_kernel_lock_without_deleting_state() {
        let temporary = TempDir::new().unwrap();
        let root = private_root(&temporary);
        let lock_path = root.join(RUNTIME_LOCK_FILE);
        {
            let _first = RuntimeLock::acquire(&root).unwrap();
            assert!(lock_path.is_file());
        }
        assert!(lock_path.is_file());
        let _second = RuntimeLock::acquire(&root).expect("lock is reclaimable");
    }

    #[cfg(unix)]
    #[test]
    fn lock_file_is_private() {
        use std::os::unix::fs::PermissionsExt;

        let temporary = TempDir::new().unwrap();
        let root = private_root(&temporary);
        let _lock = RuntimeLock::acquire(&root).unwrap();
        let mode = fs::symlink_metadata(root.join(RUNTIME_LOCK_FILE))
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o077, 0);
    }

    #[cfg(unix)]
    #[test]
    fn symlink_data_root_is_rejected() {
        let outer = TempDir::new().unwrap();
        let root = private_root(&outer);
        let target = root.join("target");
        fs::create_dir(&target).unwrap();
        let link = outer.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let error = RuntimeLock::acquire(&link).expect_err("symlink root");
        assert!(matches!(error, RuntimeLockError::DataRootSymlink { .. }));
    }

    #[cfg(unix)]
    #[test]
    fn symlinked_data_root_ancestor_is_rejected() {
        let outer = TempDir::new().unwrap();
        let target = outer.path().join("target");
        fs::create_dir(&target).unwrap();
        let link = outer.path().join("link");
        std::os::unix::fs::symlink(&target, &link).unwrap();
        let error = RuntimeLock::acquire(link.join("state"))
            .expect_err("symlinked root ancestor must fail");
        assert!(matches!(error, RuntimeLockError::DataRootSymlink { .. }));
    }

    #[cfg(unix)]
    #[test]
    fn symlink_lock_file_is_rejected_without_following_it() {
        let outer = TempDir::new().unwrap();
        let root = private_root(&outer);
        let target = root.join("target");
        fs::write(&target, "other state").unwrap();
        let lock_path = root.join(RUNTIME_LOCK_FILE);
        std::os::unix::fs::symlink(&target, &lock_path).unwrap();
        let error = RuntimeLock::acquire(&root).expect_err("symlink lock");
        assert!(matches!(error, RuntimeLockError::LockSymlink { .. }));
        assert_eq!(fs::read_to_string(target).unwrap(), "other state");
    }

    #[cfg(windows)]
    #[test]
    fn windows_lock_violation_is_reported_as_contention() {
        let error = io::Error::from_raw_os_error(33);
        assert!(is_lock_contended(&error));
    }
}
