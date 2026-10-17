//! Publish scripts without exposing a writable executable inode to other forks.
use std::{fs, io, path::Path, time::Duration};
#[cfg(unix)]
use std::{io::Write, process::Stdio};

/// The Unix writer process (`scripts/executable_fixture.py` runs the same
/// program). Kept in the crate: release images build from `crates/` alone.
#[cfg(unix)]
const WRITER: &str = "umask 077\nexec /bin/cat > \"$1\"\n";

/// Atomically publish an executable, without following an existing symlink.
///
/// On Unix the writer is a separate process: the multithreaded parent never
/// opens the staged inode for writing. Otherwise an unrelated concurrent fork
/// can inherit that writable descriptor until exec, and Linux rejects execution
/// with ETXTBSY even after the parent closes it and renames the file. Waiting for
/// the writer to exit closes every writable descriptor before publication. This
/// also protects scripts executed indirectly (for example Git hooks).
pub fn write_executable(path: &Path, contents: impl AsRef<[u8]>) -> io::Result<()> {
    let parent = path
        .parent()
        .filter(|p| !p.as_os_str().is_empty())
        .unwrap_or(Path::new("."));
    let staging = tempfile::Builder::new()
        .prefix(".forge-executable-")
        .tempdir_in(parent)?;
    let staged = staging.path().join("script");
    #[cfg(unix)]
    {
        let mut writer = std::process::Command::new("/bin/sh")
            .args(["-c", WRITER, "forge-write-executable"])
            .arg(&staged)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()?;
        let written = writer
            .stdin
            .take()
            .expect("piped stdin")
            .write_all(contents.as_ref());
        let status = writer.wait()?;
        written?;
        if !status.success() {
            return Err(io::Error::other("executable writer failed"));
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        // Read-only in the parent, including during fsync and permission changes.
        let file = fs::File::open(&staged)?;
        file.sync_all()?;
        file.set_permissions(fs::Permissions::from_mode(0o755))?;
    }
    #[cfg(not(unix))]
    {
        fs::write(&staged, contents)?;
        // Windows requires a writable handle for FlushFileBuffers.
        fs::OpenOptions::new()
            .write(true)
            .open(&staged)?
            .sync_all()?;
    }
    fs::rename(&staged, path)
}

fn retry_delay(error: &io::Error, attempt: u32) -> Option<Duration> {
    (error.kind() == io::ErrorKind::ExecutableFileBusy && attempt < 5)
        .then(|| Duration::from_millis(10 << attempt))
}

fn retry_spawn<T>(mut spawn: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    for attempt in 0.. {
        match spawn() {
            Err(error) => match retry_delay(&error, attempt) {
                Some(delay) => std::thread::sleep(delay),
                None => return Err(error),
            },
            result => return result,
        }
    }
    unreachable!()
}

async fn retry_spawn_async<T>(mut spawn: impl FnMut() -> io::Result<T>) -> io::Result<T> {
    for attempt in 0.. {
        match spawn() {
            Err(error) => match retry_delay(&error, attempt) {
                Some(delay) => tokio::time::sleep(delay).await,
                None => return Err(error),
            },
            result => return result,
        }
    }
    unreachable!()
}

/// Retry only ETXTBSY spawn errors, at most five times (310 ms total delay).
/// A successful spawn, nonzero exit, or any other error is never retried.
pub fn spawn_std(command: &mut std::process::Command) -> io::Result<std::process::Child> {
    retry_spawn(|| command.spawn())
}

/// Async equivalent of [`spawn_std`]; waiting does not block the runtime.
pub async fn spawn(command: &mut tokio::process::Command) -> io::Result<tokio::process::Child> {
    retry_spawn_async(|| command.spawn()).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn executable_spawn_retry_is_bounded_and_only_retries_text_file_busy() {
        let mut calls = 0;
        assert_eq!(
            retry_spawn(|| {
                calls += 1;
                if calls < 3 {
                    Err(io::ErrorKind::ExecutableFileBusy.into())
                } else {
                    Ok(42)
                }
            })
            .unwrap(),
            42
        );
        assert_eq!(calls, 3);
        let mut calls = 0;
        let error = retry_spawn::<()>(|| {
            calls += 1;
            Err(io::ErrorKind::ExecutableFileBusy.into())
        })
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::ExecutableFileBusy);
        assert_eq!(calls, 6);
        let mut calls = 0;
        let error = retry_spawn::<()>(|| {
            calls += 1;
            Err(io::ErrorKind::PermissionDenied.into())
        })
        .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::PermissionDenied);
        assert_eq!(calls, 1);
    }

    #[tokio::test]
    async fn executable_async_spawn_retry_recovers_and_preserves_errors() {
        let mut calls = 0;
        retry_spawn_async(|| {
            calls += 1;
            if calls < 3 {
                Err(io::ErrorKind::ExecutableFileBusy.into())
            } else {
                Ok(())
            }
        })
        .await
        .unwrap();
        assert_eq!(calls, 3);
        let mut calls = 0;
        assert_eq!(
            retry_spawn_async::<()>(|| {
                calls += 1;
                Err(io::ErrorKind::ExecutableFileBusy.into())
            })
            .await
            .unwrap_err()
            .kind(),
            io::ErrorKind::ExecutableFileBusy
        );
        assert_eq!(calls, 6);
        let mut calls = 0;
        assert_eq!(
            retry_spawn_async::<()>(|| {
                calls += 1;
                Err(io::ErrorKind::NotFound.into())
            })
            .await
            .unwrap_err()
            .kind(),
            io::ErrorKind::NotFound
        );
        assert_eq!(calls, 1);
    }

    #[cfg(unix)]
    #[test]
    fn executable_publication_replaces_inode_without_following_links() {
        use std::os::unix::fs::{symlink, PermissionsExt};
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("script");
        let outside = temp.path().join("outside");
        fs::write(&outside, "untouched").unwrap();
        symlink(&outside, &path).unwrap();
        write_executable(&path, "#!/bin/sh\nprintf first\n").unwrap();
        assert_eq!(fs::read_to_string(&outside).unwrap(), "untouched");
        let old = fs::File::open(&path).unwrap();
        write_executable(&path, "#!/bin/sh\nprintf second\n").unwrap();
        assert_eq!(
            std::io::read_to_string(old).unwrap(),
            "#!/bin/sh\nprintf first\n"
        );
        assert_eq!(
            fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o755
        );
        let output = std::process::Command::new(&path).output().unwrap();
        assert!(output.status.success());
        assert_eq!(output.stdout, b"second");
        assert_eq!(fs::read_dir(temp.path()).unwrap().count(), 2);
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn executable_spawn_retry_waits_for_a_writable_descriptor_to_close() {
        let temp = tempfile::tempdir().unwrap();
        let path = temp.path().join("script");
        write_executable(&path, "#!/bin/sh\nexit 0\n").unwrap();
        let writer = fs::OpenOptions::new().write(true).open(&path).unwrap();
        let release = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(40));
            drop(writer);
        });
        let mut child = spawn_std(&mut std::process::Command::new(&path)).unwrap();
        assert!(child.wait().unwrap().success());
        release.join().unwrap();
    }
}
