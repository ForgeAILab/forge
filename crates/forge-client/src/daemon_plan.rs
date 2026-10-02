//! Bounded owner-local plan files. Terminal content is the publication input;
//! the private snapshot makes publication and rollback repeatable after a crash.
use api_types::MAX_EXECUTION_PLAN_BYTES;
use serde::{Deserialize, Serialize};
use std::{
    fs,
    io::{Read, Write},
    path::{Path, PathBuf},
};

fn read(path: &Path, root: &Path) -> anyhow::Result<Option<String>> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    anyhow::ensure!(metadata.is_file(), "plan must be a regular file");
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        anyhow::ensure!(
            metadata.nlink() == 1,
            "plan must have exactly one hard link"
        );
    }
    anyhow::ensure!(
        path.canonicalize()?.starts_with(root.canonicalize()?),
        "plan escapes owner directory"
    );
    anyhow::ensure!(
        metadata.len() <= MAX_EXECUTION_PLAN_BYTES,
        "plan exceeds the {MAX_EXECUTION_PLAN_BYTES}-byte limit: {} bytes",
        metadata.len()
    );
    let mut options = fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK);
    }
    let file = options.open(path)?;
    let opened = file.metadata()?;
    anyhow::ensure!(opened.is_file(), "plan must be a regular file");
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        anyhow::ensure!(
            opened.dev() == metadata.dev() && opened.ino() == metadata.ino() && opened.nlink() == 1,
            "plan changed during capture"
        );
    }
    let mut content = String::new();
    file.take(MAX_EXECUTION_PLAN_BYTES + 1)
        .read_to_string(&mut content)?;
    validate(&content)?;
    Ok(Some(content))
}

pub(crate) fn validate(content: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        content.len() as u64 <= MAX_EXECUTION_PLAN_BYTES,
        "plan exceeds the {MAX_EXECUTION_PLAN_BYTES}-byte limit: {} bytes",
        content.len()
    );
    Ok(())
}

fn sync_directory(path: &Path) -> anyhow::Result<()> {
    #[cfg(unix)]
    fs::File::open(path)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

fn outbox(worktree: &Path, execution_id: &str) -> anyhow::Result<PathBuf> {
    executors::prepare_execution_outbox(worktree, execution_id)
        .map_err(|error| anyhow::anyhow!(error.to_string()))
}

pub(crate) fn seed(
    worktree: &Path,
    execution_id: &str,
    content: Option<&str>,
) -> anyhow::Result<()> {
    let outbox = outbox(worktree, execution_id)?;
    let destination = outbox.join(executors::OUTBOX_PLAN_FILE);
    if read(&destination, &outbox)?.is_none() {
        if let Some(content) = content {
            validate(content)?;
            let mut file = fs::OpenOptions::new()
                .create_new(true)
                .write(true)
                .open(destination)?;
            file.write_all(content.as_bytes())?;
            file.sync_all()?;
            sync_directory(&outbox)?;
        }
    }
    Ok(())
}

pub(crate) fn harvest(worktree: &Path, execution_id: &str) -> anyhow::Result<Option<String>> {
    let Some(outbox) = executors::existing_execution_outbox(worktree, execution_id)
        .map_err(|error| anyhow::anyhow!(error.to_string()))?
    else {
        return Ok(None);
    };
    let content = read(&outbox.join(executors::OUTBOX_PLAN_FILE), &outbox)?;
    Ok(content)
}

#[derive(Serialize, Deserialize)]
struct Publication {
    content: String,
    previous: Option<String>,
}

fn snapshot(worktree: &Path, execution_id: &str) -> anyhow::Result<PathBuf> {
    anyhow::ensure!(
        executors::execution_outbox_path(worktree, execution_id).is_some(),
        "invalid execution id"
    );
    let root = worktree
        .parent()
        .ok_or_else(|| anyhow::anyhow!("workspace has no parent"))?;
    let stage = root.join(".forge-plan-staging");
    fs::create_dir_all(&stage)?;
    anyhow::ensure!(
        fs::symlink_metadata(&stage)?.is_dir()
            && stage.canonicalize()?.starts_with(root.canonicalize()?),
        "invalid plan staging directory"
    );
    Ok(stage.join(format!("{execution_id}.transport.json")))
}

fn replace(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
    let root = path.parent().unwrap();
    let temporary = root.join(format!(".plan-{}.tmp", uuid::Uuid::new_v4()));
    let mut file = fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(&temporary)?;
    file.write_all(bytes)?;
    file.sync_all()?;
    fs::rename(&temporary, path)?;
    sync_directory(root)?;
    Ok(())
}

fn load(path: &Path) -> anyhow::Result<Option<Publication>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            anyhow::ensure!(
                metadata.is_file() && metadata.len() <= 12 * MAX_EXECUTION_PLAN_BYTES + 1024,
                "invalid plan snapshot"
            );
            let publication: Publication = serde_json::from_slice(&fs::read(path)?)?;
            validate(&publication.content)?;
            Ok(Some(publication))
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn publish(worktree: &Path, execution_id: &str, content: &str) -> anyhow::Result<()> {
    validate(content)?;
    let stage = snapshot(worktree, execution_id)?;
    let canonical = worktree.parent().unwrap().join("plan.md");
    let root = canonical.parent().unwrap();
    match load(&stage)? {
        Some(existing) => {
            anyhow::ensure!(existing.content == content, "conflicting plan candidate")
        }
        None => {
            let publication = Publication {
                content: content.into(),
                previous: read(&canonical, root)?,
            };
            replace(&stage, &serde_json::to_vec(&publication)?)?;
        }
    }
    replace(&canonical, content.as_bytes())
}

pub(crate) fn restore(worktree: &Path, execution_id: &str) -> anyhow::Result<()> {
    let Some(publication) = load(&snapshot(worktree, execution_id)?)? else {
        return Ok(());
    };
    let canonical = worktree.parent().unwrap().join("plan.md");
    let root = canonical.parent().unwrap();
    let current = read(&canonical, root)?;
    if current == publication.previous {
        return Ok(());
    }
    anyhow::ensure!(
        current.as_deref() == Some(publication.content.as_str()),
        "canonical plan changed after publication"
    );
    match publication.previous {
        Some(content) => replace(&canonical, content.as_bytes()),
        None => {
            fs::remove_file(&canonical)?;
            sync_directory(root)?;
            Ok(())
        }
    }
}

pub(crate) fn discard(worktree: &Path, execution_id: &str) -> anyhow::Result<()> {
    anyhow::ensure!(
        executors::execution_id_is_path_safe(execution_id),
        "invalid execution id"
    );
    let root = worktree
        .parent()
        .ok_or_else(|| anyhow::anyhow!("workspace has no parent"))?;
    if !root.exists() {
        return Ok(());
    }
    let stage = root.join(".forge-plan-staging");
    if stage.exists() {
        anyhow::ensure!(
            fs::symlink_metadata(&stage)?.is_dir()
                && stage.canonicalize()?.starts_with(root.canonicalize()?),
            "invalid plan staging directory"
        );
    }
    match fs::remove_file(stage.join(format!("{execution_id}.transport.json"))) {
        Ok(()) => (),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => (),
        Err(error) => return Err(error.into()),
    }
    if let Some(outbox) = executors::existing_execution_outbox(worktree, execution_id)
        .map_err(|error| anyhow::anyhow!(error.to_string()))?
    {
        fs::remove_dir_all(outbox)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn owner_plan_seed_publication_replay_restore_and_bounds() {
        let temp = tempfile::tempdir().unwrap();
        let worktree = temp.path().join("task/repo");
        fs::create_dir_all(&worktree).unwrap();
        seed(&worktree, "exec-1", Some("- [ ] initial\n")).unwrap();
        // Restarting preparation must preserve the worker's edits.
        let candidate = outbox(&worktree, "exec-1").unwrap().join("plan.md");
        fs::write(&candidate, "- [x] completed\n").unwrap();
        seed(&worktree, "exec-1", Some("- [ ] initial\n")).unwrap();
        let content = harvest(&worktree, "exec-1").unwrap().unwrap();
        assert_eq!(content, "- [x] completed\n");
        fs::write(worktree.parent().unwrap().join("plan.md"), "- [ ] prior\n").unwrap();
        publish(&worktree, "exec-1", &content).unwrap();
        publish(&worktree, "exec-1", &content).unwrap();
        assert!(publish(&worktree, "exec-1", "- [ ] different\n").is_err());
        restore(&worktree, "exec-1").unwrap();
        restore(&worktree, "exec-1").unwrap();
        assert_eq!(
            fs::read_to_string(worktree.parent().unwrap().join("plan.md")).unwrap(),
            "- [ ] prior\n"
        );
        fs::write(
            &candidate,
            "x".repeat(MAX_EXECUTION_PLAN_BYTES as usize + 1),
        )
        .unwrap();
        assert!(harvest(&worktree, "exec-1")
            .unwrap_err()
            .to_string()
            .contains("exceeds"));
        discard(&worktree, "exec-1").unwrap();
        discard(&worktree, "exec-1").unwrap();
        assert!(!candidate.exists());
    }
}
