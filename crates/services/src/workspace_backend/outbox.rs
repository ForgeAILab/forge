use std::{
    fs,
    io::Read,
    path::{Component, Path, PathBuf},
};

use api_types::{
    ExecutionOutboxArtifact, ExecutionOutboxEntry, ExecutionOutboxEvidenceKind,
    ExecutionOutboxWorklogKind, MAX_EXECUTION_OUTBOX_ENTRIES_PER_KIND,
    MAX_EXECUTION_OUTBOX_EVIDENCE_BYTES, MAX_EXECUTION_OUTBOX_FILE_BYTES,
    MAX_EXECUTION_OUTBOX_SUMMARY_CHARS,
};
use serde_json::Value;

use super::OutboxHarvest;

pub(super) fn harvest(worktree: &Path, execution_id: &str) -> OutboxHarvest {
    let mut result = OutboxHarvest::default();
    let Some(outbox) = executors::execution_outbox_path(worktree, execution_id) else {
        return result;
    };
    if !outbox.is_dir() {
        return result;
    }
    let mut evidence_bytes = 0;
    for (file, worklog) in [
        (executors::OUTBOX_WORKLOG_FILE, true),
        (executors::OUTBOX_EVIDENCE_FILE, false),
    ] {
        let path = outbox.join(file);
        let Ok(metadata) = fs::symlink_metadata(&path) else {
            continue;
        };
        if !metadata.is_file() {
            result.rejected.push(format!("{file}: not a regular file"));
            continue;
        }
        let text = match read_bounded(&path, MAX_EXECUTION_OUTBOX_FILE_BYTES)
            .and_then(|bytes| String::from_utf8(bytes).map_err(|error| error.to_string()))
        {
            Ok(text) => text,
            Err(error) => {
                result.rejected.push(format!("{file}: {error}"));
                continue;
            }
        };
        let lines = text
            .lines()
            .enumerate()
            .filter(|(_, line)| !line.trim().is_empty())
            .collect::<Vec<_>>();
        if lines.len() > MAX_EXECUTION_OUTBOX_ENTRIES_PER_KIND {
            result.rejected.push(format!(
                "{file}: only the first {MAX_EXECUTION_OUTBOX_ENTRIES_PER_KIND} of {} entries were harvested",
                lines.len(),
            ));
        }
        for (index, line) in lines
            .into_iter()
            .take(MAX_EXECUTION_OUTBOX_ENTRIES_PER_KIND)
        {
            let entry = parse_entry(
                worktree,
                &outbox,
                index as u32 + 1,
                line,
                worklog,
                &mut evidence_bytes,
            );
            match entry {
                Ok(entry) => result.entries.push(entry),
                Err(error) => result
                    .rejected
                    .push(format!("{file}:{}: {error}", index + 1)),
            }
        }
    }
    // Ingestion/terminal acknowledgement consumes the directory, not harvesting.
    result
}

fn parse_entry(
    worktree: &Path,
    outbox: &Path,
    line_number: u32,
    line: &str,
    worklog: bool,
    evidence_bytes: &mut u64,
) -> Result<ExecutionOutboxEntry, String> {
    let value: Value = serde_json::from_str(line).map_err(|error| error.to_string())?;
    if worklog {
        let kind: ExecutionOutboxWorklogKind = serde_json::from_value(value["kind"].clone())
            .map_err(|_| "kind must be progress, decision, validation, or blocker")?;
        let summary = value["summary"]
            .as_str()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or("summary is required")?;
        if summary.chars().count() > MAX_EXECUTION_OUTBOX_SUMMARY_CHARS {
            return Err(format!(
                "summary exceeds the {MAX_EXECUTION_OUTBOX_SUMMARY_CHARS} character worklog limit"
            ));
        }
        return Ok(ExecutionOutboxEntry::Worklog {
            line_number,
            kind,
            summary: summary.to_owned(),
        });
    }

    let kind: ExecutionOutboxEvidenceKind = serde_json::from_value(value["kind"].clone())
        .map_err(|_| "kind must be screenshot, walkthrough_video, log, report, or other")?;
    let caption = value["caption"]
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or("caption describing the artifact is required")?;
    let path = value["path"]
        .as_str()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let content = value["content"]
        .as_str()
        .filter(|value| !value.trim().is_empty());
    let remaining = MAX_EXECUTION_OUTBOX_EVIDENCE_BYTES.saturating_sub(*evidence_bytes);
    let artifact = match (path, content) {
        (Some(path), None) => {
            let resolved = resolve_artifact(worktree, outbox, path)?;
            let bytes = read_bounded(&resolved, remaining)?;
            if bytes.is_empty() {
                return Err("captured artifact is empty".to_owned());
            }
            let filename = resolved
                .file_name()
                .and_then(|value| value.to_str())
                .unwrap_or("artifact")
                .to_owned();
            let content_type = content_type_for(&filename, kind);
            *evidence_bytes += bytes.len() as u64;
            Some(ExecutionOutboxArtifact {
                filename,
                content_type,
                bytes,
            })
        }
        (None, Some(content)) => {
            if content.len() as u64 > remaining {
                return Err("execution outbox evidence exceeds size budget".to_owned());
            }
            *evidence_bytes += content.len() as u64;
            None
        }
        (Some(_), Some(_)) => return Err("supply either path or content, not both".to_owned()),
        (None, None) => return Err("evidence requires either a path or inline content".to_owned()),
    };
    Ok(ExecutionOutboxEntry::Evidence {
        line_number,
        kind,
        caption: caption.to_owned(),
        path: path.map(str::to_owned),
        content: content.map(str::to_owned),
        artifact,
    })
}

fn read_bounded(path: &Path, limit: u64) -> Result<Vec<u8>, String> {
    let metadata = fs::metadata(path).map_err(|error| error.to_string())?;
    if !metadata.is_file() {
        return Err("captured artifact is not a file".to_owned());
    }
    if metadata.len() > limit {
        return Err(format!("exceeds the {limit} byte outbox limit"));
    }
    let mut bytes = Vec::new();
    fs::File::open(path)
        .map_err(|error| error.to_string())?
        .take(limit + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| error.to_string())?;
    if bytes.len() as u64 > limit {
        return Err(format!("exceeds the {limit} byte outbox limit"));
    }
    Ok(bytes)
}

fn resolve_artifact(worktree: &Path, outbox: &Path, relative: &str) -> Result<PathBuf, String> {
    let path = Path::new(relative);
    let (root, candidate) = if path.is_absolute() {
        (outbox, path.to_path_buf())
    } else {
        if path
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::Prefix(_)))
        {
            return Err("captured artifact path escapes the Task workspace".to_owned());
        }
        (worktree, worktree.join(path))
    };
    let canonical_root = root.canonicalize().map_err(|error| error.to_string())?;
    let canonical = candidate
        .canonicalize()
        .map_err(|error| error.to_string())?;
    if !canonical.starts_with(canonical_root) {
        return Err(if path.is_absolute() {
            "an absolute evidence path must point inside the outbox; use a worktree-relative path otherwise"
        } else {
            "captured artifact path escapes the Task workspace"
        }.to_owned());
    }
    Ok(canonical)
}

fn content_type_for(filename: &str, kind: ExecutionOutboxEvidenceKind) -> String {
    let extension = Path::new(filename)
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    match extension.as_str() {
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "webm" => "video/webm",
        "mp4" => "video/mp4",
        "json" => "application/json",
        "txt" | "log" | "md" => "text/plain",
        _ if kind == ExecutionOutboxEvidenceKind::Screenshot => "image/png",
        _ if kind == ExecutionOutboxEvidenceKind::WalkthroughVideo => "video/webm",
        _ => "application/octet-stream",
    }
    .to_owned()
}
