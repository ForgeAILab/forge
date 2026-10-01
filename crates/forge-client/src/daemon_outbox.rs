use std::{
    fs::{self, File},
    io::Read,
    path::Path,
};

use api_types::{
    ExecutionOutboxArtifact, ExecutionOutboxEntry, ExecutionOutboxEvidenceKind,
    ExecutionOutboxWorklogKind, ExecutionTerminalNotification,
    MAX_EXECUTION_OUTBOX_ENTRIES_PER_KIND, MAX_EXECUTION_OUTBOX_FILE_BYTES,
    MAX_EXECUTION_OUTBOX_SUMMARY_CHARS,
};
use serde_json::Value;

use crate::{daemon_fs, daemon_persistence::MAX_TERMINAL_REPORT_SIZE};

// JSON byte arrays can cost four bytes per captured byte. Bound serialized
// entries, reserving half the terminal report for usage and result metadata.
const MAX_EMBEDDED_OUTBOX_BYTES: usize = MAX_TERMINAL_REPORT_SIZE / 2;
const MAX_EMBEDDED_ARTIFACT_BYTES: usize = MAX_EMBEDDED_OUTBOX_BYTES / 4;
const TRUNCATION_LINE: &str = "4294967295";

pub(crate) fn harvest(
    worktree: &Path,
    execution_id: &str,
    workspace_root: &Path,
) -> Vec<ExecutionOutboxEntry> {
    let Some(outbox) = executors::execution_outbox_path(worktree, execution_id) else {
        return Vec::new();
    };
    if !outbox.exists() {
        return Vec::new();
    }
    let mut entries = Vec::new();
    let mut dropped = 0;
    let Ok(outbox) = daemon_fs::validate_within_root(&outbox, workspace_root) else {
        return vec![truncation_marker(1)];
    };
    let mut bytes = 2_usize;
    for (filename, evidence) in [
        (executors::OUTBOX_WORKLOG_FILE, false),
        (executors::OUTBOX_EVIDENCE_FILE, true),
    ] {
        let path = outbox.join(filename);
        let metadata = match fs::symlink_metadata(&path) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(_) => {
                dropped += 1;
                continue;
            }
        };
        if !metadata.is_file() || metadata.len() > MAX_EXECUTION_OUTBOX_FILE_BYTES {
            dropped += 1;
            continue;
        }
        let mut text = String::new();
        if File::open(&path)
            .and_then(|file| {
                file.take(MAX_EXECUTION_OUTBOX_FILE_BYTES + 1)
                    .read_to_string(&mut text)
            })
            .is_err()
            || text.len() as u64 > MAX_EXECUTION_OUTBOX_FILE_BYTES
        {
            dropped += 1;
            continue;
        }
        for (position, value) in
            api_types::execution_outbox::parse_entries(&text, MAX_EXECUTION_OUTBOX_ENTRIES_PER_KIND)
        {
            let entry = value
                .ok()
                .and_then(|value| parse_entry(value, position, evidence, worktree, &outbox));
            let Some(entry) = entry else {
                dropped += 1;
                continue;
            };
            let size = serde_json::to_vec(&entry)
                .map(|bytes| bytes.len() + 1)
                .unwrap_or(usize::MAX);
            if bytes.saturating_add(size) > MAX_EMBEDDED_OUTBOX_BYTES - 1024 {
                dropped += 1;
                continue;
            }
            bytes += size;
            entries.push(entry);
        }
    }
    if dropped > 0 {
        append_truncation_marker(&mut entries, dropped);
    }
    entries
}

/// The final serialized report, rather than raw artifact bytes, sets the bound.
/// The reserved validation entry travels and replays with the terminal report.
pub(crate) fn fit_report(report: &mut ExecutionTerminalNotification) {
    let mut dropped = 0;
    while serde_json::to_vec(report)
        .map(|bytes| bytes.len() > MAX_TERMINAL_REPORT_SIZE - 1024)
        .unwrap_or(true)
        && !report.outbox_entries.is_empty()
    {
        report.outbox_entries.pop();
        dropped += 1;
    }
    if dropped > 0 {
        report.outbox_entries.retain(|entry| {
            !matches!(
                entry,
                ExecutionOutboxEntry::Worklog {
                    position,
                    ..
                } if position == TRUNCATION_LINE
            )
        });
        reserve_truncation_room(report);
        append_truncation_marker(&mut report.outbox_entries, dropped);
    }
}

fn reserve_truncation_room(report: &mut ExecutionTerminalNotification) {
    // Keep accounting and identities intact even when a CLI's summary or
    // error leaves no space for the durable outbox truncation marker.
    const SUFFIX: &str = "\n[truncated to fit daemon journal]";
    for index in 0..2 {
        let excess = serde_json::to_vec(report)
            .map(|bytes| bytes.len().saturating_sub(MAX_TERMINAL_REPORT_SIZE - 1024))
            .unwrap_or(0);
        if excess == 0 {
            break;
        }
        let field = if index == 0 {
            &mut report.summary
        } else {
            &mut report.error
        };
        if let Some(text) = field {
            let mut keep = text
                .len()
                .saturating_sub(excess.saturating_add(SUFFIX.len()));
            while !text.is_char_boundary(keep) {
                keep -= 1;
            }
            text.truncate(keep);
            text.push_str(SUFFIX);
        }
    }
}

fn append_truncation_marker(entries: &mut Vec<ExecutionOutboxEntry>, mut dropped: usize) {
    if entries
        .iter()
        .filter(|entry| matches!(entry, ExecutionOutboxEntry::Worklog { .. }))
        .count()
        >= MAX_EXECUTION_OUTBOX_ENTRIES_PER_KIND
    {
        if let Some(index) = entries
            .iter()
            .rposition(|entry| matches!(entry, ExecutionOutboxEntry::Worklog { .. }))
        {
            entries.remove(index);
            dropped += 1;
        }
    }
    entries.push(truncation_marker(dropped));
}

fn truncation_marker(dropped: usize) -> ExecutionOutboxEntry {
    ExecutionOutboxEntry::Worklog {
        position: TRUNCATION_LINE.to_owned(),
        kind: ExecutionOutboxWorklogKind::Validation,
        summary: format!("Forge daemon outbox truncated: {dropped} entries or files were omitted by capture or terminal-report bounds."),
    }
}

fn parse_entry(
    value: Value,
    position: String,
    evidence: bool,
    worktree: &Path,
    outbox: &Path,
) -> Option<ExecutionOutboxEntry> {
    let kind = value.get("kind")?.as_str()?;
    if !evidence {
        let summary = value.get("summary")?.as_str()?.trim();
        if summary.is_empty() || summary.chars().count() > MAX_EXECUTION_OUTBOX_SUMMARY_CHARS {
            return None;
        }
        return Some(ExecutionOutboxEntry::Worklog {
            position,
            kind: serde_json::from_value(Value::String(kind.to_owned())).ok()?,
            summary: summary.to_owned(),
        });
    }
    let raw_kind = kind;
    if raw_kind.trim().is_empty() {
        return None;
    }
    let kind: ExecutionOutboxEvidenceKind =
        serde_json::from_value(Value::String(raw_kind.to_owned()))
            .unwrap_or(ExecutionOutboxEvidenceKind::Other);
    let caption = value.get("caption")?.as_str()?.trim();
    if caption.is_empty() || caption.chars().count() > MAX_EXECUTION_OUTBOX_SUMMARY_CHARS {
        return None;
    }
    let caption = if kind == ExecutionOutboxEvidenceKind::Other && raw_kind != "other" {
        format!("[{raw_kind}] {caption}")
    } else {
        caption.to_owned()
    };
    let path = value
        .get("path")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|s| !s.is_empty());
    let content = value
        .get("content")
        .and_then(Value::as_str)
        .filter(|s| !s.trim().is_empty());
    let artifact = match (path, content) {
        (Some(path), None) => {
            let requested = Path::new(path);
            let resolved = if requested.is_absolute() {
                daemon_fs::validate_within_root(requested, outbox).ok()?
            } else {
                daemon_fs::validate_within_root(requested, worktree).ok()?
            };
            let metadata = fs::metadata(&resolved).ok()?;
            if !metadata.is_file()
                || metadata.len() == 0
                || metadata.len() as usize > MAX_EMBEDDED_ARTIFACT_BYTES
            {
                return None;
            }
            let mut bytes = Vec::new();
            File::open(&resolved)
                .ok()?
                .take(MAX_EMBEDDED_ARTIFACT_BYTES as u64 + 1)
                .read_to_end(&mut bytes)
                .ok()?;
            if bytes.is_empty() || bytes.len() > MAX_EMBEDDED_ARTIFACT_BYTES {
                return None;
            }
            let filename = resolved.file_name()?.to_str()?.to_owned();
            Some(ExecutionOutboxArtifact {
                content_type: content_type(&filename, kind),
                filename,
                bytes,
            })
        }
        (None, Some(text)) if text.len() <= MAX_EMBEDDED_OUTBOX_BYTES => None,
        _ => return None,
    };
    Some(ExecutionOutboxEntry::Evidence {
        position,
        kind,
        caption: caption.to_owned(),
        path: path.map(str::to_owned),
        content: content.map(str::to_owned),
        artifact,
    })
}

fn content_type(filename: &str, kind: ExecutionOutboxEvidenceKind) -> String {
    let extension = Path::new(filename)
        .extension()
        .and_then(|s| s.to_str())
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oversized_evidence_is_marked_and_report_fits_journal() {
        let dir = tempfile::tempdir().unwrap();
        let worktree = dir.path().join("task/repo");
        fs::create_dir_all(&worktree).unwrap();
        let outbox = executors::execution_outbox_path(&worktree, "exec-1").unwrap();
        fs::create_dir_all(&outbox).unwrap();
        fs::write(
            outbox.join("huge.png"),
            vec![1; MAX_EMBEDDED_ARTIFACT_BYTES + 1],
        )
        .unwrap();
        fs::write(outbox.join(executors::OUTBOX_EVIDENCE_FILE), serde_json::json!({"kind":"screenshot", "caption":"large", "path":outbox.join("huge.png")}).to_string()).unwrap();
        let entries = harvest(&worktree, "exec-1", dir.path());
        assert!(
            matches!(&entries[0], ExecutionOutboxEntry::Worklog { position, summary, .. } if position == TRUNCATION_LINE && summary.contains("truncated"))
        );
        let mut report: ExecutionTerminalNotification = serde_json::from_value(serde_json::json!({"terminal_report_id":"report-1", "execution_id":"exec-1", "exit_code":0, "signal":null, "error":null, "ts":"now", "usage_reports":[]})).unwrap();
        report.outbox_entries = entries;
        fit_report(&mut report);
        crate::daemon_persistence::DaemonJournal::new(dir.path())
            .retain(&report)
            .unwrap();
    }

    #[test]
    fn serialized_outbox_and_final_report_caps_keep_a_truncation_marker() {
        let dir = tempfile::tempdir().unwrap();
        let worktree = dir.path().join("task/repo");
        fs::create_dir_all(&worktree).unwrap();
        let outbox = executors::execution_outbox_path(&worktree, "exec-many").unwrap();
        fs::create_dir_all(&outbox).unwrap();
        let line = serde_json::json!({"kind":"progress", "summary":"x".repeat(MAX_EXECUTION_OUTBOX_SUMMARY_CHARS)}).to_string();
        fs::write(
            outbox.join(executors::OUTBOX_WORKLOG_FILE),
            format!("{line}\n").repeat(MAX_EXECUTION_OUTBOX_ENTRIES_PER_KIND),
        )
        .unwrap();
        let entries = harvest(&worktree, "exec-many", dir.path());
        assert!(serde_json::to_vec(&entries).unwrap().len() <= MAX_EMBEDDED_OUTBOX_BYTES);
        assert!(entries.len() <= MAX_EXECUTION_OUTBOX_ENTRIES_PER_KIND);
        for (index, near_cap) in [false, true].into_iter().enumerate() {
            let mut report: ExecutionTerminalNotification = serde_json::from_value(serde_json::json!({"terminal_report_id":format!("report-many-{index}"), "execution_id":"exec-many", "exit_code":0, "signal":null, "error":null, "ts":"now", "usage_reports":[]})).unwrap();
            let summary_bytes = if near_cap {
                MAX_TERMINAL_REPORT_SIZE - serde_json::to_vec(&report).unwrap().len() - 16
            } else {
                900 * 1024
            };
            report.summary = Some("s".repeat(summary_bytes));
            assert!(serde_json::to_vec(&report).unwrap().len() < MAX_TERMINAL_REPORT_SIZE);
            report.outbox_entries = entries.clone();
            fit_report(&mut report);
            assert!(serde_json::to_vec(&report).unwrap().len() <= MAX_TERMINAL_REPORT_SIZE);
            assert!(report.outbox_entries.iter().any(|entry| matches!(
                entry,
                ExecutionOutboxEntry::Worklog {
                    position,
                    ..
                } if position == TRUNCATION_LINE
            )));
            crate::daemon_persistence::DaemonJournal::new(dir.path())
                .retain(&report)
                .unwrap();
        }
    }
}
