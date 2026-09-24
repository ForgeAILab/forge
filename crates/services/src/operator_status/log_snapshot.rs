//! Rebuildable, bounded read-model cache. Never used for Task completion,
//! lease ownership, authorization, or usage accounting.

use std::{
    collections::VecDeque,
    fs::Metadata,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime},
};

use executors::{LogEntry, LogKind};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, BufReader};

const MAX_CACHED_LOGS: usize = 64;
// Also bounds staleness on filesystems with coarse modification timestamps.
const MAX_CACHE_AGE: Duration = Duration::from_secs(30);

type SharedSnapshot = Arc<tokio::sync::Mutex<Option<CachedSnapshot>>>;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct ExecutionLogSnapshot {
    pub turn_count: u32,
    pub last_event: Option<String>,
    pub last_event_time: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
struct FileStamp {
    len: u64,
    modified: Option<SystemTime>,
    created: Option<SystemTime>,
    #[cfg(unix)]
    dev: u64,
    #[cfg(unix)]
    ino: u64,
    #[cfg(unix)]
    changed: (i64, i64),
}

impl FileStamp {
    fn of(metadata: &Metadata) -> Self {
        #[cfg(unix)]
        use std::os::unix::fs::MetadataExt;
        Self {
            len: metadata.len(),
            modified: metadata.modified().ok(),
            created: metadata.created().ok(),
            #[cfg(unix)]
            dev: metadata.dev(),
            #[cfg(unix)]
            ino: metadata.ino(),
            #[cfg(unix)]
            changed: (metadata.ctime(), metadata.ctime_nsec()),
        }
    }
}

struct CachedSnapshot {
    stamp: FileStamp,
    snapshot: ExecutionLogSnapshot,
    stored_at: Instant,
}

#[derive(Default)]
struct SnapshotRead {
    snapshot: ExecutionLogSnapshot,
    cache_hit: bool,
    scanned_bytes: u64,
    scanned_lines: u64,
    parsed_entries: u64,
    success: bool,
}

#[derive(Default)]
pub(super) struct ExecutionLogSnapshots {
    // Only the lookup/eviction uses the synchronous mutex. File I/O is
    // serialized per log, not across unrelated executions.
    entries: Mutex<VecDeque<(PathBuf, SharedSnapshot)>>,
}

impl ExecutionLogSnapshots {
    fn entry(&self, path: &Path) -> SharedSnapshot {
        let mut entries = self
            .entries
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let entry = entries
            .iter()
            .position(|(key, _)| key == path)
            .and_then(|position| entries.remove(position))
            .unwrap_or_else(|| (path.to_path_buf(), Arc::new(tokio::sync::Mutex::new(None))));
        let shared = Arc::clone(&entry.1);
        entries.push_back(entry);
        while entries.len() > MAX_CACHED_LOGS {
            entries.pop_front();
        }
        shared
    }

    pub async fn read(&self, logs_path: Option<String>) -> ExecutionLogSnapshot {
        let Some(logs_path) = logs_path else {
            return ExecutionLogSnapshot::default();
        };
        let started = Instant::now();
        let read = self.read_measured(Path::new(&logs_path)).await;
        // Intentionally no path, execution id, prompt, tool output, or error
        // text. These samples may be included in a support report.
        tracing::debug!(
            target: "forge::perf",
            operation = "operations_log_snapshot",
            elapsed_ms = started.elapsed().as_secs_f64() * 1000.0,
            cache_hit = read.cache_hit,
            scanned_bytes = read.scanned_bytes,
            scanned_lines = read.scanned_lines,
            parsed_entries = read.parsed_entries,
            success = read.success,
            "performance sample"
        );
        read.snapshot
    }

    async fn read_measured(&self, path: &Path) -> SnapshotRead {
        let shared = self.entry(path);
        let mut cached = shared.lock().await;
        let mut read = SnapshotRead::default();
        // Open and stat the same handle used for scanning. A rename between
        // requests cannot make us reuse a snapshot solely by pathname.
        let Ok(file) = tokio::fs::File::open(path).await else {
            *cached = None;
            return read;
        };
        let Ok(metadata) = file.metadata().await else {
            *cached = None;
            return read;
        };
        let stamp = FileStamp::of(&metadata);
        if let Some(hit) = cached.as_ref().filter(|hit| {
            stamp.modified.is_some()
                && hit.stamp == stamp
                && hit.stored_at.elapsed() < MAX_CACHE_AGE
        }) {
            read.snapshot = hit.snapshot.clone();
            read.cache_hit = true;
            read.success = true;
            return read;
        }
        *cached = None;

        // A growing log cannot keep this query alive indefinitely: scan at
        // most the bytes present when the file was opened, in ONE pass.
        let mut reader = BufReader::new(file.take(stamp.len));
        let mut line = Vec::new();
        loop {
            line.clear();
            match reader.read_until(b'\n', &mut line).await {
                Ok(0) => break,
                Ok(bytes) => {
                    read.scanned_bytes = read.scanned_bytes.saturating_add(bytes as u64);
                    read.scanned_lines = read.scanned_lines.saturating_add(1);
                }
                Err(_) => return read,
            }
            let Ok(entry) = serde_json::from_slice::<LogEntry>(&line) else {
                continue;
            };
            read.parsed_entries = read.parsed_entries.saturating_add(1);
            if entry.kind == LogKind::Assistant {
                read.snapshot.turn_count = read.snapshot.turn_count.saturating_add(1);
            }
            read.snapshot.last_event = Some(entry.kind.to_string());
            read.snapshot.last_event_time = Some(entry.timestamp);
        }
        read.success = true;
        // Do not cache a partial observation of a file changed during the
        // scan. A later request will rebuild; a read failure also never poisons
        // the cache. This cache deliberately contains summaries, not raw logs.
        if let Ok(after) = reader.get_ref().get_ref().metadata().await {
            if stamp.modified.is_some() && stamp == FileStamp::of(&after) {
                *cached = Some(CachedSnapshot {
                    stamp,
                    snapshot: read.snapshot.clone(),
                    stored_at: Instant::now(),
                });
            }
        }
        read
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::AsyncWriteExt;

    fn line(sequence: u64, kind: &str) -> String {
        format!(
            "{}\n",
            serde_json::json!({
                "schema_version": 1,
                "sequence": sequence,
                "timestamp": "2026-09-15T00:00:00Z",
                "execution_id": "test-execution",
                "kind": kind,
                "stream": "main",
                "payload": {},
                "truncated": false,
            })
        )
    }

    #[tokio::test]
    async fn snapshot_scans_each_line_once_and_reuses_unchanged_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution.jsonl");
        let contents = (0..10_000)
            .map(|seq| line(seq, "assistant"))
            .collect::<String>();
        tokio::fs::write(&path, &contents).await.unwrap();
        let cache = ExecutionLogSnapshots::default();
        let first = cache.read_measured(&path).await;
        assert!(first.success);
        assert_eq!(first.snapshot.turn_count, 10_000);
        assert_eq!(first.scanned_lines, 10_000);
        assert_eq!(first.scanned_bytes, contents.len() as u64);
        let second = cache.read_measured(&path).await;
        assert!(second.cache_hit);
        assert_eq!(second.scanned_bytes, 0);
        assert_eq!(second.parsed_entries, 0);
        assert_eq!(first.snapshot, second.snapshot);
    }

    #[tokio::test]
    async fn snapshot_refreshes_on_append_truncate_and_replacement() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution.jsonl");
        let cache = ExecutionLogSnapshots::default();
        tokio::fs::write(&path, line(0, "assistant")).await.unwrap();
        assert_eq!(cache.read_measured(&path).await.snapshot.turn_count, 1);
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .await
            .unwrap();
        file.write_all(line(1, "assistant").as_bytes())
            .await
            .unwrap();
        file.flush().await.unwrap();
        drop(file);
        assert_eq!(cache.read_measured(&path).await.snapshot.turn_count, 2);
        tokio::fs::write(&path, "").await.unwrap();
        assert_eq!(cache.read_measured(&path).await.snapshot.turn_count, 0);
        tokio::fs::remove_file(&path).await.unwrap();
        tokio::fs::write(&path, line(0, "system")).await.unwrap();
        let replaced = cache.read_measured(&path).await;
        assert!(!replaced.cache_hit);
        assert_eq!(replaced.snapshot.last_event.as_deref(), Some("system"));
    }

    #[tokio::test]
    async fn snapshot_retries_partial_json_and_does_not_cache_missing_files() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution.jsonl");
        let cache = ExecutionLogSnapshots::default();
        assert!(!cache.read_measured(&path).await.success);
        let complete = line(1, "assistant");
        let cut = complete.len() / 2;
        tokio::fs::write(
            &path,
            format!("{}garbage\n{}", line(0, "system"), &complete[..cut]),
        )
        .await
        .unwrap();
        let partial = cache.read_measured(&path).await;
        assert_eq!(partial.snapshot.turn_count, 0);
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .await
            .unwrap();
        file.write_all(&complete.as_bytes()[cut..]).await.unwrap();
        file.flush().await.unwrap();
        drop(file);
        assert_eq!(cache.read_measured(&path).await.snapshot.turn_count, 1);
        tokio::fs::remove_file(&path).await.unwrap();
        let missing = cache.read_measured(&path).await;
        assert!(!missing.success);
        assert_eq!(missing.snapshot, ExecutionLogSnapshot::default());
    }

    #[test]
    fn snapshot_cache_is_bounded_and_reuses_entries() {
        let cache = ExecutionLogSnapshots::default();
        let first = cache.entry(Path::new("first"));
        assert!(Arc::ptr_eq(&first, &cache.entry(Path::new("first"))));
        for index in 0..MAX_CACHED_LOGS + 1 {
            cache.entry(Path::new(&format!("log-{index}")));
        }
        assert_eq!(cache.entries.lock().unwrap().len(), MAX_CACHED_LOGS);
        assert!(!Arc::ptr_eq(&first, &cache.entry(Path::new("first"))));
    }

    #[tokio::test]
    async fn concurrent_reads_share_one_scan_and_cache_expiry_rebuilds() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("execution.jsonl");
        tokio::fs::write(&path, line(0, "assistant")).await.unwrap();
        let cache = ExecutionLogSnapshots::default();
        let (first, second) = tokio::join!(cache.read_measured(&path), cache.read_measured(&path));
        assert_eq!(first.snapshot, second.snapshot);
        assert_eq!(u8::from(first.cache_hit) + u8::from(second.cache_hit), 1);
        let shared = cache.entry(&path);
        shared.lock().await.as_mut().unwrap().stored_at =
            Instant::now() - MAX_CACHE_AGE - Duration::from_secs(1);
        assert!(!cache.read_measured(&path).await.cache_hit);
    }
}
