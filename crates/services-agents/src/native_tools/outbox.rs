//! Execution outbox ingestion and bounded artifact parsing.

use super::*;

impl CoordinationToolProvider {
    /// Ingest the worklog and evidence a CLI harness wrote to its outbox.
    ///
    /// A CLI harness has no Forge tool channel, so this is its equivalent of
    /// `task.worklog` and `task.evidence`, applied when the execution ends.
    /// Every entry is validated exactly as the native tools validate it, and
    /// provenance still comes from the execution row rather than the files.
    /// A malformed entry is skipped and reported, never fatal: the run's real
    /// outcome must not hinge on its bookkeeping. An authorized plan candidate
    /// is frozen into a bounded host-owned stage before the outbox is removed;
    /// only the terminal-CAS winner may publish that exact snapshot.
    pub async fn ingest_execution_outbox(
        &self,
        input: &ExecutionOutboxInput<'_>,
    ) -> ExecutionOutboxReport {
        let mut report = ExecutionOutboxReport::default();
        let worktree = Path::new(input.worktree_path);
        let outbox = match executors::existing_execution_outbox(worktree, input.execution_id) {
            Ok(Some(outbox)) => outbox,
            Ok(None) => return report,
            Err(error) => {
                report
                    .rejected
                    .push(format!("execution outbox is unsafe: {error}"));
                return report;
            }
        };

        let author_name = input.role.unwrap_or("agent").to_owned();
        for (position, entry) in
            read_outbox_entries(&outbox, executors::OUTBOX_WORKLOG_FILE, &mut report)
        {
            let result = match entry {
                Ok(entry) => {
                    self.ingest_outbox_worklog(input, &author_name, &position, &entry)
                        .await
                }
                Err(reason) => Err(reason),
            };
            match result {
                Ok(()) => report.worklog_entries += 1,
                Err(reason) => report.rejected.push(format!(
                    "{}:{position}: {reason}",
                    executors::OUTBOX_WORKLOG_FILE
                )),
            }
        }
        for (position, entry) in
            read_outbox_entries(&outbox, executors::OUTBOX_EVIDENCE_FILE, &mut report)
        {
            let result = match entry {
                Ok(entry) => {
                    self.ingest_local_outbox_evidence(
                        input,
                        &author_name,
                        &outbox,
                        &position,
                        &entry,
                    )
                    .await
                }
                Err(reason) => Err(reason),
            };
            match result {
                Ok(()) => report.evidence_items += 1,
                Err(reason) => report.rejected.push(format!(
                    "{}:{position}: {reason}",
                    executors::OUTBOX_EVIDENCE_FILE
                )),
            }
        }

        if executors::task_role_can_write_plan(input.role) {
            match crate::plan_artifact::stage_execution_outbox_plan(
                &outbox,
                std::path::Path::new(input.worktree_path),
                input.execution_id,
            ) {
                Ok(true) => {
                    report.plan_candidate = true;
                }
                Ok(false) => {}
                Err(error) => {
                    report.plan_rejected = true;
                    report.rejected.push(format!("plan.md: {error}"));
                }
            }
        }

        // A staging failure may be transient (for example a process exited
        // during the prior no-replace install). Keep the source until the
        // terminal cascade either recovers it or settles the failure path.
        if !report.plan_rejected {
            if let Err(error) = executors::remove_execution_outbox(worktree, input.execution_id) {
                report.rejected.push(format!(
                    "outbox could not be removed after ingestion: {error}"
                ));
            }
        }
        report
    }

    async fn ingest_outbox_worklog(
        &self,
        input: &ExecutionOutboxInput<'_>,
        author_name: &str,
        position: &str,
        entry: &Value,
    ) -> Result<(), String> {
        let idempotency_key = format!("outbox:{}:worklog:{position}", input.execution_id);
        if self
            .outbox_worklog_receipt_exists(input.task_id, &idempotency_key)
            .await?
        {
            return Ok(());
        }
        let kind = entry
            .get("kind")
            .and_then(Value::as_str)
            .filter(|value| matches!(*value, "progress" | "decision" | "validation" | "blocker"))
            .ok_or("kind must be progress, decision, validation, or blocker")?;
        let summary = entry
            .get("summary")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or("summary is required")?;
        if summary.chars().count() > MAX_WORKLOG_SUMMARY_CHARS {
            return Err(format!(
                "summary exceeds the {MAX_WORKLOG_SUMMARY_CHARS} character worklog limit"
            ));
        }
        self.append_outbox_worklog(input, author_name, kind, summary, idempotency_key)
            .await
    }

    pub(super) async fn ingest_local_outbox_evidence(
        &self,
        input: &ExecutionOutboxInput<'_>,
        author_name: &str,
        outbox: &Path,
        position: &str,
        entry: &Value,
    ) -> Result<(), String> {
        let idempotency_key = format!("outbox:{}:evidence:{position}", input.execution_id);
        if self
            .outbox_worklog_receipt_exists(input.task_id, &idempotency_key)
            .await?
        {
            return Ok(());
        }

        let (kind, caption) = evidence_kind_and_caption(entry)?;
        let path = entry
            .get("path")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let content = entry
            .get("content")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty());
        let (bytes, filename, content_type) = match (path, content) {
            (Some(path), None) => {
                let (resolved, allowed_root, require_single_link) =
                    resolve_outbox_artifact(Path::new(input.worktree_path), outbox, path)?;
                let bytes = read_bounded_regular_file(
                    &resolved,
                    &allowed_root,
                    MAX_CAPTURED_EVIDENCE_BYTES as u64,
                    require_single_link,
                )?
                .ok_or("captured artifact does not exist")?;
                let name = resolved
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or("artifact")
                    .to_owned();
                let content_type = content_type_for(&name, kind);
                (bytes, name, content_type)
            }
            (None, Some(content)) => (
                content.as_bytes().to_vec(),
                format!("{kind}.txt"),
                "text/plain".to_owned(),
            ),
            (Some(_), Some(_)) => return Err("supply either path or content, not both".to_owned()),
            (None, None) => {
                return Err("evidence requires either a path or inline content".to_owned());
            }
        };
        if bytes.is_empty() {
            return Err("captured artifact is empty".to_owned());
        }
        if bytes.len() as i64 > MAX_CAPTURED_EVIDENCE_BYTES {
            return Err(format!(
                "captured artifact exceeds the {MAX_CAPTURED_EVIDENCE_BYTES} byte capture limit"
            ));
        }
        self.store_task_evidence_with_id(
            EvidenceAuthor::Agent(input.agent_id),
            input.task_id,
            &filename,
            content_type,
            &bytes,
            outbox_evidence_media_id(input.execution_id, position),
        )
        .await
        .map_err(|error| error.to_string())?;
        // The file has no response channel to report the stored asset back
        // through, so the caption becomes the worklog line naming it.
        self.append_outbox_worklog(
            input,
            author_name,
            "validation",
            &format!("Captured {kind} evidence `{filename}`: {caption}"),
            idempotency_key,
        )
        .await
    }

    /// Apply owner-harvested entries without opening any owner-local paths.
    pub async fn ingest_execution_outbox_entries(
        &self,
        input: &ExecutionOutboxInput<'_>,
        entries: Vec<api_types::ExecutionOutboxEntry>,
    ) -> ExecutionOutboxReport {
        use api_types::{ExecutionOutboxEntry, ExecutionOutboxWorklogKind};
        let mut report = ExecutionOutboxReport::default();
        let author_name = input.role.unwrap_or("agent");
        let mut worklog_count = 0;
        let mut evidence_count = 0;
        let mut evidence_bytes = 0_u64;
        for entry in entries {
            let (file, line_no, result) = match entry {
                ExecutionOutboxEntry::Worklog {
                    position,
                    kind,
                    summary,
                } => {
                    worklog_count += 1;
                    let kind = match kind {
                        ExecutionOutboxWorklogKind::Progress => "progress",
                        ExecutionOutboxWorklogKind::Decision => "decision",
                        ExecutionOutboxWorklogKind::Validation => "validation",
                        ExecutionOutboxWorklogKind::Blocker => "blocker",
                    };
                    let result = if worklog_count > api_types::MAX_EXECUTION_OUTBOX_ENTRIES_PER_KIND
                    {
                        Err("execution outbox has too many worklog entries".to_owned())
                    } else if position.is_empty() {
                        Err("line number must be positive".to_owned())
                    } else if summary.trim().is_empty() {
                        Err("summary is required".to_owned())
                    } else if summary.trim().chars().count() > MAX_WORKLOG_SUMMARY_CHARS {
                        Err(format!(
                            "summary exceeds the {MAX_WORKLOG_SUMMARY_CHARS} character worklog limit"
                        ))
                    } else {
                        self.append_outbox_worklog(
                            input,
                            author_name,
                            kind,
                            summary.trim(),
                            format!("outbox:{}:worklog:{position}", input.execution_id),
                        )
                        .await
                    };
                    if result.is_ok() {
                        report.worklog_entries += 1;
                    }
                    (executors::OUTBOX_WORKLOG_FILE, position, result)
                }
                ExecutionOutboxEntry::Evidence {
                    position,
                    kind,
                    caption,
                    path,
                    content,
                    artifact,
                } => {
                    evidence_count += 1;
                    let size = artifact
                        .as_ref()
                        .map(|artifact| artifact.bytes.len())
                        .or_else(|| content.as_ref().map(String::len))
                        .unwrap_or(0) as u64;
                    evidence_bytes = evidence_bytes.saturating_add(size);
                    let result =
                        if evidence_count > api_types::MAX_EXECUTION_OUTBOX_ENTRIES_PER_KIND {
                            Err("execution outbox has too many evidence entries".to_owned())
                        } else if evidence_bytes > api_types::MAX_EXECUTION_OUTBOX_EVIDENCE_BYTES {
                            Err("execution outbox evidence exceeds size budget".to_owned())
                        } else {
                            self.ingest_outbox_evidence(
                                input,
                                author_name,
                                ExecutionOutboxEntry::Evidence {
                                    position: position.clone(),
                                    kind,
                                    caption,
                                    path,
                                    content,
                                    artifact,
                                },
                            )
                            .await
                        };
                    if result.is_ok() {
                        report.evidence_items += 1;
                    }
                    (executors::OUTBOX_EVIDENCE_FILE, position, result)
                }
            };
            if let Err(reason) = result {
                report.rejected.push(format!("{file}:{line_no}: {reason}"));
            }
        }
        report
    }

    async fn ingest_outbox_evidence(
        &self,
        input: &ExecutionOutboxInput<'_>,
        author_name: &str,
        entry: api_types::ExecutionOutboxEntry,
    ) -> Result<(), String> {
        use api_types::{ExecutionOutboxEntry, ExecutionOutboxEvidenceKind};
        let ExecutionOutboxEntry::Evidence {
            position: line_no,
            kind,
            caption,
            path,
            content,
            artifact,
        } = entry
        else {
            return Err("expected an evidence entry".to_owned());
        };
        let kind = match kind {
            ExecutionOutboxEvidenceKind::Screenshot => "screenshot",
            ExecutionOutboxEvidenceKind::WalkthroughVideo => "walkthrough_video",
            ExecutionOutboxEvidenceKind::Log => "log",
            ExecutionOutboxEvidenceKind::Report => "report",
            ExecutionOutboxEvidenceKind::Other => "other",
        };
        if line_no.is_empty() {
            return Err("line number must be positive".to_owned());
        }
        let caption = caption.trim();
        if caption.is_empty() {
            return Err("caption describing the artifact is required".to_owned());
        }
        let path = path
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let content = content.as_deref().filter(|value| !value.trim().is_empty());
        let (bytes, filename, content_type) = match (path, content, artifact) {
            (Some(_), None, Some(artifact)) => {
                if artifact.filename.is_empty()
                    || artifact.filename.contains('/')
                    || artifact.filename.contains('\\')
                {
                    return Err("captured artifact filename is invalid".to_owned());
                }
                (artifact.bytes, artifact.filename, artifact.content_type)
            }
            (None, Some(content), None) => (
                content.as_bytes().to_vec(),
                format!("{kind}.txt"),
                "text/plain".to_owned(),
            ),
            (Some(_), Some(_), _) => {
                return Err("supply either path or content, not both".to_owned());
            }
            (Some(_), None, None) => {
                return Err("owner did not supply captured artifact bytes".to_owned());
            }
            _ => {
                return Err(
                    "evidence requires either a path with captured bytes or inline content"
                        .to_owned(),
                );
            }
        };
        if bytes.is_empty() {
            return Err("captured artifact is empty".to_owned());
        }
        if bytes.len() as i64 > MAX_CAPTURED_EVIDENCE_BYTES {
            return Err(format!(
                "captured artifact exceeds the {MAX_CAPTURED_EVIDENCE_BYTES} byte capture limit"
            ));
        }
        let idempotency_key = format!("outbox:{}:evidence:{line_no}", input.execution_id);
        let already_ingested = self
            .db
            .native_ingested_task_comment(input.task_id, &idempotency_key)
            .await
            .map_err(|error| error.to_string())?
            .is_some();
        if already_ingested {
            return Ok(());
        }
        self.store_task_evidence_with_id(
            EvidenceAuthor::Agent(input.agent_id),
            input.task_id,
            &filename,
            content_type,
            &bytes,
            outbox_evidence_media_id(input.execution_id, &line_no),
        )
        .await
        .map_err(|error| error.to_string())?;
        self.append_outbox_worklog(
            input,
            author_name,
            "validation",
            &format!("Captured {kind} evidence `{filename}`: {caption}"),
            idempotency_key,
        )
        .await
    }

    async fn outbox_worklog_receipt_exists(
        &self,
        task_id: &str,
        idempotency_key: &str,
    ) -> Result<bool, String> {
        self.db
            .native_task_worklog_exists(task_id, idempotency_key)
            .await
            .map_err(|error| error.to_string())
    }

    pub(super) async fn append_outbox_worklog(
        &self,
        input: &ExecutionOutboxInput<'_>,
        author_name: &str,
        kind: &str,
        summary: &str,
        idempotency_key: String,
    ) -> Result<(), String> {
        let now = db::now_rfc3339();
        let result = db::TaskCommentRepo::create_comment(
            &*self.db,
            db::CreateTaskComment {
                id: db::new_uuid_v4(),
                task_id: input.task_id.to_owned(),
                author_type: db::CommentAuthorType::Agent,
                author_id: Some(input.agent_id.to_owned()),
                author_name: author_name.to_owned(),
                content: summary.to_owned(),
                execution_id: Some(input.execution_id.to_owned()),
                role: input.role.map(str::to_owned),
                worklog_kind: Some(kind.to_owned()),
                idempotency_key: Some(idempotency_key.clone()),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await;
        match result {
            Ok(_) => Ok(()),
            Err(error) => {
                // A concurrent replay can win the insert after the repository's
                // lookup. Its durable receipt makes this append successful too.
                if self
                    .outbox_worklog_receipt_exists(input.task_id, &idempotency_key)
                    .await?
                {
                    Ok(())
                } else {
                    Err(error.to_string())
                }
            }
        }
    }
}

pub(super) fn outbox_evidence_media_id(execution_id: &str, position: &str) -> String {
    Uuid::new_v5(
        &Uuid::NAMESPACE_OID,
        format!("forge:execution:{execution_id}:outbox-evidence:{position}").as_bytes(),
    )
    .to_string()
}

/// JSON objects from a bounded file, including pretty-printed and concatenated
/// objects. Malformed content is reported before resuming at the next line.
pub(super) fn read_outbox_entries(
    outbox: &Path,
    file: &str,
    report: &mut ExecutionOutboxReport,
) -> Vec<(String, Result<Value, String>)> {
    let path = outbox.join(file);
    let bytes = match read_bounded_regular_file(&path, outbox, MAX_OUTBOX_FILE_BYTES, true) {
        Ok(Some(bytes)) => bytes,
        Ok(None) => return Vec::new(),
        Err(error) => {
            report.rejected.push(format!("{file}: unreadable: {error}"));
            return Vec::new();
        }
    };
    let text = match String::from_utf8(bytes) {
        Ok(text) => text,
        Err(_) => {
            report
                .rejected
                .push(format!("{file}: unreadable: content is not valid UTF-8"));
            return Vec::new();
        }
    };
    let line_starts = std::iter::once(0)
        .chain(text.match_indices('\n').map(|(offset, _)| offset + 1))
        .collect::<Vec<_>>();
    let mut entries = Vec::new();
    let mut offset = 0;
    let mut previous_line = 0;
    while offset < text.len() {
        offset += text.as_bytes()[offset..]
            .iter()
            .take_while(|byte| matches!(**byte, b' ' | b'\t' | b'\r' | b'\n'))
            .count();
        if offset == text.len() {
            break;
        }
        if entries.len() == MAX_OUTBOX_ENTRIES {
            report.rejected.push(format!(
                "{file}: only the first {MAX_OUTBOX_ENTRIES} entries were ingested"
            ));
            break;
        }
        let line_no = line_starts.partition_point(|start| *start <= offset);
        // Preserve existing JSONL receipt keys; a second object on the same
        // physical line also needs its byte column to remain distinct.
        let position = if line_no == previous_line {
            format!("{line_no}:{}", offset - line_starts[line_no - 1] + 1)
        } else {
            line_no.to_string()
        };
        previous_line = line_no;
        let mut stream = serde_json::Deserializer::from_str(&text[offset..]).into_iter::<Value>();
        let Some(entry) = stream.next() else {
            break;
        };
        match entry {
            Ok(value) => {
                offset += stream.byte_offset();
                entries.push((
                    position,
                    if value.is_object() {
                        Ok(value)
                    } else {
                        Err("entry must be a JSON object".to_owned())
                    },
                ));
            }
            Err(error) => {
                entries.push((position, Err(error.to_string())));
                offset += text[offset..]
                    .find('\n')
                    .map_or(text.len() - offset, |index| index + 1);
            }
        }
    }
    entries
}

/// Resolve an outbox evidence path: worktree-relative, or absolute inside the
/// outbox (where a read-only role saves what it captured).
pub(super) fn resolve_outbox_artifact(
    worktree: &Path,
    outbox: &Path,
    path: &str,
) -> Result<(PathBuf, PathBuf, bool), String> {
    if !Path::new(path).is_absolute() {
        return resolve_workspace_artifact(worktree, path)
            .map(|path| (path, worktree.to_path_buf(), false));
    }
    let canonical_outbox = outbox
        .canonicalize()
        .map_err(|error| format!("outbox is unavailable: {error}"))?;
    let canonical = Path::new(path)
        .canonicalize()
        .map_err(|error| format!("captured artifact does not exist: {error}"))?;
    if !canonical.starts_with(&canonical_outbox) {
        return Err(
            "an absolute evidence path must point inside the outbox; use a worktree-relative path otherwise"
                .to_owned(),
        );
    }
    if !canonical.is_file() {
        return Err("captured artifact is not a file".to_owned());
    }
    Ok((PathBuf::from(path), outbox.to_path_buf(), true))
}
