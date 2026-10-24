//! Native Task evidence capture, storage, and workspace artifact reads.

use super::*;

impl CoordinationToolProvider {
    /// Returns the bounded Project projection used by the Project Agent
    /// orchestration tool.  It intentionally contains no repository path,
    /// Workspace lease, credential, or cross-Project metadata.
    /// Capture one artifact this Task run produced as authoritative evidence.
    ///
    /// This is deliberately the only evidence-*producing* operation in the
    /// system, and it is deliberately Task-scoped. A Project Agent session has
    /// no workspace and no process by construction, so anything it "captured"
    /// would be authored rather than observed. A Task session has both, so the
    /// artifact it registers here is a real product of a real run.
    /// Append one entry to the Task worklog the next role will read.
    ///
    /// Provenance is server-derived on purpose: the Task comes from the bound
    /// scope, the execution and role from the run that is actually holding the
    /// workspace, and the identity from the session. An agent can write what it
    /// did; it cannot assert who it was or which run it belonged to.
    async fn running_task_execution(
        &self,
        actor: &str,
        runtime: &str,
        scope: &CanonicalScope,
    ) -> Result<(String, String, Option<String>), AgentHostError> {
        // The same lookup the authority resolver admits the session with.
        let execution = self
            .db
            .task_record_execution(actor, &scope.scope_id, runtime)
            .await
            .map_err(|_| AgentHostError::ProtectedPersistence)?
            .ok_or_else(|| {
                AgentHostError::Authority(
                    "Task record requires one exact running execution for this native session"
                        .into(),
                )
            })?;
        Ok((execution.id, execution.role, execution.workspace_id))
    }

    pub(super) async fn execute_task_worklog_append(
        &self,
        actor_identity_id: &str,
        runtime_session_id: &str,
        scope: &CanonicalScope,
        arguments: &Value,
        payload: &operation_registry::task_records::Worklog,
    ) -> Result<Value, AgentHostError> {
        let task_id = scope.scope_id.clone();
        let kind = payload.kind.as_str().to_owned();
        let summary = payload.summary.trim();
        if summary.is_empty() {
            return Err(invalid_arguments("summary is required".into()));
        }
        let (execution_id, role, _) = self
            .running_task_execution(actor_identity_id, runtime_session_id, scope)
            .await?;
        let execution_id = Some(execution_id);
        let role = Some(role);

        let idempotency_key = correlation_id(arguments, TASK_WORKLOG_OPERATION, scope);
        let now = db::now_rfc3339();
        let created = db::TaskCommentRepo::create_comment(
            &*self.db,
            db::CreateTaskComment {
                id: db::new_uuid_v4(),
                task_id: task_id.clone(),
                author_type: db::CommentAuthorType::Agent,
                author_id: Some(actor_identity_id.to_owned()),
                author_name: role.clone().unwrap_or_else(|| "agent".to_owned()),
                content: summary.to_owned(),
                execution_id,
                role: role.clone(),
                worklog_kind: Some(kind.clone()),
                idempotency_key: Some(idempotency_key),
                created_at: now.clone(),
                updated_at: now,
            },
        )
        .await
        .map_err(|error| AgentHostError::Runtime(error.to_string()))?;
        // The key returns the entry it first wrote. Reporting that entry for
        // a different one would tell the run its note was recorded when it
        // was dropped.
        if created.content != summary || created.worklog_kind.as_deref() != Some(kind.as_str()) {
            return Err(invalid_arguments(
                "correlation_id was already used for a different worklog entry on this Task; \
                 send this entry with a new correlation_id"
                    .to_owned(),
            ));
        }

        Ok(json!({
            "operation": TASK_WORKLOG_OPERATION,
            "task_id": task_id,
            "comment_id": created.id,
            "kind": kind,
            "execution_id": created.execution_id,
            "role": created.role,
            "domain_committed": true,
        }))
    }

    pub(super) async fn execute_task_evidence_capture(
        &self,
        actor_identity_id: &str,
        runtime_session_id: &str,
        scope: &CanonicalScope,
        payload: &operation_registry::task_records::Evidence,
    ) -> Result<Value, AgentHostError> {
        let task_id = scope.scope_id.clone();
        let (execution_id, role, execution_workspace_id) = self
            .running_task_execution(actor_identity_id, runtime_session_id, scope)
            .await?;
        let kind_and_caption = json!({"kind":payload.kind,"caption":payload.caption});
        let (kind, caption) =
            evidence_kind_and_caption(&kind_and_caption).map_err(invalid_arguments)?;
        let path = payload
            .path
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let content = payload
            .content
            .as_deref()
            .filter(|value| !value.trim().is_empty());
        // One source of bytes, never both: a caller supplying each would leave
        // the stored artifact ambiguous about what was actually observed.
        let (bytes, default_name, content_type) = match (path, content) {
            (Some(path), None) => {
                // The run's own workspace (a child Task runs in its root's);
                // an execution that recorded none uses the Task's, exactly
                // as the turn itself does.
                let workspace = self
                    .task_execution_workspace(&task_id, execution_workspace_id.as_deref())
                    .await?;
                let root = self.inspected_task_worktree(&task_id, &workspace).await?;
                let resolved =
                    resolve_workspace_artifact(&root, path).map_err(invalid_arguments)?;
                let relative = resolved
                    .strip_prefix(root.canonicalize().map_err(|error| {
                        AgentHostError::Runtime(format!("Task workspace is unavailable: {error}"))
                    })?)
                    .map_err(|error| AgentHostError::Authority(error.to_string()))?
                    .to_string_lossy()
                    .into_owned();
                let bytes = workspace
                    .backend
                    .read(
                        &workspace.placement,
                        &relative,
                        MAX_CAPTURED_EVIDENCE_BYTES as u64,
                    )
                    .await
                    .map_err(|error| {
                        AgentHostError::Runtime(format!("captured artifact is unreadable: {error}"))
                    })?;
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
            (Some(_), Some(_)) => {
                return Err(invalid_arguments(
                    "supply either path or content, not both".to_owned(),
                ));
            }
            (None, None) => {
                return Err(invalid_arguments(
                    "evidence capture requires either a workspace path or inline content"
                        .to_owned(),
                ));
            }
        };
        if bytes.is_empty() {
            return Err(invalid_arguments("captured artifact is empty".to_owned()));
        }
        if bytes.len() as i64 > MAX_CAPTURED_EVIDENCE_BYTES {
            return Err(invalid_arguments(format!(
                "captured artifact exceeds the {MAX_CAPTURED_EVIDENCE_BYTES} byte capture limit"
            )));
        }
        let filename = payload
            .filename
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty() && !value.contains('/') && !value.contains('\\'))
            .map_or(default_name, str::to_owned);
        let filename = match Some(payload.kind.as_str()) {
            Some(original_kind) if original_kind != kind => {
                format!("[{}] {filename}", original_kind.replace(['/', '\\'], "_"))
            }
            _ => filename,
        };

        let stored = self
            .store_task_evidence(actor_identity_id, &task_id, &filename, content_type, &bytes)
            .await?;

        Ok(json!({
            "operation": TASK_EVIDENCE_OPERATION,
            "task_id": task_id,
            "media_id": stored.media_id,
            "asset_id": stored.asset_id,
            "checksum": stored.checksum,
            "byte_size": stored.byte_size,
            "kind": kind,
            "caption": caption,
            "execution_id": execution_id,
            "role": role,
            "domain_committed": true,
        }))
    }

    pub(super) async fn execute_task_plan_write(
        &self,
        actor_identity_id: &str,
        runtime_session_id: &str,
        scope: &CanonicalScope,
        payload: &operation_registry::task_records::Plan,
    ) -> Result<Value, AgentHostError> {
        let content = payload.content.as_str();
        let (execution_id, role, workspace_id) = self
            .running_task_execution(actor_identity_id, runtime_session_id, scope)
            .await?;
        if !executors::task_role_can_write_plan(Some(&role)) {
            return Err(AgentHostError::Authority(
                "task.plan is available only to planner and implementation roles".to_owned(),
            ));
        }
        let workspace_id = workspace_id.ok_or_else(|| {
            AgentHostError::Authority(
                "task.plan requires an execution-bound Task workspace".to_owned(),
            )
        })?;
        let worktree_path: Option<String> = self
            .db
            .native_ready_workspace_path(&workspace_id)
            .await
            .map_err(|error| AgentHostError::Runtime(error.to_string()))?;
        let worktree_path = worktree_path.ok_or_else(|| {
            AgentHostError::Runtime(
                "this execution has no active workspace for its plan candidate".to_owned(),
            )
        })?;
        match crate::plan_artifact::write_execution_outbox_plan(
            Path::new(&worktree_path),
            &execution_id,
            content,
        ) {
            Ok(()) => {}
            Err(
                crate::plan_artifact::PlanArtifactError::MissingChecklist { .. }
                | crate::plan_artifact::PlanArtifactError::FileTooLarge { .. },
            ) => {
                return Err(invalid_arguments(
                    "plan content must be a Markdown checklist no larger than 1 MiB".to_owned(),
                ));
            }
            Err(_) => {
                return Err(AgentHostError::Runtime(
                    "Forge could not store this execution's plan candidate".to_owned(),
                ));
            }
        }
        let checklist_items = crate::plan_artifact::parse_plan_markdown(content)
            .items
            .len();
        Ok(json!({
            "operation": TASK_PLAN_OPERATION,
            "task_id": scope.scope_id,
            "execution_id": execution_id,
            "role": role,
            "checklist_items": checklist_items,
            "domain_committed": true,
        }))
    }

    /// Store one captured artifact as Task media with the checksum milestone
    /// evidence attachment compares against.
    /// The admitted server verify hook uses the same idempotent Task evidence
    /// store as task.evidence and CLI outboxes, without inventing an Agent run.
    pub(crate) async fn capture_verify_evidence(
        &self,
        task_id: &str,
        media_id: &str,
        filename: &str,
        kind: &str,
        _caption: &str,
        bytes: &[u8],
    ) -> Result<String, AgentHostError> {
        if bytes.is_empty() || bytes.len() as i64 > MAX_CAPTURED_EVIDENCE_BYTES {
            return Err(invalid_arguments(
                "verify evidence must be 1 byte to 25 MiB".into(),
            ));
        }
        let stored = self
            .store_task_evidence_with_id(
                EvidenceAuthor::Verify,
                task_id,
                filename,
                content_type_for(filename, kind),
                bytes,
                media_id.to_owned(),
            )
            .await?;
        Ok(stored.media_id)
    }

    async fn store_task_evidence(
        &self,
        actor_identity_id: &str,
        task_id: &str,
        filename: &str,
        content_type: String,
        bytes: &[u8],
    ) -> Result<StoredEvidence, AgentHostError> {
        self.store_task_evidence_with_id(
            EvidenceAuthor::Agent(actor_identity_id),
            task_id,
            filename,
            content_type,
            bytes,
            db::new_uuid_v4(),
        )
        .await
    }

    /// Store Task evidence under a caller-selected durable identity.
    ///
    /// CLI outbox ingestion derives this identity from the execution and record
    /// position. If the process stops after the media row commits but before its
    /// caption comment is appended, replay resolves the same media instead of
    /// creating another asset.
    pub(super) async fn store_task_evidence_with_id(
        &self,
        author: EvidenceAuthor<'_>,
        task_id: &str,
        filename: &str,
        content_type: String,
        bytes: &[u8],
        media_id: String,
    ) -> Result<StoredEvidence, AgentHostError> {
        let media_root = self.media_root_handle().ok_or_else(|| {
            AgentHostError::Configuration("media storage is not configured".to_owned())
        })?;
        let storage_key = format!("{task_id}/{media_id}__{filename}");
        let byte_size = bytes.len() as i64;
        let checksum = hex::encode(Sha256::digest(bytes));

        if let Some(existing) = db::TaskMediaRepo::get_media_by_id(&*self.db, &media_id, true)
            .await
            .map_err(|error| AgentHostError::Runtime(error.to_string()))?
        {
            return self
                .resolve_task_evidence_receipt(
                    existing,
                    author,
                    task_id,
                    filename,
                    &content_type,
                    &storage_key,
                    byte_size,
                    &checksum,
                )
                .await;
        }

        let destination = safe_media_destination(&media_root, &storage_key)?;
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                AgentHostError::Runtime(format!("media storage is unavailable: {error}"))
            })?;
        }
        std::fs::write(&destination, bytes).map_err(|error| {
            AgentHostError::Runtime(format!("captured artifact could not be stored: {error}"))
        })?;

        let created = db::TaskMediaRepo::create_media(
            &*self.db,
            db::CreateTaskMedia {
                id: media_id.clone(),
                task_id: task_id.to_owned(),
                display_filename: filename.to_owned(),
                content_type: content_type.clone(),
                byte_size,
                storage_key: storage_key.clone(),
                author_type: author.kind(),
                author_id: author.id().map(str::to_owned),
                author_name: author.name().to_owned(),
                created_at: db::now_rfc3339(),
            },
        )
        .await;
        let created = match created {
            Ok(created) => created,
            Err(error) => {
                // A concurrent replay may have won the deterministic media
                // insert after the lookup above. Resolve that exact receipt;
                // do not turn its unique-key win into a second artifact.
                match db::TaskMediaRepo::get_media_by_id(&*self.db, &media_id, true).await {
                    Ok(Some(existing)) => existing,
                    Ok(None) => {
                        let _ = std::fs::remove_file(&destination);
                        return Err(AgentHostError::Runtime(error.to_string()));
                    }
                    Err(lookup_error) => {
                        return Err(AgentHostError::Runtime(format!(
                            "{error}; evidence receipt lookup failed: {lookup_error}"
                        )));
                    }
                }
            }
        };

        self.resolve_task_evidence_receipt(
            created,
            author,
            task_id,
            filename,
            &content_type,
            &storage_key,
            byte_size,
            &checksum,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn resolve_task_evidence_receipt(
        &self,
        media: db::TaskMedia,
        author: EvidenceAuthor<'_>,
        task_id: &str,
        filename: &str,
        content_type: &str,
        storage_key: &str,
        byte_size: i64,
        checksum: &str,
    ) -> Result<StoredEvidence, AgentHostError> {
        if media.task_id != task_id
            || media.display_filename != filename
            || media.content_type != content_type
            || media.byte_size != byte_size
            || media.storage_key != storage_key
            || media.author_type != author.kind()
            || media.author_id.as_deref() != author.id()
            || media.deleted_at.is_some()
        {
            return Err(AgentHostError::Runtime(
                "outbox evidence receipt conflicts with the persisted artifact".to_owned(),
            ));
        }

        // Milestone evidence attachment compares the caller's checksum against
        // this column; a Task artifact without one can never back a check.
        db::SharedMediaRepo::set_media_asset_checksum(
            &*self.db,
            &media.id,
            byte_size,
            checksum,
            &db::now_rfc3339(),
        )
        .await
        .map_err(|error| AgentHostError::Runtime(error.to_string()))?;
        let asset = db::SharedMediaRepo::get_media_asset_for_task_media(&*self.db, &media.id)
            .await
            .map_err(|error| AgentHostError::Runtime(error.to_string()))?
            .ok_or_else(|| {
                AgentHostError::Runtime("captured artifact has no media asset".to_owned())
            })?;
        Ok(StoredEvidence {
            media_id: media.id,
            asset_id: asset.id,
            checksum: checksum.to_owned(),
            byte_size,
        })
    }
    /// The Task worktree an artifact is read from, as the workspace manager
    /// finds it right now. Nothing is repaired for a read.
    async fn inspected_task_worktree(
        &self,
        task_id: &str,
        resolved: &crate::workspace_backend::ResolvedWorkspace,
    ) -> Result<PathBuf, AgentHostError> {
        let runtime = |error: String| AgentHostError::Runtime(error);
        let service = self
            .task_service_handle()
            .ok_or_else(|| runtime("workspace router is not configured".to_owned()))?;
        let task = db::TaskRepo::get_by_id(&*self.db, task_id, false)
            .await
            .map_err(|error| runtime(error.to_string()))?
            .ok_or_else(|| runtime(format!("task {task_id} not found")))?;
        let workspace = db::WorkspaceRepo::get_by_id(&*self.db, &resolved.placement.workspace_id)
            .await
            .map_err(|error| runtime(error.to_string()))?
            .ok_or_else(|| runtime("Task workspace is unavailable: no workspace".to_owned()))?;
        crate::workspace_manager::inspect_path(
            &self.db,
            service.workspace_root(),
            &service.workspace_backend_router(),
            &task,
            workspace,
        )
        .await
        .map_err(|error| runtime(error.to_string()))?
        .ok_or_else(|| {
            runtime("Task workspace is unavailable: its worktree is not usable".to_owned())
        })
    }

    async fn task_execution_workspace(
        &self,
        task_id: &str,
        workspace_id: Option<&str>,
    ) -> Result<crate::workspace_backend::ResolvedWorkspace, AgentHostError> {
        let workspace = match workspace_id {
            Some(workspace_id) => db::WorkspaceRepo::get_by_id(&*self.db, workspace_id).await,
            None => db::WorkspaceRepo::get_by_task_id(&*self.db, task_id).await,
        }
        .map_err(|error| AgentHostError::Runtime(error.to_string()))?
        .filter(|workspace| workspace.status != db::WorkspaceStatus::Cleaned)
        .ok_or_else(|| {
            AgentHostError::Runtime(
                "this Task has no active workspace to capture an artifact from".to_owned(),
            )
        })?;
        let router = self
            .task_service_handle()
            .map(|service| service.workspace_backend_router())
            .ok_or_else(|| {
                AgentHostError::Runtime("workspace router is not configured".to_owned())
            })?;
        router
            .resolve(&self.db, &workspace)
            .await
            .map_err(|error| AgentHostError::Runtime(error.to_string()))
    }

    /// The Project Agent's own verification workspace root (`forge/` plus the
    /// disposable `checkout/`), as persisted on its `project_verify` scope.
    async fn project_verify_workspace_root(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
    ) -> Result<PathBuf, AgentHostError> {
        let path: Option<String> = self
            .db
            .native_project_verify_workspace_path(
                actor_identity_id,
                scope_type_name(scope.scope_type),
                &scope.scope_id,
            )
            .await
            .map_err(|error| AgentHostError::Runtime(error.to_string()))?;
        path.filter(|value| !value.trim().is_empty())
            .map(PathBuf::from)
            .ok_or_else(|| {
                AgentHostError::Runtime(
                    "this session has no Project verification workspace to capture an artifact \
                     from; pass the artifact text inline as `content` instead"
                        .to_owned(),
                )
            })
    }

    /// Store an artifact the Project Agent's own verification run produced as
    /// a Project media asset, and rewrite the `capture` payload into the
    /// equivalent bounded `attach`. This is the Project-scope sibling of
    /// `task.evidence`: a Task run captures what its own execution did, while
    /// this path captures what the Agent itself observed in its workspace.
    pub(super) async fn capture_project_evidence_asset(
        &self,
        actor_identity_id: &str,
        scope: &CanonicalScope,
        payload: Value,
        arguments: &Value,
    ) -> Result<Value, AgentHostError> {
        let project_id = self
            .authorization
            .project_orchestration_target(actor_identity_id, scope)
            .await
            .map_err(native_scope_error)?;
        let (kind, caption) = evidence_kind_and_caption(&payload).map_err(invalid_arguments)?;
        if payload
            .get("asset_id")
            .is_some_and(|value| !value.is_null())
            || payload
                .get("checksum")
                .is_some_and(|value| !value.is_null())
        {
            return Err(invalid_arguments(
                "capture creates the asset itself; asset_id and checksum belong to attach"
                    .to_owned(),
            ));
        }
        let path = payload
            .get("path")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty());
        let content = payload
            .get("content")
            .and_then(Value::as_str)
            .filter(|value| !value.trim().is_empty());
        // One source of bytes, never both: a caller supplying each would leave
        // the stored artifact ambiguous about what was actually observed.
        let (bytes, default_name, content_type) = match (path, content) {
            (Some(path), None) => {
                let root = self
                    .project_verify_workspace_root(actor_identity_id, scope)
                    .await?;
                let resolved =
                    resolve_workspace_artifact(&root, path).map_err(invalid_arguments)?;
                let bytes = read_bounded_regular_file(
                    &resolved,
                    &root,
                    MAX_CAPTURED_EVIDENCE_BYTES as u64,
                    false,
                )
                .map_err(|error| {
                    AgentHostError::Runtime(format!("captured artifact is unreadable: {error}"))
                })?
                .ok_or_else(|| {
                    AgentHostError::Runtime("captured artifact does not exist".to_owned())
                })?;
                let name = resolved
                    .file_name()
                    .and_then(|value| value.to_str())
                    .unwrap_or("artifact")
                    .to_owned();
                // The Project media store accepts a closed content-type set;
                // structured-text and unknown captures are stored as plain
                // text rather than refused after the bytes were read.
                let content_type = match content_type_for(&name, kind).as_str() {
                    "application/json" | "application/octet-stream" => "text/plain".to_owned(),
                    other => other.to_owned(),
                };
                (bytes, name, content_type)
            }
            (None, Some(content)) => (
                content.as_bytes().to_vec(),
                format!("{kind}.txt"),
                "text/plain".to_owned(),
            ),
            (Some(_), Some(_)) => {
                return Err(invalid_arguments(
                    "supply either path or content, not both".to_owned(),
                ));
            }
            (None, None) => {
                return Err(invalid_arguments(
                    "evidence capture requires either a workspace path or inline content"
                        .to_owned(),
                ));
            }
        };
        if bytes.is_empty() {
            return Err(invalid_arguments("captured artifact is empty".to_owned()));
        }
        if bytes.len() as i64 > MAX_CAPTURED_EVIDENCE_BYTES {
            return Err(invalid_arguments(format!(
                "captured artifact exceeds the {MAX_CAPTURED_EVIDENCE_BYTES} byte capture limit"
            )));
        }
        let filename = payload
            .get("filename")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty() && !value.contains('/') && !value.contains('\\'))
            .map_or(default_name, str::to_owned);
        let media_root = self.media_root_handle().ok_or_else(|| {
            AgentHostError::Configuration("media storage is not configured".to_owned())
        })?;
        let project_version: Option<i64> = self
            .db
            .native_project_version(&project_id)
            .await
            .map_err(|error| AgentHostError::Runtime(error.to_string()))?;
        let project_version = project_version.ok_or_else(|| {
            AgentHostError::Runtime("the bound Project no longer exists".to_owned())
        })?;
        let dedupe_key = required_argument(arguments, "dedupe_key")?;
        let correlation_id = required_argument(arguments, "correlation_id")?;
        let byte_size = bytes.len() as i64;
        let checksum = hex::encode(Sha256::digest(&bytes));
        let asset_id = db::new_uuid_v4();
        // The storage key matches the user upload route so one GC/tombstone
        // path covers every Project media asset.
        let storage_key = format!("projects/{project_id}/{asset_id}__{filename}");
        let destination = safe_media_destination(&media_root, &storage_key)?;
        if let Some(parent) = destination.parent() {
            std::fs::create_dir_all(parent).map_err(|error| {
                AgentHostError::Runtime(format!("media storage is unavailable: {error}"))
            })?;
        }
        std::fs::write(&destination, &bytes).map_err(|error| {
            AgentHostError::Runtime(format!("captured artifact could not be stored: {error}"))
        })?;
        let mutation_fingerprint = hex::encode(Sha256::digest(
            json!({
                "operation": "project.evidence.capture",
                "project_id": project_id,
                "filename": filename,
                "byte_size": byte_size,
                "checksum": checksum,
            })
            .to_string()
            .as_bytes(),
        ));
        let created = db::SharedMediaRepo::create_project_media_asset(
            &*self.db,
            db::CreateProjectMediaAsset {
                id: asset_id.clone(),
                project_id: project_id.clone(),
                display_filename: filename,
                content_type,
                byte_size,
                storage_key,
                checksum: checksum.clone(),
                idempotency_key: format!("agent-evidence-capture:{dedupe_key}"),
                mutation_fingerprint,
                expected_project_version: project_version,
                actor_type: "agent".to_owned(),
                actor_id: Some(actor_identity_id.to_owned()),
                authorization_event_id: correlation_id,
                created_at: db::now_rfc3339(),
            },
        )
        .await;
        let created = match created {
            Ok(asset) => asset,
            Err(error) => {
                let _ = std::fs::remove_file(&destination);
                return Err(AgentHostError::Runtime(error.to_string()));
            }
        };
        if created.id != asset_id {
            // Idempotent replay returned the already-stored asset; the bytes
            // written above belong to no row.
            let _ = std::fs::remove_file(&destination);
        }
        // Creation leaves the asset quarantined until its bytes are in place
        // (the user upload flow stages first). Capture wrote the final bytes
        // above, so promote to available now — the attach command refuses a
        // quarantined asset. Finalize is idempotent on replay.
        db::SharedMediaRepo::finalize_project_media_upload(
            &*self.db,
            &project_id,
            &created.id,
            &db::now_rfc3339(),
        )
        .await
        .map_err(|error| AgentHostError::Runtime(error.to_string()))?;
        let mut object = payload.as_object().cloned().unwrap_or_default();
        object.insert("action".to_owned(), json!("attach"));
        object.insert("kind".to_owned(), json!(kind));
        object.insert("caption".to_owned(), json!(caption));
        object.insert("asset_id".to_owned(), json!(created.id));
        object.insert("checksum".to_owned(), json!(checksum));
        object.remove("content");
        object.remove("path");
        object.remove("filename");
        Ok(Value::Object(object))
    }
}

pub(super) fn evidence_kind_and_caption(payload: &Value) -> Result<(&str, String), String> {
    let kind = payload
        .get("kind")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .ok_or("kind must be a non-empty string")?;
    let caption = payload
        .get("caption")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or("caption describing the artifact is required")?;
    match kind {
        "screenshot" | "walkthrough_video" | "log" | "report" | "other" => {
            Ok((kind, caption.to_owned()))
        }
        _ => Ok(("other", format!("[{kind}] {caption}"))),
    }
}

/// Resolve a caller-supplied artifact path inside the Task workspace.
/// Traversal, absolute paths, and symlinked escapes are all rejected: the
/// capture surface must never read a file the Task session could not read.
pub(super) fn resolve_workspace_artifact(root: &Path, relative: &str) -> Result<PathBuf, String> {
    let candidate = Path::new(relative);
    if candidate.is_absolute() {
        return Err("captured artifact path must be workspace-relative".to_owned());
    }
    if candidate
        .components()
        .any(|component| matches!(component, Component::ParentDir | Component::Prefix(_)))
    {
        return Err("captured artifact path escapes the Task workspace".to_owned());
    }
    let joined = root.join(candidate);
    let root_metadata = fs::symlink_metadata(root)
        .map_err(|error| format!("Task workspace is unavailable: {error}"))?;
    if !root_metadata.file_type().is_dir() {
        return Err("Task workspace must be a real directory".to_owned());
    }
    let leaf_metadata = fs::symlink_metadata(&joined)
        .map_err(|error| format!("captured artifact does not exist: {error}"))?;
    if !leaf_metadata.file_type().is_file() {
        return Err("captured artifact must be a regular file, not a symlink".to_owned());
    }
    let canonical_root = root
        .canonicalize()
        .map_err(|error| format!("Task workspace is unavailable: {error}"))?;
    let canonical = joined
        .canonicalize()
        .map_err(|error| format!("captured artifact does not exist: {error}"))?;
    if !canonical.starts_with(&canonical_root) {
        return Err("captured artifact path escapes the Task workspace".to_owned());
    }
    if !canonical.is_file() {
        return Err("captured artifact is not a file".to_owned());
    }
    Ok(joined)
}

/// Build the on-disk destination for a storage key without letting the key
/// itself walk out of the media root.
pub(super) fn safe_media_destination(
    root: &Path,
    storage_key: &str,
) -> Result<PathBuf, AgentHostError> {
    let candidate = Path::new(storage_key);
    if candidate.is_absolute()
        || candidate
            .components()
            .any(|component| matches!(component, Component::ParentDir | Component::Prefix(_)))
    {
        return Err(AgentHostError::Authority(
            "media storage key is invalid".to_owned(),
        ));
    }
    Ok(root.join(candidate))
}

pub(super) fn content_type_for(filename: &str, kind: &str) -> String {
    let extension = Path::new(filename)
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase();
    match extension.as_str() {
        "png" => "image/png".to_owned(),
        "jpg" | "jpeg" => "image/jpeg".to_owned(),
        "gif" => "image/gif".to_owned(),
        "webp" => "image/webp".to_owned(),
        "webm" => "video/webm".to_owned(),
        "mp4" => "video/mp4".to_owned(),
        "json" => "application/json".to_owned(),
        "txt" | "log" | "md" => "text/plain".to_owned(),
        _ if kind == "screenshot" => "image/png".to_owned(),
        _ if kind == "walkthrough_video" => "video/webm".to_owned(),
        _ => "application/octet-stream".to_owned(),
    }
}
