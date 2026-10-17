-- Passive integration storage only: no legacy data writes and no logic triggers.
-- Rows are evidence, not authority: a deleted Repo (or its Project) removes its
-- queues and their attempts, a deleted location detaches, a deleted Task keeps
-- the attempt under its opaque task_ref. Nothing here restricts a deletion.
CREATE TABLE integration_queue (
    id TEXT PRIMARY KEY,
    repo_id TEXT NOT NULL REFERENCES repo(id) ON DELETE CASCADE,
    target_branch TEXT NOT NULL,
    target_location_id TEXT REFERENCES repo_location(id) ON DELETE SET NULL,
    target_owner_json TEXT CHECK(target_owner_json IS NULL OR json_valid(target_owner_json)),
    next_seq INTEGER NOT NULL DEFAULT 1 CHECK(next_seq >= 1),
    head_attempt_id TEXT REFERENCES integration_attempt(id) ON DELETE SET NULL DEFERRABLE INITIALLY DEFERRED,
    lease_owner TEXT,
    lease_until TEXT,
    fence_generation INTEGER NOT NULL DEFAULT 0 CHECK(fence_generation >= 0),
    state TEXT NOT NULL CHECK(state IN ('open','suspended','quarantined','closed')),
    available_at TEXT,
    updated_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    revision INTEGER NOT NULL DEFAULT 1 CHECK(revision >= 1),
    last_error_kind TEXT CHECK(last_error_kind IN ('infrastructure','target_unconfigured','target_ambiguous','target_unavailable','owner_required','unsupported_path','corrupt_import','contradictory_proof','needs_fact','timeout','workspace_lost','candidate_check_failed')),
    last_error TEXT CHECK(length(CAST(last_error AS BLOB)) <= 4096),
    UNIQUE(repo_id,target_branch),
    CHECK((lease_owner IS NULL) = (lease_until IS NULL))
);

CREATE TABLE integration_attempt (
    id TEXT PRIMARY KEY,
    queue_id TEXT REFERENCES integration_queue(id) ON DELETE CASCADE,
    task_id TEXT REFERENCES task(id) ON DELETE SET NULL,
    task_ref TEXT NOT NULL,
    project_ref TEXT NOT NULL,
    queue_seq INTEGER NOT NULL CHECK(queue_seq >= 1),
    attempt_number INTEGER NOT NULL DEFAULT 1 CHECK(attempt_number >= 1),
    predecessor_attempt_id TEXT REFERENCES integration_attempt(id) ON DELETE SET NULL,
    current INTEGER NOT NULL CHECK(current IN (0,1)),
    admission_key TEXT NOT NULL,
    expected_status TEXT NOT NULL,
    expected_epoch INTEGER NOT NULL CHECK(expected_epoch >= 0),
    observed_task_version INTEGER NOT NULL,
    workflow_ref_id TEXT REFERENCES task_step_workflow(id) ON DELETE SET NULL,
    enqueued_at TEXT NOT NULL,
    execution_id TEXT REFERENCES execution(id) ON DELETE SET NULL,
    execution_ref TEXT,
    workspace_id TEXT REFERENCES workspace(id) ON DELETE SET NULL,
    workspace_ref TEXT,
    placement_id TEXT REFERENCES workspace_placement(id) ON DELETE SET NULL,
    placement_ref TEXT,
    repo_location_id TEXT REFERENCES repo_location(id) ON DELETE SET NULL,
    repo_location_ref TEXT,
    owner_kind TEXT CHECK(owner_kind IN ('server','daemon')),
    daemon_id TEXT,
    runtime_id TEXT,
    placement_generation INTEGER,
    original_candidate_sha TEXT,
    candidate_sha TEXT,
    target_tip_sha TEXT,
    contract_execution_id TEXT,
    review_id TEXT,
    reviewed_paths_json TEXT CHECK(reviewed_paths_json IS NULL OR json_valid(reviewed_paths_json)),
    changed_paths_json TEXT CHECK(changed_paths_json IS NULL OR json_valid(changed_paths_json)),
    conflict_paths_json TEXT CHECK(conflict_paths_json IS NULL OR json_valid(conflict_paths_json)),
    repair_paths_json TEXT CHECK(repair_paths_json IS NULL OR json_valid(repair_paths_json)),
    guard_paths_json TEXT CHECK(guard_paths_json IS NULL OR json_valid(guard_paths_json)),
    state TEXT NOT NULL CHECK(state IN ('queued','path_wait','validating','rebasing','checking','awaiting_task_step','ready_ff','ff_inflight','reconciling','applied','ejected','needs_review','parked','quarantined','completed','cancelled','superseded')),
    resume_state TEXT CHECK(resume_state IN ('queued','path_wait','validating','rebasing','checking','awaiting_task_step','ready_ff','ff_inflight','reconciling','applied','ejected','needs_review','parked','quarantined','completed','cancelled','superseded')),
    failure_kind TEXT CHECK(failure_kind IN ('infrastructure','target_unconfigured','target_ambiguous','target_unavailable','owner_required','unsupported_path','corrupt_import','contradictory_proof','needs_fact','timeout','workspace_lost','candidate_check_failed')),
    failure_message TEXT CHECK(length(CAST(failure_message AS BLOB)) <= 4096),
    slot_generation INTEGER NOT NULL DEFAULT 0 CHECK(slot_generation >= 0),
    permit_json TEXT CHECK(permit_json IS NULL OR json_valid(permit_json)),
    operation_kind TEXT CHECK(operation_kind IN ('merge','rebase','check','fast_forward','reconcile')),
    operation_id TEXT,
    current_operation_state TEXT CHECK(current_operation_state IN ('pending','running','succeeded','failed','uncertain','acknowledged')),
    operation_receipts_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(operation_receipts_json)),
    checks_json TEXT CHECK(checks_json IS NULL OR json_valid(checks_json)),
    checks_commit_sha TEXT,
    deadline TEXT,
    effect_seq INTEGER NOT NULL DEFAULT 0 CHECK(effect_seq >= 0),
    effect_ack_json TEXT CHECK(effect_ack_json IS NULL OR json_valid(effect_ack_json)),
    acknowledged_at TEXT,
    integrated_before_sha TEXT,
    integrated_sha TEXT,
    available_at TEXT,
    last_error_kind TEXT CHECK(last_error_kind IN ('infrastructure','target_unconfigured','target_ambiguous','target_unavailable','owner_required','unsupported_path','corrupt_import','contradictory_proof','needs_fact','timeout','workspace_lost','candidate_check_failed')),
    last_error TEXT CHECK(length(CAST(last_error AS BLOB)) <= 4096),
    started_at TEXT,
    updated_at TEXT NOT NULL,
    created_at TEXT NOT NULL,
    completed_at TEXT,
    import_source_json TEXT CHECK(import_source_json IS NULL OR json_valid(import_source_json)),
    observations_json TEXT NOT NULL DEFAULT '[]' CHECK(json_valid(observations_json) AND length(CAST(observations_json AS BLOB)) <= 1048576),
    observations_dropped INTEGER NOT NULL DEFAULT 0 CHECK(observations_dropped >= 0),
    revision INTEGER NOT NULL DEFAULT 1 CHECK(revision >= 1),
    UNIQUE(queue_id,queue_seq,attempt_number),
    UNIQUE(queue_id,admission_key),
    CHECK(queue_id IS NOT NULL OR (current=0 AND import_source_json IS NOT NULL)),
    CHECK(state NOT IN ('completed','cancelled','superseded') OR current=0)
);
CREATE INDEX integration_queue_ready ON integration_queue(state,available_at);
CREATE INDEX integration_queue_expired_lease ON integration_queue(lease_until) WHERE lease_until IS NOT NULL;
CREATE INDEX integration_queue_location ON integration_queue(target_location_id);
CREATE UNIQUE INDEX integration_attempt_current_task ON integration_attempt(task_ref) WHERE current=1;
CREATE UNIQUE INDEX integration_attempt_current_seq ON integration_attempt(queue_id,queue_seq) WHERE current=1;
CREATE UNIQUE INDEX integration_attempt_orphan_admission ON integration_attempt(admission_key) WHERE queue_id IS NULL;
CREATE INDEX integration_attempt_members ON integration_attempt(queue_id,state,available_at,queue_seq) WHERE current=1;
CREATE INDEX integration_attempt_history ON integration_attempt(task_ref,created_at DESC);
CREATE INDEX integration_attempt_reconnect ON integration_attempt(daemon_id,current_operation_state);
CREATE UNIQUE INDEX integration_attempt_operation ON integration_attempt(operation_id) WHERE operation_id IS NOT NULL;
CREATE INDEX integration_attempt_retention ON integration_attempt(state,completed_at);
CREATE INDEX integration_attempt_import ON integration_attempt(task_ref,expected_epoch) WHERE import_source_json IS NOT NULL;
CREATE INDEX integration_attempt_import_disposition ON integration_attempt(json_extract(import_source_json,'$.disposition')) WHERE import_source_json IS NOT NULL;
