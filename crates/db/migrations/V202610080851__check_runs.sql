-- Stage A: passive evidence only. No old review/check row is promoted to cache.
CREATE TABLE check_run (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES project(id) ON DELETE CASCADE,
    repo_id TEXT NOT NULL REFERENCES repo(id) ON DELETE CASCADE,
    commit_sha TEXT NOT NULL,
    spec_digest TEXT NOT NULL,
    identity_key TEXT NOT NULL,
    input_json TEXT NOT NULL CHECK(json_valid(input_json) AND length(CAST(input_json AS BLOB)) <= 131072),
    cacheable INTEGER NOT NULL CHECK(cacheable IN (0,1)),
    state TEXT NOT NULL CHECK(state IN ('queued','running','cancelling','cleaning','uncertain','succeeded','failed','cancelled')),
    operation_id TEXT NOT NULL UNIQUE,
    workspace_id TEXT REFERENCES workspace(id) ON DELETE SET NULL,
    machine_id TEXT REFERENCES daemon(id) ON DELETE SET NULL,
    lease_owner TEXT,
    lease_generation INTEGER NOT NULL DEFAULT 0 CHECK(lease_generation >= 0),
    lease_until TEXT,
    version INTEGER NOT NULL DEFAULT 1 CHECK(version >= 1),
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    finished_at TEXT,
    UNIQUE(id, identity_key),
    CHECK((lease_owner IS NULL) = (lease_until IS NULL))
);
CREATE UNIQUE INDEX check_run_live_identity ON check_run(identity_key)
    WHERE state IN ('queued','running','cancelling','cleaning','uncertain');
CREATE INDEX check_run_state_lease ON check_run(state, lease_until);
CREATE INDEX check_run_machine_state ON check_run(machine_id, state);
CREATE TABLE check_result (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    identity_key TEXT NOT NULL,
    outcome TEXT NOT NULL CHECK(outcome IN ('pass','fail','timed_out','cancelled','infrastructure_failed')),
    cleanup TEXT NOT NULL CHECK(cleanup IN ('success','failed','uncertain','not_performed')),
    certified INTEGER NOT NULL CHECK(certified IN (0,1)),
    cacheable INTEGER NOT NULL CHECK(cacheable IN (0,1)),
    steps_json TEXT NOT NULL CHECK(json_valid(steps_json) AND length(CAST(steps_json AS BLOB)) <= 262144),
    output_truncated INTEGER NOT NULL CHECK(output_truncated IN (0,1)),
    created_at TEXT NOT NULL,
    FOREIGN KEY(run_id, identity_key) REFERENCES check_run(id, identity_key) ON DELETE CASCADE,
    CHECK(certified = 0 OR (outcome = 'pass' AND cleanup = 'success'))
);
CREATE INDEX check_result_run ON check_result(run_id, created_at);
CREATE UNIQUE INDEX check_result_reusable_identity ON check_result(identity_key)
    WHERE outcome = 'pass' AND cleanup = 'success' AND certified = 1 AND cacheable = 1;
CREATE TABLE check_consumer (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES project(id) ON DELETE CASCADE,
    repo_id TEXT NOT NULL REFERENCES repo(id) ON DELETE CASCADE,
    task_id TEXT REFERENCES task(id) ON DELETE SET NULL,
    status_epoch INTEGER NOT NULL CHECK(status_epoch >= 0),
    origin TEXT NOT NULL CHECK(origin IN ('entry','manual_review','conformance','before_work','lifecycle','environment','integration')),
    request_key TEXT NOT NULL UNIQUE,
    identity_key TEXT NOT NULL,
    run_id TEXT REFERENCES check_run(id) ON DELETE SET NULL,
    result_id TEXT REFERENCES check_result(id) ON DELETE SET NULL,
    created_at TEXT NOT NULL
);
CREATE INDEX check_consumer_task_epoch ON check_consumer(task_id, status_epoch);
CREATE INDEX check_consumer_run ON check_consumer(run_id);
CREATE INDEX check_consumer_result ON check_consumer(result_id);
