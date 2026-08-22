-- Durable Project execution-setup state.
--
-- Project creation and repository/role provisioning cross a SQLite
-- transaction and the local filesystem.  These records make that seam
-- recoverable without treating a partially observed setup as executable.
-- Existing Projects are backfilled from authoritative rows only; this
-- migration never creates an identity, repository, role assignment, or
-- successful setup claim.

CREATE TABLE project_provisioning_operation (
    id                    TEXT PRIMARY KEY CHECK (length(trim(id)) > 0),
    project_id            TEXT NOT NULL UNIQUE REFERENCES project(id) ON DELETE CASCADE,
    idempotency_key       TEXT NOT NULL UNIQUE CHECK (length(trim(idempotency_key)) > 0),
    status                TEXT NOT NULL CHECK (status IN ('provisioning', 'setup_required', 'ready', 'failed')),
    current_checkpoint    TEXT NOT NULL CHECK (current_checkpoint IN (
        'preflight', 'repository_initialized', 'repository_registered',
        'repository_linked', 'roles_assigned', 'completed'
    )),
    attempt_count         INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    max_attempts          INTEGER NOT NULL DEFAULT 3 CHECK (max_attempts > 0),
    lease_owner           TEXT,
    lease_expires_at      TEXT,
    next_retry_at         TEXT,
    retryable             INTEGER NOT NULL DEFAULT 0 CHECK (retryable IN (0, 1)),
    last_error_code       TEXT,
    last_error_message    TEXT,
    created_at            TEXT NOT NULL CHECK (length(trim(created_at)) > 0),
    updated_at            TEXT NOT NULL CHECK (length(trim(updated_at)) > 0),
    completed_at          TEXT,
    version               INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
    CHECK (attempt_count <= max_attempts),
    CHECK (retryable = 1 OR next_retry_at IS NULL),
    CHECK (lease_owner IS NOT NULL OR lease_expires_at IS NULL),
    CHECK (status != 'provisioning' OR (lease_owner IS NOT NULL AND lease_expires_at IS NOT NULL)),
    CHECK (status = 'provisioning' OR lease_owner IS NULL),
    CHECK (status = 'provisioning' OR lease_expires_at IS NULL),
    CHECK (completed_at IS NULL OR status = 'ready'),
    CHECK (
        status != 'ready'
        OR (
            current_checkpoint = 'completed'
            AND completed_at IS NOT NULL
            AND lease_owner IS NULL
            AND lease_expires_at IS NULL
            AND next_retry_at IS NULL
            AND retryable = 0
        )
    )
);

CREATE INDEX idx_project_provisioning_operation_status
    ON project_provisioning_operation(status, next_retry_at, updated_at);
CREATE INDEX idx_project_provisioning_operation_lease
    ON project_provisioning_operation(lease_expires_at)
    WHERE lease_owner IS NOT NULL;

CREATE TABLE project_provisioning_checkpoint (
    id                    TEXT PRIMARY KEY CHECK (length(trim(id)) > 0),
    operation_id          TEXT NOT NULL REFERENCES project_provisioning_operation(id) ON DELETE CASCADE,
    checkpoint            TEXT NOT NULL CHECK (checkpoint IN (
        'preflight', 'repository_initialized', 'repository_registered',
        'repository_linked', 'roles_assigned'
    )),
    status                TEXT NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed', 'skipped')),
    attempt_count         INTEGER NOT NULL DEFAULT 0 CHECK (attempt_count >= 0),
    error_code            TEXT,
    error_message         TEXT,
    details_json          TEXT NOT NULL DEFAULT '{}' CHECK (json_valid(details_json)),
    started_at            TEXT,
    completed_at          TEXT,
    created_at            TEXT NOT NULL CHECK (length(trim(created_at)) > 0),
    updated_at            TEXT NOT NULL CHECK (length(trim(updated_at)) > 0),
    version               INTEGER NOT NULL DEFAULT 1 CHECK (version >= 1),
    CHECK (
        (status IN ('completed', 'skipped') AND completed_at IS NOT NULL)
        OR (status IN ('pending', 'running', 'failed') AND completed_at IS NULL)
    ),
    UNIQUE (operation_id, checkpoint)
);

CREATE INDEX idx_project_provisioning_checkpoint_operation
    ON project_provisioning_checkpoint(operation_id, checkpoint);

CREATE TABLE project_provisioning_error (
    id                    TEXT PRIMARY KEY CHECK (length(trim(id)) > 0),
    operation_id          TEXT NOT NULL REFERENCES project_provisioning_operation(id) ON DELETE CASCADE,
    checkpoint_id         TEXT REFERENCES project_provisioning_checkpoint(id) ON DELETE SET NULL,
    code                  TEXT NOT NULL CHECK (length(trim(code)) > 0),
    message               TEXT NOT NULL CHECK (length(trim(message)) > 0),
    retryable             INTEGER NOT NULL CHECK (retryable IN (0, 1)),
    attempt_count         INTEGER NOT NULL CHECK (attempt_count >= 0),
    created_at            TEXT NOT NULL CHECK (length(trim(created_at)) > 0)
);

CREATE INDEX idx_project_provisioning_error_operation
    ON project_provisioning_error(operation_id, created_at DESC, id DESC);

-- A setup-ready backfill is intentionally conservative. It derives the
-- required role names from the persisted workflow (and an active baseline's
-- reviewer-independence policy), then accepts only assignments that point at
-- an effectively eligible, non-archived identity. The SQL cannot verify the
-- filesystem, so it never claims that repository initialization completed;
-- persisted health, daemon, and capacity rows are used for eligibility.
-- Main/Project Agent identities, profileless identities, and identities in
-- the runtime error state are excluded. Busy/offline status is retained
-- because compute_effective_status can still classify such identities as
-- active when health, daemon, and capacity checks permit.
WITH workflow_rows AS (
    SELECT
        p.id AS project_id,
        CASE
            WHEN trim(coalesce(p.workflow_definition, '')) IN ('', '{}') THEN 1
            WHEN json_valid(p.workflow_definition)
             AND json_type(p.workflow_definition, '$.roles') = 'array'
             AND json_type(p.workflow_definition, '$.states') = 'array'
             AND NOT EXISTS (
                 SELECT 1
                 FROM json_each(p.workflow_definition, '$.states') AS candidate_state
                 WHERE json_type(candidate_state.value, '$.name') != 'text'
                    OR json_type(candidate_state.value, '$.kind') != 'text'
                    OR json_extract(candidate_state.value, '$.kind') NOT IN
                        ('backlog', 'initial', 'active', 'gate', 'terminal', 'custom')
                    OR json_type(candidate_state.value, '$.column') != 'text'
                    OR json_type(candidate_state.value, '$.display_name') != 'text'
                    OR (
                        json_type(candidate_state.value, '$.role') IS NOT NULL
                        AND json_type(candidate_state.value, '$.role') NOT IN ('text', 'null')
                    )
                    OR json_type(candidate_state.value, '$.hooks') != 'object'
                    OR json_type(candidate_state.value, '$.gate_config') IS NULL
                    OR json_type(candidate_state.value, '$.gate_config') NOT IN ('null', 'object')
                    OR json_type(candidate_state.value, '$.config') IS NULL
             )
             AND NOT EXISTS (
                 SELECT 1
                 FROM json_each(p.workflow_definition, '$.roles') AS candidate_role
                 WHERE json_type(candidate_role.value, '$.name') != 'text'
                    OR json_type(candidate_role.value, '$.display_name') != 'text'
                    OR json_type(candidate_role.value, '$.description') != 'text'
             ) THEN 1
            ELSE 0
        END AS workflow_verified,
        CAST(state.key AS INTEGER) AS state_index,
        CASE
            WHEN json_extract(state.value, '$.role') IS NOT NULL
                THEN json_extract(state.value, '$.role')
            WHEN lower(json_extract(state.value, '$.kind')) = 'active'
                THEN 'assignee'
            ELSE NULL
        END AS role_name,
        json_extract(state.value, '$.canonical_phase') AS canonical_phase_raw,
        json_type(state.value, '$.canonical_phase') AS canonical_phase_type,
        CASE
            WHEN json_extract(state.value, '$.canonical_phase') IN
                ('backlog', 'ready', 'working', 'review', 'done')
                THEN json_extract(state.value, '$.canonical_phase')
            WHEN lower(trim(json_extract(state.value, '$.column'))) = 'backlog'
                THEN 'backlog'
            WHEN lower(trim(json_extract(state.value, '$.column'))) IN ('todo', 'ready')
                THEN 'ready'
            WHEN lower(trim(json_extract(state.value, '$.column'))) IN ('in progress', 'working')
                THEN 'working'
            WHEN lower(trim(json_extract(state.value, '$.column'))) = 'review'
                THEN 'review'
            WHEN lower(trim(json_extract(state.value, '$.column'))) = 'done'
                THEN 'done'
            WHEN lower(trim(json_extract(state.value, '$.name'))) = 'backlog'
                THEN 'backlog'
            WHEN lower(trim(json_extract(state.value, '$.name'))) = 'todo'
                THEN 'ready'
            WHEN lower(trim(json_extract(state.value, '$.name'))) IN ('planning', 'in_progress')
                THEN 'working'
            WHEN lower(trim(json_extract(state.value, '$.name'))) IN
                ('review', 'merging', 'merge_failed')
                THEN 'review'
            WHEN lower(trim(json_extract(state.value, '$.name'))) IN ('done', 'cancelled')
                THEN 'done'
            WHEN lower(json_extract(state.value, '$.kind')) = 'backlog'
                THEN 'backlog'
            WHEN lower(json_extract(state.value, '$.kind')) = 'initial'
                THEN 'ready'
            WHEN lower(json_extract(state.value, '$.kind')) = 'active'
                THEN 'working'
            WHEN lower(json_extract(state.value, '$.kind')) = 'gate'
             AND (
                 lower(json_extract(state.value, '$.name')) LIKE '%review%'
                 OR lower(json_extract(state.value, '$.name')) LIKE '%merge%'
             )
                THEN 'review'
            WHEN lower(json_extract(state.value, '$.kind')) IN ('active', 'custom')
                THEN 'working'
            WHEN lower(json_extract(state.value, '$.kind')) = 'terminal'
                THEN 'done'
            -- WorkflowDefinition::canonical_phase_for_state defaults an
            -- otherwise unknown non-terminal state to working (with a
            -- warning). Keep the backfill's role selection on that same
            -- conservative runtime path.
            ELSE 'working'
        END AS canonical_phase
    FROM project AS p
    LEFT JOIN json_each(
        CASE
            WHEN json_valid(p.workflow_definition)
             AND json_type(p.workflow_definition, '$.states') = 'array'
            THEN p.workflow_definition
            ELSE json_object('states', json_array())
        END,
        '$.states'
    ) AS state ON 1 = 1
), workflow_requirements AS (
    SELECT
        p.id AS project_id,
        CASE
            WHEN MAX(wr.workflow_verified) = 1
             AND SUM(
                 CASE
                     WHEN wr.canonical_phase_raw IS NOT NULL
                      AND (
                          wr.canonical_phase_type != 'text'
                          OR wr.canonical_phase_raw NOT IN
                              ('backlog', 'ready', 'working', 'review', 'done')
                      )
                     THEN 1 ELSE 0
                 END
             ) = 0
            THEN 1 ELSE 0
        END AS workflow_verified,
        CASE
            WHEN MAX(wr.workflow_verified) = 1
             AND trim(coalesce(p.workflow_definition, '')) IN ('', '{}')
            THEN 'coder'
            ELSE (
                SELECT role_name
                FROM workflow_rows AS worker_row
                WHERE worker_row.project_id = p.id
                  AND worker_row.workflow_verified = 1
                  AND worker_row.canonical_phase = 'working'
                  AND worker_row.role_name IS NOT NULL
                  AND worker_row.role_name NOT IN ('planner', 'reviewer')
                ORDER BY worker_row.state_index
                LIMIT 1
            )
        END AS worker_role,
        CASE
            WHEN MAX(wr.workflow_verified) = 1
             AND trim(coalesce(p.workflow_definition, '')) IN ('', '{}')
            THEN 'reviewer'
            ELSE (
                SELECT role_name
                FROM workflow_rows AS reviewer_row
                WHERE reviewer_row.project_id = p.id
                  AND reviewer_row.workflow_verified = 1
                  AND reviewer_row.canonical_phase = 'review'
                  AND reviewer_row.role_name IS NOT NULL
                ORDER BY reviewer_row.state_index
                LIMIT 1
            )
        END AS workflow_reviewer_role
    FROM project AS p
    JOIN workflow_rows AS wr ON wr.project_id = p.id
    GROUP BY p.id
), active_baseline_policy AS (
    SELECT
        p.id AS project_id,
        CASE
            WHEN r.id IS NULL THEN 0
            WHEN NOT json_valid(r.release_policy_json) THEN 1
            WHEN json_type(r.release_policy_json, '$.reviewer_independence_rules') = 'array'
             AND json_array_length(r.release_policy_json, '$.reviewer_independence_rules') > 0
            THEN 1
            ELSE 0
        END AS independent_reviewer_required
    FROM project AS p
    LEFT JOIN project_execution_baseline AS b
      ON b.project_id = p.id AND b.lifecycle = 'active'
    LEFT JOIN project_execution_baseline_revision AS r
      ON r.id = b.current_revision_id
     AND r.baseline_id = b.id
     AND r.lifecycle = 'approved'
), required_roles AS (
    SELECT
        wr.project_id,
        wr.workflow_verified,
        wr.worker_role,
        CASE
            WHEN wr.workflow_reviewer_role IS NOT NULL THEN wr.workflow_reviewer_role
            WHEN COALESCE(abp.independent_reviewer_required, 0) = 1 THEN 'reviewer'
            ELSE NULL
        END AS reviewer_role
    FROM workflow_requirements AS wr
    LEFT JOIN active_baseline_policy AS abp ON abp.project_id = wr.project_id
), role_rows AS (
    SELECT
        p.id AS project_id,
        CAST(assignment.key AS INTEGER) AS assignment_index,
        json_extract(value, '$.role_name') AS role_name,
        json_extract(value, '$.assignee_type') AS assignee_type,
        json_extract(value, '$.assignee_id') AS assignee_id
    FROM project AS p
    LEFT JOIN json_each(
        CASE
            WHEN json_valid(p.settings)
             AND json_type(p.settings, '$.default_role_assignments') = 'array'
            THEN p.settings
            ELSE json_object('default_role_assignments', json_array())
        END,
        '$.default_role_assignments'
    ) AS assignment ON 1 = 1
), effective_role_rows AS (
    SELECT assignment.*
    FROM role_rows AS assignment
    WHERE assignment.assignee_type = 'agent'
      AND assignment.role_name IS NOT NULL
      AND length(trim(assignment.role_name)) > 0
      AND assignment.assignee_id IS NOT NULL
      AND length(trim(assignment.assignee_id)) > 0
      AND NOT EXISTS (
          SELECT 1
          FROM role_rows AS newer
          WHERE newer.project_id = assignment.project_id
            AND newer.role_name = assignment.role_name
            AND newer.assignee_type = 'agent'
            AND newer.assignee_id IS NOT NULL
            AND length(trim(newer.role_name)) > 0
            AND length(trim(newer.assignee_id)) > 0
            AND newer.assignment_index > assignment.assignment_index
      )
), daemon_capacities AS (
    SELECT
        daemon.id,
        CASE
            WHEN json_valid(daemon.labels_json)
             AND json_type(daemon.labels_json, '$.max_concurrent_sessions') = 'integer'
             AND json_extract(daemon.labels_json, '$.max_concurrent_sessions') > 0
                THEN CAST(json_extract(daemon.labels_json, '$.max_concurrent_sessions') AS INTEGER)
            WHEN json_valid(daemon.labels_json)
             AND json_type(daemon.labels_json, '$.max_sessions') = 'integer'
             AND json_extract(daemon.labels_json, '$.max_sessions') > 0
                THEN CAST(json_extract(daemon.labels_json, '$.max_sessions') AS INTEGER)
            WHEN json_valid(daemon.labels_json)
             AND json_type(daemon.labels_json, '$.active_session_cap') = 'integer'
             AND json_extract(daemon.labels_json, '$.active_session_cap') > 0
                THEN CAST(json_extract(daemon.labels_json, '$.active_session_cap') AS INTEGER)
            WHEN json_valid(daemon.labels_json)
             AND json_type(daemon.labels_json, '$.max_concurrent_tasks') = 'integer'
             AND json_extract(daemon.labels_json, '$.max_concurrent_tasks') > 0
                THEN CAST(json_extract(daemon.labels_json, '$.max_concurrent_tasks') AS INTEGER)
            ELSE NULL
        END AS session_cap
    FROM daemon
), eligible_identities AS (
    SELECT identity.id, identity.visibility, identity.owner_id
    FROM agent_identity AS identity
    JOIN agent_profile AS profile
      ON profile.id = identity.selected_profile_id
     AND profile.identity_id = identity.id
    WHERE identity.archived_at IS NULL
      AND identity.paused = 0
      AND identity.status != 'error'
      AND length(trim(profile.executor_type)) > 0
      AND (
          (
              profile.backend_kind = 'native'
              AND EXISTS (
                  SELECT 1 FROM agent_connection_health AS health
                  WHERE health.profile_id = profile.id
                    AND health.status = 'healthy'
              )
          )
          OR (
              profile.backend_kind = 'cli'
              AND EXISTS (
                    SELECT 1 FROM daemon
                  WHERE (profile.daemon_id IS NULL OR daemon.id = profile.daemon_id)
                    AND daemon.status = 'online'
                    AND json_valid(daemon.detected_clis_json)
                    AND json_type(daemon.detected_clis_json) = 'array'
                    AND EXISTS (
                        SELECT 1 FROM json_each(daemon.detected_clis_json)
                        WHERE json_extract(value, '$.kind') = profile.executor_type
                          AND json_extract(value, '$.availability') = 'authenticated'
                    )
              )
          )
      )
      AND (
          (
              SELECT COUNT(*) FROM execution
              WHERE execution.agent_id = identity.id
                AND execution.status = 'running'
          ) + (
              SELECT COUNT(*) FROM agent_chat_turn_job
              WHERE agent_chat_turn_job.responder_identity_id = identity.id
                AND agent_chat_turn_job.status IN ('leased', 'running')
          )
      ) < identity.max_concurrent_tasks
      AND (
          profile.daemon_id IS NULL
          OR NOT EXISTS (
              SELECT 1
              FROM daemon_capacities AS capacity
              WHERE capacity.id = profile.daemon_id
                AND capacity.session_cap IS NOT NULL
                AND (
                    (
                        SELECT COUNT(*)
                        FROM execution
                        JOIN agent_current AS running_agent
                          ON running_agent.id = execution.agent_id
                        WHERE running_agent.daemon_id = profile.daemon_id
                          AND execution.status = 'running'
                    ) + (
                        SELECT COUNT(*)
                        FROM agent_chat_turn_job
                        JOIN agent_current AS running_agent
                          ON running_agent.id = agent_chat_turn_job.responder_identity_id
                        WHERE running_agent.daemon_id = profile.daemon_id
                          AND agent_chat_turn_job.status IN ('leased', 'running')
                    )
                ) >= capacity.session_cap
          )
      )
      AND NOT EXISTS (
          SELECT 1 FROM account_main_agent_binding AS main_binding
          WHERE main_binding.identity_id = identity.id
            AND main_binding.state = 'active'
      )
      AND NOT EXISTS (
          SELECT 1 FROM project_agent_binding AS project_binding
          WHERE project_binding.identity_id = identity.id
            AND project_binding.state = 'active'
      )
), valid_roles AS (
    SELECT
        rr.id AS project_id,
        required.worker_role,
        required.reviewer_role,
        required.workflow_verified,
        (
            SELECT assignment.assignee_id
            FROM effective_role_rows AS assignment
            JOIN eligible_identities AS eligible
              ON eligible.id = assignment.assignee_id
            JOIN project AS candidate_project ON candidate_project.id = rr.id
            WHERE assignment.project_id = rr.id
              AND assignment.role_name = required.worker_role
              AND assignment.assignee_type = 'agent'
              AND (
                  eligible.visibility = 'global'
                  OR (eligible.visibility = 'account' AND eligible.owner_id = candidate_project.owner_id)
              )
            ORDER BY assignment.assignee_id
            LIMIT 1
        ) AS worker_id,
        (
            SELECT assignment.assignee_id
            FROM effective_role_rows AS assignment
            JOIN eligible_identities AS eligible
              ON eligible.id = assignment.assignee_id
            JOIN project AS candidate_project ON candidate_project.id = rr.id
            WHERE assignment.project_id = rr.id
              AND assignment.role_name = required.reviewer_role
              AND assignment.assignee_type = 'agent'
              AND (
                  eligible.visibility = 'global'
                  OR (eligible.visibility = 'account' AND eligible.owner_id = candidate_project.owner_id)
              )
              AND assignment.assignee_id != COALESCE((
                  SELECT worker_assignment.assignee_id
                  FROM effective_role_rows AS worker_assignment
                  JOIN eligible_identities AS worker_eligible
                    ON worker_eligible.id = worker_assignment.assignee_id
                  JOIN project AS worker_project ON worker_project.id = rr.id
                  WHERE worker_assignment.project_id = rr.id
                    AND worker_assignment.role_name = required.worker_role
                    AND worker_assignment.assignee_type = 'agent'
                    AND (
                        worker_eligible.visibility = 'global'
                        OR (
                            worker_eligible.visibility = 'account'
                            AND worker_eligible.owner_id = worker_project.owner_id
                        )
                    )
                  LIMIT 1
              ), '')
            ORDER BY assignment.assignee_id
            LIMIT 1
        ) AS reviewer_id
    FROM required_roles AS required
    JOIN project AS rr ON rr.id = required.project_id
), readiness AS (
    SELECT
        p.id AS project_id,
        vr.workflow_verified,
        vr.worker_role,
        vr.reviewer_role,
        vr.worker_id,
        vr.reviewer_id,
        CASE
            WHEN p.primary_repo_id IS NOT NULL
             AND EXISTS (
                 SELECT 1 FROM repo AS r
                 WHERE r.id = p.primary_repo_id
                   AND r.project_id = p.id
             ) THEN 1 ELSE 0
        END AS repository_linked,
        CASE
            WHEN p.primary_repo_id IS NOT NULL
             AND EXISTS (
                 SELECT 1 FROM repo AS r
                 WHERE r.id = p.primary_repo_id
                   AND r.project_id = p.id
                   AND r.local_path IS NULL
             ) THEN 1 ELSE 0
        END AS repository_setup_verified,
        CASE
            WHEN vr.workflow_verified = 1
             AND vr.worker_role IS NOT NULL
             AND vr.worker_id IS NOT NULL
             AND (
                 vr.reviewer_role IS NULL
                 OR (
                     vr.reviewer_id IS NOT NULL
                     AND vr.reviewer_id != vr.worker_id
                 )
             )
             AND p.primary_repo_id IS NOT NULL
             AND EXISTS (
                 SELECT 1 FROM repo AS r
                 WHERE r.id = p.primary_repo_id
                   AND r.project_id = p.id
                   AND r.local_path IS NULL
             )
            THEN 1 ELSE 0
        END AS setup_ready,
        CASE
            WHEN p.primary_repo_id IS NOT NULL
             AND EXISTS (
                 SELECT 1 FROM repo AS r
                 WHERE r.id = p.primary_repo_id AND r.project_id = p.id
             )
            THEN 'repository_linked'
            ELSE 'preflight'
        END AS current_checkpoint
    FROM project AS p
    JOIN valid_roles AS vr ON vr.project_id = p.id
)
INSERT INTO project_provisioning_operation (
    id, project_id, idempotency_key, status, current_checkpoint,
    attempt_count, max_attempts, retryable, last_error_code, last_error_message,
    created_at, updated_at, completed_at, version
)
SELECT
    lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
        lower(substr(hex(randomblob(2)), 2, 3)) || '-' ||
        substr('89ab', 1 + (abs(random()) % 4), 1) ||
        lower(substr(hex(randomblob(2)), 2, 3)) || '-' || lower(hex(randomblob(6))),
    p.id,
    'project-provisioning:' || p.id,
    CASE WHEN r.setup_ready = 1 THEN 'ready' ELSE 'setup_required' END,
    CASE WHEN r.setup_ready = 1 THEN 'completed' ELSE r.current_checkpoint END,
    0,
    3,
    CASE WHEN r.setup_ready = 1 THEN 0 ELSE 1 END,
    CASE
        WHEN r.repository_linked = 0 THEN 'repository_required'
        WHEN r.workflow_verified = 0 THEN 'workflow_unavailable'
        WHEN r.worker_role IS NULL OR r.worker_id IS NULL THEN 'worker_required'
        WHEN r.reviewer_role IS NOT NULL AND r.reviewer_id IS NULL
            THEN 'independent_reviewer_required'
        WHEN r.repository_setup_verified = 0 THEN 'repository_initialization_unverified'
        ELSE NULL
    END,
    CASE
        WHEN r.repository_linked = 0
            THEN 'Project requires a verified primary repository binding'
        WHEN r.workflow_verified = 0
            THEN 'Project workflow is missing or cannot be verified for execution setup'
        WHEN r.worker_role IS NULL OR r.worker_id IS NULL
            THEN 'Project requires an eligible Worker assignment'
        WHEN r.reviewer_role IS NOT NULL AND r.reviewer_id IS NULL
            THEN 'Project requires a distinct eligible independent reviewer assignment'
        WHEN r.repository_setup_verified = 0
            THEN 'Project repository binding exists but filesystem initialization is not verified'
        ELSE NULL
    END,
    p.created_at,
    p.updated_at,
    CASE WHEN r.setup_ready = 1 THEN p.updated_at ELSE NULL END,
    1
FROM project AS p
JOIN readiness AS r ON r.project_id = p.id;

WITH operation_state AS (
    SELECT
        o.id AS operation_id,
        o.project_id,
        o.status,
        o.current_checkpoint,
        CASE
            WHEN o.status = 'ready' THEN 1 ELSE 0
        END AS setup_ready
    FROM project_provisioning_operation AS o
), checkpoint_rows(checkpoint) AS (
    VALUES ('preflight'), ('repository_initialized'),
           ('repository_registered'), ('repository_linked'), ('roles_assigned')
)
INSERT INTO project_provisioning_checkpoint (
    id, operation_id, checkpoint, status, attempt_count,
    details_json, completed_at, created_at, updated_at, version
)
SELECT
    lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
        lower(substr(hex(randomblob(2)), 2, 3)) || '-' ||
        substr('89ab', 1 + (abs(random()) % 4), 1) ||
        lower(substr(hex(randomblob(2)), 2, 3)) || '-' || lower(hex(randomblob(6))),
    os.operation_id,
    cr.checkpoint,
    CASE
        WHEN cr.checkpoint = 'preflight' THEN 'completed'
        WHEN cr.checkpoint = 'repository_initialized' THEN 'skipped'
        WHEN cr.checkpoint IN ('repository_registered', 'repository_linked')
             AND EXISTS (
                 SELECT 1 FROM project AS p
                 JOIN repo AS r ON r.id = p.primary_repo_id AND r.project_id = p.id
                 WHERE p.id = os.project_id
             ) THEN 'completed'
        WHEN cr.checkpoint = 'roles_assigned' AND os.setup_ready = 1 THEN 'completed'
        ELSE 'pending'
    END,
    0,
    CASE
        WHEN cr.checkpoint = 'repository_initialized'
            THEN json_object(
                'source', 'V087_backfill',
                'authoritative', json('true'),
                'filesystem_verified', json('false'),
                'note', 'The migration does not inspect or claim local filesystem state'
            )
        ELSE json_object('source', 'V087_backfill', 'authoritative', 1)
    END,
    CASE
        WHEN cr.checkpoint IN ('preflight', 'repository_initialized') THEN p.updated_at
        WHEN cr.checkpoint IN ('repository_registered', 'repository_linked')
             AND EXISTS (
                 SELECT 1 FROM project AS linked_project
                 JOIN repo AS linked_repo
                   ON linked_repo.id = linked_project.primary_repo_id
                  AND linked_repo.project_id = linked_project.id
                 WHERE linked_project.id = os.project_id
             ) THEN p.updated_at
        WHEN cr.checkpoint = 'roles_assigned' AND os.setup_ready = 1 THEN p.updated_at
        ELSE NULL
    END,
    p.created_at,
    p.updated_at,
    1
FROM operation_state AS os
JOIN project AS p ON p.id = os.project_id
CROSS JOIN checkpoint_rows AS cr;

INSERT INTO project_provisioning_error (
    id, operation_id, checkpoint_id, code, message, retryable, attempt_count, created_at
)
SELECT
    lower(hex(randomblob(4))) || '-' || lower(hex(randomblob(2))) || '-4' ||
        lower(substr(hex(randomblob(2)), 2, 3)) || '-' ||
        substr('89ab', 1 + (abs(random()) % 4), 1) ||
        lower(substr(hex(randomblob(2)), 2, 3)) || '-' || lower(hex(randomblob(6))),
    o.id,
    (
        SELECT c.id
        FROM project_provisioning_checkpoint AS c
        WHERE c.operation_id = o.id
          AND c.checkpoint = CASE o.last_error_code
              WHEN 'repository_required' THEN 'preflight'
              WHEN 'workflow_unavailable' THEN 'preflight'
              WHEN 'worker_required' THEN 'roles_assigned'
              WHEN 'independent_reviewer_required' THEN 'roles_assigned'
              WHEN 'repository_initialization_unverified' THEN 'repository_initialized'
              ELSE o.current_checkpoint
          END
    ),
    o.last_error_code,
    o.last_error_message,
    o.retryable,
    o.attempt_count,
    o.updated_at
FROM project_provisioning_operation AS o
WHERE o.status = 'setup_required'
  AND o.last_error_code IS NOT NULL;
