-- A Project Agent Chat may now hold its own workspace, and the scope table
-- could not store that.
--
-- `agent_context_scope.workspace_access` admitted only 'deny', 'task_read',
-- and 'task_write', and the compound CHECK pinned every 'agent_chat' row to
-- 'deny'. Persisting a Project Agent session therefore failed with
--   CHECK constraint failed: workspace_access IN ('deny','task_read','task_write')
-- and the session fell back to a denied scope, which the turn then contradicted
-- with "native turn scope does not match the server-issued session binding".
--
-- 'project_verify' is admitted for 'agent_chat' rows only. A Task scope keeps
-- exactly its two access levels, and account/project/room scopes stay denied,
-- so the new level widens nothing except the surface it was built for.

PRAGMA foreign_keys = OFF;
BEGIN IMMEDIATE;

-- SQLite refuses to rebuild a table while another table's trigger still
-- references it, so the session and manifest guards come down and go back up
-- unchanged around the swap.
DROP TRIGGER agent_chat_context_scope_guard_insert;
DROP TRIGGER agent_chat_context_scope_guard_update;
DROP TRIGGER agent_context_scope_reject_legacy_room_insert;
DROP TRIGGER agent_context_scope_reject_legacy_room_update;
DROP TRIGGER agent_context_scope_identity_profile_guard;
DROP TRIGGER agent_context_scope_identity_profile_guard_update;
DROP TRIGGER context_manifest_immutable_delete;

CREATE TABLE agent_context_scope_new (
    id                  TEXT PRIMARY KEY,
    identity_id         TEXT NOT NULL REFERENCES agent_identity(id) ON DELETE CASCADE,
    scope_type          TEXT NOT NULL
                            CHECK (scope_type IN (
                                'account', 'project', 'room', 'task', 'agent_chat'
                            )),
    scope_id            TEXT NOT NULL,
    project_id          TEXT REFERENCES project(id) ON DELETE CASCADE,
    room_id             TEXT REFERENCES legacy_room(id) ON DELETE CASCADE,
    task_id             TEXT REFERENCES task(id) ON DELETE CASCADE,
    task_role           TEXT,
    workspace_access    TEXT NOT NULL DEFAULT 'deny'
                            CHECK (workspace_access IN (
                                'deny', 'task_read', 'task_write', 'project_verify'
                            )),
    authority_json      TEXT NOT NULL DEFAULT '{}',
    version             INTEGER NOT NULL DEFAULT 1,
    created_at          TEXT NOT NULL,
    updated_at          TEXT NOT NULL,
    CHECK (
        (scope_type = 'account' AND project_id IS NULL AND room_id IS NULL
            AND task_id IS NULL AND task_role IS NULL AND workspace_access = 'deny')
        OR (scope_type = 'project' AND project_id = scope_id AND room_id IS NULL
            AND task_id IS NULL AND task_role IS NULL AND workspace_access = 'deny')
        OR (scope_type = 'room' AND room_id = scope_id AND task_id IS NULL
            AND task_role IS NULL AND workspace_access = 'deny')
        OR (scope_type = 'task' AND task_id = scope_id AND project_id IS NOT NULL
            AND task_role IS NOT NULL AND workspace_access IN ('task_read', 'task_write'))
        -- A Project Agent Chat carries `project_verify`; a Main Chat has no
        -- Project and stays denied.
        OR (scope_type = 'agent_chat' AND room_id IS NULL AND task_id IS NULL
            AND task_role IS NULL
            AND (
                workspace_access = 'deny'
                OR (workspace_access = 'project_verify' AND project_id IS NOT NULL)
            ))
    )
);

INSERT INTO agent_context_scope_new (
    id, identity_id, scope_type, scope_id, project_id, room_id, task_id,
    task_role, workspace_access, authority_json, version, created_at, updated_at
)
SELECT
    id, identity_id, scope_type, scope_id, project_id, room_id, task_id,
    task_role, workspace_access, authority_json, version, created_at, updated_at
FROM agent_context_scope;

DROP TABLE agent_context_scope;
ALTER TABLE agent_context_scope_new RENAME TO agent_context_scope;

CREATE UNIQUE INDEX ux_agent_context_scope_non_task
    ON agent_context_scope(identity_id, scope_type, scope_id)
    WHERE scope_type <> 'task';
CREATE UNIQUE INDEX ux_agent_context_scope_task_role
    ON agent_context_scope(identity_id, scope_type, scope_id, task_role)
    WHERE scope_type = 'task';
CREATE INDEX idx_agent_context_scope_scope
    ON agent_context_scope(scope_type, scope_id, identity_id);

CREATE TRIGGER agent_chat_context_scope_guard_insert
BEFORE INSERT ON agent_context_scope
WHEN NEW.scope_type = 'agent_chat'
BEGIN
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1
            FROM agent_chat AS chat
            WHERE chat.id = NEW.scope_id
              AND (
                  (chat.project_id IS NULL AND NEW.project_id IS NULL)
                  OR chat.project_id = NEW.project_id
              )
        ) THEN RAISE(ABORT, 'Agent Chat context scope must reference its chat')
    END;
END;

CREATE TRIGGER agent_chat_context_scope_guard_update
BEFORE UPDATE OF scope_type, scope_id, project_id ON agent_context_scope
WHEN NEW.scope_type = 'agent_chat'
BEGIN
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1
            FROM agent_chat AS chat
            WHERE chat.id = NEW.scope_id
              AND (
                  (chat.project_id IS NULL AND NEW.project_id IS NULL)
                  OR chat.project_id = NEW.project_id
              )
        ) THEN RAISE(ABORT, 'Agent Chat context scope must reference its chat')
    END;
END;

CREATE TRIGGER agent_context_scope_reject_legacy_room_insert
BEFORE INSERT ON agent_context_scope
WHEN NEW.scope_type = 'room'
BEGIN
    SELECT RAISE(ABORT, 'Room scopes are retired; use an Agent Chat scope');
END;

CREATE TRIGGER agent_context_scope_reject_legacy_room_update
BEFORE UPDATE OF scope_type, scope_id, room_id ON agent_context_scope
WHEN NEW.scope_type = 'room'
BEGIN
    SELECT RAISE(ABORT, 'Room scopes are retired; use an Agent Chat scope');
END;

CREATE TRIGGER agent_context_scope_identity_profile_guard
BEFORE INSERT ON agent_session
BEGIN
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1
            FROM agent_context_scope AS scope
            JOIN agent_profile AS profile ON profile.id = NEW.profile_id
            WHERE scope.id = NEW.context_scope_id
              AND scope.identity_id = NEW.identity_id
              AND profile.identity_id = NEW.identity_id
              AND profile.backend_kind = NEW.backend_kind
        )
        THEN RAISE(ABORT, 'session identity, profile, and scope must match')
    END;
END;

CREATE TRIGGER agent_context_scope_identity_profile_guard_update
BEFORE UPDATE OF identity_id, profile_id, context_scope_id ON agent_session
BEGIN
    SELECT CASE
        WHEN NOT EXISTS (
            SELECT 1
            FROM agent_context_scope AS scope
            JOIN agent_profile AS profile ON profile.id = NEW.profile_id
            WHERE scope.id = NEW.context_scope_id
              AND scope.identity_id = NEW.identity_id
              AND profile.identity_id = NEW.identity_id
              AND profile.backend_kind = NEW.backend_kind
        )
        THEN RAISE(ABORT, 'session identity, profile, and scope must match')
    END;
END;

CREATE TRIGGER context_manifest_immutable_delete
BEFORE DELETE ON context_manifest
WHEN EXISTS (
    SELECT 1 FROM agent_context_scope AS scope
    WHERE scope.id = OLD.context_scope_id
)
BEGIN
    SELECT RAISE(ABORT, 'context manifests are immutable');
END;

COMMIT;
PRAGMA foreign_keys = ON;
