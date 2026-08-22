-- Project deletion is a bounded teardown, not a rewrite.
--
-- `ProjectRepo::delete` already installs a `project_deletion_guard` row so the
-- Project-scoped append-only tables allow their rows to be removed exactly
-- once, in dependency order. `context_manifest` and `context_manifest_source`
-- never joined that contract: their delete triggers aborted unconditionally.
--
-- They are not deleted by the teardown's explicit statements either. They are
-- reached by cascade: `DELETE FROM project` removes the Project's Agent Chats,
-- which cascades to `agent_context_scope`, which cascades to the manifests.
-- The unconditional trigger therefore aborted the whole transaction, and any
-- Project whose agent had produced a single context manifest could never be
-- deleted.
--
-- Immutability here means "a committed manifest cannot be rewritten or removed
-- while the scope it describes still exists" — not "the owning Project is
-- permanent". Both triggers now abort only when their parent row is still
-- present, which is exactly the direct-delete case. During a cascade SQLite has
-- already removed the parent, so the teardown proceeds. Update immutability is
-- untouched and still unconditional.
--
-- No data is rewritten or removed by this migration.

DROP TRIGGER IF EXISTS context_manifest_immutable_delete;
CREATE TRIGGER context_manifest_immutable_delete
BEFORE DELETE ON context_manifest
WHEN EXISTS (
    SELECT 1 FROM agent_context_scope s WHERE s.id = OLD.context_scope_id
)
BEGIN
    SELECT RAISE(ABORT, 'context manifests are immutable');
END;

DROP TRIGGER IF EXISTS context_manifest_source_immutable_delete;
CREATE TRIGGER context_manifest_source_immutable_delete
BEFORE DELETE ON context_manifest_source
WHEN EXISTS (
    SELECT 1 FROM context_manifest m WHERE m.id = OLD.manifest_id
)
BEGIN
    SELECT RAISE(ABORT, 'context manifest sources are immutable');
END;
