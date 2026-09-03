-- The Task dispatcher automatically pauses a Project that has no primary
-- repository (see the task_dispatcher crate), rather than parking its Tasks
-- in `backlog` at creation. This column names that reason so it stays
-- distinct from a user's own POST /projects/{id}/pause: only a Project
-- carrying this exact reason is auto-resumed once a repository is attached.
-- Any explicit pause/resume, or a full Project update that sets `paused_at`,
-- clears this column, so a deliberate user pause is never auto-resumed and
-- never misattributed to the system.
ALTER TABLE project ADD COLUMN system_pause_reason TEXT;
