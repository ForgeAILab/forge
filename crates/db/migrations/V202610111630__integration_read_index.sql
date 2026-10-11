-- Read path of the Task list's `integration` summary: the validator of a
-- cached list reads the members in progress of one Project. Additive: an
-- index only, no row is changed.
CREATE INDEX integration_attempt_project_live ON integration_attempt(project_ref) WHERE current=1;
