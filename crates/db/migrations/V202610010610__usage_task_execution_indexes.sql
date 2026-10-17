-- The immutable usage ledger remains the source of truth.
CREATE INDEX idx_usage_invocation_task ON usage_invocation(task_id);
CREATE INDEX idx_usage_invocation_execution ON usage_invocation(execution_id);
CREATE INDEX idx_usage_invocation_source ON usage_invocation(source_id);
CREATE INDEX idx_usage_event_task ON usage_event(task_id);
CREATE INDEX idx_usage_event_execution ON usage_event(execution_id);
CREATE INDEX idx_usage_event_source ON usage_event(source_id);
