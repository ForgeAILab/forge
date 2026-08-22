-- Durable command receipts are the replay boundary for shared orchestration
-- commands.  The operation/scope/idempotency tuple is the replay identity;
-- principal and input digest are frozen facts checked on every replay.
CREATE TABLE command_receipt (
    id                           TEXT PRIMARY KEY CHECK (length(trim(id)) > 0),
    principal_type               TEXT NOT NULL CHECK (length(trim(principal_type)) > 0),
    principal_id                 TEXT NOT NULL CHECK (length(trim(principal_id)) > 0),
    scope_type                   TEXT NOT NULL CHECK (scope_type IN ('account', 'agent_chat', 'project', 'task')),
    scope_id                     TEXT NOT NULL CHECK (length(trim(scope_id)) > 0),
    operation                    TEXT NOT NULL CHECK (length(trim(operation)) > 0),
    idempotency_key              TEXT NOT NULL CHECK (length(trim(idempotency_key)) > 0),
    input_digest                 TEXT NOT NULL CHECK (length(trim(input_digest)) > 0),
    policy_result                TEXT NOT NULL CHECK (length(trim(policy_result)) > 0),
    correlation_id               TEXT NOT NULL CHECK (length(trim(correlation_id)) > 0),
    causation_id                 TEXT CHECK (causation_id IS NULL OR length(trim(causation_id)) > 0),
    causation_depth              INTEGER NOT NULL DEFAULT 0 CHECK (causation_depth BETWEEN 0 AND 16),
    event_id                     TEXT NOT NULL CHECK (length(trim(event_id)) > 0) REFERENCES domain_event(id) ON DELETE RESTRICT,
    agent_action_execution_id   TEXT CHECK (agent_action_execution_id IS NULL OR length(trim(agent_action_execution_id)) > 0) REFERENCES agent_action_execution(id) ON DELETE RESTRICT,
    outcome_json                 TEXT NOT NULL CHECK (json_valid(outcome_json)),
    committed_at                 TEXT NOT NULL CHECK (length(trim(committed_at)) > 0),
    UNIQUE (
        scope_type,
        scope_id,
        operation,
        idempotency_key
    )
);

CREATE INDEX idx_command_receipt_scope_created
    ON command_receipt(scope_type, scope_id, committed_at DESC, id DESC);

CREATE INDEX idx_command_receipt_action
    ON command_receipt(agent_action_execution_id)
    WHERE agent_action_execution_id IS NOT NULL;

-- Command receipts are frozen outcomes.  There is deliberately no update or
-- delete path: replay must return the committed result that was originally
-- observed, including its provenance and input digest.
CREATE TRIGGER command_receipt_immutable_update
BEFORE UPDATE ON command_receipt
BEGIN
    SELECT RAISE(ABORT, 'Command receipts are immutable');
END;

CREATE TRIGGER command_receipt_immutable_delete
BEFORE DELETE ON command_receipt
BEGIN
    SELECT RAISE(ABORT, 'Command receipts are immutable');
END;
