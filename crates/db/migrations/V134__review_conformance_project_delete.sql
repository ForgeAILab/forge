-- Review evidence is immutable while its Task exists, but it belongs to the
-- Task execution graph and must leave with an explicitly deleted Project.
-- V132 omitted cascade actions and its unconditional delete triggers blocked
-- both execution/task cascades.
PRAGMA foreign_keys = OFF;

DROP TRIGGER IF EXISTS execution_review_contract_immutable_update;
DROP TRIGGER IF EXISTS execution_review_contract_immutable_delete;
DROP TRIGGER IF EXISTS execution_review_assessment_immutable_update;
DROP TRIGGER IF EXISTS execution_review_assessment_immutable_delete;

CREATE TABLE execution_review_contract_v134 (
    execution_id TEXT PRIMARY KEY REFERENCES execution(id) ON DELETE CASCADE,
    task_id TEXT NOT NULL REFERENCES task(id),
    contract_digest TEXT NOT NULL UNIQUE,
    source_digest TEXT NOT NULL,
    contract_json TEXT NOT NULL CHECK(json_valid(contract_json)),
    created_at TEXT NOT NULL
);

INSERT INTO execution_review_contract_v134 (
    execution_id, task_id, contract_digest, source_digest, contract_json, created_at
)
SELECT execution_id, task_id, contract_digest, source_digest, contract_json, created_at
FROM execution_review_contract;

CREATE TABLE execution_review_assessment_v134 (
    execution_id TEXT PRIMARY KEY
        REFERENCES execution_review_contract_v134(execution_id) ON DELETE CASCADE,
    conformance_json TEXT NOT NULL CHECK(json_valid(conformance_json)),
    created_at TEXT NOT NULL
);

INSERT INTO execution_review_assessment_v134 (execution_id, conformance_json, created_at)
SELECT execution_id, conformance_json, created_at
FROM execution_review_assessment;

DROP TABLE execution_review_assessment;
DROP TABLE execution_review_contract;
ALTER TABLE execution_review_contract_v134 RENAME TO execution_review_contract;
ALTER TABLE execution_review_assessment_v134 RENAME TO execution_review_assessment;

CREATE TRIGGER execution_review_contract_immutable_update
BEFORE UPDATE ON execution_review_contract
BEGIN
    SELECT RAISE(ABORT, 'review contracts are immutable');
END;

-- A direct delete remains forbidden. During Task/Project cascade the Task row
-- has already left and the owned immutable evidence may be removed with it.
CREATE TRIGGER execution_review_contract_immutable_delete
BEFORE DELETE ON execution_review_contract
WHEN EXISTS (SELECT 1 FROM task WHERE id = OLD.task_id)
BEGIN
    SELECT RAISE(ABORT, 'review contracts are immutable');
END;

CREATE TRIGGER execution_review_assessment_immutable_update
BEFORE UPDATE ON execution_review_assessment
BEGIN
    SELECT RAISE(ABORT, 'review assessments are immutable');
END;

-- A contract cascade removes its parent row before this child action. A
-- standalone assessment delete still sees the live contract and is rejected.
CREATE TRIGGER execution_review_assessment_immutable_delete
BEFORE DELETE ON execution_review_assessment
WHEN EXISTS (
    SELECT 1 FROM execution_review_contract
    WHERE execution_id = OLD.execution_id
)
BEGIN
    SELECT RAISE(ABORT, 'review assessments are immutable');
END;

PRAGMA foreign_keys = ON;
