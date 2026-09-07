-- Historical reviews remain unchanged. Only new admissions obtain contracts.
CREATE TABLE execution_review_contract (
    execution_id TEXT PRIMARY KEY REFERENCES execution(id),
    task_id TEXT NOT NULL REFERENCES task(id),
    contract_digest TEXT NOT NULL UNIQUE,
    source_digest TEXT NOT NULL,
    contract_json TEXT NOT NULL CHECK(json_valid(contract_json)),
    created_at TEXT NOT NULL
);
CREATE TABLE execution_review_assessment (
    execution_id TEXT PRIMARY KEY REFERENCES execution_review_contract(execution_id),
    conformance_json TEXT NOT NULL CHECK(json_valid(conformance_json)),
    created_at TEXT NOT NULL
);
CREATE TRIGGER execution_review_contract_immutable_update
BEFORE UPDATE ON execution_review_contract BEGIN SELECT RAISE(ABORT, 'review contracts are immutable'); END;
CREATE TRIGGER execution_review_contract_immutable_delete
BEFORE DELETE ON execution_review_contract BEGIN SELECT RAISE(ABORT, 'review contracts are immutable'); END;
CREATE TRIGGER execution_review_assessment_immutable_update
BEFORE UPDATE ON execution_review_assessment BEGIN SELECT RAISE(ABORT, 'review assessments are immutable'); END;
CREATE TRIGGER execution_review_assessment_immutable_delete
BEFORE DELETE ON execution_review_assessment BEGIN SELECT RAISE(ABORT, 'review assessments are immutable'); END;

-- Rewrite the retired built-in prompt selector; custom templates stay intact.
UPDATE project SET workflow_definition = replace(workflow_definition, '"reviewer.default.v2"', '"reviewer.conformance.v1"')
WHERE instr(workflow_definition, '"reviewer.default.v2"') > 0;
