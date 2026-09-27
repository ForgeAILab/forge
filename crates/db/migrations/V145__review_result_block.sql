-- Reviewers now answer in Markdown ending with one result block
-- {"result": "pass|fail|blocked", "reason": "..."}, and Forge stores the
-- assessment as {result, reason, report}. The previous structured assessment
-- ({contract_digest, verdict, requirements, findings}) no longer parses, so
-- rewrite every stored one: the verdict becomes the result, the recorded
-- conformance reason becomes the reason, and the original assessment JSON is
-- kept verbatim as the report so no reviewer output is lost.
--
-- The immutable assessment row and the Review's copy must stay equal after
-- parsing (the integration guard compares them). Assessments are otherwise
-- immutable; the update trigger is lifted for this one rewrite and restored.

DROP TRIGGER execution_review_assessment_immutable_update;

UPDATE execution_review_assessment
SET conformance_json = json_set(
    conformance_json,
    '$.assessment',
    json_object(
        'result', json_extract(conformance_json, '$.assessment.verdict'),
        'reason', coalesce(json_extract(conformance_json, '$.reason'), ''),
        'report', '```json' || char(10)
            || json_extract(conformance_json, '$.assessment') || char(10) || '```'
    )
)
WHERE json_type(conformance_json, '$.assessment.verdict') = 'text';

CREATE TRIGGER execution_review_assessment_immutable_update
BEFORE UPDATE ON execution_review_assessment
BEGIN
    SELECT RAISE(ABORT, 'review assessments are immutable');
END;

-- A Review's copy of a frozen assessment takes the migrated immutable row
-- verbatim. The two copies were serialized with different key orders, so
-- rendering each one separately would leave their reports unequal.
UPDATE review
SET step_results_json = json_set(
    step_results_json,
    '$.conformance.assessment',
    json((
        SELECT json_extract(a.conformance_json, '$.assessment')
        FROM execution_review_assessment a
        WHERE a.execution_id
            = json_extract(review.step_results_json, '$.conformance.contract.execution_id')
    ))
)
WHERE json_valid(step_results_json)
  AND json_type(step_results_json, '$.conformance.assessment.verdict') = 'text'
  AND EXISTS (
      SELECT 1 FROM execution_review_assessment a
      WHERE a.execution_id
          = json_extract(review.step_results_json, '$.conformance.contract.execution_id')
        AND json_type(a.conformance_json, '$.assessment.result') = 'text'
  );

-- Any other Review copy has no frozen counterpart to match.
UPDATE review
SET step_results_json = json_set(
    step_results_json,
    '$.conformance.assessment',
    json_object(
        'result', json_extract(step_results_json, '$.conformance.assessment.verdict'),
        'reason', coalesce(json_extract(step_results_json, '$.conformance.reason'), ''),
        'report', '```json' || char(10)
            || json_extract(step_results_json, '$.conformance.assessment') || char(10) || '```'
    )
)
WHERE json_valid(step_results_json)
  AND json_type(step_results_json, '$.conformance.assessment.verdict') = 'text';
