//! Canonical JSON contracts for migrated native orchestration operations.
//!
//! This module owns the provider-facing schemas and preparation-time
//! structural validation. Domain and lifecycle validation remains in the
//! service command boundary.

use std::collections::BTreeSet;

use agent_runtime::core::prelude::RuntimeError;
use serde_json::{Value, json};

use crate::operation_catalog::{
    MAIN_CHARTER_APPROVAL_TARGET_OPERATION, MAIN_CHARTER_DIFF_OPERATION,
    MAIN_CHARTER_DRAFT_OPERATION, MAIN_CHARTER_READ_OPERATION, MAIN_CHARTER_READINESS_OPERATION,
    MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION, MAIN_GENESIS_PROJECT_AGENTS_READ_OPERATION,
    MAIN_GENESIS_START_OPERATION, MAIN_INQUIRY_RUN_OPERATION, MAIN_PROJECT_CREATE_OPERATION,
    PROJECT_CHARTER_ADOPTION_OPERATION, PROJECT_CHARTER_READ_OPERATION,
    PROJECT_CURRENT_STATE_OPERATION, PROJECT_DECISION_OPERATION, PROJECT_DOCUMENT_OPERATION,
    PROJECT_EVIDENCE_OPERATION, PROJECT_MILESTONE_OPERATION, PROJECT_OBSERVATIONS_OPERATION,
    PROJECT_READINESS_OPERATION, PROJECT_RELEASE_OPERATION, PROJECT_SKILL_SECTION_NAMES,
    PROJECT_SKILL_SECTION_OPERATION, PROJECT_VALIDATION_OPERATION, TASK_ADAPTIVE_OPERATION,
    TASK_EVIDENCE_OPERATION, TASK_PROPOSE_OPERATION, TASK_RECOVER_OPERATION, TASK_REVIEW_OPERATION,
    TASK_WORKLOG_OPERATION,
};

pub(crate) fn object_schema(properties: Value, required: &[&str]) -> Value {
    json!({
        "type": "object",
        "properties": properties,
        "required": required,
        "additionalProperties": false,
    })
}
pub(crate) fn described_object_schema(
    properties: Value,
    required: &[&str],
    description: &str,
) -> Value {
    let mut schema = object_schema(properties, required);
    schema["description"] = Value::String(description.to_owned());
    schema
}

pub(crate) fn string_array_schema() -> Value {
    json!({"type":"array","items":{"type":"string"}})
}

pub(crate) fn string_or_null_schema() -> Value {
    json!({"type":["string","null"]})
}

pub(crate) fn principal_ref_schema() -> Value {
    object_schema(
        json!({
            "kind": {"type":"string","enum":["user","agent","worker","reviewer","service","system"]},
            "id": {"type":"string","minLength":1},
            "display_name": string_or_null_schema(),
        }),
        &["kind", "id"],
    )
}

pub(crate) fn provenance_ref_schema() -> Value {
    object_schema(
        json!({
            "source_kind": {"type":"string","enum":["user","main_chat","project_chat","research","task","validation","document","decision","milestone","release","system"]},
            "source_id": {"type":"string","minLength":1},
            "revision_id": string_or_null_schema(),
            "digest": string_or_null_schema(),
            "label": string_or_null_schema(),
            "observed_at": string_or_null_schema(),
        }),
        &["source_kind", "source_id"],
    )
}

pub(crate) fn artifact_ref_schema() -> Value {
    object_schema(
        json!({
            "artifact_id": {"type":"string","minLength":1},
            "revision_id": {"type":"string","minLength":1},
            "content_digest": {"type":"string","minLength":1},
            "render_version": string_or_null_schema(),
            "render_digest": string_or_null_schema(),
        }),
        &["artifact_id", "revision_id", "content_digest"],
    )
}

pub(crate) fn revision_provenance_schema() -> Value {
    object_schema(
        json!({
            "author": principal_ref_schema(),
            "profile_revision": string_or_null_schema(),
            "operating_skill_revision": string_or_null_schema(),
            "source_refs": {"type":"array","items":provenance_ref_schema()},
            "change_summary": {"type":"string","minLength":1},
            "material_diff": string_or_null_schema(),
        }),
        &["author", "change_summary"],
    )
}

pub(crate) fn charter_risk_schema() -> Value {
    object_schema(
        json!({
            "id": {"type":"string","minLength":1},
            "description": {"type":"string","minLength":1},
            "impact": string_or_null_schema(),
            "treatment": string_or_null_schema(),
            "revisit_trigger": string_or_null_schema(),
            "owner": {"oneOf":[principal_ref_schema(), {"type":"null"}]},
        }),
        &["id", "description"],
    )
}

pub(crate) fn charter_knowledge_item_schema() -> Value {
    object_schema(
        json!({
            "id": {"type":"string","minLength":1},
            "statement": {"type":"string","minLength":1},
            "kind": {"type":"string","enum":["observed_fact","user_decision","research_finding","assumption","hypothesis","open_decision","research_queue"]},
            "normative": {"type":"boolean"},
            "transfer_approved": {"type":"boolean"},
            "provenance": {"type":"array","items":provenance_ref_schema()},
            "confidence": {"type":["string","null"],"enum":["low","medium","high","not_applicable",null]},
            "observed_at": string_or_null_schema(),
            "freshness_expires_at": string_or_null_schema(),
            "impact": string_or_null_schema(),
            "owner": {"oneOf":[principal_ref_schema(), {"type":"null"}]},
            "default_value": string_or_null_schema(),
            "revisit_trigger": string_or_null_schema(),
            "falsification_evidence": string_or_null_schema(),
            "blocking": {"type":"boolean"},
        }),
        &["id", "statement", "kind", "normative", "transfer_approved"],
    )
}

pub(crate) fn charter_content_schema() -> Value {
    object_schema(
        json!({
            "identity": object_schema(json!({
                "working_name": {"type":"string","minLength":1},
                "slug_proposal": string_or_null_schema(),
                "one_line_vision": {"type":"string","minLength":1},
                "maturity": {"type":"string","enum":["prototype","mvp","production","critical"]},
                "lifecycle_intent": string_or_null_schema(),
                "project_type": string_or_null_schema(),
                "value_proposition": string_or_null_schema(),
            }), &["working_name", "one_line_vision", "maturity"]),
            "problem_and_people": object_schema(json!({
                "problem_or_opportunity": {"type":"string","minLength":1},
                "target_users": string_array_schema(),
                "beneficiaries": string_array_schema(),
                "jobs_pains_opportunity": string_array_schema(),
                "current_alternatives": string_array_schema(),
                "stakeholders": string_array_schema(),
                "excluded_audiences": string_array_schema(),
            }), &["problem_or_opportunity"]),
            "core_experience": object_schema(json!({
                "primary_outcome": {"type":"string","minLength":1},
                "core_loop": string_or_null_schema(),
                "principal_journeys": string_array_schema(),
            }), &["primary_outcome"]),
            "scope": object_schema(json!({
                "must_have_outcomes": string_array_schema(),
                "required_deliverables": string_array_schema(),
                "later_possibilities": string_array_schema(),
                "explicit_non_goals": string_array_schema(),
            }), &[]),
            "success": object_schema(json!({
                "qualitative_outcome": string_or_null_schema(),
                "success_signals": string_array_schema(),
                "acceptance_statements": string_array_schema(),
                "required_evidence": string_array_schema(),
                "non_claims": string_array_schema(),
            }), &[]),
            "constraints_and_risks": object_schema(json!({
                "product": string_array_schema(),
                "time_and_budget": string_array_schema(),
                "technology": string_array_schema(),
                "data": string_array_schema(),
                "integrations": string_array_schema(),
                "security_privacy_compliance": string_array_schema(),
                "accessibility": string_array_schema(),
                "operations": string_array_schema(),
                "migration": string_array_schema(),
                "launch": string_array_schema(),
                "agent_authority": string_array_schema(),
                "risks": {"type":"array","items":charter_risk_schema()},
            }), &[]),
            "knowledge_ledger": object_schema(json!({
                "items": {"type":"array","items":charter_knowledge_item_schema()},
            }), &[]),
            "scaffold": {"oneOf":[object_schema(json!({
                "template":{"type":"string","minLength":1,"description":"spark template id: nextjs or vite-react"},
                "packs":string_array_schema(),
            }), &["template"]), {"type":"null"}], "description":"Repository scaffold for a web product: a spark template plus feature packs. Omit for a non-web product or a repository the user brings."},
            "handoff_note": {"oneOf":[object_schema(json!({
                "recommended_first_action": string_or_null_schema(),
                "bounded_summary": string_or_null_schema(),
                "unresolved_item_ids": string_array_schema(),
            }), &[]), {"type":"null"}]},
        }),
        &[
            "identity",
            "problem_and_people",
            "core_experience",
            "scope",
            "success",
            "constraints_and_risks",
            "knowledge_ledger",
        ],
    )
}

pub(crate) fn document_content_schema() -> Value {
    json!({
        "oneOf": [
            object_schema(json!({
                "question":{"type":"string","minLength":1},
                "decision_informed":{"type":"string","minLength":1},
                "scope":{"type":"string","minLength":1},
                "stopping_condition":{"type":"string","minLength":1},
                "sources":{"type":"array","items":object_schema(json!({
                    "id":{"type":"string","minLength":1},"url":{"type":"string","minLength":1},"title":{"type":"string","minLength":1},"retrieved_at":{"type":"string","minLength":1},"quality":string_or_null_schema(),"claim":{"type":"string","minLength":1},"is_inference":{"type":"boolean"}
                }), &["id","url","title","retrieved_at","claim","is_inference"])},
                "findings":string_array_schema(),"evidence":string_array_schema(),"inferences":string_array_schema(),"alternatives":string_array_schema(),"recommendation":string_or_null_schema(),"uncertainty":string_array_schema(),"unresolved_questions":string_array_schema(),"affected_artifact_ids":string_array_schema(),"affected_decision_ids":string_array_schema()
            }), &["question","decision_informed","scope","stopping_condition"]),
            object_schema(json!({
                "intended_deliverables":string_array_schema(),"boundaries":string_array_schema(),"plan_items":{"type":"array","items":object_schema(json!({"id":{"type":"string","minLength":1},"outcome":{"type":"string","minLength":1},"dependencies":string_array_schema(),"task_ids":string_array_schema()}), &["id","outcome"])},"acceptance_matrix":{"type":"array","items":object_schema(json!({"id":{"type":"string","minLength":1},"statement":{"type":"string","minLength":1},"evidence":string_array_schema(),"required":{"type":"boolean"}}), &["id","statement","required"])},"risks":{"type":"array","items":charter_risk_schema()},"rollback_and_recovery":string_array_schema(),"adaptive_envelope":string_array_schema(),"governing_charter_revision_id":string_or_null_schema()
            }), &[]),
            object_schema(json!({
                "problem_and_outcome":{"type":"string","minLength":1},"actors":string_array_schema(),"journeys_and_flows":string_array_schema(),"functional_requirements":string_array_schema(),"loading_empty_error_recovery_states":string_array_schema(),"acceptance_scenarios":{"type":"array"},"non_functional_and_safety_requirements":string_array_schema(),"out_of_scope":string_array_schema(),"traceability":{"type":"array","items":artifact_ref_schema()}
            }), &["problem_and_outcome"]),
            object_schema(json!({"experience_principles":string_array_schema(),"information_architecture":string_array_schema(),"flows":string_array_schema(),"design_tokens_reference":string_or_null_schema(),"component_states":string_array_schema(),"responsive_behavior":string_array_schema(),"accessibility":string_array_schema(),"prototype_or_evidence_links":string_array_schema(),"open_decisions":string_array_schema()}), &[]),
            object_schema(json!({"context_and_constraints":{"type":"string","minLength":1},"system_boundary":string_array_schema(),"components_and_data":string_array_schema(),"interfaces":string_array_schema(),"security_and_privacy":string_array_schema(),"concurrency":string_array_schema(),"failure_and_recovery":string_array_schema(),"observability_and_operations":string_array_schema(),"migrations":string_array_schema(),"alternatives_and_tradeoffs":string_array_schema(),"validation_plan":string_array_schema()}), &["context_and_constraints"]),
            object_schema(json!({"ordered_milestone_outcomes":string_array_schema(),"dependencies":string_array_schema(),"risks":{"type":"array","items":charter_risk_schema()},"linked_artifact_refs":{"type":"array","items":artifact_ref_schema()},"task_queries_or_ids":string_array_schema(),"acceptance_evidence_contract":{"type":"array"},"release_notes":string_array_schema(),"known_issues":string_array_schema()}), &[]),
        ]
    })
}

pub(crate) fn orchestration_payload_schema(operation: &str) -> Value {
    match operation {
        MAIN_GENESIS_START_OPERATION => object_schema(
            json!({
                "action":{"const":"start"},
                "maturity":{"type":["string","null"],"enum":["prototype","mvp","production","critical",null]},
                "preferred_project_agent_identity_id":string_or_null_schema()
            }),
            &["action"],
        ),
        MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION => described_object_schema(
            json!({
                "action":{"const":"select"},
                "genesis_session_id":string_or_null_schema(),
                "expected_session_version":{"type":"integer","minimum":1},
                "project_agent_identity_id":{"type":"string","minLength":1}
            }),
            &[
                "action",
                "expected_session_version",
                "project_agent_identity_id",
            ],
            "Persist the exact structured Project Agent preference for the active Product Genesis session. Read genesis.project_agents.read first; Charter approval will freeze this identity's current profile, operating-skill revision, and policy digest. This operation never changes Charter prose.",
        ),
        MAIN_CHARTER_DRAFT_OPERATION => object_schema(
            json!({
                "action":{"const":"save_revision"},"charter_id":{"type":"string","minLength":1},"base_revision_id":string_or_null_schema(),"project_mode":{"type":"string","enum":["compact","standard"]},"maturity":{"type":"string","enum":["prototype","mvp","production","critical"]},"content":charter_content_schema(),"rendered_view":{"type":"string","minLength":1,"description":"Omit. The server renders the canonical view from content; provide only to round-trip an exact server-rendered value."},"render_version":{"type":"string","minLength":1,"description":"Omit. The server stamps its own render version; provide only to round-trip an exact server value."},"provenance":revision_provenance_schema()
            }),
            // `rendered_view`/`render_version` stay optional: the server
            // renders the canonical view itself and only verifies these
            // fields when a caller round-trips them. Requiring them forces
            // the model to reproduce the server renderer byte-for-byte,
            // which always fails.
            &[
                "action",
                "charter_id",
                "project_mode",
                "maturity",
                "content",
                "provenance",
            ],
        ),
        MAIN_CHARTER_READINESS_OPERATION => object_schema(
            json!({"action":{"const":"evaluate"},"charter_id":{"type":"string","minLength":1},"revision_id":{"type":"string","minLength":1},"content_digest":{"type":"string","minLength":1},"render_digest":{"type":"string","minLength":1},"expected_charter_version":{"type":"integer","minimum":1}}),
            &[
                "action",
                "charter_id",
                "revision_id",
                "content_digest",
                "render_digest",
                "expected_charter_version",
            ],
        ),
        MAIN_CHARTER_DIFF_OPERATION => object_schema(
            json!({"action":{"const":"compare_revisions"},"charter_id":{"type":"string","minLength":1},"base_revision_id":{"type":"string","minLength":1},"candidate_revision_id":{"type":"string","minLength":1}}),
            &[
                "action",
                "charter_id",
                "base_revision_id",
                "candidate_revision_id",
            ],
        ),
        MAIN_CHARTER_APPROVAL_TARGET_OPERATION => object_schema(
            json!({"action":{"const":"present"},"charter_id":{"type":"string","minLength":1},"revision_id":{"type":"string","minLength":1},"content_digest":{"type":"string","minLength":1},"render_digest":{"type":"string","minLength":1},"expected_charter_version":{"type":"integer","minimum":1},"approved_project_name":{"type":"string","minLength":1},"approved_project_slug":string_or_null_schema(),"project_mode":{"type":"string","enum":["compact","standard"]},"selected_project_agent_identity_id":{"type":"string","minLength":1},"selected_project_agent_profile_revision_id":{"type":"string","minLength":1},"selected_project_agent_operating_skill_revision":{"type":"string","minLength":1},"selected_project_agent_policy_digest":{"type":"string","minLength":1}}),
            &[
                "action",
                "charter_id",
                "revision_id",
                "content_digest",
                "render_digest",
                "expected_charter_version",
                "approved_project_name",
                "project_mode",
                "selected_project_agent_identity_id",
                "selected_project_agent_profile_revision_id",
                "selected_project_agent_operating_skill_revision",
                "selected_project_agent_policy_digest",
            ],
        ),
        MAIN_PROJECT_CREATE_OPERATION => object_schema(
            json!({"action":{"const":"create_from_approval"},"approval_id":{"type":"string","minLength":1}}),
            &["action", "approval_id"],
        ),
        PROJECT_CHARTER_ADOPTION_OPERATION => described_object_schema(
            json!({
                "action":{"const":"draft_revision"},
                "charter_id":{"type":"string","minLength":1,"description":"Omit to start the Project's adoption Charter. The server mints the id and returns it; supply it only to revise the Charter it already returned."},
                "base_revision_id":string_or_null_schema(),
                "expected_charter_version":{"type":"integer","minimum":0,"description":"0 to start the Project's adoption Charter. To revise an existing draft, send the charter_version returned by your previous action result; a conflict names the value to send."},
                "project_mode":{"type":"string","enum":["compact","standard"]},
                "maturity":{"type":"string","enum":["prototype","mvp","production","critical"]},
                "content":charter_content_schema(),
                "rendered_view":{"type":"string","minLength":1,"description":"Omit. The server renders the canonical view from content; provide only to round-trip an exact server-rendered value."},
                "render_version":{"type":"string","minLength":1,"description":"Omit. The server stamps its own render version; provide only to round-trip an exact server value."},
                "provenance":revision_provenance_schema()
            }),
            // `rendered_view`/`render_version` stay optional for the same
            // reason as the Main Charter draft: the render is derived from
            // `content`, and a model cannot reproduce the server renderer
            // byte-for-byte.
            &[
                "action",
                "expected_charter_version",
                "project_mode",
                "maturity",
                "content",
                "provenance",
            ],
            "Setup-only Project Agent adoption Charter draft. The bound Project may have no current Charter; this creates an unapproved candidate only and cannot approve, attach, apply, or authorize execution.",
        ),
        PROJECT_DOCUMENT_OPERATION => {
            let document_kinds = json!({
                "type":"string",
                "enum":["research","delivery_brief","product_spec","design","architecture","execution_plan"]
            });
            let identity = json!({
                "document_id":{"type":"string","minLength":1},
                "kind":document_kinds,
                "title":{"type":"string","minLength":1},
            });
            let draft_properties = {
                let mut properties = identity.clone();
                properties["action"] = json!({"enum":["draft_revision","propose_approval"]});
                properties["base_revision_id"] = string_or_null_schema();
                properties["expected_document_version"] = json!({"type":"integer","minimum":1});
                properties["content"] = document_content_schema();
                properties
            };
            let draft = object_schema(
                draft_properties,
                &[
                    "action",
                    "document_id",
                    "kind",
                    "title",
                    "expected_document_version",
                    "content",
                ],
            );
            let approval = object_schema(
                {
                    let mut properties = identity;
                    properties["action"] = json!({"const":"approve"});
                    properties["revision_id"] = json!({"type":"string","minLength":1});
                    properties["content_digest"] = json!({"type":"string","minLength":1});
                    properties["render_digest"] = json!({"type":"string","minLength":1});
                    properties["expected_document_version"] = json!({"type":"integer","minimum":1});
                    properties["envelope_digest"] = string_or_null_schema();
                    properties
                },
                &[
                    "action",
                    "document_id",
                    "kind",
                    "title",
                    "revision_id",
                    "content_digest",
                    "render_digest",
                    "expected_document_version",
                ],
            );
            // Keep the operation-level property contract discoverable for
            // clients that inspect `action.enum`, while the closed oneOf
            // variants make draft/proposal versus policy-scoped approval
            // payloads exact and disallow cross-action fields.
            json!({
                "type":"object",
                "required":["action","document_id","kind","title","expected_document_version"],
                "properties":{
                    "action":{"type":"string","enum":["draft_revision","propose_approval","approve"]},
                    "document_id":{"type":"string","minLength":1},
                    "kind":{"type":"string","enum":["research","delivery_brief","product_spec","design","architecture","execution_plan"]},
                    "title":{"type":"string","minLength":1},
                    "base_revision_id":string_or_null_schema(),
                    "revision_id":{"type":"string","minLength":1},
                    "expected_document_version":{"type":"integer","minimum":1},
                    "content":document_content_schema(),
                    "content_digest":{"type":"string","minLength":1},
                    "render_digest":{"type":"string","minLength":1},
                    "envelope_digest":string_or_null_schema()
                },
                "oneOf":[draft, approval],
                "additionalProperties":false
            })
        }
        PROJECT_DECISION_OPERATION => described_object_schema(
            json!({"action":{"enum":["record_candidate","record_effective"]},"question":{"type":"string","minLength":1},"options":string_array_schema(),"selected_outcome":string_or_null_schema(),"rationale":string_or_null_schema(),"decision_class":{"const":"project_implementation"},"expected_project_version":{"type":"integer","minimum":1},"decision_id":string_or_null_schema(),"affected_artifact_refs":{"type":"array","items":artifact_ref_schema()},"affected_task_ids":string_array_schema(),"affected_milestone_ids":string_array_schema()}),
            &[
                "action",
                "question",
                "decision_class",
                "expected_project_version",
            ],
            "Project Agent decisions are limited to implementation choices inside the approved Project Charter. User-scope decisions, policy decisions, waivers, and manual approvals are user-only actions outside this tool.",
        ),
        PROJECT_MILESTONE_OPERATION => object_schema(
            json!({"action":{"enum":["define","revise","set_primary"]},"milestone_id":string_or_null_schema(),"display_label":string_or_null_schema(),"expected_milestone_version":{"type":"integer","minimum":1},"primary_milestone_id":string_or_null_schema(),"content":{"type":"object","properties":{"name":{"type":"string","minLength":1},"outcome":{"type":"string","minLength":1},"included_scope":string_array_schema(),"excluded_scope":string_array_schema(),"charter_revision":{"oneOf":[artifact_ref_schema(),{"type":"null"}]},"document_revisions":{"type":"array","items":artifact_ref_schema()},"task_ids":string_array_schema(),"dependencies":string_array_schema(),"risks":{"type":"array","items":charter_risk_schema()},"acceptance_checks":{"type":"array","items":object_schema(json!({"id":{"type":"string","minLength":1,"description":"Stable canonical id; preserve it across milestone revisions."},"description":{"type":"string","minLength":1},"required":{"type":"boolean"},"source_kind":{"type":"string","enum":["manual","policy_waiver","task_validation"],"description":"Only source kinds with a current authoritative result path are admitted. Use manual for a genuinely human observation, and task_validation for an integrated check an agent can run and record through project.validation."},"expected_result":{"type":"string","minLength":1},"latest_result":string_or_null_schema(),"latest_result_id":string_or_null_schema(),"latest_result_digest":string_or_null_schema()}), &["id","description","required","source_kind","expected_result"])},"evidence_requirements":{"type":"array","items":object_schema(json!({"id":{"type":"string","minLength":1,"description":"Use the exact acceptance-check id this evidence proves."},"description":{"type":"string","minLength":1},"required":{"type":"boolean"},"evidence_kind":{"type":["string","null"],"enum":["screenshot","walkthrough_video","log","report","other",null]}}), &["id","description","required"])},"known_issues":string_array_schema(),"target_date":string_or_null_schema()},"additionalProperties":false}}),
            &["action", "expected_milestone_version"],
        ),
        PROJECT_EVIDENCE_OPERATION => described_object_schema(
            json!({"action":{"enum":["attach","capture"],"description":"attach binds an existing Project media asset (asset_id + checksum required). capture stores an artifact your own verification run produced and attaches it in the same call: supply exactly one of content or path, never asset_id/checksum."},"milestone_id":{"type":"string","minLength":1},"expected_milestone_version":{"type":"integer","minimum":1},"asset_id":{"type":["string","null"],"minLength":1,"description":"attach only: the existing Project media asset to bind."},"task_id":string_or_null_schema(),"source_validation_id":{"type":["string","null"],"minLength":1,"description":"The id of the recorded validation RESULT this artifact backs — the `validation_id`/result id returned by `project.validation` (`record`), never the acceptance-check id. Record the check result first, then capture its proof citing that returned id."},"acceptance_check_ids":string_array_schema(),"caption":{"type":"string","minLength":1},"kind":{"type":"string","enum":["screenshot","walkthrough_video","log","report","other"]},"checksum":{"type":["string","null"],"minLength":1,"description":"attach only: SHA-256 of the named asset's bytes."},"content":{"type":["string","null"],"minLength":1,"description":"capture only: inline text artifact (for example a verification report)."},"path":{"type":["string","null"],"minLength":1,"description":"capture only: a file path under your Project workspace root (forge/ or checkout/)."},"filename":{"type":["string","null"],"minLength":1,"description":"capture only: display filename override, no path separators."}}),
            &[
                "action",
                "milestone_id",
                "expected_milestone_version",
                "caption",
                "kind",
            ],
            "Evidence for a milestone acceptance check. attach an existing asset, or capture the proof your own verification run produced — the server creates the Project media asset and attaches it atomically. Never create a Task just to produce evidence.",
        ),
        PROJECT_VALIDATION_OPERATION => object_schema(
            json!({"action":{"const":"record"},"milestone_id":{"type":"string","minLength":1},"milestone_version":{"type":"integer","minimum":1},"check_id":{"type":"string","minLength":1,"description":"Stable acceptance-check id on the milestone's current definition revision. The check's source_kind must not be manual."},"definition_revision_id":{"type":"string","minLength":1},"status":{"type":"string","enum":["pass","fail","blocked","stale","unavailable"]},"result":{"type":"string","minLength":1,"description":"What was actually observed, in the Agent's own words."},"input_digest":{"type":"string","minLength":1,"description":"Digest of the exact inputs observed, so a repeat of the same observation replays instead of appending."},"observed_command_ids":{"type":"array","items":{"type":"string","minLength":1},"description":"The observation_id values forge_task_command returned for the commands you ran in your checkout to settle this check. Required for a pass or fail: the server verifies each one is a command you ran in this Project after the delivered Task landed. A Task's or reviewer's report is narration and cannot stand in for one."},"observed_task_id":{"type":["string","null"],"minLength":1,"description":"The delivered Task whose result you exercised, for traceability only; it settles nothing by itself. Never name a Task that did not actually run: the server verifies it belongs to this Project and reached a delivered state."},"evidence_asset_id":{"type":["string","null"],"minLength":1,"description":"Optional id of an artifact backing this observation: one the observed Task captured through `task.evidence`, or one you captured yourself through `project.evidence` (`capture`)."},"governing_revision_ids":string_array_schema()}),
            &[
                "action",
                "milestone_id",
                "milestone_version",
                "check_id",
                "definition_revision_id",
                "status",
                "result",
                "input_digest",
            ],
        ),
        PROJECT_READINESS_OPERATION => object_schema(
            json!({"action":{"const":"evaluate"},"milestone_id":{"type":"string","minLength":1},"milestone_version":{"type":"integer","minimum":1}}),
            &["action", "milestone_id", "milestone_version"],
        ),
        PROJECT_RELEASE_OPERATION => described_object_schema(
            json!({
                "action":{"const":"propose_candidate"},
                "milestone_id":{"type":"string","minLength":1},
                "milestone_version":{"type":"integer","minimum":1},
                "readiness_snapshot_id":{"type":"string","minLength":1},
                "readiness_digest":{"type":"string","minLength":1}
            }),
            &[
                "action",
                "milestone_id",
                "milestone_version",
                "readiness_snapshot_id",
                "readiness_digest",
            ],
            "Project Agent release candidate only. Invoke this only for an exact current ReadinessSnapshot whose result is ready. A blocked, failed, or stale snapshot must be reported with every canonical blocker and must never be described as a release proposal or as Known Issues: None. This submits a user-release request; it never approves, executes, or creates a final release manifest.",
        ),
        TASK_PROPOSE_OPERATION => described_object_schema(
            json!({
                "action":{"const":"create"},
                "title":{"type":"string","minLength":1},
                "description":{"type":"string","minLength":1},
                "priority":{"type":"integer"},
                "task_type":{"type":"string","enum":["task","planning_task","discovery"]},
                "plan_item_id":string_or_null_schema(),
                "milestone_id":string_or_null_schema(),
                "capability_class":string_or_null_schema(),
                "risk_class":string_or_null_schema(),
                "depends_on_task_ids":string_array_schema()
            }),
            &["action", "title"],
            "Create one Task in the authenticated Project scope. The server binds the current approved Charter, optional traceability references, permissions, and Task workflow state.",
        ),
        TASK_RECOVER_OPERATION => described_object_schema(
            json!({
                "action":{"type":"string","enum":["resume_session","reexecute","reset_to_initial","reset_retry_window","cancel_task"]},
                "task_id":{"type":"string","minLength":1},
                "reason":{"type":"string","minLength":1,"description":"What stopped this Task, in your own words."}
            }),
            &["action", "task_id", "reason"],
            "Recover a Task that stopped so it can run again, rather than creating a replacement. Use this when the Task definition is sound and the failure was operational -- an expired worker lease, a runtime that went away, a dispatch race, an exhausted retry window. `resume_session` continues the interrupted run; `reexecute` starts a fresh run of the same Task; `reset_to_initial` returns it to the queue for normal dispatch; `reset_retry_window` clears an exhausted retry budget; `cancel_task` ends a Task whose outcome no longer needs a run at all -- a verification-shaped Task you absorbed by settling its checks yourself. Replace a Task only when what it asks for is actually wrong: a duplicate leaves the original stranded and the work double-counted.",
        ),
        TASK_WORKLOG_OPERATION => described_object_schema(
            json!({
                "action":{"const":"append"},
                "kind":{"type":"string","enum":["progress","decision","validation","blocker"],"description":"progress: a meaningful slice landed. decision: a choice you made and why. validation: what you ran and what it reported. blocker: what stopped you and what you need."},
                "summary":{"type":"string","minLength":1,"maxLength":4000,"description":"One concise user-visible update in your own words."}
            }),
            &["action", "kind", "summary"],
            "Append one entry to this Task's worklog, which the next role reads in its dispatch context. Write after a meaningful milestone -- initial approach, a completed slice, validation results, a deviation, a blocker -- not for every tool call or file edit. Forge derives the Task, execution, role, and identity from your session and posts the final completion summary itself, so no separate handoff entry is needed. A worklog entry never moves the Task and never satisfies an acceptance check: proof of behaviour is a captured artifact, not prose.",
        ),
        TASK_EVIDENCE_OPERATION => described_object_schema(
            json!({
                "action":{"const":"capture"},
                "kind":{"type":"string","enum":["screenshot","walkthrough_video","log","report","other"]},
                "caption":{"type":"string","minLength":1,"description":"What this artifact shows, in your own words."},
                "path":{"type":["string","null"],"minLength":1,"description":"Workspace-relative path of a file this run produced (screenshot, recording, report). Omit when supplying inline content."},
                "content":{"type":["string","null"],"minLength":1,"description":"Verbatim captured output for a log or report -- the exact stdout/stderr you observed, not a summary. Omit when supplying a path."},
                "filename":{"type":["string","null"],"minLength":1,"description":"Display filename for inline content. Defaults to a name derived from the kind."}
            }),
            &["action", "kind", "caption"],
            "Capture one artifact this Task run actually produced and register it as authoritative evidence bound to this Task. Supply exactly one of `path` (a file in the Task workspace) or `content` (verbatim captured output). The server stores the bytes, computes the checksum, and returns the asset id a milestone evidence attachment references. Never describe an observation you did not make: this records what the run produced, not what you expect it to produce.",
        ),
        TASK_REVIEW_OPERATION => described_object_schema(
            json!({
                "task_id":{"type":"string","minLength":1},
                "decision":{"type":"string","enum":["accept","reject"]},
                "expected_task_version":{"type":"integer","minimum":1},
                "reason":string_or_null_schema()
            }),
            &["task_id", "decision", "expected_task_version"],
            "Accept or reject an exact human-required Task review in the bound Project. This uses the normal Task workflow, version, CI, and evidence checks and cannot review another Project.",
        ),
        TASK_ADAPTIVE_OPERATION => {
            let child = object_schema(
                json!({
                    "title":{"type":"string","minLength":1},
                    "description":string_or_null_schema(),
                    "assignee_id":string_or_null_schema()
                }),
                &["title"],
            );
            let split = object_schema(
                json!({
                    "action":{"const":"split"},
                    "source_task_id":{"type":"string","minLength":1},
                    "expected_task_version":{"type":"integer","minimum":1},
                    "expected_board_revision":{"type":"integer","minimum":0},
                    "rationale":{"type":"string","minLength":1},
                    "items":{"type":"array","minItems":1,"items":child}
                }),
                &[
                    "action",
                    "source_task_id",
                    "expected_task_version",
                    "expected_board_revision",
                    "rationale",
                    "items",
                ],
            );
            let sequence = object_schema(
                json!({
                    "action":{"const":"sequence"},
                    "source_task_id":{"type":"string","minLength":1},
                    "expected_task_version":{"type":"integer","minimum":1},
                    "expected_board_revision":{"type":"integer","minimum":0},
                    "rationale":{"type":"string","minLength":1},
                    "ordered_task_ids":{"type":"array","minItems":1,"items":{"type":"string","minLength":1}}
                }),
                &[
                    "action",
                    "source_task_id",
                    "expected_task_version",
                    "expected_board_revision",
                    "rationale",
                    "ordered_task_ids",
                ],
            );
            let replace = object_schema(
                json!({
                    "action":{"const":"replace"},
                    "source_task_id":{"type":"string","minLength":1},
                    "expected_task_version":{"type":"integer","minimum":1},
                    "expected_board_revision":{"type":"integer","minimum":0},
                    "rationale":{"type":"string","minLength":1},
                    "title":{"type":"string","minLength":1},
                    "description":string_or_null_schema()
                }),
                &[
                    "action",
                    "source_task_id",
                    "expected_task_version",
                    "expected_board_revision",
                    "rationale",
                    "title",
                ],
            );
            json!({
                "type":"object",
                "required":["action"],
                "properties":{
                    "action":{"type":"string","enum":["split","sequence","replace"]},
                    "source_task_id":{"type":"string","minLength":1},
                    "expected_task_version":{"type":"integer","minimum":1},
                    "expected_board_revision":{"type":"integer","minimum":0},
                    "rationale":{"type":"string","minLength":1},
                    "items":{"type":"array","minItems":1,"items":child},
                    "ordered_task_ids":{"type":"array","minItems":1,"items":{"type":"string","minLength":1}},
                    "title":{"type":"string","minLength":1},
                    "description":string_or_null_schema()
                },
                "oneOf":[split,sequence,replace],
                "additionalProperties":false,
                "description":"Bounded Project Task adaptive command. Forge derives Project, actor, permission, governance, and fixed execution boundaries from the authenticated binding and active baseline; those values cannot be supplied in the payload."
            })
        }
        _ => object_schema(json!({}), &[]),
    }
}

/// Adds `type`/`enum` beside every string `const` in a JSON schema (and a
/// `type` beside bare string enums). JSON Schema `const` alone falls outside
/// the OpenAPI-style subset some provider function-calling APIs accept —
/// Gemini drops the constraint entirely and models then emit `{}` for the
/// field — so every string const also carries the equivalent one-value enum.
pub(crate) fn portable_const_schema(mut schema: Value) -> Value {
    fn walk(value: &mut Value) {
        match value {
            Value::Object(map) => {
                if let Some(constant) = map.get("const").and_then(Value::as_str).map(str::to_owned)
                {
                    map.entry("type").or_insert_with(|| json!("string"));
                    map.entry("enum").or_insert_with(|| json!([constant]));
                } else if !map.contains_key("type") {
                    let all_strings =
                        map.get("enum")
                            .and_then(Value::as_array)
                            .is_some_and(|options| {
                                !options.is_empty() && options.iter().all(Value::is_string)
                            });
                    if all_strings {
                        map.insert("type".to_owned(), json!("string"));
                    }
                }
                for entry in map.values_mut() {
                    walk(entry);
                }
            }
            Value::Array(values) => values.iter_mut().for_each(walk),
            _ => {}
        }
    }
    walk(&mut schema);
    schema
}

/// Renders one JSON-schema node as a compact structural signature.
///
/// Provider function-calling APIs only reliably deliver flat object schemas,
/// so a nested payload contract cannot travel as a schema. It travels here
/// instead, in the payload description. Naming only the top-level required
/// keys is not enough: an operation like `charter.draft` carries a deeply
/// nested `content` object, and a model that cannot see those field names
/// has to rediscover them one rejected call at a time.
fn schema_signature(schema: &Value, depth: usize) -> String {
    const MAX_DEPTH: usize = 8;
    if depth > MAX_DEPTH {
        return "object".to_owned();
    }
    if let Some(variants) = schema.get("oneOf").and_then(Value::as_array) {
        return variants
            .iter()
            .map(|variant| schema_signature(variant, depth))
            .collect::<Vec<_>>()
            .join("|");
    }
    if let Some(constant) = schema.get("const").and_then(Value::as_str) {
        return format!("\"{constant}\"");
    }
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        return values
            .iter()
            .filter_map(Value::as_str)
            .collect::<Vec<_>>()
            .join("|");
    }
    let type_names: Vec<&str> = match schema.get("type") {
        Some(Value::String(name)) => vec![name.as_str()],
        Some(Value::Array(names)) => names.iter().filter_map(Value::as_str).collect(),
        _ => Vec::new(),
    };
    let nullable = type_names.contains(&"null");
    let Some(primary) = type_names
        .iter()
        .find(|name| **name != "null")
        .copied()
        .or_else(|| type_names.is_empty().then_some("object"))
    else {
        // A `null`-only node (the null arm of a `oneOf`) carries no shape.
        return "null".to_owned();
    };
    let rendered = match primary {
        "object" => {
            let Some(properties) = schema.get("properties").and_then(Value::as_object) else {
                return if nullable {
                    "object|null".to_owned()
                } else {
                    "object".to_owned()
                };
            };
            let required: Vec<&str> = schema
                .get("required")
                .and_then(Value::as_array)
                .map(|values| values.iter().filter_map(Value::as_str).collect())
                .unwrap_or_default();
            let fields = properties
                .iter()
                .map(|(name, property)| {
                    // `?` marks an optional field so the model can tell what it
                    // must supply from what it may omit.
                    let optional = if required.contains(&name.as_str()) {
                        ""
                    } else {
                        "?"
                    };
                    format!(
                        "{name}{optional}: {}",
                        schema_signature(property, depth + 1)
                    )
                })
                .collect::<Vec<_>>()
                .join(", ");
            format!("{{{fields}}}")
        }
        "array" => {
            let items = schema
                .get("items")
                .map(|items| schema_signature(items, depth + 1))
                .unwrap_or_else(|| "any".to_owned());
            format!("[{items}]")
        }
        other => other.to_owned(),
    };
    if nullable {
        format!("{rendered}|null")
    } else {
        rendered
    }
}

/// Summarizes one operation's payload contract as a guidance line for the
/// declared tool schema. Handles both single-variant payload schemas and
/// `oneOf` payloads (e.g. document draft vs. approval).
pub(crate) fn orchestration_payload_summary(schema: &Value) -> String {
    fn variant_summary(variant: &Value) -> Option<String> {
        let properties = variant.get("properties")?;
        let action = properties.get("action").map(|action| {
            action
                .get("const")
                .and_then(Value::as_str)
                .map(str::to_owned)
                .or_else(|| {
                    action.get("enum").and_then(Value::as_array).map(|values| {
                        values
                            .iter()
                            .filter_map(Value::as_str)
                            .collect::<Vec<_>>()
                            .join("|")
                    })
                })
                .unwrap_or_default()
        });
        let required = variant
            .get("required")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .collect::<Vec<_>>()
                    .join(", ")
            })
            .unwrap_or_default();
        let shape = schema_signature(variant, 0);
        Some(match action {
            Some(action) if !action.is_empty() => {
                format!("action={action}; required: [{required}]; shape: {shape}")
            }
            _ => format!("required: [{required}]; shape: {shape}"),
        })
    }
    if let Some(variants) = schema.get("oneOf").and_then(Value::as_array) {
        return variants
            .iter()
            .filter_map(variant_summary)
            .collect::<Vec<_>>()
            .join(" OR ");
    }
    variant_summary(schema).unwrap_or_default()
}

/// Payload guidance for the generic coordination proposal surface. The
/// payload stays a free-form object at the schema level (provider
/// function-calling APIs only reliably deliver flat object schemas), so the
/// contract for operations with a typed server-side executor is carried in
/// the property description; the exact shape stays enforced server-side and
/// errors return to the model in-turn.
pub(crate) fn coordination_payload_guidance(operations: &BTreeSet<String>) -> String {
    let mut lines = Vec::new();
    if operations.contains("task.propose") {
        lines.push(concat!(
            "task.propose — create a Task in the bound Project. Fields: ",
            "title (required); description (outcome plus acceptance criteria); ",
            "priority (integer, higher runs sooner); ",
            "task_type (\"task\" implementation default, \"planning_task\", or \"discovery\"); ",
            "plan_item_id (optional traceability to a stable item in the active execution ",
            "baseline, for example \"pi-2\"; when supplied, it must exist and may have only ",
            "one non-cancelled Task); milestone_id (optional traceability, defaulting to the ",
            "active baseline's primary milestone when one exists); ",
            "capability_class (when supplied it must be one of the server-approved profiles: ",
            "\"repository_read\", \"repository_write\", \"read_only\", \"discovery_read\", ",
            "\"planning_read\" — and, when the baseline declares allowed classes, also one of ",
            "those); risk_class (only when the baseline declares allowed classes). ",
            "depends_on_task_ids (optional accepted Task ids in this Project; each prerequisite ",
            "must reach done before this Task can dispatch). ",
            "Forge binds implementation authority to the Project's current approved Charter. ",
            "Baseline references are optional traceability; never echo baseline ids or digests.",
        ));
    }
    if operations.contains(TASK_ADAPTIVE_OPERATION) {
        lines.push(concat!(
            "task.adaptive — apply one bounded adaptive command to a Task in the bound Project. ",
            "Fields: action (required: \"split\", \"sequence\", or \"replace\"); ",
            "source_task_id, expected_task_version, expected_board_revision, and rationale ",
            "(all required); split requires non-empty items of {title, description?, assignee_id?}; ",
            "sequence requires non-empty ordered_task_ids; replace requires title and optional description. ",
            "Project, scope, actor, permission, governance, and fixed-boundary values are server-derived ",
            "and unknown fields are rejected."
        ));
    }
    if operations.contains(TASK_RECOVER_OPERATION) {
        lines.push(concat!(
            "task.recover — repair a Task that stopped, instead of replacing it. ",
            "Fields: task_id (required, a Task in this Project); ",
            "reason (required: state what stopped it); ",
            "action (required: \"resume_session\", \"reexecute\", \"reset_to_initial\", ",
            "\"reset_retry_window\", or \"cancel_task\"). ",
            "Use cancel_task for a Task that cannot succeed as specified — a read-only ",
            "task_type whose work needs repository writes will fail every retry, and ",
            "cancelling it is the remedy rather than proposing a duplicate."
        ));
    }
    if lines.is_empty() {
        String::new()
    } else {
        format!(
            "Payload shape by operation (exact contract enforced server-side):\n- {}",
            lines.join("\n- ")
        )
    }
}

/// Declared payload properties for the generic coordination proposal
/// surface. Several provider function-calling APIs (notably Gemini) surface
/// only declared properties to the model, so the `task.propose` field
/// contract — including optional `plan_item_id` traceability — must be visible as real schema properties,
/// not only as description prose. The payload deliberately keeps
/// `additionalProperties` open because every admitted operation shares this
/// one envelope; each description names its owning operation.
pub(crate) fn coordination_payload_properties(operations: &BTreeSet<String>) -> Option<Value> {
    if !operations.contains("task.propose")
        && !operations.contains(TASK_ADAPTIVE_OPERATION)
        && !operations.contains(TASK_RECOVER_OPERATION)
    {
        return None;
    }
    let mut properties = json!({
        "title": {
            "type": ["string", "null"],
            "description": "task.propose: required Task title."
        },
        "description": {
            "type": ["string", "null"],
            "description": "task.propose: outcome plus acceptance criteria."
        },
        "priority": {
            "type": ["integer", "null"],
            "description": "task.propose: integer; higher runs sooner."
        },
        "task_type": {
            "type": ["string", "null"],
            "description": "task.propose: \"task\" (implementation default), \"planning_task\", or \"discovery\"."
        },
        "plan_item_id": {
            "type": ["string", "null"],
            "description": concat!(
                "task.propose: optional traceability to a stable plan-item id in the ",
                "Project's active execution baseline (for example \"pi-2\"). When supplied, ",
                "the id must exist in that baseline. Each plan item admits exactly one ",
                "non-cancelled Task — never re-propose a plan item that already has one."
            )
        },
        "milestone_id": {
            "type": ["string", "null"],
            "description": "task.propose: optional; defaults to the active baseline's primary milestone."
        },
        "task_id": {
            "type": ["string", "null"],
            "description": "task.recover: required Task id in this Project."
        },
        "reason": {
            "type": ["string", "null"],
            "description": "task.recover: required; state what stopped this Task."
        },
        "action": {
            "type": ["string", "null"],
            "description": concat!(
                "task.recover: required, one of \"resume_session\", \"reexecute\", ",
                "\"reset_to_initial\", \"reset_retry_window\", \"cancel_task\". Also the ",
                "task.adaptive action field."
            )
        },
        "capability_class": {
            "type": ["string", "null"],
            "description": "task.propose: optional server-approved capability profile (e.g. \"repository_write\", \"repository_read\")."
        },
        "risk_class": {
            "type": ["string", "null"],
            "description": "task.propose: optional; only when the baseline declares allowed risk classes."
        },
        "depends_on_task_ids": {
            "type": ["array", "null"],
            "items": {"type": "string", "minLength": 1},
            "description": "task.propose: optional accepted Task ids in this Project; every prerequisite must reach done before dispatch. Use this for implementation-before-verification ordering instead of narration."
        }
    });
    if operations.contains(TASK_ADAPTIVE_OPERATION) {
        properties["action"] = json!({
            "type": ["string", "null"],
            "description": "task.adaptive: split, sequence, or replace."
        });
        properties["source_task_id"] = json!({
            "type": ["string", "null"],
            "description": "task.adaptive: source Task id in the bound Project."
        });
        properties["expected_task_version"] = json!({
            "type": ["integer", "null"],
            "description": "task.adaptive: source Task version precondition."
        });
        properties["expected_board_revision"] = json!({
            "type": ["integer", "null"],
            "description": "task.adaptive: Project board revision precondition."
        });
        properties["rationale"] = json!({
            "type": ["string", "null"],
            "description": "task.adaptive: bounded command rationale."
        });
        properties["items"] = json!({
            "type": ["array", "null"],
            "description": "task.adaptive split: non-empty child list; each child has title and optional description/assignee_id.",
            "items": orchestration_payload_schema(TASK_ADAPTIVE_OPERATION)["properties"]["items"]["items"].clone()
        });
        properties["ordered_task_ids"] = json!({
            "type": ["array", "null"],
            "description": "task.adaptive sequence: non-empty ordered Task ids.",
            "items": {"type":"string","minLength":1}
        });
    }
    Some(properties)
}

// The declared envelope is a plain `type: object` schema: several provider
// function-calling APIs (notably Gemini) accept only an OpenAPI-style object
// at the parameters root and silently drop `oneOf`, leaving the model blind
// to the envelope — it then emits flattened payload fields and the whole
// attempt dies at provider-side schema validation. Per-operation payload
// shapes are summarized in the description instead, and the exact contracts
// stay enforced by `validate_orchestration_proposal_arguments` plus the
// server-side validators, whose errors return to the model in-turn.
pub(crate) fn orchestration_proposal_schema(operations: &BTreeSet<String>) -> Value {
    let mut guidance =
        vec!["Payload shape by operation (exact contract enforced server-side):".to_owned()];
    for operation in operations {
        let summary = orchestration_payload_summary(&orchestration_payload_schema(operation));
        guidance.push(format!("- {operation}: {summary}"));
    }
    json!({
        "type":"object",
        "required":["operation","payload","dedupe_key","correlation_id"],
        "properties":{
            "operation":{"type":"string","enum":operations.iter().collect::<Vec<_>>()},
            "payload":{"type":"object","description":guidance.join("\n")},
            "dedupe_key":{"type":"string","minLength":1},
            "correlation_id":{"type":"string","minLength":1},
            "causation_id":string_or_null_schema(),
            "causation_depth":{"type":"integer","minimum":0,"maximum":8},
        },
        "additionalProperties":false,
    })
}

pub(crate) fn orchestration_read_arguments_schema(operation: &str) -> Value {
    match operation {
        MAIN_GENESIS_PROJECT_AGENTS_READ_OPERATION => described_object_schema(
            json!({"genesis_session_id":string_or_null_schema()}),
            &[],
            "List the exact account-owned Project Agent identities eligible for explicit selection in the active Product Genesis session, plus the currently persisted preference and resolved approval selection.",
        ),
        MAIN_INQUIRY_RUN_OPERATION => described_object_schema(
            json!({
                "title":{"type":"string","minLength":1,"maxLength":120},
                "question":{"type":"string","minLength":1,"maxLength":4000},
                "context":{"type":["string","null"],"maxLength":8000}
            }),
            &["title", "question"],
            "Dispatch one ephemeral read-only sub-agent to answer a bounded question, and wait for its findings. Use this for a research excursion whose working material you do not want to carry for the rest of this conversation -- reading across many Projects, reconciling a long event history, comparing options. The sub-agent gets the account read surface and its own scratch directory; it cannot propose anything, touch a repository, or dispatch further sub-agents. `title` is what the user sees in the inquiry list while it runs, so name the question, not the activity. `question` is the sub-agent's entire brief: it does not see this conversation, so state everything it needs. Put supporting material in `context`. You get back a bounded abstract plus the path to the sub-agent's full findings file, which you can read with the file tools if the abstract is not enough.",
        ),
        PROJECT_OBSERVATIONS_OPERATION => described_object_schema(
            json!({
                "task_id":string_or_null_schema(),
                "limit":{"type":["integer","null"],"minimum":1,"maximum":50}
            }),
            &[],
            "Read what Task runs in this Project actually reported: worklog entries with the execution and role that wrote them, and the artifacts those runs captured. Captured text (a log or report) is returned inline so you can read what was observed; binary artifacts return their metadata and asset id. Use this to check an observation before citing it in `project.validation`, and to decide whether an outcome needs a corrective Task.",
        ),
        MAIN_CHARTER_READ_OPERATION => object_schema(
            json!({"charter_id":string_or_null_schema(),"revision_id":string_or_null_schema(),"genesis_session_id":string_or_null_schema()}),
            &[],
        ),
        MAIN_CHARTER_READINESS_OPERATION => object_schema(
            json!({
                "genesis_session_id":string_or_null_schema(),
                "charter_id":{"type":"string","minLength":1},
                "revision_id":{"type":"string","minLength":1},
                "content_digest":{"type":"string","minLength":1},
                "render_digest":{"type":"string","minLength":1},
                "expected_charter_version":{"type":"integer","minimum":1}
            }),
            &[
                "charter_id",
                "revision_id",
                "content_digest",
                "render_digest",
                "expected_charter_version",
            ],
        ),
        MAIN_CHARTER_DIFF_OPERATION => object_schema(
            json!({
                "genesis_session_id":string_or_null_schema(),
                "charter_id":{"type":"string","minLength":1},
                "base_revision_id":{"type":"string","minLength":1},
                "candidate_revision_id":{"type":"string","minLength":1}
            }),
            &["charter_id", "base_revision_id", "candidate_revision_id"],
        ),
        MAIN_CHARTER_APPROVAL_TARGET_OPERATION => object_schema(
            json!({
                "genesis_session_id":string_or_null_schema(),
                "charter_id":{"type":"string","minLength":1},
                "revision_id":{"type":"string","minLength":1},
                "content_digest":{"type":"string","minLength":1},
                "render_digest":{"type":"string","minLength":1},
                "expected_charter_version":{"type":"integer","minimum":1}
            }),
            &[
                "charter_id",
                "revision_id",
                "content_digest",
                "render_digest",
                "expected_charter_version",
            ],
        ),
        PROJECT_CURRENT_STATE_OPERATION => described_object_schema(
            json!({
                "limit":{"type":"integer","minimum":1,"maximum":64}
            }),
            &[],
            "Returns the server-derived closed EffectiveProjectState projection for the bound Project, including Charter/baseline references, approved Documents, Decisions, reconciliation/conflict records, Task/validation summaries, milestones/readiness, releases, unreleased changes, and source watermark/version. The response is scope-bound and never accepts a Project or authority selector.",
        ),
        PROJECT_CHARTER_READ_OPERATION => described_object_schema(
            json!({}),
            &[],
            "Returns the bound Project's current approved Charter as rendered Markdown plus its charter/revision identifiers and content/render digests. The Charter is Project data, not resident context: read it whenever its details matter to a decision. The response is scope-bound and never accepts a Project selector.",
        ),
        PROJECT_SKILL_SECTION_OPERATION => described_object_schema(
            json!({
                "section":{"type":"string","enum":PROJECT_SKILL_SECTION_NAMES}
            }),
            &["section"],
            "Returns one server-owned Project operating-doctrine section by name. Read the matching section before the first work of that kind in a conversation and re-read it when unsure: research, documents, scope_change, tasks, milestones, release.",
        ),
        _ => object_schema(json!({}), &[]),
    }
}

pub(crate) fn orchestration_read_schema(operations: &BTreeSet<String>) -> Value {
    let mut guidance = vec!["Arguments by operation:".to_owned()];
    for operation in operations {
        let arguments = orchestration_read_arguments_schema(operation);
        let keys = arguments
            .get("properties")
            .and_then(Value::as_object)
            .map(|map| map.keys().cloned().collect::<Vec<_>>().join(", "))
            .unwrap_or_default();
        guidance.push(if keys.is_empty() {
            format!("- {operation}: no arguments")
        } else {
            format!("- {operation}: optional {{{keys}}}")
        });
    }
    json!({
        "type":"object",
        "required":["operation"],
        "properties":{
            "operation":{"type":"string","enum":operations.iter().collect::<Vec<_>>()},
            "arguments":{"type":"object","description":guidance.join("\n")},
        },
        "additionalProperties":false,
    })
}
/// Apply the closed discriminant portion of the named orchestration schema at
/// preparation time as well as in the provider.  Runtime model tool calls do
/// not necessarily pass through a JSON-Schema validator, so accepting a
/// mismatched operation/action pair here would make the registry appear more
/// permissive than its advertised contract.
pub(crate) fn validate_orchestration_proposal_arguments(
    operation: &str,
    arguments: &Value,
) -> Result<(), RuntimeError> {
    let object = arguments
        .as_object()
        .ok_or_else(|| RuntimeError::tool("Forge orchestration proposal must be an object"))?;
    const ALLOWED_FIELDS: &[&str] = &[
        "operation",
        "payload",
        "dedupe_key",
        "correlation_id",
        "causation_id",
        "causation_depth",
    ];
    if let Some(field) = object
        .keys()
        .find(|field| !ALLOWED_FIELDS.contains(&field.as_str()))
    {
        return Err(RuntimeError::tool(format!(
            "Forge orchestration proposal field `{field}` is not admitted"
        )));
    }
    let payload = object
        .get("payload")
        .and_then(Value::as_object)
        .ok_or_else(|| RuntimeError::tool("Forge orchestration payload must be an object"))?;
    let action = payload
        .get("action")
        .and_then(Value::as_str)
        .ok_or_else(|| RuntimeError::tool("Forge orchestration payload action is required"))?;
    let schema = orchestration_payload_schema(operation);
    let action_schema = schema
        .get("properties")
        .and_then(|properties| properties.get("action"))
        .ok_or_else(|| RuntimeError::tool("Forge orchestration action schema is unavailable"))?;
    if let Some(expected) = action_schema.get("const").and_then(Value::as_str) {
        if action != expected {
            return Err(RuntimeError::tool(format!(
                "Forge orchestration action must be {expected}"
            )));
        }
    } else if let Some(actions) = action_schema.get("enum").and_then(Value::as_array) {
        if !actions
            .iter()
            .any(|candidate| candidate.as_str() == Some(action))
        {
            return Err(RuntimeError::tool(
                "Forge orchestration action is outside this typed contract",
            ));
        }
    } else {
        return Err(RuntimeError::tool(
            "Forge orchestration action schema is not closed",
        ));
    }
    if let Some(value) = object.get("causation_id") {
        if !value.is_null() && !value.is_string() {
            return Err(RuntimeError::tool("causation_id must be a string or null"));
        }
    }
    if let Some(value) = object.get("causation_depth") {
        let depth = value
            .as_i64()
            .ok_or_else(|| RuntimeError::tool("causation_depth must be an integer"))?;
        if !(0..=8).contains(&depth) {
            return Err(RuntimeError::tool(
                "causation_depth must be between 0 and 8",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::operation_catalog::{
        MIGRATED_OPERATION_CONTRACTS, OperationInputContract, OperationOutputContract,
        SHARED_ORCHESTRATION_OUTCOME,
    };

    #[test]
    fn every_migrated_contract_resolves_to_a_closed_input_and_shared_output() {
        for contract in MIGRATED_OPERATION_CONTRACTS {
            let schema = match contract.input {
                OperationInputContract::ReadArguments => {
                    orchestration_read_arguments_schema(contract.operation)
                }
                OperationInputContract::ProposalEnvelope
                | OperationInputContract::CoordinationEnvelope => {
                    orchestration_payload_schema(contract.operation)
                }
            };

            assert_eq!(schema.get("type").and_then(Value::as_str), Some("object"));
            assert_eq!(
                schema.get("additionalProperties").and_then(Value::as_bool),
                Some(false),
                "{} must remain a closed input contract",
                contract.operation
            );
            // Every contract must resolve past the unhandled-operation
            // fallthrough: either operation-specific properties or an
            // explicit root description marking a deliberate zero-argument
            // read (the fallthrough produces neither).
            let has_properties = schema
                .get("properties")
                .and_then(Value::as_object)
                .is_some_and(|properties| !properties.is_empty());
            let has_description = schema
                .get("description")
                .and_then(Value::as_str)
                .is_some_and(|description| !description.trim().is_empty());
            assert!(
                has_properties || has_description,
                "{} must resolve to operation-specific input metadata",
                contract.operation
            );
            assert_eq!(
                contract.output,
                OperationOutputContract {
                    envelope: "api_types::OrchestrationOutcome",
                    in_band_errors: true,
                    replay_field: "replayed",
                }
            );
            assert_eq!(contract.output, SHARED_ORCHESTRATION_OUTCOME);
        }
    }

    #[test]
    fn milestone_contract_exposes_exact_acceptance_evidence_shape() {
        let milestone = orchestration_payload_schema(PROJECT_MILESTONE_OPERATION);
        let check = &milestone["properties"]["content"]["properties"]["acceptance_checks"]["items"];
        assert_eq!(
            check["properties"]["source_kind"]["enum"],
            json!(["manual", "policy_waiver", "task_validation"])
        );
        let evidence =
            &milestone["properties"]["content"]["properties"]["evidence_requirements"]["items"];
        assert_eq!(
            evidence["properties"]["evidence_kind"]["enum"],
            json!([
                "screenshot",
                "walkthrough_video",
                "log",
                "report",
                "other",
                null
            ])
        );
    }

    #[test]
    fn genesis_start_contract_is_closed_and_carries_no_source_authority() {
        let schema = orchestration_payload_schema(MAIN_GENESIS_START_OPERATION);
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(schema["properties"]["action"]["const"], "start");
        let properties = schema["properties"].as_object().expect("properties");
        assert_eq!(
            properties.keys().cloned().collect::<Vec<_>>(),
            [
                "action".to_owned(),
                "maturity".to_owned(),
                "preferred_project_agent_identity_id".to_owned(),
            ]
        );
        for forbidden in [
            "account_id",
            "chat_id",
            "initial_idea",
            "source_message_id",
            "source_turn_id",
        ] {
            assert!(!properties.contains_key(forbidden));
        }
    }

    #[test]
    fn project_agent_selection_contract_is_structured_and_has_no_charter_prose() {
        let read = orchestration_read_arguments_schema(MAIN_GENESIS_PROJECT_AGENTS_READ_OPERATION);
        assert_eq!(read["additionalProperties"], false);
        assert!(read["properties"].get("genesis_session_id").is_some());

        let select = orchestration_payload_schema(MAIN_GENESIS_PROJECT_AGENT_SELECT_OPERATION);
        assert_eq!(select["additionalProperties"], false);
        assert_eq!(select["properties"]["action"]["const"], "select");
        assert_eq!(
            select["required"],
            json!([
                "action",
                "expected_session_version",
                "project_agent_identity_id"
            ])
        );
        let properties = select["properties"].as_object().expect("properties");
        assert!(!properties.contains_key("charter_content"));
        assert!(!properties.contains_key("charter_prose"));
    }

    /// An operation constant that is not imported becomes a binding pattern in
    /// `match`, silently swallowing every arm after it -- the payload schema
    /// for one operation is then served for all of them. Rust only warns. This
    /// asserts distinct operations keep distinct schemas, which a wildcard
    /// cannot satisfy.
    #[test]
    fn every_operation_resolves_its_own_payload_schema() {
        let cases = [
            (TASK_ADAPTIVE_OPERATION, "split"),
            (TASK_RECOVER_OPERATION, "resume_session"),
            (TASK_WORKLOG_OPERATION, "append"),
            (TASK_EVIDENCE_OPERATION, "capture"),
            (PROJECT_VALIDATION_OPERATION, "record"),
        ];
        for (operation, expected_action) in cases {
            let schema = orchestration_payload_schema(operation);
            let action = &schema["properties"]["action"];
            let admits = action
                .get("const")
                .and_then(Value::as_str)
                .map(|value| value == expected_action)
                .unwrap_or_else(|| {
                    action["enum"]
                        .as_array()
                        .is_some_and(|values| values.iter().any(|v| v == expected_action))
                });
            assert!(
                admits,
                "{operation} resolved a schema whose action does not admit \
                 `{expected_action}` -- an unimported constant is matching everything"
            );
        }
    }

    #[test]
    fn adaptive_contract_is_closed_and_has_exactly_three_action_variants() {
        let schema = orchestration_payload_schema(TASK_ADAPTIVE_OPERATION);
        assert_eq!(schema["additionalProperties"], false);
        assert_eq!(
            schema["properties"]["action"]["enum"],
            json!(["split", "sequence", "replace"])
        );
        let variants = schema["oneOf"].as_array().expect("adaptive variants");
        assert_eq!(variants.len(), 3);
        for variant in variants {
            assert_eq!(variant["additionalProperties"], false);
        }
        let properties = schema["properties"].as_object().expect("properties");
        for forbidden in [
            "project_id",
            "scope_id",
            "actor_id",
            "governance",
            "fixed_boundary_digest",
        ] {
            assert!(!properties.contains_key(forbidden));
        }
    }

    #[test]
    fn adaptive_guidance_and_flat_properties_are_declared_for_coordination() {
        let operations = [TASK_ADAPTIVE_OPERATION.to_owned()]
            .into_iter()
            .collect::<BTreeSet<_>>();
        let guidance = coordination_payload_guidance(&operations);
        assert!(guidance.contains("task.adaptive"));
        assert!(guidance.contains("split"));
        let properties = coordination_payload_properties(&operations).expect("flat properties");
        assert!(properties.get("action").is_some());
        assert!(properties.get("source_task_id").is_some());
        assert!(properties.get("ordered_task_ids").is_some());
    }
}
