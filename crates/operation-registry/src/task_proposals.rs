//! Native Task commands. Task actions share the public typed verb input;
//! domain transactions retain offer eligibility, governance, CAS and receipts.
use crate::*;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Action {
    #[schemars(length(min = 1))]
    pub task_id: String,
    pub action: api_types::TaskAction,
    #[schemars(range(min = 1))]
    pub version: i64,
}
#[derive(Debug, Deserialize, Serialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Proposal {
    #[schemars(length(min = 1))]
    pub title: String,
    /// Outcome, acceptance criteria, and owned repository-relative paths. Give parallel Tasks disjoint files; use dependencies to order shared-file edits.
    pub description: Option<String>,
    pub parent_task_id: Option<String>,
    pub priority: Option<i64>,
    pub task_type: Option<String>,
    pub task_state_config: Option<String>,
    pub review_requirement_ids: Vec<String>,
    pub merge_config: Option<Value>,
    pub role_assignments: Option<Vec<api_types::InitialRoleAssignment>>,
    pub governance: Option<api_types::TaskGovernanceRequest>,
    pub plan_item_id: Option<String>,
    pub milestone_id: Option<String>,
    pub capability_class: Option<String>,
    pub risk_class: Option<String>,
    /// Optional prerequisite DAG edges only: not a parent/child hierarchy or workspace sharing.
    #[serde(default)]
    pub depends_on_task_ids: Vec<String>,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum Dependency {
    Add {
        #[schemars(length(min = 1))]
        task_id: String,
        #[schemars(length(min = 1))]
        depends_on_task_id: String,
        #[schemars(length(min = 1))]
        rationale: String,
    },
    Remove {
        #[schemars(length(min = 1))]
        task_id: String,
        #[schemars(length(min = 1))]
        depends_on_task_id: String,
        #[schemars(length(min = 1))]
        rationale: String,
    },
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct AdaptiveChild {
    #[schemars(length(min = 1))]
    pub title: String,
    pub description: Option<String>,
    pub assignee_id: Option<String>,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum Adaptive {
    /// split creates ordered direct children under a non-executing coordination root.
    Split {
        #[schemars(length(min = 1))]
        source_task_id: String,
        #[schemars(range(min = 1))]
        expected_task_version: i64,
        #[schemars(range(min = 0))]
        expected_board_revision: i64,
        #[schemars(length(min = 1))]
        rationale: String,
        #[schemars(length(min = 1))]
        items: Vec<AdaptiveChild>,
    },
    /// sequence changes child order only; it does not create a dependency graph.
    Sequence {
        #[schemars(length(min = 1))]
        source_task_id: String,
        #[schemars(range(min = 1))]
        expected_task_version: i64,
        #[schemars(range(min = 0))]
        expected_board_revision: i64,
        #[schemars(length(min = 1))]
        rationale: String,
        #[schemars(length(min = 1))]
        ordered_task_ids: Vec<String>,
    },
    Replace {
        #[schemars(length(min = 1))]
        source_task_id: String,
        #[schemars(range(min = 1))]
        expected_task_version: i64,
        #[schemars(range(min = 0))]
        expected_board_revision: i64,
        #[schemars(length(min = 1))]
        rationale: String,
        #[schemars(length(min = 1))]
        title: String,
        description: Option<String>,
    },
}
#[async_trait::async_trait]
pub trait TaskProposalContext<E>: Send + Sync {
    async fn task_action(&self, input: Action) -> Result<Value, E>;
    async fn task_propose(&self, input: Proposal) -> Result<Value, E>;
    async fn task_dependency(&self, input: Dependency) -> Result<Value, E>;
    async fn task_adaptive(&self, input: Adaptive) -> Result<Value, E>;
}
pub const IDS: &[&str] = &[
    "task.action",
    "task.propose",
    "task.dependency",
    "task.adaptive",
];
const AUTHORITY: AuthorityRule = AuthorityRule {
    principal: authority::PrincipalRule::ProjectAgent,
    permissions: &[("project", "propose_task"), ("agent_chat", "propose_task")],
    binding: "bound Project Agent",
};
const SURFACES: &[SurfaceBinding] = &[SurfaceBinding {
    native_aggregate: "forge_scope_propose",
    projection: FieldProjection::ProposalPayload,
}];
const CONSTRAINTS: &[StructuralConstraint] = &[
    StructuralConstraint::ClosedObject,
    StructuralConstraint::MaxSerializedBytes(65536),
];
pub fn specs<E: Send + 'static>() -> Vec<OperationSpec<E>> {
    vec![
        OperationSpec::typed("task.action", AUTHORITY, EffectClass::DirectCommand, AvailabilityRule::ReadyOnly, SURFACES,
            "Apply one current available_actions offer at its Task version.",
            "Copy the verb and parameter choices from work.read. The Task action system alone owns state eligibility; no agent override=true.",
            CONSTRAINTS, |c, i| Box::pin(c.task_action(i))),
        OperationSpec::typed("task.propose", AUTHORITY, EffectClass::DirectCommand, AvailabilityRule::ReadyOnly, SURFACES,
            "Create one Charter-bound Task with explicit requirement ownership and optional traceability.",
            "Copy review_requirement_ids verbatim from project.charter, or [] for none. Name owned repository-relative paths, give parallel Tasks disjoint files, and use dependencies to order shared-file edits. Depends-on IDs are prerequisite DAG edges only, not a parent/child hierarchy or workspace sharing. Configurations and assignments use their closed domain types.",
            CONSTRAINTS, |c, i| Box::pin(c.task_propose(i))),
        OperationSpec::typed("task.dependency", AUTHORITY, EffectClass::DirectCommand, AvailabilityRule::ReadyOnly, SURFACES,
            "Add or remove one same-Project prerequisite edge.", "Both Tasks must belong to this Project; a dependency does not create parentage or workspace sharing.",
            CONSTRAINTS, |c, i| Box::pin(c.task_dependency(i))),
        OperationSpec::typed("task.adaptive", AUTHORITY, EffectClass::DirectCommand, AvailabilityRule::ReadyOnly, SURFACES,
            "Split, sequence or replace work inside the approved adaptive envelope.",
            "Read Task version and board_revision from work.read. split creates ordered direct children under a non-executing coordination root. Children share the root workspace but keep independent agents, executions, and lifecycles and execute serially. sequence changes child order only; it does not create a dependency graph.",
            CONSTRAINTS, |c, i| Box::pin(c.task_adaptive(i))),
    ]
}

pub fn action_parameters_allowed(
    offer: &api_types::Offer,
    action: &api_types::TaskAction,
    owner: bool,
) -> bool {
    use api_types::TaskAction;
    let supports = |name: &str| {
        offer
            .parameters
            .iter()
            .any(|parameter| parameter.name == name)
    };
    let allows_bool = |name: &str, value: bool| {
        offer
            .parameters
            .iter()
            .find(|parameter| parameter.name == name)
            .and_then(|parameter| parameter.boolean_values.as_ref())
            .is_some_and(|values| values.contains(&value))
    };
    match action {
        TaskAction::Approve {
            override_checks, ..
        } => {
            (override_checks != &Some(true) || owner)
                && (matches!(&offer.action, TaskAction::Approve { override_checks: offered, .. } if offered == override_checks)
                    || override_checks.is_none()
                    || override_checks.is_some_and(|value| allows_bool("override", value)))
        }
        TaskAction::Retry {
            reason: _,
            fresh_session,
            refresh_workspace,
            reset_budget,
            guidance,
        } => {
            let TaskAction::Retry {
                fresh_session: offered_fresh,
                refresh_workspace: offered_refresh,
                reset_budget: offered_reset,
                ..
            } = &offer.action
            else {
                return false;
            };
            let allowed = |name: &str, supplied: Option<bool>, preset: Option<bool>| {
                supplied.is_none_or(|value| {
                    if supports(name) {
                        allows_bool(name, value)
                    } else {
                        preset == Some(value)
                    }
                })
            };
            allowed("fresh_session", *fresh_session, *offered_fresh)
                && allowed("refresh_workspace", *refresh_workspace, *offered_refresh)
                && allowed("reset_budget", *reset_budget, *offered_reset)
                && (guidance.is_none() || supports("guidance"))
        }
        TaskAction::SendBack { .. } => true,
        _ => true,
    }
}

/// Project Task reads return this exact argument schema beside current offers.
/// It projects the supplied offers, never workflow states or queue facts.
pub fn offered_action_schema(offers: &[api_types::Offer], owner: bool) -> Value {
    static ACTION: LazyLock<Value> = LazyLock::new(|| {
        serde_json::to_value(
            schemars::gen::SchemaSettings::draft07()
                .with(|s| s.inline_subschemas = true)
                .into_generator()
                .into_root_schema_for::<api_types::TaskAction>(),
        )
        .unwrap()
    });
    let mut variants = Vec::new();
    for offer in offers {
        let Some(base) = ACTION["oneOf"]
            .as_array()
            .unwrap()
            .iter()
            .find(|variant| variant["properties"]["verb"]["enum"][0] == offer.action.verb())
        else {
            continue;
        };
        let mut schema = base.clone();
        let defaults = serde_json::to_value(&offer.action).unwrap();
        for name in [
            "fresh_session",
            "refresh_workspace",
            "reset_budget",
            "override",
        ] {
            if schema["properties"].get(name).is_none() {
                continue;
            }
            let parameter = offer.parameters.iter().find(|p| p.name == name);
            let mut values = if offer.action.verb() == "retry" {
                match parameter {
                    Some(parameter) => parameter.boolean_values.clone().unwrap_or_default(),
                    None => defaults[name].as_bool().into_iter().collect(),
                }
            } else {
                let mut values = parameter
                    .and_then(|p| p.boolean_values.clone())
                    .unwrap_or_default();
                values.extend(defaults[name].as_bool());
                values.sort_unstable();
                values.dedup();
                values
            };
            if name == "override" && !owner {
                values.retain(|v| !v);
            }
            schema["properties"][name] = json!({"enum":values.into_iter().map(Value::Bool).chain([Value::Null]).collect::<Vec<_>>()});
        }
        if offer.action.verb() == "retry" && !offer.parameters.iter().any(|p| p.name == "guidance")
        {
            schema["properties"]["guidance"] = json!({"type":"null"});
        }
        for field in ["reason", "guidance"] {
            if schema["properties"].get(field).is_some() {
                // reason is always nonblank when supplied; send_back requires
                // nonblank guidance. Retry guidance retains its existing contract.
                if field == "reason" || offer.action.verb() == "send_back" {
                    schema["properties"][field]["minLength"] = json!(1);
                    schema["properties"][field]["pattern"] = json!(r"\S");
                }
            }
        }
        if offer
            .parameters
            .iter()
            .any(|p| p.name == "reason" && p.required)
        {
            schema["required"]
                .as_array_mut()
                .unwrap()
                .push(json!("reason"));
            schema["properties"]["reason"]["type"] = json!("string");
        }
        if offer.action.verb() == "retry" {
            schema["if"] =
                json!({"required":["reset_budget"],"properties":{"reset_budget":{"const":false}}});
            schema["then"] =
                json!({"required":["reason"],"properties":{"reason":{"type":"string"}}});
        } else if offer.action.verb() == "approve" && owner {
            schema["if"] =
                json!({"required":["override"],"properties":{"override":{"const":true}}});
            schema["then"] =
                json!({"required":["reason"],"properties":{"reason":{"type":"string"}}});
        }
        variants.push(schema);
    }
    if variants.is_empty() {
        json!({"not":{}})
    } else {
        json!({"anyOf":variants})
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn action_decode_covers_eight_verbs_and_refuses_unrelated_parameters() {
        let spec = crate::PROPOSAL_CATALOG.lookup("task.action").unwrap();
        for action in [
            json!({"verb":"start"}),
            json!({"verb":"hold"}),
            json!({"verb":"release"}),
            json!({"verb":"retry","fresh_session":true}),
            json!({"verb":"send_back","guidance":"repair"}),
            json!({"verb":"approve","override":false}),
            json!({"verb":"restart"}),
            json!({"verb":"cancel"}),
        ] {
            spec.validate_arguments(&json!({"task_id":"task","version":1,"action":action}))
                .unwrap();
            let mut invalid = action;
            invalid["unrelated"] = json!(true);
            assert!(spec
                .validate_arguments(&json!({"task_id":"task","version":1,"action":invalid}))
                .is_err());
        }
    }
    #[test]
    fn offered_schema_and_execution_share_boolean_and_guidance_choices() {
        for owner in [false, true] {
            let offer = api_types::Offer {
                action: api_types::TaskAction::Retry {
                    reason: None,
                    fresh_session: Some(true),
                    refresh_workspace: None,
                    reset_budget: None,
                    guidance: None,
                },
                parameters: vec![api_types::ActionParameter {
                    name: "refresh_workspace".into(),
                    required: false,
                    boolean_values: Some(vec![false]),
                    required_when: None,
                }],
                authority: vec![],
                reason: "retry".into(),
                label: "Retry".into(),
                target_execution_id: None,
                propagates: false,
            };
            let schema = offered_action_schema(std::slice::from_ref(&offer), owner);
            let validator = jsonschema::validator_for(&schema).unwrap();
            for fresh in [None, Some(false), Some(true)] {
                for refresh in [None, Some(false), Some(true)] {
                    for guidance in [None, Some("repair".to_owned())] {
                        let action = api_types::TaskAction::Retry {
                            reason: None,
                            fresh_session: fresh,
                            refresh_workspace: refresh,
                            reset_budget: None,
                            guidance: guidance.clone(),
                        };
                        assert_eq!(
                            validator.is_valid(&serde_json::to_value(&action).unwrap()),
                            action_parameters_allowed(&offer, &action, owner),
                            "{action:?}"
                        );
                    }
                }
            }
        }
        assert!(
            !jsonschema::validator_for(&offered_action_schema(&[], false))
                .unwrap()
                .is_valid(&json!({"verb":"cancel"}))
        );
    }
}
