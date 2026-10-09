//! Project Charter and on-demand doctrine reads; no Task state is read.
use crate::scope_reads::NoArguments;
use crate::*;
use schemars::JsonSchema;
use serde::Deserialize;

#[async_trait::async_trait]
pub trait ProjectReadContext<E>: Send + Sync {
    async fn project_charter(&self, input: NoArguments) -> Result<Value, E>;
    async fn skill_section(&self, input: SectionArguments) -> Result<Value, E>;
}

pub const SECTION_NAMES: &[&str] = &[
    "research",
    "documents",
    "scope_change",
    "tasks",
    "milestones",
    "release",
];
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SectionArguments {
    pub section: Section,
}
#[derive(Debug, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Section {
    Research,
    Documents,
    ScopeChange,
    Tasks,
    Milestones,
    Release,
}
impl Section {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Research => "research",
            Self::Documents => "documents",
            Self::ScopeChange => "scope_change",
            Self::Tasks => "tasks",
            Self::Milestones => "milestones",
            Self::Release => "release",
        }
    }
}
const AUTHORITY: AuthorityRule = AuthorityRule {
    principal: authority::PrincipalRule::ProjectAgent,
    permissions: &[
        ("project", "read_project"),
        ("agent_chat", "read_agent_chat"),
    ],
    binding: "bound Project Agent",
};
const SURFACES: &[SurfaceBinding] = &[SurfaceBinding {
    native_aggregate: "forge_project_orchestration_read",
    projection: FieldProjection::ReadArguments,
}];
pub const IDS: &[&str] = &["project.charter", "skill.section"];

pub fn specs<E: Send + 'static>() -> Vec<OperationSpec<E>> {
    vec![
        OperationSpec::typed::<NoArguments>("project.charter", AUTHORITY, EffectClass::Query, AvailabilityRule::ReadyOnly, SURFACES,
            "Read the bound Project's approved Charter.",
            "Returns the bound Project's current approved Charter as rendered Markdown plus its charter/revision identifiers and content/render digests. The Charter is Project data, not resident context: read it whenever its details matter to a decision. The response is scope-bound and never accepts a Project selector.",
            &[StructuralConstraint::ClosedObject], |context, input| Box::pin(context.project_charter(input))),
        OperationSpec::typed("skill.section", AUTHORITY, EffectClass::Query, AvailabilityRule::ReadyOnly, SURFACES,
            "Read one Project operating-doctrine section.",
            "Returns one server-owned Project operating-doctrine section by name. Read the matching section before the first work of that kind in a conversation and re-read it when unsure: research, documents, scope_change, tasks, milestones, release.",
            &[StructuralConstraint::ClosedObject, StructuralConstraint::Required("section"), StructuralConstraint::StringEnum {field:"section", values:SECTION_NAMES}],
            |context, input| Box::pin(context.skill_section(input))),
    ]
}
