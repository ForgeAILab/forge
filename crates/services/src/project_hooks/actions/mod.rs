use api_types::TaskType;
use db::{CreateProjectHookRun, Project, ProjectHookRunStatus};

use crate::project_hooks::triggers::TriggerMatch;

use super::ProjectHookService;

pub mod dispatch_agent;

#[derive(Debug, Clone)]
pub struct ActionOutcome {
    pub status: ProjectHookRunStatus,
    pub automation_task_id: Option<String>,
    pub execution_id: Option<String>,
    pub agent_id: Option<String>,
    pub reason: Option<String>,
}

impl ActionOutcome {
    pub fn completed(reason: impl Into<String>) -> Self {
        Self {
            status: ProjectHookRunStatus::Completed,
            automation_task_id: None,
            execution_id: None,
            agent_id: None,
            reason: Some(reason.into()),
        }
    }

    pub fn skipped(reason: impl Into<String>) -> Self {
        Self {
            status: ProjectHookRunStatus::Skipped,
            automation_task_id: None,
            execution_id: None,
            agent_id: None,
            reason: Some(reason.into()),
        }
    }
}

pub struct ActionContext<'a> {
    pub service: &'a ProjectHookService,
    pub project: &'a Project,
    pub rule_id: &'a str,
    pub run: &'a CreateProjectHookRun,
    pub trigger_match: &'a TriggerMatch,
}

pub(crate) fn task_type_to_string(task_type: TaskType) -> String {
    match task_type {
        TaskType::Task => "task",
        TaskType::PlanningTask => "planning_task",
        TaskType::SubTask => "sub_task",
        TaskType::Discovery => "discovery",
    }
    .to_owned()
}
