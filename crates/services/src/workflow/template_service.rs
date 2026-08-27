use std::{
    collections::HashSet,
    fs,
    io::ErrorKind,
    path::{Path, PathBuf},
};

use api_types::{WorkflowDefinition, WorkflowTemplateResponse, WorkflowTemplateSummary};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{
    workflow::{
        default_autonomous_workflow::default_autonomous_workflow,
        default_workflow::default_workflow, validation::validate_workflow,
    },
    Result, ServiceError,
};

#[derive(Debug, Clone)]
pub struct WorkflowTemplateService {
    workflows_dir: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TemplateDocument {
    display_name: String,
    description: String,
    definition: WorkflowDefinition,
}

impl WorkflowTemplateService {
    pub fn new(workflows_dir: PathBuf) -> Self {
        Self { workflows_dir }
    }

    pub async fn initialize(&self) -> Result<()> {
        fs::create_dir_all(&self.workflows_dir).map_err(|error| {
            ServiceError::InvalidOperation {
                message: format!(
                    "failed to create workflow template directory {}: {error}",
                    self.workflows_dir.display()
                ),
            }
        })?;

        self.ensure_builtin_templates()
    }

    pub async fn list_templates(&self) -> Result<Vec<WorkflowTemplateSummary>> {
        let entries = match fs::read_dir(&self.workflows_dir) {
            Ok(entries) => entries,
            Err(error) if error.kind() == ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => {
                return Err(ServiceError::InvalidOperation {
                    message: format!(
                        "failed to read workflow template directory {}: {error}",
                        self.workflows_dir.display()
                    ),
                });
            }
        };

        let mut seen_names = HashSet::new();
        let mut templates = Vec::new();

        for entry in entries {
            let entry = entry.map_err(|error| ServiceError::InvalidOperation {
                message: format!(
                    "failed to read workflow template directory entry in {}: {error}",
                    self.workflows_dir.display()
                ),
            })?;
            let path = entry.path();
            if !is_yaml_path(&path) {
                continue;
            }

            let Some(name) = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .map(str::to_owned)
            else {
                return Err(ServiceError::InvalidOperation {
                    message: format!(
                        "workflow template filename is not valid UTF-8: {}",
                        path.display()
                    ),
                });
            };

            if !Self::validate_name(&name) {
                return Err(ServiceError::InvalidOperation {
                    message: format!("invalid workflow template filename: {}", path.display()),
                });
            }
            if !seen_names.insert(name.clone()) {
                return Err(ServiceError::InvalidOperation {
                    message: format!("duplicate workflow template name '{name}'"),
                });
            }

            let document = self.read_template_document(&path)?;
            templates.push(WorkflowTemplateSummary {
                name,
                display_name: document.display_name,
                description: document.description,
                is_builtin: path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .map(is_builtin_template_name)
                    .unwrap_or(false),
            });
        }

        templates.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(templates)
    }

    pub async fn get_template(&self, name: &str) -> Result<WorkflowTemplateResponse> {
        if !Self::validate_name(name) {
            return Err(ServiceError::InvalidOperation {
                message: format!("invalid workflow template name '{name}'"),
            });
        }

        let path = self
            .existing_template_path(name)
            .ok_or_else(|| ServiceError::NotFound {
                entity: "workflow template",
                id: name.to_owned(),
            })?;
        let document = self.read_template_document(&path)?;

        Ok(WorkflowTemplateResponse {
            name: name.to_owned(),
            display_name: document.display_name,
            description: document.description,
            is_builtin: is_builtin_template_name(name),
            definition: document.definition,
        })
    }

    pub async fn save_template(
        &self,
        name: &str,
        display_name: String,
        description: String,
        definition: WorkflowDefinition,
    ) -> Result<()> {
        if !Self::validate_name(name) {
            return Err(ServiceError::InvalidOperation {
                message: format!("invalid workflow template name '{name}'"),
            });
        }

        validate_workflow(&definition)?;
        fs::create_dir_all(&self.workflows_dir).map_err(|error| {
            ServiceError::InvalidOperation {
                message: format!(
                    "failed to create workflow template directory {}: {error}",
                    self.workflows_dir.display()
                ),
            }
        })?;

        let path = self.yaml_template_path(name);
        let document = TemplateDocument {
            display_name,
            description,
            definition,
        };
        self.write_template_document(&path, &document)?;

        let legacy_path = self.yml_template_path(name);
        if legacy_path != path && legacy_path.exists() {
            fs::remove_file(&legacy_path).map_err(|error| ServiceError::InvalidOperation {
                message: format!(
                    "failed to remove legacy workflow template {}: {error}",
                    legacy_path.display()
                ),
            })?;
        }

        Ok(())
    }

    pub async fn delete_template(&self, name: &str) -> Result<()> {
        if !Self::validate_name(name) {
            return Err(ServiceError::InvalidOperation {
                message: format!("invalid workflow template name '{name}'"),
            });
        }
        if is_builtin_template_name(name) {
            return Err(ServiceError::InvalidOperation {
                message: format!("cannot delete builtin workflow template '{name}'"),
            });
        }

        let path = self
            .existing_template_path(name)
            .ok_or_else(|| ServiceError::NotFound {
                entity: "workflow template",
                id: name.to_owned(),
            })?;
        fs::remove_file(&path).map_err(|error| match error.kind() {
            ErrorKind::NotFound => ServiceError::NotFound {
                entity: "workflow template",
                id: name.to_owned(),
            },
            _ => ServiceError::InvalidOperation {
                message: format!(
                    "failed to delete workflow template {}: {error}",
                    path.display()
                ),
            },
        })?;

        let legacy_path = self.yml_template_path(name);
        if legacy_path != path && legacy_path.exists() {
            fs::remove_file(&legacy_path).map_err(|error| ServiceError::InvalidOperation {
                message: format!(
                    "failed to delete workflow template {}: {error}",
                    legacy_path.display()
                ),
            })?;
        }

        Ok(())
    }

    fn validate_name(name: &str) -> bool {
        if name.is_empty() || name.len() > 64 {
            return false;
        }

        let mut chars = name.chars();
        let Some(first) = chars.next() else {
            return false;
        };
        if !first.is_ascii_lowercase() && !first.is_ascii_digit() {
            return false;
        }

        chars.all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_' || ch == '-')
    }

    fn existing_template_path(&self, name: &str) -> Option<PathBuf> {
        let yaml_path = self.yaml_template_path(name);
        if yaml_path.exists() {
            return Some(yaml_path);
        }

        let yml_path = self.yml_template_path(name);
        if yml_path.exists() {
            return Some(yml_path);
        }

        None
    }

    fn yaml_template_path(&self, name: &str) -> PathBuf {
        self.workflows_dir.join(format!("{name}.yaml"))
    }

    fn yml_template_path(&self, name: &str) -> PathBuf {
        self.workflows_dir.join(format!("{name}.yml"))
    }

    fn read_template_document(&self, path: &Path) -> Result<TemplateDocument> {
        let contents = fs::read_to_string(path).map_err(|error| match error.kind() {
            ErrorKind::NotFound => ServiceError::NotFound {
                entity: "workflow template",
                id: path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .unwrap_or_default()
                    .to_owned(),
            },
            _ => ServiceError::InvalidOperation {
                message: format!(
                    "failed to read workflow template {}: {error}",
                    path.display()
                ),
            },
        })?;

        let document: TemplateDocument =
            serde_yaml::from_str(&contents).map_err(|error| ServiceError::InvalidOperation {
                message: format!("invalid workflow template {}: {error}", path.display()),
            })?;
        Ok(document)
    }

    fn write_template_document(&self, path: &Path, document: &TemplateDocument) -> Result<()> {
        let contents =
            serde_yaml::to_string(document).map_err(|error| ServiceError::InvalidOperation {
                message: format!(
                    "failed to serialize workflow template {}: {error}",
                    path.display()
                ),
            })?;
        let temp_path = path.with_file_name(format!(
            ".{}.{}.tmp",
            path.file_name()
                .and_then(|file_name| file_name.to_str())
                .unwrap_or("workflow-template"),
            Uuid::new_v4()
        ));

        fs::write(&temp_path, contents).map_err(|error| ServiceError::InvalidOperation {
            message: format!(
                "failed to write workflow template temp file {}: {error}",
                temp_path.display()
            ),
        })?;
        fs::rename(&temp_path, path).map_err(|error| {
            let _ = fs::remove_file(&temp_path);
            ServiceError::InvalidOperation {
                message: format!(
                    "failed to replace workflow template {}: {error}",
                    path.display()
                ),
            }
        })?;

        Ok(())
    }

    fn ensure_builtin_templates(&self) -> Result<()> {
        for (name, document) in builtin_templates() {
            let path = self.yaml_template_path(name);
            self.write_template_document(&path, &document)?;
            let legacy_path = self.yml_template_path(name);
            if legacy_path != path && legacy_path.exists() {
                fs::remove_file(&legacy_path).map_err(|error| ServiceError::InvalidOperation {
                    message: format!(
                        "failed to remove legacy workflow template {}: {error}",
                        legacy_path.display()
                    ),
                })?;
            }
        }
        Ok(())
    }
}

fn is_yaml_path(path: &Path) -> bool {
    matches!(
        path.extension().and_then(|ext| ext.to_str()),
        Some("yaml" | "yml")
    )
}

fn builtin_default_template() -> TemplateDocument {
    TemplateDocument {
        display_name: "Agent review".to_owned(),
        description: "Plan, implement, then let the assigned reviewer Agent review".to_owned(),
        definition: default_workflow(),
    }
}

fn builtin_autonomous_v1_template() -> TemplateDocument {
    TemplateDocument {
        display_name: "Autonomous v1".to_owned(),
        description: "Single-worker workflow with hard validation and human review approval."
            .to_owned(),
        definition: default_autonomous_workflow(),
    }
}

fn builtin_human_required_template() -> TemplateDocument {
    let mut definition = default_workflow();
    if let Some(review) = definition
        .states
        .iter_mut()
        .find(|state| state.name == crate::workflow::default_states::REVIEW)
    {
        review.role = None;
        review.dispatch = None;
        if let Some(gate_config) = review.gate_config.as_mut() {
            gate_config.requires_user_approval = Some(true);
            gate_config.optional_when_unassigned = Some(false);
            gate_config.approve_label = Some("Accept".to_owned());
            gate_config.reject_label = Some("Reject".to_owned());
        }
    }

    TemplateDocument {
        display_name: "Human required".to_owned(),
        description: "Wait for the user or the bound Project Agent to accept or reject".to_owned(),
        definition,
    }
}

fn builtin_no_review_template() -> TemplateDocument {
    let mut definition = default_workflow();
    if let Some(review) = definition
        .states
        .iter_mut()
        .find(|state| state.name == crate::workflow::default_states::REVIEW)
    {
        review.role = None;
        review.dispatch = None;
        if let Some(gate_config) = review.gate_config.as_mut() {
            gate_config.requires_user_approval = Some(false);
            gate_config.optional_when_unassigned = Some(true);
            gate_config.reject_label = None;
        }
    }

    TemplateDocument {
        display_name: "No review".to_owned(),
        description: "Run required checks and continue without a reviewer execution".to_owned(),
        definition,
    }
}

fn builtin_templates() -> [(&'static str, TemplateDocument); 4] {
    [
        ("default", builtin_default_template()),
        ("autonomous_v1", builtin_autonomous_v1_template()),
        ("human-required", builtin_human_required_template()),
        ("no-review", builtin_no_review_template()),
    ]
}

fn is_builtin_template_name(name: &str) -> bool {
    matches!(
        name,
        "default" | "autonomous_v1" | "human-required" | "no-review"
    )
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::{
        builtin_autonomous_v1_template, builtin_default_template, WorkflowTemplateService,
    };

    fn temp_workflows_dir() -> std::path::PathBuf {
        let path =
            std::env::temp_dir().join(format!("forge-workflow-templates-{}", uuid::Uuid::new_v4()));
        fs::create_dir_all(&path).expect("create temp workflows dir");
        path
    }

    #[tokio::test]
    async fn initialize_repairs_invalid_default_template() {
        let workflows_dir = temp_workflows_dir();
        let service = WorkflowTemplateService::new(workflows_dir.clone());
        let invalid_default = r#"
display_name: Default
description: invalid default
definition:
  roles: []
  states: []
  cancellation_state: cancelled
"#;
        fs::write(workflows_dir.join("default.yaml"), invalid_default)
            .expect("write invalid default template");

        service.initialize().await.expect("initialize succeeds");

        let template = service
            .get_template("default")
            .await
            .expect("default template loads");
        assert_eq!(template.display_name, "Agent review");
        assert_eq!(template.description, builtin_default_template().description);
        assert_eq!(template.definition, builtin_default_template().definition);
    }

    #[tokio::test]
    async fn initialize_rewrites_stale_builtin_template() {
        let workflows_dir = temp_workflows_dir();
        let service = WorkflowTemplateService::new(workflows_dir.clone());
        let mut stale = builtin_default_template();
        stale.definition.configuration = Vec::new();
        service
            .write_template_document(&workflows_dir.join("default.yaml"), &stale)
            .expect("write stale default template");

        service.initialize().await.expect("initialize succeeds");

        let template = service
            .get_template("default")
            .await
            .expect("default template loads");
        assert_eq!(template.definition, builtin_default_template().definition);
        assert!(!template.definition.configuration.is_empty());
    }

    #[tokio::test]
    async fn initialize_adds_human_required_template() {
        let workflows_dir = temp_workflows_dir();
        let service = WorkflowTemplateService::new(workflows_dir);

        service.initialize().await.expect("initialize succeeds");

        let template = service
            .get_template("human-required")
            .await
            .expect("human-required template loads");
        assert!(template.is_builtin);
        assert_eq!(template.display_name, "Human required");
        let review = template
            .definition
            .states
            .iter()
            .find(|state| state.name == crate::workflow::default_states::REVIEW)
            .expect("review state exists");
        assert!(review
            .gate_config
            .as_ref()
            .expect("review gate config")
            .requires_user_approval());
        assert!(!template.definition.configuration.is_empty());
    }

    #[tokio::test]
    async fn initialize_adds_autonomous_v1_template() {
        let workflows_dir = temp_workflows_dir();
        let service = WorkflowTemplateService::new(workflows_dir);

        service.initialize().await.expect("initialize succeeds");

        let template = service
            .get_template("autonomous_v1")
            .await
            .expect("autonomous template loads");
        assert!(template.is_builtin);
        assert_eq!(template.display_name, "Autonomous v1");
        assert_eq!(
            template.definition,
            builtin_autonomous_v1_template().definition
        );
        assert_eq!(template.definition.roles.len(), 1);
        assert_eq!(template.definition.roles[0].name, "worker");
    }

    #[tokio::test]
    async fn initialize_adds_no_review_template() {
        let workflows_dir = temp_workflows_dir();
        let service = WorkflowTemplateService::new(workflows_dir);

        service.initialize().await.expect("initialize succeeds");

        let template = service
            .get_template("no-review")
            .await
            .expect("no-review template loads");
        assert!(template.is_builtin);
        assert_eq!(template.display_name, "No review");
        let planning = template
            .definition
            .states
            .iter()
            .find(|state| state.name == crate::workflow::default_states::PLANNING)
            .expect("planning state exists");
        let gate_config = planning.gate_config.as_ref().expect("planning gate config");
        assert!(!gate_config.requires_user_approval());
        assert!(gate_config.optional_when_unassigned());
        assert_eq!(gate_config.reject_label, None);
        let review = template
            .definition
            .states
            .iter()
            .find(|state| state.name == crate::workflow::default_states::REVIEW)
            .expect("review check state exists");
        assert!(review.role.is_none());
        assert!(review.dispatch.is_none());
        assert!(!template.definition.configuration.is_empty());
    }
}
