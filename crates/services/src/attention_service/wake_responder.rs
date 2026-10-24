//! Wake responder identity and eligibility resolution.

use super::*;

impl AttentionService {
    pub(super) async fn wake_identity_for_event(
        &self,
        event: &DomainEvent,
    ) -> Result<Option<String>> {
        // Resolve the current owner before looking at the source actor.  An
        // unresolved incident survives binding replacement and must wake the
        // replacement identity, never the stale identity that authored the
        // original event.
        let (scope_type, scope_id) = self.event_scope(event).await?;
        match scope_type.as_str() {
            "project" => {
                return Ok(self
                    .db
                    .attention_project_responder_identity(&scope_id)
                    .await?);
            }
            "account" => {
                return Ok(self.db.attention_main_responder_identity(&scope_id).await?);
            }
            "agent_chat" => {
                let chat = self.db.attention_responder_chat_scope(&scope_id).await?;
                if let Some(chat) = chat {
                    let kind: String = chat.kind?;
                    if kind == "account_main" {
                        return Ok(self
                            .db
                            .attention_chat_main_responder_identity(chat.account_id?)
                            .await?);
                    }
                    if kind == "project" {
                        return Ok(self
                            .db
                            .attention_chat_project_responder_identity(chat.project_id?)
                            .await?);
                    }
                }
                return Ok(None);
            }
            "task" => {
                return Ok(self.db.attention_task_assignee_identity(&scope_id).await?);
            }
            _ => {}
        }

        // Non-canonical/legacy scopes have no current Main/Project responder
        // to resolve.  Keep the source actor as provenance only for these
        // scopes; callers will record a setup-required decision when it is
        // not eligible.
        if let Some(identity_id) = self.event_identity(event).await? {
            return Ok(Some(identity_id));
        }
        let payload = serde_json::from_str::<Value>(&event.payload_json).unwrap_or(Value::Null);
        for key in [
            "identity_id",
            "agent_id",
            "responder_identity_id",
            "assignee_id",
        ] {
            let Some(identity_id) = payload.get(key).and_then(Value::as_str) else {
                continue;
            };
            let exists = self
                .db
                .attention_requested_identity_count(identity_id)
                .await?
                > 0;
            if exists {
                return Ok(Some(identity_id.to_owned()));
            }
        }
        Ok(None)
    }

    /// Check whether a current Main/Project binding exists for this canonical
    /// scope.  `identity_id` is optional so projection can distinguish a
    /// missing responder (setup-required) from a configured responder that is
    /// ineligible for the particular event (deterministic suppression).
    pub(super) async fn wake_responder_is_configured(
        &self,
        scope_type: &str,
        scope_id: &str,
        identity_id: Option<&str>,
    ) -> Result<bool> {
        let count = match scope_type {
            "account" => {
                if let Some(identity_id) = identity_id {
                    self.db
                        .attention_main_identity_binding_count(scope_id, identity_id)
                        .await?
                } else {
                    self.db.attention_main_binding_count(scope_id).await?
                }
            }
            "project" => {
                if let Some(identity_id) = identity_id {
                    self.db
                        .attention_project_identity_binding_count(scope_id, identity_id)
                        .await?
                } else {
                    self.db.attention_project_binding_count(scope_id).await?
                }
            }
            "agent_chat" => {
                let chat = self.db.attention_configured_chat_scope(scope_id).await?;
                let Some(chat) = chat else {
                    return Ok(false);
                };
                let kind: String = chat.kind?;
                let account_id: Option<String> = chat.account_id?;
                let project_id: Option<String> = chat.project_id?;
                let configured = match kind.as_str() {
                    "account_main" => {
                        let Some(account_id) = account_id else {
                            return Ok(false);
                        };
                        if let Some(identity_id) = identity_id {
                            self.db
                                .attention_chat_main_identity_binding_count(account_id, identity_id)
                                .await?
                        } else {
                            self.db
                                .attention_chat_main_binding_count(account_id)
                                .await?
                        }
                    }
                    "project" => {
                        let Some(project_id) = project_id else {
                            return Ok(false);
                        };
                        if let Some(identity_id) = identity_id {
                            self.db
                                .attention_chat_project_identity_binding_count(
                                    project_id,
                                    identity_id,
                                )
                                .await?
                        } else {
                            self.db
                                .attention_chat_project_binding_count(project_id)
                                .await?
                        }
                    }
                    _ => 0,
                };
                configured
            }
            "task" => {
                if let Some(identity_id) = identity_id {
                    self.db
                        .attention_task_identity_assignment_count(scope_id, identity_id)
                        .await?
                } else {
                    self.db
                        .attention_task_agent_assignment_count(scope_id)
                        .await?
                }
            }
            _ => 0,
        };
        Ok(count > 0)
    }

    pub(super) async fn wake_identity_is_eligible(
        &self,
        identity_id: &str,
        scope_type: &str,
        scope_id: &str,
    ) -> Result<bool> {
        let eligible = match scope_type {
            "account" => {
                self.db
                    .attention_account_identity_access_count(identity_id, scope_id)
                    .await?
                    > 0
            }
            "project" => {
                self.db
                    .attention_project_identity_access_count(identity_id, scope_id)
                    .await?
                    > 0
            }
            "agent_chat" => {
                self.db
                    .attention_chat_identity_access_count(scope_id, identity_id, identity_id)
                    .await?
                    > 0
            }
            "task" => {
                self.db
                    .attention_task_identity_access_count(scope_id, identity_id)
                    .await?
                    > 0
            }
            _ => false,
        };
        Ok(eligible)
    }
}
