// Shared integration suites: preserve original target stems for focused filters.
// Global tracing collectors and heavy daemon/model fixtures remain separate.

mod common;

#[path = "suite/integration_quarantine.rs"]
mod integration_quarantine;

#[path = "suite/account_resource_binding.rs"]
mod account_resource_binding;

#[path = "suite/adapter_parity_characterization.rs"]
mod adapter_parity_characterization;

#[path = "suite/admin_api.rs"]
mod admin_api;

#[path = "suite/agent_chats.rs"]
mod agent_chats;

#[path = "suite/agent_inquiries.rs"]
mod agent_inquiries;

#[path = "suite/agent_usage_reads.rs"]
mod agent_usage_reads;

#[path = "suite/analytics_usage.rs"]
mod analytics_usage;

#[path = "suite/assignee_notification.rs"]
mod assignee_notification;

#[path = "suite/auth.rs"]
mod auth;

#[path = "suite/auto_cascade_fail.rs"]
mod auto_cascade_fail;

#[path = "suite/backlog_claim_rejection.rs"]
mod backlog_claim_rejection;

#[path = "suite/bounce_reason_reaches_coder.rs"]
mod bounce_reason_reaches_coder;

#[path = "suite/canonical_phase.rs"]
mod canonical_phase;

#[path = "suite/check_conditions.rs"]
mod check_conditions;

#[path = "suite/coordination.rs"]
mod coordination;

#[path = "suite/custom_workflow.rs"]
mod custom_workflow;

#[path = "suite/daemon_connect.rs"]
mod daemon_connect;

#[path = "suite/daemon_onboarding.rs"]
mod daemon_onboarding;

#[path = "suite/daemon_scoping.rs"]
mod daemon_scoping;

#[path = "suite/data_dir_isolation.rs"]
mod data_dir_isolation;

#[path = "suite/e2e.rs"]
mod e2e;

#[path = "suite/environment_surfaces.rs"]
mod environment_surfaces;

#[path = "suite/event_publication_audit.rs"]
mod event_publication_audit;

#[path = "suite/events_replay.rs"]
mod events_replay;

#[path = "suite/execution_controls.rs"]
mod execution_controls;

#[path = "suite/execution_logs.rs"]
mod execution_logs;

#[path = "suite/explicit_cancellation.rs"]
mod explicit_cancellation;

#[path = "suite/external_links.rs"]
mod external_links;

#[path = "suite/facade_actions.rs"]
mod facade_actions;

#[path = "suite/follow_up_budget_exhausted.rs"]
mod follow_up_budget_exhausted;

#[path = "suite/follow_up_chat.rs"]
mod follow_up_chat;

#[path = "suite/follow_up_merge_conflict.rs"]
mod follow_up_merge_conflict;

#[path = "suite/follow_up_merge_fix_zero.rs"]
mod follow_up_merge_fix_zero;

#[path = "suite/follow_up_review_budget_override.rs"]
mod follow_up_review_budget_override;

#[path = "suite/follow_up_review_fail.rs"]
mod follow_up_review_fail;

#[path = "suite/follow_up_unblock_resets_budgets.rs"]
mod follow_up_unblock_resets_budgets;

#[path = "suite/fs_daemon_routing.rs"]
mod fs_daemon_routing;

#[path = "suite/happy_path.rs"]
mod happy_path;

#[path = "suite/hook_audience.rs"]
mod hook_audience;

#[path = "suite/independent_subtask_manual_transition.rs"]
mod independent_subtask_manual_transition;

#[path = "suite/integration_queue_reads.rs"]
mod integration_queue_reads;

#[path = "suite/lifecycle_hook_test_endpoint.rs"]
mod lifecycle_hook_test_endpoint;

#[path = "suite/machine_capacity.rs"]
mod machine_capacity;

#[path = "suite/manual_bounce_vs_rejection.rs"]
mod manual_bounce_vs_rejection;

#[path = "suite/manual_move_skips_hooks.rs"]
mod manual_move_skips_hooks;

#[path = "suite/manual_review_and_comments.rs"]
mod manual_review_and_comments;

#[path = "suite/mcp_oauth.rs"]
mod mcp_oauth;

#[path = "suite/mission_control.rs"]
mod mission_control;

#[path = "suite/notifications.rs"]
mod notifications;

#[path = "suite/operations_dead_letters.rs"]
mod operations_dead_letters;

#[path = "suite/operations_status.rs"]
mod operations_status;

#[path = "suite/pause_claim.rs"]
mod pause_claim;

#[path = "suite/pause_endpoints.rs"]
mod pause_endpoints;

#[path = "suite/planning_gate.rs"]
mod planning_gate;

#[path = "suite/pricing_routes.rs"]
mod pricing_routes;

#[path = "suite/product_genesis.rs"]
mod product_genesis;

#[path = "suite/project_active_task_limit.rs"]
mod project_active_task_limit;

#[path = "suite/project_charters.rs"]
mod project_charters;

#[path = "suite/project_deletion.rs"]
mod project_deletion;

#[path = "suite/project_environment_pause.rs"]
mod project_environment_pause;

#[path = "suite/project_execution_setup.rs"]
mod project_execution_setup;

#[path = "suite/project_hook_all_work_completed.rs"]
mod project_hook_all_work_completed;

#[path = "suite/project_overview.rs"]
mod project_overview;

#[path = "suite/project_reconciliations.rs"]
mod project_reconciliations;

#[path = "suite/prompt_preview.rs"]
mod prompt_preview;

#[path = "suite/protected_interactions.rs"]
mod protected_interactions;

#[path = "suite/provider_health_routes.rs"]
mod provider_health_routes;

#[path = "suite/remote_execution_roundtrip.rs"]
mod remote_execution_roundtrip;

#[path = "suite/remove_machine.rs"]
mod remove_machine;

#[path = "suite/repo_locations_endpoints.rs"]
mod repo_locations_endpoints;

#[path = "suite/repos_endpoints.rs"]
mod repos_endpoints;

#[path = "suite/retry_budget.rs"]
mod retry_budget;

#[path = "suite/review_rejection_clears_re_review_flag.rs"]
mod review_rejection_clears_re_review_flag;

#[path = "suite/reviews_endpoint.rs"]
mod reviews_endpoint;

#[path = "suite/revised_main_project_task_acceptance.rs"]
mod revised_main_project_task_acceptance;

#[path = "suite/security_adversarial.rs"]
mod security_adversarial;

#[path = "suite/security_followups.rs"]
mod security_followups;

#[path = "suite/settings_endpoint.rs"]
mod settings_endpoint;

#[path = "suite/skip_planning.rs"]
mod skip_planning;

#[path = "suite/task_detail.rs"]
mod task_detail;

#[path = "suite/task_diff.rs"]
mod task_diff;

#[path = "suite/task_media.rs"]
mod task_media;

#[path = "suite/task_step_outbox.rs"]
mod task_step_outbox;

#[path = "suite/task_type_removed.rs"]
mod task_type_removed;

#[path = "suite/terminal_sessions.rs"]
mod terminal_sessions;

#[path = "suite/transition_log.rs"]
mod transition_log;

#[path = "suite/v076_orchestration_acceptance.rs"]
mod v076_orchestration_acceptance;

#[path = "suite/workflow_migration_safety.rs"]
mod workflow_migration_safety;

#[path = "suite/workflow_templates.rs"]
mod workflow_templates;

#[path = "suite/project_authorization.rs"]
mod project_authorization;
