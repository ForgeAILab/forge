// Integration suites share one migration template and one link step.
// Keep the original target name as the module for focused test filters.

#[path = "suite/agent_chat_scopes.rs"]
mod agent_chat_scopes;
#[path = "suite/agent_inquiries.rs"]
mod agent_inquiries;
#[path = "suite/agent_wake_dispositions.rs"]
mod agent_wake_dispositions;
#[path = "suite/attention_projection.rs"]
mod attention_projection;
#[path = "suite/audit2_worker_metadata.rs"]
mod audit2_worker_metadata;
#[path = "suite/audit_21b_upgrade.rs"]
mod audit_21b_upgrade;
#[path = "suite/budget_resolution.rs"]
mod budget_resolution;
#[path = "suite/command_documents_decisions.rs"]
mod command_documents_decisions;
#[path = "suite/command_milestones.rs"]
mod command_milestones;
#[path = "suite/command_receipts.rs"]
mod command_receipts;
#[path = "suite/coordination.rs"]
mod coordination;
#[path = "suite/daemon_owned_workspaces.rs"]
mod daemon_owned_workspaces;
#[path = "suite/execution_terminal_receipt.rs"]
mod execution_terminal_receipt;
#[path = "suite/file_backed_migration.rs"]
mod file_backed_migration;
#[path = "suite/foreign_key_pragma_migration.rs"]
mod foreign_key_pragma_migration;
#[path = "suite/lead_comment_kinds.rs"]
mod lead_comment_kinds;
#[path = "suite/level_triggered_wakes_upgrade.rs"]
mod level_triggered_wakes_upgrade;
#[path = "suite/main_chat_topics.rs"]
mod main_chat_topics;
#[path = "suite/notify_hooks_consumers.rs"]
mod notify_hooks_consumers;
#[path = "suite/pricing_repository.rs"]
mod pricing_repository;
#[path = "suite/project_charter_commands.rs"]
mod project_charter_commands;
#[path = "suite/project_deletion_context_manifests.rs"]
mod project_deletion_context_manifests;
#[path = "suite/project_deletion_media.rs"]
mod project_deletion_media;
#[path = "suite/project_environment_pause.rs"]
mod project_environment_pause;
#[path = "suite/project_execution_setup_commands.rs"]
mod project_execution_setup_commands;
#[path = "suite/project_execution_setup_migration.rs"]
mod project_execution_setup_migration;
#[path = "suite/project_orchestration.rs"]
mod project_orchestration;
#[path = "suite/remove_pull_request_work_mode.rs"]
mod remove_pull_request_work_mode;
#[path = "suite/retire_event_delivery.rs"]
mod retire_event_delivery;
#[path = "suite/revised_lease_invariants.rs"]
mod revised_lease_invariants;
#[path = "suite/shared_media.rs"]
mod shared_media;
#[path = "suite/shared_media_command.rs"]
mod shared_media_command;
#[path = "suite/singular_agent_chats.rs"]
mod singular_agent_chats;
#[path = "suite/sqlite_connection.rs"]
mod sqlite_connection;
#[path = "suite/task_authority_and_metadata.rs"]
mod task_authority_and_metadata;
#[path = "suite/task_budgets.rs"]
mod task_budgets;
#[path = "suite/task_condition_producer_cost.rs"]
mod task_condition_producer_cost;
#[path = "suite/task_condition_readers_upgrade.rs"]
mod task_condition_readers_upgrade;
#[path = "suite/task_condition_statement_cost.rs"]
mod task_condition_statement_cost;
#[path = "suite/task_condition_write_cost.rs"]
mod task_condition_write_cost;
#[path = "suite/task_schedule_queue.rs"]
mod task_schedule_queue;
#[path = "suite/task_schedule_triggers.rs"]
mod task_schedule_triggers;
#[path = "suite/task_schedule_upgrade.rs"]
mod task_schedule_upgrade;
#[path = "suite/task_single_writer_upgrade.rs"]
mod task_single_writer_upgrade;
#[path = "suite/task_step_epoch_upgrade.rs"]
mod task_step_epoch_upgrade;
#[path = "suite/typed_workflow_bridges.rs"]
mod typed_workflow_bridges;
#[path = "suite/unified_assignee_migration.rs"]
mod unified_assignee_migration;
#[path = "suite/v076_data_preservation.rs"]
mod v076_data_preservation;
#[path = "suite/v138_project_owned_task_repository.rs"]
mod v138_project_owned_task_repository;
#[path = "suite/worker_dead_letter_resolution.rs"]
mod worker_dead_letter_resolution;
