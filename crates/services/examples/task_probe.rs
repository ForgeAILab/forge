//! Deterministic probe for the Project Agent `task.propose` tool path.
//! Proposes the SimpleTodo MVP tasks exactly as an agent tool call would.
//! Run: FORGE_PROBE_DB=... cargo run -p services --example task_probe
use std::sync::Arc;

use db::{create_sqlite_pool, SqliteDb};
use forge_agent_host::{CanonicalScope, CanonicalScopeType, ForgeToolProvider, WorkspaceAccess};
use serde_json::json;
use services::CoordinationToolProvider;

const TASKS: &[(&str, &str)] = &[
    ("Scaffold app shell and localStorage state module", "Create the client-only application shell with a reactive state module that reads and writes the todo list to localStorage (plan item pi-1)."),
    ("Add-task input", "Input form and creation handler that appends a new todo item to the list and persists it (plan item pi-2)."),
    ("Toggle complete", "Completion-status toggle per item with visual treatment and persisted state (plan item pi-3)."),
    ("Inline edit task title", "Inline title editing with commit and cancel controls, persisted on commit (plan item pi-4)."),
    ("Delete task", "Delete an item and update the persisted list state (plan item pi-5)."),
    ("All/Active/Completed filter bar", "Filter bar with item-count summary projecting the list by status (plan item pi-6)."),
];

fn main() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    runtime.block_on(run());
}

async fn run() {
    let db_path = std::env::var("FORGE_PROBE_DB").expect("set FORGE_PROBE_DB");
    let pool = create_sqlite_pool(&format!("sqlite:{db_path}"))
        .await
        .expect("pool opens");
    let db = Arc::new(SqliteDb::new(pool));
    let provider = CoordinationToolProvider::new(Arc::clone(&db));
    let scope = CanonicalScope {
        scope_type: CanonicalScopeType::AgentChat,
        scope_id: "84762861-af55-475d-9e7f-3582c61fbafe".to_owned(),
        workspace_access: WorkspaceAccess::Deny,
    };
    for (index, (title, description)) in TASKS.iter().enumerate() {
        let marker = uuid::Uuid::new_v4();
        let arguments = json!({
            "operation": "task.propose",
            "payload": {
                "title": title,
                "description": description,
                "priority": (index as i64) + 1,
            },
            "dedupe_key": format!("simple-todo-task-{index}-{marker}"),
            "correlation_id": format!("simple-todo-tasks-{marker}"),
        });
        match provider
            .propose(
                "cceb9983-0265-42c8-98d7-98e86097eb4f",
                &scope,
                "task.propose",
                arguments,
            )
            .await
        {
            Ok(result) => println!(
                "OK {index}: action={} status={} version={}",
                result
                    .get("action_id")
                    .or_else(|| result.get("id"))
                    .cloned()
                    .unwrap_or_default(),
                result.get("status").cloned().unwrap_or_default(),
                result.get("version").cloned().unwrap_or_default(),
            ),
            Err(error) => println!("ERR {index}: {error}"),
        }
    }
}
