use std::sync::Arc;

use db::SqliteDb;
use events::EventBus;
use services::{AgentChatService, AgentService, TaskService};

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<SqliteDb>,
    pub task_service: Arc<TaskService>,
    pub agent_service: Arc<AgentService>,
    pub agent_chat_service: Arc<AgentChatService<SqliteDb>>,
    pub event_bus: Arc<EventBus>,
    pub workspace_backend_router: Arc<services::workspace_backend::WorkspaceBackendRouter>,
}

impl AppState {
    #[cfg(test)]
    pub fn new(db: Arc<SqliteDb>, event_bus: Arc<EventBus>) -> Self {
        let task_service = Arc::new(TaskService::new_for_test(
            Arc::clone(&db),
            Arc::clone(&event_bus),
        ));
        let agent_service = Arc::new(AgentService::new(Arc::clone(&db), Arc::clone(&event_bus)));
        Self::with_task_service(db, event_bus, task_service, agent_service)
    }

    pub fn with_task_service(
        db: Arc<SqliteDb>,
        event_bus: Arc<EventBus>,
        task_service: Arc<TaskService>,
        agent_service: Arc<AgentService>,
    ) -> Self {
        let agent_chat_service = Arc::new(AgentChatService::new(Arc::clone(&db)));
        let workspace_backend_router = task_service.workspace_backend_router();
        Self {
            db,
            task_service,
            agent_service,
            agent_chat_service,
            event_bus,
            workspace_backend_router,
        }
    }
}
