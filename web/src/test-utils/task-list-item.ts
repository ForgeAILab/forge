import type { TaskListItem } from '@/types/generated'

export function taskListItem(overrides: Partial<TaskListItem> = {}): TaskListItem {
  return {
    id: 'task-1',
    project_id: 'project-1',
    parent_task_id: null,
    assignee_type: null,
    assignee_id: null,
    title: 'Task',
    task_type: 'task',
    status: 'todo',
    canonical_phase: 'ready',
    awaiting_human: false,
    priority: 0,
    board_position: 0,
    subtask_order: null,
    role_assignments: [],
    remaining_retries: {},
  retry_limits: {},
    condition: { kind: 'clear', details: { failure_kind: null, diagnostic: null, interruption: null, human_wait: false, entry_wait: false } },
    workflow_health: null,
    workflow_exception: null,
    review_passed_at: null,
    archived_at: null,
    external_issue_number: null,
    external_issue_url: null,
    execution_observability: { latest_execution_id: null },
    version: 1,
    created_at: '2026-01-01T00:00:00Z',
    updated_at: '2026-01-01T00:00:00Z',
    ...overrides,
  }
}
