import type { Task, TaskExecutionObservability, TasksResponse } from '@/types/generated'

export type TaskListItem = Omit<
  Task,
  | 'description'
  | 'task_state_config'
  | 'workspace'
  | 'plan_progress'
  | 'plan_artifact'
  | 'execution_actions'
  | 'execution_evidence'
  | 'execution_blocker'
  | 'execution_observability'
> & {
  execution_observability?: Pick<TaskExecutionObservability, 'latest_execution_id'>
}

export type TaskListResponse = Omit<TasksResponse, 'items'> & { items: TaskListItem[] }
