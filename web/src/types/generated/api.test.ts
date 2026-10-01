import { expectTypeOf, it } from 'vitest'
import type { TaskListItem, TasksResponse } from './api'

it('types project task pages as compact items with required observability', () => {
  type Item = TasksResponse['items'][number]
  expectTypeOf<Item>().toEqualTypeOf<TaskListItem>()
  expectTypeOf<Item>().toEqualTypeOf<Required<Item>>()
  expectTypeOf<Item['execution_observability']>().toEqualTypeOf<{
    latest_execution_id: string | null
  }>()
  expectTypeOf<
    Extract<
      keyof Item,
      | 'description'
      | 'task_state_config'
      | 'workspace'
      | 'plan_progress'
      | 'plan_artifact'
      | 'execution_actions'
      | 'execution_evidence'
      | 'execution_blocker'
    >
  >().toEqualTypeOf<never>()
})
