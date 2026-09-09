import { describe, expect, it } from 'vitest'

import {
  deriveColumns,
  getBlockingAnnotation,
  getStaleBlockingAnnotation,
  getTaskWorkflowWarning,
  outgoingWorkflowEdges,
  taskTypes,
  taskHasError,
  workflowTriggerTargets,
} from './workflow-utils'
import type { StateDefinition, StateKind, Task, WorkflowDefinition } from '@/types/generated'
import { emptyUsage } from '@/test-utils/usage'

const emptyHooks = {
  before_exit: [],
  on_exit: [],
  before_enter: [],
  on_enter: [],
  after_enter: [],
}

describe('project task types', () => {
  it('keeps Discovery available to Project-scoped task surfaces', () => {
    expect(taskTypes).toContain('discovery')
  })
})

function state(
  name: string,
  kind: StateKind,
  column: string,
  displayName: string,
): StateDefinition {
  return {
    name,
    kind,
    column,
    display_name: displayName,
    role: null,
    hooks: emptyHooks,
    canonical_phase: null,
    cleanup: null,
    gate_config: null,
    dispatch: null,
    triggers: {},
    config: {},
  }
}

describe('deriveColumns', () => {
  it('uses the state matching the column label as the primary drop target', () => {
    const workflow: WorkflowDefinition = {
      roles: [],
      states: [
        state('todo', 'initial', 'Todo', 'Todo'),
        state('planning', 'gate', 'In Progress', 'Planning'),
        state('in_progress', 'active', 'In Progress', 'In Progress'),
        state('review', 'gate', 'Review', 'Review'),
      ],
      configuration: [],
      cancellation_state: null,
    }

    const columns = deriveColumns(workflow)

    expect(columns.map((column) => column.primaryState)).toEqual(['todo', 'in_progress', 'review'])
    expect(columns[1].states).toEqual(['planning', 'in_progress'])
  })
})

describe('outgoingWorkflowEdges', () => {
  it('adds an implicit accept edge to the next declared state', () => {
    const workflow: WorkflowDefinition = {
      roles: [],
      states: [
        state('todo', 'initial', 'Todo', 'Todo'),
        state('in_progress', 'active', 'In Progress', 'In Progress'),
        state('done', 'terminal', 'Done', 'Done'),
      ],
      configuration: [],
      cancellation_state: null,
    }

    expect(outgoingWorkflowEdges(workflow, 'todo')).toEqual([
      { from: 'todo', to: 'in_progress', trigger: 'accept' },
    ])
    expect(workflowTriggerTargets(workflow, 'in_progress')).toEqual(['done'])
    expect(outgoingWorkflowEdges(workflow, 'done')).toEqual([])
  })

  it('keeps explicit accept edges authoritative', () => {
    const todo = state('todo', 'initial', 'Todo', 'Todo')
    todo.triggers = { accept: { to: 'done', dispatch: null } }
    const workflow: WorkflowDefinition = {
      roles: [],
      states: [
        todo,
        state('in_progress', 'active', 'In Progress', 'In Progress'),
        state('done', 'terminal', 'Done', 'Done'),
      ],
      configuration: [],
      cancellation_state: null,
    }

    expect(outgoingWorkflowEdges(workflow, 'todo')).toEqual([
      { from: 'todo', to: 'done', trigger: 'accept' },
    ])
  })
})

describe('task interruption annotations', () => {
  function taskWithExecutionIds(blockedExecutionId: string, latestExecutionId: string): Task {
    return {
      id: 'task-1',
      project_id: 'project-1',
      repo_id: 'repo-1',
      title: 'Annotated task',
      task_type: 'task',
      description: null,
      status: 'in_progress',
      priority: 0,
      board_position: 0,
      role_assignments: [],
      remaining_retries: {},
      blocked: null,
      failed: null,
      error_annotation: {
        type: 'executor_failed',
        blocking_reason: 'executor_failed',
        blocked_by: 'system:executor',
        blocked_at: '2026-05-01T00:00:00Z',
        blocked_execution_id: blockedExecutionId,
        artifact: null,
        message: 'Previous execution failed',
        recovery_actions: ['reexecute'],
      },
      execution_observability: {
        counts: {
          task_execution_count: 2,
          chat_turn_count: 0,
          inquiry_count: 0,
          provider_attempt_count: 0,
        },
        tokens: emptyUsage.tokens,
        cost: emptyUsage.cost,
        active_execution_id: null,
        active_role: null,
        active_started_at: null,
        active_elapsed_seconds: null,
        latest_execution_id: latestExecutionId,
        latest_execution_status: 'completed',
        latest_role: 'coder',
        latest_started_at: null,
        latest_stopped_at: null,
        latest_runtime_seconds: 1,
        total_runtime_seconds: 1,
      },
      plan_progress: null,
      version: 1,
      created_at: '2026-05-01T00:00:00Z',
      updated_at: '2026-05-01T00:00:00Z',
    }
  }

  it('treats annotations from older executions as historical warnings', () => {
    const task = taskWithExecutionIds('execution-old', 'execution-new')

    expect(taskHasError(task)).toBe(false)
    expect(getBlockingAnnotation(task)).toBeNull()
    expect(getStaleBlockingAnnotation(task)?.message).toBe('Previous execution failed')
  })

  it('keeps the annotation active when it belongs to the latest execution', () => {
    const task = taskWithExecutionIds('execution-latest', 'execution-latest')

    expect(taskHasError(task)).toBe(true)
    expect(getBlockingAnnotation(task)?.blocking_reason).toBe('executor_failed')
    expect(getStaleBlockingAnnotation(task)).toBeNull()
  })

  it('warns when completed coder work cannot leave in-progress with an open plan', () => {
    const task = taskWithExecutionIds('execution-old', 'execution-new')
    task.error_annotation = null
    task.plan_progress = {
      total: 10,
      completed: 6,
      remaining: 4,
      available: true,
      warnings: [],
    }

    expect(getTaskWorkflowWarning(task)?.message).toContain('4 checklist items are unchecked')
  })

  it('does not warn while an execution is still running', () => {
    const task = taskWithExecutionIds('execution-old', 'execution-new')
    task.error_annotation = null
    task.plan_progress = {
      total: 10,
      completed: 6,
      remaining: 4,
      available: true,
      warnings: [],
    }
    task.execution_observability = {
      ...task.execution_observability!,
      active_execution_id: 'execution-running',
      latest_execution_status: 'running',
    }

    expect(getTaskWorkflowWarning(task)).toBeNull()
  })
})
