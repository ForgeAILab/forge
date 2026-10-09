import { describe, expect, it } from 'vitest'

import {
  blockedInterruption,
  checkWait,
  checkWaitNotice,
  deriveColumns,
  getBlockingAnnotation,
  getStaleBlockingAnnotation,
  getTaskWorkflowWarning,
  isTaskBlocked,
  matchesFilters,
  outgoingWorkflowEdges,
  taskTypes,
  taskHasError,
  workflowTriggerTargets,
} from './workflow-utils'
import type { StateDefinition, StateKind, Task, WorkflowDefinition } from '@/types/generated'
import { emptyUsage } from '@/test-utils/usage'
import { taskListItem } from '@/test-utils/task-list-item'

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

describe('failed and blocked records', () => {
  const record = { kind: 'executor_failed' as const, reason: 'stored reason', created_at: '2026-10-07T00:00:00Z' }
  const withDetails = (failed: boolean, blocked: boolean) =>
    taskListItem({
      // A failure record does not make the condition kind `failed` while a run is live.
      condition: {
        kind: 'clear',
        details: { failure_kind: 'executor_failed', diagnostic: null, interruption: record, failed, blocked, human_wait: blocked, entry_wait: false },
      },
    })

  it('does not treat a failed-only Task as blocked', () => {
    const failedOnly = withDetails(true, false)
    expect(isTaskBlocked(failedOnly)).toBe(false)
    expect(blockedInterruption(failedOnly)).toBeNull()
    expect(matchesFilters(failedOnly, { types: [], blockedOnly: true })).toBe(false)
    expect(taskHasError(failedOnly)).toBe(true)
  })

  it('keeps a blocked Task blocked, with or without a failure beside it', () => {
    for (const task of [withDetails(false, true), withDetails(true, true)]) {
      expect(isTaskBlocked(task)).toBe(true)
      expect(blockedInterruption(task)?.reason).toBe('stored reason')
      expect(matchesFilters(task, { types: [], blockedOnly: true })).toBe(true)
    }
  })
})

describe('task interruption annotations', () => {
  function taskWithExecutionIds(blockedExecutionId: string, latestExecutionId: string): Task {
    return {
      id: 'task-1',
      project_id: 'project-1',
      title: 'Annotated task',
      task_type: 'task',
      description: null,
      status: 'in_progress',
      priority: 0,
      board_position: 0,
      role_assignments: [],
      effective_coder: null,
      effective_coder_source: null,
      remaining_retries: {},
  retry_limits: {},
      placement: null,
      condition: { kind: 'clear', details: { failure_kind: 'executor_failed', interruption: null, failed: false, blocked: false, human_wait: false, entry_wait: false, diagnostic: {
        type: 'executor_failed',
        blocking_reason: 'executor_failed',
        blocked_by: 'system:executor',
        blocked_at: '2026-05-01T00:00:00Z',
        blocked_execution_id: blockedExecutionId,
        artifact: null,
        message: 'Previous execution failed',
      } } },
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

  it('uses compact list observability to identify stale annotations', () => {
    const fullTask = taskWithExecutionIds('execution-old', 'execution-new')
    const task = taskListItem({
      condition: fullTask.condition,
      execution_observability: { latest_execution_id: 'execution-new' },
    })
    expect(taskHasError(task)).toBe(false)
    expect(getBlockingAnnotation(task)).toBeNull()
    expect(getStaleBlockingAnnotation(task)?.message).toBe('Previous execution failed')
  })

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
    task.condition.details.diagnostic = null
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
    task.condition.details.diagnostic = null
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


describe('check wait conditions', () => {
  const waiting = (phase: 'result' | 'slot' | 'infrastructure_exhausted') =>
    taskListItem({
      condition: {
        kind: 'parked',
        primary: { kind: 'check', wait: { phase, consumer_id: 'consumer', origin: 'entry' } },
        additional: [], resume: { kind: 'reconcile' }, since: null,
        details: {
          failure_kind: null, diagnostic: null, interruption: null, failed: false, blocked: false, human_wait: false, entry_wait: false,
          owner: phase === 'infrastructure_exhausted' ? 'user' : 'check_runner',
          recovery: phase === 'infrastructure_exhausted' ? 'retry_check' : 'wait_for_check',
        },
      },
    })

  it('labels a result or slot wait as owned work, not a block or failure', () => {
    for (const [phase, title] of [['result', 'Waiting for checks'], ['slot', 'Waiting for a check slot']] as const) {
      const task = waiting(phase)
      expect(checkWait(task)?.phase).toBe(phase)
      expect(checkWaitNotice(task)).toMatchObject({ title, needsOwner: false })
      expect(isTaskBlocked(task)).toBe(false)
      expect(taskHasError(task)).toBe(false)
      expect(matchesFilters(task, { types: [], blockedOnly: true })).toBe(false)
    }
  })

  it('shows exhausted check retries as needing the owner without calling the change failed', () => {
    const task = waiting('infrastructure_exhausted')
    expect(checkWaitNotice(task)).toMatchObject({ title: 'Checks could not run', needsOwner: true })
    expect(isTaskBlocked(task)).toBe(true)
    expect(taskHasError(task)).toBe(false)
    expect(matchesFilters(task, { types: [], blockedOnly: true })).toBe(true)
  })

  it('finds a check wait behind another owner and none on an ordinary Task', () => {
    const held = waiting('result')
    if (held.condition.kind !== 'parked') throw new Error('fixture')
    held.condition.additional = [held.condition.primary]
    held.condition.primary = { kind: 'held', actor: 'user' }
    expect(checkWait(held)?.phase).toBe('result')
    expect(checkWaitNotice(taskListItem({}))).toBeNull()
  })
})

describe('integration conditions', () => {
  it('reads queue ownership without a manual block or failure', () => {
    const task = taskListItem({
      condition: {
        kind: 'parked',
        primary: { kind: 'integration', reason: { kind: 'waiting', attempt_id: 'attempt' } },
        additional: [], resume: { kind: 'integration', attempt_id: 'attempt' }, since: null,
        details: { failure_kind: null, diagnostic: null, interruption: null, failed: false, blocked: false, human_wait: false, entry_wait: false, owner: 'integration_worker', recovery: 'wait_for_integration' },
      },
    })
    expect(isTaskBlocked(task)).toBe(false)
    expect(taskHasError(task)).toBe(false)
    expect(blockedInterruption(task)).toBeNull()
    expect(matchesFilters(task, { types: [], blockedOnly: true })).toBe(false)
  })

  it('keeps another owner hold visible beside integration and shows real owner failures', () => {
    const task = taskListItem({
      condition: {
        kind: 'parked', primary: { kind: 'held', actor: 'user' },
        additional: [{ kind: 'integration', reason: { kind: 'deferred', attempt_id: 'attempt', cause: 'target_dirty', owner_id: 'owner', message: 'target dirty' } }],
        resume: { kind: 'reconcile' }, since: null,
        details: { failure_kind: 'manual_stop', diagnostic: null, interruption: { kind: 'manual_stop', reason: 'held', created_at: '' }, failed: false, blocked: true, human_wait: true, entry_wait: false },
      },
    })
    expect(isTaskBlocked(task)).toBe(true)
    expect(taskHasError(task)).toBe(true)
    expect(blockedInterruption(task)?.reason).toBe('held')
    if (task.condition.kind !== 'parked') throw new Error('fixture kind')
    task.condition.primary = task.condition.additional[0]
    task.condition.additional = []
    task.condition.details.failure_kind = 'target_repo_dirty'
    task.condition.details.interruption = { kind: 'target_repo_dirty', reason: 'target dirty', created_at: '' }
    expect(blockedInterruption(task)?.reason).toBe('target dirty')
    expect(isTaskBlocked(task)).toBe(true)
  })
})
