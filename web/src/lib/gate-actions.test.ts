import { describe, expect, it } from 'vitest'

import { getHumanGateActions } from './gate-actions'
import type { StateDefinition, StateKind, Task, WorkflowDefinition } from '@/types/generated'

const emptyHooks = {
  before_exit: [],
  on_exit: [],
  before_enter: [],
  on_enter: [],
  after_enter: [],
}

function state(
  name: string,
  kind: StateKind,
  role: string | null,
  gateConfig: StateDefinition['gate_config'] = null,
): StateDefinition {
  return {
    name,
    kind,
    column: name,
    display_name: name.replace(/_/g, ' '),
    role,
    hooks: emptyHooks,
    canonical_phase: null,
    cleanup: null,
    gate_config: gateConfig,
    dispatch: null,
    triggers: {},
    config: {},
  }
}

function workflow(
  states: StateDefinition[],
  edges: Array<{ from: string; to: string; trigger: string }> = [],
): WorkflowDefinition {
  for (const edge of edges) {
    const source = states.find((candidate) => candidate.name === edge.from)
    if (source) {
      source.triggers = {
        ...source.triggers,
        [edge.trigger]: { to: edge.to, dispatch: null },
      }
    }
  }
  return {
    roles: [],
    states,
    configuration: [],
    cancellation_state: 'cancelled',
  }
}

function task(
  status: string,
  roleName: string,
  awaitingHuman = true,
  assigneeType: 'agent' | 'user' = 'user',
): Task & { awaiting_human: boolean } {
  return {
    id: 'task-1',
    project_id: 'project-1',
    title: 'Task',
    task_type: 'task',
    status,
    awaiting_human: awaitingHuman,
    priority: 0,
    board_position: 1,
    role_assignments: [
      {
        id: 'assignment-1',
        task_id: 'task-1',
        role_name: roleName,
        assignee_type: assigneeType,
        assignee_id: assigneeType === 'user' ? 'human' : 'agent-1',
        created_at: '',
        updated_at: '',
      },
    ],
    remaining_retries: {},
    version: 1,
    created_at: '',
    updated_at: '',
  }
}

describe('getHumanGateActions', () => {
  it('uses configured labels and exposes approve/reject for a review-style gate', () => {
    const actions = getHumanGateActions(
      task('review', 'reviewer'),
      workflow(
        [
          state('in_progress', 'active', 'coder'),
          state('review', 'gate', 'reviewer', {
            reject_target: null,
            max_rejections: 2,
            approve_label: 'Ship it',
            reject_label: 'Needs work',
            requires_user_approval: false,
          }),
          state('merging', 'gate', null),
        ],
        [
          { from: 'review', to: 'merging', trigger: 'accept' },
          { from: 'review', to: 'in_progress', trigger: 'reject' },
        ],
      ),
    )

    expect(actions).toEqual({
      stateName: 'review',
      approveLabel: 'Ship it',
      rejectLabel: 'Needs work',
    })
  })

  it('falls back to state display text and exposes configured self-reject targets', () => {
    const actions = getHumanGateActions(
      task('planning', 'planner'),
      workflow(
        [
          state('planning', 'gate', 'planner', {
            reject_target: 'planning',
            max_rejections: 2,
            approve_label: null,
            reject_label: null,
            requires_user_approval: false,
          }),
          state('in_progress', 'active', 'coder'),
          state('blocked', 'custom', null),
        ],
        [
          { from: 'planning', to: 'planning', trigger: 'reject' },
          { from: 'planning', to: 'in_progress', trigger: 'accept' },
          { from: 'planning', to: 'blocked', trigger: 'fail' },
        ],
      ),
    )

    expect(actions).toEqual({
      stateName: 'planning',
      approveLabel: 'Approve planning',
      rejectLabel: 'Reject planning',
    })
  })

  it('does not show gate actions when the gate is not held by a human', () => {
    const actions = getHumanGateActions(
      task('review', 'reviewer', false, 'agent'),
      workflow(
        [state('review', 'gate', 'reviewer'), state('merging', 'gate', null)],
        [{ from: 'review', to: 'merging', trigger: 'accept' }],
      ),
    )

    expect(actions).toBeNull()
  })

  it('does not show approve/reject controls for blocked gate states', () => {
    const blockedTask: Task = {
      ...task('merging', 'coder'),
      blocked: {
        reason: 'target repository has uncommitted changes',
        created_at: '2026-05-01T00:00:00Z',
        kind: 'target_repo_dirty',
        source: 'system:merge',
        execution_id: null,
      },
    }

    const actions = getHumanGateActions(
      blockedTask,
      workflow(
        [
          state('merging', 'gate', null),
          state('merge_failed', 'active', 'coder'),
          state('done', 'terminal', null),
        ],
        [
          { from: 'merging', to: 'done', trigger: 'accept' },
          { from: 'merging', to: 'merge_failed', trigger: 'retry' },
        ],
      ),
    )

    expect(actions).toBeNull()
  })
})

describe('explicit rejection targets', () => {
  it('does not infer rejection from failure-suffixed state names', () => {
    const actions = getHumanGateActions(
      task('qa', 'reviewer'),
      workflow(
        [
          state('qa', 'gate', 'reviewer'),
          state('done', 'active', 'coder'),
          state('qa_failed', 'custom', null),
        ],
        [
          { from: 'qa', to: 'done', trigger: 'accept' },
          { from: 'qa', to: 'qa_failed', trigger: 'route' },
        ],
      ),
    )

    // The qa_failed edge has no reject/fail trigger and no configured
    // reject_target, so its name alone must not produce a reject button.
    expect(actions?.approveLabel).toBe('Approve qa')
    expect(actions?.rejectLabel).toBeUndefined()
  })

  it('offers rejection when the workflow declares it explicitly', () => {
    const actions = getHumanGateActions(
      task('qa', 'reviewer'),
      workflow(
        [
          state('qa', 'gate', 'reviewer'),
          state('done', 'active', 'coder'),
          state('qa_failed', 'custom', null),
        ],
        [
          { from: 'qa', to: 'done', trigger: 'accept' },
          { from: 'qa', to: 'qa_failed', trigger: 'reject' },
        ],
      ),
    )

    expect(actions?.rejectLabel).toBe('Reject qa')
  })
})

describe('no rejection inference from extra edges', () => {
  it('does not infer rejection from a second active edge', () => {
    const actions = getHumanGateActions(
      task('qa', 'reviewer'),
      workflow(
        [
          state('qa', 'gate', 'reviewer'),
          state('done', 'active', 'coder'),
          state('triage', 'active', 'coder'),
        ],
        [
          { from: 'qa', to: 'done', trigger: 'accept' },
          { from: 'qa', to: 'triage', trigger: 'route' },
        ],
      ),
    )

    // A second active-kind edge is not a rejection declaration; only a
    // reject/fail trigger or gate_config.reject_target produces the button.
    expect(actions?.approveLabel).toBe('Approve qa')
    expect(actions?.rejectLabel).toBeUndefined()
  })
})
