import { describe, expect, it } from 'vitest'
import type { IntegrationWait, TaskIntegrationSummary } from '@/types/generated'
import {
  integrationLabel,
  integrationNotice,
  integrationTone,
  integrationWaitText,
} from './integration-queue'

function wait(code: IntegrationWait['code'], overrides: Partial<IntegrationWait> = {}): IntegrationWait {
  return { code, message: null, machine_id: null, machine_name: null, retry_at: null, ...overrides }
}

function summary(overrides: Partial<TaskIntegrationSummary>): TaskIntegrationSummary {
  return {
    attempt_id: 'attempt',
    queue_id: 'queue',
    repo_id: 'repo',
    target_branch: 'main',
    queue_state: 'open',
    state: 'waiting',
    phase: null,
    position: null,
    wait: null,
    action_hint: null,
    timings: {
      enqueued_at: '2026-10-01T00:00:00Z',
      head_since: null,
      rounds: 0,
      queued_ms: null,
      rebase_ms: null,
      check_ms: null,
      fast_forward_ms: null,
    },
    ...overrides,
  }
}

describe('integrationLabel', () => {
  it('counts the members ahead of a Task in line', () => {
    expect(integrationLabel(summary({ position: 2, wait: wait('queue') }))).toBe(
      'In merge queue (2 ahead)',
    )
    expect(integrationLabel(summary({ position: 1, wait: wait('queue') }))).toBe(
      'In merge queue (1 ahead)',
    )
    expect(integrationLabel(summary({ position: 0 }))).toBe('In merge queue (next)')
  })

  it('names the phase of the head', () => {
    const phases = [
      ['validating', 'Merging: validating'],
      ['rebasing', 'Merging: rebasing'],
      ['checking', 'Merging: checking'],
      ['awaiting_carry', 'Merging: carrying its review'],
      ['awaiting_authorization', 'Merging: authorizing'],
      ['fast_forwarding', 'Merging: fast-forwarding'],
      ['reconciling', 'Merging: confirming the result'],
    ] as const
    for (const [phase, label] of phases) {
      expect(integrationLabel(summary({ state: 'owned', phase, position: 0 }))).toBe(label)
    }
    expect(integrationLabel(summary({ state: 'owned', position: 0 }))).toBe('Merging: starting')
  })

  it('names what a stopped Task waits for', () => {
    expect(integrationLabel(summary({ wait: wait('project_paused') }))).toBe(
      'Waiting: project paused',
    )
    expect(integrationLabel(summary({ wait: wait('task_held') }))).toBe('Waiting: held')
    expect(integrationLabel(summary({ wait: wait('queue_suspended') }))).toBe(
      'Waiting: the target checkout is not ready',
    )
    expect(
      integrationLabel(
        summary({ wait: wait('owner_offline', { machine_id: 'daemon-1', machine_name: 'build-box' }) }),
      ),
    ).toBe('Waiting for build-box')
    expect(
      integrationLabel(
        summary({ state: 'deferred', wait: wait('owner_offline', { machine_id: 'daemon-1' }) }),
      ),
    ).toBe('Waiting for daemon-1')
  })

  it('says "Fix merge" in the repair state and nowhere else', () => {
    const states: TaskIntegrationSummary['state'][] = [
      'waiting',
      'owned',
      'repair',
      'review_required',
      'candidate_check_failed',
      'deferred',
      'applied',
    ]
    for (const state of states) {
      const label = integrationLabel(summary({ state }))
      expect(label.includes('Fix merge')).toBe(state === 'repair')
    }
    expect(integrationLabel(summary({ state: 'repair' }))).toBe('Fix merge')
    expect(integrationLabel(summary({ state: 'review_required' }))).toBe('Merge needs a fresh review')
    expect(integrationLabel(summary({ state: 'candidate_check_failed' }))).toBe('Merge check failed')
    expect(integrationLabel(summary({ state: 'applied' }))).toBe('Merged')
  })

  it('names the cause of a merge that stopped', () => {
    expect(integrationLabel(summary({ state: 'deferred', wait: wait('target_dirty') }))).toBe(
      'Merge stopped: the target checkout has local changes',
    )
    expect(
      integrationLabel(
        summary({ state: 'deferred', wait: wait('unresolved_result', { machine_name: 'build-box' }) }),
      ),
    ).toBe('Merge stopped: merge result unknown on build-box')
    expect(integrationLabel(summary({ state: 'deferred' }))).toBe('Merge stopped')
  })

  it('has words for every wait code', () => {
    const codes: IntegrationWait['code'][] = [
      'queue',
      'queue_suspended',
      'queue_quarantined',
      'task_held',
      'task_blocked',
      'task_failed',
      'task_entry_blocked',
      'task_awaiting_human',
      'project_paused',
      'owner_offline',
      'target_dirty',
      'infrastructure',
      'budget_exhausted',
      'owner_required',
      'unresolved_result',
      'cancel_requested',
    ]
    for (const code of codes) {
      expect(integrationWaitText(wait(code)).length).toBeGreaterThan(0)
    }
  })
})

describe('integrationTone and integrationNotice', () => {
  it('reads a head as progress, a line as a wait and a stop as attention', () => {
    expect(integrationTone(summary({ state: 'owned', phase: 'checking' }))).toBe('progress')
    expect(integrationTone(summary({ position: 1, wait: wait('queue') }))).toBe('waiting')
    expect(integrationTone(summary({ wait: wait('task_held'), action_hint: 'release' }))).toBe(
      'attention',
    )
    expect(integrationTone(summary({ state: 'deferred', action_hint: 'retry' }))).toBe('attention')
    expect(integrationTone(summary({ state: 'repair' }))).toBe('attention')
  })

  it('tells the owner the target, the line and the action that moves it', () => {
    expect(integrationNotice(summary({ position: 2, wait: wait('queue') }))).toEqual({
      title: 'In merge queue (2 ahead)',
      message: 'Target branch main. 2 Tasks merge before this one.',
      tone: 'waiting',
    })
    expect(integrationNotice(summary({ position: 1, wait: wait('queue') })).message).toBe(
      'Target branch main. 1 Task merges before this one.',
    )
    expect(
      integrationNotice(
        summary({
          state: 'deferred',
          wait: wait('target_dirty', { message: 'the target checkout has uncommitted changes' }),
          action_hint: 'retry',
        }),
      ),
    ).toEqual({
      title: 'Merge stopped: the target checkout has local changes',
      message:
        'Target branch main. The target checkout has uncommitted changes. Retry the merge to put the Task back in line.',
      tone: 'attention',
    })
    expect(
      integrationNotice(summary({ wait: wait('project_paused'), action_hint: 'resume_project' }))
        .message,
    ).toBe('Target branch main. Resume the Project to continue the merge.')
  })
})

describe('values a newer server sends', () => {
  // The enums are open: the web must read a state, phase, wait code or hint
  // it has no words for without throwing and without printing `undefined`.
  const unknown = <T,>(value: string) => value as unknown as T
  it('labels an unknown state, phase, cause and hint neutrally', () => {
    const state = summary({ state: unknown('speculating'), position: 2 })
    expect(integrationLabel(state)).toBe('In merge queue')
    expect(integrationTone(state)).toBe('waiting')
    expect(integrationNotice(state)).toEqual({
      title: 'In merge queue',
      message: 'Target branch main.',
      tone: 'waiting',
    })
    expect(integrationLabel(summary({ state: 'owned', phase: unknown('bisecting') }))).toBe(
      'Merging: in progress',
    )
    const cause = wait(unknown('merge_train'), { message: 'the merge train is forming' })
    expect(integrationWaitText(cause)).toBe('the merge train is forming')
    expect(integrationWaitText(wait(unknown('merge_train')))).toBe('see the Task for details')
    const waiting = summary({ wait: cause, action_hint: unknown('reorder') })
    expect(integrationLabel(waiting)).toBe('Waiting: the merge train is forming')
    const notice = integrationNotice(waiting)
    expect(notice).toEqual({
      title: 'Waiting: the merge train is forming',
      message: 'Target branch main.',
      tone: 'attention',
    })
    expect(JSON.stringify(notice)).not.toContain('undefined')
    expect(
      integrationLabel(summary({ state: 'deferred', wait: wait(unknown('merge_train')) })),
    ).toBe('Merge stopped: see the Task for details')
  })

  it('has words for every state, phase and cause this build knows', () => {
    const states = ['waiting', 'owned', 'repair', 'review_required', 'candidate_check_failed', 'deferred', 'applied'] as const
    for (const state of states) {
      expect(integrationLabel(summary({ state }))).not.toMatch(/undefined|^In merge queue$/)
    }
    const codes = [
      'queue', 'queue_suspended', 'queue_quarantined', 'task_held', 'task_blocked', 'task_failed',
      'task_entry_blocked', 'task_awaiting_human', 'project_paused', 'owner_offline', 'target_dirty',
      'infrastructure', 'budget_exhausted', 'owner_required', 'unresolved_result', 'cancel_requested',
    ] as const
    for (const code of codes) {
      expect(integrationWaitText(wait(code))).not.toMatch(/undefined|see the Task/)
    }
  })
})
