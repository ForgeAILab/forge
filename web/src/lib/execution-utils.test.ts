import { describe, expect, it } from 'vitest'

import {
  buildExecutionChains,
  continuesParentSession,
  isResumeExecution,
  turnLabel,
} from '@/lib/execution-utils'
import type { Execution } from '@/types/generated'

function execution(overrides: Partial<Execution>): Execution {
  return {
    id: overrides.id ?? 'execution',
    task_id: 'task',
    role: overrides.role ?? 'coder',
    status: overrides.status ?? 'completed',
    created_at: overrides.created_at ?? '2026-05-02T10:00:00.000Z',
    updated_at: overrides.updated_at ?? '2026-05-02T10:00:00.000Z',
    ...overrides,
  }
}

describe('execution utils', () => {
  it('detects resume executions from dispatch metadata and executor fallback config', () => {
    expect(
      isResumeExecution(
        execution({
          executor_config_snapshot: {
            dispatch: { execution_policy: 'resume_latest_target_role_thread' },
          },
        }),
      ),
    ).toBe(true)

    expect(
      isResumeExecution(
        execution({
          executor_config_snapshot: {
            config: { resume_thread_in_place: true },
          },
        }),
      ),
    ).toBe(true)

    expect(
      isResumeExecution(
        execution({
          executor_config_snapshot: {
            dispatch: { execution_policy: 'new_execution' },
          },
        }),
      ),
    ).toBe(false)
  })

  it('groups follow-up turns under their root and sorts chains by active/latest turn', () => {
    const oldRoot = execution({
      id: 'old-root',
      created_at: '2026-05-02T10:00:00.000Z',
    })
    const child = execution({
      id: 'child',
      parent_execution_id: oldRoot.id,
      status: 'running',
      created_at: '2026-05-02T10:05:00.000Z',
      executor_config_snapshot: {
        dispatch: { execution_policy: 'resume_latest_target_role_thread' },
      },
    })
    const newerRoot = execution({
      id: 'newer-root',
      created_at: '2026-05-02T10:03:00.000Z',
    })

    const chains = buildExecutionChains([newerRoot, child, oldRoot])

    expect(chains.map((chain) => chain.root.id)).toEqual(['old-root', 'newer-root'])
    expect(chains[0].turns.map((turn) => turn.id)).toEqual(['old-root', 'child'])
    expect(turnLabel(0, oldRoot)).toBe('Initial run')
    expect(turnLabel(1, child)).toBe('Follow-up turn')
  })

  it('keeps a reviewer bound to its candidate run in its own session', () => {
    const coder = execution({
      id: 'coder',
      role: 'coder',
      agent_id: 'worker-agent',
      created_at: '2026-05-02T10:00:00.000Z',
    })
    // A reviewer points at the candidate it reviewed; that is not a turn of
    // the coder's session, even when both agents run the same model.
    const reviewer = execution({
      id: 'reviewer',
      role: 'reviewer',
      agent_id: 'reviewer-agent',
      parent_execution_id: coder.id,
      created_at: '2026-05-02T10:05:00.000Z',
    })
    const reviewerRetry = execution({
      id: 'reviewer-retry',
      role: 'reviewer',
      agent_id: 'reviewer-agent',
      parent_execution_id: reviewer.id,
      created_at: '2026-05-02T10:07:00.000Z',
    })

    const chains = buildExecutionChains([coder, reviewer, reviewerRetry])

    expect(chains.map((chain) => chain.turns.map((turn) => turn.id))).toEqual([
      ['reviewer', 'reviewer-retry'],
      ['coder'],
    ])
    expect(continuesParentSession(reviewer, coder)).toBe(false)
    expect(continuesParentSession(reviewerRetry, reviewer)).toBe(true)
  })
})
