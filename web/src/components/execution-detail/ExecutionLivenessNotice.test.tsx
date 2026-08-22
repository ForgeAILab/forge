import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import type { Execution } from '@/types/generated'

import { ExecutionLivenessNotice } from './ExecutionLivenessNotice'

const baseExecution: Execution = {
  id: 'execution-1',
  task_id: 'task-1',
  role: 'worker',
  status: 'running',
  owner_health: 'healthy',
  created_at: '2026-08-21T12:00:00Z',
  updated_at: '2026-08-21T12:01:00Z',
}

describe('ExecutionLivenessNotice', () => {
  it('separates an expired owner lease from semantic progress and offers bounded recovery', () => {
    const onRetry = vi.fn()
    render(
      <ExecutionLivenessNotice
        execution={{
          ...baseExecution,
          status: 'failed',
          owner_health: 'expired',
          liveness_warning: 'owner_lease_expired',
          stop_reason: 'execution_stalled',
          last_progress_at: '2026-08-21T12:00:42Z',
          interruption: {
            reason: 'Execution owner lease expired.',
            kind: 'owner_lease_expired',
            created_at: '2026-08-21T12:01:05Z',
          },
        }}
        actions={{ onRetry }}
        nextActionLabel="Retry run"
      />,
    )

    const notice = screen.getByRole('alert')
    expect(notice.textContent).toContain('Owner lease expired')
    expect(notice.textContent).toContain('owner_lease_expired')
    expect(notice.textContent).toContain('Last semantic progress')
    expect(notice.textContent).toContain('Execution owner lease expired.')

    fireEvent.click(screen.getByRole('button', { name: 'Retry run' }))
    expect(onRetry).toHaveBeenCalledOnce()
  })

  it('shows hard-deadline interruption as a live status with refresh', () => {
    const onRefresh = vi.fn()
    render(
      <ExecutionLivenessNotice
        execution={{
          ...baseExecution,
          liveness_warning: 'hard_deadline_reached',
          hard_deadline_at: '2026-08-21T12:02:00Z',
          last_heartbeat_at: '2026-08-21T12:01:30Z',
        }}
        actions={{ onRefresh }}
      />,
    )

    const notice = screen.getByRole('status')
    expect(notice.textContent).toContain('Hard deadline reached')
    expect(notice.textContent).toContain('Hard deadline')
    expect(notice.textContent).toContain('A heartbeat cannot extend it')

    fireEvent.click(screen.getByRole('button', { name: 'Refresh run' }))
    expect(onRefresh).toHaveBeenCalledOnce()
  })

  it('keeps a pending recovery action motion-safe and full width on compact screens', () => {
    render(
      <ExecutionLivenessNotice
        execution={{ ...baseExecution, liveness_warning: 'hard_deadline_reached' }}
        actions={{ onRefresh: vi.fn(), refreshPending: true }}
      />,
    )

    const button = screen.getByRole('button', { name: 'Refresh run…' })
    expect(button.className).toContain('w-full')
    expect(button.querySelector('.motion-reduce\\:animate-none')).toBeTruthy()
  })

  it('does not add a liveness warning for a healthy running owner', () => {
    render(<ExecutionLivenessNotice execution={baseExecution} />)

    expect(screen.queryByRole('status')).toBeNull()
    expect(screen.queryByRole('alert')).toBeNull()
  })

  it('does not infer a semantic warning from an old progress timestamp', () => {
    render(
      <ExecutionLivenessNotice
        execution={{
          ...baseExecution,
          last_progress_at: '2020-01-01T00:00:00Z',
        }}
      />,
    )

    expect(screen.queryByRole('status')).toBeNull()
    expect(screen.queryByRole('alert')).toBeNull()
  })
})
