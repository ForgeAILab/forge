import { cleanup, render, screen } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import type { TransitionLogEntry } from '@/types/generated'
import { TransitionTimeline } from './transition-timeline'

const { query } = vi.hoisted(() => ({ query: vi.fn() }))
vi.mock('@/api/hooks', () => ({ useTransitionLogQuery: query }))
afterEach(cleanup)

function entry(bridge_kind: TransitionLogEntry['bridge_kind'], trigger_reason: string): TransitionLogEntry {
  return { id: 'transition', task_id: 'task', from_state: 'review', to_state: 'merging',
    triggered_by: 'system:workflow', trigger_reason, bridge_kind, bridge_payload: null,
    hook_results_json: [], rejection: false, created_at: '2026-10-05T12:00:00Z' }
}

describe('typed transition history', () => {
  it('shows the CI-only badge from typed history with arbitrary prose', () => {
    query.mockReturnValue({ data: [entry('ci_only_review_passed', 'Checks passed')], isLoading: false, isError: false })
    render(<TransitionTimeline taskId="task" />)
    expect(screen.getByText('CI-only re-review')).not.toBeNull()
  })
  it('labels a re-plan and a skipped step check from the typed bridge kind', () => {
    query.mockReturnValue({
      data: [
        { ...entry('replan', 'subtask parked'), id: 'a', from_state: 'in_progress', to_state: 'planning' },
        { ...entry('gate_skipped', 'no assignee'), id: 'b', from_state: 'lead_check', to_state: 'done' },
        { ...entry('gate_skipped', 'no assignee'), id: 'c', from_state: 'planning', to_state: 'in_progress' },
      ],
      isLoading: false,
      isError: false,
    })
    render(<TransitionTimeline taskId="task" />)
    expect(screen.getByText('re-plan by the lead')).not.toBeNull()
    expect(screen.getByText('no lead: check skipped')).not.toBeNull()
    expect(screen.getByText('gate skipped')).not.toBeNull()
    // The inherited state is named, not left as an unknown-state fallback.
    expect(screen.getByText('lead check').className).not.toContain('purple')
  })
  it('adds no bridge label to an ordinary or a rejected transition', () => {
    query.mockReturnValue({
      data: [entry(null, 'user action'), { ...entry('gate_rejected', 'returned'), id: 'r' }],
      isLoading: false,
      isError: false,
    })
    render(<TransitionTimeline taskId="task" />)
    expect(screen.queryByText(/re-plan|skipped/)).toBeNull()
  })
  it('does not classify marker words in ordinary audit prose', () => {
    query.mockReturnValue({ data: [entry(null, 'CI-only pass_ci_only [review-refresh]')], isLoading: false, isError: false })
    render(<TransitionTimeline taskId="task" />)
    expect(screen.queryByText('CI-only re-review')).toBeNull()
    expect(screen.getByText('CI-only pass_ci_only [review-refresh]')).not.toBeNull()
  })
})
