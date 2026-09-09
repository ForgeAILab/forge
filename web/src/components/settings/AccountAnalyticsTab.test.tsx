import { fireEvent, render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { AccountAnalyticsTab } from './AccountAnalyticsTab'

const hooks = vi.hoisted(() => ({
  useAccountUsageAnalytics: vi.fn(),
}))

vi.mock('@/api/hooks', () => hooks)

const tokens = {
  input_tokens: 10,
  output_tokens: 4,
  cache_read_tokens: 0,
  cache_write_tokens: 0,
}

const counts = {
  task_execution_count: 1,
  chat_turn_count: 1,
  inquiry_count: 1,
  provider_attempt_count: 1,
}

const cost = {
  kind: 'none' as const,
  coverage: 'no_usage' as const,
  provider_reported: null,
  estimated: null,
  known_subtotal: null,
  complete_total: null,
  usage_coverage: {
    total_runs_or_turns: 0,
    pending_runs_or_turns: 0,
    no_provider_call_runs_or_turns: 0,
    fully_metered_runs_or_turns: 0,
    fully_costed_runs_or_turns: 0,
    partially_costed_runs_or_turns: 0,
    unavailable_cost_runs_or_turns: 0,
    total_provider_attempts: 0,
    settled_provider_attempts: 0,
    pending_provider_attempts: 0,
    unsettled_provider_attempts: 0,
    metered_provider_attempts: 0,
    unmetered_provider_attempts: 0,
    costed_provider_attempts: 0,
    unpriced_provider_attempts: 0,
    priced_tokens: tokens,
    unpriced_tokens: tokens,
    reasons: [],
  },
  sources: [],
}

const emptyAnalytics = {
  counts,
  tokens,
  cost,
  by_surface: [],
  by_model: [],
  by_agent: [],
}

describe('AccountAnalyticsTab', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('exposes an accessible busy state while account usage loads', () => {
    hooks.useAccountUsageAnalytics.mockReturnValue({
      data: undefined,
      isLoading: true,
      isError: false,
    })
    render(<AccountAnalyticsTab />)
    expect(screen.getByRole('status', { name: 'Loading account analytics' })).toBeTruthy()
  })

  it('renders account usage and Project grouping from the exact response wrapper', () => {
    hooks.useAccountUsageAnalytics.mockReturnValue({
      data: {
        window: { from: null, to: null },
        token_usage: emptyAnalytics,
        by_project: [
          {
            project_id: 'project-1',
            project_name_snapshot: 'Project One',
            counts,
            tokens,
            cost,
          },
        ],
      },
      isLoading: false,
      isError: false,
    })
    render(<AccountAnalyticsTab />)
    expect(screen.getByText('Account usage and cost')).toBeTruthy()
    expect(screen.getByText('Project grouping')).toBeTruthy()
    expect(screen.getByText('Project One')).toBeTruthy()
    expect(screen.getByText(/All available activity/)).toBeTruthy()
  })

  it('keeps a selected finite window stable across unrelated rerenders', () => {
    hooks.useAccountUsageAnalytics.mockReturnValue({
      data: {
        window: { from: null, to: null },
        token_usage: emptyAnalytics,
        by_project: [],
      },
      isLoading: false,
      isError: false,
    })
    const view = render(<AccountAnalyticsTab />)

    fireEvent.click(screen.getByRole('button', { name: 'Last 7 days' }))
    const lastCall = () =>
      hooks.useAccountUsageAnalytics.mock.calls[
        hooks.useAccountUsageAnalytics.mock.calls.length - 1
      ]
    const selectedWindow = lastCall()?.slice(0)

    view.rerender(<AccountAnalyticsTab />)

    expect(lastCall()?.slice(0)).toEqual(selectedWindow)
  })
})
