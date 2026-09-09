import { fireEvent, render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { AnalyticsTab } from './AnalyticsTab'

const hooks = vi.hoisted(() => ({
  useProjectAnalytics: vi.fn(),
}))

vi.mock('@/api/hooks', () => hooks)

const zeroTokens = {
  input_tokens: 0,
  output_tokens: 0,
  cache_read_tokens: 0,
  cache_write_tokens: 0,
}

const emptyCoverage = {
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
  priced_tokens: zeroTokens,
  unpriced_tokens: zeroTokens,
  reasons: [],
}

const noUsageCost = {
  kind: 'none' as const,
  coverage: 'no_usage' as const,
  provider_reported: null,
  estimated: null,
  known_subtotal: null,
  complete_total: null,
  usage_coverage: emptyCoverage,
  sources: [],
}

const usage = {
  counts: {
    task_execution_count: 0,
    chat_turn_count: 0,
    inquiry_count: 0,
    provider_attempt_count: 0,
  },
  tokens: zeroTokens,
  cost: noUsageCost,
  by_surface: [],
  by_model: [],
  by_agent: [],
}

const response = {
  window: { from: null, to: null },
  ci_steps: [],
  token_usage: usage,
  review_summary: {
    total_reviews: 0,
    passed: 0,
    failed: 0,
    cancelled: 0,
    avg_duration_ms: null,
    pass_rate: 0,
  },
  outcome_economics: {
    outcome_kind: 'released_milestone' as const,
    numerator: null,
    denominator: 1,
    amount_per_outcome: null,
    scope: { project_id: 'project-1', from: null, to: null },
    eligibility: 'incomplete_cost' as const,
    ineligibility_reason: 'cost_partial' as const,
  },
}

describe('AnalyticsTab', () => {
  beforeEach(() => {
    vi.clearAllMocks()
  })

  it('exposes a busy state while Project analytics loads', () => {
    hooks.useProjectAnalytics.mockReturnValue({ data: undefined, isLoading: true, isError: false })
    render(<AnalyticsTab projectId="project-1" />)
    expect(screen.getByRole('status', { name: 'Loading project analytics' })).toBeTruthy()
  })

  it('renders the exact Project wrapper and released-milestone ineligibility reason', () => {
    hooks.useProjectAnalytics.mockReturnValue({ data: response, isLoading: false, isError: false })
    render(<AnalyticsTab projectId="project-1" />)
    expect(screen.getByText('Released-milestone economics')).toBeTruthy()
    expect(screen.getByText('Usage cost is only partially covered.')).toBeTruthy()
    expect(screen.getAllByText('No provider usage was recorded in this window.').length).toBe(4)
  })

  it('keeps a selected finite window stable across unrelated rerenders', () => {
    hooks.useProjectAnalytics.mockReturnValue({ data: response, isLoading: false, isError: false })
    const view = render(<AnalyticsTab projectId="project-1" />)

    fireEvent.click(screen.getByRole('button', { name: 'Last 7 days' }))
    const lastCall = () =>
      hooks.useProjectAnalytics.mock.calls[hooks.useProjectAnalytics.mock.calls.length - 1]
    const selectedWindow = lastCall()?.slice(1)

    view.rerender(<AnalyticsTab projectId="project-1" />)

    expect(lastCall()?.slice(1)).toEqual(selectedWindow)
  })

  it('makes the horizontally scrollable CI table keyboard-focusable and named', () => {
    hooks.useProjectAnalytics.mockReturnValue({
      data: {
        ...response,
        ci_steps: [
          {
            command: 'pnpm test',
            total_runs: 1,
            pass_count: 1,
            fail_count: 0,
            success_rate: 1,
            avg_duration_ms: 100,
            p50_duration_ms: 100,
            p95_duration_ms: 100,
            last_run_at: '2026-09-07T12:00:00Z',
          },
        ],
      },
      isLoading: false,
      isError: false,
    })
    render(<AnalyticsTab projectId="project-1" />)

    const region = screen.getByRole('region', { name: 'CI step outcomes' })
    expect(region.getAttribute('tabindex')).toBe('0')
  })
})
