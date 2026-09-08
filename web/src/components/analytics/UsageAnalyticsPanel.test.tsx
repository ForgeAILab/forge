import { render, screen, within } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import type { UsageAnalytics, UsageSurface } from '@/types/generated'
import { UsageAnalyticsPanel, ProjectUsageTable } from './UsageAnalyticsPanel'
import { ACCOUNT_USAGE_SURFACES, PROJECT_USAGE_SURFACES } from './analytics-format'

const tokens = {
  input_tokens: 12,
  output_tokens: 7,
  cache_read_tokens: 3,
  cache_write_tokens: 1,
}

const coverage = {
  total_runs_or_turns: 1,
  pending_runs_or_turns: 0,
  no_provider_call_runs_or_turns: 0,
  fully_metered_runs_or_turns: 1,
  fully_costed_runs_or_turns: 1,
  partially_costed_runs_or_turns: 0,
  unavailable_cost_runs_or_turns: 0,
  total_provider_attempts: 1,
  settled_provider_attempts: 1,
  pending_provider_attempts: 0,
  unsettled_provider_attempts: 0,
  metered_provider_attempts: 1,
  unmetered_provider_attempts: 0,
  costed_provider_attempts: 1,
  unpriced_provider_attempts: 0,
  priced_tokens: tokens,
  unpriced_tokens: {
    input_tokens: 0,
    output_tokens: 0,
    cache_read_tokens: 0,
    cache_write_tokens: 0,
  },
  reasons: [],
}

const cost = {
  kind: 'provider_reported' as const,
  coverage: 'complete' as const,
  provider_reported: { currency: 'USD' as const, decimal: '0.12' },
  estimated: null,
  known_subtotal: { currency: 'USD' as const, decimal: '0.12' },
  complete_total: { currency: 'USD' as const, decimal: '0.12' },
  usage_coverage: coverage,
  sources: [
    {
      source_kind: 'provider_reported' as const,
      rate_revision_id: null,
      catalog_snapshot_id: null,
      catalog_digest: null,
      effective_at: null,
      fetched_at: null,
      freshness: 'not_applicable' as const,
      retrospective: false,
      formula_revision: null,
    },
  ],
}

function analyticsFixture(): UsageAnalytics {
  const counts = {
    task_execution_count: 1,
    chat_turn_count: 1,
    inquiry_count: 1,
    provider_attempt_count: 1,
  }
  return {
    counts,
    tokens,
    cost,
    by_surface: ['main_inquiry', 'main_chat', 'genesis_chat', 'project_chat', 'task_execution'].map(
      (surface) => ({ surface: surface as UsageSurface, counts, tokens, cost }),
    ),
    by_model: [
      {
        provider_id: 'provider/with/a/long/id',
        model_id: 'model-with-a-long-id',
        counts,
        tokens,
        cost,
      },
    ],
    by_agent: [
      {
        agent_id: 'agent-1',
        agent_name_snapshot: 'Historical Agent',
        profile_id: 'profile-1',
        executor_type: 'direct',
        counts,
        tokens,
        cost,
      },
    ],
  }
}

describe('UsageAnalyticsPanel', () => {
  it('renders all returned surface mappings plus explicit counts and groupings', () => {
    render(<UsageAnalyticsPanel analytics={analyticsFixture()} />)

    for (const label of [
      'Task execution',
      'Project Chat',
      'Genesis Chat',
      'Main Chat',
      'Main inquiry',
    ]) {
      expect(screen.getAllByText(label).length).toBeGreaterThan(0)
    }
    for (const label of ['Task executions', 'Chat turns', 'Inquiries', 'Provider attempts']) {
      expect(screen.getAllByText(label).length).toBeGreaterThan(0)
    }
    expect(screen.getByText('By provider / model')).toBeTruthy()
    expect(screen.getByText('By agent')).toBeTruthy()
    expect(screen.getByText('Historical Agent')).toBeTruthy()
    expect(screen.getByText('provider/with/a/long/id')).toBeTruthy()
    expect(
      screen.getAllByRole('columnheader').every((header) => header.getAttribute('scope') === 'col'),
    ).toBe(true)
    expect(screen.getAllByRole('region').length).toBe(4)
    expect(
      screen
        .getAllByRole('region')
        .filter((region) => region.getAttribute('aria-label') !== null)
        .every((region) => region.getAttribute('tabindex') === '0'),
    ).toBe(true)
  })

  it('materializes every surface in scope with explicit no-usage rows', () => {
    const analytics = analyticsFixture()
    render(
      <UsageAnalyticsPanel
        analytics={{
          ...analytics,
          by_surface: [analytics.by_surface.find((row) => row.surface === 'task_execution')!],
        }}
        surfaces={PROJECT_USAGE_SURFACES}
      />,
    )

    for (const label of ['Task execution', 'Project Chat', 'Genesis Chat']) {
      expect(screen.getAllByText(label).length).toBeGreaterThan(0)
    }
    expect(screen.queryByText('Main Chat')).toBeNull()
    expect(screen.queryByText('Main inquiry')).toBeNull()
    const surfaceRegion = screen.getByRole('region', { name: 'Usage and cost by product surface' })
    expect(
      within(surfaceRegion).getAllByText('No provider usage was recorded in this window.'),
    ).toHaveLength(2)
  })

  it('materializes all account surfaces even when the API response is sparse', () => {
    const analytics = analyticsFixture()
    render(
      <UsageAnalyticsPanel
        analytics={{
          ...analytics,
          by_surface: [analytics.by_surface.find((row) => row.surface === 'task_execution')!],
        }}
        surfaces={ACCOUNT_USAGE_SURFACES}
      />,
    )

    const surfaceRegion = screen.getByRole('region', { name: 'Usage and cost by product surface' })
    expect(within(surfaceRegion).getAllByRole('row')).toHaveLength(6)
    for (const label of [
      'Task execution',
      'Project Chat',
      'Genesis Chat',
      'Main Chat',
      'Main inquiry',
    ]) {
      expect(screen.getAllByText(label).length).toBeGreaterThan(0)
    }
  })

  it('keeps account-only project grouping explicit', () => {
    render(
      <ProjectUsageTable
        rows={[
          {
            project_id: null,
            project_name_snapshot: null,
            counts: analyticsFixture().counts,
            tokens,
            cost,
          },
        ]}
      />,
    )
    expect(screen.getByText('Account-only usage')).toBeTruthy()
    expect(screen.getByText('No Project ID')).toBeTruthy()
    expect(
      screen.getByText(/account-only Main Chat and inquiry usage stays unassigned/i),
    ).toBeTruthy()
  })

  it('keeps a missing historical Project name distinct from account-only usage', () => {
    render(
      <ProjectUsageTable
        rows={[
          {
            project_id: 'project-unknown',
            project_name_snapshot: null,
            counts: analyticsFixture().counts,
            tokens,
            cost,
          },
        ]}
      />,
    )

    expect(screen.getByText('Unknown Project')).toBeTruthy()
    expect(screen.getByText('project-unknown')).toBeTruthy()
    expect(screen.queryByText('Account-only usage')).toBeNull()
  })
})
