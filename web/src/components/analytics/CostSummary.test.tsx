import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import type { CostSourceRef, CostSummary, UsageCostCoverage } from '@/types/generated'
import { CostSummaryView } from './CostSummary'

const zeroTokens = {
  input_tokens: 0,
  output_tokens: 0,
  cache_read_tokens: 0,
  cache_write_tokens: 0,
}

function coverage(overrides: Partial<UsageCostCoverage> = {}): UsageCostCoverage {
  return {
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
    priced_tokens: zeroTokens,
    unpriced_tokens: zeroTokens,
    reasons: [],
    ...overrides,
  }
}

function source(overrides: Partial<CostSourceRef> = {}): CostSourceRef {
  return {
    source_kind: 'models_dev_catalog',
    rate_revision_id: 'rate-1',
    catalog_snapshot_id: 'catalog-1',
    catalog_digest: 'digest-1',
    effective_at: '2026-09-07T12:00:00Z',
    fetched_at: '2026-09-07T12:00:00Z',
    freshness: 'fresh',
    retrospective: false,
    formula_revision: 'fixed-point-v1',
    ...overrides,
  }
}

function summary(overrides: Partial<CostSummary> = {}): CostSummary {
  return {
    kind: 'provider_reported',
    coverage: 'complete',
    provider_reported: { currency: 'USD', decimal: '1.25' },
    estimated: null,
    known_subtotal: { currency: 'USD', decimal: '1.25' },
    complete_total: { currency: 'USD', decimal: '1.25' },
    usage_coverage: coverage(),
    sources: [
      source({
        source_kind: 'provider_reported',
        rate_revision_id: null,
        catalog_snapshot_id: null,
      }),
    ],
    ...overrides,
  }
}

describe('CostSummaryView', () => {
  it('shows reported and estimated amounts separately and only shows a complete total when complete', () => {
    render(
      <CostSummaryView
        summary={summary({
          kind: 'mixed',
          provider_reported: { currency: 'USD', decimal: '1.25' },
          estimated: { currency: 'USD', decimal: '0.000001' },
          known_subtotal: { currency: 'USD', decimal: '1.250001' },
          complete_total: { currency: 'USD', decimal: '1.250001' },
          sources: [
            source({ source_kind: 'provider_reported' }),
            source({ source_kind: 'models_dev_catalog' }),
          ],
        })}
      />,
    )

    expect(screen.getByText('Provider-reported')).toBeTruthy()
    expect(screen.getByText('Estimated')).toBeTruthy()
    expect(screen.getByText('Known subtotal')).toBeTruthy()
    expect(screen.getByText('Complete total')).toBeTruthy()
    expect(screen.getByText('$0.000001')).toBeTruthy()
    expect(screen.getByText('models.dev catalog · Fresh at admission')).toBeTruthy()
  })

  it('renders an explicit zero as zero without turning missing amounts into zero', () => {
    const { unmount } = render(
      <CostSummaryView
        summary={summary({
          provider_reported: { currency: 'USD', decimal: '0' },
          known_subtotal: { currency: 'USD', decimal: '0' },
          complete_total: { currency: 'USD', decimal: '0' },
        })}
      />,
    )
    expect(screen.getAllByText('$0.00').length).toBeGreaterThanOrEqual(2)
    unmount()

    render(
      <CostSummaryView
        summary={summary({
          kind: 'unknown',
          coverage: 'unavailable',
          provider_reported: null,
          estimated: null,
          known_subtotal: null,
          complete_total: null,
          usage_coverage: coverage({
            fully_costed_runs_or_turns: 0,
            unavailable_cost_runs_or_turns: 1,
            costed_provider_attempts: 0,
            unpriced_provider_attempts: 1,
            reasons: [
              {
                code: 'missing_binding',
                run_or_turn_count: 1,
                provider_attempt_count: 1,
                tokens: { ...zeroTokens, input_tokens: 3 },
              },
            ],
          }),
          sources: [],
        })}
      />,
    )
    expect(
      screen.getByText(
        'Cost unknown: settled provider attempts have no usable reported amount or exact rate.',
      ),
    ).toBeTruthy()
    expect(screen.queryByText('$0.00')).toBeNull()
  })

  it('explains when unavailable cost is caused by terminally unsettled attempts', () => {
    render(
      <CostSummaryView
        summary={summary({
          kind: 'unknown',
          coverage: 'unavailable',
          provider_reported: null,
          estimated: null,
          known_subtotal: null,
          complete_total: null,
          usage_coverage: coverage({
            fully_costed_runs_or_turns: 0,
            unavailable_cost_runs_or_turns: 1,
            settled_provider_attempts: 0,
            unsettled_provider_attempts: 1,
            costed_provider_attempts: 0,
            unpriced_provider_attempts: 1,
            reasons: [
              {
                code: 'unsettled',
                run_or_turn_count: 1,
                provider_attempt_count: 1,
                tokens: zeroTokens,
              },
            ],
          }),
          sources: [],
        })}
      />,
    )

    expect(screen.getByRole('status').textContent).toBe(
      'Cost unknown: one or more provider attempts ended terminally unsettled before a usable cost was recorded.',
    )
  })

  it('keeps pending, partial, and no-usage states truthful', () => {
    const cases: Array<{ coverage: CostSummary['coverage']; message: string }> = [
      {
        coverage: 'pending',
        message: 'Cost pending: one or more provider attempts still need settlement.',
      },
      {
        coverage: 'partial',
        message:
          'Partial coverage: some provider attempts are costed, but a complete total is unavailable.',
      },
      { coverage: 'no_usage', message: 'No provider usage was recorded in this window.' },
    ]

    for (const item of cases) {
      const { unmount } = render(
        <CostSummaryView
          summary={summary({
            kind: item.coverage === 'no_usage' ? 'none' : 'estimated',
            coverage: item.coverage,
            provider_reported: null,
            estimated: item.coverage === 'partial' ? { currency: 'USD', decimal: '0.01' } : null,
            known_subtotal:
              item.coverage === 'partial' ? { currency: 'USD', decimal: '0.01' } : null,
            complete_total: null,
            usage_coverage: coverage({
              pending_runs_or_turns: item.coverage === 'pending' ? 1 : 0,
              pending_provider_attempts: item.coverage === 'pending' ? 1 : 0,
            }),
          })}
        />,
      )
      expect(screen.getByText(item.message)).toBeTruthy()
      unmount()
    }
  })

  it('shows all coverage denominators, reasons, and stale/refresh-failed provenance', () => {
    render(
      <CostSummaryView
        summary={summary({
          kind: 'unknown',
          coverage: 'partial',
          provider_reported: null,
          estimated: null,
          known_subtotal: { currency: 'USD', decimal: '0.009999999' },
          complete_total: null,
          usage_coverage: coverage({
            total_runs_or_turns: 4,
            pending_runs_or_turns: 1,
            no_provider_call_runs_or_turns: 1,
            fully_metered_runs_or_turns: 2,
            fully_costed_runs_or_turns: 1,
            partially_costed_runs_or_turns: 2,
            unavailable_cost_runs_or_turns: 1,
            total_provider_attempts: 5,
            settled_provider_attempts: 3,
            pending_provider_attempts: 1,
            unsettled_provider_attempts: 1,
            metered_provider_attempts: 2,
            unmetered_provider_attempts: 2,
            costed_provider_attempts: 1,
            unpriced_provider_attempts: 4,
            priced_tokens: { ...zeroTokens, input_tokens: 7 },
            unpriced_tokens: { ...zeroTokens, cache_write_tokens: 11 },
            reasons: [
              {
                code: 'missing_rate',
                run_or_turn_count: 1,
                provider_attempt_count: 1,
                tokens: { ...zeroTokens, cache_write_tokens: 11 },
              },
              {
                code: 'pending',
                run_or_turn_count: 1,
                provider_attempt_count: 1,
                tokens: zeroTokens,
              },
            ],
          }),
          sources: [
            source({ freshness: 'refresh_failed', source_kind: 'models_dev_catalog' }),
            source({
              freshness: 'stale',
              source_kind: 'manual_override',
              rate_revision_id: 'override-1',
            }),
          ],
        })}
      />,
    )

    for (const label of [
      'Total runs / turns',
      'Pending runs / turns',
      'No provider call',
      'Fully metered runs / turns',
      'Fully costed runs / turns',
      'Partially costed runs / turns',
      'Unavailable-cost runs / turns',
      'Total provider attempts',
      'Settled provider attempts',
      'Pending provider attempts',
      'Unsettled provider attempts',
      'Metered provider attempts',
      'Unmetered provider attempts',
      'Costed provider attempts',
      'Unpriced provider attempts',
      'Priced tokens',
      'Unpriced tokens',
    ]) {
      expect(screen.getByText(label)).toBeTruthy()
    }
    expect(screen.getByText('Missing required token rate')).toBeTruthy()
    expect(screen.getByText('Pending settlement')).toBeTruthy()
    expect(screen.getByText('models.dev catalog · Refresh failed at admission')).toBeTruthy()
    expect(screen.getByText('Manual override · Stale at admission')).toBeTruthy()
  })

  it('describes retrospective source freshness at retrospective selection', () => {
    render(
      <CostSummaryView
        summary={summary({
          sources: [source({ retrospective: true, freshness: 'stale' })],
        })}
      />,
    )

    expect(
      screen.getByText(
        'models.dev catalog · Stale at retrospective selection · Retrospective estimate',
      ),
    ).toBeTruthy()
  })
})
