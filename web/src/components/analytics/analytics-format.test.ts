import { describe, expect, it } from 'vitest'
import {
  formatCostFreshness,
  formatCostSummaryMessage,
  formatCoverageReason,
  formatOutcomeIneligibilityReason,
  formatUsageSurface,
  sortBySurface,
  sortCoverageReasons,
} from './analytics-format'

describe('analytics presentation vocabulary', () => {
  it('keeps every usage surface mapped to a deterministic label and order', () => {
    expect([
      formatUsageSurface('task_execution'),
      formatUsageSurface('project_chat'),
      formatUsageSurface('genesis_chat'),
      formatUsageSurface('main_chat'),
      formatUsageSurface('main_inquiry'),
    ]).toEqual(['Task execution', 'Project Chat', 'Genesis Chat', 'Main Chat', 'Main inquiry'])

    const rows = [
      { surface: 'main_inquiry' as const },
      { surface: 'task_execution' as const },
      { surface: 'genesis_chat' as const },
      { surface: 'main_chat' as const },
      { surface: 'project_chat' as const },
    ]
    expect(sortBySurface(rows).map((row) => row.surface)).toEqual([
      'task_execution',
      'project_chat',
      'genesis_chat',
      'main_chat',
      'main_inquiry',
    ])
  })

  it('uses deterministic reason copy for every coverage reason', () => {
    const reasons = [
      'invalid_legacy_usage',
      'missing_rate',
      'pending',
      'unresolved_tier',
      'identity_mismatch',
      'unmetered',
      'missing_provider',
      'missing_model',
      'missing_binding',
      'unsettled',
    ] as const
    for (const reason of reasons) {
      expect(formatCoverageReason(reason)).toBeTruthy()
    }
    expect(
      sortCoverageReasons(
        reasons.map((code) => ({
          code,
          run_or_turn_count: 1,
          provider_attempt_count: 1,
          tokens: {
            input_tokens: 0,
            output_tokens: 0,
            cache_read_tokens: 0,
            cache_write_tokens: 0,
          },
        })),
      ).map((reason) => reason.code),
    ).toEqual([
      'pending',
      'unsettled',
      'unmetered',
      'missing_provider',
      'missing_model',
      'missing_binding',
      'missing_rate',
      'unresolved_tier',
      'identity_mismatch',
      'invalid_legacy_usage',
    ])
  })

  it('keeps coverage state copy distinct', () => {
    expect(formatCostSummaryMessage('complete')).toContain('complete cost')
    expect(formatCostSummaryMessage('partial')).toContain('Partial coverage')
    expect(formatCostSummaryMessage('pending')).toContain('pending')
    expect(formatCostSummaryMessage('unavailable')).toContain('Cost unknown')
    expect(formatCostSummaryMessage('no_usage')).toContain('No provider usage')
    expect(formatCostSummaryMessage('unavailable', ['unsettled'])).toContain('terminally unsettled')
  })

  it('labels retrospective pricing freshness separately from admission freshness', () => {
    expect(formatCostFreshness('fresh')).toBe('Fresh at admission')
    expect(formatCostFreshness('fresh', true)).toBe('Fresh at retrospective selection')
    expect(formatCostFreshness('stale', true)).toBe('Stale at retrospective selection')
    expect(formatCostFreshness('not_applicable', true)).toBe('Freshness not applicable')
  })

  it('names every released-milestone ineligibility reason', () => {
    expect(formatOutcomeIneligibilityReason('no_released_milestones')).toContain('No successful')
    expect(formatOutcomeIneligibilityReason('no_usage_cost')).toContain('No usage cost')
    expect(formatOutcomeIneligibilityReason('cost_pending')).toContain('pending')
    expect(formatOutcomeIneligibilityReason('cost_partial')).toContain('partially')
    expect(formatOutcomeIneligibilityReason('cost_unavailable')).toContain('unavailable')
  })
})
