import { describe, expect, it } from 'vitest'
import { visibleRemainingBudgets } from './retry-budget-display'

describe('visibleRemainingBudgets', () => {
  const remaining = {
    review: 1,
    review_gate: 2,
    merge_fix: 1,
    execution: 3,
    report_correction: 2,
    automatic_review_recovery: 0,
  }

  it('hides per-invocation, folded and disabled kinds', () => {
    expect(
      visibleRemainingBudgets(remaining, { ...remaining, automatic_review_recovery: 0 }),
    ).toEqual([
      ['review', 1],
      ['merge_fix', 1],
      ['execution', 3],
    ])
  })

  it('shows automatic review recovery when it has a limit', () => {
    expect(
      visibleRemainingBudgets(remaining, { ...remaining, automatic_review_recovery: 1 }),
    ).toContainEqual(['automatic_review_recovery', 0])
  })
})
