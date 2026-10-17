import { describe, expect, it } from 'vitest'
import { budgetLabel, visibleRemainingBudgets } from './retry-budget-display'

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

describe('budgetLabel', () => {
  it('names the lead budgets and keeps the other keys as words', () => {
    expect(budgetLabel('replan')).toBe('re-plans by the lead')
    expect(budgetLabel('lead_check')).toBe('lead check returns')
    expect(budgetLabel('merge_fix')).toBe('merge fix')
    expect(budgetLabel('review')).toBe('review')
  })

  it('keeps both lead budgets among the visible rows', () => {
    expect(
      visibleRemainingBudgets({ replan: 4, lead_check: 2, review_gate: 1 }, {}).map(([key]) => key),
    ).toEqual(['replan', 'lead_check'])
  })
})
