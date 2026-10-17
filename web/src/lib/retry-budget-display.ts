/**
 * The remaining-budget entries worth showing for one Task: only kinds with an
 * active limit. The API keeps every key; this is display only.
 *
 * - `report_correction` is per reviewer invocation, never a Task budget.
 * - `review_gate` is the cancelled-review entry cap, already shown as `review`.
 * - `automatic_review_recovery` has no limit while recovery is disabled.
 */
export function visibleRemainingBudgets(
  remaining: Record<string, number>,
  limits: Record<string, number> | undefined,
): Array<[string, number]> {
  return Object.entries(remaining).filter(([key]) => {
    if (key === 'report_correction' || key === 'review_gate') return false
    if (key === 'automatic_review_recovery') return (limits?.[key] ?? 0) > 0
    return true
  })
}

/**
 * Names for the budget kinds whose key does not read as words. The keys are
 * the server's (`TaskResponse.remaining_retries`): a gate is keyed by its
 * state name, so the step check's returns are `lead_check`; `replan` exists
 * on a coordination root that has re-planned.
 */
const BUDGET_LABELS: Record<string, string> = {
  replan: 're-plans by the lead',
  lead_check: 'lead check returns',
}

export function budgetLabel(key: string): string {
  return BUDGET_LABELS[key] ?? key.replace(/_/g, ' ')
}
