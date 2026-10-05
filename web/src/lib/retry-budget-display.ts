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
