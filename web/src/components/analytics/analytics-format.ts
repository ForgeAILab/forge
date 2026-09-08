import type {
  AgentUsageBreakdown,
  CostCoverageReason,
  CostCoverageReasonCode,
  CostKind,
  CostSourceFreshness,
  CostSourceKind,
  CostSourceRef,
  ModelUsageBreakdown,
  ProjectUsageBreakdown,
  SurfaceUsageBreakdown,
  TokenCounters,
  UsageSurface,
} from '@/types/generated'

export const USAGE_SURFACE_ORDER: readonly UsageSurface[] = [
  'task_execution',
  'project_chat',
  'genesis_chat',
  'main_chat',
  'main_inquiry',
]

export const PROJECT_USAGE_SURFACES: readonly UsageSurface[] = [
  'task_execution',
  'project_chat',
  'genesis_chat',
]

export const ACCOUNT_USAGE_SURFACES: readonly UsageSurface[] = USAGE_SURFACE_ORDER

export const USAGE_SURFACE_LABELS: Record<UsageSurface, string> = {
  task_execution: 'Task execution',
  project_chat: 'Project Chat',
  genesis_chat: 'Genesis Chat',
  main_chat: 'Main Chat',
  main_inquiry: 'Main inquiry',
}

const COST_KIND_LABELS: Record<CostKind, string> = {
  provider_reported: 'Provider-reported',
  estimated: 'Estimated',
  mixed: 'Mixed provenance',
  unknown: 'Unknown',
  none: 'No usage',
}

const COST_COVERAGE_LABELS = {
  complete: 'Complete',
  partial: 'Partial',
  unavailable: 'Unavailable',
  pending: 'Pending settlement',
  no_usage: 'No usage',
} as const

const COST_SOURCE_LABELS: Record<CostSourceKind, string> = {
  provider_reported: 'Provider-reported amount',
  legacy_provider_reported: 'Legacy provider-reported amount',
  models_dev_catalog: 'models.dev catalog',
  manual_override: 'Manual override',
}

const COST_FRESHNESS_LABELS: Record<CostSourceFreshness, string> = {
  fresh: 'Fresh at admission',
  stale: 'Stale at admission',
  refresh_failed: 'Refresh failed at admission',
  not_applicable: 'Freshness not applicable',
}

const REASON_ORDER: readonly CostCoverageReasonCode[] = [
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
]

const REASON_LABELS: Record<CostCoverageReasonCode, string> = {
  pending: 'Pending settlement',
  unsettled: 'Terminally unsettled provider attempt',
  unmetered: 'No reliable token telemetry',
  missing_provider: 'Missing provider identity',
  missing_model: 'Missing model identity',
  missing_binding: 'Missing exact pricing binding',
  missing_rate: 'Missing required token rate',
  unresolved_tier: 'Unresolved context pricing tier',
  identity_mismatch: 'Provider/model identity mismatch',
  invalid_legacy_usage: 'Invalid legacy usage evidence',
}

export function formatCount(value: number): string {
  return new Intl.NumberFormat('en-US').format(value)
}

export function formatTokens(value: number): string {
  return formatCount(value)
}

export function formatTokenCounters(counters: TokenCounters): string {
  return [
    `Input ${formatTokens(counters.input_tokens)}`,
    `Output ${formatTokens(counters.output_tokens)}`,
    `Cache read ${formatTokens(counters.cache_read_tokens)}`,
    `Cache write ${formatTokens(counters.cache_write_tokens)}`,
  ].join(' · ')
}

export function formatCostKind(kind: CostKind): string {
  return COST_KIND_LABELS[kind]
}

export function formatCostCoverage(coverage: keyof typeof COST_COVERAGE_LABELS): string {
  return COST_COVERAGE_LABELS[coverage]
}

export function formatUsageSurface(surface: UsageSurface): string {
  return USAGE_SURFACE_LABELS[surface]
}

export function sortBySurface<T extends { surface: UsageSurface }>(rows: readonly T[]): T[] {
  const order = new Map(USAGE_SURFACE_ORDER.map((surface, index) => [surface, index]))
  return [...rows].sort(
    (left, right) =>
      (order.get(left.surface) ?? USAGE_SURFACE_ORDER.length) -
        (order.get(right.surface) ?? USAGE_SURFACE_ORDER.length) ||
      USAGE_SURFACE_LABELS[left.surface].localeCompare(USAGE_SURFACE_LABELS[right.surface]),
  )
}

export function sortCoverageReasons(reasons: readonly CostCoverageReason[]): CostCoverageReason[] {
  const order = new Map(REASON_ORDER.map((reason, index) => [reason, index]))
  return [...reasons].sort(
    (left, right) =>
      (order.get(left.code) ?? REASON_ORDER.length) -
        (order.get(right.code) ?? REASON_ORDER.length) || left.code.localeCompare(right.code),
  )
}

export function sortCostSources(sources: readonly CostSourceRef[]): CostSourceRef[] {
  return [...sources].sort((left, right) => {
    const leftLabel = `${left.source_kind}:${left.rate_revision_id ?? ''}:${left.catalog_snapshot_id ?? ''}`
    const rightLabel = `${right.source_kind}:${right.rate_revision_id ?? ''}:${right.catalog_snapshot_id ?? ''}`
    return leftLabel.localeCompare(rightLabel)
  })
}

export function formatCoverageReason(code: CostCoverageReasonCode): string {
  return REASON_LABELS[code]
}

export function formatCostSourceKind(kind: CostSourceKind): string {
  return COST_SOURCE_LABELS[kind]
}

export function formatCostFreshness(freshness: CostSourceFreshness, retrospective = false): string {
  if (retrospective && freshness !== 'not_applicable') {
    return COST_FRESHNESS_LABELS[freshness].replace('at admission', 'at retrospective selection')
  }
  return COST_FRESHNESS_LABELS[freshness]
}

export function formatCostSummaryMessage(
  coverage: keyof typeof COST_COVERAGE_LABELS,
  reasons: readonly CostCoverageReasonCode[] = [],
): string {
  switch (coverage) {
    case 'complete':
      return 'Every provider attempt in this window has a complete cost.'
    case 'partial':
      return 'Partial coverage: some provider attempts are costed, but a complete total is unavailable.'
    case 'unavailable':
      if (reasons.includes('unsettled')) {
        return 'Cost unknown: one or more provider attempts ended terminally unsettled before a usable cost was recorded.'
      }
      return 'Cost unknown: settled provider attempts have no usable reported amount or exact rate.'
    case 'pending':
      return 'Cost pending: one or more provider attempts still need settlement.'
    case 'no_usage':
      return 'No provider usage was recorded in this window.'
  }
}

export function formatOutcomeEligibility(
  eligibility: 'eligible' | 'no_outcomes' | 'incomplete_cost' | 'pending_cost',
): string {
  switch (eligibility) {
    case 'eligible':
      return 'Eligible'
    case 'no_outcomes':
      return 'No released milestones'
    case 'incomplete_cost':
      return 'Incomplete cost'
    case 'pending_cost':
      return 'Cost pending'
  }
}

export function formatOutcomeIneligibilityReason(
  reason:
    | 'no_released_milestones'
    | 'no_usage_cost'
    | 'cost_pending'
    | 'cost_partial'
    | 'cost_unavailable',
): string {
  switch (reason) {
    case 'no_released_milestones':
      return 'No successful released-milestone snapshots fall in this window.'
    case 'no_usage_cost':
      return 'No usage cost is available for this window.'
    case 'cost_pending':
      return 'Usage cost is still pending provider settlement.'
    case 'cost_partial':
      return 'Usage cost is only partially covered.'
    case 'cost_unavailable':
      return 'Usage cost is unavailable because invoked attempts are unpriced or unsettled.'
  }
}

export function formatNullableIdentity(value: string | null, fallback: string): string {
  return value && value.length > 0 ? value : fallback
}

export function formatAnalyticsTimestamp(value: string | null): string {
  if (!value) return 'Not recorded'
  const date = new Date(value)
  return date.toString() === 'Invalid Date' ? value : date.toLocaleString()
}

export type UsageGrouping =
  | SurfaceUsageBreakdown
  | ModelUsageBreakdown
  | AgentUsageBreakdown
  | ProjectUsageBreakdown
