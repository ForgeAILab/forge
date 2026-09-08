import { formatMoneyAmount } from '@/lib/money-format'
import { cn } from '@/lib/cn'
import type { ActivityCounts, CostSummary, TokenCounters } from '@/types/generated'
import {
  formatAnalyticsTimestamp,
  formatCostCoverage,
  formatCostFreshness,
  formatCostKind,
  formatCostSourceKind,
  formatCostSummaryMessage,
  formatCount,
  formatCoverageReason,
  formatTokenCounters,
  formatTokens,
  sortCostSources,
  sortCoverageReasons,
} from './analytics-format'

export function TokenCountersView({
  counters,
  className,
}: {
  counters: TokenCounters
  className?: string
}) {
  const values = [
    ['Input', counters.input_tokens],
    ['Output', counters.output_tokens],
    ['Cache read', counters.cache_read_tokens],
    ['Cache write', counters.cache_write_tokens],
  ] as const

  return (
    <dl className={cn('grid grid-cols-2 gap-2 text-xs sm:grid-cols-4', className)}>
      {values.map(([label, value]) => (
        <div
          key={label}
          className="min-w-0 rounded-md border border-border-subtle bg-muted/30 px-2.5 py-2"
        >
          <dt className="text-muted-foreground">{label}</dt>
          <dd className="mt-0.5 font-mono text-ui font-medium text-foreground">
            {formatTokens(value)}
          </dd>
        </div>
      ))}
    </dl>
  )
}

export function ActivityCountsView({
  counts,
  className,
}: {
  counts: ActivityCounts
  className?: string
}) {
  const values = [
    ['Task executions', counts.task_execution_count],
    ['Chat turns', counts.chat_turn_count],
    ['Inquiries', counts.inquiry_count],
    ['Provider attempts', counts.provider_attempt_count],
  ] as const

  return (
    <dl className={cn('grid grid-cols-2 gap-2 sm:grid-cols-4', className)}>
      {values.map(([label, value]) => (
        <div
          key={label}
          className="min-w-0 rounded-md border border-border-subtle bg-card px-3 py-2.5 shadow-xs"
        >
          <dt className="text-xs text-muted-foreground">{label}</dt>
          <dd className="mt-1 font-mono text-lg font-semibold tabular-nums text-foreground">
            {formatCount(value)}
          </dd>
        </div>
      ))}
    </dl>
  )
}

function CoverageDetails({ summary, compact }: { summary: CostSummary; compact: boolean }) {
  const coverage = summary.usage_coverage
  const logicalMetrics = [
    ['Total runs / turns', coverage.total_runs_or_turns],
    ['Pending runs / turns', coverage.pending_runs_or_turns],
    ['No provider call', coverage.no_provider_call_runs_or_turns],
    ['Fully metered runs / turns', coverage.fully_metered_runs_or_turns],
    ['Fully costed runs / turns', coverage.fully_costed_runs_or_turns],
    ['Partially costed runs / turns', coverage.partially_costed_runs_or_turns],
    ['Unavailable-cost runs / turns', coverage.unavailable_cost_runs_or_turns],
  ] as const
  const attemptMetrics = [
    ['Total provider attempts', coverage.total_provider_attempts],
    ['Settled provider attempts', coverage.settled_provider_attempts],
    ['Pending provider attempts', coverage.pending_provider_attempts],
    ['Unsettled provider attempts', coverage.unsettled_provider_attempts],
    ['Metered provider attempts', coverage.metered_provider_attempts],
    ['Unmetered provider attempts', coverage.unmetered_provider_attempts],
    ['Costed provider attempts', coverage.costed_provider_attempts],
    ['Unpriced provider attempts', coverage.unpriced_provider_attempts],
  ] as const

  const content = (
    <div className="space-y-3">
      <div className="grid gap-4 md:grid-cols-2">
        <div>
          <h5 className="font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
            Run / turn coverage
          </h5>
          <dl className="mt-2 divide-y divide-border-subtle rounded-md border border-border-subtle">
            {logicalMetrics.map(([label, value]) => (
              <div
                key={label}
                className="flex min-w-0 items-baseline justify-between gap-3 px-3 py-2 text-xs"
              >
                <dt className="min-w-0 text-muted-foreground">{label}</dt>
                <dd className="shrink-0 font-mono font-medium tabular-nums text-foreground">
                  {formatCount(value)}
                </dd>
              </div>
            ))}
          </dl>
        </div>
        <div>
          <h5 className="font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
            Provider-attempt coverage
          </h5>
          <dl className="mt-2 divide-y divide-border-subtle rounded-md border border-border-subtle">
            {attemptMetrics.map(([label, value]) => (
              <div
                key={label}
                className="flex min-w-0 items-baseline justify-between gap-3 px-3 py-2 text-xs"
              >
                <dt className="min-w-0 text-muted-foreground">{label}</dt>
                <dd className="shrink-0 font-mono font-medium tabular-nums text-foreground">
                  {formatCount(value)}
                </dd>
              </div>
            ))}
          </dl>
        </div>
      </div>

      <div className="grid gap-3 sm:grid-cols-2">
        <div className="min-w-0 rounded-md border border-border-subtle bg-muted/20 px-3 py-2.5">
          <p className="text-xs text-muted-foreground">Priced tokens</p>
          <p className="mt-1 break-words font-mono text-xs text-foreground">
            {formatTokenCounters(coverage.priced_tokens)}
          </p>
        </div>
        <div className="min-w-0 rounded-md border border-border-subtle bg-muted/20 px-3 py-2.5">
          <p className="text-xs text-muted-foreground">Unpriced tokens</p>
          <p className="mt-1 break-words font-mono text-xs text-foreground">
            {formatTokenCounters(coverage.unpriced_tokens)}
          </p>
        </div>
      </div>

      {coverage.reasons.length > 0 ? (
        <div>
          <h5 className="font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
            Coverage reasons
          </h5>
          <ul className="mt-2 space-y-2" aria-label="Cost coverage reasons">
            {sortCoverageReasons(coverage.reasons).map((reason) => (
              <li
                key={reason.code}
                className="min-w-0 rounded-md border border-warning/30 bg-warning/5 px-3 py-2 text-xs"
              >
                <p className="font-medium text-foreground">{formatCoverageReason(reason.code)}</p>
                <p className="mt-0.5 break-words text-muted-foreground">
                  {formatCount(reason.run_or_turn_count)} run/turns ·{' '}
                  {formatCount(reason.provider_attempt_count)} provider attempts ·{' '}
                  {formatTokenCounters(reason.tokens)}
                </p>
              </li>
            ))}
          </ul>
        </div>
      ) : null}
    </div>
  )

  if (!compact) {
    return (
      <div>
        <h4 className="font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
          Coverage denominators
        </h4>
        <div className="mt-3">{content}</div>
      </div>
    )
  }

  return (
    <details className="mt-2 min-w-0 rounded-md border border-border-subtle bg-muted/10 px-2.5 py-2">
      <summary className="cursor-pointer text-xs font-medium text-muted-foreground outline-none transition-colors hover:text-foreground focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2">
        Coverage denominators
      </summary>
      <div className="mt-3">{content}</div>
    </details>
  )
}

export function CostSummaryView({
  summary,
  compact = false,
  className,
}: {
  summary: CostSummary
  compact?: boolean
  className?: string
}) {
  const sources = sortCostSources(summary.sources)
  const hasCompleteTotal = summary.coverage === 'complete' && summary.complete_total !== null

  return (
    <div
      data-testid="cost-summary"
      className={cn(
        'min-w-0 rounded-lg border border-border-subtle bg-card p-4 shadow-soft',
        compact && 'rounded-md bg-muted/10 p-2.5 shadow-none',
        className,
      )}
    >
      <div className="flex min-w-0 flex-wrap items-start justify-between gap-2">
        <div className="min-w-0">
          <p className="font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
            Cost
          </p>
          <p className="mt-1 text-sm font-semibold text-foreground">
            {formatCostKind(summary.kind)}
          </p>
        </div>
        <span className="shrink-0 rounded-full border border-border bg-muted px-2 py-1 text-xs font-medium text-foreground">
          {formatCostCoverage(summary.coverage)}
        </span>
      </div>

      <dl className="mt-3 grid min-w-0 gap-2 text-xs sm:grid-cols-2">
        {summary.provider_reported ? (
          <div className="min-w-0 rounded-md border border-border-subtle bg-muted/20 px-3 py-2">
            <dt className="text-muted-foreground">Provider-reported</dt>
            <dd className="mt-0.5 font-mono text-ui font-medium text-foreground">
              {formatMoneyAmount(summary.provider_reported)}
            </dd>
          </div>
        ) : null}
        {summary.estimated ? (
          <div className="min-w-0 rounded-md border border-border-subtle bg-muted/20 px-3 py-2">
            <dt className="text-muted-foreground">Estimated</dt>
            <dd className="mt-0.5 font-mono text-ui font-medium text-foreground">
              {formatMoneyAmount(summary.estimated)}
            </dd>
          </div>
        ) : null}
        {summary.known_subtotal ? (
          <div className="min-w-0 rounded-md border border-border-subtle bg-ember-surface px-3 py-2">
            <dt className="text-muted-foreground">
              Known subtotal{summary.coverage === 'partial' ? ' (partial)' : ''}
            </dt>
            <dd className="mt-0.5 font-mono text-ui font-medium text-foreground">
              {formatMoneyAmount(summary.known_subtotal)}
            </dd>
          </div>
        ) : null}
        {hasCompleteTotal ? (
          <div className="min-w-0 rounded-md border border-ember-border bg-ember-surface px-3 py-2">
            <dt className="text-muted-foreground">Complete total</dt>
            <dd className="mt-0.5 font-mono text-ui font-semibold text-foreground">
              {formatMoneyAmount(summary.complete_total!)}
            </dd>
          </div>
        ) : null}
      </dl>

      <p
        className={cn(
          'mt-3 text-xs leading-relaxed',
          summary.coverage === 'complete' ? 'text-success' : 'text-muted-foreground',
        )}
        role="status"
        aria-live="polite"
      >
        {formatCostSummaryMessage(
          summary.coverage,
          summary.usage_coverage.reasons.map((reason) => reason.code),
        )}
      </p>

      <div className="mt-3 min-w-0 border-t border-border-subtle pt-3">
        <p className="font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
          Price provenance
        </p>
        {sources.length > 0 ? (
          <ul className="mt-2 space-y-2 text-xs">
            {sources.map((source) => (
              <li
                key={`${source.source_kind}:${source.rate_revision_id ?? ''}:${source.catalog_snapshot_id ?? ''}`}
                className="min-w-0"
              >
                <p className="break-words text-foreground">
                  {formatCostSourceKind(source.source_kind)} ·{' '}
                  {formatCostFreshness(source.freshness, source.retrospective)}
                  {source.retrospective ? ' · Retrospective estimate' : ''}
                </p>
                <p className="mt-0.5 break-words font-mono text-xs text-muted-foreground">
                  {source.effective_at
                    ? `Effective ${formatAnalyticsTimestamp(source.effective_at)}`
                    : null}
                  {source.fetched_at
                    ? ` · Fetched ${formatAnalyticsTimestamp(source.fetched_at)}`
                    : null}
                  {source.formula_revision ? ` · Formula ${source.formula_revision}` : null}
                </p>
                {source.rate_revision_id || source.catalog_snapshot_id || source.catalog_digest ? (
                  <p className="mt-0.5 break-all font-mono text-xs text-muted-foreground">
                    {source.rate_revision_id ? `Rate revision ${source.rate_revision_id}` : null}
                    {source.catalog_snapshot_id
                      ? ` · Catalog snapshot ${source.catalog_snapshot_id}`
                      : null}
                    {source.catalog_digest ? ` · Digest ${source.catalog_digest}` : null}
                  </p>
                ) : null}
              </li>
            ))}
          </ul>
        ) : (
          <p className="mt-2 text-xs text-muted-foreground">No pricing source is recorded.</p>
        )}
      </div>

      <CoverageDetails summary={summary} compact={compact} />
    </div>
  )
}
