import { useMemo, useState } from 'react'
import { useProjectAnalytics } from '@/api/hooks'
import { ErrorBanner } from '@/components/error-banner'
import {
  AnalyticsRangeControls,
  analyticsRangeWindow,
  formatAnalyticsWindow,
  type AnalyticsRange,
} from '@/components/analytics/AnalyticsWindowControls'
import { UsageAnalyticsPanel } from '@/components/analytics/UsageAnalyticsPanel'
import {
  formatAnalyticsTimestamp,
  formatCount,
  formatOutcomeEligibility,
  formatOutcomeIneligibilityReason,
  PROJECT_USAGE_SURFACES,
} from '@/components/analytics/analytics-format'
import { formatMoneyAmount } from '@/lib/money-format'
import { SettingsSection } from '@/components/settings/SettingsSection'
import { formatDuration, formatRate } from '@/components/settings/project-settings-utils'
import { Skeleton } from '@/components/ui/skeleton'
import { cn } from '@/lib/cn'
import type { OutcomeCostMetric } from '@/types/generated'

function AnalyticsLoadingState() {
  return (
    <div
      className="space-y-4"
      role="status"
      aria-busy="true"
      aria-label="Loading project analytics"
    >
      <Skeleton className="h-40 w-full" />
      <Skeleton className="h-52 w-full" />
      <Skeleton className="h-60 w-full" />
    </div>
  )
}

function ReviewSummary({
  summary,
}: {
  summary: {
    total_reviews: number
    passed: number
    failed: number
    cancelled: number
    avg_duration_ms: number | null
    pass_rate: number
  }
}) {
  const values = [
    ['Total reviews', formatCount(summary.total_reviews)],
    ['Passed', formatCount(summary.passed)],
    ['Failed', formatCount(summary.failed)],
    ['Cancelled', formatCount(summary.cancelled)],
    ['Pass rate', formatRate(summary.pass_rate)],
    ['Average duration', formatDuration(summary.avg_duration_ms)],
  ] as const

  return (
    <SettingsSection
      title="Review summary"
      description="Review outcomes remain separate from usage and cost."
    >
      <dl className="grid grid-cols-2 gap-2 sm:grid-cols-3">
        {values.map(([label, value]) => (
          <div
            key={label}
            className="min-w-0 rounded-md border border-border-subtle bg-card px-3 py-2.5 shadow-xs"
          >
            <dt className="text-xs text-muted-foreground">{label}</dt>
            <dd
              className={cn(
                'mt-1 font-mono text-ui font-semibold tabular-nums text-foreground',
                label === 'Pass rate' &&
                  (summary.pass_rate >= 0.8 ? 'text-success' : 'text-destructive'),
              )}
            >
              {value}
            </dd>
          </div>
        ))}
      </dl>
    </SettingsSection>
  )
}

function CiSteps({
  steps,
}: {
  steps: Array<{
    command: string
    total_runs: number
    pass_count: number
    fail_count: number
    success_rate: number
    avg_duration_ms: number | null
    p50_duration_ms: number | null
    p95_duration_ms: number | null
    last_run_at: string | null
  }>
}) {
  const sorted = [...steps].sort((left, right) => right.total_runs - left.total_runs)

  return (
    <SettingsSection
      title="CI steps"
      description="Command outcomes for the selected Project window."
    >
      {sorted.length === 0 ? (
        <p
          className="rounded-md border border-dashed border-border-subtle bg-muted/20 px-3 py-4 text-sm text-muted-foreground"
          role="status"
        >
          No CI step data was returned for this window.
        </p>
      ) : (
        <div
          className="min-w-0 overflow-x-auto rounded-md border border-border-subtle focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2"
          tabIndex={0}
          role="region"
          aria-label="CI step outcomes"
        >
          <table className="min-w-[720px] text-xs">
            <caption className="sr-only">CI step outcomes</caption>
            <thead className="bg-muted/50 text-left">
              <tr>
                <th scope="col" className="px-3 py-2 font-medium">
                  Command
                </th>
                <th scope="col" className="px-3 py-2 font-medium">
                  Total runs
                </th>
                <th scope="col" className="px-3 py-2 font-medium">
                  Success rate
                </th>
                <th scope="col" className="px-3 py-2 font-medium">
                  Average
                </th>
                <th scope="col" className="px-3 py-2 font-medium">
                  P50
                </th>
                <th scope="col" className="px-3 py-2 font-medium">
                  P95
                </th>
                <th scope="col" className="px-3 py-2 font-medium">
                  Last run
                </th>
              </tr>
            </thead>
            <tbody>
              {sorted.map((step) => (
                <tr key={step.command} className="border-t border-border-subtle align-top">
                  <th
                    scope="row"
                    className="max-w-72 whitespace-normal break-words px-3 py-2 text-left font-mono text-xs font-normal"
                  >
                    {step.command}
                  </th>
                  <td className="whitespace-nowrap px-3 py-2 font-mono tabular-nums">
                    {formatCount(step.total_runs)}
                  </td>
                  <td className="whitespace-nowrap px-3 py-2">
                    <span className="font-mono tabular-nums">{formatRate(step.success_rate)}</span>
                    <span className="sr-only">
                      {formatCount(step.pass_count)} passed, {formatCount(step.fail_count)} failed
                    </span>
                  </td>
                  <td className="whitespace-nowrap px-3 py-2">
                    {formatDuration(step.avg_duration_ms)}
                  </td>
                  <td className="whitespace-nowrap px-3 py-2">
                    {formatDuration(step.p50_duration_ms)}
                  </td>
                  <td className="whitespace-nowrap px-3 py-2">
                    {formatDuration(step.p95_duration_ms)}
                  </td>
                  <td className="whitespace-nowrap px-3 py-2 text-muted-foreground">
                    {formatAnalyticsTimestamp(step.last_run_at)}
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </SettingsSection>
  )
}

function OutcomeEconomics({ metric }: { metric: OutcomeCostMetric }) {
  if (metric.outcome_kind !== 'released_milestone') return null

  const eligible = metric.eligibility === 'eligible'
  return (
    <section
      className="border-b border-border-subtle py-6"
      aria-labelledby="outcome-economics-heading"
    >
      <div className="grid items-start gap-4 md:grid-cols-[220px_minmax(0,1fr)] md:gap-8">
        <div>
          <h3
            id="outcome-economics-heading"
            className="text-ui font-semibold leading-snug text-foreground"
          >
            Released-milestone economics
          </h3>
          <p className="mt-1 text-xs leading-relaxed text-muted-foreground">
            Cost per outcome uses successful immutable release snapshots in this exact Project
            window.
          </p>
        </div>
        <div className="min-w-0 rounded-lg border border-border-subtle bg-card p-4 shadow-soft">
          <div className="flex min-w-0 flex-wrap items-start justify-between gap-2">
            <div>
              <p className="font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
                Outcome kind
              </p>
              <h4 className="mt-1 text-sm font-semibold text-foreground">Released milestone</h4>
            </div>
            <span
              className={cn(
                'rounded-full border px-2 py-1 text-xs font-medium',
                eligible
                  ? 'border-ember-border bg-ember-surface text-foreground'
                  : 'border-warning/40 bg-warning/10 text-foreground',
              )}
            >
              {formatOutcomeEligibility(metric.eligibility)}
            </span>
          </div>

          <dl className="mt-4 grid gap-2 text-xs sm:grid-cols-2">
            <div className="min-w-0 rounded-md border border-border-subtle bg-muted/20 px-3 py-2">
              <dt className="text-muted-foreground">Released milestone snapshots</dt>
              <dd className="mt-0.5 font-mono text-ui font-medium tabular-nums">
                {formatCount(metric.denominator)}
              </dd>
            </div>
            <div className="min-w-0 rounded-md border border-border-subtle bg-muted/20 px-3 py-2">
              <dt className="text-muted-foreground">Cost numerator</dt>
              <dd className="mt-0.5 font-mono text-ui font-medium">
                {metric.numerator ? formatMoneyAmount(metric.numerator) : 'Not available'}
              </dd>
            </div>
            {metric.amount_per_outcome ? (
              <div className="min-w-0 rounded-md border border-ember-border bg-ember-surface px-3 py-2">
                <dt className="text-muted-foreground">Cost per released milestone</dt>
                <dd className="mt-0.5 font-mono text-ui font-semibold">
                  {formatMoneyAmount(metric.amount_per_outcome)}
                </dd>
              </div>
            ) : null}
            <div className="min-w-0 rounded-md border border-border-subtle bg-muted/20 px-3 py-2">
              <dt className="text-muted-foreground">Scope</dt>
              <dd className="mt-0.5 break-words font-mono text-xs">
                Project <span className="break-all">{metric.scope.project_id}</span>
                <br />
                {metric.scope.from
                  ? formatAnalyticsTimestamp(metric.scope.from)
                  : 'Beginning'} →{' '}
                {metric.scope.to ? formatAnalyticsTimestamp(metric.scope.to) : 'Now'}
              </dd>
            </div>
          </dl>

          <p className="mt-3 text-xs leading-relaxed text-muted-foreground" role="status">
            {metric.ineligibility_reason
              ? formatOutcomeIneligibilityReason(metric.ineligibility_reason)
              : eligible
                ? 'Eligible: complete cost coverage and released-milestone snapshots share this exact window.'
                : 'No ineligibility reason was supplied.'}
          </p>
        </div>
      </div>
    </section>
  )
}

export function AnalyticsTab({ projectId }: { projectId: string }) {
  const [analyticsRange, setAnalyticsRange] = useState<AnalyticsRange>('all')
  const analyticsWindow = useMemo(() => analyticsRangeWindow(analyticsRange), [analyticsRange])
  const analyticsQuery = useProjectAnalytics(projectId, analyticsWindow.from, analyticsWindow.to)

  return (
    <>
      <div className="mb-6">
        <h2 className="text-page font-semibold tracking-tight">Analytics</h2>
        <p className="mt-1 text-sm text-muted-foreground">
          Project review outcomes, CI performance, and typed usage cost across Task, Project Chat,
          and Genesis Chat surfaces.
        </p>
      </div>

      <div className="mb-4 flex flex-wrap items-center justify-between gap-3">
        <AnalyticsRangeControls value={analyticsRange} onChange={setAnalyticsRange} />
        {analyticsQuery.data ? (
          <p className="text-xs text-muted-foreground" role="status">
            Window: {formatAnalyticsWindow(analyticsQuery.data.window)}
          </p>
        ) : null}
      </div>

      {analyticsQuery.isLoading ? <AnalyticsLoadingState /> : null}

      {analyticsQuery.isError ? (
        <div role="alert">
          <ErrorBanner
            error={analyticsQuery.error}
            fallback="Analytics failed to load"
            onRetry={() => void analyticsQuery.refetch()}
          />
        </div>
      ) : null}

      {analyticsQuery.data ? (
        <>
          <ReviewSummary summary={analyticsQuery.data.review_summary} />
          <CiSteps steps={analyticsQuery.data.ci_steps} />
          <UsageAnalyticsPanel
            analytics={analyticsQuery.data.token_usage}
            surfaces={PROJECT_USAGE_SURFACES}
            heading="Usage and cost"
            description="Task execution, Project Chat, and Genesis Chat remain distinct; monetary totals preserve provider and estimator provenance."
          />
          <OutcomeEconomics metric={analyticsQuery.data.outcome_economics} />
        </>
      ) : null}
    </>
  )
}
