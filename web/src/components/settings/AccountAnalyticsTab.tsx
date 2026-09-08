import { useMemo, useState } from 'react'
import { useAccountUsageAnalytics } from '@/api/hooks'
import {
  AnalyticsRangeControls,
  analyticsRangeWindow,
  formatAnalyticsWindow,
  type AnalyticsRange,
} from '@/components/analytics/AnalyticsWindowControls'
import { ProjectUsageTable, UsageAnalyticsPanel } from '@/components/analytics/UsageAnalyticsPanel'
import { ACCOUNT_USAGE_SURFACES } from '@/components/analytics/analytics-format'
import { ErrorBanner } from '@/components/error-banner'
import { Skeleton } from '@/components/ui/skeleton'

export function AccountAnalyticsTab() {
  const [analyticsRange, setAnalyticsRange] = useState<AnalyticsRange>('all')
  const analyticsWindow = useMemo(() => analyticsRangeWindow(analyticsRange), [analyticsRange])
  const analyticsQuery = useAccountUsageAnalytics(analyticsWindow.from, analyticsWindow.to)

  return (
    <>
      <div className="mb-6">
        <h2 className="text-page font-semibold tracking-tight">Usage analytics</h2>
        <p className="mt-1 text-sm text-muted-foreground">
          Account-scoped usage and cost across Task, Project, Genesis, Main Chat, and Main inquiry
          surfaces.
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

      {analyticsQuery.isLoading ? (
        <div
          className="space-y-4"
          role="status"
          aria-busy="true"
          aria-label="Loading account analytics"
        >
          <Skeleton className="h-40 w-full" />
          <Skeleton className="h-60 w-full" />
        </div>
      ) : null}

      {analyticsQuery.isError ? (
        <div role="alert">
          <ErrorBanner
            error={analyticsQuery.error}
            fallback="Account analytics failed to load"
            onRetry={() => void analyticsQuery.refetch()}
          />
        </div>
      ) : null}

      {analyticsQuery.data ? (
        <>
          <UsageAnalyticsPanel
            analytics={analyticsQuery.data.token_usage}
            surfaces={ACCOUNT_USAGE_SURFACES}
            heading="Account usage and cost"
            description="Every account surface is shown once, including explicit no-usage rows. Main Chat and Main inquiries remain account-scoped, while Project grouping uses only immutable Project attribution."
          />
          <section
            className="border-b border-border-subtle py-6"
            aria-labelledby="account-project-usage-heading"
          >
            <h3
              id="account-project-usage-heading"
              className="text-lg font-semibold text-foreground"
            >
              Project grouping
            </h3>
            <p className="mt-1 text-sm text-muted-foreground">
              Compare scoped usage by Project without attaching account-only Main activity to a
              later handoff.
            </p>
            <div className="mt-4">
              <ProjectUsageTable rows={analyticsQuery.data.by_project} />
            </div>
          </section>
        </>
      ) : null}
    </>
  )
}
