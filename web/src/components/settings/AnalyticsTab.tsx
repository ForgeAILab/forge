import { useState } from 'react'
import { productTerm } from '@/lib/i18n'
import { useProjectAnalytics } from '@/api/hooks'
import { ErrorBanner } from '@/components/error-banner'
import { SettingsSection } from '@/components/settings/SettingsSection'
import {
  formatCost,
  formatDuration,
  formatRate,
  formatTokens,
} from '@/components/settings/project-settings-utils'
import { Skeleton } from '@/components/ui/skeleton'
import { cn } from '@/lib/cn'

export function AnalyticsTab({ projectId }: { projectId: string }) {
  const [analyticsRange, setAnalyticsRange] = useState<'7d' | '30d' | 'all'>('all')

  const analyticsFrom =
    analyticsRange === '7d'
      ? new Date(Date.now() - 7 * 86400000).toISOString()
      : analyticsRange === '30d'
        ? new Date(Date.now() - 30 * 86400000).toISOString()
        : undefined
  const analyticsTo = analyticsRange !== 'all' ? new Date().toISOString() : undefined
  const analyticsQuery = useProjectAnalytics(projectId, analyticsFrom, analyticsTo)

  const sortedCiSteps = [...(analyticsQuery.data?.ci_steps ?? [])].sort(
    (a, b) => b.total_runs - a.total_runs,
  )
  const tokenUsage = analyticsQuery.data?.token_usage
  const reviewSummary = analyticsQuery.data?.review_summary

  const rangeButtonClass = (range: typeof analyticsRange) =>
    cn(
      'rounded-md px-3 py-1.5 text-sm transition-colors',
      analyticsRange === range
        ? 'bg-stone-700 text-white'
        : 'border border-stone-700 bg-stone-900 text-stone-400 hover:bg-stone-800 hover:text-stone-200',
    )

  return (
    <>
      <div className="mb-8">
        <h2 className="text-page font-semibold tracking-tight">Analytics</h2>
        <p className="mt-1 text-sm text-muted-foreground">
          Project-level review outcomes, CI performance, and token usage.
        </p>
      </div>

      <div className="mb-6 flex flex-wrap gap-2">
        <button type="button" onClick={() => setAnalyticsRange('7d')} className={rangeButtonClass('7d')}>
          Last 7 days
        </button>
        <button type="button" onClick={() => setAnalyticsRange('30d')} className={rangeButtonClass('30d')}>
          Last 30 days
        </button>
        <button type="button" onClick={() => setAnalyticsRange('all')} className={rangeButtonClass('all')}>
          All time
        </button>
      </div>

      {analyticsQuery.isLoading ? (
        <div className="space-y-4">
          <Skeleton className="h-40 w-full" />
          <Skeleton className="h-52 w-full" />
          <Skeleton className="h-60 w-full" />
        </div>
      ) : analyticsQuery.isError ? (
        <ErrorBanner
          error={analyticsQuery.error}
          fallback="Analytics failed to load"
          onRetry={() => void analyticsQuery.refetch()}
        />
      ) : (
        <>
          <SettingsSection title="Review Summary">
            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
              <div className="rounded-md border p-3">
                <p className="text-xs text-muted-foreground">Total Reviews</p>
                <p className="mt-1 text-xl font-semibold">{reviewSummary?.total_reviews ?? 0}</p>
              </div>
              <div className="rounded-md border p-3">
                <p className="text-xs text-muted-foreground">Passed</p>
                <p className="mt-1 text-xl font-semibold">{reviewSummary?.passed ?? 0}</p>
              </div>
              <div className="rounded-md border p-3">
                <p className="text-xs text-muted-foreground">Failed</p>
                <p className="mt-1 text-xl font-semibold">{reviewSummary?.failed ?? 0}</p>
              </div>
              <div className="rounded-md border p-3">
                <p className="text-xs text-muted-foreground">Cancelled</p>
                <p className="mt-1 text-xl font-semibold">{reviewSummary?.cancelled ?? 0}</p>
              </div>
              <div className="rounded-md border p-3">
                <p className="text-xs text-muted-foreground">Pass Rate</p>
                <p
                  className={cn(
                    'mt-1 text-xl font-semibold',
                    (reviewSummary?.pass_rate ?? 0) >= 0.8 ? 'text-emerald-400' : 'text-red-400',
                  )}
                >
                  {formatRate(reviewSummary?.pass_rate ?? 0)}
                </p>
              </div>
              <div className="rounded-md border p-3">
                <p className="text-xs text-muted-foreground">Avg Duration</p>
                <p className="mt-1 text-xl font-semibold">
                  {formatDuration(reviewSummary?.avg_duration_ms ?? null)}
                </p>
              </div>
            </div>
          </SettingsSection>

          <SettingsSection title="CI Steps">
            {sortedCiSteps.length === 0 ? (
              <p className="text-sm text-muted-foreground">No CI step data</p>
            ) : (
              <div className="overflow-x-auto rounded-md border">
                <table className="min-w-full text-sm">
                  <thead className="bg-muted/50">
                    <tr className="text-left">
                      <th className="px-3 py-2 font-medium">Command</th>
                      <th className="px-3 py-2 font-medium">Total Runs</th>
                      <th className="px-3 py-2 font-medium">Success Rate</th>
                      <th className="px-3 py-2 font-medium">Avg Duration</th>
                      <th className="px-3 py-2 font-medium">P95 Duration</th>
                      <th className="px-3 py-2 font-medium">Last Run</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sortedCiSteps.map((step) => (
                      <tr key={step.command} className="border-t">
                        <td className="px-3 py-2 font-mono text-xs">{step.command}</td>
                        <td className="px-3 py-2">{step.total_runs}</td>
                        <td className="px-3 py-2">
                          <div className="space-y-1">
                            <p>{formatRate(step.success_rate)}</p>
                            <div className="h-1.5 w-24 overflow-hidden rounded bg-stone-800">
                              <div
                                className="h-full rounded bg-emerald-500"
                                style={{
                                  width: `${Math.max(0, Math.min(100, step.success_rate * 100))}%`,
                                }}
                              />
                            </div>
                          </div>
                        </td>
                        <td className="px-3 py-2">{formatDuration(step.avg_duration_ms)}</td>
                        <td className="px-3 py-2">{formatDuration(step.p95_duration_ms)}</td>
                        <td className="px-3 py-2 text-xs text-muted-foreground">
                          {step.last_run_at ? new Date(step.last_run_at).toLocaleString() : '—'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </SettingsSection>

          <SettingsSection title="Token Usage">
            <div className="space-y-6">
              <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
                <div className="rounded-md border p-3">
                  <p className="text-xs text-muted-foreground">Total Input</p>
                  <p className="mt-1 text-xl font-semibold">
                    {formatTokens(tokenUsage?.total_input_tokens ?? 0)}
                  </p>
                </div>
                <div className="rounded-md border p-3">
                  <p className="text-xs text-muted-foreground">Total Output</p>
                  <p className="mt-1 text-xl font-semibold">
                    {formatTokens(tokenUsage?.total_output_tokens ?? 0)}
                  </p>
                </div>
                <div className="rounded-md border p-3">
                  <p className="text-xs text-muted-foreground">Cache Read</p>
                  <p className="mt-1 text-xl font-semibold">
                    {formatTokens(tokenUsage?.total_cache_read_tokens ?? 0)}
                  </p>
                </div>
                <div className="rounded-md border p-3">
                  <p className="text-xs text-muted-foreground">Cache Write</p>
                  <p className="mt-1 text-xl font-semibold">
                    {formatTokens(tokenUsage?.total_cache_write_tokens ?? 0)}
                  </p>
                </div>
                <div className="rounded-md border p-3">
                  <p className="text-xs text-muted-foreground">Cost</p>
                  <p className="mt-1 text-xl font-semibold">
                    {formatCost(tokenUsage?.total_cost_usd ?? null)}
                  </p>
                </div>
                <div className="rounded-md border p-3">
                  <p className="text-xs text-muted-foreground">{productTerm('run', 0)}</p>
                  <p className="mt-1 text-xl font-semibold">{tokenUsage?.execution_count ?? 0}</p>
                </div>
              </div>

              <div className="space-y-2">
                <p className="text-sm font-medium">By Agent</p>
                {(tokenUsage?.by_agent?.length ?? 0) === 0 ? (
                  <p className="text-sm text-muted-foreground">No agent usage data</p>
                ) : (
                  <div className="overflow-x-auto rounded-md border">
                    <table className="min-w-full text-sm">
                      <thead className="bg-muted/50">
                        <tr className="text-left">
                          <th className="px-3 py-2 font-medium">Agent</th>
                          <th className="px-3 py-2 font-medium">Executor</th>
                          <th className="px-3 py-2 font-medium">Model</th>
                          <th className="px-3 py-2 font-medium">{productTerm('run', 0)}</th>
                          <th className="px-3 py-2 font-medium">Success Rate</th>
                          <th className="px-3 py-2 font-medium">Input</th>
                          <th className="px-3 py-2 font-medium">Output</th>
                          <th className="px-3 py-2 font-medium">Cache Read</th>
                          <th className="px-3 py-2 font-medium">Cache Write</th>
                          <th className="px-3 py-2 font-medium">Cost</th>
                          <th className="px-3 py-2 font-medium">Avg Duration</th>
                        </tr>
                      </thead>
                      <tbody>
                        {tokenUsage?.by_agent.map((agent) => (
                          <tr key={agent.agent_id} className="border-t">
                            <td className="px-3 py-2 font-medium">{agent.agent_name}</td>
                            <td className="px-3 py-2 text-xs font-mono text-muted-foreground">{agent.executor_type}</td>
                            <td className="px-3 py-2 text-xs">{agent.model ?? '—'}</td>
                            <td className="px-3 py-2">{agent.execution_count}</td>
                            <td className="px-3 py-2">
                              {agent.success_rate != null ? formatRate(agent.success_rate) : '—'}
                            </td>
                            <td className="px-3 py-2">{formatTokens(agent.input_tokens)}</td>
                            <td className="px-3 py-2">{formatTokens(agent.output_tokens)}</td>
                            <td className="px-3 py-2">{formatTokens(agent.cache_read_tokens)}</td>
                            <td className="px-3 py-2">{formatTokens(agent.cache_write_tokens)}</td>
                            <td className="px-3 py-2">{formatCost(agent.cost_usd)}</td>
                            <td className="px-3 py-2">{formatDuration(agent.avg_duration_ms)}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>

              <div className="space-y-2">
                <p className="text-sm font-medium">By Model</p>
                {(tokenUsage?.by_model.length ?? 0) === 0 ? (
                  <p className="text-sm text-muted-foreground">No token data</p>
                ) : (
                  <div className="overflow-x-auto rounded-md border">
                    <table className="min-w-full text-sm">
                      <thead className="bg-muted/50">
                        <tr className="text-left">
                          <th className="px-3 py-2 font-medium">Provider</th>
                          <th className="px-3 py-2 font-medium">Model</th>
                          <th className="px-3 py-2 font-medium">Input</th>
                          <th className="px-3 py-2 font-medium">Output</th>
                          <th className="px-3 py-2 font-medium">Cache Read</th>
                          <th className="px-3 py-2 font-medium">Cache Write</th>
                          <th className="px-3 py-2 font-medium">Cost</th>
                          <th className="px-3 py-2 font-medium">{productTerm('run', 0)}</th>
                        </tr>
                      </thead>
                      <tbody>
                        {tokenUsage?.by_model.map((model) => (
                          <tr key={`${model.provider}:${model.model}`} className="border-t">
                            <td className="px-3 py-2">{model.provider}</td>
                            <td className="px-3 py-2">{model.model}</td>
                            <td className="px-3 py-2">{formatTokens(model.input_tokens)}</td>
                            <td className="px-3 py-2">{formatTokens(model.output_tokens)}</td>
                            <td className="px-3 py-2">{formatTokens(model.cache_read_tokens)}</td>
                            <td className="px-3 py-2">{formatTokens(model.cache_write_tokens)}</td>
                            <td className="px-3 py-2">{formatCost(model.cost_usd)}</td>
                            <td className="px-3 py-2">{model.execution_count}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            </div>
          </SettingsSection>
        </>
      )}
    </>
  )
}
