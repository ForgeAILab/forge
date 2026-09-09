import { Info } from '@phosphor-icons/react'

import { formatRuntimeSeconds, formatTokenCount } from '@/components/task-execution-observability'
import { CostSummaryView } from '@/components/analytics/CostSummary'
import { formatMoneyAmount } from '@/lib/money-format'
import { Tooltip } from '@/components/ui/tooltip'
import {
  executionRuntimeSeconds,
  formatDate,
  formatRelativeDate,
  latestLog,
  usageTotals,
} from '@/components/execution-detail/execution-detail-format'
import type { Execution, LogEntry, UsageBreakdown } from '@/types/generated'

function Metric({ label, value, title }: { label: string; value: string; title?: string }) {
  return (
    <div className="rounded-md border bg-background px-3 py-2" title={title}>
      <p className="font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
        {label}
      </p>
      <p className="mt-1 truncate font-mono text-sm font-semibold tabular-nums text-foreground">
        {value}
      </p>
    </div>
  )
}

export function ExecutionObservabilitySection({
  execution,
  logs,
  usage,
}: {
  execution: Execution
  logs: LogEntry[]
  usage: UsageBreakdown[]
}) {
  const totals = usageTotals(usage)
  const totalTokens =
    totals.inputTokens +
    totals.outputTokens +
    totals.cacheReadTokens +
    totals.cacheWriteTokens
  const assistantTurns = logs.filter((log) => log.kind === 'assistant').length
  const recentLog = latestLog(logs)
  const costHeadline = executionCostHeadline(execution, usage)

  return (
    <section className="space-y-3">
      <div className="flex items-center gap-1.5 text-micro font-medium uppercase tracking-wider text-muted-foreground">
        <Info className="h-3 w-3" />
        <span>Observability</span>
      </div>
      <div className="grid grid-cols-2 gap-2">
        <Metric label="Runtime" value={formatRuntimeSeconds(executionRuntimeSeconds(execution))} />
        <Metric label="Turns" value={assistantTurns.toLocaleString()} />
        <Metric
          label="Tokens"
          title={`${formatTokenCount(totals.inputTokens)} input / ${formatTokenCount(totals.outputTokens)} output / ${formatTokenCount(totals.cacheReadTokens)} cache read / ${formatTokenCount(totals.cacheWriteTokens)} cache write`}
          value={formatTokenCount(totalTokens, true)}
        />
        <Metric label="Cost" value={costHeadline} />
      </div>
      {usage.length > 0 ? (
        <div className="space-y-2">
          <p className="font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
            Usage breakdown
          </p>
          <div className="space-y-2">
            {usage.map((item, index) => (
              <div
                key={`${item.invocation_id}:${item.usage_event_id ?? 'pending'}:${index}`}
                className="min-w-0 rounded-md border border-border-subtle bg-background px-3 py-2"
              >
                <div className="flex min-w-0 flex-wrap items-baseline justify-between gap-2">
                  <p className="min-w-0 break-all text-xs font-medium text-foreground">
                    {item.attribution.provider_id ?? 'Provider unavailable'}
                    {' / '}
                    {item.attribution.model_id ?? 'Model unavailable'}
                  </p>
                  <span className="shrink-0 text-micro uppercase text-muted-foreground">
                    Attempt {item.attribution.attempt_ordinal}
                  </span>
                </div>
                <p className="mt-1 break-all text-micro text-muted-foreground">
                  {item.usage_event_id ? `Event ${item.usage_event_id}` : 'Awaiting usage event'}
                  {' · '}
                  {item.telemetry_state.replace(/_/g, ' ')}
                  {item.attribution.candidate_key ? ` · ${item.attribution.candidate_key}` : ''}
                </p>
                {item.counters ? (
                  <p className="mt-1 text-xs text-muted-foreground">
                    {formatTokenCount(item.counters.input_tokens)} input ·{' '}
                    {formatTokenCount(item.counters.output_tokens)} output ·{' '}
                    {formatTokenCount(item.counters.cache_read_tokens)} cache read ·{' '}
                    {formatTokenCount(item.counters.cache_write_tokens)} cache write
                  </p>
                ) : (
                  <p className="mt-1 text-xs text-muted-foreground">Token telemetry unavailable</p>
                )}
                <CostSummaryView summary={item.cost} compact />
              </div>
            ))}
          </div>
        </div>
      ) : null}
      <div className="rounded-md border bg-background px-3 py-2">
        <div className="flex items-center justify-between gap-3">
          <p className="font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
            Last Event
          </p>
          {recentLog ? (
            <Tooltip content={formatDate(recentLog.timestamp)}>
              <span className="shrink-0 text-micro text-muted-foreground">
                {formatRelativeDate(recentLog.timestamp)}
              </span>
            </Tooltip>
          ) : null}
        </div>
        <p className="mt-1 truncate text-sm text-foreground">
          {recentLog ? recentLog.kind.replace(/_/g, ' ') : 'No loaded events'}
        </p>
        <p className="mt-0.5 text-xs text-muted-foreground">
          {logs.length.toLocaleString()} loaded log events
        </p>
      </div>
    </section>
  )
}

function executionCostHeadline(execution: Execution, usage: UsageBreakdown[]): string {
  if (usage.length === 0) return execution.status === 'running' ? 'Pending' : 'No usage'
  if (usage.some((item) => item.cost.coverage === 'pending')) return 'Pending'
  if (usage.length === 1) {
    const summary = usage[0].cost
    if (summary.coverage === 'complete' && summary.complete_total) {
      return formatMoneyAmount(summary.complete_total)
    }
    if (summary.coverage === 'no_usage') return 'No usage'
  }
  if (usage.every((item) => item.cost.coverage === 'complete')) return 'See breakdown'
  return 'Cost unknown'
}
