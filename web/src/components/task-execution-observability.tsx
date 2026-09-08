import { Link } from '@tanstack/react-router'
import { cn } from '@/lib/cn'
import { productTerm } from '@/lib/i18n'
import { formatMoneyAmount } from '@/lib/money-format'
import { CostSummaryView } from '@/components/analytics/CostSummary'
import type { TaskExecutionObservability } from '@/types/generated'

export function formatRuntimeSeconds(value?: number | null): string {
  if (value == null || !Number.isFinite(value)) return '-'
  const totalSeconds = Math.max(0, Math.floor(value))
  if (totalSeconds < 60) return `${totalSeconds}s`
  const minutes = Math.floor(totalSeconds / 60)
  if (minutes < 60) {
    const seconds = totalSeconds % 60
    return seconds > 0 ? `${minutes}m ${seconds}s` : `${minutes}m`
  }
  const hours = Math.floor(minutes / 60)
  const remainingMinutes = minutes % 60
  if (hours < 24) return remainingMinutes > 0 ? `${hours}h ${remainingMinutes}m` : `${hours}h`
  const days = Math.floor(hours / 24)
  const remainingHours = hours % 24
  return remainingHours > 0 ? `${days}d ${remainingHours}h` : `${days}d`
}

export function formatTokenCount(value?: number | null, compact = false): string {
  if (value == null || !Number.isFinite(value)) return '-'
  if (compact) {
    return new Intl.NumberFormat(undefined, {
      notation: 'compact',
      maximumFractionDigits: value >= 10_000 ? 1 : 0,
    }).format(value)
  }
  return Math.max(0, Math.trunc(value)).toLocaleString()
}

export function formatDateTime(value?: string | null): string {
  if (!value) return '-'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return date.toLocaleString()
}

export function formatStatusLabel(value?: string | null): string {
  if (!value) return '-'
  return value.replace(/_/g, ' ')
}

export function TaskExecutionObservabilityPanel({
  value,
  taskId,
  formatDate = formatDateTime,
  className,
}: {
  value?: TaskExecutionObservability | null
  taskId?: string
  formatDate?: (value?: string | null) => string
  className?: string
}) {
  if (!value) {
    return (
      <p
        className={cn(
          'rounded-md border border-dashed px-3 py-2 text-xs text-muted-foreground',
          className,
        )}
      >
        No {productTerm('run').toLowerCase()} metrics yet.
      </p>
    )
  }

  const active = value.active_execution_id
    ? {
        id: value.active_execution_id,
        role: value.active_role ?? productTerm('run').toLowerCase(),
        startedAt: value.active_started_at,
        elapsedSeconds: value.active_elapsed_seconds,
      }
    : null
  const latest = value.latest_execution_id
    ? {
        id: value.latest_execution_id,
        role: value.latest_role ?? productTerm('run').toLowerCase(),
        status: value.latest_execution_status,
        stoppedAt: value.latest_stopped_at,
        runtimeSeconds: value.latest_runtime_seconds,
      }
    : null
  const totalTokens =
    value.tokens.input_tokens +
    value.tokens.output_tokens +
    value.tokens.cache_read_tokens +
    value.tokens.cache_write_tokens
  const costHeadline =
    value.cost.coverage === 'complete' && value.cost.complete_total
      ? formatMoneyAmount(value.cost.complete_total)
      : value.cost.coverage === 'pending'
        ? 'Pending'
        : value.cost.coverage === 'no_usage'
          ? 'No usage'
          : 'Cost unknown'

  return (
    <section className={cn('space-y-3', className)}>
      <div className="grid grid-cols-2 gap-2">
        <Metric label={productTerm('run', 0)} value={formatTokenCount(value.counts.task_execution_count)} />
        <Metric label="Runtime" value={formatRuntimeSeconds(value.total_runtime_seconds)} />
        <Metric
          label="Tokens"
          title={`${formatTokenCount(value.tokens.input_tokens)} input / ${formatTokenCount(value.tokens.output_tokens)} output / ${formatTokenCount(value.tokens.cache_read_tokens)} cache read / ${formatTokenCount(value.tokens.cache_write_tokens)} cache write`}
          value={formatTokenCount(totalTokens, true)}
        />
        <Metric label="Cost" value={costHeadline} />
      </div>

      <CostSummaryView summary={value.cost} compact />

      {active ? (
        <ExecutionLine
          executionId={active.id}
          label="Active"
          primary={`${active.role} running`}
          secondary={`Elapsed ${formatRuntimeSeconds(active.elapsedSeconds)} since ${formatDate(active.startedAt)}`}
          taskId={taskId}
        />
      ) : latest ? (
        <ExecutionLine
          executionId={latest.id}
          label="Latest"
          primary={`${formatStatusLabel(latest.status)} ${latest.role}`}
          secondary={`Runtime ${formatRuntimeSeconds(latest.runtimeSeconds)} stopped ${formatDate(latest.stoppedAt)}`}
          taskId={taskId}
        />
      ) : (
        <p className="rounded-md border border-dashed px-3 py-2 text-xs text-muted-foreground">
          No {productTerm('run', 0).toLowerCase()} yet.
        </p>
      )}
    </section>
  )
}

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

function ExecutionLine({
  label,
  primary,
  secondary,
  executionId,
  taskId,
}: {
  label: string
  primary: string
  secondary: string
  executionId: string
  taskId?: string
}) {
  return (
    <div className="rounded-md border bg-background px-3 py-2">
      <div className="flex min-w-0 items-center justify-between gap-3">
        <p className="font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
          {label}
        </p>
        {taskId ? (
          <Link
            to="/tasks/$taskId/executions/$executionId"
            params={{ taskId, executionId }}
            className="truncate font-mono text-micro text-primary hover:underline focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
          >
            {executionId}
          </Link>
        ) : (
          <span className="truncate font-mono text-micro text-muted-foreground">{executionId}</span>
        )}
      </div>
      <p className="mt-1 truncate text-sm font-medium text-foreground">{primary}</p>
      <p className="mt-0.5 text-xs text-muted-foreground">{secondary}</p>
    </div>
  )
}
