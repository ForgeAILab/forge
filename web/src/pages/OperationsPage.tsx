import { Link } from '@tanstack/react-router'
import { ArrowClockwise, CheckCircle, Pulse, WarningCircle } from '@phosphor-icons/react'
import type { ReactNode } from 'react'
import { useOperationsStatusQuery, useRefreshOperationsMutation } from '@/api/hooks'
import { ErrorBanner } from '@/components/error-banner'
import { PlanChecklist } from '@/components/plan-checklist'
import { PolicyBadge } from '@/components/policy-badge'
import { CostSummaryView } from '@/components/analytics/CostSummary'
import { formatRuntimeSeconds, formatTokenCount } from '@/components/task-execution-observability'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { cn } from '@/lib/cn'
import { productTerm } from '@/lib/i18n'
import { formatMoneyAmount } from '@/lib/money-format'
import type {
  ActiveExecutionSummary,
  AgentPressureSummary,
  DaemonIssueSummary,
  DaemonPressureSummary,
  OperatorSeverity,
  RecentErrorSummary,
  RetryPressureSummary,
  TokenTotalsSummary,
  WorkspaceCleanupSummary,
} from '@/types/generated'

const severityConfig: Record<OperatorSeverity, { label: string; className: string; dot: string }> =
  {
    healthy: {
      label: 'Healthy',
      className: 'border-emerald-500/20 bg-emerald-500/15 text-emerald-700 dark:text-emerald-400',
      dot: 'bg-emerald-500',
    },
    attention: {
      label: 'Attention',
      className: 'border-yellow-500/25 bg-yellow-500/15 text-yellow-700 dark:text-yellow-300',
      dot: 'bg-yellow-500',
    },
    blocked: {
      label: 'Blocked',
      className: 'border-orange-500/25 bg-orange-500/15 text-orange-700 dark:text-orange-400',
      dot: 'bg-orange-500',
    },
    error: {
      label: 'Error',
      className: 'border-red-500/25 bg-red-500/15 text-red-700 dark:text-red-400',
      dot: 'bg-red-500',
    },
  }

function formatDate(value?: string | null): string {
  if (!value) return '-'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return date.toLocaleString()
}

function SeverityBadge({ severity }: { severity: OperatorSeverity }) {
  const config = severityConfig[severity]
  return (
    <span
      className={cn(
        'inline-flex items-center gap-2 rounded-md border px-3 py-1 text-sm font-medium',
        config.className,
      )}
    >
      <span className={cn('h-2 w-2 rounded-full', config.dot)} />
      {config.label}
    </span>
  )
}

function Section({
  title,
  count,
  children,
}: {
  title: string
  count: number
  children: ReactNode
}) {
  if (count === 0) return null
  return (
    <section className="rounded-xl border border-border-subtle bg-background shadow-card">
      <header className="flex items-center justify-between border-b px-4 py-3">
        <h2 className="font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
          {title}
        </h2>
        <span className="font-mono text-[11px] text-muted-foreground">{count}</span>
      </header>
      {children}
    </section>
  )
}

function StatCard({ label, value }: { label: string; value: number | string }) {
  return (
    <div className="rounded-lg border bg-background px-3.5 py-3 shadow-xs transition-shadow hover:shadow-card">
      <p className="mb-2 font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
        {label}
      </p>
      <p className="font-mono text-xl font-semibold tabular-nums text-foreground">{value}</p>
    </div>
  )
}

function entityHref(entityType: string, entityId: string): string | undefined {
  if (entityType === 'task') return `/tasks/${entityId}`
  if (entityType === 'execution') return `/executions/${entityId}`
  if (entityType === 'daemon') return `/daemons/${entityId}`
  return undefined
}

function EntityLink({ href, children }: { href?: string; children: ReactNode }) {
  if (!href) return <>{children}</>
  return (
    <a
      href={href}
      className="cursor-pointer font-mono text-xs text-primary hover:underline focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
    >
      {children}
    </a>
  )
}

function tokenTotal(tokens: TokenTotalsSummary | null): number | null {
  if (!tokens) return null
  return (
    tokens.tokens.input_tokens +
    tokens.tokens.output_tokens +
    tokens.tokens.cache_read_tokens +
    tokens.tokens.cache_write_tokens
  )
}

function formatRateLimitSnapshot(snapshot: Record<string, unknown> | null): string | null {
  if (!snapshot) return null
  const entries = Object.entries(snapshot).slice(0, 3)
  if (entries.length === 0) return null
  return entries
    .map(([key, value]) => {
      if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
        return `${key}: ${String(value)}`
      }
      if (value == null) return `${key}: null`
      return `${key}: ${JSON.stringify(value)}`
    })
    .join(' / ')
}

function ActiveExecutionsSection({ executions }: { executions: ActiveExecutionSummary[] }) {
  return (
    <Section title={`Active ${productTerm('run', 0)}`} count={executions.length}>
      <div className="divide-y">
        {executions.map((execution) => (
          <div
            key={execution.execution_id}
            className="grid gap-3 px-4 py-3 transition-colors hover:bg-muted/20 lg:grid-cols-[minmax(0,1.2fr)_minmax(0,1fr)_minmax(220px,0.8fr)]"
          >
            <div className="min-w-0 space-y-1">
              <div className="flex min-w-0 flex-wrap items-center gap-2">
                <EntityLink href={`/executions/${execution.execution_id}`}>
                  {execution.execution_id}
                </EntityLink>
                <span className="text-xs text-muted-foreground">for</span>
                <Link
                  to="/tasks/$taskId"
                  params={{ taskId: execution.task_id }}
                  className="font-mono text-xs text-primary hover:underline"
                >
                  {execution.task_title ?? execution.task_id}
                </Link>
              </div>
              <p
                className="truncate text-sm text-muted-foreground"
                title={execution.workspace_path ?? undefined}
              >
                {execution.workspace_path ?? 'No workspace path'}
              </p>
              <div className="flex flex-wrap gap-x-3 gap-y-1 text-xs text-muted-foreground">
                <span>Elapsed {formatRuntimeSeconds(execution.elapsed_seconds)}</span>
                <span>Role {execution.role}</span>
                {execution.agent_name || execution.agent_id ? (
                  <span>Agent {execution.agent_name ?? execution.agent_id}</span>
                ) : null}
                <span>Started {formatDate(execution.started_at)}</span>
                {execution.daemon_id ? (
                  <EntityLink href={`/daemons/${execution.daemon_id}`}>
                    Daemon {execution.daemon_id}
                  </EntityLink>
                ) : null}
                {execution.session_id ? <span>Session {execution.session_id}</span> : null}
                {execution.workspace_id ? <span>Workspace {execution.workspace_id}</span> : null}
              </div>
            </div>

            <div className="min-w-0 space-y-2">
              {execution.effective_policy ? (
                <PolicyBadge policy={execution.effective_policy} />
              ) : null}
              <p
                className="truncate text-xs text-muted-foreground"
                title={execution.latest_event ?? undefined}
              >
                {execution.last_event ?? execution.latest_event ?? 'No recent event'}
              </p>
              <div className="flex flex-wrap gap-x-3 gap-y-1 text-xs text-muted-foreground">
                <span>{execution.turn_count} turns</span>
                {execution.last_event_time ? (
                  <span>Last event {formatDate(execution.last_event_time)}</span>
                ) : null}
                {execution.token_totals ? (
                  <span
                    title={`${formatTokenCount(execution.token_totals.tokens.input_tokens)} input / ${formatTokenCount(execution.token_totals.tokens.output_tokens)} output`}
                  >
                    {formatTokenCount(tokenTotal(execution.token_totals), true)} tokens
                  </span>
                ) : null}
                {execution.token_totals ? (
                  <span>
                    {execution.token_totals.cost.coverage === 'pending'
                      ? 'Cost pending'
                      : execution.token_totals.cost.coverage === 'complete' && execution.token_totals.cost.complete_total
                        ? formatMoneyAmount(execution.token_totals.cost.complete_total)
                        : execution.token_totals.cost.coverage === 'no_usage'
                          ? 'No usage'
                          : 'Cost unknown'}
                  </span>
                ) : null}
                {formatRateLimitSnapshot(execution.rate_limit_snapshot) ? (
                  <span
                    className="truncate"
                    title={formatRateLimitSnapshot(execution.rate_limit_snapshot) ?? undefined}
                  >
                    Rate {formatRateLimitSnapshot(execution.rate_limit_snapshot)}
                  </span>
                ) : null}
              </div>
            </div>

            <PlanChecklist progress={execution.plan_progress} />
          </div>
        ))}
      </div>
    </Section>
  )
}

function DaemonPressureSection({ items }: { items: DaemonPressureSummary[] }) {
  return (
    <Section title={`${productTerm('runtime')} Pressure`} count={items.length}>
      <div className="divide-y">
        {items.map((item) => (
          <div
            key={item.daemon_id}
            className="flex min-w-0 items-center justify-between gap-4 px-4 py-3 transition-colors hover:bg-muted/20"
          >
            <div className="min-w-0">
              <EntityLink href={`/daemons/${item.daemon_id}`}>
                {item.hostname ?? item.daemon_id}
              </EntityLink>
              <p className="mt-1 text-xs text-muted-foreground">
                {item.active_sessions}/{item.max_sessions ?? '-'} active sessions
              </p>
            </div>
            <CapacityBadge atCapacity={item.at_capacity} />
          </div>
        ))}
      </div>
    </Section>
  )
}

function AgentPressureSection({ items }: { items: AgentPressureSummary[] }) {
  return (
    <Section title="Agent Pressure" count={items.length}>
      <div className="divide-y">
        {items.map((item) => (
          <div
            key={item.agent_id}
            className="flex min-w-0 items-center justify-between gap-4 px-4 py-3 transition-colors hover:bg-muted/20"
          >
            <div className="min-w-0">
              <p className="truncate text-sm font-medium text-foreground">{item.agent_name}</p>
              <div className="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-xs text-muted-foreground">
                <span>
                  {item.active_sessions}/{item.max_sessions} active sessions
                </span>
                {item.daemon_id ? (
                  <EntityLink href={`/daemons/${item.daemon_id}`}>
                    Daemon {item.daemon_id}
                  </EntityLink>
                ) : null}
              </div>
            </div>
            <CapacityBadge atCapacity={item.at_capacity} />
          </div>
        ))}
      </div>
    </Section>
  )
}

function CapacityBadge({ atCapacity }: { atCapacity: boolean }) {
  return (
    <span
      className={cn(
        'shrink-0 rounded-md border px-2.5 py-1 text-xs font-medium',
        atCapacity
          ? 'border-orange-500/25 bg-orange-500/15 text-orange-700 dark:text-orange-400'
          : 'border-emerald-500/20 bg-emerald-500/10 text-emerald-700 dark:text-emerald-400',
      )}
    >
      {atCapacity ? 'At capacity' : 'Available'}
    </span>
  )
}

function BlockedTasksSection({
  tasks,
}: {
  tasks: Array<{
    task_id: string
    title: string
    blocked_reason: string | null
    blocked_since: string | null
  }>
}) {
  return (
    <Section title="Blocked Tasks" count={tasks.length}>
      <div className="divide-y">
        {tasks.map((task) => (
          <div
            key={task.task_id}
            className="flex min-w-0 items-start justify-between gap-4 px-4 py-3 transition-colors hover:bg-muted/20"
          >
            <div className="min-w-0">
              <Link
                to="/tasks/$taskId"
                params={{ taskId: task.task_id }}
                className="text-sm font-medium text-primary hover:underline focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
              >
                {task.title}
              </Link>
              <p className="mt-1 break-words text-xs text-muted-foreground">
                {task.blocked_reason ?? 'No blocked reason recorded'}
              </p>
            </div>
            <span className="shrink-0 text-xs text-muted-foreground">
              {formatDate(task.blocked_since)}
            </span>
          </div>
        ))}
      </div>
    </Section>
  )
}

function DaemonIssuesSection({ issues }: { issues: DaemonIssueSummary[] }) {
  return (
    <Section title={`${productTerm('runtime')} Issues`} count={issues.length}>
      <div className="divide-y">
        {issues.map((issue) => (
          <div
            key={`${issue.daemon_id}-${issue.issue}`}
            className="flex min-w-0 items-start justify-between gap-4 px-4 py-3 transition-colors hover:bg-muted/20"
          >
            <div className="min-w-0">
              <div className="flex flex-wrap items-center gap-2">
                <EntityLink href={`/daemons/${issue.daemon_id}`}>
                  {issue.hostname ?? issue.daemon_id}
                </EntityLink>
                <SeverityBadge severity={issue.severity} />
              </div>
              <p className="mt-1 break-words text-sm text-muted-foreground">{issue.issue}</p>
            </div>
            <span className="shrink-0 text-xs text-muted-foreground">
              {formatDate(issue.detected_at)}
            </span>
          </div>
        ))}
      </div>
    </Section>
  )
}

function WorkspaceCleanupSection({ items }: { items: WorkspaceCleanupSummary[] }) {
  return (
    <Section title="Workspace Cleanup" count={items.length}>
      <div className="divide-y">
        {items.map((item) => (
          <div
            key={item.workspace_id}
            className="grid gap-2 px-4 py-3 transition-colors hover:bg-muted/20 md:grid-cols-[minmax(0,1fr)_auto]"
          >
            <div className="min-w-0">
              <div className="flex flex-wrap items-center gap-2">
                <span className="font-mono text-xs text-muted-foreground">{item.workspace_id}</span>
                <Link
                  to="/tasks/$taskId"
                  params={{ taskId: item.task_id }}
                  className="font-mono text-xs text-primary hover:underline"
                >
                  {item.task_id}
                </Link>
              </div>
              <p
                className="mt-1 truncate font-mono text-xs text-muted-foreground"
                title={item.worktree_path ?? undefined}
              >
                {item.worktree_path ?? 'No worktree path'}
              </p>
            </div>
            <span className="text-xs text-muted-foreground">
              Cleanup after {formatDate(item.cleanup_after)}
            </span>
          </div>
        ))}
      </div>
    </Section>
  )
}

function RetryPressureSection({ items }: { items: RetryPressureSummary[] }) {
  return (
    <Section title="Retry Pressure" count={items.length}>
      <div className="divide-y">
        {items.map((item) => (
          <div
            key={item.task_id}
            className="flex min-w-0 items-start justify-between gap-4 px-4 py-3 transition-colors hover:bg-muted/20"
          >
            <div className="min-w-0">
              <Link
                to="/tasks/$taskId"
                params={{ taskId: item.task_id }}
                className="text-sm font-medium text-primary hover:underline focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring"
              >
                {item.title}
              </Link>
              <p className="mt-1 text-xs text-muted-foreground">{item.current_state}</p>
              <div className="mt-1 flex flex-wrap gap-x-3 gap-y-1 text-xs text-muted-foreground">
                {item.retry_reason ? <span>{item.retry_reason}</span> : null}
                {item.due_time ? <span>Due {formatDate(item.due_time)}</span> : null}
              </div>
              {item.last_error ? (
                <p className="mt-1 break-words text-xs text-red-600 dark:text-red-300">
                  {item.last_error}
                </p>
              ) : null}
            </div>
            <span className="shrink-0 font-mono text-xs text-muted-foreground">
              {item.attempt_count}/{item.max_attempts ?? '-'} attempts
            </span>
          </div>
        ))}
      </div>
    </Section>
  )
}

function RecentErrorsSection({ errors }: { errors: RecentErrorSummary[] }) {
  return (
    <Section title="Recent Errors" count={errors.length}>
      <div className="divide-y">
        {errors.map((error) => (
          <div
            key={`${error.entity_type}-${error.entity_id}-${error.occurred_at}`}
            className="flex min-w-0 items-start gap-3 px-4 py-3 transition-colors hover:bg-muted/20"
          >
            <WarningCircle size={16} className="mt-0.5 shrink-0 text-red-500" />
            <div className="min-w-0 flex-1">
              <div className="flex flex-wrap items-center gap-2">
                <SeverityBadge severity={error.severity} />
                <EntityLink href={entityHref(error.entity_type, error.entity_id)}>
                  {error.entity_type}:{error.entity_id}
                </EntityLink>
                <span className="text-xs text-muted-foreground">
                  {formatDate(error.occurred_at)}
                </span>
              </div>
              <p className="mt-1 break-words text-sm text-muted-foreground">{error.error}</p>
            </div>
          </div>
        ))}
      </div>
    </Section>
  )
}

export function OperationsPage() {
  const statusQuery = useOperationsStatusQuery()
  const refreshOperations = useRefreshOperationsMutation()
  const status = statusQuery.data

  if (statusQuery.isLoading) {
    return (
      <div className="space-y-4">
        <Skeleton className="h-20 w-full" />
        <div className="grid grid-cols-2 gap-3 lg:grid-cols-6">
          {[0, 1, 2, 3, 4, 5].map((item) => (
            <Skeleton key={item} className="h-24 w-full" />
          ))}
        </div>
        <Skeleton className="h-56 w-full" />
      </div>
    )
  }

  if (statusQuery.isError || !status) {
    return (
      <ErrorBanner
        error={statusQuery.error}
        fallback="Operations status failed to load"
        onRetry={() => void statusQuery.refetch()}
      />
    )
  }

  const empty =
    status.active_executions.length === 0 &&
    status.blocked_tasks.length === 0 &&
    status.daemon_issues.length === 0 &&
    status.daemon_pressure.length === 0 &&
    status.agent_pressure.length === 0 &&
    status.workspace_cleanup.length === 0 &&
    status.retry_pressure.length === 0 &&
    status.recent_errors.length === 0

  return (
    <div className="space-y-4">
      <header className="rounded-xl border border-border-subtle bg-background px-5 py-4 shadow-card">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="min-w-0">
            <div className="flex items-center gap-2">
              <Pulse size={20} className="text-muted-foreground" />
              <h1 className="text-xl font-semibold">Operations</h1>
            </div>
            <p className="mt-1 text-sm text-muted-foreground">
              Snapshot computed {formatDate(status.computed_at)}
            </p>
          </div>
          <div className="flex items-center gap-2">
            <Button
              className="gap-1.5"
              disabled={refreshOperations.isPending}
              size="sm"
              variant="outline"
              onClick={() => refreshOperations.mutate()}
            >
              <ArrowClockwise
                size={14}
                className={refreshOperations.isPending ? 'animate-spin' : undefined}
              />
              Refresh
            </Button>
            <SeverityBadge severity={status.overall_severity} />
          </div>
        </div>
      </header>

      <div className="grid grid-cols-2 gap-3 lg:grid-cols-6">
        <StatCard label="Active" value={status.active_executions.length} />
        <StatCard label="Blocked" value={status.blocked_tasks.length} />
        <StatCard label={productTerm('runtime', 0)} value={status.daemon_issues.length} />
        <StatCard label="Cleanup" value={status.workspace_cleanup.length} />
        <StatCard label="Retries" value={status.retry_pressure.length} />
        <StatCard label="Errors" value={status.recent_errors.length} />
      </div>

      {status.usage_summary ? (
        <div className="space-y-3">
          <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
            <StatCard
              label="Task executions"
              value={status.usage_summary.counts.task_execution_count}
            />
            <StatCard
              label="Provider attempts"
              value={status.usage_summary.counts.provider_attempt_count}
            />
            <StatCard label="Input Tokens" value={status.usage_summary.tokens.input_tokens} />
            <StatCard label="Output Tokens" value={status.usage_summary.tokens.output_tokens} />
          </div>
          <CostSummaryView summary={status.usage_summary.cost} />
        </div>
      ) : null}

      {empty ? (
        <div className="rounded-xl border border-dashed bg-background p-10 text-center">
          <div className="mb-3 flex justify-center">
            <CheckCircle size={32} weight="duotone" className="text-success" />
          </div>
          <p className="text-sm font-medium text-foreground">All systems healthy</p>
          <p className="mt-1.5 text-sm text-muted-foreground">
            No active {productTerm('run', 0).toLowerCase()}, blocked tasks,{' '}
            {productTerm('runtime').toLowerCase()} issues, or recent errors.
          </p>
        </div>
      ) : null}

      <ActiveExecutionsSection executions={status.active_executions} />
      <BlockedTasksSection tasks={status.blocked_tasks} />
      <DaemonIssuesSection issues={status.daemon_issues} />
      <DaemonPressureSection items={status.daemon_pressure} />
      <AgentPressureSection items={status.agent_pressure} />
      <WorkspaceCleanupSection items={status.workspace_cleanup} />
      <RetryPressureSection items={status.retry_pressure} />
      <RecentErrorsSection errors={status.recent_errors} />
    </div>
  )
}
