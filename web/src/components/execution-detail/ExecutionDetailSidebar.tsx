import { useState } from 'react'
import { ArrowBendUpLeft, Check, Clock, Copy, GitBranch, Info, Play, Spinner, StopCircle } from '@phosphor-icons/react'

import { PlanChecklist } from '@/components/plan-checklist'
import {
  ExecutionLivenessNotice,
  getExecutionLivenessNotice,
} from '@/components/execution-detail/ExecutionLivenessNotice'
import { ExecutionObservabilitySection } from '@/components/execution-detail/ExecutionObservabilitySection'
import { ExecutionStatusBadge } from '@/components/execution-detail/ExecutionStatusBadge'
import { formatDate, formatRelativeDate, shortHash } from '@/components/execution-detail/execution-detail-format'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Separator } from '@/components/ui/separator'
import { Skeleton } from '@/components/ui/skeleton'
import { Tooltip } from '@/components/ui/tooltip'
import { cn } from '@/lib/cn'
import { isResumeExecution, roleDisplayName } from '@/lib/execution-utils'
import { productTerm } from '@/lib/i18n'
import type { Execution, ExecutionUsage, LogEntry } from '@/types/generated'

function CopyableId({ value, label }: { value: string; label?: string }) {
  const [copied, setCopied] = useState(false)
  const copy = () => {
    navigator.clipboard.writeText(value)
    setCopied(true)
    setTimeout(() => setCopied(false), 1500)
  }
  return (
    <button
      type="button"
      onClick={copy}
      className="group/copy inline-flex items-center gap-1.5 text-xs font-mono text-muted-foreground hover:text-foreground transition-colors cursor-pointer"
      aria-label={`Copy ${label ?? 'value'}`}
    >
      <span className="truncate">{value}</span>
      {copied ? (
        <Check className="h-3 w-3 shrink-0 text-emerald-500" />
      ) : (
        <Copy className="h-3 w-3 shrink-0 opacity-0 group-hover/copy:opacity-60 transition-opacity" />
      )}
    </button>
  )
}

type HookLogEntry = {
  event: string
  status: 'success' | 'failure' | string
  hook_type: 'script' | string
  command?: string
  plugin_name?: string | null
  duration_ms: number
  exit_code?: number | null
  timeout?: boolean
  working_dir?: string
  error?: string
  stdout?: string
  stderr?: string
}

type SidebarActions = {
  onStop?: () => void
  stopPending?: boolean
  onRefresh?: () => void
  refreshPending?: boolean
  onContinue?: () => void
  continuePending?: boolean
  onRetry?: () => void
  retryPending?: boolean
}

export function ExecutionDetailSidebar({
  isLoading,
  execution,
  logs,
  usage,
  hookLogs,
  agentName,
  taskId,
  onNavigateParent,
  actions,
}: {
  isLoading: boolean
  execution: Execution | null
  logs: LogEntry[]
  usage: ExecutionUsage[]
  hookLogs: HookLogEntry[]
  agentName: (agentId?: string | null) => string | null | undefined
  taskId: string
  onNavigateParent: (taskId: string, executionId: string) => void
  actions?: SidebarActions
}) {
  const remainingPlanItems = execution?.plan_progress?.remaining ?? 0
  const completedWithOpenPlan =
    execution?.status === 'completed' && execution.role !== 'planner' && remainingPlanItems > 0
  const livenessNotice = execution ? getExecutionLivenessNotice(execution) : null
  const terminalRecoveryLabel = actions?.onContinue
    ? 'Continue session'
    : actions?.onRetry
      ? 'Retry run'
      : undefined

  const hasActions = Boolean(actions?.onStop || actions?.onContinue || actions?.onRetry)

  return (
    <aside className="flex h-full flex-col">
      <div className="flex items-center justify-between border-b px-4 py-2.5 shrink-0">
        <h3 className="text-xs font-semibold uppercase tracking-wider text-muted-foreground">Details</h3>
        <kbd className="rounded border bg-muted px-1.5 py-0.5 text-micro font-mono text-muted-foreground">I</kbd>
      </div>

      <div className="flex-1 min-h-0 overflow-auto p-4 space-y-5">
        {isLoading ? (
          <div className="space-y-4">
            {Array.from({ length: 5 }, (_, i) => (
              <div key={i} className="space-y-1.5">
                <Skeleton className="h-3 w-16" />
                <Skeleton className="h-4 w-full" />
              </div>
            ))}
          </div>
        ) : execution ? (
          <>
            {execution.parent_execution_id && (
              <section>
                <div className="flex items-center gap-2 rounded-md border border-dashed px-2.5 py-2">
                  <ArrowBendUpLeft className="h-3.5 w-3.5 shrink-0 text-muted-foreground" />
                  <div className="min-w-0 flex-1">
                    <p className="text-xs text-muted-foreground">
                      {isResumeExecution(execution)
                        ? `Continues ${roleDisplayName(execution.role)} session`
                        : 'Follow-up'}
                    </p>
                    <button
                      className="text-[11px] font-mono text-primary hover:underline cursor-pointer"
                      type="button"
                      onClick={() => onNavigateParent(taskId, execution.parent_execution_id ?? '')}
                    >
                      {execution.parent_execution_id.slice(0, 8)}
                    </button>
                  </div>
                </div>
              </section>
            )}

            <section className="space-y-3">
              <div className="flex items-center gap-2">
                <ExecutionStatusBadge status={execution.status} />
                <Badge variant="outline" className="text-xs">{execution.role}</Badge>
              </div>
              <div className="space-y-1">
                <p className="text-micro font-medium uppercase tracking-wider text-muted-foreground">
                  {productTerm('run')} ID
                </p>
                <CopyableId value={execution.id} label={`${productTerm('run').toLowerCase()} ID`} />
              </div>
              {execution.agent_id && (
                <div className="space-y-1">
                  <p className="text-micro font-medium uppercase tracking-wider text-muted-foreground">Agent</p>
                  <p className="text-sm font-medium">{agentName(execution.agent_id)}</p>
                </div>
              )}
              {execution.agent_session_id && (
                <div className="space-y-1">
                  <p className="text-micro font-medium uppercase tracking-wider text-muted-foreground">Session ID</p>
                  <CopyableId value={execution.agent_session_id} label="session ID" />
                </div>
              )}
            </section>

            {livenessNotice ? (
              <ExecutionLivenessNotice
                execution={execution}
                actions={
                  execution.status === 'running'
                    ? {
                        onRefresh: actions?.onRefresh,
                        refreshPending: actions?.refreshPending,
                      }
                    : undefined
                }
                nextActionLabel={terminalRecoveryLabel}
              />
            ) : null}

            <Separator />

            <ExecutionObservabilitySection execution={execution} logs={logs} usage={usage} />

            {execution.plan_progress || execution.plan_artifact ? (
              <>
                <Separator />
                <section>
                  {completedWithOpenPlan ? (
                    <div className="mb-3 rounded-lg border border-amber-300 bg-amber-50 p-3 text-sm text-amber-900 dark:border-amber-800 dark:bg-amber-950 dark:text-amber-200">
                      <p className="font-semibold">Task Still In Progress</p>
                      <p className="mt-1">
                        This {productTerm('run').toLowerCase()} completed, but {remainingPlanItems} plan checklist{' '}
                        {remainingPlanItems === 1 ? 'item is' : 'items are'} still unchecked. The
                        current {productTerm('phase').toLowerCase()} will not move to review until the checklist is complete.
                      </p>
                    </div>
                  ) : null}
                  <PlanChecklist
                    progress={execution.plan_progress}
                    artifact={execution.plan_artifact}
                  />
                </section>
              </>
            ) : null}

            <Separator />

            {(execution.before_sha || execution.after_sha) && (
              <>
                <section className="space-y-3">
                  <div className="flex items-center gap-1.5 text-micro font-medium uppercase tracking-wider text-muted-foreground">
                    <GitBranch className="h-3 w-3" />
                    <span>Git</span>
                  </div>
                  <div className="grid grid-cols-2 gap-3">
                    <div className="space-y-1">
                      <p className="text-micro text-muted-foreground">Before</p>
                      <code className="text-xs font-mono text-muted-foreground">{shortHash(execution.before_sha)}</code>
                    </div>
                    <div className="space-y-1">
                      <p className="text-micro text-muted-foreground">After</p>
                      <code className="text-xs font-mono text-muted-foreground">{shortHash(execution.after_sha)}</code>
                    </div>
                  </div>
                </section>
                <Separator />
              </>
            )}

            <section className="space-y-3">
              <div className="flex items-center gap-1.5 text-micro font-medium uppercase tracking-wider text-muted-foreground">
                <Clock className="h-3 w-3" />
                <span>Timeline</span>
              </div>
              <div className="space-y-2">
                <div className="flex items-center justify-between">
                  <span className="text-xs text-muted-foreground">Created</span>
                  <Tooltip content={formatDate(execution.created_at)}>
                    <span className="text-xs cursor-default">{formatRelativeDate(execution.created_at)}</span>
                  </Tooltip>
                </div>
                <div className="flex items-center justify-between">
                  <span className="text-xs text-muted-foreground">Updated</span>
                  <Tooltip content={formatDate(execution.updated_at)}>
                    <span className="text-xs cursor-default">{formatRelativeDate(execution.updated_at)}</span>
                  </Tooltip>
                </div>
              </div>
            </section>

            {execution.error && (
              <>
                <Separator />
                <section className="space-y-2">
                  <p className="text-micro font-medium uppercase tracking-wider text-red-500">Error</p>
                  <div className="rounded-md border border-red-500/20 bg-red-500/5 p-2.5">
                    <p className="text-xs text-red-600 dark:text-red-400 break-all leading-relaxed">{execution.error}</p>
                  </div>
                </section>
              </>
            )}

            {hookLogs.length > 0 && (
              <>
                <Separator />
                <section className="space-y-3">
                  <div className="flex items-center gap-1.5 text-micro font-medium uppercase tracking-wider text-muted-foreground">
                    <Info className="h-3 w-3" />
                    <span>Lifecycle Hooks</span>
                    <span className="ml-auto rounded-full bg-muted px-1.5 py-0.5 text-micro">
                      {hookLogs.length}
                    </span>
                  </div>
                  <div className="space-y-2">
                    {hookLogs.map((entry, idx) => (
                      <div key={idx} className="rounded-md border p-2.5 text-xs space-y-1.5 bg-muted/30">
                        <div className="flex items-center justify-between gap-2">
                          <span className="font-medium truncate">{entry.event}</span>
                          <span className={cn(
                            'shrink-0 inline-flex items-center rounded-full px-1.5 py-0.5 text-micro font-medium',
                            entry.status === 'success'
                              ? 'bg-emerald-500/15 text-emerald-700 dark:text-emerald-400'
                              : 'bg-red-500/15 text-red-700 dark:text-red-400',
                          )}>
                            {entry.status}
                          </span>
                        </div>
                        <p className="text-muted-foreground truncate">
                          {entry.hook_type === 'script' ? entry.command : entry.plugin_name ?? entry.hook_type}
                        </p>
                        <p className="text-muted-foreground/60">{entry.duration_ms}ms</p>
                        <div className="flex flex-wrap gap-x-3 gap-y-1 text-muted-foreground/70">
                          {typeof entry.exit_code === 'number' ? <span>exit {entry.exit_code}</span> : null}
                          {entry.timeout ? <span>timeout</span> : null}
                        </div>
                        {entry.working_dir ? (
                          <p className="break-all font-mono text-micro text-muted-foreground/70">{entry.working_dir}</p>
                        ) : null}
                        {entry.error && <p className="text-red-600 dark:text-red-400 break-all">{entry.error}</p>}
                        {entry.stdout ? (
                          <pre className="max-h-24 overflow-auto whitespace-pre-wrap rounded bg-background p-2 font-mono text-micro text-foreground">
                            {entry.stdout}
                          </pre>
                        ) : null}
                        {entry.stderr ? (
                          <pre className="max-h-24 overflow-auto whitespace-pre-wrap rounded bg-red-500/10 p-2 font-mono text-micro text-red-700 dark:text-red-300">
                            {entry.stderr}
                          </pre>
                        ) : null}
                      </div>
                    ))}
                  </div>
                </section>
              </>
            )}
          </>
        ) : (
          <div className="flex flex-col items-center justify-center py-8 text-center">
            <Info className="h-8 w-8 text-muted-foreground/40 mb-2" />
            <p className="text-sm text-muted-foreground">{productTerm('run')} not found</p>
          </div>
        )}
      </div>

      {hasActions && (
        <div className="shrink-0 border-t px-4 py-3 flex flex-wrap gap-2">
          {actions?.onStop && (
            <Tooltip content={`Stop this ${productTerm('run').toLowerCase()} without cancelling the task`}>
              <Button
                size="sm"
                variant="outline"
                className="gap-1.5 text-red-600 hover:text-red-700 hover:bg-red-50 dark:text-red-400 dark:hover:bg-red-950/30"
                disabled={actions.stopPending}
                onClick={actions.onStop}
              >
                {actions.stopPending ? (
                  <Spinner className="h-3.5 w-3.5 animate-spin" />
                ) : (
                  <StopCircle className="h-3.5 w-3.5" />
                )}
                Stop {productTerm('run')}
              </Button>
            </Tooltip>
          )}
          {actions?.onContinue && (
            <Tooltip content="Resume with the same agent session context">
              <Button
                size="sm"
                variant="outline"
                className="gap-1.5"
                disabled={actions.continuePending}
                onClick={actions.onContinue}
              >
                {actions.continuePending ? (
                  <Spinner className="h-3.5 w-3.5 animate-spin" />
                ) : (
                  <Play className="h-3.5 w-3.5" />
                )}
                Continue Session
              </Button>
            </Tooltip>
          )}
          {actions?.onRetry && (
            <Tooltip content={`Retry this ${productTerm('run').toLowerCase()} with a new bounded attempt`}>
              <Button
                size="sm"
                variant="outline"
                className="gap-1.5"
                disabled={actions.retryPending}
                onClick={actions.onRetry}
              >
                {actions.retryPending ? (
                  <Spinner className="h-3.5 w-3.5 animate-spin" />
                ) : (
                  <ArrowBendUpLeft className="h-3.5 w-3.5" />
                )}
                {actions.retryPending ? 'Retrying…' : `Retry ${productTerm('run')}`}
              </Button>
            </Tooltip>
          )}
        </div>
      )}
    </aside>
  )
}
