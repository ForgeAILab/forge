import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from '@tanstack/react-router'
import { CaretRight, Play, Spinner, StopCircle } from '@phosphor-icons/react'
import { toast } from 'sonner'
import { apiFetch } from '@/api/client'
import { qk } from '@/api/query-keys'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { Tooltip } from '@/components/ui/tooltip'
import { getApiErrorMessage } from '@/lib/api-error'
import { cn } from '@/lib/cn'
import { buildExecutionChains, roleDisplayName, turnLabel } from '@/lib/execution-utils'
import { productTerm } from '@/lib/i18n'
import { TaskActionButtons } from '@/components/task-detail/task-action-buttons'
import type { Execution, LaunchExecutionResponse, Offer } from '@/types/generated'

const statusColors: Record<Execution['status'], string> = {
  running: 'bg-sky-100 text-sky-900 dark:bg-sky-950 dark:text-sky-300',
  completed: 'bg-emerald-100 text-emerald-900 dark:bg-emerald-950 dark:text-emerald-300',
  failed: 'bg-red-100 text-red-900 dark:bg-red-950 dark:text-red-300',
  cancelled: 'bg-zinc-100 text-zinc-700 dark:bg-zinc-800 dark:text-zinc-400',
}

type Props = {
  taskId: string
  version: number
  offers: Offer[]
  executions: Execution[]
  isLoading: boolean
  agentName: (agentId?: string | null) => string | undefined | null
  formatDate: (value?: string | null) => string
}

export function TaskExecutionsTab({
  taskId,
  version,
  offers,
  executions,
  isLoading,
  agentName,
  formatDate,
}: Props) {
  const navigate = useNavigate()
  const queryClient = useQueryClient()

  const stopMutation = useMutation({
    mutationFn: (executionId: string) =>
      apiFetch<Execution>(`/executions/${executionId}/stop`, {
        method: 'POST',
        body: JSON.stringify({}),
      }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.executions(taskId) })
      void queryClient.invalidateQueries({ queryKey: qk.task(taskId) })
      void queryClient.invalidateQueries({ queryKey: qk.taskDetail(taskId) })
      toast.success(`${productTerm('run')} stopped`)
    },
    onError: (error) => toast.error(getApiErrorMessage(error, 'Stop failed')),
  })

  const followUpMutation = useMutation({
    mutationFn: (executionId: string) =>
      apiFetch<LaunchExecutionResponse>(`/executions/${executionId}/follow-up`, {
        method: 'POST',
        body: JSON.stringify({ message: 'Resume' }),
      }),
    onSuccess: (response) => {
      void queryClient.invalidateQueries({ queryKey: qk.executions(taskId) })
      void queryClient.invalidateQueries({ queryKey: qk.task(taskId) })
      void queryClient.invalidateQueries({ queryKey: qk.taskDetail(taskId) })
      void navigate({
        to: '/tasks/$taskId/executions/$executionId',
        params: { taskId, executionId: response.data.execution.id },
      })
    },
    onError: (error) => toast.error(getApiErrorMessage(error, 'Follow-up failed')),
  })

  const anyPending = stopMutation.isPending || followUpMutation.isPending
  const freshRetries = (next: Offer[]) =>
    next
      .filter(
        (offer) =>
          offer.action.verb === 'retry' &&
          (offer.action.fresh_session === true ||
            offer.parameters.some(
              (spec) => spec.name === 'fresh_session' && spec.boolean_values?.includes(true),
            )),
      )
      .map((offer) => ({
        ...offer,
        action: { ...offer.action, fresh_session: true } as Offer['action'],
        parameters: offer.parameters.map((spec) =>
          spec.name === 'fresh_session' ? { ...spec, boolean_values: [true] } : spec,
        ),
      }))

  if (isLoading) {
    return (
      <div className="space-y-2">
        <Skeleton className="h-28 w-full" />
        <Skeleton className="h-28 w-full" />
      </div>
    )
  }

  if (executions.length === 0) {
    return (
      <div className="space-y-3">
        <TaskActionButtons
          taskId={taskId}
          version={version}
          offers={offers}
          transformOffers={freshRetries}
        />
        <div className="rounded-md border border-dashed p-8 text-center text-sm text-muted-foreground">
          No {productTerm('run', 0).toLowerCase()} yet
        </div>
      </div>
    )
  }

  const chains = buildExecutionChains(executions)

  return (
    <div className="space-y-3">
      <TaskActionButtons
        taskId={taskId}
        version={version}
        offers={offers}
        transformOffers={freshRetries}
      />
      {chains.map((chain) => {
        const totalTurns = chain.turns.length
        const lastTurn = chain.turns[totalTurns - 1]
        const sessionAgent = agentName(chain.root.agent_id)
        const runningTurns = chain.turns
          .map((execution, index) => ({ execution, turn: index + 1 }))
          .filter(({ execution }) => execution.status === 'running')
        const isRunning = runningTurns.length > 0
        const isTerminal = !isRunning
        const sessionStatus = isRunning ? 'running' : lastTurn.status
        const hasSession = Boolean(lastTurn.agent_session_id)
        const reversedTurns = [...chain.turns].reverse()

        return (
          <div
            key={chain.root.id}
            className={cn(
              'rounded-lg border overflow-hidden',
              isRunning && 'border-sky-200 dark:border-sky-900',
            )}
          >
            {/* Session header */}
            <div
              className={cn(
                'flex items-center justify-between gap-2 px-4 py-3',
                isRunning ? 'bg-sky-50/60 dark:bg-sky-950/20' : 'bg-muted/30',
              )}
            >
              <div className="flex items-center gap-2.5 min-w-0">
                <p className="text-sm font-semibold">{roleDisplayName(chain.root.role)} Session</p>
                {totalTurns > 1 && (
                  <span className="text-xs text-muted-foreground">{totalTurns} turns</span>
                )}
                {sessionAgent && (
                  <span className="text-xs text-muted-foreground/60 truncate">
                    · {sessionAgent}
                  </span>
                )}
              </div>
              <div className="flex flex-wrap items-center gap-2">
                {/* Action buttons on the session header */}
                {runningTurns.map(({ execution, turn }) => (
                  <Tooltip key={execution.id} content={execution.summary || `Stop turn ${turn}`}>
                    <Button
                      size="sm"
                      variant="outline"
                      className="h-7 gap-1.5 px-2.5 text-red-600 hover:text-red-700 hover:bg-red-50 dark:text-red-400 dark:hover:bg-red-950/30"
                      disabled={anyPending}
                      onClick={() => stopMutation.mutate(execution.id)}
                    >
                      {stopMutation.isPending && stopMutation.variables === execution.id ? (
                        <Spinner className="h-3.5 w-3.5 animate-spin" />
                      ) : (
                        <StopCircle className="h-3.5 w-3.5" />
                      )}
                      <span className="text-xs">
                        {runningTurns.length === 1 ? 'Stop' : `Stop turn ${turn}`}
                      </span>
                    </Button>
                  </Tooltip>
                ))}
                {isTerminal && hasSession && (
                  <Tooltip content="Resume with the same agent session">
                    <Button
                      size="sm"
                      variant="outline"
                      className="h-7 gap-1.5 px-2.5"
                      disabled={anyPending}
                      onClick={() => followUpMutation.mutate(lastTurn.id)}
                    >
                      {followUpMutation.isPending ? (
                        <Spinner className="h-3.5 w-3.5 animate-spin" />
                      ) : (
                        <Play className="h-3.5 w-3.5" />
                      )}
                      <span className="text-xs">Continue</span>
                    </Button>
                  </Tooltip>
                )}
                <Badge className={cn('border-transparent', statusColors[sessionStatus])}>
                  {sessionStatus}
                </Badge>
              </div>
            </div>

            {/* Turn rows */}
            <div className="divide-y">
              {reversedTurns.map((execution, displayIndex) => {
                const originalIndex = totalTurns - 1 - displayIndex
                const isLatest = displayIndex === 0

                if (isLatest) {
                  return (
                    <button
                      key={execution.id}
                      type="button"
                      className="flex w-full items-start gap-3 px-4 py-4 text-left transition-colors hover:bg-accent cursor-pointer"
                      onClick={() =>
                        navigate({
                          to: '/tasks/$taskId/executions/$executionId',
                          params: { taskId, executionId: execution.id },
                        })
                      }
                    >
                      <div className="min-w-0 flex-1">
                        <div className="flex flex-wrap items-center gap-2">
                          {totalTurns > 1 && (
                            <span className="text-sm font-semibold">Turn {originalIndex + 1}</span>
                          )}
                          <span
                            className={cn(
                              'text-sm',
                              totalTurns > 1 ? 'text-muted-foreground' : 'font-semibold',
                            )}
                          >
                            {turnLabel(originalIndex, execution)}
                          </span>
                          <Badge
                            className={cn('border-transparent', statusColors[execution.status])}
                          >
                            {execution.status}
                          </Badge>
                          <span className="text-xs text-muted-foreground/60">
                            {formatDate(execution.created_at)}
                          </span>
                        </div>
                        {execution.summary && (
                          <p className="mt-1.5 line-clamp-2 text-xs text-muted-foreground/70">
                            {execution.summary}
                          </p>
                        )}
                      </div>
                      <CaretRight className="mt-0.5 h-4 w-4 shrink-0 text-muted-foreground/40" />
                    </button>
                  )
                }

                return (
                  <button
                    key={execution.id}
                    type="button"
                    className="flex w-full items-center gap-3 px-4 py-2.5 text-left transition-colors hover:bg-accent cursor-pointer"
                    onClick={() =>
                      navigate({
                        to: '/tasks/$taskId/executions/$executionId',
                        params: { taskId, executionId: execution.id },
                      })
                    }
                  >
                    <span className="w-7 shrink-0 text-[11px] font-mono text-muted-foreground/40 text-right">
                      T{originalIndex + 1}
                    </span>
                    <div className="min-w-0 flex-1 flex flex-wrap items-center gap-2">
                      <span className="text-xs text-muted-foreground">
                        {turnLabel(originalIndex, execution)}
                      </span>
                      <Badge
                        className={cn(
                          'border-transparent text-[11px]',
                          statusColors[execution.status],
                        )}
                      >
                        {execution.status}
                      </Badge>
                      <span className="text-[11px] text-muted-foreground/40">
                        {formatDate(execution.created_at)}
                      </span>
                    </div>
                    <CaretRight className="h-3.5 w-3.5 shrink-0 text-muted-foreground/30" />
                  </button>
                )
              })}
            </div>
          </div>
        )
      })}
    </div>
  )
}
