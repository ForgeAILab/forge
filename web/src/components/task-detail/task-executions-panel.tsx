import {   useState } from 'react'
import { useNavigate } from '@tanstack/react-router'
import {  CaretDown, CaretRight } from '@phosphor-icons/react'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { cn } from '@/lib/cn'
import { buildExecutionChains, roleDisplayName, turnLabel } from '@/lib/execution-utils'
import { productTerm } from '@/lib/i18n'
import type { Execution, Review, } from '@/types/generated'

const executionStatusColors: Record<Execution['status'], string> = {
  running: 'bg-sky-100 text-sky-900 dark:bg-sky-950 dark:text-sky-300',
  completed: 'bg-emerald-100 text-emerald-900 dark:bg-emerald-950 dark:text-emerald-300',
  failed: 'bg-red-100 text-red-900 dark:bg-red-950 dark:text-red-300',
  cancelled: 'bg-zinc-100 text-zinc-700 dark:bg-zinc-800 dark:text-zinc-400',
}

export const reviewStatusColors: Record<Review['status'], string> = {
  running: 'bg-amber-100 text-amber-900 dark:bg-amber-950 dark:text-amber-300',
  awaiting_human: 'bg-violet-100 text-violet-900 dark:bg-violet-950 dark:text-violet-300',
  passed: 'bg-emerald-100 text-emerald-900 dark:bg-emerald-950 dark:text-emerald-300',
  failed: 'bg-red-100 text-red-900 dark:bg-red-950 dark:text-red-300',
  cancelled: 'bg-zinc-100 text-zinc-700 dark:bg-zinc-800 dark:text-zinc-400',
}



interface TaskExecutionsPanelProps {
  taskId: string
  executions: Execution[]
  isLoading: boolean
  agentName: (agentId?: string | null) => string | undefined | null
  formatDate: (value?: string | null) => string
  onClose: () => void
}

const VISIBLE_TURNS = 3

export function TaskExecutionsPanel({
  taskId,
  executions,
  isLoading,
  agentName,
  formatDate,
  onClose,
}: TaskExecutionsPanelProps) {
  const navigate = useNavigate()
  const [expandedChains, setExpandedChains] = useState<Set<string>>(new Set())

  if (isLoading) {
    return (
      <div className="space-y-2">
        <Skeleton className="h-16 w-full" />
        <Skeleton className="h-16 w-full" />
      </div>
    )
  }

  if (executions.length === 0) {
    return (
      <div className="rounded-lg border border-dashed p-8 text-center text-sm text-muted-foreground">
        No {productTerm('run', 0).toLowerCase()} yet
      </div>
    )
  }

  const chains = buildExecutionChains(executions)

  return (
    <div className="space-y-2.5">
      {chains.map((chain) => {
        const totalTurns = chain.turns.length
        const lastTurn = chain.turns[totalTurns - 1]
        const sessionAgent = agentName(chain.root.agent_id)
        const isRunning = lastTurn.status === 'running'
        const reversedTurns = [...chain.turns].reverse()
        const isExpanded = expandedChains.has(chain.root.id)
        const hiddenCount = totalTurns - VISIBLE_TURNS
        const visibleTurns = isExpanded || hiddenCount <= 0
          ? reversedTurns
          : reversedTurns.slice(0, VISIBLE_TURNS)

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
                'flex items-center justify-between gap-2 px-3 py-2',
                isRunning ? 'bg-sky-50/60 dark:bg-sky-950/20' : 'bg-muted/40',
              )}
            >
              <div className="flex items-center gap-2 min-w-0">
                <span className="text-xs font-semibold">
                  {roleDisplayName(chain.root.role)} Session
                </span>
                {sessionAgent && (
                  <span className="text-[11px] text-muted-foreground truncate">{sessionAgent}</span>
                )}
                {totalTurns > 1 && (
                  <span className="text-[11px] text-muted-foreground/60">
                    · {totalTurns} turns
                  </span>
                )}
              </div>
              <Badge
                className={cn(
                  'shrink-0 border-transparent text-[11px]',
                  executionStatusColors[lastTurn.status],
                )}
              >
                {lastTurn.status}
              </Badge>
            </div>

            {/* Turn rows — latest first, capped at VISIBLE_TURNS */}
            <div className="divide-y">
              {visibleTurns.map((execution, displayIndex) => {
                const originalIndex = totalTurns - 1 - displayIndex
                return (
                  <button
                    key={execution.id}
                    className="flex w-full items-center gap-2.5 px-3 py-2 text-left transition-colors hover:bg-accent cursor-pointer"
                    type="button"
                    onClick={() => {
                      onClose()
                      void navigate({
                        to: '/tasks/$taskId/executions/$executionId',
                        params: { taskId, executionId: execution.id },
                      })
                    }}
                  >
                    {totalTurns > 1 && (
                      <span className="w-6 shrink-0 text-[11px] font-mono text-muted-foreground/40 text-right">
                        T{originalIndex + 1}
                      </span>
                    )}
                    <div className="min-w-0 flex-1">
                      <div className="flex flex-wrap items-center gap-1.5">
                        <span className="text-xs text-muted-foreground">
                          {turnLabel(originalIndex, execution)}
                        </span>
                        <Badge
                          className={cn(
                            'border-transparent text-[11px]',
                            executionStatusColors[execution.status],
                          )}
                        >
                          {execution.status}
                        </Badge>
                        <span className="text-[11px] text-muted-foreground/50">
                          {formatDate(execution.created_at)}
                        </span>
                      </div>
                    </div>
                    <CaretRight size={13} className="shrink-0 text-muted-foreground/40" />
                  </button>
                )
              })}

              {/* Expand button for older turns */}
              {!isExpanded && hiddenCount > 0 && (
                <button
                  type="button"
                  className="flex w-full items-center justify-center gap-1.5 px-3 py-1.5 text-[11px] text-muted-foreground hover:text-foreground hover:bg-accent transition-colors cursor-pointer"
                  onClick={() => setExpandedChains((prev) => new Set(prev).add(chain.root.id))}
                >
                  <CaretDown size={11} />
                  <span>{hiddenCount} older {hiddenCount === 1 ? 'turn' : 'turns'}</span>
                </button>
              )}
            </div>
          </div>
        )
      })}
    </div>
  )
}

