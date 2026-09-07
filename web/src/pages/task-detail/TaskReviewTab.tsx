import { ArrowCounterClockwise, CaretDown, Spinner } from '@phosphor-icons/react'
import { useState } from 'react'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Select } from '@/components/ui/select'
import { Skeleton } from '@/components/ui/skeleton'
import { ChatEntryContainer } from '@/components/chat'
import { WorkflowExceptionPanel } from '@/components/task-detail/workflow-exception-panel'
import { cn } from '@/lib/cn'
import type { RecoveryAction, Review, Task, WorkflowExceptionAction } from '@/types/generated'
import { formatDate } from './utils'
import { ReviewConformancePanel } from './ReviewConformancePanel'

const reviewStatusClassNames: Record<Review['status'], string> = {
  running: 'bg-amber-100 text-amber-900 dark:bg-amber-900/30 dark:text-amber-300',
  awaiting_human: 'bg-violet-100 text-violet-900 dark:bg-violet-900/30 dark:text-violet-300',
  passed: 'bg-emerald-100 text-emerald-900 dark:bg-emerald-900/30 dark:text-emerald-300',
  failed: 'bg-red-100 text-red-900 dark:bg-red-900/30 dark:text-red-300',
  cancelled: 'bg-zinc-100 text-zinc-700 dark:bg-zinc-800 dark:text-zinc-400',
}

function reviewStatusBadge(status: Review['status']) {
  return (
    <Badge
      className={cn(
        'border-transparent',
        reviewStatusClassNames[status],
        status === 'running' && 'animate-pulse',
      )}
    >
      {status === 'awaiting_human' ? 'awaiting human' : status}
    </Badge>
  )
}

function formatDuration(startedAt: string, finishedAt: string | null): string {
  if (!finishedAt) return 'running…'
  const ms = new Date(finishedAt).getTime() - new Date(startedAt).getTime()
  if (ms < 1000) return '< 1s'
  if (ms < 60_000) return `${Math.round(ms / 1000)}s`
  const mins = Math.floor(ms / 60_000)
  const secs = Math.round((ms % 60_000) / 1000)
  return secs > 0 ? `${mins}m ${secs}s` : `${mins}m`
}

function stepOutput(step: Review['step_results'][number]) {
  return step.output_tail?.trim() ? step.output_tail : step.stderr_tail
}

type TaskReviewTabProps = {
  task: Task
  reviews: Review[]
  latestReview?: Review
  reviewsLoading: boolean
  transitionPending: boolean
  triggerReviewPending: boolean
  recoverPending: boolean
  cancelPending: boolean
  terminal: boolean
  expandedHistoryAttempts: Set<number>
  onRerunReview: () => void
  onStatusChange: (status: string, reason?: string) => void
  onRecover: (action: RecoveryAction, input?: { reason?: string; context?: string }) => void
  onOpenWorkflowExceptionAction: (action: WorkflowExceptionAction) => void
  onCancelTask: () => void
  onToggleHistoryAttempt: (attemptNumber: number) => void
}

export function TaskReviewTab({
  task,
  reviews,
  latestReview,
  reviewsLoading,
  transitionPending,
  triggerReviewPending,
  recoverPending,
  cancelPending,
  terminal,
  expandedHistoryAttempts,
  onRerunReview,
  onStatusChange,
  onRecover,
  onOpenWorkflowExceptionAction,
  onCancelTask,
  onToggleHistoryAttempt,
}: TaskReviewTabProps) {
  const [historyOpen, setHistoryOpen] = useState(true)
  const canAct = task.status === 'review'
  const showMergeSelect = canAct && latestReview?.status === 'passed'
  const workflowExceptionActions = task.workflow_exception?.actions ?? []

  return (
    <div className="space-y-3">
      <WorkflowExceptionPanel
        task={task}
        actions={workflowExceptionActions}
        recoverPending={recoverPending}
        terminal={terminal}
        cancelPending={cancelPending}
        onRecover={onRecover}
        onOpenInteractive={onOpenWorkflowExceptionAction}
        onCancelTask={onCancelTask}
      />
      {/* Header card — attempt info + actions */}
      {(task.status === 'review' || reviews.length > 0) && (
        <div className="rounded-lg border p-4">
          <div className="flex flex-wrap items-start justify-between gap-3">
            <div className="space-y-1.5">
              <p className="text-sm font-semibold text-foreground">
                Attempt {latestReview?.attempt_number ?? reviews.length}
              </p>
              {latestReview ? (
                <div className="flex flex-wrap items-center gap-2">
                  {reviewStatusBadge(latestReview.status)}
                  <span className="text-xs text-muted-foreground">
                    {formatDate(latestReview.started_at)}
                  </span>
                  <span className="text-micro text-muted-foreground/50">·</span>
                  <span className="text-xs text-muted-foreground">
                    {formatDuration(latestReview.started_at, latestReview.finished_at)}
                  </span>
                </div>
              ) : (
                <p className="text-xs text-muted-foreground">No review details yet.</p>
              )}
            </div>

            {/* Actions grouped together */}
            <div className="flex flex-wrap items-center gap-2">
              <Button
                size="sm"
                variant="outline"
                disabled={!canAct || triggerReviewPending}
                onClick={onRerunReview}
              >
                {triggerReviewPending ? (
                  <Spinner className="h-3.5 w-3.5 animate-spin" />
                ) : (
                  <ArrowCounterClockwise className="h-3.5 w-3.5" />
                )}
                Re-run review
              </Button>
            </div>
          </div>
        </div>
      )}

      {/* Merge transition */}
      {showMergeSelect && (
        <div className="space-y-1 rounded-lg border p-4">
          <Label htmlFor="review-transition-merging">Transition to</Label>
          <Select
            id="review-transition-merging"
            disabled={transitionPending}
            value=""
            placeholder="Select next status"
            options={[{ value: 'merging', label: 'Merging' }]}
            onChange={(v) => {
              if (v === 'merging') onStatusChange('merging')
            }}
          />
        </div>
      )}

      {latestReview && <ReviewConformancePanel conformance={latestReview.details.conformance} />}

      {/* Step results */}
      <StepResultsSection latestReview={latestReview} reviewsLoading={reviewsLoading} />

      {/* Review history */}
      {reviews.length > 0 && (
        <div className="rounded-lg border">
          <button
            type="button"
            className="flex w-full cursor-pointer items-center justify-between gap-2 px-4 py-3 text-left"
            onClick={() => setHistoryOpen((o) => !o)}
          >
            <span className="text-sm font-medium">
              Review history{' '}
              <span className="ml-1 text-xs font-normal text-muted-foreground">
                ({reviews.length} {reviews.length === 1 ? 'attempt' : 'attempts'})
              </span>
            </span>
            <CaretDown
              className={cn(
                'h-3.5 w-3.5 shrink-0 text-muted-foreground/50 transition-transform',
                historyOpen ? 'rotate-0' : '-rotate-90',
              )}
            />
          </button>

          {historyOpen && (
            <div className="border-t">
              {reviewsLoading ? (
                <div className="p-3">
                  <Skeleton className="h-12 w-full" />
                </div>
              ) : (
                reviews
                  .slice()
                  .sort((a, b) => b.attempt_number - a.attempt_number)
                  .map((review, idx) => {
                    const isExpanded = expandedHistoryAttempts.has(review.attempt_number)
                    const isLast = idx === reviews.length - 1
                    return (
                      <div
                        key={review.id}
                        className={cn('border-b last:border-b-0', isLast && 'rounded-b-lg')}
                      >
                        <button
                          className="flex w-full cursor-pointer flex-wrap items-center gap-2 px-4 py-2.5 text-left hover:bg-accent/40 transition-colors"
                          type="button"
                          onClick={() => onToggleHistoryAttempt(review.attempt_number)}
                        >
                          <span className="text-xs font-medium text-muted-foreground w-16 shrink-0">
                            Attempt {review.attempt_number}
                          </span>
                          {reviewStatusBadge(review.status)}
                          <span className="text-xs text-muted-foreground">
                            {formatDate(review.started_at)}
                          </span>
                          <span className="text-micro text-muted-foreground/40">·</span>
                          <span className="text-xs text-muted-foreground">
                            {formatDuration(review.started_at, review.finished_at)}
                          </span>
                          <CaretDown
                            className={cn(
                              'ml-auto h-3 w-3 shrink-0 text-muted-foreground/40 transition-transform',
                              isExpanded ? 'rotate-0' : '-rotate-90',
                            )}
                          />
                        </button>
                        {isExpanded && (
                          <div className="border-t bg-muted/20 px-4 py-3 space-y-2">
                            <ReviewConformancePanel conformance={review.details.conformance} />
                            {review.step_results.length > 0 ? (
                              review.step_results.map((step) => (
                                <ChatEntryContainer
                                  key={`${review.id}-history-${step.index}`}
                                  variant="tool"
                                  status={step.exit_code === 0 ? 'success' : 'failed'}
                                  header={<StepHeader step={step} />}
                                  defaultCollapsed={step.exit_code === 0}
                                >
                                  {stepOutput(step) ? (
                                    <pre className="whitespace-pre-wrap break-all rounded-md bg-muted p-2 text-xs font-mono">
                                      {stepOutput(step)}
                                    </pre>
                                  ) : (
                                    <p className="text-xs text-muted-foreground">
                                      No output captured.
                                    </p>
                                  )}
                                </ChatEntryContainer>
                              ))
                            ) : (
                              <p className="text-sm text-muted-foreground">No step results.</p>
                            )}
                          </div>
                        )}
                      </div>
                    )
                  })
              )}
            </div>
          )}
        </div>
      )}
    </div>
  )
}

function StepHeader({ step }: { step: Review['step_results'][number] }) {
  return (
    <span className="flex items-center gap-2">
      <span className="font-mono text-xs">{step.command}</span>
      <Badge
        className={cn(
          'border-transparent shrink-0',
          step.exit_code === 0
            ? 'bg-emerald-100 text-emerald-900 dark:bg-emerald-900/30 dark:text-emerald-300'
            : 'bg-red-100 text-red-900 dark:bg-red-900/30 dark:text-red-300',
        )}
      >
        exit {step.exit_code}
      </Badge>
    </span>
  )
}

function StepResultsSection({
  latestReview,
  reviewsLoading,
}: {
  latestReview: Review | undefined
  reviewsLoading: boolean
}) {
  const steps = latestReview?.step_results ?? []
  const allPassed = steps.length > 0 && steps.every((s) => s.exit_code === 0)
  const stepCount = steps.length

  return (
    <div className="rounded-lg border">
      <div className="flex items-center justify-between px-4 py-3">
        <p className="text-sm font-medium">Step results</p>
        {latestReview && stepCount > 0 ? (
          <Badge
            className={cn(
              'border-transparent',
              allPassed
                ? 'bg-emerald-100 text-emerald-900 dark:bg-emerald-900/30 dark:text-emerald-300'
                : 'bg-red-100 text-red-900 dark:bg-red-900/30 dark:text-red-300',
            )}
          >
            {stepCount} {stepCount === 1 ? 'step' : 'steps'} {allPassed ? 'passed' : 'finished'}
          </Badge>
        ) : latestReview?.status === 'passed' && stepCount === 0 ? (
          <Badge className="border-transparent bg-emerald-100 text-emerald-900 dark:bg-emerald-900/30 dark:text-emerald-300">
            No CI steps
          </Badge>
        ) : null}
      </div>

      <div className="border-t px-4 py-3">
        {latestReview ? (
          stepCount > 0 ? (
            <div className="space-y-2">
              {steps.map((step) => (
                <ChatEntryContainer
                  key={step.index}
                  variant="tool"
                  status={step.exit_code === 0 ? 'success' : 'failed'}
                  header={<StepHeader step={step} />}
                  defaultCollapsed={step.exit_code === 0}
                >
                  {stepOutput(step) ? (
                    <pre className="whitespace-pre-wrap break-all rounded-md bg-muted p-2 text-xs font-mono">
                      {stepOutput(step)}
                    </pre>
                  ) : (
                    <p className="text-xs text-muted-foreground">
                      No output captured for this step.
                    </p>
                  )}
                </ChatEntryContainer>
              ))}
            </div>
          ) : latestReview.status === 'passed' ? (
            <p className="text-sm text-muted-foreground">
              No CI step results recorded. See the review assessment above.
            </p>
          ) : (
            <p className="text-sm text-muted-foreground">No step results available.</p>
          )
        ) : reviewsLoading ? (
          <Skeleton className="h-20 w-full" />
        ) : (
          <p className="text-sm text-muted-foreground">No review data available.</p>
        )}
      </div>
    </div>
  )
}
