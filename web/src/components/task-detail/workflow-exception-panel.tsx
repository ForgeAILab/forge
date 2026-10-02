import { Link } from '@tanstack/react-router'
import { cn } from '@/lib/cn'
import { workflowLabelFromKind } from '@/components/workflow-health-badge'
import { TaskActionButtons } from './task-action-buttons'
import type { Task, WorkflowExceptionSummary } from '@/types/generated'
import type { ReviewAssessment } from '@/types/generated/bindings/ReviewAssessment'

function FailingStepDetails({
  exception,
  failure,
}: {
  exception: WorkflowExceptionSummary
  failure: boolean
}) {
  const step = exception.failing_step
  if (!step) return null

  return (
    <div
      className={cn(
        'space-y-2 rounded-md border bg-white/70 p-3 text-xs dark:bg-black/20',
        failure
          ? 'border-red-200 dark:border-red-800'
          : 'border-amber-200 dark:border-amber-800',
      )}
    >
      <div className="flex flex-wrap items-center gap-x-3 gap-y-1">
        <p className="font-medium">Failing step</p>
        <span>step {step.index}</span>
        {typeof step.exit_code === 'number' ? <span>exit {step.exit_code}</span> : null}
      </div>
      {step.command ? (
        <p
          className={cn(
            'break-words font-mono',
            failure
              ? 'text-red-950 dark:text-red-100'
              : 'text-amber-950 dark:text-amber-100',
          )}
        >
          {step.command}
        </p>
      ) : null}
      {step.stderr_tail ? (
        <pre className="max-h-36 overflow-auto whitespace-pre-wrap rounded bg-red-500/10 p-2 font-mono text-[11px] text-red-800 dark:text-red-200">
          {step.stderr_tail}
        </pre>
      ) : null}
      {step.output_tail ? (
        <pre
          className={cn(
            'max-h-36 overflow-auto whitespace-pre-wrap rounded p-2 font-mono text-[11px]',
            failure
              ? 'bg-red-100/80 text-red-950 dark:bg-red-950/50 dark:text-red-100'
              : 'bg-amber-100/80 text-amber-950 dark:bg-amber-950/50 dark:text-amber-100',
          )}
        >
          {step.output_tail}
        </pre>
      ) : null}
    </div>
  )
}

export function WorkflowExceptionPanel({ task, assessment }: { task: Task; assessment?: ReviewAssessment | null }) {
  const exception = task.workflow_exception
  if (!exception) return null
  const failure = task.failed != null
  return <section className="space-y-3 rounded-lg border border-warning/40 bg-warning/10 p-4">
    <h3 className="text-sm font-semibold">{workflowLabelFromKind(exception.type)}</h3>
    <p className="text-sm">{exception.message}</p>
    {exception.type === 'review_needs_owner' ? <div className="text-xs text-muted-foreground">{assessment?.fixable_by === 'owner' ? <span>fixable by owner</span> : assessment?.repeat ? <span>repeated finding</span> : null}</div> : null}
    {exception.execution_id ? <Link to="/tasks/$taskId/executions/$executionId" params={{ taskId: task.id, executionId: exception.execution_id }} className="text-xs text-primary hover:underline">View execution</Link> : null}
    <FailingStepDetails exception={exception} failure={failure} />
    {exception.related_evidence.map((evidence, index) => <p key={index} className="text-xs">{evidence.message}</p>)}
    <TaskActionButtons taskId={task.id} version={task.version} offers={exception.actions} />
  </section>
}
