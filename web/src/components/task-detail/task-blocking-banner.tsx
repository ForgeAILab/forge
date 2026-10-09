import type { InterruptionMetadata, Task } from '@/types/generated'
import {
  getBlockingAnnotation,
  getStaleBlockingAnnotation,
  checkWaitNotice,
  getTaskWaitNotice,
  getTaskWorkflowWarning,
} from '@/lib/workflow-utils'
import { productTerm } from '@/lib/i18n'

// Informational fallback for interruption states that carry no recovery
// actions. Actionable failures are rendered by WorkflowExceptionPanel, which
// is fed by the server-derived task.workflow_exception summary; this banner
// only appears when that summary is absent.

function humanizeBlockingReason(reason: string) {
  if (reason === 'review_needs_owner') return 'Needs owner'
  const withSpaces = reason.replace(/_/g, ' ')
  return withSpaces.charAt(0).toUpperCase() + withSpaces.slice(1)
}

function InterruptionBanner({
  title,
  metadata,
  tone,
}: {
  title: string
  metadata: InterruptionMetadata
  tone: 'blocked' | 'failed'
}) {
  const classes =
    tone === 'failed'
      ? 'border-red-300 bg-red-50 text-red-900 dark:border-red-800 dark:bg-red-950 dark:text-red-200'
      : 'border-amber-300 bg-amber-50 text-amber-900 dark:border-amber-800 dark:bg-amber-950 dark:text-amber-200'

  return (
    <section className={`rounded-lg border p-4 ${classes}`}>
      <div className="space-y-1.5">
        <p className="text-sm font-semibold">{title}</p>
        <p className="text-sm">{metadata.reason}</p>
        {metadata.kind || metadata.source || metadata.execution_id ? (
          <div className="flex flex-wrap gap-x-3 gap-y-1 text-xs opacity-80">
            {metadata.kind ? <span>{humanizeBlockingReason(metadata.kind)}</span> : null}
            {metadata.source ? <span>{metadata.source}</span> : null}
            {metadata.execution_id ? (
              <span className="font-mono">{metadata.execution_id}</span>
            ) : null}
          </div>
        ) : null}
      </div>
    </section>
  )
}

export function TaskBlockingBanner({ task }: { task: Task }) {
  const staleAnnotation = getStaleBlockingAnnotation(task)
  const workflowWarning = getTaskWorkflowWarning(task)
  if (task.status === 'cancelled') return null

  if (task.condition.details.failed && task.condition.details.interruption) {
    return <InterruptionBanner title="Task Failed" metadata={task.condition.details.interruption} tone="failed" />
  }

  if (task.condition.details.blocked && task.condition.details.interruption && !getBlockingAnnotation(task)) {
    return <InterruptionBanner title="Task Blocked" metadata={task.condition.details.interruption} tone="blocked" />
  }

  if (staleAnnotation) {
    return (
      <section className="rounded-lg border border-amber-300 bg-amber-50 p-4 text-amber-900 dark:border-amber-800 dark:bg-amber-950 dark:text-amber-200">
        <div className="space-y-1.5">
          <p className="text-sm font-semibold">Previous {productTerm('run')} Warning</p>
          <p className="text-sm">
            Superseded by a later {productTerm('run').toLowerCase()} or manual{' '}
            {productTerm('phase').toLowerCase()} change.
          </p>
          {staleAnnotation.message ? <p className="text-sm">{staleAnnotation.message}</p> : null}
          {staleAnnotation.blocked_execution_id ? (
            <p className="break-all font-mono text-xs opacity-80">
              {staleAnnotation.blocked_execution_id}
            </p>
          ) : null}
        </div>
      </section>
    )
  }

  const checkNotice = checkWaitNotice(task)
  if (checkNotice) {
    return (
      <section
        className={
          checkNotice.needsOwner
            ? 'rounded-lg border border-amber-300 bg-amber-50 p-4 text-amber-900 dark:border-amber-800 dark:bg-amber-950 dark:text-amber-200'
            : 'rounded-lg border border-border bg-muted/40 p-4 text-foreground'
        }
      >
        <div className="space-y-1.5">
          <p className="text-sm font-semibold">{checkNotice.title}</p>
          <p className="text-sm">{checkNotice.message}</p>
        </div>
      </section>
    )
  }

  const waitNotice = getTaskWaitNotice(task)
  if (waitNotice) {
    return (
      <section className="rounded-lg border border-border bg-muted/40 p-4 text-foreground">
        <div className="space-y-1.5">
          <p className="text-sm font-semibold">{waitNotice.title}</p>
          <p className="text-sm">{waitNotice.message}</p>
        </div>
      </section>
    )
  }

  if (workflowWarning) {
    return (
      <section className="rounded-lg border border-amber-300 bg-amber-50 p-4 text-amber-900 dark:border-amber-800 dark:bg-amber-950 dark:text-amber-200">
        <div className="space-y-1.5">
          <p className="text-sm font-semibold">{workflowWarning.title}</p>
          <p className="text-sm">{workflowWarning.message}</p>
        </div>
      </section>
    )
  }

  return null
}
