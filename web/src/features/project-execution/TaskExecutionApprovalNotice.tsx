import { Link } from '@tanstack/react-router'
import { ArrowUpRight, LockKeyOpen } from '@phosphor-icons/react'

import type { ExecutionBlockerProjection, ExecutionEvidenceSummary } from '@/types/generated'

import { executionBlockerNavTarget } from './executionBlockerNav'

type TaskExecutionApprovalNoticeProps = {
  projectId: string
  /**
   * The Task's own canonical blocker (`TaskResponse.execution_blocker`,
   * D16/D17) — or `null` when nothing currently blocks this Task's
   * execution. This is already scoped correctly on the server: a
   * reconciliation attached to only this Task appears here even while the
   * Project's gate stays `active`, and a terminal or read-only-planning Task
   * never receives one at all. This component renders it verbatim; it never
   * re-derives its own copy from `execution_gate` or Task status/type.
   */
  blocker: ExecutionBlockerProjection | null | undefined
  /**
   * The Task's own canonical attempt/execution/commit evidence
   * (`TaskResponse.execution_evidence`). Drives the eyebrow label so a Task
   * with an attempt or a commit is never shown as "not started" (F12), even
   * while it is blocked.
   */
  evidence?: ExecutionEvidenceSummary | null
}

/**
 * Gives a blocked Task one plain-language recovery action, sourced entirely
 * from the one server-owned `ExecutionBlockerProjection` for that Task.
 *
 * Before this consumed the projection, this notice re-derived its own copy
 * from the Project-wide execution gate and always labeled the Task "Task not
 * started" — contradicting a Task's own executions/commit and unable to
 * represent a reconciliation scoped to just this Task (F12). Surfaces may
 * adapt layout, but the headline, explanation, progress label, and the one
 * permitted next action all come from here unmodified (D17).
 */
export function TaskExecutionApprovalNotice({
  projectId,
  blocker,
  evidence,
}: TaskExecutionApprovalNoticeProps) {
  if (!blocker) return null

  const target = executionBlockerNavTarget(blocker.next_action)
  const progressLabel = evidence?.progress_label ?? 'Blocked'
  const linkClassName =
    target?.variant === 'link'
      ? 'inline-flex w-full shrink-0 items-center justify-center gap-1.5 text-xs font-semibold text-primary underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring sm:w-auto'
      : 'inline-flex w-full shrink-0 items-center justify-center gap-1.5 rounded-md bg-primary px-3 py-2 text-xs font-semibold text-primary-foreground shadow-xs transition-[color,background-color,transform] hover:brightness-95 active:translate-y-px focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 sm:w-auto'

  return (
    <section
      className="min-w-0 rounded-lg border border-ember-border bg-ember-surface p-3"
      aria-labelledby="task-execution-approval-heading"
      role="status"
    >
      <div className="flex min-w-0 flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
        <div className="flex min-w-0 items-start gap-2.5">
          <LockKeyOpen size={18} className="mt-0.5 shrink-0 text-primary" aria-hidden />
          <div className="min-w-0">
            <p className="font-mono text-micro font-semibold uppercase tracking-[0.1em] text-primary">
              {progressLabel}
            </p>
            <h2
              id="task-execution-approval-heading"
              className="mt-1 text-sm font-semibold text-foreground"
            >
              {blocker.headline}
            </h2>
            <p className="mt-1 break-words text-xs leading-5 text-muted-foreground">
              {blocker.safe_explanation}
            </p>
          </div>
        </div>
        {target ? (
          <Link to={target.to} params={{ projectId }} hash={target.hash} className={linkClassName}>
            {target.label} <ArrowUpRight size={14} aria-hidden />
          </Link>
        ) : null}
      </div>
    </section>
  )
}
