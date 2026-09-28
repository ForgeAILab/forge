import { useState } from 'react'
import { Link } from '@tanstack/react-router'
import { CaretDown } from '@phosphor-icons/react'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Textarea } from '@/components/ui/textarea'
import { Tooltip } from '@/components/ui/tooltip'
import { workflowLabelFromKind } from '@/components/workflow-health-badge'
import { cn } from '@/lib/cn'
import { productTerm } from '@/lib/i18n'
import type {
  RecoveryAction,
  Task,
  WorkflowExceptionAction,
  WorkflowExceptionSummary,
} from '@/types/generated'

const EXCEPTION_RETRY_FAMILY: RecoveryAction[] = [
  'resume_process',
  'retry_hook',
  'update_workspace_and_retry_hook',
  'reexecute',
  'resume_session',
  'reset_retry_window',
  'skip_hook_once',
  'proceed_once',
]

const EXCEPTION_RETRY_PRIMARY_ORDER: RecoveryAction[] = [
  'resume_process',
  'retry_hook',
  'reexecute',
  'resume_session',
  'update_workspace_and_retry_hook',
  'reset_retry_window',
  'skip_hook_once',
  'proceed_once',
]

function actionKey(action: WorkflowExceptionAction, index: number) {
  return `${action.kind}:${action.target_execution_id ?? ''}:${index}`
}

function isFailureTone(exception: WorkflowExceptionSummary): boolean {
  return exception.type === 'task_failed'
}

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

export function WorkflowExceptionPanel({
  task,
  actions,
  recoverPending,
  terminal,
  cancelPending,
  onRecover,
  onOpenInteractive,
  onCancelTask,
}: {
  task: Task
  actions: WorkflowExceptionAction[]
  recoverPending: boolean
  terminal: boolean
  cancelPending: boolean
  onRecover: (action: RecoveryAction, input?: { reason?: string; context?: string }) => void
  onOpenInteractive: (action: WorkflowExceptionAction) => void
  onCancelTask: () => void
}) {
  const [confirmingAction, setConfirmingAction] = useState<WorkflowExceptionAction | null>(null)
  const [reasonDraft, setReasonDraft] = useState('')
  const [guidanceDraft, setGuidanceDraft] = useState('')

  const exception = task.workflow_exception
  if (!exception) return null

  const failure = isFailureTone(exception)
  const title = workflowLabelFromKind(exception.type)
  const step = exception.failing_step
  const details = [
    exception.state ? `state ${exception.state}` : null,
    exception.role ? `role ${exception.role}` : null,
    exception.target_state ? `target ${exception.target_state}` : null,
    exception.target_role ? `target role ${exception.target_role}` : null,
  ].filter(Boolean)

  const retryActions = actions.filter((a) => EXCEPTION_RETRY_FAMILY.includes(a.kind))
  const primaryRetry =
    EXCEPTION_RETRY_PRIMARY_ORDER.map((k) =>
      retryActions.find((a) => a.kind === k && a.enabled),
    ).find(Boolean) ??
    EXCEPTION_RETRY_PRIMARY_ORDER.map((k) => retryActions.find((a) => a.kind === k)).find(
      Boolean,
    ) ??
    retryActions[0] ??
    null
  const secondaryRetries = retryActions.filter((a) => a !== primaryRetry)
  const openInteractive = actions.find((a) => a.kind === 'open_interactive') ?? null
  // cancel_task gets the dedicated destructive button below; keep it out of
  // the generic action row so the panel never shows two cancel buttons.
  const standaloneActions = actions.filter(
    (a) =>
      !EXCEPTION_RETRY_FAMILY.includes(a.kind) &&
      a.kind !== 'open_interactive' &&
      a.kind !== 'cancel_task',
  )

  const requestAction = (action: WorkflowExceptionAction) => {
    if (!action.enabled || recoverPending) return
    if (action.kind === 'open_interactive') {
      onOpenInteractive(action)
      return
    }
    if (action.requires_reason || action.requires_guidance) {
      setConfirmingAction(action)
      setReasonDraft('')
      setGuidanceDraft('')
      return
    }
    onRecover(action.kind)
  }

  const closeDialog = () => {
    setConfirmingAction(null)
    setReasonDraft('')
    setGuidanceDraft('')
  }

  const submitDialog = () => {
    if (!confirmingAction) return
    const reason = reasonDraft.trim()
    const guidance = guidanceDraft.trim()
    if (confirmingAction.requires_reason && !reason) return
    if (confirmingAction.requires_guidance && !guidance) return
    onRecover(confirmingAction.kind, {
      reason: reason || undefined,
      context: guidance || undefined,
    })
    closeDialog()
  }

  return (
    <section
      className={cn(
        'rounded-lg border p-4',
        failure
          ? 'border-red-300 bg-red-50 text-red-950 dark:border-red-800 dark:bg-red-950 dark:text-red-100'
          : 'border-amber-300 bg-amber-50 text-amber-950 dark:border-amber-800 dark:bg-amber-950 dark:text-amber-100',
      )}
    >
      <div className="space-y-3">
        <div className="space-y-1">
          <div className="flex flex-wrap items-center justify-between gap-2">
            <p className="text-sm font-semibold">{title}</p>
            <div className="flex flex-wrap gap-2 text-xs">
              {exception.review_id ? (
                <Link
                  to="/tasks/$taskId/$tab"
                  params={{ taskId: task.id, tab: 'review' }}
                  className="font-mono text-primary hover:underline"
                >
                  Review {exception.review_id.slice(0, 8)}
                </Link>
              ) : null}
              {exception.execution_id ? (
                <Link
                  to="/tasks/$taskId/executions/$executionId"
                  params={{ taskId: task.id, executionId: exception.execution_id }}
                  className="font-mono text-primary hover:underline"
                >
                  {productTerm('run')} {exception.execution_id.slice(0, 8)}
                </Link>
              ) : null}
            </div>
          </div>
          <p className="text-sm">{exception.message}</p>
          {details.length > 0 ? (
            <div className="flex flex-wrap gap-x-3 gap-y-1 text-xs opacity-80">
              {details.map((item) => (
                <span key={item}>{item}</span>
              ))}
            </div>
          ) : null}
        </div>

        {step ? <FailingStepDetails exception={exception} failure={failure} /> : null}

        {exception.related_evidence.length > 0 ? (
          <div className="space-y-1 text-xs">
            <p className="font-medium uppercase tracking-wide opacity-75">Related evidence</p>
            <div className="space-y-1">
              {exception.related_evidence.map((item, index) => (
                <p key={`${item.kind}:${item.id ?? index}`} className="break-words opacity-85">
                  <span className="font-medium">{workflowLabelFromKind(item.kind)}</span>
                  {item.id ? <span className="font-mono"> {item.id.slice(0, 8)}</span> : null}
                  {item.message ? <span> - {item.message}</span> : null}
                </p>
              ))}
            </div>
          </div>
        ) : null}

        {(actions.length > 0 || !terminal) ? (
          <div className="flex flex-wrap items-center gap-2 pt-1">
            {primaryRetry ? (
              <div className="flex items-center">
                <Button
                  size="sm"
                  variant="outline"
                  disabled={!primaryRetry.enabled || recoverPending}
                  title={
                    primaryRetry.enabled
                      ? primaryRetry.propagates
                        ? 'This action may automatically advance the task'
                        : undefined
                      : (primaryRetry.disabled_reason ?? undefined)
                  }
                  className={secondaryRetries.length > 0 ? 'rounded-r-none border-r-0' : ''}
                  onClick={() => requestAction(primaryRetry)}
                >
                  {primaryRetry.label}
                </Button>
                {secondaryRetries.length > 0 ? (
                  <DropdownMenu>
                    <DropdownMenuTrigger
                      disabled={recoverPending}
                      className="inline-flex h-7 w-6 cursor-pointer items-center justify-center rounded-l-none rounded-r-md border border-input bg-card text-secondary-foreground transition-colors hover:bg-muted hover:text-foreground disabled:pointer-events-none disabled:opacity-50"
                    >
                      <CaretDown className="h-3 w-3" weight="bold" />
                    </DropdownMenuTrigger>
                    <DropdownMenuContent align="end">
                      {secondaryRetries.map((action, index) => (
                        <DropdownMenuItem
                          key={actionKey(action, index)}
                          disabled={!action.enabled || recoverPending}
                          onClick={() => requestAction(action)}
                        >
                          {action.label}
                        </DropdownMenuItem>
                      ))}
                    </DropdownMenuContent>
                  </DropdownMenu>
                ) : null}
              </div>
            ) : null}
            {openInteractive ? (
              <Button
                size="sm"
                variant="outline"
                disabled={!openInteractive.enabled || recoverPending}
                onClick={() => requestAction(openInteractive)}
              >
                {openInteractive.label}
              </Button>
            ) : null}
            {standaloneActions.map((action, index) => {
              const btn = (
                <Button
                  key={actionKey(action, index)}
                  size="sm"
                  variant="outline"
                  disabled={!action.enabled || recoverPending}
                  onClick={() => requestAction(action)}
                >
                  {action.label}
                </Button>
              )
              return action.enabled ? (
                btn
              ) : action.disabled_reason ? (
                <Tooltip key={actionKey(action, index)} content={action.disabled_reason}>
                  <span>{btn}</span>
                </Tooltip>
              ) : (
                btn
              )
            })}
            {!terminal ? (
              <Button
                size="sm"
                variant="outline"
                disabled={cancelPending}
                className="text-destructive hover:bg-destructive/10 hover:text-destructive"
                onClick={onCancelTask}
              >
                Cancel Task
              </Button>
            ) : null}
          </div>
        ) : null}
      </div>
      <Dialog
        open={confirmingAction != null}
        onOpenChange={(open) => {
          if (!open) closeDialog()
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>{confirmingAction?.label ?? 'Confirm Recovery'}</DialogTitle>
          </DialogHeader>
          <div className="space-y-4">
            {confirmingAction?.kind === 'mark_reviewed' ? (
              <p className="rounded-md border border-amber-300 bg-amber-100/70 p-3 text-sm text-amber-950 dark:border-amber-700 dark:bg-amber-950/50 dark:text-amber-100">
                This records a new, auditable manual pass. The failed review remains in history.
              </p>
            ) : null}
            {confirmingAction?.requires_reason ? (
              <div className="space-y-2">
                <Label htmlFor="workflow-recovery-reason">Reason</Label>
                <Input
                  id="workflow-recovery-reason"
                  value={reasonDraft}
                  onChange={(event) => setReasonDraft(event.target.value)}
                  placeholder="Why is this recovery action needed?"
                />
              </div>
            ) : null}
            {confirmingAction?.requires_guidance ? (
              <div className="space-y-2">
                <Label htmlFor="workflow-recovery-guidance">Guidance</Label>
                <Textarea
                  id="workflow-recovery-guidance"
                  value={guidanceDraft}
                  onChange={(event) => setGuidanceDraft(event.target.value)}
                  placeholder="Add instructions for the next workflow step"
                  rows={4}
                />
              </div>
            ) : null}
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={closeDialog}>
              Cancel
            </Button>
            <Button
              disabled={
                recoverPending ||
                (confirmingAction?.requires_reason && !reasonDraft.trim()) ||
                (confirmingAction?.requires_guidance && !guidanceDraft.trim())
              }
              onClick={submitDialog}
            >
              Confirm
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </section>
  )
}
