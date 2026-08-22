import { ArrowClockwise, Play, Spinner, WarningCircle } from '@phosphor-icons/react'

import { Button } from '@/components/ui/button'
import type { Execution } from '@/types/generated'

type LivenessActions = {
  onRefresh?: () => void
  refreshPending?: boolean
  onContinue?: () => void
  continuePending?: boolean
  onRetry?: () => void
  retryPending?: boolean
}

type NoticeKind =
  | 'deadline'
  | 'lease_expired'
  | 'owner_unverified'
  | 'owner_recovery'
  | 'owner_disconnected'

type NoticeState = {
  kind: NoticeKind
  code: string
  title: string
  description: string
}

function normalize(value: string | null | undefined): string {
  return value?.trim().toLowerCase() ?? ''
}

function formatTimestamp(value: string | null | undefined): string | null {
  if (!value) return null
  const date = new Date(value)
  return Number.isNaN(date.getTime())
    ? value
    : date.toLocaleString([], { dateStyle: 'medium', timeStyle: 'short' })
}

function interruptionText(execution: Execution): string | null {
  const reason = execution.interruption?.reason?.trim()
  if (reason) return reason
  const stopReason = execution.stop_reason?.trim()
  return stopReason ? stopReason.replaceAll('_', ' ') : null
}

export function getExecutionLivenessNotice(execution: Execution): NoticeState | null {
  const warning = normalize(execution.liveness_warning)
  const interruptionKind = normalize(execution.interruption?.kind)
  const stopReason = normalize(execution.stop_reason)

  if (
    warning === 'hard_deadline_reached' ||
    interruptionKind === 'hard_deadline_reached' ||
    interruptionKind === 'hard_deadline' ||
    stopReason === 'agent_timeout'
  ) {
    return {
      kind: 'deadline',
      code: 'hard_deadline_reached',
      title: 'Hard deadline reached',
      description:
        execution.status === 'running'
          ? 'The immutable execution deadline has been reached. A heartbeat cannot extend it; refresh to confirm the server-owned state.'
          : 'The immutable execution deadline ended this run. Its terminal result remains authoritative; a late heartbeat cannot reopen it.',
    }
  }

  if (
    warning === 'owner_lease_expired' ||
    interruptionKind === 'owner_lease_expired' ||
    interruptionKind === 'execution_stalled' ||
    stopReason === 'execution_stalled'
  ) {
    return {
      kind: 'lease_expired',
      code: 'owner_lease_expired',
      title: 'Owner lease expired',
      description:
        execution.status === 'running'
          ? 'The execution owner stopped renewing its lease. This is an owner-liveness condition, not semantic progress failure; refresh the run before recovering it.'
          : 'The execution owner stopped renewing its lease and the run was terminalized. This is distinct from semantic progress; recover only through the bounded action below.',
    }
  }

  if (warning === 'owner_lease_unverified' || execution.owner_health === 'unknown') {
    return {
      kind: 'owner_unverified',
      code: 'owner_lease_unverified',
      title: 'Owner liveness unverified',
      description:
        'Forge cannot currently verify the execution owner lease. Semantic progress and owner health remain separate; refresh the server projection before acting.',
    }
  }

  if (
    warning === 'owner_recovery' ||
    interruptionKind === 'owner_recovery' ||
    stopReason === 'crash_recovery'
  ) {
    return {
      kind: 'owner_recovery',
      code: 'owner_recovery',
      title: 'Owner recovery recorded',
      description:
        'The server recorded owner recovery for this run. The terminal or retry state below remains authoritative until a bounded recovery action is accepted.',
    }
  }

  if (
    warning === 'remote_owner_disconnected' ||
    interruptionKind === 'remote_owner_disconnected' ||
    stopReason === 'daemon_disconnected'
  ) {
    return {
      kind: 'owner_disconnected',
      code: 'remote_owner_disconnected',
      title: 'Remote owner disconnected',
      description:
        'The server recorded a disconnected remote owner. Refresh the run and use only the bounded recovery action offered for this terminal state.',
    }
  }

  return null
}

function actionForNotice(state: NoticeState, actions: LivenessActions, isRunning: boolean) {
  if (isRunning && actions.onRefresh) {
    return actions.onRefresh
      ? {
          label: 'Refresh run',
          pending: actions.refreshPending,
          onClick: actions.onRefresh,
          icon: <ArrowClockwise size={14} aria-hidden />,
        }
      : null
  }

  if (
    state.kind === 'deadline' ||
    state.kind === 'lease_expired' ||
    state.kind === 'owner_recovery' ||
    state.kind === 'owner_disconnected'
  ) {
    if (actions.onContinue) {
      return {
        label: 'Continue session',
        pending: actions.continuePending,
        onClick: actions.onContinue,
        icon: <Play size={14} aria-hidden />,
      }
    }
    if (actions.onRetry) {
      return {
        label: 'Retry run',
        pending: actions.retryPending,
        onClick: actions.onRetry,
        icon: <ArrowClockwise size={14} aria-hidden />,
      }
    }
  }

  return null
}

export function ExecutionLivenessNotice({
  execution,
  actions = {},
  nextActionLabel,
}: {
  execution: Execution
  actions?: LivenessActions
  nextActionLabel?: string
}) {
  const state = getExecutionLivenessNotice(execution)
  if (!state) return null

  const terminal = execution.status !== 'running'
  const action = actionForNotice(state, actions, !terminal)
  const interruption = execution.interruption
  const interruptionReason = interruptionText(execution)
  const ownerHealth = execution.owner_health
  const lastHeartbeat = formatTimestamp(execution.last_heartbeat_at)
  const lastProgress = formatTimestamp(execution.last_progress_at)
  const leaseExpiry = formatTimestamp(execution.lease_expires_at)
  const hardDeadline = formatTimestamp(execution.hard_deadline_at)

  return (
    <section
      className="rounded-lg border border-warning/40 bg-warning/10 p-3 text-warning-foreground"
      role={terminal ? 'alert' : 'status'}
      aria-live={terminal ? 'assertive' : 'polite'}
    >
      <div className="flex min-w-0 items-start gap-2">
        <WarningCircle className="mt-0.5 h-4 w-4 shrink-0 text-warning" aria-hidden />
        <div className="min-w-0 flex-1">
          <div className="flex min-w-0 flex-wrap items-center justify-between gap-2">
            <h4 className="text-sm font-semibold text-foreground">{state.title}</h4>
            <span className="rounded-full border border-warning/40 px-2 py-0.5 font-mono text-micro uppercase tracking-[0.08em] text-warning-foreground">
              {state.code}
            </span>
          </div>
          <p className="mt-1 break-words text-xs leading-5 text-muted-foreground">
            {state.description}
          </p>

          {interruptionReason || interruption?.kind || interruption?.created_at ? (
            <dl className="mt-3 grid min-w-0 gap-x-4 gap-y-2 border-t border-warning/20 pt-3 text-micro xl:grid-cols-2">
              {interruptionReason ? (
                <div className="min-w-0">
                  <dt className="font-mono uppercase tracking-[0.08em] text-muted-foreground">
                    Interruption
                  </dt>
                  <dd className="mt-1 break-words text-foreground">{interruptionReason}</dd>
                </div>
              ) : null}
              {interruption?.kind ? (
                <div className="min-w-0">
                  <dt className="font-mono uppercase tracking-[0.08em] text-muted-foreground">
                    Kind
                  </dt>
                  <dd className="mt-1 break-all font-mono text-foreground">{interruption.kind}</dd>
                </div>
              ) : null}
              {interruption?.created_at ? (
                <div className="min-w-0">
                  <dt className="font-mono uppercase tracking-[0.08em] text-muted-foreground">
                    Recorded
                  </dt>
                  <dd className="mt-1 break-words text-foreground">
                    {formatTimestamp(interruption.created_at)}
                  </dd>
                </div>
              ) : null}
            </dl>
          ) : null}

          {ownerHealth || lastHeartbeat || lastProgress || leaseExpiry || hardDeadline ? (
            <dl className="mt-3 grid min-w-0 gap-x-4 gap-y-2 border-t border-warning/20 pt-3 text-micro xl:grid-cols-2">
              {ownerHealth ? (
                <div className="min-w-0">
                  <dt className="font-mono uppercase tracking-[0.08em] text-muted-foreground">
                    Owner health
                  </dt>
                  <dd className="mt-1 break-words text-foreground">{ownerHealth}</dd>
                </div>
              ) : null}
              {lastHeartbeat ? (
                <div className="min-w-0">
                  <dt className="font-mono uppercase tracking-[0.08em] text-muted-foreground">
                    Last heartbeat
                  </dt>
                  <dd className="mt-1 break-words text-foreground">{lastHeartbeat}</dd>
                </div>
              ) : null}
              {lastProgress ? (
                <div className="min-w-0">
                  <dt className="font-mono uppercase tracking-[0.08em] text-muted-foreground">
                    Last semantic progress
                  </dt>
                  <dd className="mt-1 break-words text-foreground">{lastProgress}</dd>
                </div>
              ) : null}
              {leaseExpiry ? (
                <div className="min-w-0">
                  <dt className="font-mono uppercase tracking-[0.08em] text-muted-foreground">
                    Lease expiry
                  </dt>
                  <dd className="mt-1 break-words text-foreground">{leaseExpiry}</dd>
                </div>
              ) : null}
              {hardDeadline ? (
                <div className="min-w-0">
                  <dt className="font-mono uppercase tracking-[0.08em] text-muted-foreground">
                    Hard deadline
                  </dt>
                  <dd className="mt-1 break-words text-foreground">{hardDeadline}</dd>
                </div>
              ) : null}
            </dl>
          ) : null}

          {action ? (
            <Button
              type="button"
              size="sm"
              variant="outline"
              className="mt-3 w-full sm:w-auto"
              disabled={action.pending}
              onClick={action.onClick}
            >
              {action.pending ? (
                <Spinner className="h-3.5 w-3.5 animate-spin motion-reduce:animate-none" />
              ) : (
                action.icon
              )}
              {action.pending ? `${action.label}…` : action.label}
            </Button>
          ) : nextActionLabel ? (
            <p className="mt-3 text-xs font-medium text-foreground">
              Next action: {nextActionLabel}.
            </p>
          ) : null}
        </div>
      </div>
      <span className="sr-only">
        {action
          ? `Next action: ${action.label}.`
          : nextActionLabel
            ? `Next action: ${nextActionLabel}.`
            : 'No recovery action is currently available.'}
      </span>
    </section>
  )
}
