import { ArrowRight, CheckCircle, XCircle, SkipForward, Warning } from '@phosphor-icons/react'
import { useTransitionLogQuery } from '@/api/hooks'
import { cn } from '@/lib/cn'
import { formatStateName, getStateColors } from '@/lib/workflow-utils'
import type { HookResultEntry, TransitionLogEntry } from '@/types/generated'

function formatTriggeredBy(triggeredBy: string): string {
  if (triggeredBy === 'system') return 'System'
  if (triggeredBy.startsWith('agent:')) return `Agent ${triggeredBy.slice(6).slice(0, 8)}`
  if (triggeredBy.startsWith('user:')) return triggeredBy.slice(5)
  return triggeredBy
}

function formatTime(iso: string): string {
  const d = new Date(iso)
  return d.toLocaleString(undefined, {
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
  })
}

function OutcomeIcon({ outcome }: { outcome: string }) {
  if (outcome === 'ok') return <CheckCircle size={12} className="text-emerald-500" weight="fill" />
  if (outcome === 'skipped')
    return <SkipForward size={12} className="text-zinc-400" weight="fill" />
  if (outcome === 'failed') return <XCircle size={12} className="text-red-500" weight="fill" />
  return <Warning size={12} className="text-amber-400" weight="fill" />
}

export function TransitionTimeline({ taskId }: { taskId: string }) {
  const query = useTransitionLogQuery(taskId)

  if (query.isLoading) {
    return (
      <section aria-label="Transition history" className="space-y-3 p-4">
        <h3 className="text-sm font-medium">Transition history</h3>
        {[1, 2, 3].map((i) => (
          <div key={i} className="flex gap-3">
            <div className="mt-1 h-4 w-4 shrink-0 rounded-full bg-muted animate-pulse" />
            <div className="flex-1 space-y-1.5">
              <div className="h-3 w-2/3 rounded bg-muted animate-pulse" />
              <div className="h-3 w-1/3 rounded bg-muted animate-pulse" />
            </div>
          </div>
        ))}
      </section>
    )
  }

  if (query.isError) {
    return (
      <section aria-label="Transition history" className="p-4">
        <h3 className="text-sm font-medium">Transition history</h3>
        <p className="mt-3 text-sm text-destructive">Failed to load transition history.</p>
      </section>
    )
  }

  const entries = query.data ?? []

  if (entries.length === 0) {
    return (
      <section aria-label="Transition history" className="p-4">
        <h3 className="text-sm font-medium">Transition history</h3>
        <p className="mt-3 text-sm text-muted-foreground">No transitions recorded yet.</p>
      </section>
    )
  }

  return (
    <section aria-label="Transition history" className="space-y-0 py-2">
      <h3 className="px-4 pb-2 text-sm font-medium">Transition history</h3>
      {entries.map((entry: TransitionLogEntry, idx: number) => {
        const fromColors = getStateColors(entry.from_state)
        const toColors = getStateColors(entry.to_state)
        const isLast = idx === entries.length - 1

        return (
          <div key={entry.id} className="flex gap-3 px-4">
            {/* Spine */}
            <div className="flex flex-col items-center">
              <div
                className={cn(
                  'mt-2.5 h-2.5 w-2.5 shrink-0 rounded-full ring-2 ring-background',
                  entry.rejection ? 'bg-red-500' : toColors.dot,
                )}
              />
              {!isLast && <div className="mt-0.5 w-px flex-1 bg-border" />}
            </div>

            {/* Content */}
            <div className={cn('min-w-0 flex-1 pb-4', isLast && 'pb-2')}>
              {/* Header row */}
              <div className="flex flex-wrap items-center gap-1.5">
                <span
                  className={cn(
                    'rounded px-1.5 py-0.5 text-[11px] font-medium',
                    fromColors.bg,
                    fromColors.text,
                  )}
                >
                  {formatStateName(entry.from_state)}
                </span>
                <ArrowRight size={11} className="shrink-0 text-muted-foreground" />
                <span
                  className={cn(
                    'rounded px-1.5 py-0.5 text-[11px] font-medium',
                    toColors.bg,
                    toColors.text,
                  )}
                >
                  {formatStateName(entry.to_state)}
                </span>
                {entry.rejection && (
                  <span className="rounded bg-red-100 px-1.5 py-0.5 text-micro font-medium text-red-700 dark:bg-red-950 dark:text-red-300">
                    rejection
                  </span>
                )}
                {entry.bridge_kind === 'ci_only_review_passed' && (
                  <span className="rounded bg-emerald-100 px-1.5 py-0.5 text-micro font-medium text-emerald-700 dark:bg-emerald-950 dark:text-emerald-300">
                    CI-only re-review
                  </span>
                )}
              </div>

              {/* Meta */}
              <div className="mt-0.5 flex flex-wrap items-center gap-x-2 gap-y-0.5 text-[11px] text-muted-foreground">
                <span>{formatTime(entry.created_at)}</span>
                <span aria-hidden="true">·</span>
                <span>{formatTriggeredBy(entry.triggered_by)}</span>
                {entry.trigger_reason && entry.trigger_reason !== 'user action' && (
                  <>
                    <span aria-hidden="true">·</span>
                    <span className="italic">{entry.trigger_reason}</span>
                  </>
                )}
              </div>

              {/* Hook results */}
              {entry.hook_results_json.length > 0 && (
                <div className="mt-1.5 space-y-0.5">
                  {entry.hook_results_json.map((hook: HookResultEntry, hi: number) => (
                    <div key={hi} className="flex items-center gap-1.5 text-[11px]">
                      <OutcomeIcon outcome={hook.outcome} />
                      <span className="font-mono text-muted-foreground">{hook.action}</span>
                      <span className="text-muted-foreground/60">({hook.phase})</span>
                      {hook.error && (
                        <span className="ml-1 truncate text-destructive" title={hook.error}>
                          {hook.error}
                        </span>
                      )}
                    </div>
                  ))}
                </div>
              )}
            </div>
          </div>
        )
      })}
    </section>
  )
}
