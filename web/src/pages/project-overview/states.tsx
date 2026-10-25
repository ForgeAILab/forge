// Project Overview projection, loading, access, and error states.

import { useQueryClient } from '@tanstack/react-query'
import { Link, useNavigate } from '@tanstack/react-router'
import { useEffect } from 'react'
import { ArrowClockwise, CircleNotch, LockKey, Pulse, WarningCircle } from '@phosphor-icons/react'
import { ConflictDetails } from '@/components/conflict-details'
import { Button } from '@/components/ui/button'
import { buttonClassName } from '@/components/ui/button-styles'
import { isApiStatus } from '@/lib/api-error'
import { clearDeletedProjectScope, resolveNextProjectId } from '@/stores/project-scope'
import type { OverviewProjectionState } from '@/types/generated'
import { humanize, shortId } from './utils'

export function ProjectionBanner({
  state,
  watermark,
  onRetry,
}: {
  state: OverviewProjectionState
  watermark: string
  onRetry: () => void
}) {
  if (state === 'current') return null
  const stale = state === 'stale'
  const loading = state === 'loading'
  return (
    <div
      className={`flex min-w-0 items-start gap-2 rounded-md border p-3 text-sm ${stale ? 'border-warning/40 bg-warning/10' : 'border-border-subtle bg-muted'}`}
      role={state === 'error' ? 'alert' : 'status'}
      aria-live="polite"
    >
      {loading ? (
        <CircleNotch size={17} className="mt-0.5 shrink-0 animate-spin" aria-hidden />
      ) : (
        <Pulse size={17} className="mt-0.5 shrink-0" aria-hidden />
      )}
      <div className="min-w-0">
        <p className="font-medium text-foreground">
          {stale
            ? 'Overview is stale'
            : loading
              ? 'Overview is refreshing'
              : `Overview projection ${humanize(state)}`}
        </p>
        <p className="mt-1 break-words text-xs text-muted-foreground">
          {stale
            ? `Cached progress is shown for inspection only; it is not current release truth. Source watermark ${shortId(watermark)}.`
            : 'Some projection sources are not current. Review the affected canonical records before treating this as ready or released.'}
        </p>
      </div>
      {state !== 'loading' ? (
        <Button
          type="button"
          size="sm"
          variant="outline"
          className="ml-auto shrink-0"
          onClick={onRetry}
        >
          <ArrowClockwise size={13} aria-hidden />
          {stale ? 'Refresh' : 'Retry'}
        </Button>
      ) : null}
    </div>
  )
}

export function LoadingState() {
  return (
    <div
      className="mx-auto flex w-full max-w-[1440px] flex-col gap-5"
      aria-busy="true"
      role="status"
    >
      <div className="space-y-3">
        <div className="h-3 w-32 animate-pulse rounded bg-muted" />
        <div className="h-8 w-2/3 animate-pulse rounded bg-muted" />
        <div className="h-4 w-full max-w-2xl animate-pulse rounded bg-muted" />
      </div>
      <div className="grid min-w-0 gap-5 xl:grid-cols-[minmax(0,1.45fr)_minmax(300px,0.75fr)]">
        <div className="space-y-5">
          <div className="h-32 animate-pulse rounded-lg border border-border-subtle bg-muted" />
          <div className="h-72 animate-pulse rounded-lg border border-border-subtle bg-muted" />
        </div>
        <div className="space-y-5">
          <div className="h-56 animate-pulse rounded-lg border border-border-subtle bg-muted" />
          <div className="h-48 animate-pulse rounded-lg border border-border-subtle bg-muted" />
        </div>
      </div>
      Loading Project Overview…
    </div>
  )
}

export function DeniedState({ projectId }: { projectId: string }) {
  return (
    <div className="mx-auto flex w-full max-w-2xl flex-col items-center gap-4 py-16 text-center">
      <div className="rounded-full border border-border-subtle bg-muted p-3 text-muted-foreground">
        <LockKey size={22} aria-hidden />
      </div>
      <div>
        <h1 className="text-lg font-semibold text-foreground">Project Overview access denied</h1>
        <p className="mt-2 text-sm leading-6 text-muted-foreground">
          This account is not authorized to read the Project Overview projection. Protected Project
          details and media are withheld.
        </p>
      </div>
      <div className="flex flex-wrap justify-center gap-2">
        <Link
          to="/projects/$projectId/chat"
          params={{ projectId }}
          className={buttonClassName({ variant: 'outline' })}
        >
          Open Project Agent Chat
        </Link>
        <Link
          to="/projects/$projectId/tasks"
          params={{ projectId }}
          search={{ sort_by: 'updated_at', sort_order: 'desc' }}
          className={buttonClassName({ variant: 'ghost' })}
        >
          View Tasks
        </Link>
      </div>
    </div>
  )
}

/**
 * The Project this route names is gone — deleted by this user elsewhere, by
 * another authorized member, or by this same delete just committing. F17 /
 * 8.4.4 requires external deletion and an authorized 404 to converge the
 * same way explicit deletion does: clear the deleted scope and land on
 * another authorized Project or Main Chat, not a dead Overview page.
 */
export function DeletedProjectRedirect({ projectId }: { projectId: string }) {
  const queryClient = useQueryClient()
  const navigate = useNavigate()

  useEffect(() => {
    let cancelled = false
    clearDeletedProjectScope(queryClient, projectId)
    void resolveNextProjectId(queryClient, projectId).then((nextProjectId) => {
      if (cancelled) return
      void navigate(
        nextProjectId
          ? { to: '/projects/$projectId/board', params: { projectId: nextProjectId } }
          : { to: '/chat' },
      )
    })
    return () => {
      cancelled = true
    }
  }, [projectId, queryClient, navigate])

  return (
    <div
      className="mx-auto flex w-full max-w-2xl flex-col items-center gap-3 py-16 text-center"
      role="status"
      aria-live="polite"
    >
      <p className="text-sm text-muted-foreground">This Project no longer exists. Redirecting…</p>
    </div>
  )
}

export function ErrorState({
  error,
  onRetry,
  projectId,
}: {
  error: unknown
  onRetry: () => void
  projectId: string
}) {
  const conflict = isApiStatus(error, 409)
  return (
    <div className="mx-auto flex w-full max-w-2xl flex-col items-center gap-4 py-16 text-center">
      <WarningCircle size={24} className="text-destructive" aria-hidden />
      <div>
        <h1 className="text-lg font-semibold text-foreground">
          {conflict ? 'Overview projection conflict' : 'Overview unavailable'}
        </h1>
        <p className="mt-2 text-sm leading-6 text-muted-foreground">
          {conflict
            ? 'The displayed projection changed while it was loading. Refresh to reconcile against current canonical records.'
            : 'Forge could not load this Project Overview. Existing Tasks and Project Agent Chat remain available.'}
        </p>
        {conflict ? (
          <ConflictDetails error={error} fallbackAuthority="Project Overview projection" />
        ) : null}
      </div>
      <div className="flex flex-wrap justify-center gap-2">
        <Button onClick={onRetry}>
          <ArrowClockwise size={15} aria-hidden /> Retry
        </Button>
        <Link
          to="/projects/$projectId/chat"
          params={{ projectId }}
          className={buttonClassName({ variant: 'outline' })}
        >
          Open Project Agent Chat
        </Link>
      </div>
    </div>
  )
}
