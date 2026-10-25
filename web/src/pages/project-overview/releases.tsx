// Project release history and release review dialog.

import { Link } from '@tanstack/react-router'
import { ArrowUpRight } from '@phosphor-icons/react'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import type { ProjectRelease } from '@/types/generated'
import { count, formatDate, shortId } from './utils'
import type { ReleaseCandidate } from './utils'
import { StatusLabel, SectionCard, EmptyInline } from './shared'

export function ReleaseHistory({
  releases,
  projectId,
}: {
  releases: ProjectRelease[]
  projectId: string
}) {
  return (
    <SectionCard title="Release history" eyebrow="Immutable released truth">
      {releases.length === 0 ? (
        <EmptyInline text="No immutable release snapshots exist yet. A readiness result is only a release candidate." />
      ) : (
        <ol className="space-y-3">
          {releases.map((release) => {
            const snapshot = release.snapshot
            return (
              <li
                key={release.id}
                className="min-w-0 rounded-md border border-border-subtle bg-background p-3"
              >
                <div className="flex min-w-0 flex-wrap items-start justify-between gap-3">
                  <div className="min-w-0">
                    <p className="break-words text-sm font-semibold text-foreground">
                      {snapshot.display_label ?? release.release_identity}
                    </p>
                    <p className="mt-1 font-mono text-micro text-muted-foreground">
                      {snapshot.milestone_canonical_id}-r{count(snapshot.release_revision)} ·{' '}
                      {formatDate(snapshot.released_at)}
                    </p>
                  </div>
                  <StatusLabel status="released" />
                </div>
                <p className="mt-3 break-words text-xs leading-5 text-muted-foreground">
                  {snapshot.summary}
                </p>
                <div className="mt-3 grid gap-2 border-t border-border-subtle pt-3 text-xs sm:grid-cols-2">
                  <p className="break-words text-muted-foreground">
                    Released by{' '}
                    <span className="text-foreground">
                      {snapshot.released_by.display_name ?? snapshot.released_by.id}
                    </span>
                  </p>
                  <p className="break-all font-mono text-micro text-muted-foreground">
                    digest {shortId(snapshot.snapshot_digest)} · {snapshot.evidence_pins.length}{' '}
                    evidence pin{snapshot.evidence_pins.length === 1 ? '' : 's'}
                  </p>
                </div>
                {snapshot.known_issues.length > 0 ? (
                  <p className="mt-2 break-words text-xs text-warning">
                    Known issues: {snapshot.known_issues.join(' · ')}
                  </p>
                ) : null}
                <Link
                  to="/projects/$projectId/releases/$releaseId"
                  params={{ projectId, releaseId: release.id }}
                  className="mt-3 inline-flex items-center gap-1 text-xs font-medium text-primary hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                >
                  Inspect immutable snapshot <ArrowUpRight size={13} aria-hidden />
                </Link>
              </li>
            )
          })}
        </ol>
      )}
    </SectionCard>
  )
}

export function ReleaseReviewDialog({
  candidate,
  open,
  hasUser,
  isPending,
  error,
  onOpenChange,
  onConfirm,
}: {
  candidate: ReleaseCandidate | null
  open: boolean
  hasUser: boolean
  isPending: boolean
  error: string | null
  onOpenChange: (open: boolean) => void
  onConfirm: () => void
}) {
  if (!candidate) return null
  return (
    <Dialog open={open} onOpenChange={onOpenChange} ariaLabel="Review milestone release">
      <DialogContent className="max-w-xl">
        <DialogHeader>
          <DialogTitle>Review release · {candidate.canonicalId}</DialogTitle>
          <DialogDescription>
            Confirm the exact readiness snapshot Forge will record as immutable release truth. Forge
            never releases from a readiness result automatically.
          </DialogDescription>
        </DialogHeader>
        <dl className="mt-5 grid gap-3 rounded-md border border-border-subtle bg-muted/30 p-3 text-xs sm:grid-cols-2">
          <div className="min-w-0">
            <dt className="text-muted-foreground">Outcome</dt>
            <dd className="mt-1 break-words font-medium text-foreground">{candidate.label}</dd>
          </div>
          <div className="min-w-0">
            <dt className="text-muted-foreground">Release milestone version</dt>
            <dd className="mt-1 font-mono text-micro text-foreground">
              v{candidate.expectedMilestoneVersion}
            </dd>
          </div>
          <div className="min-w-0 sm:col-span-2">
            <dt className="text-muted-foreground">Readiness snapshot ID</dt>
            <dd className="mt-1 break-all font-mono text-micro text-foreground">
              {candidate.snapshotId}
            </dd>
          </div>
          <div className="min-w-0 sm:col-span-2">
            <dt className="text-muted-foreground">Readiness digest</dt>
            <dd className="mt-1 break-all font-mono text-micro text-foreground">
              {candidate.digest}
            </dd>
          </div>
          <div className="min-w-0 sm:col-span-2">
            <dt className="text-muted-foreground">Readiness captured against</dt>
            <dd className="mt-1 font-mono text-micro text-muted-foreground">
              milestone v{candidate.readinessExpectedMilestoneVersion}
            </dd>
          </div>
        </dl>
        {!hasUser ? (
          <p className="mt-3 text-xs text-warning" role="status">
            Sign in again before releasing this milestone. The server accepts only an authorization
            receipt for the current user.
          </p>
        ) : null}
        {error ? (
          <p className="mt-3 break-words text-xs leading-5 text-destructive" role="alert">
            {error}
          </p>
        ) : null}
        <DialogFooter className="mt-5 gap-2">
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={isPending}>
            Cancel
          </Button>
          <Button disabled={!hasUser || isPending} onClick={onConfirm}>
            {isPending ? 'Releasing exact snapshot…' : 'Confirm release'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
