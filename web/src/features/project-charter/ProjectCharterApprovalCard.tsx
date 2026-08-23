import { useState } from 'react'
import { Scroll } from '@phosphor-icons/react'

import { Button } from '@/components/ui/button'
import { cn } from '@/lib/cn'

import { useProjectCharterApproval } from './hooks'
import { ProjectCharterReviewDialog } from './ProjectCharterReviewDialog'

/**
 * Pinned Charter approval card for the Project Agent Chat.
 *
 * A Project Agent can draft an adoption Charter but never approve one, so the
 * draft it commits dead-ends until a user acts on it. This surfaces that
 * request where the user is already working, next to the conversation that
 * produced it.
 */
export function ProjectCharterApprovalCard({
  projectId,
  className,
}: {
  projectId: string
  className?: string
}) {
  const approval = useProjectCharterApproval(projectId)
  const [reviewOpen, setReviewOpen] = useState(false)

  const { revision, blockedReason, isPending, error, approve } = approval
  if (!revision) return null

  return (
    <section
      className={cn(
        'mx-4 mt-3 min-w-0 rounded-lg border border-ember-border bg-card p-3 shadow-xs sm:mx-6 sm:p-4',
        className,
      )}
      aria-labelledby="project-charter-approval-heading"
    >
      <div className="flex min-w-0 flex-wrap items-center justify-between gap-3">
        <div className="flex min-w-0 items-center gap-2">
          <Scroll size={16} className="text-primary" aria-hidden />
          <h2
            id="project-charter-approval-heading"
            className="text-sm font-semibold text-foreground"
          >
            Project Charter awaiting your approval
          </h2>
          <span className="rounded-full border border-border bg-muted/30 px-2 py-0.5 font-mono text-micro uppercase tracking-[0.08em] text-muted-foreground">
            Revision {Number(revision.revision_number)} · {revision.lifecycle}
          </span>
        </div>
        <div className="flex shrink-0 items-center gap-2">
          <Button variant="outline" size="sm" onClick={() => setReviewOpen(true)}>
            Review
          </Button>
          <Button
            size="sm"
            disabled={isPending || blockedReason !== null}
            onClick={() => {
              void approve().catch(() => {
                // Message is rendered below; the card stays in place.
              })
            }}
          >
            {isPending ? 'Approving…' : 'Approve Charter'}
          </Button>
        </div>
      </div>
      <p className="mt-1.5 max-w-2xl text-xs leading-5 text-muted-foreground">
        Approving this exact revision makes it Project truth and unblocks planning and release. The
        Project Agent drafted it; only you can adopt it.
      </p>
      {blockedReason ? (
        <p className="mt-2 text-xs leading-5 text-warning" role="status">
          {blockedReason}
        </p>
      ) : null}
      {error ? (
        <p className="mt-2 text-xs leading-5 text-destructive" role="alert">
          {error}
        </p>
      ) : null}

      <ProjectCharterReviewDialog
        open={reviewOpen}
        onOpenChange={setReviewOpen}
        approval={approval}
      />
    </section>
  )
}
