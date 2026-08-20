import { useState } from 'react'
import { Link } from '@tanstack/react-router'
import { WarningCircle } from '@phosphor-icons/react'

import { Button } from '@/components/ui/button'

import { useProjectCharterApproval } from './hooks'
import { ProjectCharterReviewDialog } from './ProjectCharterReviewDialog'

/**
 * Project Overview banner for a Project that predates an approved Charter.
 *
 * Until the Project Agent has drafted one there is nothing to act on and the
 * banner says where to go. Once a draft exists this is a real decision point,
 * not a note: the same review-and-approve flow the Project Agent Chat pins.
 */
export function ProjectCharterAdoptionBanner({ projectId }: { projectId: string }) {
  const approval = useProjectCharterApproval(projectId)
  const [reviewOpen, setReviewOpen] = useState(false)
  const { revision, blockedReason, isPending, error, approve } = approval

  return (
    <div
      className="flex min-w-0 items-start gap-3 rounded-lg border border-warning/40 bg-warning/10 p-4"
      role="status"
    >
      <WarningCircle size={19} className="mt-0.5 shrink-0 text-warning" aria-hidden />
      <div className="min-w-0 flex-1">
        <p className="font-medium text-foreground">Charter adoption is required before release</p>
        {revision ? (
          <>
            <p className="mt-1 break-words text-sm leading-6 text-muted-foreground">
              The Project Agent drafted an adoption Charter (revision {Number(revision.revision_number)}).
              Approving this exact revision makes it Project truth and unblocks planning and
              release.
            </p>
            <div className="mt-3 flex flex-wrap items-center gap-2">
              <Button size="sm" variant="outline" onClick={() => setReviewOpen(true)}>
                Review Charter
              </Button>
              <Button
                size="sm"
                disabled={isPending || blockedReason !== null}
                onClick={() => {
                  void approve().catch(() => {
                    // Message renders below; the banner stays put.
                  })
                }}
              >
                {isPending ? 'Approving…' : 'Approve Charter'}
              </Button>
            </div>
            {blockedReason ? (
              <p className="mt-2 text-xs leading-5 text-warning">{blockedReason}</p>
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
          </>
        ) : (
          <p className="mt-1 break-words text-sm leading-6 text-muted-foreground">
            This Project predates an approved Charter. Tasks, evidence, Documents, and Project
            Agent Chat remain usable;{' '}
            <Link
              to="/projects/$projectId/chat"
              params={{ projectId }}
              className="font-medium text-primary hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              ask the Project Agent
            </Link>{' '}
            to prepare an adoption Charter for explicit user approval.
          </p>
        )}
      </div>
    </div>
  )
}
