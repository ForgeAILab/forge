import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'

import type { ProjectCharterApprovalState } from './hooks'

/**
 * The exact Charter revision under review, plus the approve action.
 *
 * Approval is the moment a legacy Project becomes Charter-backed, so the user
 * reads the server's canonical render — not chat prose — before deciding.
 */
export function ProjectCharterReviewDialog({
  open,
  onOpenChange,
  approval,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
  approval: ProjectCharterApprovalState
}) {
  const { revision, blockedReason, isPending, error, approve } = approval
  if (!revision) return null

  return (
    <Dialog open={open} onOpenChange={onOpenChange} ariaLabel="Review Project Charter">
      <DialogContent className="max-w-3xl">
        <DialogHeader>
          <DialogTitle>Project Charter · revision {Number(revision.revision_number)}</DialogTitle>
        </DialogHeader>
        <div className="max-h-[60vh] overflow-auto rounded-md border border-border-subtle bg-muted/20 p-3">
          <pre className="whitespace-pre-wrap font-mono text-xs leading-5 text-foreground">
            {revision.rendered_view}
          </pre>
        </div>
        <p className="mt-2 font-mono text-micro uppercase tracking-[0.08em] text-muted-foreground">
          content digest {revision.content_digest.slice(0, 16)}…
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
        <DialogFooter>
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button
            disabled={isPending || blockedReason !== null}
            onClick={() => {
              void approve()
                .then(() => onOpenChange(false))
                .catch(() => {
                  // The hook surfaces the message; keep the dialog open so the
                  // user can read it against the revision they were reviewing.
                })
            }}
          >
            {isPending ? 'Approving…' : 'Approve Charter'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
