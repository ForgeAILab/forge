import { useState } from 'react'
import { toast } from 'sonner'
import { useDaemonRemovalPreview, useRemoveDaemon } from '@/api/hooks'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { getApiErrorMessage } from '@/lib/api-error'
import { useAuthStore } from '@/stores/auth'
import type { Daemon } from '@/types/generated'

function count(value: number, one: string, many: string) {
  return `${value} ${value === 1 ? one : many}`
}

export function RemoveMachineButton({
  daemon,
  onRemoved,
}: {
  daemon: Daemon
  onRemoved?: () => void
}) {
  const user = useAuthStore((state) => state.user)
  const mutation = useRemoveDaemon()
  const [open, setOpen] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const preview = useDaemonRemovalPreview(daemon.id, open)
  const mayRemove = user?.is_admin || (daemon.owner_id != null && daemon.owner_id === user?.id)
  if (!mayRemove) return null
  const local = daemon.machine_id.startsWith('embedded:')
  const connected = daemon.status === 'online'
  const hint = local
    ? 'The embedded server machine cannot be removed.'
    : connected
      ? 'Stop the daemon before removing this machine.'
      : 'Revokes this machine’s registration and clears its pending remote cleanup.'
  const disabled = local || connected || mutation.isPending
  const hintId = `remove-machine-hint-${daemon.id}`
  function remove() {
    setError(null)
    mutation.mutate(daemon.id, {
      onSuccess: () => {
        setOpen(false)
        toast.success(`Removed machine ${daemon.hostname}`)
        onRemoved?.()
      },
      onError: (cause) => setError(getApiErrorMessage(cause, 'Could not remove machine')),
    })
  }
  return (
    <section aria-label="Remove machine" className="space-y-3">
      <Button
        variant="destructive"
        disabled={disabled}
        aria-describedby={hintId}
        onClick={() => {
          setError(null)
          setOpen(true)
        }}
      >
        Remove
      </Button>
      <p id={hintId} className="text-sm text-muted-foreground">
        {hint}
      </p>
      <Dialog
        open={open}
        onOpenChange={(value) => {
          if (!mutation.isPending) setOpen(value)
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Remove {daemon.hostname}?</DialogTitle>
            <DialogDescription>
              This revokes the machine’s credential and clears pending remote cleanup. Execution
              history keeps the machine’s name. Connecting again requires a new registration.
            </DialogDescription>
          </DialogHeader>
          <p role="status" className="text-sm">
            {preview.data
              ? `${count(preview.data.tasks_to_replace, 'Task has', 'Tasks have')} a workspace on this machine and will be re-placed on another machine. ${count(preview.data.agents_to_retire, 'Agent', 'Agents')} pinned to it will be retired.`
              : preview.isError
                ? 'Could not count the affected Tasks. Every Task with a workspace on this machine will be re-placed on another machine.'
                : 'Counting affected Tasks…'}
          </p>
          <p className="text-sm font-medium text-destructive">
            Work on this machine that was not pushed is abandoned. Re-placed Tasks continue from
            their last server-known branch.
          </p>
          {connected && (
            <p role="status" className="text-sm">
              {hint}
            </p>
          )}
          {error && (
            <p role="alert" className="text-sm text-destructive">
              {error}
            </p>
          )}
          <DialogFooter>
            <Button variant="outline" disabled={mutation.isPending} onClick={() => setOpen(false)}>
              Cancel
            </Button>
            <Button variant="destructive" disabled={disabled} onClick={remove}>
              {mutation.isPending ? 'Removing…' : 'Remove machine'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </section>
  )
}
