import { useState } from 'react'
import { ApiError, apiFetch } from '@/api/client'
import { useTaskAction } from '@/api/hooks'
import { toastApiError } from '@/lib/api-error'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import type { TaskActionsResponse, TaskAction, Offer } from '@/types/generated'
import {
  TaskActionButtons,
  TaskActionFields,
  actionInputValid,
  actionForPost,
} from './task-action-buttons'

type Row = { id: string; title: string; parent_task_id?: string | null }
type CancelEntry = { task: Row; version: number; offer: Offer; action: TaskAction }
export function TaskRowActions({ taskId }: { taskId: string }) {
  const [data, setData] = useState<TaskActionsResponse | null>(null)
  const [open, setOpen] = useState(false)
  const load = async () => {
    setOpen(true)
    setData(null)
    try {
      setData(await apiFetch<TaskActionsResponse>(`/tasks/${taskId}/actions`))
    } catch (error) {
      toastApiError(error, 'Actions failed to load')
      setOpen(false)
    }
  }
  return (
    <div onClick={(event) => event.stopPropagation()}>
      <Button
        size="sm"
        variant="ghost"
        onClick={() => {
          void load()
        }}
      >
        Actions
      </Button>
      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent>
          <DialogHeader className="mb-4">
            <DialogTitle>Task actions</DialogTitle>
            <DialogDescription>Choose an available action.</DialogDescription>
          </DialogHeader>
          {data ? (
            data.available_actions.length ? (
              <TaskActionButtons
                taskId={taskId}
                version={data.version}
                offers={data.available_actions}
              />
            ) : (
              <p>No actions available.</p>
            )
          ) : (
            <p role="status">Loading actions…</p>
          )}
        </DialogContent>
      </Dialog>
    </div>
  )
}
export function BulkCancelTasks({ tasks, onComplete }: { tasks: Row[]; onComplete: () => void }) {
  const command = useTaskAction()
  const [open, setOpen] = useState(false)
  const [entries, setEntries] = useState<CancelEntry[] | null>(null)
  const [skipped, setSkipped] = useState(0)
  const [notice, setNotice] = useState('')
  const load = async (announce = false) => {
    setOpen(true)
    setEntries(null)
    try {
      const availability: string[] = []
      const next = await Promise.all(
        tasks.map(async (task) => {
          const data = await apiFetch<TaskActionsResponse>(`/tasks/${task.id}/actions`)
          availability.push(
            `${task.title}: ${data.available_actions.map((offer) => offer.label).join(', ') || 'none'}`,
          )
          const offer = data.available_actions.find((item) => item.action.verb === 'cancel')
          return offer ? { task, offer, version: data.version, action: { ...offer.action } } : null
        }),
      )
      const cancellable = next.filter((entry): entry is CancelEntry => entry !== null)
      const roots = new Set(
        cancellable.filter((entry) => entry.offer.propagates).map((entry) => entry.task.id),
      )
      setEntries(
        cancellable.filter(
          (entry) => !entry.task.parent_task_id || !roots.has(entry.task.parent_task_id),
        ),
      )
      setSkipped(next.filter((entry) => entry === null).length)
      setNotice(announce ? `Available now: ${availability.join('; ')}` : '')
    } catch (error) {
      toastApiError(error, 'Actions failed to load')
      setOpen(false)
    }
  }
  const apply = async () => {
    if (!entries) return
    try {
      for (const entry of entries) {
        try {
          await command.mutateAsync({
            taskId: entry.task.id,
            version: entry.version,
            action: actionForPost(entry.offer, entry.action),
          })
        } catch (error) {
          if (error instanceof ApiError && error.status === 409) {
            const latest = await apiFetch<TaskActionsResponse>(`/tasks/${entry.task.id}/actions`)
            if (!latest.available_actions.some((offer) => offer.action.verb === 'cancel')) continue
          }
          throw error
        }
      }
      setOpen(false)
      onComplete()
    } catch (error) {
      toastApiError(error, 'Cancellation failed; refresh and review the available actions')
      await load(true)
    }
  }
  return (
    <>
      <Button
        size="sm"
        variant="outline"
        onClick={() => {
          void load()
        }}
      >
        Cancel selected
      </Button>
      <Dialog open={open} onOpenChange={setOpen}>
        <DialogContent className="max-h-[85dvh] overflow-y-auto">
          <DialogHeader className="mb-4">
            <DialogTitle>Cancel selected Tasks</DialogTitle>
            <DialogDescription>
              Only Tasks currently offering cancel will be cancelled.
            </DialogDescription>
          </DialogHeader>
          {notice ? (
            <p role="status" className="text-sm text-muted-foreground">
              {notice}
            </p>
          ) : null}
          {entries ? (
            <div className="space-y-4">
              {skipped ? <p role="status">{skipped} selected Tasks do not offer cancel.</p> : null}
              {entries.map((entry, index) => (
                <section key={entry.task.id} className="space-y-2 rounded-md border p-3">
                  <h3 className="text-sm font-semibold">{entry.task.title}</h3>
                  <TaskActionFields
                    offer={entry.offer}
                    action={entry.action}
                    onChange={(action) =>
                      setEntries(
                        entries.map((item, i) => (i === index ? { ...item, action } : item)),
                      )
                    }
                  />
                </section>
              ))}
            </div>
          ) : (
            <p role="status">Loading actions…</p>
          )}
          <DialogFooter className="mt-4">
            <Button variant="outline" onClick={() => setOpen(false)}>
              Close
            </Button>
            <Button
              disabled={
                command.isPending ||
                !entries?.length ||
                !entries.every((entry) => actionInputValid(entry.offer, entry.action))
              }
              onClick={() => {
                void apply()
              }}
            >
              Cancel {entries?.length ?? 0} Tasks
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  )
}
