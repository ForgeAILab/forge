import { useId, useState } from 'react'
import { toast } from 'sonner'
import { useTaskAction } from '@/api/hooks'
import { ApiError, apiFetch } from '@/api/client'
import { getApiErrorMessage } from '@/lib/api-error'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Label } from '@/components/ui/label'
import { Textarea } from '@/components/ui/textarea'
import type { Offer, TaskAction, TaskActionsResponse } from '@/types/generated'

function values(action: TaskAction): Record<string, unknown> {
  return { ...action }
}
export function parameterRequired(spec: Offer['parameters'][number], action: TaskAction) {
  return (
    spec.required ||
    Boolean(
      spec.required_when &&
      values(action)[spec.required_when.parameter] === spec.required_when.value,
    )
  )
}
export function actionInputValid(offer: Offer, action: TaskAction) {
  return offer.parameters.every((spec) => {
    const value = values(action)[spec.name]
    if (spec.boolean_values)
      return typeof value === 'boolean' && spec.boolean_values.includes(value)
    return (
      !parameterRequired(spec, action) || (typeof value === 'string' && value.trim().length > 0)
    )
  })
}
export function TaskActionFields({
  offer,
  action,
  onChange,
}: {
  offer: Offer
  action: TaskAction
  onChange: (action: TaskAction) => void
}) {
  const id = useId()
  return (
    <div className="space-y-4">
      {offer.action.verb === 'cancel' ? (
        <p className="text-sm">
          {offer.propagates
            ? 'This cancels this Task and its subtasks.'
            : 'This cancels this Task.'}
        </p>
      ) : null}
      {offer.parameters.map((spec) => {
        const required = parameterRequired(spec, action)
        const label = spec.name === 'override' ? 'Override checks' : spec.name.replaceAll('_', ' ')
        const fieldId = `${id}-${spec.name}`
        const value = values(action)[spec.name]
        return (
          <div className="space-y-2" key={spec.name}>
            <Label htmlFor={fieldId} className="capitalize">
              {label}
              {required ? ' (required)' : ' (optional)'}
            </Label>
            {spec.boolean_values ? (
              <select
                id={fieldId}
                className="w-full rounded-md border bg-background p-2 text-sm"
                value={String(value)}
                disabled={spec.boolean_values.length === 1}
                onChange={(event) =>
                  onChange({ ...action, [spec.name]: event.target.value === 'true' } as TaskAction)
                }
              >
                {spec.boolean_values.map((choice) => (
                  <option key={String(choice)} value={String(choice)}>
                    {choice ? 'Yes' : 'No'}
                  </option>
                ))}
              </select>
            ) : (
              <Textarea
                id={fieldId}
                required={required}
                value={typeof value === 'string' ? value : ''}
                onChange={(event) =>
                  onChange({ ...action, [spec.name]: event.target.value } as TaskAction)
                }
              />
            )}
          </div>
        )
      })}
    </div>
  )
}

/** Validity, parameters and labels come exclusively from the server's offers. */
export function TaskActionButtons({
  taskId,
  version,
  offers,
}: {
  taskId: string
  version: number
  offers: Offer[]
}) {
  const command = useTaskAction()
  const [refreshed, setRefreshed] = useState<TaskActionsResponse | null>(null)
  const [selected, setSelected] = useState<Offer | null>(null)
  const [action, setAction] = useState<TaskAction | null>(null)
  const [notice, setNotice] = useState('')
  const current =
    refreshed && refreshed.version >= version ? refreshed : { available_actions: offers, version }
  const apply = (value: TaskAction) =>
    command.mutate(
      { taskId, version: current.version, action: value },
      {
        onSuccess: (task) => {
          setSelected(null)
          setAction(null)
          setRefreshed({ available_actions: task.available_actions ?? [], version: task.version })
          setNotice('')
        },
        onError: (error) => {
          if (error instanceof ApiError && error.status === 409) {
            setSelected(null)
            setAction(null)
            void apiFetch<TaskActionsResponse>(`/tasks/${taskId}/actions`)
              .then((next) => {
                setRefreshed(next)
                setNotice(
                  `Actions changed. Available now: ${next.available_actions.map((offer) => offer.label).join(', ') || 'none'}.`,
                )
              })
              .catch((refreshError: unknown) =>
                toast.error(getApiErrorMessage(refreshError, 'Could not refresh actions')),
              )
          }
          toast.error(getApiErrorMessage(error, 'Task action failed'))
        },
      },
    )
  const choose = (offer: Offer) => {
    if (offer.parameters.length === 0 && offer.action.verb !== 'cancel') {
      apply(offer.action)
      return
    }
    setSelected(offer)
    const draft = { ...offer.action } as TaskAction
    for (const spec of offer.parameters) {
      if (spec.boolean_values?.length && values(draft)[spec.name] === undefined)
        Object.assign(draft, { [spec.name]: spec.boolean_values[0] })
    }
    setAction(draft)
  }
  return (
    <>
      {notice ? (
        <p role="status" className="text-sm text-muted-foreground">
          {notice}
        </p>
      ) : null}
      <div className="flex flex-wrap gap-2">
        {current.available_actions.map((offer) => (
          <Button
            key={`${offer.action.verb}:${offer.reason}`}
            size="sm"
            variant="outline"
            disabled={command.isPending}
            onClick={() => choose(offer)}
          >
            {offer.label}
          </Button>
        ))}
      </div>
      <Dialog
        open={selected != null}
        onOpenChange={(open) => {
          if (!open) setSelected(null)
        }}
      >
        <DialogContent className="max-h-[85dvh] overflow-y-auto">
          <DialogHeader className="mb-4">
            <DialogTitle>{selected?.label}</DialogTitle>
            <DialogDescription>Review the action and provide any required input.</DialogDescription>
          </DialogHeader>
          {selected && action ? (
            <TaskActionFields offer={selected} action={action} onChange={setAction} />
          ) : null}
          <DialogFooter className="mt-4">
            <Button variant="outline" onClick={() => setSelected(null)}>
              Close
            </Button>
            <Button
              disabled={
                command.isPending || !action || !selected || !actionInputValid(selected, action)
              }
              onClick={() => {
                if (action) apply(action)
              }}
            >
              Apply
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  )
}
