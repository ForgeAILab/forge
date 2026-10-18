import { TaskFieldConflictError, type TaskConflictField } from '@/api/task-update-conflict'
import { getApiErrorMessage } from '@/lib/api-error'

const fieldLabels: Record<TaskConflictField, string> = {
  title: 'Title',
  description: 'Description',
  priority: 'Priority',
}

function currentFieldValue(error: TaskFieldConflictError, field: TaskConflictField): string {
  const value = error.currentTask[field]
  if (field === 'description' && (value == null || value === '')) return 'Empty'
  return String(value ?? '')
}

export function TaskEditSaveError({ error, field }: { error: unknown; field: TaskConflictField }) {
  if (!error) return null

  if (error instanceof TaskFieldConflictError && error.fields.includes(field)) {
    const label = fieldLabels[field]
    return (
      <div
        className="rounded-md border border-warning/50 bg-warning/10 p-3 text-sm text-foreground"
        data-task-edit-conflict={field}
        role="alert"
      >
        <p className="font-medium">
          {error.retryRaced
            ? 'This task kept changing while the edit was saved.'
            : `${label} changed in another client.`}
        </p>
        <p className="mt-1 break-words whitespace-pre-wrap">
          <span className="font-medium">Current {field}:</span> {currentFieldValue(error, field)}
        </p>
        <p className="mt-1 text-xs text-muted-foreground">
          Your draft is still here. Review it, then Save again to overwrite the current value or
          Cancel to keep it.
        </p>
      </div>
    )
  }

  return (
    <p className="text-sm text-destructive" role="alert">
      {getApiErrorMessage(error, `Failed to update task ${field}`)}
    </p>
  )
}
