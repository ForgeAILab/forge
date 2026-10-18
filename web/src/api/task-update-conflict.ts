import type { Task } from '@/types/generated'

export type TaskConflictField = 'title' | 'description' | 'priority'
export type TaskConflictValue = string | number | null | undefined

export function taskFieldValue(task: Task, field: TaskConflictField): TaskConflictValue {
  return task[field]
}

export function taskFieldValuesEqual(
  field: TaskConflictField,
  left: TaskConflictValue,
  right: TaskConflictValue,
): boolean {
  if (field === 'description') return (left ?? '') === (right ?? '')
  return left === right
}

export class TaskFieldConflictError extends Error {
  readonly currentTask: Task
  readonly fields: TaskConflictField[]
  readonly retryRaced: boolean

  constructor(currentTask: Task, fields: TaskConflictField[], retryRaced = false) {
    super(
      retryRaced
        ? 'The task changed again while the edit was being saved.'
        : 'The edited task field changed in another client.',
    )
    this.name = 'TaskFieldConflictError'
    this.currentTask = currentTask
    this.fields = fields
    this.retryRaced = retryRaced
  }
}
