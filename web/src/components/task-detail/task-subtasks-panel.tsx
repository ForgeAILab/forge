import { ArrowDown, ArrowUp, Plus } from '@phosphor-icons/react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useState } from 'react'
import { toast } from 'sonner'
import { reorderSubtasks } from '@/api/client'
import { useCreateTask, useTaskRelationsQuery } from '@/api/hooks'
import { qk } from '@/api/query-keys'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Skeleton } from '@/components/ui/skeleton'
import { getApiErrorMessage } from '@/lib/api-error'
import type { Execution, Task, TaskRelationSummary } from '@/types/generated'

type SubtaskSummary = Pick<
  TaskRelationSummary,
  'id' | 'title' | 'status' | 'parent_task_id' | 'subtask_order' | 'created_at'
>

export function getRootSubtasks<T extends SubtaskSummary>(tasks: T[], taskId: string): T[] {
  return tasks
    .filter((candidate) => candidate.parent_task_id === taskId)
    .sort((a, b) => {
      const orderA = a.subtask_order ?? Number.MAX_SAFE_INTEGER
      const orderB = b.subtask_order ?? Number.MAX_SAFE_INTEGER
      if (orderA !== orderB) return orderA - orderB
      return a.created_at.localeCompare(b.created_at) || a.id.localeCompare(b.id)
    })
}

export function hasIncompleteSubtasks(
  subtasks: Array<Pick<TaskRelationSummary, 'status'>>,
): boolean {
  return subtasks.length > 0 && subtasks.some((subtask) => subtask.status !== 'done')
}

export function TaskSubtasksPanel({ task, executions }: { task: Task; executions: Execution[] }) {
  const queryClient = useQueryClient()
  const relationsQuery = useTaskRelationsQuery(
    task.id,
    task.project_id,
    task.parent_task_id == null,
  )
  const createTask = useCreateTask(task.project_id)
  const subtasks = getRootSubtasks(
    (relationsQuery.data?.subtasks ?? []).filter((subtask) => subtask.status !== 'cancelled'),
    task.id,
  )
  const hasWorkspace = executions.some((execution) => execution.workspace_id)
  const reorderDisabledReason = hasWorkspace
    ? 'Task already has a workspace.'
    : subtasks.some((subtask) => subtask.status !== 'todo')
      ? 'Subtask sequence has started.'
      : undefined
  const reorderMutation = useMutation({
    mutationFn: (orderedIds: string[]) => reorderSubtasks(task.id, { ordered_ids: orderedIds }),
    onSuccess: (updatedTask) => {
      void queryClient.invalidateQueries({ queryKey: qk.task(updatedTask.id) })
      void queryClient.invalidateQueries({ queryKey: qk.taskRelations(task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.projectTasks(updatedTask.project_id) })
    },
    onError: (error) => toast.error(getApiErrorMessage(error, 'Subtask reorder failed')),
  })

  const [showCreate, setShowCreate] = useState(false)
  const [newTitle, setNewTitle] = useState('')

  if (task.parent_task_id != null) return null

  const moveSubtask = (index: number, direction: -1 | 1) => {
    const targetIndex = index + direction
    if (targetIndex < 0 || targetIndex >= subtasks.length || reorderDisabledReason) return
    const next = [...subtasks]
    const [moved] = next.splice(index, 1)
    next.splice(targetIndex, 0, moved)
    reorderMutation.mutate(next.map((subtask) => subtask.id))
  }

  const submitCreate = () => {
    if (!newTitle.trim()) return
    createTask.mutate(
      {
        title: newTitle.trim(),
        parent_task_id: task.id,
      },
      {
        onSuccess: () => {
          setNewTitle('')
          setShowCreate(false)
          void queryClient.invalidateQueries({ queryKey: qk.taskDetail(task.id) })
          void queryClient.invalidateQueries({ queryKey: qk.taskRelations(task.id) })
        },
        onError: (error) => toast.error(getApiErrorMessage(error, 'Failed to create subtask')),
      },
    )
  }

  return (
    <div className="mt-4 border-t pt-4">
      <div className="mb-2 flex items-center justify-between">
        <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
          Subtasks
        </p>
        <Button
          size="icon"
          variant="ghost"
          className="h-6 w-6"
          onClick={() => setShowCreate((v) => !v)}
        >
          <Plus size={14} />
        </Button>
      </div>

      {showCreate ? (
        <div className="mb-3 space-y-2 rounded-md border p-3">
          <Input
            autoFocus
            placeholder="Subtask title"
            value={newTitle}
            className="h-8"
            onChange={(e) => setNewTitle(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Enter') submitCreate()
              if (e.key === 'Escape') setShowCreate(false)
            }}
          />
          <div className="flex gap-2">
            <Button
              size="sm"
              disabled={!newTitle.trim() || createTask.isPending}
              onClick={submitCreate}
            >
              Add
            </Button>
            <Button size="sm" variant="outline" onClick={() => setShowCreate(false)}>
              Cancel
            </Button>
          </div>
        </div>
      ) : null}

      {relationsQuery.isLoading ? (
        <div className="space-y-2">
          <Skeleton className="h-9 w-full" />
          <Skeleton className="h-9 w-full" />
        </div>
      ) : relationsQuery.isError && !relationsQuery.data ? (
        <div className="flex items-center justify-between gap-2 rounded-md border px-3 py-2">
          <p role="status" className="text-sm text-muted-foreground">
            Couldn&apos;t load task relationships
          </p>
          <Button size="sm" variant="ghost" onClick={() => void relationsQuery.refetch()}>
            Retry
          </Button>
        </div>
      ) : subtasks.length === 0 ? (
        <p className="text-sm text-muted-foreground">No subtasks</p>
      ) : (
        <div className="space-y-2">
          {subtasks.map((subtask, index) => {
            const canMoveUp = index > 0 && !reorderDisabledReason
            const canMoveDown = index < subtasks.length - 1 && !reorderDisabledReason
            return (
              <div key={subtask.id} className="rounded-md border bg-background p-2">
                <div className="flex items-start gap-2">
                  <div className="min-w-0 flex-1">
                    <p className="truncate text-sm font-medium">{subtask.title}</p>
                    <p className="mt-0.5 text-xs text-muted-foreground">
                      {subtask.status} - order {subtask.subtask_order ?? index + 1}
                    </p>
                  </div>
                  <div className="flex shrink-0 items-center gap-1">
                    <span title={!canMoveUp ? reorderDisabledReason : undefined}>
                      <Button
                        aria-label="Move subtask up"
                        className="h-7 w-7"
                        disabled={!canMoveUp || reorderMutation.isPending}
                        size="icon"
                        variant="ghost"
                        onClick={() => moveSubtask(index, -1)}
                      >
                        <ArrowUp size={14} />
                      </Button>
                    </span>
                    <span title={!canMoveDown ? reorderDisabledReason : undefined}>
                      <Button
                        aria-label="Move subtask down"
                        className="h-7 w-7"
                        disabled={!canMoveDown || reorderMutation.isPending}
                        size="icon"
                        variant="ghost"
                        onClick={() => moveSubtask(index, 1)}
                      >
                        <ArrowDown size={14} />
                      </Button>
                    </span>
                  </div>
                </div>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}
