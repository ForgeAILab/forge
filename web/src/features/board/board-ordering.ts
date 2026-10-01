import type { TaskListItem as Task, TasksResponse } from '@/types/generated'
import type { MoveTaskRequest } from '@/types/generated'

export type BoardSnapshot = {
  tasks: Task[]
  boardRevision: number
  complete: boolean
  mixedRevisions: boolean
}

export type BoardOrderingInput = {
  hasSearchOrFilters: boolean
  snapshot: BoardSnapshot
  workflowAvailable: boolean
  committing: boolean
}

export type BoardOrderingEligibility = {
  enabled: boolean
  reason?: string
}

export type BoardMovePlan = {
  request: MoveTaskRequest
  optimisticSnapshot: BoardSnapshot
  changed: boolean
}

export function assembleBoardSnapshot(pages: TasksResponse[] | undefined): BoardSnapshot {
  if (!pages || pages.length === 0) {
    return { tasks: [], boardRevision: 0, complete: false, mixedRevisions: false }
  }
  const revisions = new Set(pages.map((page) => page.board_revision))
  return {
    tasks: pages.flatMap((page) => page.items),
    boardRevision: pages[0].board_revision,
    complete: !pages.at(-1)?.has_more,
    mixedRevisions: revisions.size > 1,
  }
}

export function orderingEligibility(input: BoardOrderingInput): BoardOrderingEligibility {
  if (input.committing) {
    return { enabled: false, reason: 'A board move is being saved.' }
  }
  if (input.hasSearchOrFilters) {
    return {
      enabled: false,
      reason: 'Ordering requires the complete unfiltered board. Clear search and filters to move cards.',
    }
  }
  if (!input.workflowAvailable) {
    return { enabled: false, reason: 'Ordering is unavailable until the workflow loads.' }
  }
  if (!input.snapshot.complete) {
    return { enabled: false, reason: 'Load every task page before changing board order.' }
  }
  if (input.snapshot.mixedRevisions) {
    return { enabled: false, reason: 'Board pages changed while loading. Refresh before ordering.' }
  }
  return { enabled: true }
}

export function tasksForColumn(
  tasks: Task[],
  primaryStatus: string,
  columnStates: Record<string, string[]>,
): Task[] {
  const statuses = new Set(columnStates[primaryStatus] ?? [primaryStatus])
  return tasks
    .filter((task) => statuses.has(task.status))
    .sort(compareBoardTasks)
}

export function planBoardMove({
  snapshot,
  draggableId,
  targetColumn,
  targetStatus,
  destinationIndex,
  operationId,
  columnStates,
}: {
  snapshot: BoardSnapshot
  draggableId: string
  targetColumn: string
  targetStatus: string
  destinationIndex: number
  operationId: string
  columnStates: Record<string, string[]>
}): BoardMovePlan | undefined {
  // Identity is resolved from the stable draggable ID; source indexes are never consulted.
  const task = snapshot.tasks.find((candidate) => candidate.id === draggableId)
  if (!task) return undefined

  const destination = tasksForColumn(snapshot.tasks, targetColumn, columnStates).filter(
    (candidate) => candidate.id !== draggableId,
  )
  const insertionIndex = Math.max(0, Math.min(destinationIndex, destination.length))
  const beforeId = destination[insertionIndex - 1]?.id ?? null
  const afterId = destination[insertionIndex]?.id ?? null
  const sourceColumn = Object.entries(columnStates).find(([, statuses]) =>
    statuses.includes(task.status),
  )?.[0]
  const sourceOrder = sourceColumn
    ? tasksForColumn(snapshot.tasks, sourceColumn, columnStates).map((item) => item.id)
    : []
  const nextDestinationOrder = destination.map((item) => item.id)
  nextDestinationOrder.splice(insertionIndex, 0, draggableId)
  const changed = sourceColumn !== targetColumn || !sameIds(sourceOrder, nextDestinationOrder)

  const lower = destination[insertionIndex - 1]?.board_position
  const upper = destination[insertionIndex]?.board_position
  const optimisticPosition =
    lower !== undefined && upper !== undefined
      ? (lower + upper) / 2
      : lower !== undefined
        ? lower + 1
        : upper !== undefined
          ? upper - 1
          : Math.max(0, ...snapshot.tasks.map((item) => item.board_position)) + 1
  const optimisticTask = {
    ...task,
    status: targetStatus,
    board_position: optimisticPosition,
  }

  return {
    request: {
      operation_id: operationId,
      task_version: task.version,
      board_revision: snapshot.boardRevision,
      target_status: targetStatus,
      before_id: beforeId,
      after_id: afterId,
    },
    optimisticSnapshot: {
      ...snapshot,
      tasks: snapshot.tasks
        .map((candidate) => (candidate.id === draggableId ? optimisticTask : candidate))
        .sort(compareBoardTasks),
    },
    changed,
  }
}

export function compareBoardTasks(left: Task, right: Task): number {
  return (
    left.board_position - right.board_position ||
    left.created_at.localeCompare(right.created_at) ||
    left.id.localeCompare(right.id)
  )
}

function sameIds(left: string[], right: string[]): boolean {
  return left.length === right.length && left.every((id, index) => id === right[index])
}
