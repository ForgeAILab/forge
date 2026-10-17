import { useCallback, useEffect, useReducer, useRef } from 'react'
import type { DragStart, DragUpdate, DropResult } from '@hello-pangea/dnd'
import { toast } from 'sonner'
import { ApiError } from '@/api/client'
import {
  getApiErrorCode,
  isTaskBusy,
  notifyTaskBusy,
  TASK_BUSY_QUEUED_MESSAGE,
  toastApiError,
} from '@/lib/api-error'
import { useMoveTask } from './board-api'
import { planBoardMove, type BoardSnapshot } from './board-ordering'
import { boardReducer, createBoardState, type BoardState } from './board-reducer'

const STALE_BOARD_MESSAGE =
  'Board changed while you were dragging; refreshed to the latest version.'

export function useBoardDragSession({
  snapshot,
  columnStates,
  refresh,
}: {
  snapshot: BoardSnapshot
  columnStates: Record<string, string[]>
  refresh: () => Promise<BoardSnapshot>
}) {
  const [state, dispatch] = useReducer(boardReducer, snapshot, createBoardState)
  const stateRef = useRef<BoardState>(state)
  const moveTask = useMoveTask()

  useEffect(() => {
    stateRef.current = state
  }, [state])

  useEffect(() => {
    dispatch({ type: 'server_snapshot', snapshot })
  }, [snapshot])

  const reconcile = useCallback(async () => {
    try {
      const current = await refresh()
      dispatch({ type: 'reconciled', snapshot: current })
    } catch (error) {
      toastApiError(error, 'Board refresh failed')
    }
  }, [refresh])

  const onDragStart = useCallback((start: DragStart) => {
    dispatch({
      type: 'drag_started',
      draggableId: start.draggableId,
      operationId: crypto.randomUUID(),
    })
  }, [])

  const onDragUpdate = useCallback((update: DragUpdate) => {
    dispatch({ type: 'drag_updated', activeDropStatus: update.destination?.droppableId })
  }, [])

  const onDragEnd = useCallback(
    async (result: DropResult) => {
      const current = stateRef.current
      if (current.phase !== 'dragging' || !result.destination) {
        dispatch({ type: 'drag_cancelled' })
        return
      }
      const frozen = current.frozen
      const draggableId = result.draggableId
      const task = frozen?.tasks.find((candidate) => candidate.id === draggableId)
      if (!frozen || !task || !current.operationId) {
        dispatch({ type: 'drag_cancelled' })
        return
      }
      const targetColumn = result.destination.droppableId
      const sourceColumn = Object.entries(columnStates).find(([, statuses]) =>
        statuses.includes(task.status),
      )?.[0]
      const targetStatus = sourceColumn === targetColumn ? task.status : targetColumn
      const plan = planBoardMove({
        snapshot: frozen,
        draggableId,
        targetColumn,
        targetStatus,
        destinationIndex: result.destination.index,
        operationId: current.operationId,
        columnStates,
      })
      if (!plan || !plan.changed) {
        dispatch({ type: 'drag_cancelled' })
        return
      }

      dispatch({ type: 'commit_started', snapshot: plan.optimisticSnapshot })
      try {
        const response = await moveTask.mutateAsync({ taskId: draggableId, body: plan.request })
        dispatch({ type: 'commit_succeeded', response })
        await reconcile()
      } catch (error) {
        if (isTaskBusy(error)) {
          // Accepted and queued behind the current step; the board refreshes
          // now and again when the queued move applies.
          dispatch({ type: 'commit_failed', announcement: TASK_BUSY_QUEUED_MESSAGE })
          notifyTaskBusy(error)
          await reconcile()
          return
        }
        const code = error instanceof ApiError ? getApiErrorCode(error) : undefined
        const stale =
          error instanceof ApiError &&
          error.status === 409 &&
          (code === 'board_revision_conflict' || code === 'version_conflict')
        const announcement = stale ? STALE_BOARD_MESSAGE : 'Move was not applied; board refreshed.'
        dispatch({ type: 'commit_failed', announcement })
        if (stale) toast.error(STALE_BOARD_MESSAGE)
        else toastApiError(error, 'Move failed')
        await reconcile()
      }
    },
    [columnStates, moveTask, reconcile],
  )

  return {
    state,
    movePending: state.phase === 'committing' || state.phase === 'reconciling',
    onDragStart,
    onDragUpdate,
    onDragEnd,
    dismissAnnouncement: () => dispatch({ type: 'dismiss_announcement' }),
  }
}
