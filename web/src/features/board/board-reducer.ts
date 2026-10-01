import type { MoveTaskResponse } from '@/types/generated'
import { compareBoardTasks, type BoardSnapshot } from './board-ordering'

export type BoardPhase = 'idle' | 'dragging' | 'committing' | 'reconciling'

export type BoardState = {
  phase: BoardPhase
  rendered: BoardSnapshot
  frozen?: BoardSnapshot
  queued?: BoardSnapshot
  draggableId?: string
  operationId?: string
  activeDropStatus?: string
  announcement?: string
}

export type BoardAction =
  | { type: 'server_snapshot'; snapshot: BoardSnapshot }
  | { type: 'drag_started'; draggableId: string; operationId: string }
  | { type: 'drag_updated'; activeDropStatus?: string }
  | { type: 'drag_cancelled' }
  | { type: 'commit_started'; snapshot: BoardSnapshot }
  | { type: 'commit_succeeded'; response: MoveTaskResponse }
  | { type: 'commit_failed'; announcement: string }
  | { type: 'reconciled'; snapshot: BoardSnapshot }
  | { type: 'dismiss_announcement' }

export function createBoardState(snapshot: BoardSnapshot): BoardState {
  return { phase: 'idle', rendered: snapshot }
}

export function boardReducer(state: BoardState, action: BoardAction): BoardState {
  switch (action.type) {
    case 'server_snapshot':
      if (state.phase === 'dragging' || state.phase === 'committing') {
        return {
          ...state,
          queued: newerSnapshot(state.queued, action.snapshot),
        }
      }
      if (state.phase === 'reconciling') {
        return settle(state, action.snapshot)
      }
      return { ...state, rendered: action.snapshot }
    case 'drag_started':
      if (state.phase !== 'idle') return state
      return {
        ...state,
        phase: 'dragging',
        frozen: state.rendered,
        queued: undefined,
        draggableId: action.draggableId,
        operationId: action.operationId,
        activeDropStatus: undefined,
      }
    case 'drag_updated':
      if (state.phase !== 'dragging') return state
      return { ...state, activeDropStatus: action.activeDropStatus }
    case 'drag_cancelled':
      return settle(state, state.queued ?? state.frozen ?? state.rendered)
    case 'commit_started':
      if (state.phase !== 'dragging') return state
      return { ...state, phase: 'committing', rendered: action.snapshot }
    case 'commit_succeeded': {
      const tasks = state.rendered.tasks
        .map((task) =>
          task.id === action.response.task.id
            ? {
                ...task,
                ...action.response.task,
                execution_observability: {
                  latest_execution_id:
                    action.response.task.execution_observability?.latest_execution_id ?? null,
                },
              }
            : task,
        )
        .sort(compareBoardTasks)
      return {
        ...state,
        phase: 'reconciling',
        rendered: {
          ...state.rendered,
          tasks,
          boardRevision: action.response.board_revision,
        },
        announcement: 'Move saved. Reconciling the latest board.',
      }
    }
    case 'commit_failed':
      return {
        ...state,
        phase: 'reconciling',
        rendered: state.queued ?? state.frozen ?? state.rendered,
        announcement: action.announcement,
      }
    case 'reconciled':
      return settle(state, newerSnapshot(state.queued, action.snapshot) ?? action.snapshot)
    case 'dismiss_announcement':
      return { ...state, announcement: undefined }
  }
}

function settle(state: BoardState, snapshot: BoardSnapshot): BoardState {
  return {
    phase: 'idle',
    rendered: snapshot,
    announcement: state.announcement,
  }
}

function newerSnapshot(
  current: BoardSnapshot | undefined,
  candidate: BoardSnapshot,
): BoardSnapshot {
  if (!current || candidate.boardRevision >= current.boardRevision) return candidate
  return current
}
