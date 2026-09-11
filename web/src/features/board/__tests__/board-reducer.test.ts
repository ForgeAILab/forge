import { describe, expect, it } from 'vitest'
import type { MoveTaskResponse, Task } from '@/types/generated'
import { boardReducer, createBoardState } from '../board-reducer'
import type { BoardSnapshot } from '../board-ordering'

function task(id: string, position: number, version = 1): Task {
  return {
    id,
    project_id: 'project',
    title: id,
    task_type: 'task',
    status: 'todo',
    priority: 0,
    board_position: position,
    role_assignments: [],
    remaining_retries: {},
    version,
    created_at: '2026-07-22T00:00:00Z',
    updated_at: '2026-07-22T00:00:00Z',
  }
}

function snapshot(revision: number, tasks = [task('a', 1), task('b', 2)]): BoardSnapshot {
  return { tasks, boardRevision: revision, complete: true, mixedRevisions: false }
}

describe('board reducer', () => {
  it('queues newer server snapshots while dragging without replacing the frozen collection', () => {
    const initial = snapshot(41)
    let state = createBoardState(initial)
    state = boardReducer(state, {
      type: 'drag_started',
      draggableId: 'b',
      operationId: 'gesture',
    })
    const inserted = snapshot(42, [task('new', 0), ...initial.tasks])
    state = boardReducer(state, { type: 'server_snapshot', snapshot: inserted })
    expect(state.phase).toBe('dragging')
    expect(state.rendered.tasks.map((item) => item.id)).toEqual(['a', 'b'])
    expect(state.frozen?.tasks.map((item) => item.id)).toEqual(['a', 'b'])
    expect(state.queued?.tasks.map((item) => item.id)).toEqual(['new', 'a', 'b'])
  })

  it('rolls a conflict back to queued server truth and reconciles deterministically', () => {
    let state = createBoardState(snapshot(41))
    state = boardReducer(state, {
      type: 'drag_started',
      draggableId: 'b',
      operationId: 'gesture',
    })
    state = boardReducer(state, { type: 'server_snapshot', snapshot: snapshot(42) })
    state = boardReducer(state, { type: 'commit_started', snapshot: snapshot(41) })
    state = boardReducer(state, {
      type: 'commit_failed',
      announcement: 'Board changed while you were dragging; refreshed to the latest version.',
    })
    expect(state.phase).toBe('reconciling')
    expect(state.rendered.boardRevision).toBe(42)
    state = boardReducer(state, { type: 'reconciled', snapshot: snapshot(42) })
    expect(state.phase).toBe('idle')
    expect(state.announcement).toContain('Board changed')
  })

  it('applies an idempotent success once and waits for reconciliation', () => {
    let state = createBoardState(snapshot(41))
    state = boardReducer(state, {
      type: 'drag_started',
      draggableId: 'b',
      operationId: 'gesture',
    })
    state = boardReducer(state, { type: 'commit_started', snapshot: snapshot(41) })
    const response: MoveTaskResponse = {
      task: task('b', 0, 2),
      board_revision: 42,
      operation_id: 'gesture',
    }
    state = boardReducer(state, { type: 'commit_succeeded', response })
    state = boardReducer(state, { type: 'commit_succeeded', response })
    expect(state.phase).toBe('reconciling')
    expect(state.rendered.tasks.filter((item) => item.id === 'b')).toHaveLength(1)
    expect(state.rendered.tasks[0].id).toBe('b')
  })
})
