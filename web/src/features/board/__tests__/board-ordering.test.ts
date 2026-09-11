import { describe, expect, it } from 'vitest'
import type { Task, TasksResponse } from '@/types/generated'
import {
  assembleBoardSnapshot,
  orderingEligibility,
  planBoardMove,
  type BoardSnapshot,
} from '../board-ordering'

const columnStates = { todo: ['todo'], review: ['review', 'review_failed'] }

function task(id: string, status: string, position: number, version = 1): Task {
  return {
    id,
    project_id: 'project',
    title: id,
    task_type: 'task',
    status,
    priority: 0,
    board_position: position,
    role_assignments: [],
    remaining_retries: {},
    version,
    created_at: `2026-07-22T00:00:0${position}Z`,
    updated_at: '2026-07-22T00:00:00Z',
  }
}

function snapshot(tasks: Task[]): BoardSnapshot {
  return { tasks, boardRevision: 41, complete: true, mixedRevisions: false }
}

describe('board ordering', () => {
  it('resolves identity by draggable ID when a task was inserted ahead of the source index', () => {
    const frozen = snapshot([task('a', 'todo', 1, 4), task('drag-me', 'todo', 2, 7)])
    const plan = planBoardMove({
      snapshot: frozen,
      draggableId: 'drag-me',
      targetColumn: 'review',
      targetStatus: 'review',
      destinationIndex: 0,
      operationId: 'gesture-id',
      columnStates,
    })
    expect(plan?.request).toMatchObject({
      operation_id: 'gesture-id',
      task_version: 7,
      board_revision: 41,
      target_status: 'review',
    })
  })

  it('keeps the dragged task identity when another frozen-snapshot card is removed', () => {
    const frozen = snapshot([
      task('removed-later', 'todo', 1),
      task('drag-me', 'todo', 2, 9),
      task('still-here', 'review', 3),
    ])
    const plan = planBoardMove({
      snapshot: frozen,
      draggableId: 'drag-me',
      targetColumn: 'review',
      targetStatus: 'review',
      destinationIndex: 1,
      operationId: 'same-gesture',
      columnStates,
    })
    expect(plan?.request.task_version).toBe(9)
    expect(plan?.request.before_id).toBe('still-here')
    expect(plan?.request.operation_id).toBe('same-gesture')
  })

  it('derives preceding and following neighbors after removing the moved task', () => {
    const plan = planBoardMove({
      snapshot: snapshot([
        task('a', 'todo', 1),
        task('drag-me', 'todo', 2),
        task('b', 'todo', 3),
      ]),
      draggableId: 'drag-me',
      targetColumn: 'todo',
      targetStatus: 'todo',
      destinationIndex: 2,
      operationId: 'gesture-id',
      columnStates,
    })
    expect(plan?.request.before_id).toBe('b')
    expect(plan?.request.after_id).toBeNull()
    expect(plan?.changed).toBe(true)
  })

  it('uses null neighbors only for an empty destination column', () => {
    const plan = planBoardMove({
      snapshot: snapshot([task('drag-me', 'todo', 1)]),
      draggableId: 'drag-me',
      targetColumn: 'review',
      targetStatus: 'review',
      destinationIndex: 0,
      operationId: 'gesture-id',
      columnStates,
    })
    expect(plan?.request.before_id).toBeNull()
    expect(plan?.request.after_id).toBeNull()
  })

  it('detects incomplete and mixed-revision pages', () => {
    const page = (revision: number, hasMore: boolean): TasksResponse => ({
      items: [],
      next_cursor: hasMore ? 'next' : null,
      has_more: hasMore,
      total_count: null,
      board_revision: revision,
    })
    const mixed = assembleBoardSnapshot([page(4, true), page(5, false)])
    expect(mixed).toMatchObject({ complete: true, mixedRevisions: true, boardRevision: 4 })
    expect(orderingEligibility({
      hasSearchOrFilters: false,
      snapshot: mixed,
      workflowAvailable: true,
      committing: false,
    }).enabled).toBe(false)
  })

  it('enables ordering only for a complete unfiltered stable workflow snapshot', () => {
    const stable = snapshot([])
    expect(orderingEligibility({
      hasSearchOrFilters: false,
      snapshot: stable,
      workflowAvailable: true,
      committing: false,
    })).toEqual({ enabled: true })
    expect(orderingEligibility({
      hasSearchOrFilters: true,
      snapshot: stable,
      workflowAvailable: true,
      committing: false,
    }).reason).toContain('complete unfiltered board')
  })
})
