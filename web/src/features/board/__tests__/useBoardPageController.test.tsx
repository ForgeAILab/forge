import { act, renderHook } from '@testing-library/react'
import type { DropResult } from '@hello-pangea/dnd'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { WorkflowDefinition } from '@/types/generated'
import { taskListItem } from '@/test-utils/task-list-item'
import { useFilterStore } from '@/stores/filters'
import { useBoardPageController } from '../useBoardPageController'

const controls = vi.hoisted(() => ({
  draggableId: 'subtask',
  detailReady: true,
  detailError: false,
  detailQuery: vi.fn(),
  onDragEnd: vi.fn(),
}))
vi.mock('@tanstack/react-router', () => ({
  useNavigate: () => vi.fn(),
  useSearch: () => ({ includeCancelled: true }),
}))
vi.mock('@/api/hooks', () => ({
  useAgentsQuery: () => ({ data: { items: [] } }),
  useAssignRole: () => ({}),
  useCreateTask: () => ({}),
  useTransitionTask: () => ({}),
  useWorkflowQuery: () => ({ data: projectWorkflow }),
  useTaskDetailQuery: (id: string) => {
    controls.detailQuery(id)
    return {
      data: controls.detailReady ? { workflow: inheritedWorkflow } : undefined,
      isError: controls.detailError,
    }
  },
}))
vi.mock('../useBoardData', () => ({
  useBoardData: () => ({ query: {}, snapshot, refresh: vi.fn() }),
}))
vi.mock('../useBoardDragSession', () => ({
  useBoardDragSession: () => ({
    state: { rendered: snapshot, draggableId: controls.draggableId },
    movePending: false,
    onDragEnd: controls.onDragEnd,
  }),
}))

function workflow(names: string[]): WorkflowDefinition {
  return {
    roles: [],
    configuration: [],
    cancellation_state: 'cancelled',
    states: names.map((name, index) => ({
      name,
      kind: index === 0 ? 'initial' : ['done', 'cancelled'].includes(name) ? 'terminal' : 'active',
      column: name === 'lead_check' ? 'in_progress' : name,
      display_name: name,
      role: null,
      canonical_phase: null,
      cleanup: null,
      gate_config: null,
      dispatch: null,
      config: {},
      hooks: { before_exit: [], on_exit: [], before_enter: [], on_enter: [], after_enter: [] },
      triggers:
        name === 'todo'
          ? {
              accept: { to: 'in_progress', dispatch: null },
              fail: { to: 'cancelled', dispatch: null },
            }
          : name === 'in_progress'
            ? {
                accept: { to: names.includes('review') ? 'review' : 'lead_check', dispatch: null },
                fail: { to: 'cancelled', dispatch: null },
              }
            : name === 'lead_check'
              ? {
                  accept: { to: 'done', dispatch: null },
                  reject: { to: 'in_progress', dispatch: null },
                  fail: { to: 'cancelled', dispatch: null },
                }
              : {},
    })),
  }
}
const projectWorkflow = workflow(['todo', 'in_progress', 'review', 'done', 'cancelled'])
const inheritedWorkflow = workflow(['todo', 'in_progress', 'lead_check', 'done', 'cancelled'])
inheritedWorkflow.states.find((state) => state.name === 'cancelled')!.column = 'done'
const subtask = taskListItem({ id: 'subtask', parent_task_id: 'root', status: 'in_progress' })
const snapshot = {
  tasks: [subtask, taskListItem({ id: 'root', status: 'in_progress' })],
  complete: true,
  mixedRevisions: false,
  boardRevision: 1,
}
const drop: DropResult = {
  draggableId: 'subtask',
  type: 'DEFAULT',
  reason: 'DROP',
  mode: 'FLUID',
  combine: null,
  source: { droppableId: 'in_progress', index: 0 },
  destination: { droppableId: 'review', index: 0 },
}

beforeEach(() => {
  controls.draggableId = 'subtask'
  controls.detailReady = true
  controls.detailError = false
  controls.detailQuery.mockClear()
  controls.onDragEnd.mockClear()
  subtask.status = 'in_progress'
  useFilterStore.getState().resetFilters()
})

describe('board Task workflow exits', () => {
  it('uses the effective workflow from Task detail for a subtask, not Project review', () => {
    const { result } = renderHook(() => useBoardPageController('project-1'))
    expect(controls.detailQuery).toHaveBeenCalledWith('subtask')
    expect(result.current.validDropStatuses).toEqual(['in_progress', 'cancelled'])
    act(() => {
      void result.current.dragSession.onDragEnd(drop)
    })
    expect(controls.onDragEnd).toHaveBeenCalledWith(expect.objectContaining({ destination: null }))
  })

  it.each([
    ['todo', ['in_progress', 'cancelled']],
    ['lead_check', ['done', 'in_progress', 'cancelled']],
    ['done', []],
    ['cancelled', []],
  ])('offers only the effective workflow exits from %s', (status, expected) => {
    subtask.status = status as string
    const { result } = renderHook(() => useBoardPageController('project-1'))
    expect(result.current.validDropStatuses).toEqual(expected)
  })

  it.each([false, true])(
    'offers no cross-column subtask exit while its workflow is unavailable (error: %s)',
    (error) => {
      controls.detailReady = false
      controls.detailError = error
      const { result } = renderHook(() => useBoardPageController('project-1'))
      expect(result.current.validDropStatuses).toEqual([])
      act(() => {
        void result.current.dragSession.onDragEnd(drop)
      })
      expect(controls.onDragEnd).toHaveBeenCalledWith(
        expect.objectContaining({ destination: null }),
      )
    },
  )

  it('keeps the Project workflow for a top-level Task', () => {
    controls.draggableId = 'root'
    const { result } = renderHook(() => useBoardPageController('project-1'))
    expect(result.current.validDropStatuses).toEqual(['review', 'cancelled'])
    act(() => {
      void result.current.dragSession.onDragEnd({ ...drop, draggableId: 'root' })
    })
    expect(controls.onDragEnd).toHaveBeenCalledWith({ ...drop, draggableId: 'root' })
  })

  it('allows same-column reordering while a subtask workflow loads', () => {
    controls.detailReady = false
    const { result } = renderHook(() => useBoardPageController('project-1'))
    const reorder = { ...drop, destination: { droppableId: 'in_progress', index: 1 } }
    act(() => {
      void result.current.dragSession.onDragEnd(reorder)
    })
    expect(controls.onDragEnd).toHaveBeenCalledWith(reorder)
  })
})
