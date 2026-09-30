import type { ReactNode } from 'react'
import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import type { Task } from '@/types/generated'
import { KanbanTaskCard } from './kanban-task-card'
import { TaskDetailHeader } from './task-detail/task-detail-header'
import { TaskBlockingBanner } from './task-detail/task-blocking-banner'

vi.mock('@hello-pangea/dnd', () => ({
  Draggable: ({
    children,
  }: {
    children: (drag: {
      innerRef: () => void
      draggableProps: object
      dragHandleProps: object
    }) => ReactNode
  }) =>
    children({
      innerRef: () => {},
      draggableProps: {},
      dragHandleProps: {},
    }),
}))
vi.mock('@/api/hooks', () => ({
  useMembersQuery: () => ({ data: [] }),
  useProjectAgentsQuery: () => ({ data: [] }),
}))

function queuedTask(reason = 'project_at_capacity'): Task {
  return {
    id: 'task-50',
    project_id: 'project-1',
    title: 'NK-50',
    status: 'todo',
    priority: 0,
    role_assignments: [],
    workflow_health: {
      kind: 'waiting_for_agent',
      label: 'Waiting for a Slot',
      severity: 'info',
      message:
        reason === 'project_waiting_on_owner'
          ? 'Waiting on you: 10 parked'
          : 'Waiting for a slot (5/5 active)',
      state: 'todo',
      role: 'coder',
      execution_id: null,
      review_id: null,
      since: null,
      stale_reason: reason,
    },
  } as unknown as Task
}

describe('Project capacity waits on task surfaces', () => {
  it('labels owner review findings on fallback failure details', () => {
    render(
      <TaskBlockingBanner
        task={{
          ...queuedTask(),
          status: 'review',
          failed: {
            kind: 'review_needs_owner',
            reason: 'Needs macOS measurements',
            created_at: '2026-09-30T12:00:00Z',
          },
        }}
      />,
    )
    expect(screen.getByText('Needs owner')).toBeTruthy()
  })
  it.each(['project_at_capacity', 'project_waiting_on_owner'])(
    'shows the %s message on a Kanban card',
    (reason) => {
      const task = queuedTask(reason)
      render(
        <KanbanTaskCard
          task={task}
          index={0}
          showSubStateBadge={false}
          dragDisabled={false}
          movePending={false}
          agents={[]}
          agentNamesById={new Map()}
          claimPending={false}
          menuItems={null}
          onAssignAgent={vi.fn()}
          onClick={vi.fn()}
          onContextMenu={vi.fn()}
        />,
      )
      expect(screen.getByText(task.workflow_health!.message!)).toBeTruthy()
    },
  )

  it('shows the same capacity reason in the Task detail header', () => {
    render(
      <TaskDetailHeader
        task={queuedTask()}
        editingTitle={false}
        titleDraft="NK-50"
        updatePending={false}
        onTitleChange={vi.fn()}
        onTitleKeyDown={vi.fn()}
        onSaveTitle={vi.fn()}
        onCancelTitle={vi.fn()}
        onEditTitle={vi.fn()}
        onOpenFullPage={vi.fn()}
        onClose={vi.fn()}
      />,
    )
    expect(screen.getByText('Waiting for a slot (5/5 active)')).toBeTruthy()
  })

  it('does not present another dispatch capability as a capacity wait', () => {
    render(
      <KanbanTaskCard
        task={queuedTask('governance_denied')}
        index={0}
        showSubStateBadge={false}
        dragDisabled={false}
        movePending={false}
        agents={[]}
        agentNamesById={new Map()}
        claimPending={false}
        menuItems={null}
        onAssignAgent={vi.fn()}
        onClick={vi.fn()}
        onContextMenu={vi.fn()}
      />,
    )
    expect(screen.queryByText('Waiting for a slot (5/5 active)')).toBeNull()
  })
})
