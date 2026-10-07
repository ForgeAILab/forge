import type { ReactNode } from 'react'
import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import type { TaskListItem } from '@/types/generated'
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

function queuedTask(reason = 'project_at_capacity'): TaskListItem {
  return {
    id: 'task-50',
    project_id: 'project-1',
    title: 'NK-50',
    status: 'todo',
    priority: 0,
    parent_task_id: null,
    assignee_type: null,
    assignee_id: null,
    task_type: 'task',
    canonical_phase: 'ready',
    awaiting_human: false,
    board_position: 0,
    subtask_order: null,
    remaining_retries: {},
  retry_limits: {},
    condition: { kind: 'clear', details: { failure_kind: null, diagnostic: null, interruption: null, failed: false, blocked: false, human_wait: false, entry_wait: false } },
    workflow_exception: null,
    review_passed_at: null,
    archived_at: null,
    external_issue_number: null,
    external_issue_url: null,
    execution_observability: { latest_execution_id: null },
    version: 0,
    created_at: '2026-09-30T12:00:00Z',
    updated_at: '2026-09-30T12:00:00Z',
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
  }
}

describe('Project capacity waits on task surfaces', () => {
  it('labels owner review findings on fallback failure details', () => {
    render(
      <TaskBlockingBanner
        task={{
          ...queuedTask(),
          execution_observability: undefined,
          placement: null,
          effective_coder: null,
          effective_coder_source: null,
          status: 'review',
          condition: { kind: 'failed', failure: {kind: 'failure',failure_kind: 'review_needs_owner'},additional: [],resume: {kind: 'reconcile'},since: null,details: {failure_kind: 'review_needs_owner',diagnostic: null,human_wait: true,entry_wait: false,failed: true,blocked: false,interruption: {kind: 'review_needs_owner',reason: 'Needs macOS measurements',created_at: '2026-09-30T12:00:00Z'}} },
        }}
      />,
    )
    expect(screen.getByText('Needs owner')).toBeTruthy()
  })
  it.each(['project_at_capacity', 'project_waiting_on_owner', 'machine_capacity'])(
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
        task={{
          ...queuedTask(),
          execution_observability: undefined,
          placement: null,
          effective_coder: null,
          effective_coder_source: null,
        }}
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
