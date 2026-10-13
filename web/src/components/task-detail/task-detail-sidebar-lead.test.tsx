import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, render, screen, within } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import type { Agent, Task, WorkflowDefinition } from '@/types/generated'
import { TaskDetailSidebar } from './task-detail-sidebar'

const { relations, agents } = vi.hoisted(() => ({ relations: vi.fn(), agents: vi.fn() }))

vi.mock('@/api/hooks', () => {
  const mutation = () => ({ mutate: vi.fn(), isPending: false })
  return {
    useAssignRole: mutation,
    useRemoveRole: mutation,
    useDuplicateTask: mutation,
    useUpdateTask: mutation,
    useMembersQuery: () => ({ data: undefined }),
    useProjectAgentsQuery: () => ({ data: agents() }),
    useTaskRelationsQuery: () => relations(),
  }
})
vi.mock('@/components/task-execution-observability', () => ({
  TaskExecutionObservabilityPanel: () => null,
}))
vi.mock('./task-action-buttons', () => ({ TaskActionButtons: () => null }))

afterEach(() => {
  cleanup()
  vi.clearAllMocks()
})

const now = '2026-10-12T12:00:00Z'

function agent(id: string, name: string, backend_kind: string): Agent {
  return {
    id,
    name,
    backend_kind,
    status: 'idle',
    effective_status: 'idle',
    paused: false,
    max_concurrent_tasks: 1,
  } as Agent
}

function role(task_id: string, role_name: string, assignee_id: string) {
  return {
    id: `${task_id}-${role_name}`,
    task_id,
    role_name,
    assignee_type: 'agent',
    assignee_id,
    created_at: now,
    updated_at: now,
  }
}

function task(overrides: Partial<Task>): Task {
  return {
    id: 'task-1',
    project_id: 'project-1',
    parent_task_id: null,
    title: 'Task',
    task_type: 'task',
    status: 'todo',
    priority: 0,
    board_position: 0,
    role_assignments: [],
    effective_coder: null,
    effective_coder_source: null,
    effective_lead: null,
    remaining_retries: {},
    retry_limits: {},
    available_actions: [],
    condition: {
      kind: 'clear',
      details: {
        failure_kind: null,
        diagnostic: null,
        interruption: null,
        failed: false,
        blocked: false,
        human_wait: false,
        entry_wait: false,
      },
    },
    placement: null,
    workspace: null,
    placement_diagnostics: [],
    workflow_exception: null,
    execution_observability: { latest_execution_id: null },
    version: 1,
    created_at: now,
    updated_at: now,
    ...overrides,
  } as Task
}

function state(name: string, display_name: string, roleName: string | null) {
  return { name, display_name, column: display_name, kind: 'active', role: roleName, triggers: {} }
}

/** The inherited subtask workflow, as the Task detail returns it for a subtask. */
const subtaskWorkflow = {
  roles: [
    { name: 'coder', display_name: 'Coder', description: '' },
    { name: 'planner', display_name: 'Lead', description: '' },
  ],
  states: [
    state('todo', 'Todo', null),
    state('in_progress', 'In Progress', 'coder'),
    { ...state('lead_check', 'Lead Check', 'planner'), kind: 'gate', column: 'In Progress' },
    state('done', 'Done', null),
  ],
} as unknown as WorkflowDefinition

const rootWorkflow = {
  roles: [
    { name: 'coder', display_name: 'Coder', description: '' },
    { name: 'planner', display_name: 'Planner', description: '' },
    { name: 'reviewer', display_name: 'Reviewer', description: '' },
  ],
  states: [
    state('todo', 'Todo', null),
    { ...state('planning', 'Planning', 'planner'), kind: 'gate' },
    state('in_progress', 'In Progress', 'coder'),
    { ...state('review', 'Review', 'reviewer'), kind: 'gate' },
    state('done', 'Done', null),
  ],
} as unknown as WorkflowDefinition

function renderSidebar(value: Task, workflow: WorkflowDefinition) {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(
    <QueryClientProvider client={client}>
      <TaskDetailSidebar
        task={value}
        isLoading={false}
        workflow={workflow}
        agents={[]}
        executions={[]}
        availableTransitions={[]}
        transitionPending={false}
        priorityDraft="0"
        setPriorityDraft={() => {}}
        savePriority={() => {}}
        agentName={(id) => (id === 'lead-agent' ? 'Ada Lead' : id)}
        formatDate={(value) => value ?? ''}
        onStatusChange={() => {}}
      />
    </QueryClientProvider>,
  )
}

describe('the lead on the Task detail sidebar', () => {
  it('shows the step-check gate and the lead for a subtask in lead_check', () => {
    relations.mockReturnValue({ data: { parent: { id: 'root-1', title: 'Root' }, subtasks: [] } })
    agents.mockReturnValue([agent('lead-agent', 'Ada Lead', 'native')])
    renderSidebar(
      task({
        id: 'sub-1',
        parent_task_id: 'root-1',
        task_type: 'sub_task',
        status: 'lead_check',
        effective_lead: {
          assignment: role('root-1', 'planner', 'lead-agent'),
          source: 'inherited_from_root',
        },
      }),
      subtaskWorkflow,
    )

    const gate = screen.getByTestId('step-check')
    expect(within(gate).getByText('Lead Check')).not.toBeNull()
    expect(within(gate).getByText(/Ada Lead/)).not.toBeNull()
    // The role is held by the root: the subtask shows it, and offers no picker.
    expect(screen.queryByRole('button', { name: 'Assign Lead' })).toBeNull()
    expect(screen.getByTestId('inherited-lead').textContent).toContain('Ada Lead')
  })

  it('passes a subtask with no lead through lead_check without an error or a picker', () => {
    relations.mockReturnValue({ data: { parent: { id: 'root-1', title: 'Root' }, subtasks: [] } })
    agents.mockReturnValue([])
    renderSidebar(
      task({ id: 'sub-1', parent_task_id: 'root-1', status: 'lead_check', effective_lead: null }),
      subtaskWorkflow,
    )

    const gate = screen.getByTestId('step-check')
    expect(within(gate).getByText('Lead Check')).not.toBeNull()
    expect(gate.textContent).toContain('No lead')
    expect(screen.queryByRole('alert')).toBeNull()
    expect(screen.queryByRole('button', { name: 'Assign Lead' })).toBeNull()
    expect(screen.queryByTestId('inherited-lead')).toBeNull()
  })

  it('offers the Lead picker beside the default worker on a coordination root', () => {
    relations.mockReturnValue({
      data: { parent: null, subtasks: [{ id: 'sub-1', status: 'todo', parent_task_id: 'root-1' }] },
    })
    agents.mockReturnValue([
      agent('lead-agent', 'Ada Lead', 'native'),
      agent('cli-agent', 'Codex CLI', 'cli'),
    ])
    renderSidebar(
      task({
        id: 'root-1',
        status: 'in_progress',
        role_assignments: [role('root-1', 'planner', 'lead-agent')],
        effective_lead: { assignment: role('root-1', 'planner', 'lead-agent'), source: 'own' },
      }),
      rootWorkflow,
    )

    expect(screen.getByRole('button', { name: 'Assign Default worker' })).not.toBeNull()
    const lead = screen.getByRole('button', { name: 'Assign Lead' })
    expect(lead.textContent).toContain('Ada Lead')
    expect(screen.queryByRole('button', { name: 'Assign Planner' })).toBeNull()
  })

  it('keeps the plain planner picker on a Task without subtasks', () => {
    relations.mockReturnValue({ data: { parent: null, subtasks: [] } })
    agents.mockReturnValue([agent('cli-agent', 'Codex CLI', 'cli')])
    renderSidebar(task({ id: 'plain-1' }), rootWorkflow)

    expect(screen.getByRole('button', { name: 'Assign Planner' })).not.toBeNull()
    expect(screen.getByRole('button', { name: 'Assign Coder' })).not.toBeNull()
    expect(screen.queryByRole('button', { name: 'Assign Lead' })).toBeNull()
  })
})
