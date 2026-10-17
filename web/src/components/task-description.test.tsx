import type { ComponentProps, ReactNode } from 'react'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { useTaskDetailQuery, useUpdateTask } from '@/api/hooks'
import { TaskOverviewPanel } from '@/pages/task-detail/TaskOverviewPanel'
import { taskListItem } from '@/test-utils/task-list-item'
import type { Task, UpdateTaskRequest } from '@/types/generated'
import { TaskDetailModal } from './task-detail-modal'

vi.mock('@tanstack/react-router', () => ({ useNavigate: () => vi.fn() }))
vi.mock('@/api/hooks', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/hooks')>()),
  useAgentsQuery: () => ({ data: { items: [] } }),
  useMembersQuery: () => ({ data: [] }),
  useProjectAgentsQuery: () => ({ data: [] }),
  useReviewsQuery: () => ({ data: [] }),
}))
vi.mock('@/components/ui/markdown-editor', () => ({
  MarkdownEditor: ({ value, onChange }: { value: string; onChange: (value: string) => void }) => (
    <textarea
      aria-label="Markdown editor"
      value={value}
      onChange={(e) => onChange(e.target.value)}
    />
  ),
  MarkdownView: ({ content }: { content: string }) => <p>{content}</p>,
}))
vi.mock('@/components/task-detail/task-detail-sidebar', () => ({ TaskDetailSidebar: () => null }))
vi.mock('@/components/task-detail/task-executions-panel', () => ({
  TaskExecutionsPanel: () => null,
  reviewStatusColors: {},
}))
vi.mock('@/components/task-detail/task-subtasks-panel', () => ({ TaskSubtasksPanel: () => null }))
vi.mock('@/components/task-detail/task-dependencies-panel', () => ({
  TaskDependenciesPanel: () => null,
}))
vi.mock('@/components/task-detail/task-history-panel', () => ({ TaskHistoryPanel: () => null }))
vi.mock('@/components/task-detail/task-action-buttons', () => ({ TaskActionButtons: () => null }))
vi.mock('@/components/task-detail/task-role-assignees', () => ({
  TaskRoleAssignees: () => null,
  TaskStepCheck: () => null,
}))
vi.mock('@/components/task-detail/task-external-links', () => ({ TaskExternalLinks: () => null }))
vi.mock('@/components/task-detail/workflow-exception-panel', () => ({
  WorkflowExceptionPanel: () => null,
}))
vi.mock('@/components/task-execution-observability', () => ({
  TaskExecutionObservabilityPanel: () => null,
}))

function fullPageProps(task: Task): ComponentProps<typeof TaskOverviewPanel> {
  return {
    task,
    isLoading: false,
    isError: false,
    error: null,
    onRetryLoad: vi.fn(),
    updatePending: false,
    transitionPending: false,
    rolePickerPending: false,
    duplicatePending: false,
    errorInfo: undefined,
    availableTransitions: [],
    managedStatusDisabledReason: undefined,
    reviewDisabledReason: undefined,
    terminal: false,
    currentRole: null,
    assignableRoles: [],
    coderRole: 'coder',
    isCoordinationRoot: false,
    workflow: undefined,
    agents: [],
    runSuffix: '',
    workflowRetryBudgets: undefined,
    agentName: () => undefined,
    onUpdateTitle: vi.fn(),
    onUpdateDescription: vi.fn(),
    onUpdatePriority: vi.fn(),
    onStatusChange: vi.fn(),
    onAssigneeChange: vi.fn(),
    onDuplicateTask: vi.fn(),
    onSaveRetryBudgets: vi.fn(),
  }
}

function FullPage() {
  const detail = useTaskDetailQuery('task-1')
  const update = useUpdateTask()
  if (!detail.data) return null
  const task = detail.data.task
  return (
    <TaskOverviewPanel
      {...fullPageProps(task)}
      onUpdateDescription={(description) =>
        update.mutate({ taskId: task.id, body: { description, version: task.version } })
      }
    />
  )
}

let client: QueryClient
afterEach(() => {
  client?.clear()
  vi.restoreAllMocks()
})

function setup(view: 'modal' | 'full page') {
  let stored: Task = {
    ...taskListItem(),
    title: 'Notification schema',
    description: 'Original description',
    effective_coder: null,
    effective_coder_source: null,
    placement: null,
    execution_observability: undefined,
  }
  const patches: UpdateTaskRequest[] = []
  vi.spyOn(window, 'fetch').mockImplementation(async (input, init) => {
    const path = new URL(String(input)).pathname
    if (path === '/api/v1/tasks/task-1' && init?.method === 'PATCH') {
      const body: UpdateTaskRequest = JSON.parse(String(init.body))
      patches.push(body)
      // Match the current API contract: null/omitted means no change, "" clears.
      if (body.description != null) stored = { ...stored, description: body.description }
      stored = { ...stored, version: stored.version + 1 }
      return Response.json(stored)
    }
    if (path === '/api/v1/tasks/task-1/detail') {
      return Response.json({
        task: stored,
        workflow: null,
        executions: { items: [], has_more: false, next_cursor: null, total_count: 0 },
      })
    }
    throw new Error(`Unexpected test request: ${path}`)
  })
  client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  })
  function Wrapper({ children }: { children: ReactNode }) {
    return <QueryClientProvider client={client}>{children}</QueryClientProvider>
  }
  const ui =
    view === 'modal' ? <TaskDetailModal taskId="task-1" open onClose={vi.fn()} /> : <FullPage />
  return { ui, Wrapper, patches, stored: () => stored }
}

describe.each(['modal', 'full page'] as const)('Task description in %s', (view) => {
  it.each(['', '   '])(
    'persists a cleared description (%j) and restores the empty prompt after remount',
    async (draft) => {
      const test = setup(view)
      const rendered = render(test.ui, { wrapper: test.Wrapper })
      fireEvent.click(await screen.findByRole('button', { name: 'Original description' }))
      const editor = screen.getByRole('textbox', { name: 'Markdown editor' })
      await waitFor(() =>
        expect((editor as HTMLTextAreaElement).value).toBe('Original description'),
      )
      fireEvent.change(editor, { target: { value: draft } })
      fireEvent.click(screen.getByRole('button', { name: 'Save' }))

      await screen.findByRole('button', { name: 'Click to add a description…' })
      expect(test.patches).toEqual([{ description: '', version: 1 }])
      expect(test.stored().description).toBe('')
      expect(screen.queryByText('Original description')).toBeNull()

      rendered.unmount()
      client.clear()
      render(test.ui, { wrapper: test.Wrapper })
      fireEvent.click(await screen.findByRole('button', { name: 'Click to add a description…' }))
      await waitFor(() =>
        expect(
          (screen.getByRole('textbox', { name: 'Markdown editor' }) as HTMLTextAreaElement).value,
        ).toBe(''),
      )
    },
  )

  it('saves replacement text and preserves it when a later edit is cancelled', async () => {
    const test = setup(view)
    render(test.ui, { wrapper: test.Wrapper })
    fireEvent.click(await screen.findByRole('button', { name: 'Original description' }))
    const editor = screen.getByRole('textbox', { name: 'Markdown editor' })
    await waitFor(() => expect((editor as HTMLTextAreaElement).value).toBe('Original description'))
    fireEvent.change(editor, { target: { value: '  Updated description  ' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))
    fireEvent.click(await screen.findByRole('button', { name: 'Updated description' }))
    fireEvent.change(screen.getByRole('textbox', { name: 'Markdown editor' }), {
      target: { value: '' },
    })
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }))
    expect(screen.getByRole('button', { name: 'Updated description' })).toBeTruthy()
    expect(test.patches).toEqual([{ description: 'Updated description', version: 1 }])
  })
})

describe('full-page Task type', () => {
  it.each([
    ['task', null],
    ['planning_task', 'Planning Task'],
    ['discovery', 'Discovery'],
    ['sub_task', 'Sub Task'],
  ] as const)('renders the %s type beside the title', (taskType, label) => {
    const task: Task = {
      ...taskListItem({ task_type: taskType }),
      effective_coder: null,
      effective_coder_source: null,
      placement: null,
      execution_observability: undefined,
    }
    render(<TaskOverviewPanel {...fullPageProps(task)} />)
    if (label) {
      expect(screen.getByText('Task type:')).toBeTruthy()
      expect(screen.getByText(label)).toBeTruthy()
    } else {
      expect(screen.queryByText('Task type:')).toBeNull()
    }
  })
})
