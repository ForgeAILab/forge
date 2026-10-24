import type { ComponentProps, ReactNode } from 'react'
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { useTaskDetailQuery, useUpdateTask } from '@/api/hooks'
import { qk } from '@/api/query-keys'
import { TaskOverviewPanel } from '@/pages/task-detail/TaskOverviewPanel'
import { taskListItem } from '@/test-utils/task-list-item'
import type { Task, TaskDetailResponse, UpdateTaskRequest } from '@/types/generated'
import { TaskDetailModal } from './task-detail-modal'
import { ApiError } from '@/api/client'
import { toast } from 'sonner'

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
      onUpdateDescription={(description, expectedTask) =>
        update.mutateAsync({
          taskId: task.id,
          body: { description, version: task.version },
          expectedTask,
        })
      }
    />
  )
}

let client: QueryClient
afterEach(() => {
  client?.clear()
  vi.restoreAllMocks()
})

type ConflictKind = 'unrelated-field' | 'description'

function setup(view: 'modal' | 'full page', options: { firstPatchConflict?: ConflictKind } = {}) {
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
  let conflictPending = options.firstPatchConflict !== undefined
  vi.spyOn(window, 'fetch').mockImplementation(async (input, init) => {
    const path = new URL(String(input)).pathname
    if (path === '/api/v1/tasks/task-1' && init?.method === 'PATCH') {
      const body: UpdateTaskRequest = JSON.parse(String(init.body))
      patches.push(body)
      if (conflictPending) {
        conflictPending = false
        stored = {
          ...stored,
          ...(options.firstPatchConflict === 'description'
            ? { description: 'Description from another client' }
            : { priority: stored.priority + 1 }),
          version: stored.version + 1,
        }
        return Response.json(
          {
            code: 'version_conflict',
            message: 'resource version conflict',
            details: null,
            request_id: 'request-conflict',
          },
          { status: 409 },
        )
      }
      // Match the current API contract: null/omitted means no change, "" clears.
      if (body.title != null) stored = { ...stored, title: body.title }
      if (body.description != null) stored = { ...stored, description: body.description }
      stored = { ...stored, version: stored.version + 1 }
      return Response.json(stored)
    }
    if (path === '/api/v1/tasks/task-1') return Response.json(stored)
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

describe('Full-page priority editing', () => {
  it('resets a draft on task navigation and only saves with the new task snapshot', async () => {
    const test = setup('full page')
    const first = test.stored()
    const second = { ...first, id: 'task-2', priority: 6, version: 11 }
    const save = vi.fn().mockResolvedValue(undefined)
    const props = { ...fullPageProps(first), onUpdatePriority: save }
    const view = render(<TaskOverviewPanel {...props} />, { wrapper: test.Wrapper })
    await waitFor(() =>
      expect((screen.getByLabelText('Priority') as HTMLInputElement).value).toBe(
        String(first.priority),
      ),
    )
    fireEvent.change(screen.getByLabelText('Priority'), { target: { value: '4' } })
    view.rerender(<TaskOverviewPanel {...props} task={second} />)
    await waitFor(() =>
      expect((screen.getByLabelText('Priority') as HTMLInputElement).value).toBe('6'),
    )
    fireEvent.blur(screen.getByLabelText('Priority'))
    expect(save).not.toHaveBeenCalled()
    fireEvent.change(screen.getByLabelText('Priority'), { target: { value: '7' } })
    fireEvent.blur(screen.getByLabelText('Priority'))
    await waitFor(() => expect(save).toHaveBeenCalledExactlyOnceWith(7, second))
  })

  it.each(['success', 'failure'] as const)(
    'ignores a previous task priority save %s after navigation',
    async (outcome) => {
      const test = setup('full page')
      const first = test.stored()
      const second = { ...first, id: 'task-2', priority: 6, version: 11 }
      let resolve!: () => void
      let reject!: (error: Error) => void
      const save = vi.fn(
        () =>
          new Promise<void>((yes, no) => {
            resolve = yes
            reject = no
          }),
      )
      const errorToast = vi.spyOn(toast, 'error')
      const props = { ...fullPageProps(first), onUpdatePriority: save }
      const view = render(<TaskOverviewPanel {...props} />, { wrapper: test.Wrapper })
      await waitFor(() =>
        expect((screen.getByLabelText('Priority') as HTMLInputElement).value).toBe(
          String(first.priority),
        ),
      )
      fireEvent.change(screen.getByLabelText('Priority'), { target: { value: '4' } })
      fireEvent.blur(screen.getByLabelText('Priority'))
      expect(save).toHaveBeenCalledExactlyOnceWith(4, first)
      view.rerender(<TaskOverviewPanel {...props} task={second} />)
      await act(async () => {
        await new Promise((resolve) => setTimeout(resolve, 20))
      })
      await act(async () => {
        if (outcome === 'success') resolve()
        else reject(new Error('Previous task failed'))
      })
      await waitFor(() =>
        expect((screen.getByLabelText('Priority') as HTMLInputElement).value).toBe('6'),
      )
      fireEvent.blur(screen.getByLabelText('Priority'))
      expect(save).toHaveBeenCalledTimes(1)
      expect(errorToast).not.toHaveBeenCalled()
    },
  )

  it('shows the queued notice instead of an error toast for a busy priority save', async () => {
    const test = setup('full page')
    const save = vi
      .fn()
      .mockRejectedValue(Object.assign(new ApiError('Queued', 409), { code: 'task_busy' }))
    const info = vi.spyOn(toast, 'info')
    const error = vi.spyOn(toast, 'error')
    render(<TaskOverviewPanel {...fullPageProps(test.stored())} onUpdatePriority={save} />, {
      wrapper: test.Wrapper,
    })
    await waitFor(() =>
      expect((screen.getByLabelText('Priority') as HTMLInputElement).value).toBe(
        String(test.stored().priority),
      ),
    )
    fireEvent.change(screen.getByLabelText('Priority'), { target: { value: '4' } })
    fireEvent.blur(screen.getByLabelText('Priority'))
    await waitFor(() =>
      expect(info).toHaveBeenCalledWith('Queued; it will apply after the current step'),
    )
    expect(error).not.toHaveBeenCalled()
  })

  it('keeps a priority draft and its edit-start snapshot across a detail refetch', async () => {
    const test = setup('full page')
    const task = test.stored()
    const props = fullPageProps(task)
    const save = vi.fn().mockResolvedValue(undefined)
    const view = render(<TaskOverviewPanel {...props} onUpdatePriority={save} />, {
      wrapper: test.Wrapper,
    })
    const input = screen.getByLabelText('Priority') as HTMLInputElement
    await waitFor(() => expect(input.value).toBe(String(task.priority)))
    fireEvent.change(input, { target: { value: '4' } })
    view.rerender(
      <TaskOverviewPanel
        {...props}
        task={{ ...task, version: task.version + 1, description: 'Another writer' }}
        onUpdatePriority={save}
      />,
    )
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 20))
    })
    expect(input.value).toBe('4')
    fireEvent.blur(input)
    await waitFor(() => expect(save).toHaveBeenCalledExactlyOnceWith(4, task))
  })
})

describe('Task detail title editing', () => {
  it('shows a successful title save', async () => {
    const test = setup('modal')
    render(test.ui, { wrapper: test.Wrapper })
    fireEvent.click(await screen.findByRole('button', { name: 'Notification schema' }))
    const input = screen.getByRole('textbox')
    fireEvent.change(input, { target: { value: 'Notification schema v2' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))

    await screen.findByRole('button', { name: 'Notification schema v2' })
    expect(test.patches).toEqual([{ title: 'Notification schema v2', version: 1 }])
  })

  it('retries a 409 when only another field changed', async () => {
    const test = setup('modal', { firstPatchConflict: 'unrelated-field' })
    render(test.ui, { wrapper: test.Wrapper })
    fireEvent.click(await screen.findByRole('button', { name: 'Notification schema' }))
    fireEvent.change(screen.getByRole('textbox'), {
      target: { value: 'Notification schema v2' },
    })
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))

    await screen.findByRole('button', { name: 'Notification schema v2' })
    expect(test.patches).toEqual([
      { title: 'Notification schema v2', version: 1 },
      { title: 'Notification schema v2', version: 2 },
    ])
    expect(test.stored().priority).toBe(1)
  })
})

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

describe('Task detail description conflicts', () => {
  it('keeps a cleared draft open and shows the current value after a same-field 409', async () => {
    const test = setup('modal', { firstPatchConflict: 'description' })
    render(test.ui, { wrapper: test.Wrapper })
    fireEvent.click(await screen.findByRole('button', { name: 'Original description' }))
    const editor = screen.getByRole('textbox', { name: 'Markdown editor' })
    fireEvent.change(editor, { target: { value: '' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))

    const alert = await screen.findByRole('alert')
    expect(alert.textContent).toContain('Description from another client')
    expect((editor as HTMLTextAreaElement).value).toBe('')
    expect(screen.queryByRole('button', { name: 'Click to add a description…' })).toBeNull()
    expect(test.patches).toEqual([{ description: '', version: 1 }])
  })

  it('preserves the draft when a live detail refresh changes the same field', async () => {
    const test = setup('modal')
    render(test.ui, { wrapper: test.Wrapper })
    fireEvent.click(await screen.findByRole('button', { name: 'Original description' }))
    const editor = screen.getByRole('textbox', { name: 'Markdown editor' })
    fireEvent.change(editor, { target: { value: '' } })

    act(() => {
      client.setQueryData<TaskDetailResponse>(qk.taskDetail('task-1'), (current) =>
        current
          ? {
              ...current,
              task: {
                ...current.task,
                description: 'Description received over the live refresh',
                version: current.task.version + 1,
              },
            }
          : current,
      )
    })

    await waitFor(() => expect((editor as HTMLTextAreaElement).value).toBe(''))
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))
    const alert = await screen.findByRole('alert')
    expect(alert.textContent).toContain('Description received over the live refresh')
    expect((editor as HTMLTextAreaElement).value).toBe('')
    expect(test.patches).toEqual([])
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
