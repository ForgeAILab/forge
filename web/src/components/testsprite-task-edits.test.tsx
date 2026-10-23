import type { ReactNode } from 'react'
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { TaskDetailModal } from './task-detail-modal'
import { useBoardData } from '@/features/board/useBoardData'
import { taskListItem } from '@/test-utils/task-list-item'
import type { Task, TaskDetailResponse, UpdateTaskRequest } from '@/types/generated'
import { qk } from '@/api/query-keys'

vi.mock('@tanstack/react-router', () => ({ useNavigate: () => vi.fn() }))
vi.mock('@/api/hooks', async (original) => ({
  ...(await original<typeof import('@/api/hooks')>()),
  useAgentsQuery: () => ({ data: { items: [] } }),
  useReviewsQuery: () => ({ data: [] }),
}))
vi.mock('@/components/ui/markdown-editor', () => ({
  MarkdownEditor: () => null,
  MarkdownView: ({ content }: { content: string }) => <p>{content}</p>,
}))
vi.mock('@/components/task-detail/task-detail-sidebar', () => ({
  TaskDetailSidebar: ({
    priorityDraft,
    setPriorityDraft,
    savePriority,
  }: {
    priorityDraft: string
    setPriorityDraft: (value: string) => void
    savePriority: () => void
  }) => (
    <input
      aria-label="Priority"
      value={priorityDraft}
      onChange={(event) => setPriorityDraft(event.target.value)}
      onBlur={savePriority}
    />
  ),
}))
vi.mock('@/components/task-detail/task-executions-panel', () => ({
  TaskExecutionsPanel: () => null,
  reviewStatusColors: {},
}))
vi.mock('@/components/task-detail/task-subtasks-panel', () => ({ TaskSubtasksPanel: () => null }))
vi.mock('@/components/task-detail/task-dependencies-panel', () => ({
  TaskDependenciesPanel: () => null,
}))
vi.mock('@/components/task-detail/task-history-panel', () => ({ TaskHistoryPanel: () => null }))
vi.mock('@/components/task-detail/workflow-exception-panel', () => ({
  WorkflowExceptionPanel: () => null,
}))

let client: QueryClient
afterEach(() => {
  client?.clear()
  vi.restoreAllMocks()
})

function setup(options: { holdPriority?: boolean; priorityError?: boolean } = {}) {
  let releasePriority!: () => void
  const priorityHeld = new Promise<void>((resolve) => {
    releasePriority = resolve
  })
  let stored: Task = {
    ...taskListItem({ title: 'Set up CI pipeline', priority: 3 }),
    description: 'Original description',
    effective_coder: null,
    effective_coder_source: null,
    placement: null,
    execution_observability: undefined,
  }
  const patches: UpdateTaskRequest[] = []
  vi.spyOn(window, 'fetch').mockImplementation(async (input, init) => {
    const path = new URL(String(input)).pathname
    if (path.endsWith('/tasks/task-1') && init?.method === 'PATCH') {
      const body: UpdateTaskRequest = JSON.parse(String(init.body))
      patches.push(body)
      if (body.priority !== undefined && options.priorityError) {
        await priorityHeld
        return Response.json({ message: 'Priority unavailable' }, { status: 503 })
      }
      if (body.version !== stored.version)
        return Response.json(
          { code: 'version_conflict', message: 'resource version conflict' },
          { status: 409 },
        )
      stored = {
        ...stored,
        ...body,
        description: body.description ?? stored.description,
        version: stored.version + 1,
      }
      const response = Response.json(stored)
      if (body.priority !== undefined && options.holdPriority) await priorityHeld
      return response
    }
    if (path.endsWith('/tasks/task-1')) return Response.json(stored)
    if (path.endsWith('/tasks/task-1/detail'))
      return Response.json({ task: stored, workflow: null, executions: { items: [] } })
    if (path.endsWith('/projects/project-1/tasks'))
      return Response.json({
        items: [stored],
        board_revision: stored.version,
        has_more: false,
        next_cursor: null,
      })
    throw new Error(`Unexpected request: ${path}`)
  })
  client = new QueryClient({
    defaultOptions: { queries: { retry: false, staleTime: Infinity }, mutations: { retry: false } },
  })
  function BoardCard() {
    const board = useBoardData('project-1', {})
    const task = board.snapshot.tasks[0]
    return (
      <div data-testid="board-card">
        {task?.title} P{task?.priority}
      </div>
    )
  }
  function Wrapper({ children }: { children: ReactNode }) {
    return <QueryClientProvider client={client}>{children}</QueryClientProvider>
  }
  const onClose = vi.fn()
  render(
    <>
      <BoardCard />
      <TaskDetailModal taskId="task-1" open onClose={onClose} />
    </>,
    { wrapper: Wrapper },
  )
  return {
    patches,
    releasePriority,
    onClose,
    stored: () => stored,
    secondClient: (changes: Partial<Task>) => {
      stored = { ...stored, ...changes, version: stored.version + 1 }
    },
  }
}

describe('TestSprite task edits', () => {
  it('clears the priority edit base when a title mutation overlaps its pending save', async () => {
    const test = setup({ holdPriority: true })
    const input = (await screen.findByRole('textbox', { name: 'Priority' })) as HTMLInputElement
    await waitFor(() => expect(input.value).toBe('3'))
    fireEvent.change(input, { target: { value: '4' } })
    fireEvent.blur(input)
    await waitFor(() => expect(test.patches).toHaveLength(1))
    fireEvent.click(screen.getByRole('button', { name: 'Set up CI pipeline' }))
    const title = screen.getByRole('textbox', { name: '' })
    fireEvent.change(title, { target: { value: 'Title saved while priority is pending' } })
    fireEvent.keyDown(title, { key: 'Enter' })
    await screen.findByRole('button', { name: 'Title saved while priority is pending' })
    await act(async () => {
      test.releasePriority()
    })
    await waitFor(() => expect(client.isMutating()).toBe(0))
    test.secondClient({ priority: 8, description: 'Latest cached priority' })
    act(() =>
      client.setQueryData<TaskDetailResponse>(qk.taskDetail('task-1'), (detail) =>
        detail ? { ...detail, task: test.stored() } : detail,
      ),
    )
    await waitFor(() => expect(input.value).toBe('8'))
    fireEvent.change(input, { target: { value: '9' } })
    fireEvent.blur(input)
    await waitFor(() => expect(test.stored().priority).toBe(9))
  })

  it('restores the latest cached priority after a pending save fails', async () => {
    const test = setup({ priorityError: true })
    const input = (await screen.findByRole('textbox', { name: 'Priority' })) as HTMLInputElement
    await waitFor(() => expect(input.value).toBe('3'))
    fireEvent.change(input, { target: { value: '4' } })
    fireEvent.blur(input)
    await waitFor(() => expect(test.patches).toHaveLength(1))
    test.secondClient({ priority: 8, description: 'Latest cached priority' })
    await act(async () => {
      client.setQueryData<TaskDetailResponse>(qk.taskDetail('task-1'), (detail) =>
        detail ? { ...detail, task: test.stored() } : detail,
      )
    })
    await screen.findByText('Latest cached priority')
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 20))
    })
    expect(input.value).toBe('4')
    await act(async () => {
      test.releasePriority()
    })
    await waitFor(() => expect(client.isMutating()).toBe(0))
    await waitFor(() => expect(input.value).toBe('8'))
  })

  it('Escape refreshes detail and task lists without invalidating other task panels', async () => {
    setup()
    const otherKeys = [
      qk.taskDiff('task-1'),
      qk.reviews('task-1'),
      qk.comments('task-1'),
      qk.taskMedia('task-1'),
    ]
    for (const key of otherKeys) client.setQueryData(key, [])
    fireEvent.click(await screen.findByRole('button', { name: 'Set up CI pipeline' }))
    const invalidate = vi.spyOn(client, 'invalidateQueries')
    fireEvent.keyDown(screen.getByRole('textbox', { name: '' }), { key: 'Escape' })
    expect(invalidate).toHaveBeenCalledWith({ queryKey: qk.taskDetail('task-1'), exact: true })
    expect(invalidate).toHaveBeenCalledWith({ queryKey: qk.projectTasks('project-1') })
    for (const key of otherKeys) expect(client.getQueryState(key)?.isInvalidated).toBe(false)
  })

  it('consumes Escape while cancelling an edit so outer dialog handlers cannot close it', async () => {
    setup()
    fireEvent.click(await screen.findByRole('button', { name: 'Set up CI pipeline' }))
    const input = screen.getByRole('textbox', { name: '' })
    fireEvent.change(input, { target: { value: 'Unsaved title' } })
    const outerEscape = vi.fn()
    document.addEventListener('keydown', outerEscape)
    try {
      fireEvent.keyDown(input, { key: 'Escape' })
      expect(outerEscape).not.toHaveBeenCalled()
    } finally {
      document.removeEventListener('keydown', outerEscape)
    }
  })

  it.each([false, true])(
    'Escape discards the draft and displays stored title in detail and card (concurrent=%s)',
    async (concurrent) => {
      const test = setup()
      fireEvent.click(await screen.findByRole('button', { name: 'Set up CI pipeline' }))
      const input = screen.getByRole('textbox', { name: '' })
      fireEvent.change(input, { target: { value: 'Unsaved title' } })
      if (concurrent) test.secondClient({ title: 'Title saved by second client' })
      fireEvent.keyDown(input, { key: 'Escape' })
      expect(test.onClose).not.toHaveBeenCalled()
      await screen.findByRole('button', { name: test.stored().title })
      await waitFor(() =>
        expect(screen.getByTestId('board-card').textContent).toContain(test.stored().title),
      )
      expect(test.patches).toEqual([])
    },
  )

  it.each([false, true])(
    'saves priority to detail and board, retrying an unrelated writer once (concurrent=%s)',
    async (concurrent) => {
      const test = setup()
      const input = await screen.findByRole('textbox', { name: 'Priority' })
      await waitFor(() => expect((input as HTMLInputElement).value).toBe('3'))
      fireEvent.change(input, { target: { value: '4' } })
      if (concurrent) test.secondClient({ description: 'Description saved by second client' })
      fireEvent.blur(input)
      await waitFor(() => expect(test.stored().priority).toBe(4))
      await waitFor(() => expect(screen.getByTestId('board-card').textContent).toContain('P4'))
      expect(test.patches.map((body) => body.version)).toEqual(concurrent ? [1, 2] : [1])
      if (concurrent) expect(test.stored().description).toBe('Description saved by second client')
      await act(async () => {})
    },
  )

  it('preserves a priority draft across a detail refetch from another writer', async () => {
    const test = setup()
    const input = await screen.findByRole('textbox', { name: 'Priority' })
    await waitFor(() => expect((input as HTMLInputElement).value).toBe('3'))
    fireEvent.change(input, { target: { value: '4' } })
    test.secondClient({ description: 'Description from another client' })
    await act(async () => {
      client.setQueryData(qk.taskDetail('task-1'), {
        task: test.stored(),
        workflow: null,
        executions: { items: [] },
      })
    })
    await screen.findByText('Description from another client')
    await act(async () => {
      await new Promise((resolve) => setTimeout(resolve, 20))
    })
    expect((input as HTMLInputElement).value).toBe('4')
    fireEvent.blur(input)
    await waitFor(() => expect(test.stored().priority).toBe(4))
  })

  it('does not overwrite another client changing the same priority', async () => {
    const test = setup()
    const input = await screen.findByRole('textbox', { name: 'Priority' })
    await waitFor(() => expect((input as HTMLInputElement).value).toBe('3'))
    fireEvent.change(input, { target: { value: '4' } })
    test.secondClient({ priority: 5 })
    fireEvent.blur(input)
    await waitFor(() => expect((input as HTMLInputElement).value).toBe('5'))
    expect(test.stored().priority).toBe(5)
    expect(test.patches.map((body) => body.version)).toEqual([1])
    await waitFor(() => expect(screen.getByTestId('board-card').textContent).toContain('P5'))
  })
})
