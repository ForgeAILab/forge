import type { ReactNode } from 'react'
import { act, fireEvent, render, renderHook, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { TaskCreateDialog } from '@/components/task-create-dialog'
import { useBoardData } from '@/features/board/useBoardData'
import { taskListItem } from '@/test-utils/task-list-item'
import type { CreateTaskRequest } from '@/types/generated'
import { useCreateTask, useProjectOverviewQuery } from './hooks'
import { qk } from './query-keys'

vi.mock('@/components/ui/markdown-editor', () => ({
  MarkdownEditor: ({ value, onChange }: { value: string; onChange: (value: string) => void }) => (
    <textarea aria-label="Description" value={value} onChange={(e) => onChange(e.target.value)} />
  ),
}))

let client: QueryClient
afterEach(() => {
  client?.clear()
  vi.restoreAllMocks()
})

function setup() {
  let tasks = [
    ...Array.from({ length: 4 }, (_, index) =>
      taskListItem({ id: `todo-${index}`, title: `Existing ${index}` }),
    ),
    taskListItem({ id: 'backlog-1', status: 'backlog' }),
    taskListItem({ id: 'done-1', status: 'done' }),
  ]
  const posts: CreateTaskRequest[] = []
  const fetch = vi.spyOn(window, 'fetch').mockImplementation(async (input, init) => {
    const path = new URL(String(input)).pathname
    if (path === '/api/v1/projects/project-1/tasks' && init?.method === 'POST') {
      const body: CreateTaskRequest = JSON.parse(String(init.body))
      posts.push(body)
      const task = taskListItem({
        id: 'created-task',
        title: body.title,
        task_type: body.task_type ?? 'task',
      })
      tasks = [...tasks, task]
      return Response.json(task)
    }
    if (path === '/api/v1/projects/project-1/tasks') {
      return Response.json({
        items: tasks,
        has_more: false,
        next_cursor: null,
        total_count: tasks.length,
        board_revision: posts.length + 1,
      })
    }
    if (path === '/api/v1/projects/project-1/overview') {
      // Overview's Backlog metric includes Todo (project_overview::classify_status).
      return Response.json({
        task_counts: {
          total: tasks.length,
          backlog: tasks.filter((task) => ['todo', 'backlog'].includes(task.status)).length,
        },
      })
    }
    throw new Error(`Unexpected test request: ${path}`)
  })
  // Fresh cached queries only refresh if the create mutation invalidates them.
  client = new QueryClient({
    defaultOptions: { queries: { retry: false, staleTime: Infinity }, mutations: { retry: false } },
  })
  function Wrapper({ children }: { children: ReactNode }) {
    return <QueryClientProvider client={client}>{children}</QueryClientProvider>
  }
  return { posts, fetch, Wrapper }
}

describe('task creation and counts without SSE', () => {
  it.each(['task', 'planning_task'] as const)(
    'creates one %s from the board dialog and refreshes board and Overview',
    async (taskType) => {
      const test = setup()
      const { result } = renderHook(
        () => ({
          board: useBoardData('project-1', { limit: 200 }),
          overview: useProjectOverviewQuery('project-1'),
        }),
        { wrapper: test.Wrapper },
      )
      await waitFor(() => expect(result.current.board.snapshot.tasks).toHaveLength(6))
      await waitFor(() => expect(result.current.overview.data?.task_counts.total).toBe(6))
      const beforeTodo = result.current.board.snapshot.tasks.filter(
        (task) => task.status === 'todo',
      ).length
      const beforeCounts = result.current.overview.data!.task_counts
      const onCreated = vi.fn()
      const onOpenChange = vi.fn()
      render(
        <TaskCreateDialog
          open
          projectId="project-1"
          onCreated={onCreated}
          onOpenChange={onOpenChange}
        />,
        { wrapper: test.Wrapper },
      )

      fireEvent.change(screen.getByRole('textbox', { name: 'Title' }), {
        target: { value: 'TSK board create' },
      })
      fireEvent.click(screen.getByRole('button', { name: 'Type' }))
      if (taskType === 'planning_task') {
        fireEvent.click(screen.getByRole('option', { name: 'Planning Task' }))
      } else {
        fireEvent.click(screen.getByRole('option', { name: 'Task' }))
      }
      fireEvent.click(screen.getByRole('button', { name: 'Create Task' }))

      await waitFor(() => expect(result.current.board.snapshot.tasks).toHaveLength(7))
      await waitFor(() =>
        expect(result.current.overview.data?.task_counts.total).toBe(
          Number(beforeCounts.total) + 1,
        ),
      )
      expect(result.current.overview.data?.task_counts.backlog).toBe(
        Number(beforeCounts.backlog) + 1,
      )
      const todo = result.current.board.snapshot.tasks.filter((task) => task.status === 'todo')
      expect(todo).toHaveLength(beforeTodo + 1)
      expect(todo.filter((task) => task.id === 'created-task')).toEqual([
        expect.objectContaining({ title: 'TSK board create', task_type: taskType }),
      ])
      expect(new Set(result.current.board.snapshot.tasks.map((task) => task.id)).size).toBe(7)
      expect(test.posts).toEqual([{ title: 'TSK board create', task_type: taskType }])
      expect(onCreated).toHaveBeenCalledTimes(1)
      expect(onOpenChange).toHaveBeenCalledExactlyOnceWith(false)
    },
  )

  it('refreshes a previously visited Overview when reopened after a quick create', async () => {
    const test = setup()
    const overview = renderHook(() => useProjectOverviewQuery('project-1'), {
      wrapper: test.Wrapper,
    })
    await waitFor(() => expect(overview.result.current.data?.task_counts.total).toBe(6))
    overview.unmount()
    const create = renderHook(() => useCreateTask('project-1'), { wrapper: test.Wrapper })
    await act(async () => {
      await create.result.current.mutateAsync({
        title: 'QA: overview count test',
        description: 'Quick create',
        task_type: 'task',
        priority: 0,
      })
    })
    expect(client.getQueryState(qk.projectOverview('project-1'))?.isInvalidated).toBe(true)
    const reopened = renderHook(() => useProjectOverviewQuery('project-1'), {
      wrapper: test.Wrapper,
    })
    await waitFor(() => expect(reopened.result.current.data?.task_counts.total).toBe(7))
    expect(reopened.result.current.data?.task_counts.backlog).toBe(6)
    expect(test.posts).toHaveLength(1)
  })
})
