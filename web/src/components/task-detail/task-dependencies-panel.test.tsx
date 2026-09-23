import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { TaskDependenciesPanel } from './task-dependencies-panel'
import { TaskSubtasksPanel } from './task-subtasks-panel'
import { useAuthStore } from '@/stores/auth'
import type { Execution, Task } from '@/types/generated'

vi.mock('@tanstack/react-router', () => ({ useNavigate: () => vi.fn() }))

const task: Task = {
  id: 'task-1',
  project_id: 'project-1',
  title: 'Current task',
  task_type: 'task',
  status: 'todo',
  priority: 0,
  board_position: 0,
  role_assignments: [],
  remaining_retries: {},
  version: 1,
  created_at: '2026-01-01T00:00:00Z',
  updated_at: '2026-01-01T00:00:00Z',
}

const candidate: Task = {
  ...task,
  id: 'task-2',
  title: 'Fix request retries',
}

function renderPanels() {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: 0 },
      mutations: { retry: false },
    },
  })

  return render(
    <QueryClientProvider client={queryClient}>
      <TaskDependenciesPanel task={task} />
      <TaskSubtasksPanel task={task} executions={[] as Execution[]} />
    </QueryClientProvider>,
  )
}

describe('task relationship panels', () => {
  afterEach(() => {
    cleanup()
    vi.restoreAllMocks()
    useAuthStore.getState().clearAuth()
    localStorage.clear()
  })

  it('shares relationship data and only fetches bounded picker candidates after opening it', async () => {
    useAuthStore.setState({ accessToken: 'access-token', refreshToken: 'refresh-token' })
    const fetchMock = vi.spyOn(window, 'fetch').mockImplementation(async (input) => {
      const url = new URL(input instanceof URL ? input.href : String(input))

      if (url.pathname === '/api/v1/tasks/task-1/relations') {
        return new Response(
          JSON.stringify({
            parent: null,
            subtasks: [],
            dependencies: [],
            missing_dependency_ids: ['missing-task'],
            dependents: [],
          }),
          { status: 200, headers: { 'content-type': 'application/json' } },
        )
      }

      if (url.pathname === '/api/v1/projects/project-1/tasks') {
        return new Response(
          JSON.stringify({
            items: [candidate],
            next_cursor: null,
            has_more: false,
            total_count: null,
            board_revision: 1,
          }),
          { status: 200, headers: { 'content-type': 'application/json' } },
        )
      }

      throw new Error(`Unexpected request: ${url}`)
    })

    renderPanels()
    await screen.findByRole('button', { name: 'Remove dependency on missing-task' })
    await screen.findByText('No subtasks')

    expect(fetchMock).toHaveBeenCalledTimes(1)
    expect(new URL(String(fetchMock.mock.calls[0][0])).pathname).toBe(
      '/api/v1/tasks/task-1/relations',
    )

    fireEvent.click(screen.getByRole('button', { name: 'Add dependency' }))
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2))

    const candidateRequest = new URL(String(fetchMock.mock.calls[1][0]))
    expect(candidateRequest.pathname).toBe('/api/v1/projects/project-1/tasks')
    expect(candidateRequest.searchParams.get('limit')).toBe('30')
    expect(candidateRequest.searchParams.has('q')).toBe(false)

    const searchInput = screen.getByPlaceholderText('Search task titles or descriptions…')
    fireEvent.change(searchInput, { target: { value: 'f' } })
    fireEvent.change(searchInput, { target: { value: 'fi' } })
    fireEvent.change(searchInput, { target: { value: 'fix' } })

    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(3), { timeout: 2_000 })
    const searchRequest = new URL(String(fetchMock.mock.calls[2][0]))
    expect(searchRequest.searchParams.get('q')).toBe('fix')
    expect(searchRequest.searchParams.get('limit')).toBe('30')
    expect(await screen.findByText('Fix request retries')).toBeTruthy()
  })
})
