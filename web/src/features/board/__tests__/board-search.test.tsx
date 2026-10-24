import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import {
  createMemoryHistory,
  createRootRoute,
  createRoute,
  createRouter,
  Outlet,
  RouterProvider,
} from '@tanstack/react-router'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { qk } from '@/api/query-keys'
import { routeSsePayload } from '@/api/sse'
import { useFilterStore } from '@/stores/filters'
import { taskListItem } from '@/test-utils/task-list-item'
import { BoardToolbar } from '../BoardToolbar'
import { useBoardPageController } from '../useBoardPageController'

vi.mock('@/api/hooks', () => ({
  useAgentsQuery: () => ({ data: { items: [] } }),
  useAssignRole: () => ({}),
  useCreateTask: () => ({}),
  useTransitionTask: () => ({}),
  useWorkflowQuery: () => ({}),
  useTaskDetailQuery: () => ({}),
}))

function SearchBoard() {
  const board = useBoardPageController('project-1')
  return (
    <>
      <BoardToolbar
        agents={[]}
        selectedAgentIds={board.filterAgentIds}
        q={board.filterQ}
        blockedOnly={board.filterBlockedOnly}
        includeCancelled={board.filterIncludeCancelled}
        includeArchived={board.filterIncludeArchived}
        showMobileFilters={board.showMobileFilters}
        searchInputRef={board.searchInputRef}
        onToggleMobileFilters={() => board.setShowMobileFilters((visible) => !visible)}
        onFilterChange={board.setUrlFilters}
        onNewTask={() => {}}
      />
      <ul aria-label="Rendered tasks">
        {board.tasks.map((task) => (
          <li key={task.id}>{task.title}</li>
        ))}
      </ul>
    </>
  )
}

let client: QueryClient
afterEach(() => {
  client?.clear()
  useFilterStore.getState().resetFilters()
  vi.restoreAllMocks()
})

async function setup() {
  vi.spyOn(window, 'scrollTo').mockImplementation(() => {})
  let tasks = [
    taskListItem({ id: 'socket', title: 'Implement WebSocket events' }),
    taskListItem({ id: 'ci', title: 'Set up CI pipeline' }),
  ]
  let holdSearch = false
  let holdFull = false
  let release = () => {}
  let captured = () => {}
  const capturedSearch = new Promise<void>((resolve) => {
    captured = resolve
  })
  let capturedFullRead = () => {}
  const capturedFull = new Promise<void>((resolve) => {
    capturedFullRead = resolve
  })
  const held = new Promise<void>((resolve) => {
    release = resolve
  })
  vi.spyOn(window, 'fetch').mockImplementation(async (input) => {
    const url = new URL(String(input))
    expect(url.pathname).toBe('/api/v1/projects/project-1/tasks')
    const q = url.searchParams.get('q') ?? ''
    const response = Response.json({
      items: tasks.filter((task) => task.title.toLowerCase().includes(q.toLowerCase())),
      has_more: false,
      next_cursor: null,
      board_revision: tasks.length,
    })
    if (holdSearch && q) {
      holdSearch = false
      captured()
      await held
    }
    if (holdFull && !q) {
      holdFull = false
      capturedFullRead()
      await held
    }
    return response
  })
  client = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } })
  const root = createRootRoute({ component: Outlet })
  const board = createRoute({
    getParentRoute: () => root,
    path: '/projects/$projectId/board',
    validateSearch: (search: Record<string, unknown>) => ({
      q: typeof search.q === 'string' ? search.q : undefined,
    }),
    component: SearchBoard,
  })
  const router = createRouter({
    routeTree: root.addChildren([board]),
    history: createMemoryHistory({ initialEntries: ['/projects/project-1/board'] }),
  })
  render(
    <QueryClientProvider client={client}>
      <RouterProvider router={router} />
    </QueryClientProvider>,
  )
  await screen.findByText('Set up CI pipeline')
  const event = (event_type: string, entity_id: string) =>
    routeSsePayload(
      { event_type, entity_id, project_id: 'project-1', timestamp: new Date().toISOString() },
      client,
      { dispatch: vi.fn() },
    )
  return {
    router,
    capturedSearch,
    capturedFull,
    release,
    hold: () => {
      holdSearch = true
      holdFull = true
    },
    create: () => {
      tasks = [...tasks, taskListItem({ id: 'writer', title: 'Second client task' })]
      event('task.created', 'writer')
    },
    edit: () => {
      tasks = tasks.map((task) =>
        task.id === 'ci' ? { ...task, title: 'CI edited by second client', version: 2 } : task,
      )
      event('task.updated', 'ci')
    },
  }
}

describe('KBD-001 board search', () => {
  it.each(['input', 'Clear all'] as const)(
    'restores cached full-board rows using %s',
    async (clear) => {
      const test = await setup()
      const field = screen.getByRole('textbox', { name: 'Search board tasks' })
      fireEvent.change(field, { target: { value: 'WebSocket' } })
      await waitFor(() => {
        expect(screen.getAllByRole('listitem')).toHaveLength(1)
        expect(screen.getByText('Implement WebSocket events')).toBeTruthy()
      })
      if (clear === 'input') fireEvent.change(field, { target: { value: '' } })
      else {
        fireEvent.click(screen.getByRole('button', { name: 'Filters' }))
        fireEvent.click(screen.getByRole('button', { name: 'Clear all' }))
      }
      await screen.findByText('Set up CI pipeline')
      expect((field as HTMLInputElement).value).toBe('')
      expect(useFilterStore.getState().q).toBe('')
      await waitFor(() => expect(test.router.state.location.search.q).toBeUndefined())
      const queries = client.getQueryCache().findAll({ queryKey: qk.projectTasks('project-1') })
      expect(queries.filter((query) => query.isActive()).map((query) => query.queryKey[3])).toEqual(
        ['board:[["limit",200]]'],
      )
      expect(screen.getAllByRole('listitem')).toHaveLength(2)
    },
  )

  it('keeps the full board after clearing across an older search read and create/edit events', async () => {
    const test = await setup()
    const field = screen.getByRole('textbox', { name: 'Search board tasks' })
    fireEvent.change(field, { target: { value: 'WebSocket' } })
    await waitFor(() => {
      expect(screen.getAllByRole('listitem')).toHaveLength(1)
      expect(screen.getByText('Implement WebSocket events')).toBeTruthy()
    })
    test.hold()
    act(test.create)
    await test.capturedSearch
    fireEvent.change(field, { target: { value: '' } })
    await test.capturedFull
    // Deliver the edit beyond the SSE throttle while the older full read is held.
    vi.spyOn(Date, 'now').mockReturnValue(Date.now() + 2000)
    act(test.edit)
    await act(async () => {
      test.release()
    })
    await screen.findByText('CI edited by second client', {}, { timeout: 3000 })
    await screen.findByText('Second client task')
    expect(screen.getAllByRole('listitem')).toHaveLength(3)
    expect((field as HTMLInputElement).value).toBe('')
    expect(useFilterStore.getState().q).toBe('')
    await waitFor(() => expect(test.router.state.location.search.q).toBeUndefined())
    await waitFor(() =>
      expect(client.isFetching({ queryKey: qk.projectTasks('project-1') })).toBe(0),
    )
    const queries = client.getQueryCache().findAll({ queryKey: qk.projectTasks('project-1') })
    expect(queries.filter((query) => query.isActive()).map((query) => query.queryKey[3])).toEqual([
      'board:[["limit",200]]',
    ])
  })
})
