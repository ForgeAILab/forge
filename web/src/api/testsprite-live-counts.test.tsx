import type { ReactNode } from 'react'
import { act, renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { useBoardData } from '@/features/board/useBoardData'
import { taskListItem } from '@/test-utils/task-list-item'
import { useProjectOverviewQuery } from './hooks'
import { qk } from './query-keys'
import { routeSsePayload } from './sse'

let client: QueryClient
afterEach(() => {
  client?.clear()
  vi.restoreAllMocks()
})

function setup() {
  let tasks = [taskListItem({ priority: 3 })]
  let holdNext = false
  let holdPath = 'tasks'
  let release = () => {}
  let captured = () => {}
  const capturedPromise = new Promise<void>((resolve) => {
    captured = resolve
  })
  const held = new Promise<void>((resolve) => {
    release = resolve
  })
  vi.spyOn(window, 'fetch').mockImplementation(async (input) => {
    const path = new URL(String(input)).pathname
    if (path.endsWith('/tasks')) {
      const response = Response.json({
        items: tasks,
        has_more: false,
        next_cursor: null,
        board_revision: tasks.length,
      })
      if (holdNext && holdPath === 'tasks') {
        holdNext = false
        captured()
        await held
      }
      return response
    }
    if (path.endsWith('/overview')) {
      const response = Response.json({
        task_counts: { total: tasks.length, backlog: tasks.length },
      })
      if (holdNext && holdPath === 'overview') {
        holdNext = false
        captured()
        await held
      }
      return response
    }
    throw new Error(`Unexpected request: ${path}`)
  })
  client = new QueryClient({ defaultOptions: { queries: { retry: false, staleTime: Infinity } } })
  function Wrapper({ children }: { children: ReactNode }) {
    return <QueryClientProvider client={client}>{children}</QueryClientProvider>
  }
  const hook = renderHook(
    () => ({
      board: useBoardData('project-1', {}),
      overview: useProjectOverviewQuery('project-1'),
    }),
    { wrapper: Wrapper },
  )
  const event = (event_type: string) =>
    routeSsePayload(
      {
        event_type,
        project_id: 'project-1',
        entity_id: 'task-1',
        timestamp: new Date().toISOString(),
      },
      client,
      { dispatch: vi.fn() },
    )
  return {
    ...hook,
    event,
    release,
    hold: () => {
      holdNext = true
      return capturedPromise
    },
    holdOverview: () => {
      holdPath = 'overview'
      holdNext = true
      return capturedPromise
    },
    create: () => {
      tasks = [...tasks, taskListItem({ id: 'task-2' })]
    },
    setPriority: () => {
      tasks = tasks.map((task) => ({ ...task, priority: 4, version: 2 }))
    },
  }
}

describe('TestSprite concurrent counts and list reads', () => {
  it('refreshes active Overview counts for another client creating a task', async () => {
    const test = setup()
    await waitFor(() => expect(test.result.current.overview.data?.task_counts.total).toBe(1))
    await act(async () => {
      test.create()
      test.event('task.created')
    })
    await waitFor(() => expect(test.result.current.overview.data?.task_counts.total).toBe(2))
    expect(test.result.current.overview.data?.task_counts.backlog).toBe(2)
  })

  it.each(['priority', 'create'] as const)(
    'refetches after an older list response completes across a concurrent %s event',
    async (change) => {
      const test = setup()
      await waitFor(() => expect(test.result.current.board.snapshot.tasks).toHaveLength(1))
      const captured = test.hold()
      let pending: Promise<void>
      act(() => {
        pending = client.refetchQueries({ queryKey: qk.projectTasks('project-1') })
      })
      await captured
      await act(async () => {
        if (change === 'create') test.create()
        else test.setPriority()
        test.event(change === 'create' ? 'task.created' : 'task.updated')
        test.release()
        await pending!
      })
      await waitFor(() => {
        const tasks = test.result.current.board.snapshot.tasks
        if (change === 'create') expect(tasks).toHaveLength(2)
        else expect(tasks[0].priority).toBe(4)
      })
      expect(client.getQueryState(qk.tasks('project-1', 'board:[]'))?.isInvalidated).toBe(false)
    },
  )

  it('refetches the count source after a create event arrives during an older Overview read', async () => {
    const test = setup()
    await waitFor(() => expect(test.result.current.overview.data?.task_counts.total).toBe(1))
    const captured = test.holdOverview()
    let pending: Promise<void>
    act(() => {
      pending = client.refetchQueries({ queryKey: qk.projectOverview('project-1') })
    })
    await captured
    await act(async () => {
      test.create()
      test.event('task.created')
      test.release()
      await pending!
    })
    await waitFor(() => expect(test.result.current.overview.data?.task_counts.total).toBe(2))
    expect(test.result.current.overview.data?.task_counts.backlog).toBe(2)
  })
})
