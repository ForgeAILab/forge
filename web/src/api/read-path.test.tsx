import type { ReactNode } from 'react'
import { act, cleanup, renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider, focusManager } from '@tanstack/react-query'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { useTasksQuery } from './hooks'
import { qk } from './query-keys'
import { routeSsePayload } from './sse'
import { useAgentChatMessagesQuery, useAgentChatTurnLogsQuery } from '@/features/agent-chat/hooks'
import type { TasksResponse } from '@/types/generated'

let client: QueryClient
function Wrapper({ children }: { children: ReactNode }) {
  return <QueryClientProvider client={client}>{children}</QueryClientProvider>
}
afterEach(() => {
  cleanup()
  client?.clear()
  focusManager.setFocused(undefined)
  vi.restoreAllMocks()
  vi.useRealTimers()
})
function queryClient() {
  client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
}
const page = {
  items: [
    {
      id: 'task-1',
      status: 'in_progress',
      canonical_phase: 'working',
      board_position: 1,
      version: 2,
      updated_at: 'before',
      condition: { kind: 'clear', details: { failure_kind: null, diagnostic: null, interruption: null, failed: false, blocked: false, human_wait: false, entry_wait: false } },
    },
  ],
  next_cursor: null,
  has_more: false,
  total_count: null,
  board_revision: 1,
} as TasksResponse

describe('task list read path', () => {
  it('sends the exact page validator and retains cached items on 304', async () => {
    queryClient()
    const fetch = vi
      .spyOn(window, 'fetch')
      .mockResolvedValueOnce(
        new Response(JSON.stringify(page), { headers: { etag: 'W/"page-1"' } }),
      )
      .mockResolvedValueOnce(new Response(null, { status: 304 }))
    const { result } = renderHook(() => useTasksQuery('project-1', {}), { wrapper: Wrapper })
    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    const before = result.current.data
    await act(async () => {
      await result.current.refetch()
    })
    expect(fetch).toHaveBeenCalledTimes(2)
    expect((fetch.mock.calls[1][1]?.headers as Headers).get('if-none-match')).toBe('W/"page-1"')
    expect(result.current.data).toBe(before)
    expect(result.current.data?.pages[0].items).toEqual(page.items)
  })

  it('retains the new validator when structural sharing rebuilds a refreshed page', async () => {
    queryClient()
    const fetch = vi
      .spyOn(window, 'fetch')
      .mockResolvedValueOnce(
        new Response(JSON.stringify(page), { headers: { etag: 'W/"page-1"' } }),
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ ...page, board_revision: 2 }), {
          headers: { etag: 'W/"page-2"' },
        }),
      )
      .mockResolvedValueOnce(new Response(null, { status: 304 }))
    const { result } = renderHook(() => useTasksQuery('project-1', {}), { wrapper: Wrapper })
    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    await act(async () => {
      await result.current.refetch()
    })
    const refreshed = result.current.data
    await act(async () => {
      await result.current.refetch()
    })
    expect((fetch.mock.calls[2][1]?.headers as Headers).get('if-none-match')).toBe('W/"page-2"')
    expect(result.current.data).toBe(refreshed)
  })

  it('patches a board move without another task-list request', async () => {
    queryClient()
    const fetch = vi.spyOn(window, 'fetch').mockResolvedValue(new Response(JSON.stringify(page)))
    const { result } = renderHook(() => useTasksQuery('project-1', {}), { wrapper: Wrapper })
    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    expect(result.current.data?.pages[0].items[0].board_position).toBe(1)
    act(() =>
      routeSsePayload(
        {
          event_type: 'task.moved',
          entity_id: 'task-1',
          project_id: 'project-1',
          new_status: 'in_progress',
          new_board_position: 4,
          task_version: 3,
          board_revision: 2,
          timestamp: '2026-10-01T00:00:00Z',
        },
        client,
        { dispatch: vi.fn() },
      ),
    )
    expect(
      client.getQueryData<{ pages: TasksResponse[] }>(qk.tasks('project-1', '{}'))?.pages[0]
        .items[0].board_position,
    ).toBe(4)
    await waitFor(() => expect(result.current.data?.pages[0].items[0].board_position).toBe(4))
    expect(
      client.getQueryData<{ pages: TasksResponse[] }>(qk.tasks('project-1', '{}'))?.pages[0]
        .items[0].version,
    ).toBe(3)
    expect(fetch).toHaveBeenCalledTimes(1)
  })

  it('drops the server validator after a local event patch', async () => {
    queryClient()
    const fetch = vi
      .spyOn(window, 'fetch')
      .mockResolvedValueOnce(
        new Response(JSON.stringify(page), { headers: { etag: 'W/"page-1"' } }),
      )
      .mockResolvedValueOnce(new Response(JSON.stringify({ ...page, board_revision: 2 })))
    const { result } = renderHook(() => useTasksQuery('project-1', {}), { wrapper: Wrapper })
    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    act(() =>
      routeSsePayload(
        {
          event_type: 'task.moved',
          entity_id: 'task-1',
          project_id: 'project-1',
          new_status: 'in_progress',
          new_board_position: 4,
          task_version: 3,
          board_revision: 2,
          timestamp: 'now',
        },
        client,
        { dispatch: vi.fn() },
      ),
    )
    expect(fetch).toHaveBeenCalledTimes(1)
    await act(async () => {
      await result.current.refetch()
    })
    expect((fetch.mock.calls[1][1]?.headers as Headers).get('if-none-match')).toBeNull()
  })

  it('refetches a same-status move with a revision gap and preserves the cached revision', async () => {
    vi.useFakeTimers()
    queryClient()
    const key = qk.tasks('project-1', '{}')
    client.setQueryData(key, { pages: [page], pageParams: [undefined] })
    routeSsePayload(
      {
        event_type: 'task.moved',
        entity_id: 'task-1',
        project_id: 'project-1',
        new_status: 'in_progress',
        new_board_position: 4,
        task_version: 3,
        board_revision: 3,
        timestamp: 'now',
      },
      client,
      { dispatch: vi.fn() },
    )
    expect(client.getQueryData<{ pages: TasksResponse[] }>(key)?.pages[0].board_revision).toBe(1)
    expect(client.getQueryState(key)?.isInvalidated).toBe(false)
    await vi.advanceTimersByTimeAsync(1_500)
    expect(client.getQueryState(key)?.isInvalidated).toBe(true)
  })

  it('refetches after an older in-flight response overwrites a same-status move', async () => {
    queryClient()
    let resolveOlder!: (response: Response) => void
    const moved = {
      ...page,
      items: [{ ...page.items[0], board_position: 4, version: 3 }],
      board_revision: 2,
    }
    const fetch = vi
      .spyOn(window, 'fetch')
      .mockResolvedValueOnce(new Response(JSON.stringify(page)))
      .mockImplementationOnce(
        () =>
          new Promise<Response>((resolve) => {
            resolveOlder = resolve
          }),
      )
      .mockResolvedValueOnce(new Response(JSON.stringify(moved)))
    const { result } = renderHook(() => useTasksQuery('project-1', {}), { wrapper: Wrapper })
    await waitFor(() => expect(result.current.isSuccess).toBe(true))
    vi.useFakeTimers()
    let older!: Promise<unknown>
    act(() => {
      older = result.current.refetch()
    })
    expect(client.isFetching({ queryKey: qk.projectTasks('project-1') })).toBe(1)
    act(() =>
      routeSsePayload(
        {
          event_type: 'task.moved',
          entity_id: 'task-1',
          project_id: 'project-1',
          new_status: 'in_progress',
          new_board_position: 4,
          task_version: 3,
          board_revision: 2,
          timestamp: 'now',
        },
        client,
        { dispatch: vi.fn() },
      ),
    )
    const key = qk.tasks('project-1', '{}')
    expect(
      client.getQueryData<{ pages: TasksResponse[] }>(key)?.pages[0].items[0].board_position,
    ).toBe(4)
    await act(async () => {
      resolveOlder(new Response(JSON.stringify(page)))
      await older
    })
    expect(
      client.getQueryData<{ pages: TasksResponse[] }>(key)?.pages[0].items[0].board_position,
    ).toBe(1)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(1_500)
    })
    expect(fetch).toHaveBeenCalledTimes(3)
    expect(
      client.getQueryData<{ pages: TasksResponse[] }>(key)?.pages[0].items[0].board_position,
    ).toBe(4)
  })

  it('patches status immediately and throttles the filtered-page fallback', async () => {
    vi.useFakeTimers()
    queryClient()
    client.setQueryData(qk.tasks('project-1', '{"status":"review"}'), {
      pages: [page],
      pageParams: [undefined],
    })
    routeSsePayload(
      {
        event_type: 'task.status_changed',
        entity_id: 'task-1',
        project_id: 'project-1',
        new_status: 'review',
        timestamp: 'now',
      },
      client,
      { dispatch: vi.fn() },
    )
    expect(
      client.getQueryData<{ pages: TasksResponse[] }>(qk.tasks('project-1', '{"status":"review"}'))
        ?.pages[0].items[0].status,
    ).toBe('review')
    await vi.advanceTimersByTimeAsync(1_499)
    expect(client.getQueryState(qk.tasks('project-1', '{"status":"review"}'))?.isInvalidated).toBe(
      false,
    )
    await vi.advanceTimersByTimeAsync(1)
    expect(client.getQueryState(qk.tasks('project-1', '{"status":"review"}'))?.isInvalidated).toBe(
      true,
    )
  })
})

describe('chat read path', () => {
  it('invalidates messages, turns, inquiries, topics and activity on committed chat events', () => {
    queryClient()
    const keys = [
      ['agent-chats'],
      ['agent-chats', 'chat-1', 'messages'],
      ['agent-chats', 'chat-1', 'turns'],
      ['agent-chats', 'chat-1', 'topics'],
      ['agent-chats', 'chat-1', 'inquiries', 50],
      ['agent-chats', 'chat-1', 'turns', 'turn-1', 'logs'],
      ['agent-handoffs', 'project-1'],
      ['agent-handoffs', 'project-1', 'handoff-1'],
    ]
    for (const key of keys) client.setQueryData(key, [])
    client.setQueryData(['agent-chats', 'chat-2', 'messages'], [])
    routeSsePayload(
      {
        event_type: 'domain_event.committed',
        entity_id: 'event-1',
        domain_event_type: 'agent_chat.turn.status_changed',
        entity_type: 'agent_chat_turn_job',
        scope_type: 'agent_chat',
        scope_id: 'chat-1',
        timestamp: 'now',
      },
      client,
      { dispatch: vi.fn() },
    )
    for (const key of keys) expect(client.getQueryState(key)?.isInvalidated).toBe(true)
    expect(client.getQueryState(['agent-chats', 'chat-2', 'messages'])?.isInvalidated).toBe(false)
  })

  it('uses a 15 s fallback, 1.5 s live activity, and pauses polling in the background', async () => {
    vi.useFakeTimers()
    queryClient()
    const fetch = vi
      .spyOn(window, 'fetch')
      .mockImplementation(
        async (url) =>
          new Response(
            JSON.stringify(
              String(url).includes('/logs')
                ? { items: [], has_more: false, next_sequence: null }
                : { items: [], next_cursor: null, has_more: false },
            ),
          ),
      )
    const { rerender } = renderHook(
      ({ live }) => {
        useAgentChatMessagesQuery('chat-1')
        useAgentChatTurnLogsQuery('chat-1', 'turn-1', { live })
      },
      { wrapper: Wrapper, initialProps: { live: true } },
    )
    await act(async () => {
      await vi.advanceTimersByTimeAsync(1)
    })
    expect(fetch).toHaveBeenCalledTimes(2)
    const messages = client
      .getQueryCache()
      .find({ queryKey: ['agent-chats', 'chat-1', 'messages'] })!
    expect(
      (messages.options as { refetchIntervalInBackground: boolean }).refetchIntervalInBackground,
    ).toBe(false)
    const callsTo = (path: string) =>
      fetch.mock.calls.filter(([url]) => String(url).includes(path)).length
    await act(async () => {
      await vi.advanceTimersByTimeAsync(1_498)
    })
    expect(callsTo('/logs')).toBe(1)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(1)
    })
    expect(callsTo('/logs')).toBe(2)
    expect(callsTo('/messages')).toBe(1)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(13_500)
    })
    expect(callsTo('/messages')).toBe(2)
    expect(callsTo('/logs')).toBe(11)
    const foregroundCalls = fetch.mock.calls.length
    focusManager.setFocused(false)
    await act(async () => {
      await vi.advanceTimersByTimeAsync(30_000)
    })
    expect(fetch).toHaveBeenCalledTimes(foregroundCalls)
    rerender({ live: false })
    const logs = client
      .getQueryCache()
      .find({ queryKey: ['agent-chats', 'chat-1', 'turns', 'turn-1', 'logs'] })!
    expect(
      (
        (logs.options as { refetchInterval: unknown }).refetchInterval as (
          q: typeof logs,
        ) => unknown
      )(logs),
    ).toBe(false)
  })
})
