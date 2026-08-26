import { afterEach, describe, expect, it, vi } from 'vitest'
import { act, cleanup, renderHook } from '@testing-library/react'
import { QueryClient } from '@tanstack/react-query'
import { routeSsePayload, useSSE } from './sse'
import { useAuthStore } from '@/stores/auth'
import { useChatSelection } from '@/stores/chat'
import type { AgentChatTurn } from '@/features/agent-chat/types'

function createMocks() {
  const invalidateQueries = vi.fn()
  const queryClient = { invalidateQueries } as unknown as QueryClient
  const dispatch = vi.fn()
  return { queryClient, invalidateQueries, dispatch }
}

describe('routeSsePayload', () => {
  it('does not invalidate broad queries for execution.log', () => {
    const { queryClient, invalidateQueries, dispatch } = createMocks()
    routeSsePayload(
      {
        event_type: 'execution.log',
        entity_id: 'exec-1',
        task_id: 'task-1',
        timestamp: '2026-05-05T00:00:00Z',
      },
      queryClient,
      { dispatch },
    )
    expect(invalidateQueries).not.toHaveBeenCalled()
    expect(dispatch).not.toHaveBeenCalled()
  })

  it('invalidates execution/task/agents for execution terminal and start events', () => {
    const { queryClient, invalidateQueries, dispatch } = createMocks()
    routeSsePayload(
      {
        event_type: 'execution.started',
        entity_id: 'exec-1',
        task_id: 'task-1',
        timestamp: '2026-05-05T00:00:00Z',
      },
      queryClient,
      { dispatch },
    )
    expect(invalidateQueries).toHaveBeenCalled()
    expect(invalidateQueries.mock.calls).toEqual(
      expect.arrayContaining([
        [{ queryKey: ['executions', 'exec-1'] }],
        [{ queryKey: ['tasks', 'task-1'] }],
        [{ queryKey: ['tasks', 'task-1', 'detail'] }],
        [{ queryKey: ['tasks', 'task-1', 'executions'] }],
        [{ queryKey: ['tasks', 'task-1', 'diff'] }],
        [{ queryKey: ['agents'] }],
      ]),
    )
    expect(dispatch).not.toHaveBeenCalled()
  })

  it('invalidates task and project task list on task.status_changed', () => {
    const { queryClient, invalidateQueries, dispatch } = createMocks()
    routeSsePayload(
      {
        event_type: 'task.status_changed',
        entity_id: 'task-1',
        task_id: 'task-1',
        project_id: 'proj-1',
        timestamp: '2026-05-05T00:00:00Z',
      },
      queryClient,
      { dispatch },
    )
    expect(invalidateQueries.mock.calls).toEqual(
      expect.arrayContaining([
        [{ queryKey: ['tasks', 'task-1'] }],
        [{ queryKey: ['tasks', 'task-1', 'detail'] }],
        [{ queryKey: ['projects', 'proj-1', 'tasks'] }],
      ]),
    )
  })

  it('ignores uncommitted Agent Chat streaming deltas until the ledger entry arrives', () => {
    const { queryClient, invalidateQueries, dispatch } = createMocks()
    routeSsePayload(
      {
        event_type: 'agent_chat.message_delta',
        entity_id: 'chat-1',
        chat_id: 'chat-1',
        timestamp: '2026-05-05T00:00:00Z',
      },
      queryClient,
      { dispatch },
    )
    expect(dispatch).not.toHaveBeenCalled()
    expect(invalidateQueries).not.toHaveBeenCalled()
  })

  it('invalidates Agent Chat projections for committed ledger messages', () => {
    const { queryClient, invalidateQueries, dispatch } = createMocks()
    routeSsePayload(
      {
        event_type: 'agent_chat.message_created',
        entity_id: 'message-1',
        chat_id: 'chat-1',
        project_id: 'proj-1',
        timestamp: '2026-05-05T00:00:00Z',
      },
      queryClient,
      { dispatch },
    )
    expect(dispatch).not.toHaveBeenCalled()
    expect(invalidateQueries.mock.calls).toEqual(
      expect.arrayContaining([
        [{ queryKey: ['agent-chats'] }],
        [{ queryKey: ['agent-chats', 'chat-1'] }],
        [{ queryKey: ['agent-chats', 'chat-1', 'messages'] }],
        [{ queryKey: ['agent-chats', 'chat-1', 'turns'] }],
        [{ queryKey: ['agent-handoffs', 'proj-1'] }],
      ]),
    )
  })

  it('invalidates task review data for automated review results', () => {
    const { queryClient, invalidateQueries, dispatch } = createMocks()
    routeSsePayload(
      {
        event_type: 'review.passed',
        entity_id: 'review-1',
        task_id: 'task-1',
        timestamp: '2026-05-05T00:00:00Z',
      },
      queryClient,
      { dispatch },
    )
    expect(dispatch).not.toHaveBeenCalled()
    expect(invalidateQueries.mock.calls).toEqual(
      expect.arrayContaining([
        [{ queryKey: ['tasks', 'task-1'] }],
        [{ queryKey: ['tasks', 'task-1', 'detail'] }],
        [{ queryKey: ['tasks', 'task-1', 'reviews'] }],
      ]),
    )
  })

  it('invalidates task workspace data for workspace events with task context', () => {
    const { queryClient, invalidateQueries, dispatch } = createMocks()
    routeSsePayload(
      {
        event_type: 'workspace.cleaned',
        entity_id: 'workspace-1',
        task_id: 'task-1',
        timestamp: '2026-05-05T00:00:00Z',
      },
      queryClient,
      { dispatch },
    )
    expect(dispatch).not.toHaveBeenCalled()
    expect(invalidateQueries.mock.calls).toEqual(
      expect.arrayContaining([
        [{ queryKey: ['tasks', 'task-1', 'workspace'] }],
        [{ queryKey: ['tasks', 'task-1', 'detail'] }],
      ]),
    )
  })

  it('invalidates active queries for reconciliation/resync events', () => {
    const { queryClient, invalidateQueries, dispatch } = createMocks()
    routeSsePayload(
      {
        event_type: 'reconciliation.event',
        entity_id: 'task-1',
        timestamp: '2026-05-05T00:00:00Z',
      },
      queryClient,
      { dispatch },
    )
    expect(invalidateQueries).toHaveBeenCalledWith(
      expect.objectContaining({ refetchType: 'active' }),
    )
  })

  // F16: these two event types were previously invisible to the browser —
  // the server named the SSE frame after them, but the hand-maintained
  // named-event catalog never listened for either name, so `onmessage`
  // never ran and `routeSsePayload` never saw the payload at all. D20
  // removes the per-frame SSE name entirely, so every frame reaches this
  // function through `onmessage` and is routed purely by `event_type`.
  it('routes project.created_from_charter_approval to the Project queries', () => {
    const { queryClient, invalidateQueries, dispatch } = createMocks()
    routeSsePayload(
      {
        event_type: 'project.created_from_charter_approval',
        entity_id: 'proj-1',
        timestamp: '2026-05-05T00:00:00Z',
      },
      queryClient,
      { dispatch },
    )
    expect(invalidateQueries.mock.calls).toEqual(
      expect.arrayContaining([
        [{ queryKey: ['projects', 'proj-1'] }],
        [{ queryKey: ['projects'] }],
      ]),
    )
    expect(dispatch).not.toHaveBeenCalled()
  })

  it('routes agent_chat.turn.control_transferred to the Agent Chat queries', () => {
    const { queryClient, invalidateQueries, dispatch } = createMocks()
    routeSsePayload(
      {
        event_type: 'agent_chat.turn.control_transferred',
        entity_id: 'turn-1',
        chat_id: 'chat-1',
        project_id: 'proj-1',
        timestamp: '2026-05-05T00:00:00Z',
      },
      queryClient,
      { dispatch },
    )
    expect(invalidateQueries.mock.calls).toEqual(
      expect.arrayContaining([
        [{ queryKey: ['agent-chats'] }],
        [{ queryKey: ['agent-chats', 'chat-1'] }],
        [{ queryKey: ['agent-chats', 'chat-1', 'messages'] }],
        [{ queryKey: ['agent-chats', 'chat-1', 'turns'] }],
        [{ queryKey: ['agent-handoffs', 'proj-1'] }],
      ]),
    )
  })

  it('falls back to broad invalidation for an event_type with no bespoke route instead of dropping it', () => {
    const { queryClient, invalidateQueries, dispatch } = createMocks()
    routeSsePayload(
      {
        // A `domain_event.committed` frame with no scope (or a scope this
        // router does not recognize) still needs to converge the UI.
        event_type: 'domain_event.committed',
        entity_id: 'event-1',
        timestamp: '2026-05-05T00:00:00Z',
      },
      queryClient,
      { dispatch },
    )
    expect(invalidateQueries).toHaveBeenCalledWith(
      expect.objectContaining({ refetchType: 'active' }),
    )
    expect(dispatch).not.toHaveBeenCalled()
  })

  // 8.4.2: several commands (Project creation from a Charter approval, Main
  // Genesis control transfer, Agent Chat messages/turns, milestone
  // readiness/release) write their durable event inside a larger composite
  // transaction via the `domain_event` outbox. `DomainEventBroadcastConsumer`
  // relays each row after commit as `domain_event.committed`, carrying the
  // row's `scope_type`/`entity_type`/`scope_id` — not its own `event_type` —
  // so routing here keys off scope rather than a name.
  it('routes a project-scoped domain_event.committed to the Project queries', () => {
    const { queryClient, invalidateQueries, dispatch } = createMocks()
    routeSsePayload(
      {
        event_type: 'domain_event.committed',
        entity_id: 'event-1',
        timestamp: '2026-05-05T00:00:00Z',
        scope_type: 'project',
        scope_id: 'proj-1',
        entity_type: 'project',
      },
      queryClient,
      { dispatch },
    )
    expect(invalidateQueries.mock.calls).toEqual(
      expect.arrayContaining([
        [{ queryKey: ['projects', 'proj-1'] }],
        [{ queryKey: ['projects'] }],
      ]),
    )
    expect(dispatch).not.toHaveBeenCalled()
  })

  it('also invalidates the Project Overview for a project-scoped milestone domain_event.committed', () => {
    const { queryClient, invalidateQueries } = createMocks()
    routeSsePayload(
      {
        event_type: 'domain_event.committed',
        entity_id: 'event-1',
        timestamp: '2026-05-05T00:00:00Z',
        scope_type: 'project',
        scope_id: 'proj-1',
        entity_type: 'milestone',
      },
      queryClient,
      { dispatch: vi.fn() },
    )
    expect(invalidateQueries.mock.calls).toEqual(
      expect.arrayContaining([[{ queryKey: ['projects', 'proj-1', 'overview'] }]]),
    )
  })

  it('also invalidates reconciliations for a project-scoped adaptive-boundary domain_event.committed', () => {
    const { queryClient, invalidateQueries } = createMocks()
    routeSsePayload(
      {
        event_type: 'domain_event.committed',
        entity_id: 'event-1',
        timestamp: '2026-05-05T00:00:00Z',
        scope_type: 'project',
        scope_id: 'proj-1',
        entity_type: 'task',
      },
      queryClient,
      { dispatch: vi.fn() },
    )
    expect(invalidateQueries.mock.calls).toEqual(
      expect.arrayContaining([[{ queryKey: ['projects', 'proj-1', 'reconciliations'] }]]),
    )
  })

  it('routes an agent_chat-scoped domain_event.committed to the Agent Chat queries', () => {
    const { queryClient, invalidateQueries, dispatch } = createMocks()
    routeSsePayload(
      {
        event_type: 'domain_event.committed',
        entity_id: 'turn-1',
        timestamp: '2026-05-05T00:00:00Z',
        scope_type: 'agent_chat',
        scope_id: 'chat-1',
        entity_type: 'agent_chat_turn_job',
      },
      queryClient,
      { dispatch },
    )
    expect(invalidateQueries.mock.calls).toEqual(
      expect.arrayContaining([
        [{ queryKey: ['agent-chats'] }],
        [{ queryKey: ['agent-chats', 'chat-1'] }],
        [{ queryKey: ['agent-chats', 'chat-1', 'messages'] }],
        [{ queryKey: ['agent-chats', 'chat-1', 'turns'] }],
      ]),
    )
    expect(dispatch).not.toHaveBeenCalled()
  })

  it('routes a task-scoped domain_event.committed to the Task queries', () => {
    const { queryClient, invalidateQueries } = createMocks()
    routeSsePayload(
      {
        event_type: 'domain_event.committed',
        entity_id: 'review-1',
        timestamp: '2026-05-05T00:00:00Z',
        scope_type: 'task',
        scope_id: 'task-1',
        entity_type: 'review',
      },
      queryClient,
      { dispatch: vi.fn() },
    )
    expect(invalidateQueries.mock.calls).toEqual(
      expect.arrayContaining([
        [{ queryKey: ['tasks', 'task-1'] }],
        [{ queryKey: ['tasks', 'task-1', 'detail'] }],
        [{ queryKey: ['tasks', 'task-1', 'reviews'] }],
      ]),
    )
  })
})

// A minimal `EventSource` stand-in that mirrors the real API surface `useSSE`
// depends on. `addEventListener` is tracked (not wired to `emit`) so the
// F16 regression test below proves the hook never registers one, rather
// than merely proving a registered listener would have worked.
class FakeEventSource {
  static instances: FakeEventSource[] = []
  onmessage: ((event: MessageEvent) => void) | null = null
  onerror: (() => void) | null = null
  onopen: (() => void) | null = null
  readonly url: string
  readonly listenerNames: string[] = []
  closed = false

  constructor(url: string) {
    this.url = url
    FakeEventSource.instances.push(this)
  }

  addEventListener(type: string): void {
    this.listenerNames.push(type)
  }

  removeEventListener(): void {}

  close(): void {
    this.closed = true
  }

  emit(data: string): void {
    this.onmessage?.(new MessageEvent('message', { data }))
  }
}

function turn(overrides: Partial<AgentChatTurn> = {}): AgentChatTurn {
  return {
    id: 'turn-1',
    chat_id: 'chat-1',
    input_message_id: 'msg-1',
    responder_identity_id: null,
    responder_profile_id: null,
    status: 'leased',
    pending_interaction_id: null,
    attempt_count: 1n,
    max_attempts: 3n,
    lease_expires_at: null,
    next_attempt_at: null,
    response_message_id: null,
    error: null,
    correlation_id: 'corr-1',
    version: 1n,
    created_at: '2026-05-05T00:00:00.000Z',
    updated_at: '2026-05-05T00:00:00.000Z',
    ...overrides,
  }
}

describe('useSSE', () => {
  afterEach(() => {
    cleanup()
    FakeEventSource.instances.length = 0
    vi.unstubAllGlobals()
    vi.useRealTimers()
    useAuthStore.setState({ accessToken: null, refreshToken: null, user: null })
    useChatSelection.setState({ pendingTurns: {}, projectChatIds: {} })
  })

  it('registers no named SSE listeners and routes an event a named catalog would have missed (F16)', () => {
    vi.stubGlobal('EventSource', FakeEventSource as unknown as typeof EventSource)
    const queryClient = new QueryClient()
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries')

    renderHook(() => useSSE(queryClient, 'test-token'))

    const source = FakeEventSource.instances[0]
    expect(source).toBeDefined()

    // D20: the server no longer names SSE frames after `event_type`, so a
    // hand-maintained catalog of names to subscribe to has nothing to do —
    // any such catalog could (and did, per F16) miss a real server event.
    expect(source.listenerNames).toEqual([])

    // `project.created_from_charter_approval` is exactly the kind of frame
    // F16 describes: previously named, and absent from the browser's
    // catalog, so it never reached `onmessage`. It now arrives as a
    // default `message` frame with no name to miss.
    source.emit(
      JSON.stringify({
        event_type: 'project.created_from_charter_approval',
        entity_id: 'proj-1',
        timestamp: '2026-05-05T00:00:00Z',
      }),
    )

    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['projects', 'proj-1'] })
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['projects'] })
  })

  // 8.4.3: "on stream open/reconnect/resync, invalidate active
  // authoritative queries exactly once" — this is what makes convergence
  // independent of whether any particular frame was actually lost.
  it('invalidates active queries once on connect (lost event / reconnect convergence)', () => {
    vi.stubGlobal('EventSource', FakeEventSource as unknown as typeof EventSource)
    const queryClient = new QueryClient()
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries')

    renderHook(() => useSSE(queryClient, 'test-token'))
    const source = FakeEventSource.instances[0]
    invalidateSpy.mockClear()

    source.onopen?.()

    expect(invalidateSpy).toHaveBeenCalledTimes(1)
    expect(invalidateSpy).toHaveBeenCalledWith(expect.objectContaining({ refetchType: 'active' }))
  })

  it('reconnects with a refreshed access token after a lost connection', () => {
    vi.useFakeTimers()
    vi.stubGlobal('EventSource', FakeEventSource as unknown as typeof EventSource)
    const queryClient = new QueryClient()

    renderHook(() => useSSE(queryClient, 'stale-token'))
    expect(FakeEventSource.instances).toHaveLength(1)
    expect(FakeEventSource.instances[0]?.url).toContain('token=stale-token')

    // A token refreshed elsewhere (REST traffic owns refreshing, per the
    // module comment) lands in the store before the stream reconnects.
    useAuthStore.setState({ accessToken: 'refreshed-token' })
    act(() => {
      FakeEventSource.instances[0]?.onerror?.()
      vi.advanceTimersByTime(1_000)
    })

    expect(FakeEventSource.instances).toHaveLength(2)
    expect(FakeEventSource.instances[1]?.url).toContain('token=refreshed-token')
  })

  it('closes the stream and cancels a scheduled reconnect on unmount', () => {
    vi.useFakeTimers()
    vi.stubGlobal('EventSource', FakeEventSource as unknown as typeof EventSource)
    const queryClient = new QueryClient()

    const { unmount } = renderHook(() => useSSE(queryClient, 'test-token'))
    const source = FakeEventSource.instances[0]
    source?.onerror?.()

    unmount()
    vi.advanceTimersByTime(30_000)

    expect(source?.closed).toBe(true)
    expect(FakeEventSource.instances).toHaveLength(1)
  })

  // Bounded fallback for a chat turn's completion frame that never arrives
  // — dropped connection, or delivery throttled while the tab was
  // backgrounded. Correctness cannot depend on the frame showing up.
  it('actively refetches a stale pending turn in a hidden tab', () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-05-05T00:00:10.000Z'))
    vi.stubGlobal('EventSource', FakeEventSource as unknown as typeof EventSource)
    const queryClient = new QueryClient()
    const refetchSpy = vi.spyOn(queryClient, 'refetchQueries')

    // Started 10s before "now" — already older than the staleness
    // threshold — and still shows a non-terminal status client-side.
    useChatSelection
      .getState()
      .setPendingTurns('chat-1', [
        turn({ status: 'leased', created_at: '2026-05-05T00:00:00.000Z' }),
      ])

    renderHook(() => useSSE(queryClient, 'test-token'))
    refetchSpy.mockClear()

    act(() => {
      vi.advanceTimersByTime(3_000)
    })
    expect(refetchSpy.mock.calls).toEqual(
      expect.arrayContaining([
        [{ queryKey: ['agent-chats', 'chat-1', 'messages'], type: 'active' }],
        [{ queryKey: ['agent-chats', 'chat-1', 'turns'], type: 'active' }],
      ]),
    )
  })

  it('keeps refetching a cached retrying turn after optimism clears, then stops at terminal', () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-05-05T00:00:10.000Z'))
    vi.stubGlobal('EventSource', FakeEventSource as unknown as typeof EventSource)
    const queryClient = new QueryClient()
    const refetchSpy = vi.spyOn(queryClient, 'refetchQueries')
    queryClient.setQueryData(
      ['agent-chats', 'chat-1', 'turns'],
      [turn({ status: 'retry_wait', created_at: '2026-05-05T00:00:00.000Z' })],
    )

    renderHook(() => useSSE(queryClient, 'test-token'))
    refetchSpy.mockClear()

    act(() => {
      vi.advanceTimersByTime(3_000)
    })
    expect(refetchSpy.mock.calls).toEqual(
      expect.arrayContaining([
        [{ queryKey: ['agent-chats', 'chat-1', 'messages'], type: 'active' }],
        [{ queryKey: ['agent-chats', 'chat-1', 'turns'], type: 'active' }],
      ]),
    )

    refetchSpy.mockClear()
    queryClient.setQueryData(
      ['agent-chats', 'chat-1', 'turns'],
      [turn({ status: 'failed', created_at: '2026-05-05T00:00:00.000Z' })],
    )

    act(() => {
      vi.advanceTimersByTime(3_000)
    })
    expect(refetchSpy).not.toHaveBeenCalled()
  })
})
