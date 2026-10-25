// SSE connection, reconnection, and stale turn watchdog tests.

import { FakeEventSource, turn } from './sse.test-helpers'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { act, cleanup, renderHook } from '@testing-library/react'
import { QueryClient } from '@tanstack/react-query'
import { useSSE } from './sse'
import { useAuthStore } from '@/stores/auth'
import { useChatSelection } from '@/stores/chat'

describe('useSSE', () => {
  afterEach(() => {
    cleanup()
    FakeEventSource.instances.length = 0
    vi.unstubAllGlobals()
    vi.useRealTimers()
    useAuthStore.setState({ accessToken: null, refreshToken: null, user: null })
    useChatSelection.setState({ pendingTurns: {}, projectChatIds: {} })
  })

  it('opens and reconnects a cookie-only stream without a token query parameter', () => {
    vi.useFakeTimers()
    vi.stubGlobal('EventSource', FakeEventSource as unknown as typeof EventSource)
    useAuthStore.setState({ accessToken: null, refreshToken: null, user: null })
    const client = new QueryClient()
    const { unmount } = renderHook(() => useSSE(client, null, true))
    expect(FakeEventSource.instances[0].url).toBe('/api/v1/events')
    act(() => FakeEventSource.instances[0].onerror?.())
    act(() => vi.advanceTimersByTime(1000))
    expect(FakeEventSource.instances[1].url).toBe('/api/v1/events')
    unmount()
    client.clear()
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

    expect(invalidateSpy).toHaveBeenCalledWith(
      { exact: true, queryKey: ['projects', 'proj-1'] },
      { cancelRefetch: false },
    )
    expect(invalidateSpy).toHaveBeenCalledWith(
      { exact: true, queryKey: ['projects'] },
      { cancelRefetch: false },
    )
  })

  // 8.4.3: "on stream open/reconnect/resync, invalidate active
  // authoritative queries exactly once" — this is what makes convergence
  // independent of whether any particular frame was actually lost.
  it('invalidates active queries once on connect (lost event / reconnect convergence)', () => {
    vi.useFakeTimers()
    vi.stubGlobal('EventSource', FakeEventSource as unknown as typeof EventSource)
    const queryClient = new QueryClient()
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries')

    renderHook(() => useSSE(queryClient, 'test-token'))
    const source = FakeEventSource.instances[0]
    invalidateSpy.mockClear()

    act(() => {
      source.onopen?.()
      vi.advanceTimersByTime(1_000)
    })

    expect(invalidateSpy).toHaveBeenCalledTimes(1)
    expect(invalidateSpy).toHaveBeenCalledWith(expect.objectContaining({ refetchType: 'active' }), {
      cancelRefetch: false,
    })
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

  it('does not refetch active queries for a connection that opens and immediately fails', () => {
    vi.useFakeTimers()
    vi.stubGlobal('EventSource', FakeEventSource as unknown as typeof EventSource)
    const queryClient = new QueryClient()
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries')

    renderHook(() => useSSE(queryClient, 'test-token'))
    const source = FakeEventSource.instances[0]
    source?.onopen?.()
    act(() => {
      vi.advanceTimersByTime(500)
      source?.onerror?.()
      vi.advanceTimersByTime(500)
    })

    expect(invalidateSpy).not.toHaveBeenCalled()
    expect(FakeEventSource.instances).toHaveLength(1)

    act(() => {
      vi.advanceTimersByTime(500)
    })
    expect(FakeEventSource.instances).toHaveLength(2)
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
  it('pauses the stale-turn watchdog while hidden and checks immediately on becoming visible', () => {
    const visibility = vi.spyOn(document, 'visibilityState', 'get').mockReturnValue('hidden')
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

    const { unmount } = renderHook(() => useSSE(queryClient, 'test-token'))
    refetchSpy.mockClear()

    act(() => {
      document.dispatchEvent(new Event('visibilitychange'))
      vi.advanceTimersByTime(15_000)
    })
    expect(refetchSpy).not.toHaveBeenCalled()
    visibility.mockReturnValue('visible')
    act(() => {
      document.dispatchEvent(new Event('visibilitychange'))
    })
    expect(refetchSpy.mock.calls).toEqual(
      expect.arrayContaining([
        [{ queryKey: ['agent-chats', 'chat-1', 'messages'], type: 'active' }],
        [{ queryKey: ['agent-chats', 'chat-1', 'turns'], type: 'active' }],
      ]),
    )
    unmount()
    refetchSpy.mockClear()
    document.dispatchEvent(new Event('visibilitychange'))
    expect(refetchSpy).not.toHaveBeenCalled()
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
      vi.advanceTimersByTime(15_000)
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
      vi.advanceTimersByTime(15_000)
    })
    expect(refetchSpy).not.toHaveBeenCalled()
  })
})
