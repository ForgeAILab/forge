import type { ReactNode } from 'react'
import { act, cleanup, renderHook } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { useAgentInquiriesQuery, useAgentInquiryLogsQuery } from './hooks'
import type { AgentInquiryLogsPage } from './agent-inquiries'
import type { AgentInquiryResponse, LogEntry } from '@/types/generated'

const api = vi.hoisted(() => ({
  listAgentInquiries: vi.fn(),
  listAgentInquiryLogs: vi.fn(),
  cancelAgentInquiry: vi.fn(),
}))
vi.mock('./agent-inquiries', () => api)

let queryClient: QueryClient

function Wrapper({ children }: { children: ReactNode }) {
  return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
}

function inquiry(status: AgentInquiryResponse['status']): AgentInquiryResponse {
  return {
    id: 'inquiry-1',
    chat_id: 'chat-1',
    title: 'Research',
    question: 'Find the answer',
    status,
    findings: null,
    findings_path: null,
    error: null,
    token_usage: {
      input_tokens: 0n,
      output_tokens: 0n,
      cache_read_tokens: 0n,
      cache_write_tokens: 0n,
    },
    duration_ms: null,
    version: 1n,
    created_at: '2026-09-04T12:00:00Z',
    started_at: '2026-09-04T12:00:00Z',
    finished_at: null,
  }
}

function log(sequence: number): LogEntry {
  return {
    schema_version: 1,
    sequence,
    timestamp: '2026-09-04T12:00:00Z',
    execution_id: 'inquiry-1',
    kind: 'thinking',
    stream: 'main',
    payload: { text: `Activity ${sequence}` },
    truncated: false,
  }
}

function page(items: LogEntry[]): AgentInquiryLogsPage {
  return { items, has_more: false, next_sequence: null }
}

async function advance(ms = 1) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms)
  })
}

beforeEach(() => {
  vi.useFakeTimers()
  vi.clearAllMocks()
  queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
})

afterEach(() => {
  cleanup()
  queryClient.clear()
  vi.useRealTimers()
})

describe('inquiry discovery', () => {
  it.each(['empty', 'terminal'] as const)(
    'discovers a run from an %s list without an event',
    async (initial) => {
      api.listAgentInquiries
        .mockResolvedValueOnce({
          items: initial === 'empty' ? [] : [inquiry('succeeded')],
          next_cursor: null,
          has_more: false,
        })
        .mockResolvedValue({ items: [inquiry('running')], next_cursor: null, has_more: false })

      const { result } = renderHook(() => useAgentInquiriesQuery('chat-1'), { wrapper: Wrapper })
      await advance()
      expect(api.listAgentInquiries).toHaveBeenCalledTimes(1)
      await advance(3_000)
      expect(result.current.data?.pages[0].items[0].status).toBe('running')
      expect(api.listAgentInquiries).toHaveBeenCalledTimes(2)
    },
  )
})

describe('inquiry final activity', () => {
  it.each(['succeeded', 'failed', 'cancelled'] as const)(
    'drains the tail when a watched run becomes %s',
    async (terminalStatus) => {
      api.listAgentInquiryLogs
        .mockResolvedValueOnce(page([log(0)]))
        .mockResolvedValue(page([log(1)]))
      const { result, rerender } = renderHook(
        ({ status }: { status: AgentInquiryResponse['status'] }) =>
          useAgentInquiryLogsQuery('inquiry-1', { live: status === 'running' }),
        { wrapper: Wrapper, initialProps: { status: 'running' } },
      )
      await advance()
      expect(result.current.data).toEqual([log(0)])

      rerender({ status: terminalStatus })
      await advance()
      expect(result.current.data).toEqual([log(0), log(1)])
      expect(api.listAgentInquiryLogs).toHaveBeenLastCalledWith('inquiry-1', {
        from_sequence: 1,
        limit: 1_000,
      })
      await advance(3_000)
      expect(api.listAgentInquiryLogs).toHaveBeenCalledTimes(2)
    },
  )

  it('replaces an initial read started before completion with a fresh terminal read', async () => {
    let finishOldRead!: (page: AgentInquiryLogsPage) => void
    api.listAgentInquiryLogs
      .mockReturnValueOnce(
        new Promise<AgentInquiryLogsPage>((resolve) => {
          finishOldRead = resolve
        }),
      )
      .mockResolvedValue(page([log(0), log(1)]))
    const { result, rerender } = renderHook(
      ({ live }) => useAgentInquiryLogsQuery('inquiry-1', { live }),
      { wrapper: Wrapper, initialProps: { live: true } },
    )
    await advance()
    rerender({ live: false })
    await advance()
    expect(api.listAgentInquiryLogs).toHaveBeenCalledTimes(2)
    expect(result.current.data).toEqual([log(0), log(1)])

    await act(async () => {
      finishOldRead(page([log(0)]))
    })
    expect(result.current.data).toEqual([log(0), log(1)])
  })

  it('drains a closed row on reopening after its run finishes', async () => {
    api.listAgentInquiryLogs.mockResolvedValueOnce(page([log(0)])).mockResolvedValue(page([log(1)]))
    const { result, rerender } = renderHook(
      (options) => useAgentInquiryLogsQuery('inquiry-1', options),
      { wrapper: Wrapper, initialProps: { live: true, enabled: true } },
    )
    await advance()
    rerender({ live: true, enabled: false })
    rerender({ live: false, enabled: false })
    await advance()
    expect(api.listAgentInquiryLogs).toHaveBeenCalledTimes(1)
    rerender({ live: false, enabled: true })
    await advance()
    expect(result.current.data).toEqual([log(0), log(1)])
  })
})
