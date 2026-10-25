// SSE query mocks, EventSource stand-in, and turn fixtures.

import { vi } from 'vitest'
import { QueryClient } from '@tanstack/react-query'
import type { AgentChatTurn } from '@/features/agent-chat/types'

export function createMocks() {
  const invalidateQueries = vi.fn()
  const queryClient = {
    invalidateQueries,
    getQueryCache: () => ({ findAll: () => [] }),
  } as unknown as QueryClient
  const dispatch = vi.fn()
  return { queryClient, invalidateQueries, dispatch }
}

// A minimal `EventSource` stand-in that mirrors the real API surface `useSSE`
// depends on. `addEventListener` is tracked (not wired to `emit`) so the
// F16 regression test in sse.connection.test.ts proves the hook never registers one, rather
// than merely proving a registered listener would have worked.
export class FakeEventSource {
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

export function turn(overrides: Partial<AgentChatTurn> = {}): AgentChatTurn {
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
    error_code: null,
    error_message: null,
    failure_class: null,
    retry_decision: null,
    pre_provider_failure_count: 0n,
    retry_action: null,
    error: null,
    correlation_id: 'corr-1',
    version: 1n,
    created_at: '2026-05-05T00:00:00.000Z',
    updated_at: '2026-05-05T00:00:00.000Z',
    ...overrides,
  }
}
