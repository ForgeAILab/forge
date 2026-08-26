import { afterEach, describe, expect, it, vi } from 'vitest'
import { QueryClient } from '@tanstack/react-query'
import { clearDeletedProjectScope, resolveNextProjectId } from './project-scope'
import { useChatSelection } from './chat'
import { useLayoutStore } from './layout'
import type { AgentChatTurn } from '@/features/agent-chat/types'

const apiFetch = vi.hoisted(() => vi.fn())

vi.mock('@/api/client', () => ({ apiFetch }))

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

// F17 / 8.4.4: deletion previously invalidated only the Project list query,
// leaving cached Project-scoped state (queries, the chat selection, the
// pending-turn watchdog's target, and the persisted `selectedProjectId`)
// intact. These prove the shared convergence helper actually clears all of
// it, so every caller (explicit delete, an authorized 404, an external
// `project.deleted` SSE frame) gets the same clean state.
describe('clearDeletedProjectScope', () => {
  afterEach(() => {
    apiFetch.mockReset()
    useChatSelection.setState({ pendingTurns: {}, projectChatIds: {} })
    useLayoutStore.setState({ selectedProjectId: undefined })
  })

  it('removes every cached query whose key mentions the deleted Project', () => {
    const queryClient = new QueryClient()
    queryClient.setQueryData(['projects', 'proj-1'], { id: 'proj-1' })
    queryClient.setQueryData(['projects', 'proj-1', 'overview'], { id: 'proj-1' })
    queryClient.setQueryData(['projects', 'proj-1', 'tasks', 'all'], { items: [] })
    queryClient.setQueryData(['agent-handoffs', 'proj-1'], [])
    // A different Project's cache must survive untouched.
    queryClient.setQueryData(['projects', 'proj-2'], { id: 'proj-2' })

    clearDeletedProjectScope(queryClient, 'proj-1')

    expect(queryClient.getQueryData(['projects', 'proj-1'])).toBeUndefined()
    expect(queryClient.getQueryData(['projects', 'proj-1', 'overview'])).toBeUndefined()
    expect(queryClient.getQueryData(['projects', 'proj-1', 'tasks', 'all'])).toBeUndefined()
    expect(queryClient.getQueryData(['agent-handoffs', 'proj-1'])).toBeUndefined()
    expect(queryClient.getQueryData(['projects', 'proj-2'])).toEqual({ id: 'proj-2' })
  })

  it('clears the Project Agent chat selection and its pending turns', () => {
    const queryClient = new QueryClient()
    useChatSelection.getState().setProjectChat('proj-1', { id: 'chat-1' } as never)
    useChatSelection.getState().setPendingTurns('chat-1', [turn()])

    clearDeletedProjectScope(queryClient, 'proj-1')

    const state = useChatSelection.getState()
    expect(state.projectChatIds['proj-1']).toBeUndefined()
    expect('proj-1' in state.projectChatIds).toBe(false)
    expect(state.pendingTurns['chat-1']).toBeUndefined()
  })

  it('clears the persisted selection only when it names the deleted Project', () => {
    const queryClient = new QueryClient()
    useLayoutStore.getState().setSelectedProjectId('proj-1')

    clearDeletedProjectScope(queryClient, 'proj-2')
    expect(useLayoutStore.getState().selectedProjectId).toBe('proj-1')

    clearDeletedProjectScope(queryClient, 'proj-1')
    expect(useLayoutStore.getState().selectedProjectId).toBeUndefined()
  })
})

describe('resolveNextProjectId', () => {
  afterEach(() => {
    apiFetch.mockReset()
  })

  it('returns another authorized Project, excluding the deleted one', async () => {
    apiFetch.mockResolvedValueOnce({
      items: [{ id: 'proj-1' }, { id: 'proj-2' }],
      next_cursor: null,
      has_more: false,
      total_count: 2,
    })
    const queryClient = new QueryClient()

    await expect(resolveNextProjectId(queryClient, 'proj-1')).resolves.toBe('proj-2')
  })

  it('returns undefined when no other authorized Project remains', async () => {
    apiFetch.mockResolvedValueOnce({
      items: [{ id: 'proj-1' }],
      next_cursor: null,
      has_more: false,
      total_count: 1,
    })
    const queryClient = new QueryClient()

    await expect(resolveNextProjectId(queryClient, 'proj-1')).resolves.toBeUndefined()
  })
})
