import { describe, expect, it, vi } from 'vitest'
import { cancelAgentChatTurn, listAgentChatTurnLogs, agentChatApiPaths } from './api'

const apiFetch = vi.hoisted(() => vi.fn())

vi.mock('@/api/client', () => ({ apiFetch }))

describe('agent chat cancel API', () => {
  it('posts the current turn version and idempotency key to the cancel route', async () => {
    const cancelledTurn = { id: 'turn-1', status: 'cancelled', version: 8n }
    apiFetch.mockResolvedValue(cancelledTurn)

    await cancelAgentChatTurn('chat-1', 'turn-1', {
      expected_version: 7,
      idempotency_key: 'cancel:turn-1:7',
    })

    expect(agentChatApiPaths.cancelTurn('chat-1', 'turn-1')).toBe(
      '/agent-chats/chat-1/turns/turn-1/cancel',
    )
    expect(apiFetch).toHaveBeenCalledWith(
      '/agent-chats/chat-1/turns/turn-1/cancel',
      expect.objectContaining({
        method: 'POST',
        body: JSON.stringify({
          expected_version: 7,
          idempotency_key: 'cancel:turn-1:7',
        }),
      }),
    )
  })
})

describe('agent chat turn logs API', () => {
  it('reads one keyset page of a turn activity log', async () => {
    apiFetch.mockResolvedValue({
      items: [{ sequence: 4, kind: 'tool_call' }],
      has_more: true,
      next_sequence: 5,
    })

    const page = await listAgentChatTurnLogs('chat-1', 'turn-1', { from_sequence: 4, limit: 1 })

    expect(agentChatApiPaths.turnLogs('chat-1', 'turn-1')).toBe(
      '/agent-chats/chat-1/turns/turn-1/logs',
    )
    expect(apiFetch).toHaveBeenCalledWith(
      '/agent-chats/chat-1/turns/turn-1/logs',
      expect.objectContaining({ search: { from_sequence: 4, limit: 1, tail: undefined } }),
    )
    expect(page).toEqual({
      items: [{ sequence: 4, kind: 'tool_call' }],
      has_more: true,
      next_sequence: 5,
    })
  })

  it('normalizes an empty page for a turn that has not started', async () => {
    apiFetch.mockResolvedValue(undefined)
    expect(await listAgentChatTurnLogs('chat-1', 'turn-1')).toEqual({
      items: [],
      has_more: false,
      next_sequence: null,
    })
  })
})
