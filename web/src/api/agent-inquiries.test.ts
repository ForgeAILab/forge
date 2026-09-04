import { describe, expect, it, vi } from 'vitest'
import {
  cancelAgentInquiry,
  getAgentInquiry,
  listAgentInquiries,
  listAgentInquiryLogs,
} from './agent-inquiries'

const apiFetch = vi.hoisted(() => vi.fn())

vi.mock('@/api/client', () => ({ apiFetch }))

describe('listAgentInquiries', () => {
  it('lists inquiries for a chat with cursor and limit', async () => {
    const response = { items: [{ id: 'inq-1' }], has_more: false, next_cursor: null }
    apiFetch.mockResolvedValue(response)

    await expect(
      listAgentInquiries('chat-1', { cursor: 'cursor-1', limit: 20 }),
    ).resolves.toEqual(response)

    expect(apiFetch).toHaveBeenCalledWith(
      '/agent-chats/chat-1/inquiries',
      expect.objectContaining({ search: { limit: 20, cursor: 'cursor-1' } }),
    )
  })

  it('omits search params when not provided', async () => {
    apiFetch.mockResolvedValue({ items: [], has_more: false, next_cursor: null })

    await listAgentInquiries('chat-1')

    expect(apiFetch).toHaveBeenCalledWith(
      '/agent-chats/chat-1/inquiries',
      expect.objectContaining({ search: { limit: undefined, cursor: undefined } }),
    )
  })
})

describe('getAgentInquiry', () => {
  it('reads a single inquiry by id', async () => {
    const inquiry = { id: 'inq-1', status: 'running' }
    apiFetch.mockResolvedValue(inquiry)

    await expect(getAgentInquiry('inq-1')).resolves.toEqual(inquiry)
    expect(apiFetch).toHaveBeenCalledWith('/inquiries/inq-1')
  })
})

describe('listAgentInquiryLogs', () => {
  it('reads a keyset page of the inquiry activity log', async () => {
    const page = { items: [{ sequence: 4, kind: 'tool_call' }], has_more: true, next_sequence: 5 }
    apiFetch.mockResolvedValue(page)

    const result = await listAgentInquiryLogs('inq-1', { from_sequence: 4, limit: 1 })

    expect(apiFetch).toHaveBeenCalledWith(
      '/inquiries/inq-1/logs',
      expect.objectContaining({ search: { from_sequence: 4, limit: 1, tail: undefined } }),
    )
    expect(result).toEqual(page)
  })

  it('normalizes an empty page for an inquiry that has not written anything yet', async () => {
    apiFetch.mockResolvedValue(undefined)

    await expect(listAgentInquiryLogs('inq-1')).resolves.toEqual({
      items: [],
      has_more: false,
      next_sequence: null,
    })
  })
})

describe('cancelAgentInquiry', () => {
  it('posts the expected version to the cancel route', async () => {
    const cancelled = { id: 'inq-1', status: 'cancelled', version: 2n }
    apiFetch.mockResolvedValue(cancelled)

    await expect(
      cancelAgentInquiry('inq-1', { expected_version: 1 }),
    ).resolves.toEqual(cancelled)

    expect(apiFetch).toHaveBeenCalledWith(
      '/inquiries/inq-1/cancel',
      expect.objectContaining({
        method: 'POST',
        body: JSON.stringify({ expected_version: 1 }),
      }),
    )
  })
})
