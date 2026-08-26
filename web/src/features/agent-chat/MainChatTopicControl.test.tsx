import { render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { apiFetch, ApiError } from '@/api/client'
import { MainChatTopicControl } from './MainChatTopicControl'

// F18: the singular Main Chat had no fresh-topic boundary, so a new
// conversation carried the entire prior visible topic forward. These tests
// drive the control the same way a user does: it shows the current topic,
// starts a new one on request, surfaces a D21 denial (a live turn or a
// pending Genesis decision) as a specific inline message rather than a
// generic failure, and never implies a second Main Chat is created.

vi.mock('@/api/client', () => ({
  apiFetch: vi.fn(),
  ApiError: class extends Error {
    status: number
    constructor(message: string, status: number) {
      super(message)
      this.status = status
    }
  },
}))

function topicsResponse(items: Array<Record<string, unknown>>) {
  return { items }
}

function currentTopic(overrides: Record<string, unknown> = {}) {
  return {
    id: 'topic-1',
    chat_id: 'chat-1',
    sequence: 0,
    label: 'Original conversation',
    summary: null,
    starting_message_id: null,
    starting_message_sequence: 0,
    principal_type: 'system',
    principal_id: null,
    created_at: '2026-08-20T00:00:00Z',
    is_current: true,
    ...overrides,
  }
}

function renderControl(node: ReactNode) {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: 0 } } })
  return render(<QueryClientProvider client={queryClient}>{node}</QueryClientProvider>)
}

describe('MainChatTopicControl', () => {
  beforeEach(() => {
    vi.mocked(apiFetch).mockReset()
  })

  it('shows the current topic label and a New topic action', async () => {
    vi.mocked(apiFetch).mockResolvedValue(topicsResponse([currentTopic()]))
    renderControl(<MainChatTopicControl chatId="chat-1" />)

    expect(await screen.findByText('Original conversation')).toBeTruthy()
    expect(screen.getByRole('button', { name: 'New topic' })).toBeTruthy()
  })

  it('starts a new topic and closes the dialog on success', async () => {
    vi.mocked(apiFetch).mockImplementation(async (_path: string, init?: RequestInit) => {
      if (init?.method === 'POST') {
        return {
          topic: currentTopic({ id: 'topic-2', sequence: 1, label: 'Planning', is_current: true }),
          divider_message_id: 'divider-1',
        }
      }
      return topicsResponse([currentTopic()])
    })

    renderControl(<MainChatTopicControl chatId="chat-1" />)
    // Wait for the topics query to settle first -- the button exists (but is
    // disabled) while it is still loading, and a click on a disabled button
    // is a no-op.
    await screen.findByText('Original conversation')
    const openButton = screen.getByRole('button', { name: 'New topic' })
    openButton.click()

    const dialogButton = await screen.findByRole('button', { name: 'Start topic' })
    dialogButton.click()

    await waitFor(() => {
      const posted = vi.mocked(apiFetch).mock.calls.find(([, init]) => init?.method === 'POST')
      expect(posted).toBeTruthy()
    })
    await waitFor(() => expect(screen.queryByRole('button', { name: 'Start topic' })).toBeNull())
  })

  it('shows the server denial message inline when a Main turn is live', async () => {
    vi.mocked(apiFetch).mockImplementation(async (_path: string, init?: RequestInit) => {
      if (init?.method === 'POST') {
        throw new ApiError(
          'A Main turn is in progress; wait for it to finish or cancel it before starting a new topic.',
          409,
        )
      }
      return topicsResponse([currentTopic()])
    })

    renderControl(<MainChatTopicControl chatId="chat-1" />)
    await screen.findByText('Original conversation')
    const openButton = screen.getByRole('button', { name: 'New topic' })
    openButton.click()
    const dialogButton = await screen.findByRole('button', { name: 'Start topic' })
    dialogButton.click()

    expect(await screen.findByRole('alert')).toHaveProperty(
      'textContent',
      expect.stringContaining('wait for it to finish or cancel it'),
    )
    // The dialog stays open on denial -- it never silently discards the
    // user's in-progress label input.
    expect(screen.getByRole('button', { name: 'Start topic' })).toBeTruthy()
  })

  it('disables the action with a reason while Genesis needs a decision', async () => {
    vi.mocked(apiFetch).mockResolvedValue(topicsResponse([currentTopic()]))
    renderControl(
      <MainChatTopicControl
        chatId="chat-1"
        disabled
        disabledReason="Finish or cancel the active Product Genesis session before starting a new topic."
      />,
    )

    const button = await screen.findByRole('button', { name: 'New topic' })
    expect(button.hasAttribute('disabled')).toBe(true)
    expect(button.getAttribute('title')).toContain('Product Genesis')
  })

  it('lists earlier topics as inspectable without offering to switch into them', async () => {
    vi.mocked(apiFetch).mockResolvedValue(
      topicsResponse([
        currentTopic({ id: 'topic-1', sequence: 0, label: 'Original conversation', is_current: false }),
        currentTopic({ id: 'topic-2', sequence: 1, label: 'Planning the release', is_current: true }),
      ]),
    )
    renderControl(<MainChatTopicControl chatId="chat-1" />)

    expect(await screen.findByText('Planning the release')).toBeTruthy()
    const disclosure = screen.getByText('Planning the release').closest('details')
    expect(disclosure).toBeTruthy()
    disclosure?.setAttribute('open', '')
    expect(await screen.findByText('Original conversation')).toBeTruthy()
  })
})
