import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { act, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { AgentChatTimeline, ChatComposer } from './agent-chat-timeline'
import type { AgentChat, AgentChatMessage, AgentChatTurn } from '@/features/agent-chat/types'

const mocks = vi.hoisted(() => ({
  listAgentChatMessages: vi.fn(),
  listAgentChatTurns: vi.fn(),
  listAgentHandoffs: vi.fn(),
  navigate: vi.fn(),
}))

vi.mock('@/features/agent-chat/api', () => ({
  listAgentChatMessages: mocks.listAgentChatMessages,
  listAgentChatTurns: mocks.listAgentChatTurns,
  listAgentHandoffs: mocks.listAgentHandoffs,
}))

vi.mock('@tanstack/react-router', () => ({
  useNavigate: () => mocks.navigate,
}))

const chat: AgentChat = {
  id: 'chat-1',
  kind: 'main',
  account_id: 'account-1',
  project_id: null,
  title: 'Main Agent Chat',
  status: 'ready',
  message_count: 1n,
  pending_turn_count: 1n,
  last_message_at: '2026-08-13T12:00:00Z',
  version: 1n,
  created_at: '2026-08-13T11:59:00Z',
  updated_at: '2026-08-13T12:00:00Z',
}

const userMessage: AgentChatMessage = {
  id: 'message-1',
  chat_id: 'chat-1',
  author_type: 'user',
  author_id: null,
  content: 'queued request',
  content_guard: {},
  sensitivity: 'normal',
  status: 'complete',
  outcome: null,
  model: null,
  profile_id: null,
  session_id: null,
  context_manifest_id: null,
  token_usage_json: null,
  duration_ms: null,
  error: null,
  correlation_id: 'correlation-1',
  causation_id: null,
  handoff_id: null,
  source_chat_id: null,
  source_message_id: null,
  sequence: 1n,
  created_at: '2026-08-13T12:00:00Z',
}

const assistantMessage: AgentChatMessage = {
  ...userMessage,
  id: 'message-2',
  author_type: 'agent',
  author_id: 'agent-1',
  content: 'assistant response arrived',
  sequence: 2n,
  created_at: '2026-08-13T12:00:02Z',
}

const queuedTurn: AgentChatTurn = {
  id: 'turn-1',
  chat_id: 'chat-1',
  input_message_id: 'message-1',
  responder_identity_id: 'agent-1',
  responder_profile_id: 'profile-1',
  status: 'queued',
  pending_interaction_id: null,
  attempt_count: 0n,
  max_attempts: 3n,
  lease_expires_at: null,
  next_attempt_at: null,
  response_message_id: null,
  error: null,
  correlation_id: 'correlation-1',
  version: 1n,
  created_at: '2026-08-13T12:00:00Z',
  updated_at: '2026-08-13T12:00:00Z',
}

const completedTurn: AgentChatTurn = {
  ...queuedTurn,
  status: 'succeeded',
  response_message_id: 'message-2',
  updated_at: '2026-08-13T12:00:02Z',
}

function renderTimeline({
  onSend = vi.fn(async () => undefined),
  isSending = false,
  handoffProjectIds,
  onCancelTurn,
}: {
  onSend?: (content: string) => Promise<void>
  isSending?: boolean
  handoffProjectIds?: string[]
  onCancelTurn?: (turnId: string, expectedVersion: number) => Promise<void>
} = {}) {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: Infinity },
    },
  })
  return render(
    <QueryClientProvider client={queryClient}>
      <AgentChatTimeline
        chat={chat}
        handoffProjectIds={handoffProjectIds}
        isSending={isSending}
        onSend={onSend}
        onCancelTurn={onCancelTurn}
      />
    </QueryClientProvider>,
  )
}

describe('AgentChatTimeline polling', () => {
  let turnComplete = false

  beforeEach(() => {
    vi.useFakeTimers()
    turnComplete = false
    mocks.listAgentChatMessages.mockImplementation(async () => ({
      items: turnComplete ? [userMessage, assistantMessage] : [userMessage],
      next_cursor: null,
      has_more: false,
    }))
    mocks.listAgentChatTurns.mockImplementation(async () =>
      turnComplete ? [completedTurn] : [queuedTurn],
    )
    mocks.listAgentHandoffs.mockResolvedValue([])
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.clearAllMocks()
  })

  it('shows the completed assistant response after polling without remounting the timeline', async () => {
    const view = renderTimeline()

    await act(async () => {
      await vi.runOnlyPendingTimersAsync()
      await Promise.resolve()
      await Promise.resolve()
    })
    expect(screen.getByText('queued request')).toBeTruthy()
    expect(screen.queryByText('assistant response arrived')).toBeNull()
    const timelineRoot = view.container.firstChild

    turnComplete = true
    await act(async () => {
      await vi.advanceTimersByTimeAsync(2_000)
    })

    expect(screen.getByText('assistant response arrived')).toBeTruthy()
    expect(screen.getByText('Succeeded')).toBeTruthy()
    expect(view.container.firstChild).toBe(timelineRoot)
  })

  it.each([
    ['queued', /Queued/],
    ['leased', /Thinking/],
    ['awaiting_input', /Awaiting input/],
    ['retry_wait', /Retrying/],
    ['failed', /Turn failed|Failed/],
    ['cancelled', /Cancelled/],
    ['succeeded', /Succeeded/],
  ] as const)(
    'renders the finite %s state beside its triggering message',
    async (status, label) => {
      mocks.listAgentChatMessages.mockResolvedValue({
        items: [userMessage],
        next_cursor: null,
        has_more: false,
      })
      mocks.listAgentChatTurns.mockResolvedValue([
        { ...queuedTurn, status, error: status === 'failed' ? 'Provider timed out' : null },
      ])

      const onSend = vi.fn(async () => undefined)
      renderTimeline({ onSend })
      await act(async () => {
        await vi.runOnlyPendingTimersAsync()
        await Promise.resolve()
        await Promise.resolve()
      })

      expect(screen.getByText(label)).toBeTruthy()
      if (status === 'failed' || status === 'cancelled') {
        const retry = screen.getByRole('button', { name: /Retry You turn/ })
        fireEvent.click(retry)
        await vi.waitFor(() => expect(onSend).toHaveBeenCalledWith('queued request'))
      } else {
        expect(screen.queryByRole('button', { name: /Retry You turn/ })).toBeNull()
      }
    },
  )

  it('keeps a sending state visible while admission is in flight', async () => {
    mocks.listAgentChatMessages.mockResolvedValue({ items: [], next_cursor: null, has_more: false })
    mocks.listAgentChatTurns.mockResolvedValue([])

    renderTimeline({ isSending: true })
    await act(async () => {
      await vi.runOnlyPendingTimersAsync()
      await Promise.resolve()
      await Promise.resolve()
    })

    expect(screen.getByText(/Sending/)).toBeTruthy()
  })

  it('offers cancellation for a live turn using its current optimistic version', async () => {
    mocks.listAgentChatMessages.mockResolvedValue({
      items: [userMessage],
      next_cursor: null,
      has_more: false,
    })
    mocks.listAgentChatTurns.mockResolvedValue([{ ...queuedTurn, version: 7n }])

    const onCancelTurn = vi.fn(async () => undefined)
    renderTimeline({ onCancelTurn })
    await act(async () => {
      await vi.runOnlyPendingTimersAsync()
      await Promise.resolve()
      await Promise.resolve()
    })

    fireEvent.click(screen.getByRole('button', { name: 'Cancel turn' }))
    await vi.waitFor(() => expect(onCancelTurn).toHaveBeenCalledWith('turn-1', 7))
  })

  it('navigates an explicit handoff to its target Project Agent chat', async () => {
    const handoff = {
      id: 'handoff-1',
      source_chat_id: 'chat-1',
      source_message_id: 'message-1',
      source_turn_job_id: null,
      target_project_id: 'project-target',
      target_chat_id: 'project-chat',
      author_identity_id: 'agent-1',
      content: 'Continue the bounded Project brief.',
      content_guard: {},
      sensitivity: 'internal',
      status: 'delivered',
      target_message_id: 'message-target',
      target_turn_job_id: 'turn-target',
      dedupe_key: 'handoff-dedupe',
      correlation_id: 'correlation-handoff',
      causation_id: null,
      error: null,
      created_at: '2026-08-13T12:00:00Z',
      updated_at: '2026-08-13T12:00:01Z',
      delivered_at: '2026-08-13T12:00:01Z',
    }
    mocks.listAgentChatMessages.mockResolvedValue({
      items: [{ ...userMessage, handoff_id: handoff.id }],
      next_cursor: null,
      has_more: false,
    })
    mocks.listAgentChatTurns.mockResolvedValue([])
    mocks.listAgentHandoffs.mockResolvedValue([handoff])

    renderTimeline({ handoffProjectIds: ['project-target'] })
    await act(async () => {
      await vi.runOnlyPendingTimersAsync()
      await Promise.resolve()
      await Promise.resolve()
    })

    fireEvent.click(screen.getByRole('button', { name: 'Continue with Project Agent' }))
    expect(mocks.navigate).toHaveBeenCalledWith({
      to: '/projects/$projectId/chat',
      params: { projectId: 'project-target' },
    })
  })

  it('contains long message content inside the timeline without horizontal overflow classes', async () => {
    const longContent = `https://forge.example/${'a'.repeat(400)}`
    mocks.listAgentChatMessages.mockResolvedValue({
      items: [{ ...userMessage, content: longContent }],
      next_cursor: null,
      has_more: false,
    })
    mocks.listAgentChatTurns.mockResolvedValue([])

    const view = renderTimeline()
    await act(async () => {
      await vi.runOnlyPendingTimersAsync()
      await Promise.resolve()
      await Promise.resolve()
    })

    expect(screen.getByText(longContent)).toBeTruthy()
    expect(screen.getByRole('article', { name: /You message 1/ }).className).toContain(
      'overflow-hidden',
    )
    expect(screen.getByText(longContent).className).toContain('break-words')
    expect(view.container.querySelector('[aria-label="Chat timeline"]')?.className).toContain(
      'overflow-x-hidden',
    )
  })
})

describe('ChatComposer accessibility', () => {
  it('associates a disabled reason with the message field', () => {
    render(
      <ChatComposer
        disabled
        disabledReason="A finite turn is already in progress."
        onSend={vi.fn(async () => undefined)}
      />,
    )

    const textbox = screen.getByRole('textbox', { name: 'Chat message' })
    const describedBy = textbox.getAttribute('aria-describedby')
    expect(describedBy).toBeTruthy()
    expect(document.getElementById(describedBy ?? '')?.textContent).toContain(
      'A finite turn is already in progress.',
    )
  })
})
