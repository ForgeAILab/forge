import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { act, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { AgentChatTimeline, ChatComposer, parseTokenUsage } from './agent-chat-timeline'
import type { AgentChat, AgentChatMessage, AgentChatTurn } from '@/features/agent-chat/types'
import type { LogEntry } from '@/types/generated'

const mocks = vi.hoisted(() => ({
  listAgentChatMessages: vi.fn(),
  listAgentChatTurns: vi.fn(),
  listAgentChatTurnLogs: vi.fn(),
  listAgentHandoffs: vi.fn(),
  navigate: vi.fn(),
}))

vi.mock('@/features/agent-chat/api', () => ({
  listAgentChatMessages: mocks.listAgentChatMessages,
  listAgentChatTurns: mocks.listAgentChatTurns,
  listAgentChatTurnLogs: mocks.listAgentChatTurnLogs,
  listAgentHandoffs: mocks.listAgentHandoffs,
}))

const emptyLogPage = { items: [], has_more: false, next_sequence: null }

function turnLog(sequence: number, kind: LogEntry['kind'], payload: unknown): LogEntry {
  return {
    schema_version: 1,
    sequence,
    timestamp: `2026-08-13T12:00:${String(sequence).padStart(2, '0')}Z`,
    execution_id: 'turn-1',
    kind,
    stream: 'main',
    payload,
    truncated: false,
  }
}

const skillReadLogs: LogEntry[] = [
  turnLog(0, 'thinking', { text: 'Check the operating skill first.' }),
  turnLog(1, 'tool_call', {
    call_id: 'call-1',
    name: 'forge_project_orchestration_read',
    argument_keys: ['arguments', 'operation'],
  }),
  turnLog(2, 'tool_result', {
    call_id: 'call-1',
    name: 'forge_project_orchestration_read',
    is_error: false,
    success: true,
    summary: {
      status: 'succeeded',
      code: 'ok',
      safe_message: 'the tool call completed successfully',
      retryable: false,
      recovery_action: null,
      correlation_id: 'call-1',
      operation: 'skill.section',
    },
  }),
  turnLog(3, 'tool_call', {
    call_id: 'call-2',
    name: 'forge_task_command',
    argument_keys: ['args', 'program'],
  }),
]

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
    mocks.listAgentChatTurnLogs.mockResolvedValue(emptyLogPage)
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.clearAllMocks()
  })

  it('shows what a live turn is doing from its activity log', async () => {
    mocks.listAgentChatMessages.mockResolvedValue({
      items: [userMessage],
      next_cursor: null,
      has_more: false,
    })
    mocks.listAgentChatTurns.mockResolvedValue([{ ...queuedTurn, status: 'leased' }])
    mocks.listAgentChatTurnLogs.mockResolvedValue({
      items: skillReadLogs,
      has_more: false,
      next_sequence: 4,
    })

    renderTimeline()
    await act(async () => {
      await vi.runOnlyPendingTimersAsync()
      await Promise.resolve()
      await Promise.resolve()
    })

    // The in-flight command names the current step instead of a generic label.
    await vi.waitFor(() => expect(screen.getByText('Running a command…')).toBeTruthy())
    expect(screen.queryByText('Thinking…')).toBeNull()
    expect(screen.getByLabelText('Agent activity in progress')).toBeTruthy()
    expect(screen.getByText('Read the operating skill')).toBeTruthy()
    expect(screen.getByText('skill.section')).toBeTruthy()
    expect(screen.getByText('Running a command')).toBeTruthy()
    expect(mocks.listAgentChatTurnLogs).toHaveBeenCalledWith(
      'chat-1',
      'turn-1',
      expect.objectContaining({ from_sequence: 0 }),
    )
  })

  it('keeps a settled turn\'s activity under the reply it produced', async () => {
    mocks.listAgentChatMessages.mockResolvedValue({
      items: [userMessage, assistantMessage],
      next_cursor: null,
      has_more: false,
    })
    mocks.listAgentChatTurns.mockResolvedValue([completedTurn])
    mocks.listAgentChatTurnLogs.mockResolvedValue({
      items: skillReadLogs.slice(0, 3),
      has_more: false,
      next_sequence: 3,
    })

    renderTimeline()
    await act(async () => {
      await vi.runOnlyPendingTimersAsync()
      await Promise.resolve()
      await Promise.resolve()
    })

    const toggle = screen.getByRole('button', { name: 'Toggle agent activity' })
    await vi.waitFor(() =>
      expect(toggle.textContent).toContain('1 tool call · thought it through'),
    )
    expect(screen.queryByText('Read the operating skill')).toBeNull()
    fireEvent.click(toggle)
    expect(screen.getByText('Read the operating skill')).toBeTruthy()
    expect(screen.getByLabelText('Agent activity')).toBeTruthy()
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
    // A succeeded turn whose response message is in the timeline stays silent —
    // the agent message itself is the visible outcome.
    expect(screen.queryByText('Succeeded')).toBeNull()
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
        const retry = screen.getByRole('button', { name: 'Retry turn' })
        fireEvent.click(retry)
        await vi.waitFor(() => expect(onSend).toHaveBeenCalledWith('queued request'))
      } else {
        expect(screen.queryByRole('button', { name: 'Retry turn' })).toBeNull()
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

describe('AgentChatTimeline session dividers', () => {
  beforeEach(() => {
    vi.useFakeTimers()
    mocks.listAgentChatTurns.mockResolvedValue([])
    mocks.listAgentHandoffs.mockResolvedValue([])
    mocks.listAgentChatTurnLogs.mockResolvedValue(emptyLogPage)
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.clearAllMocks()
  })

  it('divides messages that are hours apart into separate sessions', async () => {
    mocks.listAgentChatMessages.mockResolvedValue({
      items: [
        userMessage,
        { ...assistantMessage, created_at: '2026-08-13T18:00:00Z' },
      ],
      next_cursor: null,
      has_more: false,
    })

    renderTimeline()
    await act(async () => {
      await vi.runOnlyPendingTimersAsync()
      await Promise.resolve()
      await Promise.resolve()
    })

    expect(screen.getByRole('separator')).toBeTruthy()
  })

  it('keeps a close conversation as one session', async () => {
    mocks.listAgentChatMessages.mockResolvedValue({
      items: [userMessage, assistantMessage],
      next_cursor: null,
      has_more: false,
    })

    renderTimeline()
    await act(async () => {
      await vi.runOnlyPendingTimersAsync()
      await Promise.resolve()
      await Promise.resolve()
    })

    expect(screen.queryByRole('separator')).toBeNull()
  })
})

describe('AgentChatTimeline wake prompts', () => {
  const wakeMessage: AgentChatMessage = {
    ...userMessage,
    id: 'message-wake',
    author_type: 'system',
    outcome: 'attention_wake',
    content: [
      '### Attention wake: Task completed; reconcile validation, evidence, and readiness',
      '',
      'Category: delivery_followup — recommended action: reconcile_delivery.',
      'Incident: attention:delivery_followup:project:p1:task:t1',
      'DELIVERY FOLLOW-UP WORK ORDER',
      'Settle yourself, in this turn: budget-semantics, category-lifecycle.',
    ].join('\n'),
  }

  beforeEach(() => {
    vi.useFakeTimers()
    mocks.listAgentChatMessages.mockResolvedValue({
      items: [wakeMessage],
      next_cursor: null,
      has_more: false,
    })
    mocks.listAgentChatTurns.mockResolvedValue([])
    mocks.listAgentHandoffs.mockResolvedValue([])
    mocks.listAgentChatTurnLogs.mockResolvedValue(emptyLogPage)
  })

  afterEach(() => {
    vi.useRealTimers()
    vi.clearAllMocks()
  })

  it('collapses a wake to its summary and reveals the full prompt on demand', async () => {
    renderTimeline()
    await act(async () => {
      await vi.runOnlyPendingTimersAsync()
      await Promise.resolve()
      await Promise.resolve()
    })

    const toggle = screen.getByRole('button', { name: /Attention wake/ })
    expect(toggle.textContent).toContain(
      'Task completed; reconcile validation, evidence, and readiness',
    )
    expect(toggle.getAttribute('aria-expanded')).toBe('false')
    expect(screen.queryByText(/DELIVERY FOLLOW-UP WORK ORDER/)).toBeNull()
    expect(screen.queryByText(/### Attention wake/)).toBeNull()

    fireEvent.click(toggle)

    expect(toggle.getAttribute('aria-expanded')).toBe('true')
    const prompt = screen.getByText(/DELIVERY FOLLOW-UP WORK ORDER/)
    expect(prompt.textContent).toContain('recommended action: reconcile_delivery')
    expect(prompt.textContent).not.toContain('### Attention wake')

    fireEvent.click(toggle)
    expect(screen.queryByText(/DELIVERY FOLLOW-UP WORK ORDER/)).toBeNull()
  })
})

describe('parseTokenUsage', () => {
  it('shows the cached share of the input next to the totals', () => {
    expect(parseTokenUsage({ input: 77535, output: 898, cache_read: 61440, cache_write: 0 })).toBe(
      '77,535 in · 61,440 cached · 898 out',
    )
    expect(parseTokenUsage({ input: 100, output: 5, cache_read: 0, cache_write: 90 })).toBe(
      '100 in · 90 cache write · 5 out',
    )
  })

  it('still reads usage recorded before cache counts existed', () => {
    expect(parseTokenUsage({ input: 725218, output: 11942 })).toBe('725,218 in · 11,942 out')
    expect(parseTokenUsage({})).toBeNull()
  })
})

describe('ChatComposer commands', () => {
  it('opens the / menu, inserts the picked command, and runs it with its argument', async () => {
    const run = vi.fn(async () => undefined)
    const onSend = vi.fn(async () => undefined)
    render(
      <ChatComposer
        onSend={onSend}
        commands={[
          { name: 'start-product', description: 'Start Product Genesis', run },
        ]}
      />,
    )

    const textbox = screen.getByRole('textbox', { name: 'Chat message' }) as HTMLTextAreaElement
    fireEvent.change(textbox, { target: { value: '/start' } })
    expect(screen.getByRole('option', { name: /start-product/ })).toBeTruthy()

    fireEvent.keyDown(textbox, { key: 'Enter' })
    expect(textbox.value).toBe('/start-product ')

    fireEvent.change(textbox, { target: { value: '/start-product an ice cream app' } })
    fireEvent.click(screen.getByRole('button', { name: 'Send message' }))
    await vi.waitFor(() => expect(run).toHaveBeenCalledWith('an ice cream app'))
    expect(onSend).not.toHaveBeenCalled()
  })

  it('rejects an unknown command without sending it as a message', async () => {
    const onSend = vi.fn(async () => undefined)
    render(
      <ChatComposer
        onSend={onSend}
        commands={[
          {
            name: 'start-product',
            description: 'Start Product Genesis',
            run: vi.fn(async () => undefined),
          },
        ]}
      />,
    )

    const textbox = screen.getByRole('textbox', { name: 'Chat message' })
    fireEvent.change(textbox, { target: { value: '/nope do it' } })
    fireEvent.click(screen.getByRole('button', { name: 'Send message' }))
    await vi.waitFor(() =>
      expect(screen.getByRole('alert').textContent).toContain('Unknown command /nope'),
    )
    expect(onSend).not.toHaveBeenCalled()
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
