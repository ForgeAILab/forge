import { Fragment, useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { ArrowDown, ChatCircleDots } from '@phosphor-icons/react'
import { Button } from '@/components/ui/button'
import { ErrorPanel, EmptyPanel, LoadingPanel } from '@/features/federation/components'
import { LoadingState } from '@/components/chat/loading-state'
import {
  useAgentChatMessagesQuery,
  useAgentChatTurnsQuery,
  useAgentHandoffsForProjectsQuery,
} from '@/features/agent-chat/hooks'
import type {
  AgentChat,
  AgentChatMessage,
  AgentChatTurn,
  AgentHandoff,
} from '@/features/agent-chat/types'
import { useChatSelection } from '@/stores/chat'
import { type ChatCommand, ChatComposer } from './agent-chat-timeline/composer'
import {
  toNumber,
  isLiveTurn,
  normalizeTurnState,
  sessionGap,
  formatDate,
} from './agent-chat-timeline/shared'
import {
  TurnActivity,
  TimelineDivider,
  UserMessage,
  WakeMessage,
  SystemMessage,
  AgentMessage,
} from './agent-chat-timeline/messages'

const EMPTY_PENDING_TURNS: AgentChatTurn[] = []

/** How many of the newest agent replies load their activity summary eagerly. */
const RECENT_ACTIVITY_PREFETCH = 3

export function AgentChatTimeline({
  chat,
  agentName,
  projectId,
  handoffProjectIds,
  isSending,
  onSend,
  onCancelTurn,
  onRetryTurn,
  commands,
}: {
  chat: AgentChat
  agentName?: string
  projectId?: string
  handoffProjectIds?: string[]
  isSending?: boolean
  onSend: (content: string) => Promise<void>
  onCancelTurn?: (turnId: string, expectedVersion: number) => Promise<void>
  onRetryTurn?: (turnId: string, expectedVersion: number) => Promise<void>
  commands?: ChatCommand[]
}) {
  const messagesQuery = useAgentChatMessagesQuery(chat.id)
  const turnsQuery = useAgentChatTurnsQuery(chat.id)
  const handoffsQuery = useAgentHandoffsForProjectsQuery(
    handoffProjectIds ?? (projectId ? [projectId] : []),
  )
  const pendingTurns = useChatSelection(
    (state) => state.pendingTurns[chat.id] ?? EMPTY_PENDING_TURNS,
  )
  const clearPendingTurn = useChatSelection((state) => state.clearPendingTurn)
  const scrollRef = useRef<HTMLDivElement>(null)
  const endRef = useRef<HTMLDivElement>(null)
  const [autoScroll, setAutoScroll] = useState(true)
  const [retrying, setRetrying] = useState(false)
  const [cancelingTurnId, setCancelingTurnId] = useState<string | null>(null)
  const [cancelError, setCancelError] = useState<string | null>(null)
  // Bumped whenever a live turn's activity feed grows, so auto-scroll keeps
  // following the Agent's work and not only whole messages.
  const [activityVersion, setActivityVersion] = useState(0)
  const bumpActivity = useCallback(() => setActivityVersion((current) => current + 1), [])
  const messages = useMemo(
    () =>
      [...(messagesQuery.data?.items ?? [])].sort(
        (a, b) => toNumber(a.sequence) - toNumber(b.sequence),
      ),
    [messagesQuery.data],
  )
  const turns = useMemo(() => {
    const byId = new Map<string, AgentChatTurn>()
    for (const turn of pendingTurns) byId.set(turn.id, turn)
    for (const turn of turnsQuery.data ?? []) byId.set(turn.id, turn)
    return [...byId.values()]
  }, [pendingTurns, turnsQuery.data])
  const handoffs = handoffsQuery.data
  const turnInFlight = turns.some(isLiveTurn)
  const messageIds = useMemo(() => new Set(messages.map((message) => message.id)), [messages])
  // Succeeded turns whose response already appears in the timeline stay
  // silent — the agent message itself is the visible outcome.
  const visibleTurns = useMemo(
    () =>
      turns.filter(
        (turn) =>
          !(
            normalizeTurnState(turn.status) === 'succeeded' &&
            turn.response_message_id &&
            messageIds.has(turn.response_message_id)
          ),
      ),
    [messageIds, turns],
  )
  const turnsByMessage = useMemo(() => {
    const byMessage = new Map<string, AgentChatTurn[]>()
    for (const turn of visibleTurns) {
      const existing = byMessage.get(turn.input_message_id)
      if (existing) existing.push(turn)
      else byMessage.set(turn.input_message_id, [turn])
    }
    return byMessage
  }, [visibleTurns])
  const orphanTurns = useMemo(
    () => visibleTurns.filter((turn) => !messageIds.has(turn.input_message_id)),
    [messageIds, visibleTurns],
  )
  // The turn that produced each agent message, so its recorded activity can
  // sit with the reply it led to.
  const turnByResponseMessage = useMemo(() => {
    const byMessage = new Map<string, AgentChatTurn>()
    for (const turn of turns) {
      if (turn.response_message_id) byMessage.set(turn.response_message_id, turn)
    }
    return byMessage
  }, [turns])
  const recentAgentMessageIds = useMemo(
    () =>
      new Set(
        messages
          .filter((message) => message.author_type === 'agent')
          .slice(-RECENT_ACTIVITY_PREFETCH)
          .map((message) => message.id),
      ),
    [messages],
  )
  const canRetry = chat.status === 'ready' && !turnInFlight && !retrying

  function handoffFor(message: AgentChatMessage): AgentHandoff | undefined {
    return message.handoff_id
      ? handoffs.find((candidate) => candidate.id === message.handoff_id)
      : handoffs.find((candidate) => candidate.source_message_id === message.id)
  }

  async function cancelTurn(turn: AgentChatTurn) {
    if (!onCancelTurn) return
    setCancelError(null)
    setCancelingTurnId(turn.id)
    try {
      await onCancelTurn(turn.id, toNumber(turn.version))
    } catch (cause) {
      setCancelError(cause instanceof Error ? cause.message : 'The turn could not be cancelled.')
    } finally {
      setCancelingTurnId(null)
    }
  }

  async function retryTurn(turn: AgentChatTurn) {
    if (!onRetryTurn || !turn.retry_action) return
    setRetrying(true)
    try {
      await onRetryTurn(turn.id, toNumber(turn.retry_action.expected_version))
    } finally {
      setRetrying(false)
    }
  }

  useEffect(() => {
    const serverTurns = new Set((turnsQuery.data ?? []).map((turn) => turn.id))
    for (const turn of pendingTurns) {
      if (serverTurns.has(turn.id)) {
        clearPendingTurn(chat.id, turn.id)
      }
    }
  }, [chat.id, clearPendingTurn, pendingTurns, turnsQuery.data])

  useEffect(() => {
    setAutoScroll(true)
  }, [chat.id])

  useEffect(() => {
    if (!autoScroll) return
    endRef.current?.scrollIntoView?.({ block: 'end' })
  }, [autoScroll, messages.length, turns.length, activityVersion])

  if (messagesQuery.isLoading) return <LoadingPanel label="Loading chat timeline" />
  if (messagesQuery.isError) {
    return (
      <ErrorPanel
        title="Chat timeline unavailable"
        description="The server could not load this Agent Chat timeline. Your next message is not admitted until it reconnects."
        onRetry={() => void messagesQuery.refetch()}
      />
    )
  }

  function renderTurn(turn: AgentChatTurn) {
    return (
      <TurnActivity
        key={turn.id}
        chatId={chat.id}
        turn={turn}
        onCancel={onCancelTurn && isLiveTurn(turn) ? cancelTurn : undefined}
        canceling={cancelingTurnId === turn.id}
        onRetry={() => retryTurn(turn)}
        canRetry={canRetry && !!onRetryTurn && !!turn.retry_action && !cancelingTurnId}
        onActivityChange={bumpActivity}
      />
    )
  }

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div
        ref={scrollRef}
        className="min-h-0 min-w-0 flex-1 overflow-x-hidden overflow-y-auto p-4 sm:p-5"
        aria-label="Chat timeline"
        onScroll={(event) => {
          const element = event.currentTarget
          setAutoScroll(element.scrollHeight - element.scrollTop - element.clientHeight < 96)
        }}
      >
        <div className="mx-auto flex w-full max-w-3xl flex-col gap-5">
          {messages.length === 0 ? (
            <div className="flex flex-col items-center justify-center p-6 text-center">
              <EmptyPanel
                title={projectId ? 'Start the conversation' : 'What do you want to make?'}
                description={
                  projectId
                    ? 'Ask this Project Agent to plan work, create Tasks, or shape Project records.'
                    : 'Describe a new Project in your own words, ask a portfolio question, or explore an idea. Clear new-Project intent starts discovery; it does not approve or create the Project.'
                }
                icon={<ChatCircleDots size={19} aria-hidden />}
              />
              {commands && commands.length > 0 ? (
                <p className="mt-3 text-xs text-muted-foreground">
                  Type <span className="font-mono text-foreground">/</span> for commands — try{' '}
                  <span className="font-mono text-foreground">/{commands[0].name}</span>
                </p>
              ) : null}
            </div>
          ) : null}
          {messages.map((message, index) => (
            <Fragment key={message.id}>
              {sessionGap(messages[index - 1], message) ? (
                <TimelineDivider label={formatDate(message.created_at)} />
              ) : null}
              {message.author_type === 'user' ? (
                <UserMessage message={message} handoff={handoffFor(message)} />
              ) : message.outcome === 'topic_started' ? (
                // The visible Main Chat topic-boundary divider (D21/F18): a
                // real, durable, immutable message like any other, rendered
                // as a timeline separator rather than a chat bubble so a
                // fresh topic reads as a context break, not more chatter.
                <TimelineDivider label={message.content} />
              ) : message.outcome === 'attention_wake' ? (
                // A wake prompt is a full work order; the timeline shows its
                // summary line and keeps the prompt behind a toggle.
                <WakeMessage message={message} />
              ) : message.author_type === 'system' ? (
                <SystemMessage message={message} />
              ) : (
                <AgentMessage
                  message={message}
                  agentName={agentName}
                  chat={chat}
                  handoff={handoffFor(message)}
                  turn={turnByResponseMessage.get(message.id)}
                  prefetchActivity={recentAgentMessageIds.has(message.id)}
                />
              )}
              {(turnsByMessage.get(message.id) ?? []).map((turn) => renderTurn(turn))}
            </Fragment>
          ))}
          {orphanTurns.map((turn) => renderTurn(turn))}
          {isSending && !turnInFlight ? (
            <LoadingState compact label="Sending…" status="sending" />
          ) : null}
          <div ref={endRef} />
        </div>
      </div>
      {!autoScroll ? (
        <div className="border-t border-border-subtle bg-card px-4 py-2 text-center">
          <Button type="button" variant="outline" size="sm" onClick={() => setAutoScroll(true)}>
            <ArrowDown size={13} aria-hidden />
            Jump to latest
          </Button>
        </div>
      ) : null}
      {cancelError ? (
        <p
          className="border-t border-border-subtle bg-card px-4 py-2 text-xs text-destructive"
          role="alert"
        >
          {cancelError}
        </p>
      ) : null}
      <ChatComposer
        disabled={chat.status !== 'ready' || turnInFlight}
        disabledReason={
          chat.status !== 'ready'
            ? 'This Agent Chat is not ready for turns.'
            : turnInFlight
              ? 'A finite turn is already in progress. Wait for its terminal state.'
              : undefined
        }
        isSending={isSending || retrying}
        onSend={onSend}
        commands={commands}
        placeholder={
          projectId
            ? 'Message this Project Agent…'
            : 'Describe a new Project, ask about your portfolio, or explore an idea…'
        }
      />
    </div>
  )
}

export { parseTokenUsage } from './agent-chat-timeline/shared'
export { parseWakePrompt } from './agent-chat-timeline/shared'
export { ChatComposer } from './agent-chat-timeline/composer'
export type { ChatCommand } from './agent-chat-timeline/composer'
