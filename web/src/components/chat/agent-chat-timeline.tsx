import { useEffect, useId, useMemo, useRef, useState } from 'react'
import {
  ArrowDown,
  ArrowUpRight,
  CheckCircle,
  ChatCircleDots,
  CircleNotch,
  Flask,
  PaperPlaneTilt,
  Robot,
  UserCircle,
  WarningCircle,
  XCircle,
} from '@phosphor-icons/react'
import { useNavigate } from '@tanstack/react-router'
import { Button } from '@/components/ui/button'
import { Textarea } from '@/components/ui/textarea'
import { ContextManifestDialog } from '@/features/federation/ContextManifestInspector'
import { ErrorPanel, EmptyPanel, LoadingPanel, StateBadge } from '@/features/federation/components'
import { ErrorCard } from '@/components/chat/error-card'
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
import { cn } from '@/lib/cn'

type TurnState =
  | 'sending'
  | 'queued'
  | 'leased'
  | 'awaiting_input'
  | 'running'
  | 'retry_wait'
  | 'succeeded'
  | 'failed'
  | 'cancelled'

const EMPTY_PENDING_TURNS: AgentChatTurn[] = []

function formatDate(value: string | null | undefined): string {
  if (!value) return 'No timestamp'
  const date = new Date(value)
  return Number.isNaN(date.getTime())
    ? value
    : date.toLocaleString([], { dateStyle: 'medium', timeStyle: 'short' })
}

function toNumber(value: bigint | number): number {
  return typeof value === 'bigint' ? Number(value) : value
}

function authorLabel(message: AgentChatMessage, agentName?: string): string {
  if (message.author_type === 'agent') return agentName ?? 'Agent'
  if (message.author_type === 'handoff') return 'Main Agent handoff'
  if (message.author_type === 'system') return 'Forge'
  return 'You'
}

function normalizeTurnState(status: string | undefined): TurnState {
  const value = status?.toLowerCase()
  if (value === 'leased') return 'leased'
  if (value === 'awaiting_input' || value === 'awaiting') return 'awaiting_input'
  if (value === 'running' || value === 'processing') return 'running'
  if (value === 'retry_wait' || value === 'retrying' || value === 'retry') return 'retry_wait'
  if (
    value === 'succeeded' ||
    value === 'completed' ||
    value === 'complete' ||
    value === 'responded'
  ) {
    return 'succeeded'
  }
  if (value === 'failed' || value === 'error') return 'failed'
  if (value === 'cancelled' || value === 'canceled') return 'cancelled'
  return 'queued'
}

function turnLabel(state: TurnState): string {
  return {
    sending: 'Sending…',
    queued: 'Queued…',
    leased: 'Thinking…',
    awaiting_input: 'Awaiting input…',
    running: 'Thinking…',
    retry_wait: 'Retrying…',
    succeeded: 'Succeeded',
    failed: 'Failed',
    cancelled: 'Cancelled',
  }[state]
}

function turnIcon(state: TurnState) {
  if (
    state === 'sending' ||
    state === 'queued' ||
    state === 'leased' ||
    state === 'awaiting_input' ||
    state === 'running' ||
    state === 'retry_wait'
  ) {
    return <CircleNotch size={14} className="animate-spin" aria-hidden />
  }
  if (state === 'succeeded') return <CheckCircle size={14} aria-hidden />
  if (state === 'failed') return <WarningCircle size={14} aria-hidden />
  return <XCircle size={14} aria-hidden />
}

function turnTone(state: TurnState): string {
  if (state === 'succeeded') return 'border-success/30 bg-success/10 text-success'
  if (state === 'failed') return 'border-destructive/30 bg-destructive/10 text-destructive'
  if (state === 'cancelled') return 'border-border bg-muted text-muted-foreground'
  return 'border-ember-border bg-ember-surface text-foreground'
}

function isLiveTurn(turn: AgentChatTurn): boolean {
  const state = normalizeTurnState(turn.status)
  return (
    state === 'sending' ||
    state === 'queued' ||
    state === 'leased' ||
    state === 'awaiting_input' ||
    state === 'running' ||
    state === 'retry_wait'
  )
}

function TurnStateCard({
  state,
  detail,
  startedAt,
}: {
  state: TurnState
  detail?: string | null
  startedAt?: string | null
}) {
  if (
    state === 'sending' ||
    state === 'queued' ||
    state === 'leased' ||
    state === 'awaiting_input' ||
    state === 'running' ||
    state === 'retry_wait'
  ) {
    return (
      <LoadingState
        label={turnLabel(state)}
        status={state}
        startedAt={startedAt}
      />
    )
  }

  if (state === 'failed') {
    return (
      <ErrorCard
        title="Turn failed"
        description={detail ?? 'The agent turn could not complete.'}
        severity="error"
      />
    )
  }

  return (
    <div
      className={cn('flex items-center gap-2 rounded-lg border px-3 py-2 text-xs', turnTone(state))}
      role="status"
      aria-live="polite"
    >
      <span className="shrink-0">{turnIcon(state)}</span>
      <span className="font-medium">{turnLabel(state)}</span>
      {detail ? <span className="min-w-0 truncate text-muted-foreground">{detail}</span> : null}
    </div>
  )
}

function HandoffAction({ handoff }: { handoff: AgentHandoff }) {
  const navigate = useNavigate()

  return (
    <div className="mt-3 flex flex-wrap items-center justify-between gap-3 rounded-lg border border-ember-border bg-ember-surface px-3 py-2">
      <div className="flex min-w-0 items-center gap-2 text-xs text-foreground">
        <ArrowUpRight size={14} className="shrink-0 text-primary" aria-hidden />
        <span className="truncate">{handoff.content || 'Continue with the Project Agent.'}</span>
      </div>
      <Button
        type="button"
        variant="outline"
        size="sm"
        onClick={() =>
          void navigate({
            to: '/projects/$projectId/chat',
            params: { projectId: handoff.target_project_id },
          })
        }
      >
        Continue with Project Agent
        <ArrowUpRight size={13} aria-hidden />
      </Button>
    </div>
  )
}

function MessageCard({
  message,
  agentName,
  chat,
  handoffs,
  turns,
  onRetry,
  canRetry,
  onCancelTurn,
  cancelingTurnId,
  canCancelTurn,
}: {
  message: AgentChatMessage
  agentName?: string
  chat: AgentChat
  handoffs: AgentHandoff[]
  turns: AgentChatTurn[]
  onRetry: (content: string) => Promise<void>
  canRetry: boolean
  onCancelTurn?: (turn: AgentChatTurn) => Promise<void>
  cancelingTurnId?: string | null
  canCancelTurn?: boolean
}) {
  const [retrying, setRetrying] = useState(false)
  const [retryError, setRetryError] = useState<string | null>(null)
  const isAgent = message.author_type === 'agent' || message.author_type === 'handoff'
  const handoff = message.handoff_id
    ? handoffs.find((candidate) => candidate.id === message.handoff_id)
    : handoffs.find((candidate) => candidate.source_message_id === message.id)
  const turnsForMessage = turns.filter((turn) => turn.input_message_id === message.id)
  const terminalTurn = turnsForMessage.find((turn) => {
    const state = normalizeTurnState(turn.status)
    return state === 'failed' || state === 'cancelled'
  })

  async function retry() {
    setRetryError(null)
    setRetrying(true)
    try {
      await onRetry(message.content)
    } catch (cause) {
      setRetryError(cause instanceof Error ? cause.message : 'The turn could not be retried.')
    } finally {
      setRetrying(false)
    }
  }

  return (
    <article
      aria-label={`${authorLabel(message, agentName)} message ${toNumber(message.sequence)}`}
      className={cn(
        'min-w-0 max-w-full overflow-hidden rounded-xl border px-4 py-3 shadow-xs sm:max-w-3xl',
        isAgent
          ? 'mr-auto border-border-subtle bg-card'
          : 'ml-auto border-ember-border bg-ember-surface',
      )}
    >
      <header className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex min-w-0 items-center gap-2">
          <span className="flex h-6 w-6 shrink-0 items-center justify-center rounded-full bg-muted text-muted-foreground">
            {isAgent ? <Robot size={13} aria-hidden /> : <UserCircle size={13} aria-hidden />}
          </span>
          <span className="truncate text-xs font-semibold text-foreground">
            {authorLabel(message, agentName)}
          </span>
          <StateBadge status={message.status} label={message.status} />
        </div>
        <span className="font-mono text-micro text-muted-foreground">
          #{toNumber(message.sequence)} · {formatDate(message.created_at)}
        </span>
      </header>
      <p className="mt-3 whitespace-pre-wrap break-words text-sm leading-6 text-foreground">
        {message.content}
      </p>
      {message.error ? (
        <p className="mt-3 rounded-md border border-destructive/30 bg-destructive/5 px-3 py-2 text-xs text-destructive">
          {message.error}
        </p>
      ) : null}
      <footer className="mt-3 flex flex-wrap items-center justify-between gap-3 border-t border-border-subtle pt-2 text-xs text-muted-foreground">
        <span className="min-w-0 truncate">
          {message.outcome ??
            (message.model
              ? `${message.model}${message.duration_ms ? ` · ${message.duration_ms}ms` : ''}`
              : 'Recorded in the chat timeline')}
        </span>
        {isAgent && message.context_manifest_id ? (
          <ContextManifestDialog
            initialManifestId={message.context_manifest_id}
            initialIdentityId={message.author_id ?? undefined}
            initialContextScopeId={chat.id}
            label="Inspect provenance"
            contextHint="this turn"
          />
        ) : null}
      </footer>
      {turnsForMessage.map((turn) => (
        <div key={turn.id} className="mt-3 flex min-w-0 flex-wrap items-center gap-2">
          <div className="min-w-0 flex-1">
            <TurnStateCard
              state={normalizeTurnState(turn.status)}
              detail={turn.error}
              startedAt={turn.created_at}
            />
          </div>
          {isLiveTurn(turn) && onCancelTurn ? (
            <Button
              type="button"
              size="sm"
              variant="outline"
              onClick={() => void onCancelTurn(turn)}
              disabled={!canCancelTurn || cancelingTurnId === turn.id}
              aria-label="Cancel turn"
            >
              {cancelingTurnId === turn.id ? (
                <CircleNotch size={13} className="animate-spin" aria-hidden />
              ) : null}
              {cancelingTurnId === turn.id ? 'Cancelling…' : 'Cancel turn'}
            </Button>
          ) : null}
        </div>
      ))}
      {terminalTurn ? (
        <div className="mt-3 flex flex-wrap items-center justify-between gap-2 border-t border-border-subtle pt-3">
          <p className="min-w-0 text-xs text-muted-foreground">
            {canRetry
              ? 'This turn is terminal. Retrying starts a new bounded turn with the same request.'
              : 'Retry becomes available when this Agent Chat is ready and no other turn is active.'}
          </p>
          <Button
            type="button"
            size="sm"
            variant="outline"
            onClick={() => void retry()}
            disabled={retrying || !canRetry}
            aria-label={`Retry ${authorLabel(message, agentName)} turn`}
          >
            {retrying ? <CircleNotch size={13} className="animate-spin" aria-hidden /> : null}
            {retrying ? 'Retrying…' : 'Retry turn'}
          </Button>
          {retryError ? (
            <p className="basis-full text-xs text-destructive" role="alert">
              {retryError}
            </p>
          ) : null}
        </div>
      ) : null}
      {handoff ? <HandoffAction handoff={handoff} /> : null}
    </article>
  )
}

export function ChatComposer({
  disabled,
  disabledReason,
  isSending,
  onSend,
  onStartGenesis,
  isStartingGenesis,
}: {
  disabled?: boolean
  disabledReason?: string
  isSending?: boolean
  onSend: (content: string) => Promise<void>
  onStartGenesis?: (initialIdea: string) => Promise<void>
  isStartingGenesis?: boolean
}) {
  const [content, setContent] = useState('')
  const [error, setError] = useState<string | null>(null)
  const formRef = useRef<HTMLFormElement>(null)
  const statusId = useId()
  const describedBy = [disabledReason ? `${statusId}-reason` : null, error ? `${statusId}-error` : null]
    .filter(Boolean)
    .join(' ')

  async function submit(event: React.FormEvent) {
    event.preventDefault()
    const value = content.trim()
    if (!value || disabled || isSending) return
    setError(null)
    try {
      await onSend(value)
      setContent('')
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'The message could not be sent.')
    }
  }

  return (
    <form ref={formRef} onSubmit={submit} className="border-t border-border-subtle bg-muted/20 p-4">
      {disabledReason ? (
        <p id={`${statusId}-reason`} className="mb-3 text-xs text-muted-foreground" role="status">
          {disabledReason}
        </p>
      ) : null}
      {error ? (
        <p id={`${statusId}-error`} className="mb-3 text-xs text-destructive" role="alert">
          {error}
        </p>
      ) : null}
      <div className="flex items-end gap-2">
        <Textarea
          value={content}
          onChange={(event) => setContent(event.target.value)}
          onKeyDown={(event) => {
            if (event.key !== 'Enter' || event.shiftKey || event.nativeEvent.isComposing) return
            event.preventDefault()
            formRef.current?.requestSubmit()
          }}
          placeholder="Ask this agent to take the next bounded step…"
          rows={2}
          disabled={disabled || isSending || isStartingGenesis}
          aria-label="Chat message"
          aria-describedby={describedBy || undefined}
          className="min-w-0 flex-1"
        />
        {onStartGenesis ? (
          <Button
            type="button"
            variant="outline"
            onClick={async () => {
              const idea = content.trim()
              setError(null)
              try {
                await onStartGenesis(idea)
                setContent('')
              } catch (cause) {
                setError(cause instanceof Error ? cause.message : 'Product Genesis could not start.')
              }
            }}
            disabled={disabled || isSending || isStartingGenesis}
            aria-label="Start a product"
            className="gap-1.5 h-10 px-3 text-xs font-medium shrink-0"
          >
            {isStartingGenesis ? (
              <CircleNotch size={14} className="animate-spin" aria-hidden />
            ) : (
              <Flask size={14} aria-hidden />
            )}
            Start a product
          </Button>
        ) : null}
        <Button
          type="submit"
          size="icon"
          disabled={disabled || isSending || !content.trim()}
          aria-label={isSending ? 'Sending message' : 'Send message'}
        >
          {isSending ? (
            <CircleNotch size={16} className="animate-spin" aria-hidden />
          ) : (
            <PaperPlaneTilt size={16} aria-hidden />
          )}
        </Button>
      </div>
      <p className="mt-2 text-micro text-muted-foreground">
        Enter sends; Shift+Enter adds a line break. One finite turn is admitted at a time.
      </p>
    </form>
  )
}

export function AgentChatTimeline({
  chat,
  agentName,
  projectId,
  handoffProjectIds,
  isSending,
  onSend,
  onCancelTurn,
  onStartGenesis,
  isStartingGenesis,
}: {
  chat: AgentChat
  agentName?: string
  projectId?: string
  handoffProjectIds?: string[]
  isSending?: boolean
  onSend: (content: string) => Promise<void>
  onCancelTurn?: (turnId: string, expectedVersion: number) => Promise<void>
  onStartGenesis?: (initialIdea: string) => Promise<void>
  isStartingGenesis?: boolean
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
  }, [autoScroll, messages.length, turns.length])

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

  return (
    <div className="flex min-h-0 flex-1 flex-col">
      <div
        ref={scrollRef}
        className="min-h-0 min-w-0 flex-1 space-y-3 overflow-x-hidden overflow-y-auto p-4 sm:p-5"
        aria-label="Chat timeline"
        onScroll={(event) => {
          const element = event.currentTarget
          setAutoScroll(element.scrollHeight - element.scrollTop - element.clientHeight < 96)
        }}
      >
        {messages.length === 0 ? (
          <div className="flex flex-col items-center justify-center p-6 text-center">
            <EmptyPanel
              title="No turns yet"
              description="Send a bounded request. Forge will show queued, running, retrying, and terminal turn states here."
              icon={<ChatCircleDots size={19} aria-hidden />}
            />
            {onStartGenesis ? (
              <div className="mt-4">
                <Button
                  type="button"
                  onClick={async () => {
                    await onStartGenesis('')
                  }}
                  disabled={isStartingGenesis || isSending}
                  className="gap-2"
                >
                  {isStartingGenesis ? (
                    <CircleNotch size={15} className="animate-spin" aria-hidden />
                  ) : (
                    <Flask size={15} aria-hidden />
                  )}
                  Start a product
                </Button>
              </div>
            ) : null}
          </div>
        ) : null}
        {messages.map((message) => (
          <MessageCard
            key={message.id}
            message={message}
            agentName={agentName}
            chat={chat}
            handoffs={handoffs}
            turns={turns}
            canRetry={chat.status === 'ready' && !turnInFlight && !retrying}
            onCancelTurn={onCancelTurn ? cancelTurn : undefined}
            cancelingTurnId={cancelingTurnId}
            canCancelTurn={!cancelingTurnId}
            onRetry={async (content) => {
              setRetrying(true)
              try {
                await onSend(content)
              } finally {
                setRetrying(false)
              }
            }}
          />
        ))}
        {turns
          .filter((turn) => !messages.some((message) => message.id === turn.input_message_id))
          .map((turn) => (
            <div key={turn.id} className="flex min-w-0 flex-wrap items-center gap-2">
              <div className="min-w-0 flex-1">
                <TurnStateCard
                  state={normalizeTurnState(turn.status)}
                  detail={turn.error}
                  startedAt={turn.created_at}
                />
              </div>
              {isLiveTurn(turn) && onCancelTurn ? (
                <Button
                  type="button"
                  size="sm"
                  variant="outline"
                  onClick={() => void cancelTurn(turn)}
                  disabled={Boolean(cancelingTurnId)}
                  aria-label="Cancel turn"
                >
                  {cancelingTurnId === turn.id ? (
                    <CircleNotch size={13} className="animate-spin" aria-hidden />
                  ) : null}
                  {cancelingTurnId === turn.id ? 'Cancelling…' : 'Cancel turn'}
                </Button>
              ) : null}
            </div>
          ))}
        {isSending && !turnInFlight ? <TurnStateCard state="sending" /> : null}
        <div ref={endRef} />
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
        onStartGenesis={onStartGenesis}
        isStartingGenesis={isStartingGenesis}
      />
    </div>
  )
}
