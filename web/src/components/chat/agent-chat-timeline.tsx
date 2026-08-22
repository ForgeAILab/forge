import { Fragment, useEffect, useId, useMemo, useRef, useState } from 'react'
import {
  ArrowDown,
  ArrowUpRight,
  CaretDown,
  Check,
  CheckCircle,
  ChatCircleDots,
  CircleNotch,
  Copy,
  PaperPlaneTilt,
  WarningCircle,
  XCircle,
} from '@phosphor-icons/react'
import { useNavigate } from '@tanstack/react-router'
import { Button } from '@/components/ui/button'
import { Textarea } from '@/components/ui/textarea'
import { ContextManifestDialog } from '@/features/federation/ContextManifestInspector'
import { ErrorPanel, EmptyPanel, LoadingPanel } from '@/features/federation/components'
import { ChatMarkdown } from '@/components/chat/chat-markdown'
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

/** Gap between two messages after which the timeline shows a session divider. */
const SESSION_GAP_MS = 2 * 60 * 60 * 1000

export type ChatCommand = {
  name: string
  description: string
  run: (argument: string) => Promise<void>
}

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

function formatDuration(ms: bigint | number | null | undefined): string | null {
  if (ms === null || ms === undefined) return null
  const value = toNumber(ms)
  if (value < 1_000) return `${value}ms`
  return `${(value / 1_000).toFixed(value < 10_000 ? 1 : 0)}s`
}

function parseTokenUsage(json: Record<string, unknown> | null): string | null {
  if (!json) return null
  const input = json.input
  const output = json.output
  if (typeof input !== 'number' && typeof output !== 'number') return null
  const parts: string[] = []
  if (typeof input === 'number') parts.push(`${input.toLocaleString()} in`)
  if (typeof output === 'number') parts.push(`${output.toLocaleString()} out`)
  return parts.join(' · ')
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

function CopyButton({ text }: { text: string }) {
  const [copied, setCopied] = useState(false)

  return (
    <button
      type="button"
      aria-label={copied ? 'Copied' : 'Copy message'}
      onClick={() => {
        void navigator.clipboard?.writeText(text).then(() => {
          setCopied(true)
          setTimeout(() => setCopied(false), 1_500)
        })
      }}
      className="inline-flex h-6 w-6 items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-muted/60 hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
    >
      {copied ? <Check size={13} aria-hidden /> : <Copy size={13} aria-hidden />}
    </button>
  )
}

function HandoffAction({ handoff }: { handoff: AgentHandoff }) {
  const navigate = useNavigate()

  return (
    <div className="mt-3 flex max-w-xl flex-wrap items-center justify-between gap-3 rounded-lg border border-ember-border bg-ember-surface px-3 py-2">
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

function UserMessage({ message, handoff }: { message: AgentChatMessage; handoff?: AgentHandoff }) {
  return (
    <div className="flex flex-col items-end">
      <article
        aria-label={`You message ${toNumber(message.sequence)}`}
        className="ml-auto min-w-0 max-w-[85%] overflow-hidden rounded-2xl rounded-br-md border border-ember-border/50 bg-ember-surface px-4 py-2.5 sm:max-w-[75%]"
      >
        <p className="whitespace-pre-wrap break-words text-sm leading-6 text-foreground">
          {message.content}
        </p>
        {message.error ? (
          <p className="mt-2 rounded-md border border-destructive/30 bg-destructive/5 px-2.5 py-1.5 text-xs text-destructive">
            {message.error}
          </p>
        ) : null}
      </article>
      {handoff ? <HandoffAction handoff={handoff} /> : null}
    </div>
  )
}

function AgentMessage({
  message,
  agentName,
  chat,
  handoff,
}: {
  message: AgentChatMessage
  agentName?: string
  chat: AgentChat
  handoff?: AgentHandoff
}) {
  const duration = formatDuration(message.duration_ms)
  const tokens = parseTokenUsage(message.token_usage_json)
  const meta = [message.model, duration, tokens].filter(Boolean).join(' · ')

  return (
    <article
      aria-label={`${authorLabel(message, agentName)} message ${toNumber(message.sequence)}`}
      className="min-w-0 max-w-full overflow-hidden"
    >
      {message.author_type === 'handoff' ? (
        <p className="mb-1.5 inline-flex items-center gap-1.5 rounded-full border border-border-subtle bg-muted/40 px-2 py-0.5 text-micro font-medium text-muted-foreground">
          <ArrowUpRight size={11} aria-hidden />
          Handoff from Main Agent
        </p>
      ) : null}
      <div className="prose prose-sm max-w-none break-words dark:prose-invert">
        <ChatMarkdown text={message.content} />
      </div>
      {message.error ? (
        <p className="mt-2 max-w-xl rounded-md border border-destructive/30 bg-destructive/5 px-3 py-2 text-xs text-destructive">
          {message.error}
        </p>
      ) : null}
      <footer className="mt-1.5 flex flex-wrap items-center gap-2">
        <CopyButton text={message.content} />
        {meta ? (
          <span className="font-mono text-micro text-muted-foreground">{meta}</span>
        ) : null}
        <span className="text-micro text-muted-foreground/70">{formatDate(message.created_at)}</span>
        {message.context_manifest_id ? (
          <ContextManifestDialog
            initialManifestId={message.context_manifest_id}
            initialIdentityId={message.author_id ?? undefined}
            initialContextScopeId={chat.id}
            label="Inspect provenance"
            contextHint="this turn"
            compact
          />
        ) : null}
      </footer>
      {handoff ? <HandoffAction handoff={handoff} /> : null}
    </article>
  )
}

function TimelineDivider({ label }: { label: string }) {
  return (
    <div className="flex items-center gap-3 py-1" role="separator" aria-label={label}>
      <span className="h-px flex-1 bg-border-subtle" aria-hidden />
      <span className="text-micro font-medium uppercase tracking-[0.08em] text-muted-foreground/80">
        {label}
      </span>
      <span className="h-px flex-1 bg-border-subtle" aria-hidden />
    </div>
  )
}

function sessionGap(previous: AgentChatMessage | undefined, current: AgentChatMessage): boolean {
  if (!previous) return false
  const before = new Date(previous.created_at).getTime()
  const after = new Date(current.created_at).getTime()
  if (Number.isNaN(before) || Number.isNaN(after)) return false
  return after - before >= SESSION_GAP_MS
}

function SystemMessage({ message }: { message: AgentChatMessage }) {
  return (
    <article
      aria-label={`Forge message ${toNumber(message.sequence)}`}
      className="flex min-w-0 items-center justify-center gap-2 px-4 text-center"
    >
      <p className="max-w-xl break-words text-xs leading-5 text-muted-foreground">
        {message.content}
      </p>
    </article>
  )
}

/**
 * A finite turn rendered as its own agent-side timeline entry — never inside
 * the user message that admitted it. Live turns show a compact expandable
 * activity row; terminal turns show a clear outcome with a visible retry.
 */
function TurnActivity({
  turn,
  inputContent,
  onCancel,
  canceling,
  onRetry,
  canRetry,
}: {
  turn: AgentChatTurn
  inputContent: string
  onCancel?: (turn: AgentChatTurn) => Promise<void>
  canceling?: boolean
  onRetry: (content: string) => Promise<void>
  canRetry: boolean
}) {
  const [open, setOpen] = useState(false)
  const [retrying, setRetrying] = useState(false)
  const [retryError, setRetryError] = useState<string | null>(null)
  const state = normalizeTurnState(turn.status)
  const attemptCount = toNumber(turn.attempt_count)
  const maxAttempts = toNumber(turn.max_attempts)

  async function retry() {
    setRetryError(null)
    setRetrying(true)
    try {
      await onRetry(inputContent)
    } catch (cause) {
      setRetryError(cause instanceof Error ? cause.message : 'The turn could not be retried.')
    } finally {
      setRetrying(false)
    }
  }

  if (state === 'failed' || state === 'cancelled') {
    const failed = state === 'failed'
    return (
      <div
        role={failed ? 'alert' : 'status'}
        className={cn(
          'max-w-xl rounded-lg border px-3.5 py-2.5',
          failed
            ? 'border-destructive/30 bg-destructive/5'
            : 'border-border-subtle bg-muted/30',
        )}
      >
        <div className="flex items-center gap-2">
          {failed ? (
            <WarningCircle size={14} className="shrink-0 text-destructive" aria-hidden />
          ) : (
            <XCircle size={14} className="shrink-0 text-muted-foreground" aria-hidden />
          )}
          <span
            className={cn(
              'text-xs font-medium',
              failed ? 'text-destructive' : 'text-muted-foreground',
            )}
          >
            {failed ? 'Turn failed' : 'Cancelled'}
          </span>
        </div>
        {turn.error ? (
          <p className="mt-1.5 break-words text-xs leading-5 text-muted-foreground">{turn.error}</p>
        ) : null}
        <div className="mt-2 flex flex-wrap items-center gap-2">
          <Button
            type="button"
            size="sm"
            variant="outline"
            onClick={() => void retry()}
            disabled={retrying || !canRetry}
            aria-label="Retry turn"
          >
            {retrying ? <CircleNotch size={13} className="animate-spin" aria-hidden /> : null}
            {retrying ? 'Retrying…' : 'Retry turn'}
          </Button>
          {!canRetry && !retrying ? (
            <span className="text-micro text-muted-foreground">
              Retry becomes available when no other turn is active.
            </span>
          ) : null}
        </div>
        {retryError ? (
          <p className="mt-2 text-xs text-destructive" role="alert">
            {retryError}
          </p>
        ) : null}
      </div>
    )
  }

  if (state === 'succeeded') {
    return (
      <div
        className="inline-flex items-center gap-2 text-xs text-muted-foreground"
        role="status"
      >
        <CheckCircle size={14} className="text-success" aria-hidden />
        Succeeded
      </div>
    )
  }

  const detailRows = [
    `Status: ${turnLabel(state).replace('…', '')} · attempt ${Math.max(attemptCount, 1)}/${maxAttempts}`,
    turn.next_attempt_at ? `Next attempt ${formatDate(turn.next_attempt_at)}` : null,
    turn.error ? `Last attempt: ${turn.error}` : null,
  ].filter((row): row is string => Boolean(row))

  return (
    <div className="min-w-0 max-w-full">
      <div className="flex flex-wrap items-center gap-2">
        <button
          type="button"
          aria-expanded={open}
          aria-label="Toggle turn details"
          onClick={() => setOpen((current) => !current)}
          className="flex items-center gap-2 rounded-lg px-1.5 py-1 transition-colors hover:bg-muted/40 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
        >
          <LoadingState compact label={turnLabel(state)} status={state} startedAt={turn.created_at} />
          <CaretDown
            size={12}
            className={cn(
              'text-muted-foreground/60 transition-transform',
              open ? 'rotate-0' : '-rotate-90',
            )}
            aria-hidden
          />
        </button>
        {onCancel ? (
          <Button
            type="button"
            size="sm"
            variant="ghost"
            onClick={() => void onCancel(turn)}
            disabled={canceling}
            aria-label="Cancel turn"
            className="h-7 px-2 text-xs text-muted-foreground hover:text-destructive"
          >
            {canceling ? <CircleNotch size={13} className="animate-spin" aria-hidden /> : null}
            {canceling ? 'Cancelling…' : 'Cancel turn'}
          </Button>
        ) : null}
      </div>
      {open ? (
        <div className="ml-2.5 mt-1 flex flex-col gap-1 border-l border-border-subtle py-1 pl-3.5">
          {detailRows.map((row) => (
            <span key={row} className="break-words text-xs leading-5 text-muted-foreground">
              {row}
            </span>
          ))}
        </div>
      ) : null}
    </div>
  )
}

/** The `/command` currently being typed, or null when the draft is not one. */
function parseCommandToken(draft: string): string | null {
  const match = /^\/([\w-]*)$/.exec(draft.trimStart())
  return match ? match[1].toLowerCase() : null
}

export function ChatComposer({
  disabled,
  disabledReason,
  isSending,
  onSend,
  commands = [],
  placeholder = 'Message this agent…',
}: {
  disabled?: boolean
  disabledReason?: string
  isSending?: boolean
  onSend: (content: string) => Promise<void>
  commands?: ChatCommand[]
  placeholder?: string
}) {
  const [content, setContent] = useState('')
  const [error, setError] = useState<string | null>(null)
  const [commandRunning, setCommandRunning] = useState(false)
  const [menuDismissed, setMenuDismissed] = useState(false)
  const [activeCommand, setActiveCommand] = useState(0)
  const formRef = useRef<HTMLFormElement>(null)
  const textareaRef = useRef<HTMLTextAreaElement>(null)
  const statusId = useId()
  const describedBy = [disabledReason ? `${statusId}-reason` : null, error ? `${statusId}-error` : null]
    .filter(Boolean)
    .join(' ')
  const busy = isSending || commandRunning

  const commandQuery = menuDismissed ? null : parseCommandToken(content)
  const menuRows =
    commandQuery !== null ? commands.filter((command) => command.name.startsWith(commandQuery)) : []
  const menuOpen = menuRows.length > 0

  useEffect(() => {
    setActiveCommand(0)
  }, [commandQuery])

  function pickCommand(command: ChatCommand) {
    setContent(`/${command.name} `)
    setMenuDismissed(true)
    textareaRef.current?.focus()
  }

  async function submit(event: React.FormEvent) {
    event.preventDefault()
    const value = content.trim()
    if (!value || disabled || busy) return
    setError(null)
    const commandMatch = /^\/([\w-]+)\s*([\s\S]*)$/.exec(value)
    if (commandMatch && commands.length > 0) {
      const command = commands.find(
        (candidate) => candidate.name === commandMatch[1].toLowerCase(),
      )
      if (!command) {
        setError(`Unknown command /${commandMatch[1]}.`)
        return
      }
      setCommandRunning(true)
      try {
        await command.run(commandMatch[2].trim())
        setContent('')
      } catch (cause) {
        setError(
          cause instanceof Error ? cause.message : `The /${command.name} command failed.`,
        )
      } finally {
        setCommandRunning(false)
      }
      return
    }
    try {
      await onSend(value)
      setContent('')
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'The message could not be sent.')
    }
  }

  return (
    <form
      ref={formRef}
      onSubmit={submit}
      className="shrink-0 border-t border-border-subtle bg-background px-4 pb-4 pt-3 sm:px-5"
    >
      <div className="mx-auto w-full max-w-3xl">
        {disabledReason ? (
          <p id={`${statusId}-reason`} className="mb-2 text-xs text-muted-foreground" role="status">
            {disabledReason}
          </p>
        ) : null}
        {error ? (
          <p id={`${statusId}-error`} className="mb-2 text-xs text-destructive" role="alert">
            {error}
          </p>
        ) : null}
        <div className="relative">
          {menuOpen ? (
            <div
              role="listbox"
              aria-label="Commands"
              className="absolute inset-x-0 bottom-full z-10 mb-2 rounded-xl border border-border bg-card p-1 shadow-md"
            >
              {menuRows.map((command, index) => (
                <button
                  key={command.name}
                  type="button"
                  role="option"
                  aria-selected={index === activeCommand}
                  onMouseDown={(event) => event.preventDefault()}
                  onMouseEnter={() => setActiveCommand(index)}
                  onClick={() => pickCommand(command)}
                  className={cn(
                    'flex w-full items-center gap-2.5 rounded-lg px-2.5 py-2 text-left transition-colors',
                    index === activeCommand && 'bg-muted/60',
                  )}
                >
                  <span className="shrink-0 font-mono text-xs font-medium text-foreground">
                    /{command.name}
                  </span>
                  <span className="min-w-0 flex-1 truncate text-xs text-muted-foreground">
                    {command.description}
                  </span>
                </button>
              ))}
              <div className="mt-1 border-t border-border-subtle px-2.5 pb-1 pt-1.5 text-micro text-muted-foreground">
                ↑↓ to navigate · Enter to insert · Esc to dismiss
              </div>
            </div>
          ) : null}
          <div
            role="presentation"
            onClick={() => textareaRef.current?.focus()}
            className="flex cursor-text flex-col gap-1 rounded-2xl border border-border bg-card p-2 shadow-xs transition-colors focus-within:border-primary/40"
          >
            <Textarea
              ref={textareaRef}
              value={content}
              onChange={(event) => {
                setContent(event.target.value)
                setMenuDismissed(false)
              }}
              onKeyDown={(event) => {
                if (menuOpen) {
                  if (event.key === 'ArrowDown' || event.key === 'ArrowUp') {
                    event.preventDefault()
                    setActiveCommand(
                      (current) =>
                        (current + (event.key === 'ArrowDown' ? 1 : menuRows.length - 1)) %
                        menuRows.length,
                    )
                    return
                  }
                  if ((event.key === 'Enter' && !event.shiftKey) || event.key === 'Tab') {
                    event.preventDefault()
                    pickCommand(menuRows[activeCommand])
                    return
                  }
                  if (event.key === 'Escape') {
                    setMenuDismissed(true)
                    return
                  }
                }
                if (event.key !== 'Enter' || event.shiftKey || event.nativeEvent.isComposing) return
                event.preventDefault()
                formRef.current?.requestSubmit()
              }}
              placeholder={placeholder}
              rows={2}
              disabled={disabled || busy}
              aria-label="Chat message"
              aria-describedby={describedBy || undefined}
              className="min-h-[40px] resize-none border-0 bg-transparent px-2 py-1.5 shadow-none focus-visible:ring-0"
            />
            <div className="flex items-center justify-between gap-2 px-1 pb-0.5">
              <span className="text-micro text-muted-foreground">
                Enter to send · Shift+Enter for a new line
                {commands.length > 0 ? ' · / for commands' : ''}
              </span>
              <Button
                type="submit"
                size="icon"
                disabled={disabled || busy || !content.trim()}
                aria-label={busy ? 'Sending message' : 'Send message'}
                className="h-8 w-8 rounded-full"
              >
                {busy ? (
                  <CircleNotch size={15} className="animate-spin" aria-hidden />
                ) : (
                  <PaperPlaneTilt size={15} aria-hidden />
                )}
              </Button>
            </div>
          </div>
        </div>
      </div>
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
  commands,
}: {
  chat: AgentChat
  agentName?: string
  projectId?: string
  handoffProjectIds?: string[]
  isSending?: boolean
  onSend: (content: string) => Promise<void>
  onCancelTurn?: (turnId: string, expectedVersion: number) => Promise<void>
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

  async function retrySend(content: string) {
    setRetrying(true)
    try {
      await onSend(content)
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

  function renderTurn(turn: AgentChatTurn, inputContent: string) {
    return (
      <TurnActivity
        key={turn.id}
        turn={turn}
        inputContent={inputContent}
        onCancel={onCancelTurn && isLiveTurn(turn) ? cancelTurn : undefined}
        canceling={cancelingTurnId === turn.id}
        onRetry={retrySend}
        canRetry={canRetry && !cancelingTurnId}
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
              ) : message.author_type === 'system' ? (
                <SystemMessage message={message} />
              ) : (
                <AgentMessage
                  message={message}
                  agentName={agentName}
                  chat={chat}
                  handoff={handoffFor(message)}
                />
              )}
              {(turnsByMessage.get(message.id) ?? []).map((turn) =>
                renderTurn(turn, message.content),
              )}
            </Fragment>
          ))}
          {orphanTurns.map((turn) => renderTurn(turn, ''))}
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
