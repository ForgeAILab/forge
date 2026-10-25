// Agent Chat timeline messages and turn activity displays.

import { useEffect, useMemo, useState } from 'react'
import {
  ArrowUpRight,
  BellRinging,
  CaretDown,
  Check,
  CheckCircle,
  CircleNotch,
  Copy,
  Lightning,
  WarningCircle,
  XCircle,
} from '@phosphor-icons/react'
import { useNavigate } from '@tanstack/react-router'
import { Button } from '@/components/ui/button'
import { ContextManifestDialog } from '@/features/federation/ContextManifestInspector'
import { ChatMarkdown } from '@/components/chat/chat-markdown'
import { LoadingState } from '@/components/chat/loading-state'
import {
  activitySummaryLabel,
  currentActivityLabel,
  summarizeTurnActivity,
  TurnActivityFeed,
  turnActivityEntries,
} from '@/components/chat/turn-activity-feed'
import { useAgentChatTurnLogsQuery } from '@/features/agent-chat/hooks'
import type {
  AgentChat,
  AgentChatMessage,
  AgentChatTurn,
  AgentHandoff,
} from '@/features/agent-chat/types'
import { cn } from '@/lib/cn'
import {
  toNumber,
  formatDuration,
  parseTokenUsage,
  authorLabel,
  formatDate,
  parseWakePrompt,
  normalizeTurnState,
  isLiveTurn,
  turnLabel,
} from './shared'

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

export function UserMessage({ message, handoff }: { message: AgentChatMessage; handoff?: AgentHandoff }) {
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

/**
 * What the Agent did to produce a reply: the turn's recorded tool calls and
 * reasoning, collapsed under the message. Recent turns are loaded eagerly so
 * their summary reads at a glance; older ones load when opened.
 */
function TurnActivityDisclosure({
  chatId,
  turnId,
  prefetch,
}: {
  chatId: string
  turnId: string
  prefetch: boolean
}) {
  const [open, setOpen] = useState(false)
  const logsQuery = useAgentChatTurnLogsQuery(chatId, turnId, {
    live: false,
    enabled: open || prefetch,
  })
  const entries = useMemo(
    () => turnActivityEntries(logsQuery.data ?? [], { includeReply: false }),
    [logsQuery.data],
  )
  const summary = summarizeTurnActivity(entries)
  const summaryLabel = activitySummaryLabel(summary)
  const loaded = logsQuery.data !== undefined

  if (loaded && entries.length === 0 && !open) return null

  return (
    <div className="mb-2 min-w-0">
      <button
        type="button"
        aria-expanded={open}
        aria-label="Toggle agent activity"
        onClick={() => setOpen((current) => !current)}
        className="inline-flex max-w-full items-center gap-1.5 rounded-md px-1 py-0.5 text-micro text-muted-foreground transition-colors hover:bg-muted/40 hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        <Lightning size={12} aria-hidden />
        <span className="truncate">{loaded && summaryLabel ? summaryLabel : 'Show activity'}</span>
        <CaretDown
          size={11}
          className={cn('transition-transform', open ? 'rotate-0' : '-rotate-90')}
          aria-hidden
        />
      </button>
      {open ? (
        <div className="ml-1 mt-1 border-l border-border-subtle pl-2">
          {logsQuery.isLoading ? (
            <span className="px-1.5 text-xs text-muted-foreground">Loading activity…</span>
          ) : logsQuery.isError ? (
            <span className="px-1.5 text-xs text-destructive" role="alert">
              The activity for this turn could not be loaded.
            </span>
          ) : entries.length === 0 ? (
            <span className="px-1.5 text-xs text-muted-foreground">
              No activity was recorded for this turn.
            </span>
          ) : (
            <TurnActivityFeed entries={entries} />
          )}
        </div>
      ) : null}
    </div>
  )
}

export function AgentMessage({
  message,
  agentName,
  chat,
  handoff,
  turn,
  prefetchActivity = false,
}: {
  message: AgentChatMessage
  agentName?: string
  chat: AgentChat
  handoff?: AgentHandoff
  turn?: AgentChatTurn
  prefetchActivity?: boolean
}) {
  const duration = formatDuration(message.duration_ms)
  const tokens = parseTokenUsage(message.usage)
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
      {turn ? (
        <TurnActivityDisclosure chatId={chat.id} turnId={turn.id} prefetch={prefetchActivity} />
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

export function TimelineDivider({ label }: { label: string }) {
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

export function SystemMessage({ message }: { message: AgentChatMessage }) {
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
 * A wake that Forge admitted on the Agent's behalf: the incident summary at
 * a glance, with the full work order the Agent received behind a toggle.
 */
export function WakeMessage({ message }: { message: AgentChatMessage }) {
  const [open, setOpen] = useState(false)
  const { summary, body } = parseWakePrompt(message.content)

  return (
    <article
      aria-label={`Forge wake ${toNumber(message.sequence)}`}
      className="flex min-w-0 flex-col items-center gap-1.5 px-4"
    >
      <button
        type="button"
        aria-expanded={open}
        onClick={() => setOpen((current) => !current)}
        className="inline-flex min-w-0 max-w-xl items-center gap-1.5 rounded-md px-2 py-1 text-xs text-muted-foreground transition-colors hover:bg-muted/40 hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        <BellRinging size={13} className="shrink-0" aria-hidden />
        <span className="min-w-0 truncate">
          <span className="font-medium text-foreground/80">Attention wake</span>
          <span aria-hidden> · </span>
          {summary}
        </span>
        <CaretDown
          size={11}
          className={cn('shrink-0 transition-transform', open ? 'rotate-0' : '-rotate-90')}
          aria-hidden
        />
      </button>
      {open ? (
        <div className="w-full max-w-xl rounded-lg border border-border-subtle bg-muted/30 px-3 py-2 text-left">
          <div className="flex items-center justify-between gap-2">
            <span className="text-micro font-medium uppercase tracking-[0.08em] text-muted-foreground/80">
              Prompt the Agent received
            </span>
            <CopyButton text={message.content} />
          </div>
          <p className="mt-1 whitespace-pre-wrap break-words text-xs leading-5 text-muted-foreground">
            {body || message.content}
          </p>
        </div>
      ) : null}
    </article>
  )
}

/**
 * A finite turn rendered as its own agent-side timeline entry — never inside
 * the user message that admitted it. Live turns show a compact expandable
 * activity row; terminal turns show a clear outcome with a visible retry.
 */
export function TurnActivity({
  chatId,
  turn,
  onCancel,
  canceling,
  onRetry,
  canRetry,
  onActivityChange,
}: {
  chatId: string
  turn: AgentChatTurn
  onCancel?: (turn: AgentChatTurn) => Promise<void>
  canceling?: boolean
  onRetry: () => Promise<void>
  canRetry: boolean
  onActivityChange?: () => void
}) {
  const [open, setOpen] = useState(false)
  const [retrying, setRetrying] = useState(false)
  const [retryError, setRetryError] = useState<string | null>(null)
  const state = normalizeTurnState(turn.status)
  const attemptCount = toNumber(turn.attempt_count)
  const maxAttempts = toNumber(turn.max_attempts)
  const live = isLiveTurn(turn)
  // The turn's durable activity log: reasoning, tool calls and their bounded
  // results, and the reply as it streams. Followed while the turn is live so
  // the timeline shows what the Agent is doing, not just that it is busy.
  const logsQuery = useAgentChatTurnLogsQuery(chatId, turn.id, { live })
  const entries = useMemo(
    () => turnActivityEntries(logsQuery.data ?? [], { includeReply: true }),
    [logsQuery.data],
  )
  const entryCount = entries.length
  const lastEntry = entries[entries.length - 1]
  const activityKey = lastEntry
    ? `${entryCount}:${lastEntry.kind}:${lastEntry.kind === 'assistant' || lastEntry.kind === 'thinking' ? lastEntry.text.length : lastEntry.status ?? ''}`
    : '0'
  useEffect(() => {
    onActivityChange?.()
  }, [activityKey, onActivityChange])

  async function retry() {
    setRetryError(null)
    setRetrying(true)
    try {
      await onRetry()
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
        {entries.length > 0 ? (
          <TurnActivityFeed entries={entries} className="mt-2" />
        ) : null}
        {turn.error_message || turn.error ? (
          <p className="mt-1.5 break-words text-xs leading-5 text-muted-foreground">
            {turn.error_code ? `${turn.error_code}: ` : ''}
            {turn.error_message ?? turn.error}
          </p>
        ) : null}
        {turn.retry_action ? (
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
        ) : null}
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

  const deferredForUsage =
    turn.retry_decision === 'defer' && turn.failure_class?.kind === 'usage_limit'
  const deferredLabel = deferredForUsage
    ? `Usage limit reached${turn.next_attempt_at ? ` · Resumes ${formatDate(turn.next_attempt_at)}` : ''}`
    : null
  const detailRows = [
    `Status: ${turnLabel(state).replace('…', '')} · attempt ${Math.max(attemptCount, 1)}/${maxAttempts}`,
    turn.next_attempt_at ? `Next attempt ${formatDate(turn.next_attempt_at)}` : null,
    turn.error_message || turn.error
      ? `Last attempt${turn.error_code ? ` (${turn.error_code})` : ''}: ${turn.error_message ?? turn.error}`
      : null,
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
          <LoadingState
            compact
            label={deferredLabel ?? currentActivityLabel(entries) ?? turnLabel(state)}
            status={state}
            startedAt={turn.created_at}
          />
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
      {entries.length > 0 ? (
        <TurnActivityFeed entries={entries} live className="mt-1.5 max-w-3xl" />
      ) : null}
    </div>
  )
}
