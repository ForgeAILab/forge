import { Fragment, useState } from 'react'
import {
  BookOpen,
  CaretDown,
  CheckCircle,
  CircleNotch,
  FileText,
  Globe,
  Lightning,
  MagnifyingGlass,
  PencilSimple,
  ShieldCheck,
  Terminal,
  WarningCircle,
  Wrench,
} from '@phosphor-icons/react'
import { ChatMarkdown } from '@/components/chat/chat-markdown'
import type {
  ChatAggregatedToolCallsEntry,
  ChatEntry,
  ChatEntryStatus,
  ChatThinkingEntry,
  ChatToolCallEntry,
} from '@/components/chat/types'
import { logsToChatEntries } from '@/lib/logs-to-chat'
import { cn } from '@/lib/cn'
import type { LogEntry } from '@/types/generated'

/**
 * The entries an Agent Chat timeline shows for one turn's activity log.
 *
 * Reply text is only interesting while the turn is live (the durable agent
 * message is the outcome afterwards), and the user's own input is already a
 * timeline bubble, so neither is repeated once the turn has settled.
 */
export function turnActivityEntries(
  logs: LogEntry[],
  { includeReply }: { includeReply: boolean },
): ChatEntry[] {
  return logsToChatEntries(logs).filter((entry) => {
    if (entry.kind === 'assistant') return includeReply
    if (entry.kind === 'user' || entry.kind === 'session_info') return false
    return true
  })
}

type ToolCallDescription = {
  /** What the call is doing while it runs, e.g. "Reading Forge state". */
  pending: string
  /** What the call did once settled, e.g. "Read Forge state". */
  done: string
  icon: typeof Wrench
}

const TOOL_DESCRIPTIONS: Array<[RegExp, ToolCallDescription]> = [
  [/^forge_public_web_search$/, { pending: 'Searching the web', done: 'Searched the web', icon: Globe }],
  [/^forge_task_read$/, { pending: 'Reading a workspace file', done: 'Read a workspace file', icon: FileText }],
  [/^forge_task_write$/, { pending: 'Writing a workspace file', done: 'Wrote a workspace file', icon: PencilSimple }],
  [/^forge_task_command$/, { pending: 'Running a command', done: 'Ran a command', icon: Terminal }],
  [/^forge_task_validate$/, { pending: 'Validating the workspace', done: 'Validated the workspace', icon: ShieldCheck }],
  [/^forge_(setup|configuration)$/, { pending: 'Configuring Forge', done: 'Configured Forge', icon: Wrench }],
  [/_read$/, { pending: 'Reading Forge state', done: 'Read Forge state', icon: MagnifyingGlass }],
  [/_propose$/, { pending: 'Proposing a Forge action', done: 'Proposed a Forge action', icon: Lightning }],
]

const OPERATION_DESCRIPTIONS: Array<[RegExp, ToolCallDescription]> = [
  [/^skill\./, { pending: 'Reading the operating skill', done: 'Read the operating skill', icon: BookOpen }],
  [/^task\.propose$/, { pending: 'Proposing a Task', done: 'Proposed a Task', icon: Lightning }],
  [/^task\.(read|list|get)$/, { pending: 'Reading a Task', done: 'Read a Task', icon: MagnifyingGlass }],
  [/^project\.validation/, { pending: 'Recording validation', done: 'Recorded validation', icon: ShieldCheck }],
  [/^genesis\./, { pending: 'Advancing Product Genesis', done: 'Advanced Product Genesis', icon: Lightning }],
]

/**
 * A reader-facing description of one tool call. Typed Forge tools multiplex
 * many operations behind a couple of tool names, so the operation (known once
 * the call settles) is the more specific signal when present.
 */
export function describeToolCall(toolName: string, operation?: string): ToolCallDescription {
  if (operation) {
    const byOperation = OPERATION_DESCRIPTIONS.find(([pattern]) => pattern.test(operation))
    if (byOperation) return byOperation[1]
  }
  const byTool = TOOL_DESCRIPTIONS.find(([pattern]) => pattern.test(toolName))
  return byTool?.[1] ?? { pending: `Using ${toolName}`, done: `Used ${toolName}`, icon: Wrench }
}

/**
 * The specific operation a settled result carried, e.g. `task.read` for a
 * typed Forge tool call. Falls back to the live `input` preview's own
 * `operation` field so the row can show the specific operation while the
 * call is still pending, before any result has arrived.
 */
export function toolCallOperation(entry: ChatToolCallEntry): string | undefined {
  const result = entry.result
  if (typeof result === 'object' && result !== null) {
    const summary = (result as { summary?: unknown }).summary
    if (typeof summary === 'object' && summary !== null) {
      const operation = (summary as { operation?: unknown }).operation
      if (typeof operation === 'string' && operation) return operation
    }
  }
  return toolCallPreview(entry).operation
}

export function toolCallArgumentKeys(entry: ChatToolCallEntry): string[] {
  const input = entry.input
  if (typeof input !== 'object' || input === null) return []
  const keys = (input as { argument_keys?: unknown }).argument_keys
  return Array.isArray(keys) ? keys.filter((key): key is string => typeof key === 'string') : []
}

/**
 * The flat `input` preview fields a native tool_call log carries (see the
 * `input` object on the wire, `{"command":"...", "params.task_id":"..."}`).
 * Old logs — which have no server-side preview and whose `input` is instead
 * `logsToChatEntries`'s legacy fallback (the whole tool_call record, still
 * carrying `argument_keys`) — resolve to an empty preview so callers fall
 * back to `toolCallArgumentKeys` instead.
 */
export function toolCallPreview(entry: ChatToolCallEntry): Record<string, string> {
  const input = entry.input
  if (typeof input !== 'object' || input === null) return {}
  const record = input as Record<string, unknown>
  if (Array.isArray(record.argument_keys)) return {}
  const preview: Record<string, string> = {}
  for (const [key, value] of Object.entries(record)) {
    if (typeof value === 'string') preview[key] = value
  }
  return preview
}

/** Nested-field keys that rarely tell a reader what a call was about. */
const LOW_SIGNAL_FIELDS = new Set(['limit', 'offset', 'cursor', 'page', 'page_size'])

function leafKey(key: string): string {
  return key.slice(key.lastIndexOf('.') + 1)
}

/**
 * A short "what specifically" line for a tool call row, sourced from the
 * `input` preview: the command for a shell call, the path for a file call,
 * the most telling nested field (`arguments.task_id`, `params.section`) for
 * a typed/multiplexed Forge tool, or otherwise the first preview field that
 * isn't the operation itself, rendered as `key: value`. Empty (undefined)
 * for old logs with no preview.
 */
export function toolCallDetail(entry: ChatToolCallEntry): string | undefined {
  const preview = toolCallPreview(entry)
  if (Object.keys(preview).length === 0) return undefined

  if (entry.toolName === 'forge_task_command') {
    return preview.command
  }
  if (
    entry.toolName === 'forge_task_read' ||
    entry.toolName === 'forge_task_write' ||
    entry.toolName === 'forge_task_validate'
  ) {
    return preview.path ?? preview.file_path
  }

  const nested = Object.entries(preview).filter(([key]) => key.includes('.'))
  const nestedField = nested.find(([key]) => !LOW_SIGNAL_FIELDS.has(leafKey(key))) ?? nested[0]
  if (nestedField) {
    const [key, value] = nestedField
    return `${leafKey(key)}: ${value}`
  }

  const otherField = Object.entries(preview).find(([key]) => key !== 'operation')
  return otherField ? `${otherField[0]}: ${otherField[1]}` : undefined
}

function toolCallSummary(entry: ChatToolCallEntry): Record<string, unknown> | undefined {
  const result = entry.result
  if (typeof result !== 'object' || result === null) return undefined
  const summary = (result as { summary?: unknown }).summary
  return typeof summary === 'object' && summary !== null ? (summary as Record<string, unknown>) : undefined
}

/**
 * Result messages the runtime emits when it has nothing specific to say.
 * They add no information next to the status icon, so a row shows them
 * only through its outcome code (for a failure) or not at all.
 */
const BOILERPLATE_RESULT_MESSAGES = new Set([
  'the tool call completed successfully',
  'the tool call did not complete successfully',
  'command completed',
])

/** The result text worth showing on a settled row, if any. */
export function toolCallResultLabel(entry: ChatToolCallEntry): string | undefined {
  const label = entry.resultLabel
  if (!label) return undefined
  if (!BOILERPLATE_RESULT_MESSAGES.has(label)) return label
  if (entry.status === 'success') return undefined
  const code = toolCallSummary(entry)?.code
  return typeof code === 'string' && code ? code.replace(/_/g, ' ') : undefined
}

function truncate(text: string, max: number): string {
  return text.length > max ? `${text.slice(0, max)}…` : text
}

function isPending(status: ChatEntryStatus | undefined): boolean {
  return status === undefined || status === 'pending' || status === 'pending_approval'
}

function pendingToolCallLabel(entry: ChatToolCallEntry): string {
  const pending = describeToolCall(entry.toolName, toolCallOperation(entry)).pending
  const detail = toolCallDetail(entry)
  return detail ? `${pending} · ${truncate(detail, 48)}` : `${pending}…`
}

/** What the Agent is doing right now, or null when the log shows nothing in flight. */
export function currentActivityLabel(entries: ChatEntry[]): string | null {
  for (let index = entries.length - 1; index >= 0; index -= 1) {
    const entry = entries[index]
    if (entry.kind === 'assistant') return entry.isStreaming ? 'Writing a reply…' : null
    if (entry.kind === 'tool_call') {
      if (!isPending(entry.status)) continue
      return pendingToolCallLabel(entry)
    }
    if (entry.kind === 'aggregated_tool_calls') {
      const pending = [...entry.calls].reverse().find((call) => isPending(call.status))
      if (!pending) continue
      return pendingToolCallLabel(pending)
    }
    if (entry.kind === 'thinking') return entry.isStreaming ? 'Thinking…' : null
  }
  return null
}

export type TurnActivitySummary = {
  toolCalls: number
  failedToolCalls: number
  thought: boolean
}

export function summarizeTurnActivity(entries: ChatEntry[]): TurnActivitySummary {
  const summary: TurnActivitySummary = { toolCalls: 0, failedToolCalls: 0, thought: false }
  const count = (call: ChatToolCallEntry) => {
    summary.toolCalls += 1
    if (call.status === 'failed' || call.status === 'denied' || call.status === 'timed_out') {
      summary.failedToolCalls += 1
    }
  }
  for (const entry of entries) {
    if (entry.kind === 'tool_call') count(entry)
    else if (entry.kind === 'aggregated_tool_calls') entry.calls.forEach(count)
    else if (entry.kind === 'thinking') summary.thought = true
  }
  return summary
}

export function activitySummaryLabel(summary: TurnActivitySummary): string {
  const parts: string[] = []
  if (summary.toolCalls > 0) {
    parts.push(`${summary.toolCalls} tool call${summary.toolCalls === 1 ? '' : 's'}`)
    if (summary.failedToolCalls > 0) parts[0] += ` (${summary.failedToolCalls} failed)`
  }
  if (summary.thought) parts.push('thought it through')
  return parts.join(' · ')
}

function StatusIcon({ status, className }: { status: ChatEntryStatus | undefined; className?: string }) {
  if (isPending(status)) {
    return (
      <CircleNotch
        size={13}
        className={cn('shrink-0 animate-spin text-primary', className)}
        aria-label="In progress"
      />
    )
  }
  if (status === 'success') {
    return <CheckCircle size={13} className={cn('shrink-0 text-success', className)} aria-label="Succeeded" />
  }
  return <WarningCircle size={13} className={cn('shrink-0 text-destructive', className)} aria-label="Failed" />
}

function ToolCallRow({ entry }: { entry: ChatToolCallEntry }) {
  const [open, setOpen] = useState(false)
  const operation = toolCallOperation(entry)
  const description = describeToolCall(entry.toolName, operation)
  const pending = isPending(entry.status)
  const Icon = description.icon
  const preview = toolCallPreview(entry)
  const previewFields = Object.entries(preview)
  const argumentKeys = toolCallArgumentKeys(entry)
  const detail = toolCallDetail(entry)
  const resultLabel = toolCallResultLabel(entry)
  const summary = toolCallSummary(entry)

  return (
    <li className="min-w-0">
      <button
        type="button"
        aria-expanded={open}
        onClick={() => setOpen((current) => !current)}
        className="flex w-full min-w-0 items-center gap-2 rounded-md px-1.5 py-1 text-left text-xs transition-colors hover:bg-muted/40 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        <StatusIcon status={entry.status} />
        <Icon size={13} className="shrink-0 text-muted-foreground" aria-hidden />
        <span className={cn('shrink-0 font-medium', pending ? 'animate-shimmer-text' : 'text-foreground')}>
          {pending ? description.pending : description.done}
        </span>
        <code className="shrink-0 rounded bg-muted/60 px-1 font-mono text-micro text-muted-foreground">
          {operation ?? entry.toolName}
        </code>
        {pending ? (
          detail ? (
            <span className="min-w-0 flex-1 truncate font-mono text-muted-foreground">{detail}</span>
          ) : (
            <span className="min-w-0 flex-1" />
          )
        ) : detail || resultLabel ? (
          <span className="min-w-0 flex-1 truncate text-muted-foreground">
            {detail ? <span className="font-mono">{detail}</span> : null}
            {detail && resultLabel ? ' · ' : null}
            {resultLabel}
          </span>
        ) : (
          <span className="min-w-0 flex-1" />
        )}
        <CaretDown
          size={11}
          className={cn('shrink-0 text-muted-foreground/60 transition-transform', open ? 'rotate-0' : '-rotate-90')}
          aria-hidden
        />
      </button>
      {open ? (
        <dl className="ml-7 mt-0.5 grid grid-cols-[auto_1fr] gap-x-3 gap-y-0.5 pb-1 text-micro text-muted-foreground">
          <dt>Tool</dt>
          <dd className="font-mono">{entry.toolName}</dd>
          {operation ? (
            <>
              <dt>Operation</dt>
              <dd className="font-mono">{operation}</dd>
            </>
          ) : null}
          {previewFields.length > 0
            ? previewFields.map(([key, value]) => (
                <Fragment key={key}>
                  <dt>{key}</dt>
                  <dd className="font-mono break-all">{value}</dd>
                </Fragment>
              ))
            : argumentKeys.length > 0
              ? (
                  <>
                    <dt>Arguments</dt>
                    <dd className="font-mono">{argumentKeys.join(', ')}</dd>
                  </>
                )
              : null}
          {summary && typeof summary.code === 'string' ? (
            <>
              <dt>Outcome</dt>
              <dd className="font-mono">{summary.code}</dd>
            </>
          ) : null}
          {summary && typeof summary.safe_message === 'string' ? (
            <>
              <dt>Result</dt>
              <dd className="break-words">{summary.safe_message}</dd>
            </>
          ) : null}
          {entry.callId ? (
            <>
              <dt>Call</dt>
              <dd className="font-mono">{entry.callId}</dd>
            </>
          ) : null}
        </dl>
      ) : null}
    </li>
  )
}

function AggregatedToolCallsRow({ entry }: { entry: ChatAggregatedToolCallsEntry }) {
  const [open, setOpen] = useState(false)
  const status = entry.worstStatus ?? entry.status
  const description = describeToolCall(entry.toolName)
  const Icon = description.icon

  return (
    <li className="min-w-0">
      <button
        type="button"
        aria-expanded={open}
        onClick={() => setOpen((current) => !current)}
        className="flex w-full min-w-0 items-center gap-2 rounded-md px-1.5 py-1 text-left text-xs transition-colors hover:bg-muted/40 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        <StatusIcon status={status} />
        <Icon size={13} className="shrink-0 text-muted-foreground" aria-hidden />
        <span className={cn('font-medium', isPending(status) ? 'animate-shimmer-text' : 'text-foreground')}>
          {entry.calls.length} × {isPending(status) ? description.pending : description.done}
        </span>
        <code className="shrink-0 rounded bg-muted/60 px-1 font-mono text-micro text-muted-foreground">
          {entry.toolName}
        </code>
        <span className="min-w-0 flex-1" />
        <CaretDown
          size={11}
          className={cn('shrink-0 text-muted-foreground/60 transition-transform', open ? 'rotate-0' : '-rotate-90')}
          aria-hidden
        />
      </button>
      {open ? (
        <ul className="ml-4 border-l border-border-subtle pl-1">
          {entry.calls.map((call) => (
            <ToolCallRow key={`${call.sequence}-${call.callId ?? call.toolName}`} entry={call} />
          ))}
        </ul>
      ) : null}
    </li>
  )
}

function ThinkingRow({ entry }: { entry: ChatThinkingEntry }) {
  const [open, setOpen] = useState(false)
  const text = entry.text.trim()
  const snippet = text.length > 140 ? `…${text.slice(-140).trimStart()}` : text

  return (
    <li className="min-w-0">
      <button
        type="button"
        aria-expanded={open}
        onClick={() => setOpen((current) => !current)}
        className="flex w-full min-w-0 items-start gap-2 rounded-md px-1.5 py-1 text-left text-xs transition-colors hover:bg-muted/40 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        <Lightning size={13} className="mt-0.5 shrink-0 text-muted-foreground" aria-hidden />
        <span className={cn('shrink-0 font-medium', entry.isStreaming ? 'animate-shimmer-text' : 'text-foreground')}>
          {entry.isStreaming ? 'Thinking' : 'Thought'}
        </span>
        {!open && snippet ? (
          <span className="min-w-0 flex-1 truncate italic text-muted-foreground">{snippet}</span>
        ) : (
          <span className="min-w-0 flex-1" />
        )}
        <CaretDown
          size={11}
          className={cn('mt-0.5 shrink-0 text-muted-foreground/60 transition-transform', open ? 'rotate-0' : '-rotate-90')}
          aria-hidden
        />
      </button>
      {open ? (
        <p className="ml-7 max-h-64 overflow-y-auto whitespace-pre-wrap break-words pb-1 pr-2 text-xs italic leading-5 text-muted-foreground">
          {text}
        </p>
      ) : null}
    </li>
  )
}

function genericRowLabel(entry: ChatEntry): string | null {
  switch (entry.kind) {
    case 'system':
      return entry.title
    case 'file_edit':
      return `${entry.action} ${entry.path}`
    case 'shell_output':
      return entry.command ?? 'Command output'
    case 'approval':
      return entry.question
    default:
      return null
  }
}

/**
 * A compact, chronological view of what an Agent did during one turn:
 * reasoning, tool calls with their bounded results, and (while live) the
 * reply as it streams.
 */
export function TurnActivityFeed({
  entries,
  live = false,
  className,
}: {
  entries: ChatEntry[]
  live?: boolean
  className?: string
}) {
  if (entries.length === 0) return null

  return (
    <ul
      className={cn('flex min-w-0 flex-col gap-0.5', className)}
      aria-label={live ? 'Agent activity in progress' : 'Agent activity'}
      aria-live={live ? 'polite' : undefined}
    >
      {entries.map((entry) => {
        const key = `${entry.kind}-${entry.sequence}`
        switch (entry.kind) {
          case 'tool_call':
            return <ToolCallRow key={key} entry={entry} />
          case 'aggregated_tool_calls':
            return <AggregatedToolCallsRow key={key} entry={entry} />
          case 'thinking':
            return <ThinkingRow key={key} entry={entry} />
          case 'assistant':
            return (
              <li key={key} className="prose prose-sm min-w-0 max-w-none break-words px-1.5 py-1 dark:prose-invert">
                <ChatMarkdown text={entry.text} />
                {entry.isStreaming ? (
                  <span className="ml-0.5 inline-block h-3.5 w-1.5 animate-pulse rounded-sm bg-primary/70 align-middle" aria-hidden />
                ) : null}
              </li>
            )
          case 'divider':
            return (
              <li key={key} className="flex items-center gap-2 px-1.5 py-1" role="separator" aria-label={entry.label}>
                <span className="h-px flex-1 bg-border-subtle" aria-hidden />
                <span className="text-micro font-medium uppercase tracking-[0.08em] text-muted-foreground/80">
                  {entry.label}
                </span>
                <span className="h-px flex-1 bg-border-subtle" aria-hidden />
              </li>
            )
          case 'error':
            return (
              <li key={key} className="flex min-w-0 items-start gap-2 px-1.5 py-1 text-xs" role="alert">
                <WarningCircle size={13} className="mt-0.5 shrink-0 text-destructive" aria-hidden />
                <span className="min-w-0 break-words text-destructive">
                  {entry.message ?? entry.title}
                </span>
              </li>
            )
          default: {
            const label = genericRowLabel(entry)
            if (!label) return null
            return (
              <li key={key} className="flex min-w-0 items-center gap-2 px-1.5 py-1 text-xs text-muted-foreground">
                <StatusIcon status={entry.status ?? 'success'} />
                <span className="min-w-0 truncate">{label}</span>
              </li>
            )
          }
        }
      })}
    </ul>
  )
}
