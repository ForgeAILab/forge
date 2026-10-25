// Agent Chat timeline formatting, turn state, and wake parsing utilities.

import type { AgentChatMessage, AgentChatTurn } from '@/features/agent-chat/types'
import type { UsageBreakdown } from '@/types/generated/bindings/UsageBreakdown'

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

/** Gap between two messages after which the timeline shows a session divider. */
const SESSION_GAP_MS = 2 * 60 * 60 * 1000

export function formatDate(value: string | null | undefined): string {
  if (!value) return 'No timestamp'
  const date = new Date(value)
  return Number.isNaN(date.getTime())
    ? value
    : date.toLocaleString([], { dateStyle: 'medium', timeStyle: 'short' })
}

export function toNumber(value: bigint | number): number {
  return typeof value === 'bigint' ? Number(value) : value
}

export function formatDuration(ms: bigint | number | null | undefined): string | null {
  if (ms === null || ms === undefined) return null
  const value = toNumber(ms)
  if (value < 1_000) return `${value}ms`
  return `${(value / 1_000).toFixed(value < 10_000 ? 1 : 0)}s`
}

/**
 * "77,535 in · 61,440 cached · 898 out": the disjoint cache buckets stay
 * visible beside fresh input. A chat message can contain multiple provider
 * attempts/events, so the typed rows are summed before display.
 */
export function parseTokenUsage(usage: Pick<UsageBreakdown, 'counters'>[]): string | null {
  const counters = usage.flatMap((row) => (row.counters ? [row.counters] : []))
  if (counters.length === 0) return null
  const input = counters.reduce((total, value) => total + value.input_tokens, 0)
  const output = counters.reduce((total, value) => total + value.output_tokens, 0)
  const cacheRead = counters.reduce((total, value) => total + value.cache_read_tokens, 0)
  const cacheWrite = counters.reduce((total, value) => total + value.cache_write_tokens, 0)
  const parts: string[] = []
  parts.push(`${input.toLocaleString()} in`)
  if (cacheRead > 0) {
    parts.push(`${cacheRead.toLocaleString()} cached`)
  }
  if (cacheWrite > 0) {
    parts.push(`${cacheWrite.toLocaleString()} cache write`)
  }
  parts.push(`${output.toLocaleString()} out`)
  return parts.join(' · ')
}

export function authorLabel(message: AgentChatMessage, agentName?: string): string {
  if (message.author_type === 'agent') return agentName ?? 'Agent'
  if (message.author_type === 'handoff') return 'Main Agent handoff'
  if (message.author_type === 'system') return 'Forge'
  return 'You'
}

export function normalizeTurnState(status: string | undefined): TurnState {
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

export function turnLabel(state: TurnState): string {
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

export function isLiveTurn(turn: AgentChatTurn): boolean {
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

export function sessionGap(previous: AgentChatMessage | undefined, current: AgentChatMessage): boolean {
  if (!previous) return false
  const before = new Date(previous.created_at).getTime()
  const after = new Date(current.created_at).getTime()
  if (Number.isNaN(before) || Number.isNaN(after)) return false
  return after - before >= SESSION_GAP_MS
}

const WAKE_HEADING = /^###\s*Attention wake:\s*(.*)$/m

/**
 * The wake prompt's one-line summary and the work order under it. The prompt
 * opens with a `### Attention wake: <summary>` heading; a prompt without one
 * keeps its whole content as the body.
 */
export function parseWakePrompt(content: string): { summary: string; body: string } {
  const match = WAKE_HEADING.exec(content)
  if (!match) return { summary: 'Attention wake', body: content.trim() }
  const summary = match[1].trim() || 'Attention wake'
  const body = (content.slice(0, match.index) + content.slice(match.index + match[0].length)).trim()
  return { summary, body }
}
