// Log user prompt normalization, deduplication, and entry filtering.

import type { ChatEntry } from '@/components/chat/types'
import type { LogEntry } from '@/types/generated'
import { entryBase } from './shared'

export function appendUserEntry(
  log: LogEntry,
  text: string,
  entries: ChatEntry[],
  syntheticUserPrompt: string | undefined,
) {
  const normalized = normalizeUserPrompt(text)
  if (!normalized) return
  if (syntheticUserPrompt && normalized === syntheticUserPrompt) return
  if (hasUnansweredMatchingUserEntry(entries, normalized)) return
  const previousEntry = entries[entries.length - 1]
  if (
    previousEntry?.kind === 'user' &&
    normalizeUserPrompt(previousEntry.text) === normalized
  ) {
    return
  }
  entries.push({
    ...entryBase(log),
    kind: 'user',
    text,
  })
}

function hasUnansweredMatchingUserEntry(entries: ChatEntry[], normalized: string): boolean {
  for (let index = entries.length - 1; index >= 0; index -= 1) {
    const entry = entries[index]
    if (entry?.kind === 'user') {
      return normalizeUserPrompt(entry.text) === normalized
    }
    if (entry?.kind === 'assistant' && normalizeUserPrompt(entry.text)) {
      return false
    }
  }
  return false
}

export function normalizeUserPrompt(value: string | null | undefined): string | undefined {
  const trimmed = value?.trim()
  return trimmed ? trimmed : undefined
}

export function isReconstructedPromptEcho(value: string | null | undefined): boolean {
  const text = value?.trim()
  return Boolean(
    text &&
      text.includes('Available Forge MCP tools:') &&
      text.includes('Conversation history:'),
  )
}

export function shouldKeepEntry(entry: ChatEntry): boolean {
  if (entry.kind === 'assistant') return normalizeUserPrompt(entry.text) !== undefined
  if (entry.kind !== 'shell_output') return true
  if (entry.lines.length > 0) return true
  return entry.status === 'pending' || entry.status === 'failed'
}
