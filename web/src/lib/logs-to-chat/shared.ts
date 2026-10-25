// Log payload types, text extraction, and display utilities.

import type { ChatEntry, ChatEntryStatus } from '@/components/chat/types'
import type { LogEntry } from '@/types/generated'

export type PayloadRecord = Record<string, unknown>

export type AssistantEntry = Extract<ChatEntry, { kind: 'assistant' }> & {
  isStreaming?: boolean
}

export type ThinkingEntry = Extract<ChatEntry, { kind: 'thinking' }> & {
  isStreaming?: boolean
}

export type ToolCallEntry = Extract<ChatEntry, { kind: 'tool_call' }>

export type ShellOutputEntry = Extract<ChatEntry, { kind: 'shell_output' }>

export type FileEditEntry = Extract<ChatEntry, { kind: 'file_edit' }>

export type AggregatedToolCallsEntry = Extract<ChatEntry, { kind: 'aggregated_tool_calls' }> & {
  worstStatus: ChatEntryStatus
}

export function entryBase(log: LogEntry): Pick<ChatEntry, 'sequence' | 'timestamp'> {
  return {
    sequence: log?.sequence ?? 0,
    timestamp: log?.timestamp ?? '',
  }
}

export function firstLogTimestamp(logs: LogEntry[]): string {
  return logs.find((log) => log?.timestamp)?.timestamp ?? ''
}

export function asRecord(value: unknown): PayloadRecord | undefined {
  return typeof value === 'object' && value !== null ? (value as PayloadRecord) : undefined
}

export function stringField(record: PayloadRecord | undefined, field: string): string | undefined {
  const value = record?.[field]
  return value === undefined || value === null ? undefined : safeString(value)
}

export function payloadText(payload: unknown): string {
  const record = asRecord(payload)
  for (const field of ['text', 'delta', 'message', 'content']) {
    const value = record?.[field]
    if (value === undefined || value === null) continue
    // Content arrays: [{type:"text", text:"..."}] (Claude API format)
    if (Array.isArray(value)) return textFromContentArray(value)
    // Nested message object with content blocks: {message: {content: [...]}}
    const nested = asRecord(value)
    if (nested !== undefined) {
      if (Array.isArray(nested.content)) {
        const text = textFromContentArray(nested.content)
        if (text) return text
      }
      continue
    }
    return safeString(value)
  }
  return typeof payload === 'string' ? payload : safeStringify(payload)
}

export function assistantMessageKey(payload: unknown): string | undefined {
  const record = asRecord(payload)
  return stringField(record, 'itemId') ?? stringField(record, 'messageId')
}

export function payloadLineText(payload: unknown): string {
  const record = asRecord(payload)
  for (const field of ['line', 'delta', 'text', 'message', 'aggregatedOutput']) {
    const text = record?.[field]
    if (text !== undefined && text !== null) {
      return safeString(text)
    }
  }
  return typeof payload === 'string' ? payload : safeStringify(payload)
}

export function systemTitle(payload: unknown): string {
  const record = asRecord(payload)
  return safeString(record?.label ?? record?.message ?? record?.text ?? 'System')
}

export function isTurnDividerPayload(payload: unknown): boolean {
  return asRecord(payload)?.type === 'turn_divider'
}

export function resultStatus(payload: unknown): ChatEntryStatus {
  return toolResultSummaryStatus(payload) ?? (asRecord(payload)?.success === false ? 'failed' : 'success')
}

// A tool-result payload may carry a bounded `ToolResultSummary` (see
// `api_types::ToolResultSummary`) written by the durable log or the smith
// adapter. When present, its `status` is authoritative over the generic
// `success` heuristic below, since it reflects the exact typed orchestration
// outcome rather than a boolean process exit code.
function toolResultSummaryStatus(payload: unknown): ChatEntryStatus | undefined {
  const status = stringField(asRecord(asRecord(payload)?.summary), 'status')
  switch (status) {
    case 'succeeded':
      return 'success'
    case 'failed':
      return 'failed'
    case 'approval_required':
    case 'setup_required':
      return 'pending_approval'
    default:
      return undefined
  }
}

export function valueAtPath(value: unknown, path: string[]): unknown {
  let current = value
  for (const key of path) {
    const record = asRecord(current)
    if (!record || !(key in record)) return undefined
    current = record[key]
  }
  return current
}

export function textFromContentArray(value: unknown): string {
  if (!Array.isArray(value)) return ''
  return value
    .map((item) => {
      const record = asRecord(item)
      return stringField(record, 'text') ?? ''
    })
    .join('')
}

export function displayText(value: unknown): string | undefined {
  if (value === undefined || value === null) return undefined
  if (Array.isArray(value)) {
    const text = textFromContentArray(value).trim()
    return text || undefined
  }
  if (typeof value === 'string') return value
  if (typeof value === 'number' || typeof value === 'boolean') return String(value)

  const record = asRecord(value)
  if (!record) return undefined
  if (Array.isArray(record.content)) {
    const text = textFromContentArray(record.content).trim()
    if (text) return text
  }
  return displayText(record.text) ?? displayText(record.message)
}

export function shortPath(path: string): string {
  if (!path) return path
  // /repo/ marker (generic docker/CI pattern)
  const repoIndex = path.indexOf('/repo/')
  if (repoIndex !== -1) return path.slice(repoIndex + '/repo/'.length)
  // Forge workspace: .../forge-workspace/{uuid}/{name}/relative → relative
  const wsIndex = path.indexOf('/forge-workspace/')
  if (wsIndex !== -1) {
    const afterWs = path.slice(wsIndex + '/forge-workspace/'.length)
    const uuidSlash = afterWs.indexOf('/')
    if (uuidSlash !== -1) {
      const afterUuid = afterWs.slice(uuidSlash + 1)
      const nameSlash = afterUuid.indexOf('/')
      if (nameSlash !== -1) return afterUuid.slice(nameSlash + 1)
      return afterUuid // at workspace root, return workspace name
    }
  }
  const srcIndex = path.lastIndexOf('/src/')
  if (srcIndex !== -1) return path.slice(srcIndex + 1)
  return path
}

function safeStringify(value: unknown): string {
  try {
    const json = JSON.stringify(value)
    return json === undefined ? safeString(value) : json
  } catch {
    return safeString(value)
  }
}

export function safeString(value: unknown): string {
  try {
    return String(value)
  } catch {
    return '[unserializable]'
  }
}
