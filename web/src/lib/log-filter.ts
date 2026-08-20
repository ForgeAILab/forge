import type { LogEntry } from '@/types/generated'

export type LogFilterKind =
  | 'assistant'
  | 'thinking'
  | 'user'
  | 'tool_call'
  | 'tool_result'
  | 'stdout'
  | 'stderr'
  | 'system'
  | 'file_change'
  | 'shell_command'
  | 'approval_question'
  | 'session_info'
  | 'unknown'

type PayloadRecord = Record<string, unknown>

function asRecord(value: unknown): PayloadRecord | undefined {
  return typeof value === 'object' && value !== null ? (value as PayloadRecord) : undefined
}

function stringField(record: PayloadRecord | undefined, field: string): string | undefined {
  const value = record?.[field]
  return typeof value === 'string' ? value : undefined
}

function codexSessionInfoKind(payload: unknown): LogFilterKind {
  const record = asRecord(payload)
  const method = stringField(record, 'method')
  const params = asRecord(record?.params)
  const item = asRecord(params?.item)
  const itemType = stringField(item, 'type')

  if (method === 'item/commandExecution/outputDelta') return 'stdout'
  if (itemType === 'commandExecution') return 'shell_command'
  if (itemType === 'userMessage' && method === 'item/completed') return 'user'
  if (itemType === 'fileChange' && method === 'item/completed') return 'file_change'
  if (itemType === 'agentMessage' && method === 'item/completed') return 'assistant'
  if (itemType === 'toolCall' || itemType === 'functionCall' || itemType === 'mcpToolCall') {
    return 'tool_call'
  }
  if (
    itemType === 'toolResult' ||
    itemType === 'functionCallOutput' ||
    itemType === 'mcpToolResult'
  ) {
    return 'tool_result'
  }
  if (method === 'commandExecution/requestApproval') return 'approval_question'

  return 'session_info'
}

export function effectiveLogFilterKind(log: LogEntry): LogFilterKind {
  if (log.kind === 'assistant_delta') return 'assistant'
  if (log.kind === 'session_info') return codexSessionInfoKind(log.payload)
  return log.kind
}
