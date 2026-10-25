// Log tool call pairing and session tool conversion.

import type { ChatEntry, ChatEntryStatus } from '@/components/chat/types'
import type { LogEntry } from '@/types/generated'
import {
  type ToolCallEntry,
  asRecord,
  stringField,
  type PayloadRecord,
  entryBase,
  resultStatus,
  shortPath,
  displayText,
  valueAtPath,
} from './shared'

export function takeMatchingToolCall(
  unpairedToolCalls: ToolCallEntry[],
  payload: unknown,
): ToolCallEntry | undefined {
  const payloadRecord = asRecord(payload)
  const resultCallId =
    stringField(payloadRecord, 'call_id') ??
    stringField(payloadRecord, 'id') ??
    stringField(payloadRecord, 'itemId') ??
    stringField(payloadRecord, 'item_id')
  let matchIndex = -1

  if (resultCallId) {
    matchIndex = unpairedToolCalls.findIndex((entry) => entry.callId === resultCallId)
  }

  if (matchIndex === -1) {
    matchIndex = unpairedToolCalls.length - 1
  }

  if (matchIndex === -1) {
    return undefined
  }

  const [entry] = unpairedToolCalls.splice(matchIndex, 1)
  return entry
}

export function appendSessionToolCall(
  log: LogEntry,
  item: PayloadRecord | undefined,
  entries: ChatEntry[],
  unpairedToolCalls: ToolCallEntry[],
) {
  const input = item?.input ?? item?.arguments ?? item?.params
  const entry: ToolCallEntry = {
    ...entryBase(log),
    kind: 'tool_call',
    toolName: sessionToolName(item),
    callId: stringField(item, 'id') ?? stringField(item, 'call_id'),
    input,
    inputLabel: toolInputLabel(input),
    status: sessionToolStatus(item) ?? 'pending',
  }
  entries.push(entry)
  if (entry.status === 'pending') {
    unpairedToolCalls.push(entry)
  }
}

export function appendSessionToolResult(
  log: LogEntry,
  item: PayloadRecord | undefined,
  entries: ChatEntry[],
  unpairedToolCalls: ToolCallEntry[],
) {
  const payload = item ?? {}
  const match = takeMatchingToolCall(unpairedToolCalls, payload)
  if (match) {
    match.result = payload
    match.resultLabel = toolResultLabel(payload)
    match.status = sessionToolStatus(item) ?? resultStatus(payload)
    return
  }

  entries.push({
    ...entryBase(log),
    kind: 'tool_call',
    toolName: sessionToolName(item),
    callId: stringField(item, 'id') ?? stringField(item, 'call_id'),
    result: payload,
    resultLabel: toolResultLabel(payload),
    status: sessionToolStatus(item) ?? resultStatus(payload),
  })
}

function sessionToolName(item: PayloadRecord | undefined): string {
  return (
    stringField(item, 'tool') ??
    stringField(item, 'name') ??
    stringField(item, 'toolName') ??
    stringField(item, 'tool_name') ??
    stringField(asRecord(item?.mcp), 'tool') ??
    'unknown'
  )
}

export function toolCallToolName(record: PayloadRecord | undefined): string {
  return (
    stringField(record, 'tool') ??
    stringField(record, 'name') ??
    stringField(record, 'toolName') ??
    stringField(record, 'tool_name') ??
    mcpElicitationToolName(record) ??
    'unknown'
  )
}

export function toolCallInput(record: PayloadRecord | undefined): unknown {
  return (
    asRecord(record?.input) ??
    asRecord(asRecord(record?.params)?._meta)?.tool_params ??
    asRecord(record?.params)?.arguments ??
    record?.params ??
    record
  )
}

export function toolCallStatus(record: PayloadRecord | undefined): ChatEntryStatus {
  if (stringField(record, 'type') !== 'mcp_elicitation_response') {
    return 'pending'
  }
  const decision = stringField(record, 'decision')
  if (decision === 'reject' || decision === 'decline' || decision === 'deny') return 'failed'
  if (decision === 'accept' || decision === 'approve' || decision === 'allow') return 'success'
  return 'pending'
}

function mcpElicitationToolName(record: PayloadRecord | undefined): string | undefined {
  const params = asRecord(record?.params)
  const message = stringField(params, 'message')
  const match = message?.match(/tool "([^"]+)"/)
  return match?.[1]
}

function sessionToolStatus(item: PayloadRecord | undefined): ChatEntryStatus | undefined {
  const status = stringField(item, 'status')
  if (status === 'failed' || status === 'error') return 'failed'
  if (status === 'completed' || status === 'success') return 'success'
  if (status === 'inProgress' || status === 'running') return 'pending'
  return undefined
}

export function commandReadAction(item: PayloadRecord | undefined): string | undefined {
  const actions = item?.commandActions
  if (!Array.isArray(actions)) return undefined
  const first = asRecord(actions[0])
  if (stringField(first, 'type') === 'read') return stringField(first, 'path')
  return undefined
}

export function toolInputLabel(input: unknown): string | undefined {
  const record = asRecord(input)
  const path = stringField(record, 'file_path') ?? stringField(record, 'path')
  return path ? shortPath(path) : undefined
}

export function toolResultToolName(payload: unknown): string | undefined {
  const record = asRecord(payload)
  return (
    stringField(record, 'tool') ?? stringField(record, 'name') ?? stringField(record, 'tool_name')
  )
}

export function toolResultLabel(payload: unknown): string | undefined {
  const record = asRecord(payload)
  const result = asRecord(record?.result)
  const resultContent = Array.isArray(result?.content) ? asRecord(result.content[0]) : undefined
  // A bounded `ToolResultSummary` (see `api_types::ToolResultSummary`), when
  // present, carries the exact safe message the durable log and model both
  // saw for this call — prefer it over generic payload sniffing below.
  const value =
    stringField(asRecord(record?.summary), 'safe_message') ??
    displayText(valueAtPath(record, ['error', 'message'])) ??
    displayText(record?.error) ??
    displayText(record?.output) ??
    displayText(resultContent?.text) ??
    displayText(record?.result) ??
    displayText(record?.content) ??
    displayText(record?.message)
  if (!value) return undefined
  const collapsed = value.replace(/\s+/g, ' ').trim()
  if (!collapsed) return undefined
  return collapsed.length > 80 ? `${collapsed.slice(0, 77)}...` : collapsed
}
