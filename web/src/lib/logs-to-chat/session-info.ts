// Log session events, errors, and command execution conversion.

import type { ChatEntry, ChatEntryStatus } from '@/components/chat/types'
import type { LogEntry } from '@/types/generated'
import {
  type ShellOutputEntry,
  type ToolCallEntry,
  asRecord,
  stringField,
  entryBase,
  textFromContentArray,
  type PayloadRecord,
  valueAtPath,
  safeString,
  shortPath,
} from './shared'
import { isReconstructedPromptEcho, appendUserEntry } from './prompts'
import { fileEditEntry } from './files'
import { appendSessionToolCall, appendSessionToolResult, commandReadAction } from './tools'

export function appendSessionInfoEntry(
  log: LogEntry,
  payload: unknown,
  entries: ChatEntry[],
  shellOutputsById: Map<string, ShellOutputEntry>,
  syntheticUserPrompt: string | undefined,
  unpairedToolCalls: ToolCallEntry[],
): ShellOutputEntry | undefined {
  const record = asRecord(payload)
  const method = stringField(record, 'method')
  const sessionError = sessionInfoErrorMessage(record)

  if (sessionError) {
    entries.push({
      ...entryBase(log),
      kind: 'error',
      title: 'Session Error',
      message: sessionError,
      payload,
    })
    return undefined
  }

  if (method === 'item/commandExecution/outputDelta') {
    const params = asRecord(record?.params)
    const itemId = stringField(params, 'itemId')
    const delta = stringField(params, 'delta')
    if (!delta) return undefined

    let entry = itemId ? shellOutputsById.get(itemId) : undefined
    if (!entry) {
      entry = {
        ...entryBase(log),
        kind: 'shell_output',
        lines: [],
      }
      entries.push(entry)
      if (itemId) shellOutputsById.set(itemId, entry)
    }
    entry.lines.push({ stream: 'stdout', text: delta })
    return entry
  }

  if (method === 'item/started' || method === 'item/completed') {
    const item = asRecord(asRecord(record?.params)?.item)
    const itemType = stringField(item, 'type')

    if (itemType === 'commandExecution') {
      return appendCommandExecution(log, item, entries, shellOutputsById)
    }

    if (itemType === 'userMessage' && method === 'item/completed') {
      const text = textFromContentArray(item?.content)
      if (isReconstructedPromptEcho(text)) {
        return undefined
      }
      appendUserEntry(log, text, entries, syntheticUserPrompt)
      return undefined
    }

    if (itemType === 'fileChange' && method === 'item/completed') {
      const changes = Array.isArray(item?.changes) ? item.changes : []
      for (const change of changes) {
        entries.push(fileEditEntry(log, change))
      }
      return undefined
    }

    if (itemType === 'toolCall' || itemType === 'functionCall') {
      appendSessionToolCall(log, item, entries, unpairedToolCalls)
      return undefined
    }

    if (itemType === 'mcpToolCall') {
      if (method === 'item/completed') {
        appendSessionToolResult(log, item, entries, unpairedToolCalls)
      } else {
        appendSessionToolCall(log, item, entries, unpairedToolCalls)
      }
      return undefined
    }

    if (
      itemType === 'toolResult' ||
      itemType === 'functionCallOutput' ||
      itemType === 'mcpToolResult'
    ) {
      appendSessionToolResult(log, item, entries, unpairedToolCalls)
      return undefined
    }

    return undefined
  }

  if (isNoisySessionInfoMethod(method)) {
    return undefined
  }

  const sessionId =
    stringField(record, 'session_id') ??
    stringField(record, 'thread_id') ??
    stringField(asRecord(asRecord(record?.result)?.thread), 'id') ??
    stringField(asRecord(asRecord(record?.params)?.thread), 'id') ??
    stringField(record, 'threadId')

  if (!sessionId) {
    return undefined
  }

  entries.push({
    ...entryBase(log),
    kind: 'session_info',
    sessionId,
    agent:
      stringField(record, 'agent') ??
      stringField(asRecord(record?.result), 'model') ??
      stringField(asRecord(asRecord(record?.result)?.thread), 'source'),
    startedAt:
      stringField(record, 'started_at') ?? stringField(record, 'startedAt') ?? log.timestamp,
    completedAt: stringField(record, 'completed_at') ?? stringField(record, 'completedAt'),
    payload,
  })
  return undefined
}

function sessionInfoErrorMessage(record: PayloadRecord | undefined): string | undefined {
  const method = stringField(record, 'method')
  if (method === 'error') {
    return firstErrorMessage(
      valueAtPath(record, ['params', 'error', 'message']),
      valueAtPath(record, ['params', 'error']),
      valueAtPath(record, ['error', 'message']),
      valueAtPath(record, ['error']),
      valueAtPath(record, ['message']),
    )
  }

  if (method === 'thread/status/changed') {
    const statusType = stringField(asRecord(valueAtPath(record, ['params', 'status'])), 'type')
    if (statusType !== 'systemError' && statusType !== 'error' && statusType !== 'failed') {
      return undefined
    }
    return (
      firstErrorMessage(
        valueAtPath(record, ['params', 'status', 'message']),
        valueAtPath(record, ['params', 'status', 'error', 'message']),
        valueAtPath(record, ['params', 'status', 'error']),
        valueAtPath(record, ['params', 'status', 'reason']),
      ) ?? `Session ${statusType}`
    )
  }

  if (method === 'turn/completed') {
    return firstErrorMessage(
      valueAtPath(record, ['params', 'turn', 'error', 'message']),
      valueAtPath(record, ['params', 'turn', 'error']),
      valueAtPath(record, ['params', 'turn', 'errorMessage']),
      valueAtPath(record, ['params', 'error', 'message']),
      valueAtPath(record, ['params', 'error']),
      valueAtPath(record, ['params', 'errorMessage']),
    )
  }

  return undefined
}

function firstErrorMessage(...values: unknown[]): string | undefined {
  for (const value of values) {
    const message = errorMessageFromValue(value)
    if (message) return message
  }
  return undefined
}

function errorMessageFromValue(value: unknown): string | undefined {
  if (value === undefined || value === null) return undefined
  if (typeof value === 'string') {
    const trimmed = value.trim()
    if (!trimmed) return undefined
    const nested = errorMessageFromJsonString(trimmed)
    return nested ?? trimmed
  }
  const record = asRecord(value)
  if (record) {
    return firstErrorMessage(record.message, record.error, record.reason, record.detail)
  }
  return safeString(value)
}

function errorMessageFromJsonString(value: string): string | undefined {
  if (!value.startsWith('{') && !value.startsWith('[')) return undefined
  try {
    return errorMessageFromValue(JSON.parse(value))
  } catch {
    return undefined
  }
}

function appendCommandExecution(
  log: LogEntry,
  item: PayloadRecord | undefined,
  entries: ChatEntry[],
  shellOutputsById: Map<string, ShellOutputEntry>,
): ShellOutputEntry | undefined {
  const itemId = stringField(item, 'id')
  let entry = itemId ? shellOutputsById.get(itemId) : undefined

  const readAction = commandReadAction(item)
  const command = readAction ? shortPath(readAction) : stringField(item, 'command')
  const cwd = stringField(item, 'cwd')

  if (!entry) {
    entry = {
      ...entryBase(log),
      kind: 'shell_output',
      command,
      cwd,
      lines: [],
      status: commandStatus(item),
    }
    entries.push(entry)
    if (itemId) shellOutputsById.set(itemId, entry)
  } else {
    entry.command = entry.command ?? command
    entry.cwd = entry.cwd ?? cwd
    entry.status = commandStatus(item) ?? entry.status
  }

  const aggregatedOutput = stringField(item, 'aggregatedOutput')
  if (aggregatedOutput && entry.lines.length === 0) {
    entry.lines.push({ stream: commandOutputStream(item), text: aggregatedOutput })
  }

  return entry
}

function commandStatus(item: PayloadRecord | undefined): ChatEntryStatus | undefined {
  const status = stringField(item, 'status')
  const exitCode = item?.exitCode
  if (status === 'failed') return 'failed'
  if (status === 'completed') {
    return exitCode === 0 ? 'success' : 'failed'
  }
  if (status === 'inProgress') return 'pending'
  return undefined
}

function commandOutputStream(item: PayloadRecord | undefined): 'stdout' | 'stderr' {
  const exitCode = item?.exitCode
  return typeof exitCode === 'number' && exitCode !== 0 ? 'stderr' : 'stdout'
}

export function isNoisyAdapterEvent(payload: unknown): boolean {
  const record = asRecord(payload)
  const method = stringField(record, 'method')
  return (
    method === 'account/rateLimits/updated' ||
    method === 'skills/changed' ||
    (record?.id !== undefined && record.result !== undefined)
  )
}

export function isCommandApprovalRequest(payload: unknown): boolean {
  return stringField(asRecord(payload), 'method') === 'item/commandExecution/requestApproval'
}

export function isNoisyFileChangeEvent(payload: unknown): boolean {
  const method = stringField(asRecord(payload), 'method')
  return method === 'item/fileChange/requestApproval' || method === 'item/fileChange/outputDelta'
}

function isNoisySessionInfoMethod(method: string | undefined): boolean {
  return (
    method === undefined ||
    method === 'thread/status/changed' ||
    method === 'turn/started' ||
    method === 'turn/diff/updated' ||
    method === 'thread/tokenUsage/updated' ||
    method === 'serverRequest/resolved' ||
    method === 'item/fileChange/outputDelta'
  )
}
