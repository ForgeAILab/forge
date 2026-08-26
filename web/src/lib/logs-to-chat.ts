import type { ChatEntry, ChatEntryStatus } from '@/components/chat/types'
import type { LogEntry } from '@/types/generated'
import { effectiveLogFilterKind } from '@/lib/log-filter'

type PayloadRecord = Record<string, unknown>
type AssistantEntry = Extract<ChatEntry, { kind: 'assistant' }> & {
  isStreaming?: boolean
}
type ThinkingEntry = Extract<ChatEntry, { kind: 'thinking' }> & {
  isStreaming?: boolean
}
type ToolCallEntry = Extract<ChatEntry, { kind: 'tool_call' }>
type ShellOutputEntry = Extract<ChatEntry, { kind: 'shell_output' }>
type FileEditEntry = Extract<ChatEntry, { kind: 'file_edit' }>
type AggregatedToolCallsEntry = Extract<ChatEntry, { kind: 'aggregated_tool_calls' }> & {
  worstStatus: ChatEntryStatus
}

type LogsToChatOptions = {
  showHeartbeats?: boolean
  userPrompt?: string | null
  hiddenKinds?: Array<LogEntry['kind'] | string>
}

const statusPriority: Record<ChatEntryStatus, number> = {
  success: 0,
  pending: 1,
  pending_approval: 2,
  timed_out: 3,
  denied: 4,
  failed: 5,
}

export function logsToChatEntries(logs: LogEntry[], opts?: LogsToChatOptions): ChatEntry[] {
  const entries: ChatEntry[] = []
  const unpairedToolCalls: ToolCallEntry[] = []
  const shellOutputsById = new Map<string, ShellOutputEntry>()
  const syntheticUserPrompt = normalizeUserPrompt(opts?.userPrompt)
  const hiddenKinds = new Set(opts?.hiddenKinds ?? [])
  let activeAssistant: AssistantEntry | undefined
  let activeAssistantMessageKey: string | undefined
  let activeShellOutput: ShellOutputEntry | undefined
  let activeThinking: ThinkingEntry | undefined

  if (syntheticUserPrompt) {
    entries.push({
      sequence: 0,
      timestamp: firstLogTimestamp(logs),
      kind: 'user',
      text: syntheticUserPrompt,
    })
  }

  const finalizeAssistant = () => {
    if (activeAssistant) {
      delete activeAssistant.isStreaming
      activeAssistant = undefined
      activeAssistantMessageKey = undefined
    }
  }

  for (const log of logs ?? []) {
    if (!opts?.showHeartbeats && log?.stream === 'heartbeat') {
      continue
    }

    const kind = String(log?.kind ?? 'unknown')
    const payload = log?.payload
    const effectiveKind = effectiveLogFilterKind(log)
    if (hiddenKinds.has(effectiveKind)) {
      continue
    }

    if (kind !== 'assistant_delta' && kind !== 'assistant') {
      finalizeAssistant()
    }

    if (kind !== 'thinking' && activeThinking) {
      delete activeThinking.isStreaming
      activeThinking = undefined
    }

    if (kind !== 'stdout' && kind !== 'stderr' && kind !== 'shell_command') {
      activeShellOutput = undefined
    }

    switch (kind) {
      case 'assistant': {
        const text = payloadText(payload)
        const messageKey = assistantMessageKey(payload)
        if (
          activeAssistant?.isStreaming &&
          (!activeAssistantMessageKey || !messageKey || activeAssistantMessageKey === messageKey)
        ) {
          activeAssistant.text = text
          delete activeAssistant.isStreaming
          activeAssistant = undefined
          activeAssistantMessageKey = undefined
          break
        }

        finalizeAssistant()
        const entry: AssistantEntry = {
          ...entryBase(log),
          kind: 'assistant',
          text,
        }
        entries.push(entry)
        activeAssistant = entry
        activeAssistantMessageKey = messageKey
        break
      }
      case 'assistant_delta': {
        if (!activeAssistant) {
          activeAssistant = {
            ...entryBase(log),
            kind: 'assistant',
            text: '',
          }
          entries.push(activeAssistant)
          activeAssistantMessageKey = assistantMessageKey(payload)
        }
        activeAssistant.text += payloadText(payload)
        activeAssistant.isStreaming = true
        break
      }
      case 'thinking': {
        if (!activeThinking) {
          activeThinking = {
            ...entryBase(log),
            kind: 'thinking',
            text: '',
          }
          entries.push(activeThinking)
        }
        activeThinking.text += payloadText(payload)
        activeThinking.isStreaming = true
        break
      }
      case 'user':
        appendUserEntry(log, payloadText(payload), entries, syntheticUserPrompt)
        break
      case 'system':
        if (isTurnDividerPayload(payload)) {
          entries.push({
            ...entryBase(log),
            kind: 'divider',
            label: systemTitle(payload),
          })
        } else {
          entries.push({
            ...entryBase(log),
            kind: 'system',
            title: systemTitle(payload),
            payload,
          })
        }
        break
      case 'session_info':
        activeShellOutput =
          appendSessionInfoEntry(
            log,
            payload,
            entries,
            shellOutputsById,
            syntheticUserPrompt,
            unpairedToolCalls,
          ) ?? activeShellOutput
        break
      case 'tool_call': {
        if (isCommandApprovalRequest(payload)) {
          const params = asRecord(asRecord(payload)?.params)
          entries.push({
            ...entryBase(log),
            kind: 'approval',
            question:
              stringField(params, 'reason') ??
              `Approve ${stringField(params, 'command') ?? 'command execution'}?`,
            decision: 'approved',
            rationale: stringField(params, 'command'),
            payload,
            status: 'success',
          })
          break
        }
        const record = asRecord(payload)
        const input = toolCallInput(record)
        const status = toolCallStatus(record)
        const entry: ToolCallEntry = {
          ...entryBase(log),
          kind: 'tool_call',
          toolName: toolCallToolName(record),
          callId: stringField(record, 'call_id'),
          input,
          inputLabel: toolInputLabel(input),
          result: status === 'pending' ? undefined : payload,
          resultLabel: status === 'pending' ? undefined : toolResultLabel(payload),
          status,
        }
        entries.push(entry)
        if (status === 'pending') {
          unpairedToolCalls.push(entry)
        }
        break
      }
      case 'tool_result': {
        const match = takeMatchingToolCall(unpairedToolCalls, payload)
        if (match) {
          match.result = payload
          match.resultLabel = toolResultLabel(payload)
          match.status = resultStatus(payload)
        } else {
          const toolName = toolResultToolName(payload)
          if (toolName) {
            entries.push({
              ...entryBase(log),
              kind: 'tool_call',
              toolName,
              callId: stringField(asRecord(payload), 'call_id'),
              result: payload,
              resultLabel: toolResultLabel(payload),
              status: resultStatus(payload),
            })
          } else {
            entries.push({
              ...entryBase(log),
              kind: 'system',
              title: 'Tool Result',
              payload,
            })
          }
        }
        break
      }
      case 'file_change':
        if (!isNoisyFileChangeEvent(payload)) {
          entries.push(fileEditEntry(log, payload))
        }
        break
      case 'shell_command': {
        const record = asRecord(payload)
        activeShellOutput = {
          ...entryBase(log),
          kind: 'shell_output',
          command: stringField(record, 'command') ?? safeString(payload),
          cwd: stringField(record, 'cwd'),
          lines: [],
        }
        entries.push(activeShellOutput)
        break
      }
      case 'stdout':
      case 'stderr': {
        if (isNoisyAdapterEvent(payload)) {
          break
        }
        const line = {
          stream: kind,
          text: payloadLineText(payload),
        }
        if (!activeShellOutput) {
          activeShellOutput = {
            ...entryBase(log),
            kind: 'shell_output',
            lines: [line],
          }
          entries.push(activeShellOutput)
        } else {
          activeShellOutput.lines.push(line)
        }
        break
      }
      case 'approval_question': {
        const record = asRecord(payload)
        entries.push({
          ...entryBase(log),
          kind: 'approval',
          question: stringField(record, 'question') ?? safeString(payload),
          decision: stringField(record, 'decision'),
          rationale: stringField(record, 'rationale'),
          payload,
        })
        break
      }
      case 'unknown':
      default:
        entries.push({
          ...entryBase(log),
          kind: 'system',
          title: 'Unknown',
          payload,
        })
        break
    }
  }

  return aggregateToolCalls(entries.filter(shouldKeepEntry))
}

function entryBase(log: LogEntry): Pick<ChatEntry, 'sequence' | 'timestamp'> {
  return {
    sequence: log?.sequence ?? 0,
    timestamp: log?.timestamp ?? '',
  }
}

function firstLogTimestamp(logs: LogEntry[]): string {
  return logs.find((log) => log?.timestamp)?.timestamp ?? ''
}

function appendUserEntry(
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

function normalizeUserPrompt(value: string | null | undefined): string | undefined {
  const trimmed = value?.trim()
  return trimmed ? trimmed : undefined
}

function isReconstructedPromptEcho(value: string | null | undefined): boolean {
  const text = value?.trim()
  return Boolean(
    text &&
      text.includes('Available Forge MCP tools:') &&
      text.includes('Conversation history:'),
  )
}

function shouldKeepEntry(entry: ChatEntry): boolean {
  if (entry.kind === 'assistant') return normalizeUserPrompt(entry.text) !== undefined
  if (entry.kind !== 'shell_output') return true
  if (entry.lines.length > 0) return true
  return entry.status === 'pending' || entry.status === 'failed'
}

function asRecord(value: unknown): PayloadRecord | undefined {
  return typeof value === 'object' && value !== null ? (value as PayloadRecord) : undefined
}

function stringField(record: PayloadRecord | undefined, field: string): string | undefined {
  const value = record?.[field]
  return value === undefined || value === null ? undefined : safeString(value)
}

function payloadText(payload: unknown): string {
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

function assistantMessageKey(payload: unknown): string | undefined {
  const record = asRecord(payload)
  return stringField(record, 'itemId') ?? stringField(record, 'messageId')
}

function payloadLineText(payload: unknown): string {
  const record = asRecord(payload)
  for (const field of ['line', 'delta', 'text', 'message', 'aggregatedOutput']) {
    const text = record?.[field]
    if (text !== undefined && text !== null) {
      return safeString(text)
    }
  }
  return typeof payload === 'string' ? payload : safeStringify(payload)
}

function systemTitle(payload: unknown): string {
  const record = asRecord(payload)
  return safeString(record?.label ?? record?.message ?? record?.text ?? 'System')
}

function isTurnDividerPayload(payload: unknown): boolean {
  return asRecord(payload)?.type === 'turn_divider'
}

function takeMatchingToolCall(
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

function resultStatus(payload: unknown): ChatEntryStatus {
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

function appendSessionInfoEntry(
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

function valueAtPath(value: unknown, path: string[]): unknown {
  let current = value
  for (const key of path) {
    const record = asRecord(current)
    if (!record || !(key in record)) return undefined
    current = record[key]
  }
  return current
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

function appendSessionToolCall(
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

function appendSessionToolResult(
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

function toolCallToolName(record: PayloadRecord | undefined): string {
  return (
    stringField(record, 'tool') ??
    stringField(record, 'name') ??
    stringField(record, 'toolName') ??
    stringField(record, 'tool_name') ??
    mcpElicitationToolName(record) ??
    'unknown'
  )
}

function toolCallInput(record: PayloadRecord | undefined): unknown {
  return (
    asRecord(asRecord(record?.params)?._meta)?.tool_params ??
    asRecord(record?.params)?.arguments ??
    record?.params ??
    record
  )
}

function toolCallStatus(record: PayloadRecord | undefined): ChatEntryStatus {
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

function textFromContentArray(value: unknown): string {
  if (!Array.isArray(value)) return ''
  return value
    .map((item) => {
      const record = asRecord(item)
      return stringField(record, 'text') ?? ''
    })
    .join('')
}

function isNoisyAdapterEvent(payload: unknown): boolean {
  const record = asRecord(payload)
  const method = stringField(record, 'method')
  return (
    method === 'account/rateLimits/updated' ||
    method === 'skills/changed' ||
    (record?.id !== undefined && record.result !== undefined)
  )
}

function isCommandApprovalRequest(payload: unknown): boolean {
  return stringField(asRecord(payload), 'method') === 'item/commandExecution/requestApproval'
}

function isNoisyFileChangeEvent(payload: unknown): boolean {
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

function fileEditEntry(log: LogEntry, payload: unknown): FileEditEntry {
  const record = asRecord(payload)
  const diff = stringField(record, 'diff')
  const path = stringField(record, 'path')
  const counts = diff ? diffCounts(diff) : undefined

  return {
    ...entryBase(log),
    kind: 'file_edit',
    path: path ? shortPath(path) : 'unknown',
    action: fileAction(record),
    newPath: stringField(record, 'new_path') ?? stringField(record, 'newPath'),
    diff,
    before: stringField(record, 'before'),
    after: stringField(record, 'after'),
    additions: counts?.additions,
    deletions: counts?.deletions,
    payload,
  }
}

function fileAction(record: PayloadRecord | undefined): FileEditEntry['action'] {
  const action = (
    stringField(record, 'op') ??
    stringField(record, 'action') ??
    stringField(asRecord(record?.kind), 'type') ??
    'edit'
  ).toLowerCase()

  if (
    action === 'edit' ||
    action === 'update' ||
    action === 'write' ||
    action === 'add' ||
    action === 'delete' ||
    action === 'rename'
  ) {
    if (action === 'update') return 'edit'
    if (action === 'add') return 'write'
    return action
  }

  return 'edit'
}

function commandReadAction(item: PayloadRecord | undefined): string | undefined {
  const actions = item?.commandActions
  if (!Array.isArray(actions)) return undefined
  const first = asRecord(actions[0])
  if (stringField(first, 'type') === 'read') return stringField(first, 'path')
  return undefined
}

function toolInputLabel(input: unknown): string | undefined {
  const record = asRecord(input)
  const path = stringField(record, 'file_path') ?? stringField(record, 'path')
  return path ? shortPath(path) : undefined
}

function toolResultToolName(payload: unknown): string | undefined {
  const record = asRecord(payload)
  return (
    stringField(record, 'tool') ?? stringField(record, 'name') ?? stringField(record, 'tool_name')
  )
}

function toolResultLabel(payload: unknown): string | undefined {
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

function displayText(value: unknown): string | undefined {
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

function shortPath(path: string): string {
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

function diffCounts(diff: string): { additions: number; deletions: number } {
  return diff.split('\n').reduce(
    (counts, line) => {
      if (line.startsWith('+') && !line.startsWith('+++')) {
        counts.additions += 1
      }
      if (line.startsWith('-') && !line.startsWith('---')) {
        counts.deletions += 1
      }
      return counts
    },
    { additions: 0, deletions: 0 },
  )
}

function aggregateToolCalls(entries: ChatEntry[]): ChatEntry[] {
  const aggregated: ChatEntry[] = []
  let index = 0

  while (index < entries.length) {
    const entry = entries[index]

    if (entry.kind !== 'tool_call') {
      aggregated.push(entry)
      index += 1
      continue
    }

    const run: ToolCallEntry[] = [entry]
    index += 1

    while (index < entries.length) {
      const candidate = entries[index]
      if (candidate.kind !== 'tool_call' || candidate.toolName !== entry.toolName) {
        break
      }
      run.push(candidate)
      index += 1
    }

    if (run.length === 1) {
      aggregated.push(run[0])
      continue
    }

    const worstStatus = worstToolStatus(run)
    const aggregate: AggregatedToolCallsEntry = {
      ...entryBaseFromEntry(run[0]),
      kind: 'aggregated_tool_calls',
      toolName: run[0].toolName,
      calls: run,
      status: worstStatus,
      worstStatus,
    }
    aggregated.push(aggregate)
  }

  return aggregated
}

function entryBaseFromEntry(entry: ChatEntry): Pick<ChatEntry, 'sequence' | 'timestamp'> {
  return {
    sequence: entry.sequence,
    timestamp: entry.timestamp,
  }
}

function worstToolStatus(calls: ToolCallEntry[]): ChatEntryStatus {
  return calls.reduce<ChatEntryStatus>((worst, call) => {
    const status = call.status ?? 'pending'
    return statusPriority[status] > statusPriority[worst] ? status : worst
  }, 'success')
}

function safeStringify(value: unknown): string {
  try {
    const json = JSON.stringify(value)
    return json === undefined ? safeString(value) : json
  } catch {
    return safeString(value)
  }
}

function safeString(value: unknown): string {
  try {
    return String(value)
  } catch {
    return '[unserializable]'
  }
}
