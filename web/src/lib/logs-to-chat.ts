import type { ChatEntry } from '@/components/chat/types'
import type { LogEntry } from '@/types/generated'
import { effectiveLogFilterKind } from '@/lib/log-filter'
import {
  type ToolCallEntry,
  type ShellOutputEntry,
  type AssistantEntry,
  type ThinkingEntry,
  firstLogTimestamp,
  payloadText,
  assistantMessageKey,
  entryBase,
  isTurnDividerPayload,
  systemTitle,
  asRecord,
  stringField,
  resultStatus,
  safeString,
  payloadLineText,
} from './logs-to-chat/shared'
import { normalizeUserPrompt, appendUserEntry, shouldKeepEntry } from './logs-to-chat/prompts'
import {
  appendSessionInfoEntry,
  isCommandApprovalRequest,
  isNoisyFileChangeEvent,
  isNoisyAdapterEvent,
} from './logs-to-chat/session-info'
import {
  toolCallInput,
  toolCallStatus,
  toolCallToolName,
  toolInputLabel,
  toolResultLabel,
  takeMatchingToolCall,
  toolResultToolName,
} from './logs-to-chat/tools'
import { fileEditEntry } from './logs-to-chat/files'
import { aggregateToolCalls } from './logs-to-chat/aggregation'

type LogsToChatOptions = {
  showHeartbeats?: boolean
  userPrompt?: string | null
  hiddenKinds?: Array<LogEntry['kind'] | string>
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
