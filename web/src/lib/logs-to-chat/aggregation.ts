// Log tool call aggregation and status ranking.

import type { ChatEntry, ChatEntryStatus } from '@/components/chat/types'
import type { ToolCallEntry, AggregatedToolCallsEntry } from './shared'

const statusPriority: Record<ChatEntryStatus, number> = {
  success: 0,
  pending: 1,
  pending_approval: 2,
  timed_out: 3,
  denied: 4,
  failed: 5,
}

export function aggregateToolCalls(entries: ChatEntry[]): ChatEntry[] {
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
