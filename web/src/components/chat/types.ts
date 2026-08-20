export type ChatEntryStatus =
  | 'pending'
  | 'success'
  | 'failed'
  | 'denied'
  | 'timed_out'
  | 'pending_approval'

export type ChatEntryBase = {
  sequence: number
  timestamp: string
  status?: ChatEntryStatus
}

export type ChatAssistantEntry = ChatEntryBase & {
  kind: 'assistant'
  text: string
  isStreaming?: boolean
}

export type ChatThinkingEntry = ChatEntryBase & {
  kind: 'thinking'
  text: string
  isStreaming?: boolean
}

export type ChatUserEntry = ChatEntryBase & {
  kind: 'user'
  text: string
}

export type ChatSystemEntry = ChatEntryBase & {
  kind: 'system'
  title: string
  payload: unknown
}

export type ChatErrorEntry = ChatEntryBase & {
  kind: 'error'
  title: string
  message?: string
  payload: unknown
}

export type ChatToolCallEntry = ChatEntryBase & {
  kind: 'tool_call'
  toolName: string
  callId?: string
  input?: unknown
  inputLabel?: string
  result?: unknown
  resultLabel?: string
}

export type ChatAggregatedToolCallsEntry = ChatEntryBase & {
  kind: 'aggregated_tool_calls'
  toolName: string
  calls: ChatToolCallEntry[]
  worstStatus: ChatEntryStatus
}

export type ChatShellOutputEntry = ChatEntryBase & {
  kind: 'shell_output'
  command?: string
  cwd?: string
  lines: Array<{
    stream: 'stdout' | 'stderr'
    text: string
  }>
}

export type ChatFileEditEntry = ChatEntryBase & {
  kind: 'file_edit'
  action: 'edit' | 'write' | 'delete' | 'rename'
  path: string
  newPath?: string
  diff?: string
  before?: string
  after?: string
  additions?: number
  deletions?: number
  payload: unknown
}

export type ChatApprovalEntry = ChatEntryBase & {
  kind: 'approval'
  question: string
  decision?: string
  rationale?: string
  payload: unknown
}

export type ChatSessionInfoEntry = ChatEntryBase & {
  kind: 'session_info'
  sessionId?: string
  agent?: string
  startedAt?: string
  completedAt?: string
  payload: unknown
}

export type ChatDividerEntry = ChatEntryBase & {
  kind: 'divider'
  label: string
}

export type ChatEntry =
  | ChatAssistantEntry
  | ChatThinkingEntry
  | ChatUserEntry
  | ChatSystemEntry
  | ChatErrorEntry
  | ChatToolCallEntry
  | ChatAggregatedToolCallsEntry
  | ChatShellOutputEntry
  | ChatFileEditEntry
  | ChatApprovalEntry
  | ChatSessionInfoEntry
  | ChatDividerEntry
