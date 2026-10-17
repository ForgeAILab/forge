import type { AgentBindingState } from '@/types/generated/bindings/AgentBindingState'
import type { AgentChatKind } from '@/types/generated/bindings/AgentChatKind'
import type { AgentChatMessageAuthorType } from '@/types/generated/bindings/AgentChatMessageAuthorType'
import type { AgentChatMessageResponse } from '@/types/generated/bindings/AgentChatMessageResponse'
import type { AgentChatMessageStatus } from '@/types/generated/bindings/AgentChatMessageStatus'
import type { AgentChatResponse } from '@/types/generated/bindings/AgentChatResponse'
import type { AgentChatStatus } from '@/types/generated/bindings/AgentChatStatus'
import type { AgentChatSwitcherItem } from '@/types/generated/bindings/AgentChatSwitcherItem'
import type { AgentChatSwitcherResponse } from '@/types/generated/bindings/AgentChatSwitcherResponse'
import type { AgentChatTurnJobResponse } from '@/types/generated/bindings/AgentChatTurnJobResponse'
import type { AgentChatTurnStatus } from '@/types/generated/bindings/AgentChatTurnStatus'
import type { AgentHandoffResponse } from '@/types/generated/bindings/AgentHandoffResponse'
import type { CancelAgentChatTurnRequest } from '@/types/generated/bindings/CancelAgentChatTurnRequest'
import type { CreateAgentHandoffRequest } from '@/types/generated/bindings/CreateAgentHandoffRequest'
import type { RetryAgentChatTurnRequest } from '@/types/generated/bindings/RetryAgentChatTurnRequest'

export type AgentChat = AgentChatResponse
export type AgentChatEntry = AgentChatSwitcherItem
export type AgentChatSwitcher = AgentChatSwitcherResponse
export type AgentChatMessage = AgentChatMessageResponse
export type AgentChatTurn = AgentChatTurnJobResponse

export type AgentChatTurnRetryInput = Omit<RetryAgentChatTurnRequest, 'expected_version'> & {
  expected_version: number
}
export type AgentChatTurnCancelInput = Omit<CancelAgentChatTurnRequest, 'expected_version'> & {
  expected_version: number
}
export type AgentHandoff = AgentHandoffResponse
export type AgentHandoffInput = CreateAgentHandoffRequest
export type AgentChatKindValue = AgentChatKind
export type AgentChatStatusValue = AgentChatStatus
export type AgentChatMessageAuthorValue = AgentChatMessageAuthorType
export type AgentChatMessageStatusValue = AgentChatMessageStatus
export type AgentChatTurnStatusValue = AgentChatTurnStatus
export type AgentBindingStateValue = AgentBindingState

export interface AgentChatMessageInput {
  content: string
  dedupe_key?: string | null
}

export interface AgentChatMessageAdmission {
  message: AgentChatMessage
  turn_job: AgentChatTurn | null
}

export interface AgentChatMessagesResponse {
  items: AgentChatMessage[]
  next_cursor: string | null
  has_more: boolean
}

export type AgentChatTurnsResponse = AgentChatTurn[]

export interface AgentChatTurnListResponse {
  items: AgentChatTurn[]
  next_cursor?: string | null
  has_more?: boolean
}

export type AgentChatHandoffsResponse = AgentHandoff[]
