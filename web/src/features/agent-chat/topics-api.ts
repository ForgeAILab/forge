// Main Chat topic boundary REST client (design D21, live-acceptance finding
// F18: "the singular Main Chat has no fresh-topic boundary").
//
// A topic is a durable, user-owned context epoch inside the one account Main
// Chat -- this module never creates a second chat resource. It only lists
// and starts topics on an existing `agent-chats/{chat_id}` resource.

import { apiFetch, ApiError } from '@/api/client'
import type { AgentChatTopicListResponse } from '@/types/generated/bindings/AgentChatTopicListResponse'
import type { AgentChatTopicResponse } from '@/types/generated/bindings/AgentChatTopicResponse'
import type { StartAgentChatTopicRequest } from '@/types/generated/bindings/StartAgentChatTopicRequest'
import type { StartAgentChatTopicResponse } from '@/types/generated/bindings/StartAgentChatTopicResponse'

export type AgentChatTopic = AgentChatTopicResponse
export type StartAgentChatTopicInput = StartAgentChatTopicRequest

export const agentChatTopicsApiPaths = {
  topics: (chatId: string) => `/agent-chats/${chatId}/topics`,
} as const

export function listAgentChatTopics(chatId: string): Promise<AgentChatTopicListResponse> {
  return apiFetch<AgentChatTopicListResponse>(agentChatTopicsApiPaths.topics(chatId))
}

export function startAgentChatTopic(
  chatId: string,
  input: StartAgentChatTopicInput,
): Promise<StartAgentChatTopicResponse> {
  return apiFetch<StartAgentChatTopicResponse>(agentChatTopicsApiPaths.topics(chatId), {
    method: 'POST',
    body: JSON.stringify(input),
  })
}

/** A topic-reset request denied per D21 (a live Main turn, or a Genesis
 * session/approval still needing an explicit finish-or-cancel decision) is a
 * normal, explainable outcome -- not an unexpected failure. The server
 * reports it as `409 Conflict` with a safe, specific message. */
export function isTopicResetDenied(cause: unknown): cause is ApiError {
  return cause instanceof ApiError && cause.status === 409
}
