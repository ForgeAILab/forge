import { apiFetch } from '@/api/client'
import type { LogEntry } from '@/types/generated'
import type { AgentChatDetailResponse } from '@/types/generated/bindings/AgentChatDetailResponse'
import type { AgentChatMessageListResponse } from '@/types/generated/bindings/AgentChatMessageListResponse'
import type { AgentChatTurnJobResponse } from '@/types/generated/bindings/AgentChatTurnJobResponse'
import type { SendAgentChatMessageResponse } from '@/types/generated/bindings/SendAgentChatMessageResponse'
import type {
  AgentChat,
  AgentChatHandoffsResponse,
  AgentChatMessageAdmission,
  AgentChatMessageInput,
  AgentChatMessagesResponse,
  AgentChatTurnsResponse,
  AgentChatTurnListResponse,
  AgentHandoff,
  AgentHandoffInput,
  AgentChatSwitcher,
  AgentChatTurnCancelInput,
} from './types'
import type { AgentChatSwitcherResponse } from '@/types/generated/bindings/AgentChatSwitcherResponse'

export const agentChatApiPaths = {
  chats: '/agent-chats',
  chat: (chatId: string) => `/agent-chats/${chatId}`,
  messages: (chatId: string) => `/agent-chats/${chatId}/messages`,
  turns: (chatId: string) => `/agent-chats/${chatId}/turns`,
  cancelTurn: (chatId: string, turnId: string) => `/agent-chats/${chatId}/turns/${turnId}/cancel`,
  turnLogs: (chatId: string, turnId: string) => `/agent-chats/${chatId}/turns/${turnId}/logs`,
  handoffs: (projectId: string) => `/projects/${projectId}/agent-handoffs`,
  handoff: (projectId: string, handoffId: string) =>
    `/projects/${projectId}/agent-handoffs/${handoffId}`,
} as const

export function listAgentChats(): Promise<AgentChatSwitcher> {
  return apiFetch<AgentChatSwitcherResponse>(agentChatApiPaths.chats)
}

export async function getAgentChat(chatId: string): Promise<AgentChat> {
  const response = await apiFetch<AgentChatDetailResponse>(agentChatApiPaths.chat(chatId))
  return response.chat
}

export async function listAgentChatMessages(
  chatId: string,
  limit = 200,
): Promise<AgentChatMessagesResponse> {
  const response = await apiFetch<AgentChatMessageListResponse>(
    agentChatApiPaths.messages(chatId),
    {
      search: { limit },
    },
  )
  return response
}

export function listAgentChatTurns(chatId: string, limit = 100): Promise<AgentChatTurnsResponse> {
  return apiFetch<AgentChatTurnsResponse | AgentChatTurnListResponse>(
    agentChatApiPaths.turns(chatId),
    {
      search: { limit },
    },
  ).then((response) => (Array.isArray(response) ? response : response.items))
}

export function sendAgentChatMessage(
  chatId: string,
  input: AgentChatMessageInput,
): Promise<AgentChatMessageAdmission> {
  return apiFetch<SendAgentChatMessageResponse>(agentChatApiPaths.messages(chatId), {
    method: 'POST',
    body: JSON.stringify(input),
  })
}

export function cancelAgentChatTurn(
  chatId: string,
  turnId: string,
  input: AgentChatTurnCancelInput,
): Promise<AgentChatTurnJobResponse> {
  return apiFetch<AgentChatTurnJobResponse>(agentChatApiPaths.cancelTurn(chatId, turnId), {
    method: 'POST',
    body: JSON.stringify(input),
  })
}

export type AgentChatTurnLogsParams = {
  from_sequence?: number
  limit?: number
  tail?: number
}

/** One keyset page of a turn's durable activity log (same shape as execution logs). */
export type AgentChatTurnLogsPage = {
  items: LogEntry[]
  has_more: boolean
  next_sequence: number | null
}

export async function listAgentChatTurnLogs(
  chatId: string,
  turnId: string,
  params?: AgentChatTurnLogsParams,
): Promise<AgentChatTurnLogsPage> {
  const page = await apiFetch<Partial<AgentChatTurnLogsPage> | undefined>(
    agentChatApiPaths.turnLogs(chatId, turnId),
    {
      search: {
        from_sequence: params?.from_sequence,
        limit: params?.limit,
        tail: params?.tail,
      },
    },
  )
  return {
    items: page?.items ?? [],
    has_more: page?.has_more ?? false,
    next_sequence: page?.next_sequence ?? null,
  }
}

export function listAgentHandoffs(
  projectId: string,
  limit = 100,
): Promise<AgentChatHandoffsResponse> {
  return apiFetch<AgentHandoff[] | { items: AgentHandoff[] }>(
    agentChatApiPaths.handoffs(projectId),
    {
      search: { limit },
    },
  ).then((response) => (Array.isArray(response) ? response : response.items))
}

export function createAgentHandoff(
  projectId: string,
  input: AgentHandoffInput,
): Promise<AgentHandoff> {
  return apiFetch<AgentHandoff>(agentChatApiPaths.handoffs(projectId), {
    method: 'POST',
    body: JSON.stringify(input),
  })
}

export function getAgentHandoff(projectId: string, handoffId: string): Promise<AgentHandoff> {
  return apiFetch<AgentHandoff>(agentChatApiPaths.handoff(projectId, handoffId))
}
