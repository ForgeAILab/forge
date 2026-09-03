import { useMutation, useQueries, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  cancelAgentChatTurn,
  createAgentHandoff,
  getAgentChat,
  getAgentHandoff,
  listAgentChatMessages,
  listAgentChatTurnLogs,
  listAgentChatTurns,
  listAgentChats,
  listAgentHandoffs,
  sendAgentChatMessage,
} from './api'
import type {
  AgentChatMessageInput,
  AgentChatTurn,
  AgentChatTurnCancelInput,
  AgentHandoffInput,
} from './types'
import type { LogEntry } from '@/types/generated'

const AGENT_CHAT_POLL_INTERVAL = 2_000
const AGENT_CHAT_SWITCHER_POLL_INTERVAL = 5_000
/** A live turn's activity (tool calls, reasoning, reply text) is followed at this cadence. */
const AGENT_CHAT_ACTIVITY_POLL_INTERVAL = 1_000
const TURN_LOG_PAGE_LIMIT = 1_000
const TURN_LOG_MAX_PAGES_PER_FETCH = 20

export const agentChatQueryKeys = {
  chats: ['agent-chats'] as const,
  chat: (chatId: string) => ['agent-chats', chatId] as const,
  messages: (chatId: string) => ['agent-chats', chatId, 'messages'] as const,
  turns: (chatId: string) => ['agent-chats', chatId, 'turns'] as const,
  turnLogs: (chatId: string, turnId: string) =>
    ['agent-chats', chatId, 'turns', turnId, 'logs'] as const,
  handoffs: (projectId: string) => ['agent-handoffs', projectId] as const,
  handoff: (projectId: string, handoffId: string) =>
    ['agent-handoffs', projectId, handoffId] as const,
} as const

export function useAgentChatsQuery() {
  return useQuery({
    queryKey: agentChatQueryKeys.chats,
    queryFn: () => listAgentChats(),
    staleTime: 5_000,
    refetchInterval: AGENT_CHAT_SWITCHER_POLL_INTERVAL,
  })
}

export function useAgentChatQuery(chatId: string | undefined) {
  return useQuery({
    queryKey: agentChatQueryKeys.chat(chatId ?? 'none'),
    queryFn: () => getAgentChat(chatId!),
    enabled: Boolean(chatId),
    staleTime: 3_000,
    refetchInterval: AGENT_CHAT_SWITCHER_POLL_INTERVAL,
  })
}

export function useAgentChatMessagesQuery(chatId: string | undefined) {
  return useQuery({
    queryKey: agentChatQueryKeys.messages(chatId ?? 'none'),
    queryFn: () => listAgentChatMessages(chatId!),
    enabled: Boolean(chatId),
    staleTime: 2_000,
    refetchInterval: AGENT_CHAT_POLL_INTERVAL,
  })
}

export function useAgentChatTurnsQuery(chatId: string | undefined) {
  return useQuery({
    queryKey: agentChatQueryKeys.turns(chatId ?? 'none'),
    queryFn: () => listAgentChatTurns(chatId!),
    enabled: Boolean(chatId),
    staleTime: 1_000,
    refetchInterval: AGENT_CHAT_POLL_INTERVAL,
  })
}

/**
 * Extend an already-loaded activity log with everything the server has
 * recorded since its last entry. Attempts of one turn append to one log with
 * increasing sequences, so a poll only ever transfers the new tail.
 */
export async function fetchAgentChatTurnLogsAfter(
  chatId: string,
  turnId: string,
  previous: LogEntry[],
): Promise<LogEntry[]> {
  const items = [...previous]
  let from = items.length > 0 ? items[items.length - 1].sequence + 1 : 0
  for (let page = 0; page < TURN_LOG_MAX_PAGES_PER_FETCH; page += 1) {
    const response = await listAgentChatTurnLogs(chatId, turnId, {
      from_sequence: from,
      limit: TURN_LOG_PAGE_LIMIT,
    })
    items.push(...response.items)
    if (!response.has_more || response.next_sequence === null) break
    from = response.next_sequence
  }
  return items
}

/**
 * The durable activity log of one turn. While `live`, it follows the turn at
 * the activity cadence; afterwards it is fetched once per mount so a reader
 * who watched the turn sees its final tail without re-downloading the rest.
 */
export function useAgentChatTurnLogsQuery(
  chatId: string | undefined,
  turnId: string | undefined,
  options: { live: boolean; enabled?: boolean },
) {
  const queryClient = useQueryClient()
  const queryKey = agentChatQueryKeys.turnLogs(chatId ?? 'none', turnId ?? 'none')
  return useQuery({
    queryKey,
    enabled: Boolean(chatId && turnId) && (options.enabled ?? true),
    refetchInterval: options.live ? AGENT_CHAT_ACTIVITY_POLL_INTERVAL : false,
    refetchOnMount: options.live ? true : 'always',
    staleTime: options.live ? 0 : Number.POSITIVE_INFINITY,
    queryFn: () =>
      fetchAgentChatTurnLogsAfter(
        chatId!,
        turnId!,
        queryClient.getQueryData<LogEntry[]>(queryKey) ?? [],
      ),
  })
}

export function useSendAgentChatMessageMutation(chatId: string | undefined) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (input: AgentChatMessageInput) => sendAgentChatMessage(chatId!, input),
    onSuccess: () => {
      if (!chatId) return
      void queryClient.invalidateQueries({ queryKey: agentChatQueryKeys.chat(chatId) })
      void queryClient.invalidateQueries({ queryKey: agentChatQueryKeys.messages(chatId) })
      void queryClient.invalidateQueries({ queryKey: agentChatQueryKeys.turns(chatId) })
      void queryClient.invalidateQueries({ queryKey: agentChatQueryKeys.chats })
    },
  })
}

export function useCancelAgentChatTurnMutation(chatId: string | undefined) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ turnId, input }: { turnId: string; input: AgentChatTurnCancelInput }) =>
      cancelAgentChatTurn(chatId!, turnId, input),
    onSuccess: (cancelledTurn) => {
      if (!chatId) return
      queryClient.setQueryData<AgentChatTurn[] | undefined>(
        agentChatQueryKeys.turns(chatId),
        (turns) => turns?.map((turn) => (turn.id === cancelledTurn.id ? cancelledTurn : turn)),
      )
      void queryClient.invalidateQueries({ queryKey: agentChatQueryKeys.chat(chatId) })
      void queryClient.invalidateQueries({ queryKey: agentChatQueryKeys.messages(chatId) })
      void queryClient.invalidateQueries({ queryKey: agentChatQueryKeys.turns(chatId) })
      void queryClient.invalidateQueries({ queryKey: agentChatQueryKeys.chats })
    },
  })
}

export function useAgentHandoffsQuery(projectId: string | undefined) {
  return useQuery({
    queryKey: agentChatQueryKeys.handoffs(projectId ?? 'none'),
    queryFn: () => listAgentHandoffs(projectId!),
    enabled: Boolean(projectId),
    staleTime: 5_000,
    refetchInterval: AGENT_CHAT_POLL_INTERVAL,
  })
}

export function useCreateAgentHandoffMutation(projectId: string | undefined) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (input: AgentHandoffInput) => createAgentHandoff(projectId!, input),
    onSuccess: () => {
      if (!projectId) return
      void queryClient.invalidateQueries({ queryKey: agentChatQueryKeys.handoffs(projectId) })
      void queryClient.invalidateQueries({ queryKey: agentChatQueryKeys.chats })
      void queryClient.invalidateQueries({ queryKey: ['product-genesis', 'active'] })
    },
  })
}

export function useAgentHandoffsForProjectsQuery(projectIds: string[]) {
  const results = useQueries({
    queries: projectIds.map((projectId) => ({
      queryKey: agentChatQueryKeys.handoffs(projectId),
      queryFn: () => listAgentHandoffs(projectId),
      staleTime: 5_000,
      refetchInterval: AGENT_CHAT_POLL_INTERVAL,
    })),
  })
  return {
    data: results.flatMap((result) => result.data ?? []),
    isLoading: results.some((result) => result.isLoading),
    isError: results.some((result) => result.isError),
  }
}

export function useAgentHandoffQuery(projectId: string | undefined, handoffId: string | undefined) {
  return useQuery({
    queryKey: agentChatQueryKeys.handoff(projectId ?? 'none', handoffId ?? 'none'),
    queryFn: () => getAgentHandoff(projectId!, handoffId!),
    enabled: Boolean(projectId && handoffId),
    staleTime: 10_000,
    refetchInterval: AGENT_CHAT_POLL_INTERVAL,
  })
}
