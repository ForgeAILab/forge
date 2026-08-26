// Main Chat topic boundary hooks (design D21, live-acceptance finding F18).

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { listAgentChatTopics, startAgentChatTopic, type StartAgentChatTopicInput } from './topics-api'
import { agentChatQueryKeys } from './hooks'

const TOPICS_POLL_INTERVAL = 5_000

export const agentChatTopicQueryKeys = {
  topics: (chatId: string) => ['agent-chats', chatId, 'topics'] as const,
} as const

export function useAgentChatTopicsQuery(chatId: string | undefined) {
  return useQuery({
    queryKey: agentChatTopicQueryKeys.topics(chatId ?? 'none'),
    queryFn: () => listAgentChatTopics(chatId!),
    enabled: Boolean(chatId),
    staleTime: 3_000,
    refetchInterval: TOPICS_POLL_INTERVAL,
  })
}

export function useStartAgentChatTopicMutation(chatId: string | undefined) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (input: StartAgentChatTopicInput) => startAgentChatTopic(chatId!, input),
    onSuccess: () => {
      if (!chatId) return
      // A rotation both starts a new topic row and appends a visible
      // divider message, so both projections need to converge.
      void queryClient.invalidateQueries({ queryKey: agentChatTopicQueryKeys.topics(chatId) })
      void queryClient.invalidateQueries({ queryKey: agentChatQueryKeys.messages(chatId) })
      void queryClient.invalidateQueries({ queryKey: agentChatQueryKeys.chat(chatId) })
    },
  })
}
