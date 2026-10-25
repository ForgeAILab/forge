// Agent configuration, availability, and inquiry hooks.

import { useEffect, useRef } from 'react'
import {
  type InfiniteData,
  useInfiniteQuery,
  useMutation,
  useQuery,
  useQueryClient,
} from '@tanstack/react-query'
import { apiFetch } from '@/api/client'
import {
  cancelAgentInquiry,
  listAgentInquiries,
  listAgentInquiryLogs,
  type CancelAgentInquiryInput,
} from '@/api/agent-inquiries'
import { qk } from '@/api/query-keys'
import type {
  Agent,
  AgentAvailability,
  AgentDiscoveredOptions,
  CreateAgentRequest,
  LogEntry,
  PaginatedResponse,
  UpdateAgentRequest,
  AgentInquiryResponse,
} from '@/types/generated'

// --- Agents ---

const AGENTS_CACHE_STALE_TIME = 10 * 60 * 1000

const AGENTS_CACHE_GC_TIME = 60 * 60 * 1000

type AgentPagesData = InfiniteData<PaginatedResponse<Agent>, string | undefined>

function isPaginatedAgents(data: unknown): data is PaginatedResponse<Agent> {
  return (
    typeof data === 'object' && data !== null && Array.isArray((data as { items?: unknown }).items)
  )
}

function isAgentPagesData(data: unknown): data is AgentPagesData {
  return (
    typeof data === 'object' && data !== null && Array.isArray((data as { pages?: unknown }).pages)
  )
}

function findCachedAgent(
  queryClient: ReturnType<typeof useQueryClient>,
  agentId: string,
): Agent | undefined {
  const cachedAgent = queryClient.getQueryData<Agent>(qk.agent(agentId))
  if (cachedAgent) return cachedAgent

  for (const [, data] of queryClient.getQueriesData<unknown>({ queryKey: qk.agents })) {
    if (isPaginatedAgents(data)) {
      const agent = data.items.find((item) => item.id === agentId)
      if (agent) return agent
    }

    if (isAgentPagesData(data)) {
      for (const page of data.pages) {
        const agent = page.items.find((item) => item.id === agentId)
        if (agent) return agent
      }
    }
  }

  return undefined
}

export function useAgentsQuery(options: { enabled?: boolean } = {}) {
  return useQuery({
    queryKey: qk.agents,
    queryFn: ({ signal }) =>
      apiFetch<PaginatedResponse<Agent>>('/agents', { search: { limit: 100 }, signal }),
    enabled: options.enabled ?? true,
    staleTime: AGENTS_CACHE_STALE_TIME,
    gcTime: AGENTS_CACHE_GC_TIME,
  })
}

export function useAgentsInfiniteQuery(limit = 20) {
  return useInfiniteQuery({
    queryKey: qk.agentPages(limit),
    queryFn: ({ pageParam }) =>
      apiFetch<PaginatedResponse<Agent>>('/agents', {
        search: { cursor: pageParam as string | undefined, limit, include_total: true },
      }),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (last) => last.next_cursor ?? undefined,
  })
}

export function useAgentQuery(agentId?: string) {
  const queryClient = useQueryClient()
  return useQuery({
    queryKey: qk.agent(agentId ?? ''),
    queryFn: () => apiFetch<Agent>(`/agents/${agentId}`),
    enabled: Boolean(agentId),
    placeholderData: () => (agentId ? findCachedAgent(queryClient, agentId) : undefined),
    gcTime: AGENTS_CACHE_GC_TIME,
  })
}

export function useCreateAgent() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: CreateAgentRequest) =>
      apiFetch<Agent>('/agents', {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.agents })
    },
  })
}

export function useUpdateAgent() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ agentId, body }: { agentId: string; body: UpdateAgentRequest }) =>
      apiFetch<Agent>(`/agents/${agentId}`, {
        method: 'PATCH',
        body: JSON.stringify(body),
      }),
    onSuccess: (agent) => {
      void queryClient.invalidateQueries({ queryKey: qk.agent(agent.id) })
      void queryClient.invalidateQueries({ queryKey: qk.agents })
    },
  })
}

export function usePauseAgent() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (agentId: string) =>
      apiFetch<Agent>(`/agents/${agentId}/pause`, {
        method: 'POST',
      }),
    onSuccess: (agent) => {
      void queryClient.invalidateQueries({ queryKey: qk.agent(agent.id) })
      void queryClient.invalidateQueries({ queryKey: qk.agents })
    },
  })
}

export function useResumeAgent() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (agentId: string) =>
      apiFetch<Agent>(`/agents/${agentId}/resume`, {
        method: 'POST',
      }),
    onSuccess: (agent) => {
      void queryClient.invalidateQueries({ queryKey: qk.agent(agent.id) })
      void queryClient.invalidateQueries({ queryKey: qk.agents })
    },
  })
}

export function useDeleteAgent() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (agentId: string) =>
      apiFetch<void>(`/agents/${agentId}`, {
        method: 'DELETE',
      }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.agents })
    },
  })
}

export function useDuplicateAgent() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ agentId, name }: { agentId: string; name: string }) =>
      apiFetch<Agent>(`/agents/${agentId}/duplicate`, {
        method: 'POST',
        body: JSON.stringify({ name }),
      }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.agents })
    },
  })
}

export function useAgentDiscoveredOptions(agentId: string | null | undefined) {
  return useQuery({
    queryKey: qk.agentDiscoveredOptions(agentId ?? ''),
    queryFn: () => apiFetch<AgentDiscoveredOptions>(`/agents/${agentId}/discovered-options`),
    enabled: Boolean(agentId),
    staleTime: 5 * 60 * 1000,
  })
}

export function useAgentAvailability(agentId: string | null | undefined) {
  return useQuery({
    queryKey: qk.agentAvailability(agentId ?? ''),
    queryFn: () => apiFetch<AgentAvailability>(`/agents/${agentId}/availability`),
    enabled: Boolean(agentId),
    staleTime: 30 * 1000,
  })
}

// --- Agent Inquiries ---
// Ephemeral, read-only Main Agent sub-agent runs (`inquiry.run`). A run log,
// not a work item: the only mutation is cancel, and only while running.

const AGENT_INQUIRY_LIST_LIMIT = 50

const AGENT_INQUIRY_POLL_INTERVAL = 15_000

/** A running inquiry's activity (tool calls, reasoning, reply deltas) is followed at this cadence. */
const AGENT_INQUIRY_ACTIVITY_POLL_INTERVAL = 1_500

const AGENT_INQUIRY_LOG_PAGE_LIMIT = 1_000

const AGENT_INQUIRY_LOG_MAX_PAGES_PER_FETCH = 20

/**
 * Extend an already-loaded activity log with everything the server has
 * recorded since its last entry. An inquiry appends to one log with
 * increasing sequences, so a poll only ever transfers the new tail — same
 * idiom as `fetchAgentChatTurnLogsAfter`.
 */
export async function fetchAgentInquiryLogsAfter(
  inquiryId: string,
  previous: LogEntry[],
): Promise<LogEntry[]> {
  const items = [...previous]
  let from = items.length > 0 ? items[items.length - 1].sequence + 1 : 0
  for (let page = 0; page < AGENT_INQUIRY_LOG_MAX_PAGES_PER_FETCH; page += 1) {
    const response = await listAgentInquiryLogs(inquiryId, {
      from_sequence: from,
      limit: AGENT_INQUIRY_LOG_PAGE_LIMIT,
    })
    items.push(...response.items)
    if (!response.has_more || response.next_sequence === null) break
    from = response.next_sequence
  }
  return items
}

/**
 * The durable activity log of one inquiry: reasoning, tool calls with their
 * bounded results, and reply deltas. While `live`, it follows the inquiry at
 * the activity cadence; afterwards it is fetched once per mount so a reader
 * who watched it run sees its final tail without re-downloading the rest.
 */
export function useAgentInquiryLogsQuery(
  inquiryId: string | undefined,
  options: { live: boolean; enabled?: boolean },
) {
  const queryClient = useQueryClient()
  const queryKey = qk.agentInquiryLogs(inquiryId ?? 'none')
  const previous = useRef({ inquiryId, live: options.live })
  useEffect(() => {
    const wasRunning = previous.current.inquiryId === inquiryId && previous.current.live
    previous.current = { inquiryId, live: options.live }
    if (!inquiryId || !wasRunning || options.live) return

    const filter = { queryKey: qk.agentInquiryLogs(inquiryId) }
    // A poll already in flight may have read before completion. Cancel it
    // before fetching the terminal tail, including the initial empty read.
    // Closed rows stay invalidated and drain when opened again.
    void queryClient.cancelQueries(filter).then(() => queryClient.invalidateQueries(filter))
  }, [inquiryId, options.live, queryClient])

  return useQuery({
    queryKey,
    enabled: Boolean(inquiryId) && (options.enabled ?? true),
    refetchIntervalInBackground: false,
    refetchInterval: (query) =>
      query.state.status === 'error' || !options.live
        ? false
        : AGENT_INQUIRY_ACTIVITY_POLL_INTERVAL,
    refetchOnMount: options.live ? true : 'always',
    staleTime: options.live ? 0 : Number.POSITIVE_INFINITY,
    queryFn: () =>
      fetchAgentInquiryLogsAfter(inquiryId!, queryClient.getQueryData<LogEntry[]>(queryKey) ?? []),
  })
}

export function useAgentInquiriesQuery(
  chatId: string | undefined,
  limit = AGENT_INQUIRY_LIST_LIMIT,
) {
  return useInfiniteQuery({
    queryKey: qk.agentInquiryPages(chatId ?? 'none', limit),
    queryFn: ({ pageParam }) => listAgentInquiries(chatId!, { cursor: pageParam, limit }),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (last) => last.next_cursor ?? undefined,
    enabled: Boolean(chatId),
    // SSE drives updates; polling is a slow foreground fallback.
    refetchIntervalInBackground: false,
    refetchInterval: (query) =>
      query.state.status === 'error' ? false : AGENT_INQUIRY_POLL_INTERVAL,
  })
}

export function useCancelAgentInquiryMutation(chatId: string | undefined) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, input }: { id: string; input: CancelAgentInquiryInput }) =>
      cancelAgentInquiry(id, input),
    onSuccess: (cancelled) => {
      if (!chatId) return
      // Settle the cancelled inquiry into every cached page immediately so
      // the Cancel action disappears without waiting on the next poll tick.
      queryClient.setQueriesData<InfiniteData<{ items: AgentInquiryResponse[] }> | undefined>(
        { queryKey: qk.agentInquiries(chatId) },
        (data) =>
          data && {
            ...data,
            pages: data.pages.map((page) => ({
              ...page,
              items: page.items.map((item) => (item.id === cancelled.id ? cancelled : item)),
            })),
          },
      )
      void queryClient.invalidateQueries({ queryKey: qk.agentInquiries(chatId) })
    },
  })
}
