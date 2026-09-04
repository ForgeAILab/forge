import { apiFetch } from '@/api/client'
import type {
  AgentInquiryListResponse,
  AgentInquiryResponse,
  CancelAgentInquiryRequest,
  LogEntry,
} from '@/types/generated'

export type ListAgentInquiriesParams = {
  limit?: number
  cursor?: string
}

export function listAgentInquiries(
  chatId: string,
  params?: ListAgentInquiriesParams,
): Promise<AgentInquiryListResponse> {
  return apiFetch<AgentInquiryListResponse>(`/agent-chats/${chatId}/inquiries`, {
    search: { limit: params?.limit, cursor: params?.cursor },
  })
}

export function getAgentInquiry(id: string): Promise<AgentInquiryResponse> {
  return apiFetch<AgentInquiryResponse>(`/inquiries/${id}`)
}

/**
 * The generated request models `expected_version` as `bigint` (ts-rs i64
 * mapping); the wire body is a plain JSON number, so the app-facing input
 * stays numeric like every other expected-version input in this codebase.
 */
export type CancelAgentInquiryInput = Omit<CancelAgentInquiryRequest, 'expected_version'> & {
  expected_version: number
}

export function cancelAgentInquiry(
  id: string,
  input: CancelAgentInquiryInput,
): Promise<AgentInquiryResponse> {
  return apiFetch<AgentInquiryResponse>(`/inquiries/${id}/cancel`, {
    method: 'POST',
    body: JSON.stringify(input),
  })
}

export type AgentInquiryLogsParams = {
  from_sequence?: number
  limit?: number
  tail?: number
}

/**
 * One keyset page of an inquiry's durable activity log: the same Forge
 * JSONL log page shape as `/executions/{id}/logs` and an Agent Chat turn's
 * `/turns/{id}/logs`. An inquiry that has not written anything yet reads as
 * an empty page rather than a 404.
 */
export type AgentInquiryLogsPage = {
  items: LogEntry[]
  has_more: boolean
  next_sequence: number | null
}

export async function listAgentInquiryLogs(
  id: string,
  params?: AgentInquiryLogsParams,
): Promise<AgentInquiryLogsPage> {
  const page = await apiFetch<Partial<AgentInquiryLogsPage> | undefined>(`/inquiries/${id}/logs`, {
    search: {
      from_sequence: params?.from_sequence,
      limit: params?.limit,
      tail: params?.tail,
    },
  })
  return {
    items: page?.items ?? [],
    has_more: page?.has_more ?? false,
    next_sequence: page?.next_sequence ?? null,
  }
}
