// Execution, usage, and log hooks.

import { useQuery } from '@tanstack/react-query'
import { apiFetch } from '@/api/client'
import { qk } from '@/api/query-keys'
import type {
  Execution,
  ExecutionSummary,
  UsageBreakdown,
  LogEntry,
  PaginatedResponse,
  UsageAggregate,
} from '@/types/generated'

export type ExecutionLogsParams = {
  tail?: number
  from_sequence?: number
  limit?: number
}

export type ExecutionLogsResponse = {
  items: LogEntry[]
  has_more: boolean
  next_sequence?: number | null
}

// --- Executions ---

export function useExecutionsQuery(taskId: string, options: { enabled?: boolean } = {}) {
  return useQuery({
    queryKey: qk.executions(taskId),
    queryFn: ({ signal }) =>
      apiFetch<PaginatedResponse<ExecutionSummary>>(`/tasks/${taskId}/executions`, { signal }),
    enabled: Boolean(taskId) && (options.enabled ?? true),
  })
}

export function useExecutionQuery(executionId: string) {
  return useQuery({
    queryKey: qk.execution(executionId),
    queryFn: () => apiFetch<Execution>(`/executions/${executionId}`),
    enabled: Boolean(executionId),
  })
}

export function useUsageBreakdownsQuery(executionId: string) {
  return useQuery({
    queryKey: qk.usageBreakdowns(executionId),
    queryFn: () => apiFetch<UsageBreakdown[]>(`/executions/${executionId}/usage`),
  })
}

export function useTaskUsageQuery(taskId: string) {
  return useQuery({
    queryKey: qk.taskUsage(taskId),
    queryFn: ({ signal }) => apiFetch<UsageAggregate>(`/tasks/${taskId}/usage`, { signal }),
  })
}

export async function getExecutionLogs(
  executionId: string,
  params?: ExecutionLogsParams,
): Promise<ExecutionLogsResponse> {
  const data = await apiFetch<ExecutionLogsResponse>(`/executions/${executionId}/logs`, {
    search: {
      tail: params?.tail,
      from_sequence: params?.from_sequence,
      limit: params?.limit,
    },
  })
  const response: ExecutionLogsResponse = {
    items: data.items ?? [],
    has_more: data.has_more ?? false,
  }
  if (data.next_sequence !== undefined) response.next_sequence = data.next_sequence
  return response
}

export function useExecutionLogs(
  executionId: string,
  params?: ExecutionLogsParams,
  isRunning?: boolean,
) {
  return useQuery({
    queryKey: [...qk.executionLogs(executionId), params ?? {}] as const,
    enabled: Boolean(executionId),
    refetchInterval: (query) => (query.state.status === 'error' || !isRunning ? false : 3_000),
    queryFn: () => getExecutionLogs(executionId, params),
  })
}

export interface HookLogEntry {
  event: string
  hook_type: string
  command?: string
  plugin_name?: string
  duration_ms: number
  status: string
  exit_code?: number | null
  timeout?: boolean
  working_dir?: string
  stdout?: string
  stderr?: string
  error?: string
  reason?: string
}

export function getExecutionHookLogs(executionId: string): Promise<HookLogEntry[]> {
  return apiFetch<HookLogEntry[]>(`/executions/${executionId}/hook-logs`)
}

export function useExecutionHookLogs(executionId: string | undefined) {
  return useQuery({
    queryKey: qk.executionHookLogs(executionId ?? ''),
    enabled: !!executionId,
    queryFn: () => getExecutionHookLogs(executionId ?? ''),
  })
}
