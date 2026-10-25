// Daemon, operations, CLI projection, and MCP configuration hooks.

import type { RemoveDaemonPreview } from '@/types/generated/bindings/RemoveDaemonPreview'
import type { RemoveDaemonResponse } from '@/types/generated/bindings/RemoveDaemonResponse'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  apiFetch,
  getOperationsStatus,
  refreshOperations,
  replayDeadLetter,
  dismissDeadLetter,
} from '@/api/client'
import { qk } from '@/api/query-keys'
import type {
  Daemon,
  ExecutorType,
  McpConfigActionRequest,
  McpConfigResponse,
  McpEndpointResponse,
  PaginatedResponse,
} from '@/types/generated'

export type CliProjectionItem = {
  daemon_id: string
  daemon_hostname: string
  daemon_status: string
  kind: string
  availability: string
  config_path: string | null
  version: string | null
  path: string | null
  agents: Array<{
    id: string
    name: string
    executor_type: string
    effective_status: string | null
  }>
}

export type CliProjectionResponse = {
  items: CliProjectionItem[]
}

export function useExecutorTypesQuery() {
  return useQuery({
    queryKey: qk.executorTypes,
    queryFn: () => apiFetch<ExecutorType[]>('/executor-types'),
  })
}

export function useUpdateDaemonRunLimit() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({
      id,
      version,
      run_limit,
    }: {
      id: string
      version: number
      run_limit: number | null
    }) =>
      apiFetch<Daemon>(`/daemons/${id}`, {
        method: 'PATCH',
        body: JSON.stringify({ version, run_limit }),
      }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.daemons })
      void queryClient.invalidateQueries({ queryKey: qk.operationsStatus })
    },
    onError: async () => {
      await queryClient.invalidateQueries({ queryKey: qk.daemons })
    },
  })
}

/** What removing a machine would change; read while its confirmation is open. */
export function useDaemonRemovalPreview(id: string, enabled: boolean) {
  return useQuery({
    queryKey: [...qk.daemons, id, 'removal'],
    queryFn: () => apiFetch<RemoveDaemonPreview>(`/daemons/${id}/removal`),
    enabled,
    staleTime: 0,
  })
}

export function useRemoveDaemon() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (id: string) =>
      apiFetch<RemoveDaemonResponse>(`/daemons/${id}`, { method: 'DELETE' }),
    onSuccess: () => {
      for (const queryKey of [
        qk.daemons,
        qk.agents,
        qk.projects,
        ['tasks'],
        ['providers'],
        qk.operationsStatus,
      ]) {
        void queryClient.invalidateQueries({ queryKey })
      }
    },
    onError: async () => {
      await queryClient.invalidateQueries({ queryKey: qk.daemons })
    },
  })
}

export function useDaemonsQuery(enabled = true) {
  return useQuery({
    queryKey: qk.daemons,
    queryFn: () => apiFetch<PaginatedResponse<Daemon>>('/daemons'),
    enabled,
  })
}

export function useOperationsStatusQuery() {
  return useQuery({
    queryKey: qk.operationsStatus,
    queryFn: getOperationsStatus,
    refetchInterval: (query) => (query.state.status === 'error' ? false : 30_000),
  })
}

export function useRefreshOperationsMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: refreshOperations,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.operationsStatus })
    },
  })
}

export function useDeadLetterActionMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({
      id,
      action,
      reason,
    }: {
      id: string
      action: 'replay' | 'dismiss'
      reason?: string
    }) => (action === 'replay' ? replayDeadLetter(id) : dismissDeadLetter(id, reason)),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.operationsStatus })
    },
  })
}

export function useCliProjectionQuery() {
  return useQuery({
    queryKey: qk.clis,
    queryFn: () => apiFetch<CliProjectionResponse>('/clis'),
  })
}

function browserPublicBaseUrl(): string | undefined {
  if (typeof window === 'undefined') return undefined
  return window.location.origin
}

export function useMcpEndpointQuery(scope: string, projectId?: string, enabled = true) {
  return useQuery({
    queryKey: qk.mcpEndpoint(scope, projectId),
    enabled,
    queryFn: () =>
      apiFetch<McpEndpointResponse>('/config/mcp/endpoint', {
        search: { scope, ...(projectId ? { project_id: projectId } : {}) },
      }),
  })
}

export function useMcpConfigQuery(
  agent: string,
  scope: string,
  projectId?: string,
  enabled = true,
) {
  return useQuery({
    queryKey: qk.mcpConfig(agent, scope, projectId),
    enabled,
    queryFn: () =>
      apiFetch<McpConfigResponse>('/config/mcp', {
        search: {
          agent,
          scope,
          public_base_url: browserPublicBaseUrl(),
          ...(projectId ? { project_id: projectId } : {}),
        },
      }),
  })
}

export function useUpdateMcpConfig() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: McpConfigActionRequest) =>
      apiFetch<McpConfigResponse>('/config/mcp', {
        method: 'POST',
        body: JSON.stringify({
          ...body,
          public_base_url: body.public_base_url ?? browserPublicBaseUrl(),
        }),
      }),
    onSuccess: (_data, variables) => {
      void queryClient.invalidateQueries({
        queryKey: qk.mcpConfig(variables.agent, variables.scope ?? 'project', variables.project_id),
      })
    },
  })
}
