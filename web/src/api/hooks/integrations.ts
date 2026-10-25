// External integration configuration and sync hooks.

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { apiFetch } from '@/api/client'
import { qk } from '@/api/query-keys'

// --- Integrations ---

export type IntegrationResponse = {
  id: string
  project_id: string
  platform: string
  base_url: string
  owner: string
  repo: string
  token_secret_ref: string
  poll_interval_secs: number
  sync_filter: Record<string, unknown>
  default_task_state: string | null
  default_assignee_type: string | null
  default_assignee_id: string | null
  enabled: boolean
  last_polled_at: string | null
  created_at: string
  updated_at: string
}

export type CreateIntegrationRequest = {
  platform: string
  base_url: string
  owner: string
  repo: string
  token_secret_ref: string
  poll_interval_secs?: number
  sync_filter?: Record<string, unknown>
  default_task_state?: string
  default_assignee_type?: string
  default_assignee_id?: string
  enabled?: boolean
}

export type PatchIntegrationRequest = {
  platform?: string
  base_url?: string
  owner?: string
  repo?: string
  token_secret_ref?: string
  poll_interval_secs?: number
  sync_filter?: Record<string, unknown>
  default_task_state?: string
  default_assignee_type?: string
  default_assignee_id?: string
  enabled?: boolean
}

export type SyncTriggerResponse = {
  imported: number
  skipped: number
  errors: number
}

export function useIntegrationQuery(projectId: string) {
  return useQuery({
    queryKey: qk.integration(projectId),
    queryFn: () => apiFetch<IntegrationResponse | null>(`/projects/${projectId}/integration`),
    retry: false,
  })
}

export function useCreateIntegration() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ projectId, body }: { projectId: string; body: CreateIntegrationRequest }) =>
      apiFetch<IntegrationResponse>(`/projects/${projectId}/integration`, {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    onSuccess: (_, vars) => {
      void queryClient.invalidateQueries({ queryKey: qk.integration(vars.projectId) })
    },
  })
}

export function useUpdateIntegration() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ projectId, body }: { projectId: string; body: PatchIntegrationRequest }) =>
      apiFetch<IntegrationResponse>(`/projects/${projectId}/integration`, {
        method: 'PATCH',
        body: JSON.stringify(body),
      }),
    onSuccess: (_, vars) => {
      void queryClient.invalidateQueries({ queryKey: qk.integration(vars.projectId) })
    },
  })
}

export function useDeleteIntegration() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (projectId: string) =>
      apiFetch<void>(`/projects/${projectId}/integration`, { method: 'DELETE' }),
    onSuccess: (_, projectId) => {
      void queryClient.invalidateQueries({ queryKey: qk.integration(projectId) })
    },
  })
}

export function useTriggerSync() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (projectId: string) =>
      apiFetch<SyncTriggerResponse>(`/projects/${projectId}/integration/sync`, {
        method: 'POST',
      }),
    onSuccess: (_, projectId) => {
      void queryClient.invalidateQueries({ queryKey: qk.integration(projectId) })
    },
  })
}
