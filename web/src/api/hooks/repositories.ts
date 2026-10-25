// Repository query and mutation hooks.

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { apiFetch } from '@/api/client'
import { qk } from '@/api/query-keys'
import type {
  CreateRepoRequest,
  RepoSyncResponse,
  UpdateRepoRequest,
  PaginatedResponse,
  Repo,
} from '@/types/generated'

// --- Repos ---

export function useReposQuery(projectId: string, options?: { enabled?: boolean }) {
  return useQuery({
    queryKey: qk.repos(projectId),
    queryFn: () => apiFetch<PaginatedResponse<Repo>>(`/projects/${projectId}/repos`),
    enabled: options?.enabled ?? Boolean(projectId),
  })
}

export function useCreateRepo(projectId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: CreateRepoRequest) =>
      apiFetch<Repo>(`/projects/${projectId}/repos`, {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.repos(projectId) })
      void queryClient.invalidateQueries({ queryKey: qk.project(projectId) })
      void queryClient.invalidateQueries({ queryKey: qk.projects })
    },
  })
}

export function useUpdateRepo(repoId: string, projectId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: UpdateRepoRequest) =>
      apiFetch<Repo>(`/repos/${repoId}`, {
        method: 'PATCH',
        body: JSON.stringify(body),
      }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.repos(projectId) })
      void queryClient.invalidateQueries({ queryKey: qk.project(projectId) })
    },
  })
}

export function useSyncRepo(repoId: string, projectId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: () =>
      apiFetch<RepoSyncResponse>(`/repos/${repoId}/sync`, {
        method: 'POST',
      }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.repos(projectId) })
    },
  })
}
