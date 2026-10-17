import { useQuery } from '@tanstack/react-query'
import { apiFetch } from '@/api/client'
import { qk } from '@/api/query-keys'
import type { TaskFindingsResponse } from '@/types/generated/bindings/TaskFindingsResponse'

export function useTaskFindingsQuery(taskId: string) {
  return useQuery({
    // Review events, recovery and owner actions already invalidate this prefix.
    queryKey: [...qk.reviews(taskId), 'findings'],
    queryFn: ({ signal }) =>
      apiFetch<TaskFindingsResponse>(`/tasks/${taskId}/findings`, { signal }),
    enabled: Boolean(taskId),
  })
}
