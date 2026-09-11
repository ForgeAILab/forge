import { useInfiniteQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { apiFetch } from '@/api/client'
import { qk } from '@/api/query-keys'
import type { MoveTaskRequest, MoveTaskResponse, TasksResponse } from '@/types/generated'

export type BoardTaskSearch = {
  q?: string
  agent_id?: string
  assignee_type?: string
  include_archived?: boolean
  include_cancelled?: boolean
  limit?: number
}

function stableSearchKey(search: BoardTaskSearch): string {
  return JSON.stringify(
    Object.entries(search)
      .filter(([, value]) => value !== undefined && value !== '')
      .sort(([left], [right]) => left.localeCompare(right)),
  )
}

export function useBoardTasks(projectId: string, search: BoardTaskSearch) {
  return useInfiniteQuery({
    queryKey: qk.tasks(projectId, `board:${stableSearchKey(search)}`),
    queryFn: ({ pageParam, signal }) =>
      apiFetch<TasksResponse>(`/projects/${projectId}/tasks`, {
        search: { ...search, cursor: pageParam as string | undefined },
        signal,
      }),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (last) => last.next_cursor ?? undefined,
  })
}

export function useMoveTask() {
  const queryClient = useQueryClient()
  return useMutation({
    retry: false,
    mutationFn: ({ taskId, body }: { taskId: string; body: MoveTaskRequest }) =>
      apiFetch<MoveTaskResponse>(`/tasks/${taskId}/move`, {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    onSuccess: (response) => {
      void queryClient.invalidateQueries({ queryKey: qk.task(response.task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.projectTasks(response.task.project_id) })
    },
  })
}
