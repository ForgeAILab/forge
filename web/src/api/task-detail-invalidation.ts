import type { QueryClient } from '@tanstack/react-query'
import type { TaskDetailResponse } from '@/types/generated'

export function invalidateProjectTaskDetails(queryClient: QueryClient, projectId: string): void {
  void queryClient.invalidateQueries({
    predicate: (query) =>
      query.queryKey[0] === 'tasks' &&
      query.queryKey.length === 3 &&
      query.queryKey[2] === 'detail' &&
      (query.state.data as TaskDetailResponse | undefined)?.task.project_id === projectId,
    refetchType: 'active',
  })
}
