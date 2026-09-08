import type { QueryClient } from '@tanstack/react-query'

/**
 * Invalidate every time-window variant of the analytics projections affected
 * by a usage or released-milestone event. The finite-window keys include
 * their bounds after the `analytics` segment, so the prefixes here must stop
 * before those bounds.
 */
export function invalidateAnalyticsQueries(queryClient: QueryClient, projectId?: string): void {
  if (projectId) {
    void queryClient.invalidateQueries({ queryKey: ['projects', projectId, 'analytics'] })
  } else {
    void queryClient.invalidateQueries({
      predicate: (query) => query.queryKey[0] === 'projects' && query.queryKey[2] === 'analytics',
    })
  }
  void queryClient.invalidateQueries({ queryKey: ['analytics', 'usage'] })
}
