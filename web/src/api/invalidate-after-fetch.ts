import type { QueryClient, QueryFilters } from '@tanstack/react-query'

const pendingRefreshes = new WeakMap<QueryClient, Set<string>>()

/** An event can commit after an in-flight read took its snapshot. Preserve
 * that read, but follow it with one authoritative read for the missed event. */
export function invalidateAfterFetch(queryClient: QueryClient, filters: QueryFilters): void {
  const fetching = queryClient
    .getQueryCache()
    .findAll(filters)
    .filter((query) => query.state.fetchStatus === 'fetching')
  const refreshed = queryClient.invalidateQueries(filters, { cancelRefetch: false })
  let pending = pendingRefreshes.get(queryClient)
  if (!pending) {
    pending = new Set()
    pendingRefreshes.set(queryClient, pending)
  }
  for (const query of fetching) {
    if (pending.has(query.queryHash)) continue
    pending.add(query.queryHash)
    void refreshed.then(() => {
      pending.delete(query.queryHash)
      if (queryClient.getQueryCache().get(query.queryHash) !== query) return
      invalidateAfterFetch(queryClient, { queryKey: query.queryKey, exact: true })
    })
  }
}
