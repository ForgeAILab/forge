import type { QueryClient } from '@tanstack/react-query'
import { apiFetch } from '@/api/client'
import { qk } from '@/api/query-keys'
import type { PaginatedResponse, Project } from '@/types/generated'
import { useChatSelection } from './chat'
import { useLayoutStore } from './layout'

/**
 * Removes every cached query, chat-store entry, and persisted selection
 * that belongs to a Project which no longer exists — whether the user just
 * deleted it, or it was deleted elsewhere and this client is only now
 * finding out (an authorized 404, or a `project.deleted` SSE frame while a
 * deleted route is open). Both paths must converge to the same clean state
 * (F17 / 8.4.4): no stale Project-scoped chat card, no dangling
 * `selectedProjectId`, no cached Overview/board/task data a future
 * navigation could flash before its own fetch resolves.
 *
 * Every query key this app writes for a Project includes the Project's id
 * as one of its elements (`qk.project(id)`, `qk.projectTasks(id)`,
 * `qk.projectOverview(id)`, `['agent-handoffs', id]`, ...), so removing by
 * that predicate is exhaustive without hand-enumerating every key shape.
 */
export function clearDeletedProjectScope(queryClient: QueryClient, projectId: string): void {
  queryClient.removeQueries({
    predicate: (query) => query.queryKey.includes(projectId),
  })
  useChatSelection.getState().clearProjectChat(projectId)
  if (useLayoutStore.getState().selectedProjectId === projectId) {
    useLayoutStore.getState().setSelectedProjectId(undefined)
  }
}

/**
 * The Project to land on after `projectId` is gone: another authorized
 * Project if one exists, otherwise `undefined` so the caller falls back to
 * `/chat`. Reads through the cache with a forced fetch rather than trusting
 * whatever is already cached, since the list query may not have been
 * invalidated yet by the deletion that is asking for this.
 */
export async function resolveNextProjectId(
  queryClient: QueryClient,
  excludeProjectId: string,
): Promise<string | undefined> {
  const page = await queryClient.fetchQuery({
    queryKey: qk.projects,
    queryFn: () => apiFetch<PaginatedResponse<Project>>('/projects'),
  })
  return page.items.find((project) => project.id !== excludeProjectId)?.id
}
