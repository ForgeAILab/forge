import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { qk } from '@/api/query-keys'

import {
  getProjectReconciliation,
  listProjectReconciliations,
  resolveProjectReconciliation,
  type ResolveProjectReconciliationWire,
} from './reconciliation-api'

export function useProjectReconciliationsQuery(projectId: string) {
  return useQuery({
    queryKey: qk.projectReconciliations(projectId),
    queryFn: () => listProjectReconciliations(projectId),
    enabled: Boolean(projectId),
  })
}

export function useProjectReconciliationQuery(projectId: string, reconciliationId: string | null) {
  return useQuery({
    queryKey: [...qk.projectReconciliations(projectId), reconciliationId],
    queryFn: () => getProjectReconciliation(projectId, reconciliationId as string),
    enabled: Boolean(projectId) && Boolean(reconciliationId),
  })
}

function invalidateAfterReconciliationResolution(
  queryClient: ReturnType<typeof useQueryClient>,
  projectId: string,
) {
  // A resolved reconciliation can change the Project execution gate, the
  // dispatcher's view of the affected Task, and the Overview's next action
  // -- refresh every consumer so the product resumes without a manual phase
  // toggle or page reload (8.1.9).
  void queryClient.invalidateQueries({ queryKey: qk.projectReconciliations(projectId) })
  void queryClient.invalidateQueries({ queryKey: qk.projectOverview(projectId) })
  void queryClient.invalidateQueries({ queryKey: qk.projectExecutionSetup(projectId) })
  void queryClient.invalidateQueries({ queryKey: qk.project(projectId) })
}

export function useResolveProjectReconciliationMutation(projectId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({
      reconciliationId,
      input,
    }: {
      reconciliationId: string
      input: ResolveProjectReconciliationWire
    }) => resolveProjectReconciliation(projectId, reconciliationId, input),
    onSettled: () => invalidateAfterReconciliationResolution(queryClient, projectId),
  })
}
