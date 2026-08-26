import { useMutation, useQueryClient } from '@tanstack/react-query'

import { qk } from '@/api/query-keys'

import {
  approveDecisionCandidate,
  rejectDecisionCandidate,
  type ApproveDecisionCandidateWire,
  type RejectDecisionCandidateWire,
} from './decision-api'

function invalidateAfterDecisionResolution(
  queryClient: ReturnType<typeof useQueryClient>,
  projectId: string,
) {
  // A resolved Decision candidate changes the Overview's pending-decisions
  // list and Decision Log, and an approved in-envelope choice can change the
  // Project execution gate -- refresh every consumer so the product resumes
  // without a manual reload (D19/F15).
  void queryClient.invalidateQueries({ queryKey: qk.projectOverview(projectId) })
  void queryClient.invalidateQueries({ queryKey: qk.projectExecutionSetup(projectId) })
  void queryClient.invalidateQueries({ queryKey: qk.project(projectId) })
}

export function useApproveDecisionCandidateMutation(projectId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({
      approveTargetPath,
      input,
    }: {
      approveTargetPath: string
      input: ApproveDecisionCandidateWire
    }) => approveDecisionCandidate(approveTargetPath, input),
    onSettled: () => invalidateAfterDecisionResolution(queryClient, projectId),
  })
}

export function useRejectDecisionCandidateMutation(projectId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({
      rejectTargetPath,
      input,
    }: {
      rejectTargetPath: string
      input: RejectDecisionCandidateWire
    }) => rejectDecisionCandidate(rejectTargetPath, input),
    onSettled: () => invalidateAfterDecisionResolution(queryClient, projectId),
  })
}
