import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { qk } from '@/api/query-keys'
import type { ProjectExecutionSetupResponse } from '@/types/generated'

import {
  attachPrimaryRepository,
  getProjectExecutionSetup,
  retryProvisioning,
  selectIndependentReviewer,
  selectWorker,
  type AttachPrimaryRepositoryWire,
  type RetryProvisioningWire,
  type SelectExecutionPrincipalWire,
} from './api'

export function useProjectExecutionSetupQuery(projectId: string) {
  return useQuery({
    queryKey: qk.projectExecutionSetup(projectId),
    queryFn: () => getProjectExecutionSetup(projectId),
    enabled: Boolean(projectId),
    // Provisioning is durable and retryable. Poll only while the server says
    // it is in progress, and stop as soon as the projection reaches a terminal
    // state so an idle Project does not create background traffic.
    refetchInterval: (query) =>
      query.state.data?.execution_setup_state === 'provisioning' ? 4_000 : false,
  })
}

function invalidateProjectExecutionSetup(
  queryClient: ReturnType<typeof useQueryClient>,
  projectId: string,
) {
  void queryClient.invalidateQueries({ queryKey: qk.projectExecutionSetup(projectId) })
  void queryClient.invalidateQueries({ queryKey: qk.project(projectId) })
  void queryClient.invalidateQueries({ queryKey: qk.projectOverview(projectId) })
  void queryClient.invalidateQueries({ queryKey: qk.repos(projectId) })
}

export function useSelectWorkerMutation(projectId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (input: SelectExecutionPrincipalWire) => selectWorker(projectId, input),
    onSettled: () => invalidateProjectExecutionSetup(queryClient, projectId),
  })
}

export function useSelectIndependentReviewerMutation(projectId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (input: SelectExecutionPrincipalWire) =>
      selectIndependentReviewer(projectId, input),
    onSettled: () => invalidateProjectExecutionSetup(queryClient, projectId),
  })
}

export function useAttachPrimaryRepositoryMutation(projectId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (input: AttachPrimaryRepositoryWire) => attachPrimaryRepository(projectId, input),
    onSettled: () => invalidateProjectExecutionSetup(queryClient, projectId),
  })
}

export function useRetryProvisioningMutation(projectId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (input: RetryProvisioningWire) => retryProvisioning(projectId, input),
    onSettled: () => invalidateProjectExecutionSetup(queryClient, projectId),
  })
}

export type { ProjectExecutionSetupResponse }
