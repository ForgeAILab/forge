// Project queries, lifecycle actions, and milestone hooks.

import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  apiFetch,
  getProjectAnalytics,
  listProjectHookRuns,
  recheckProjectEnvironment,
} from '@/api/client'
import { invalidateAnalyticsQueries } from '@/api/analytics-query-invalidation'
import { qk } from '@/api/query-keys'
import type {
  CreateProjectRequest,
  PaginatedResponse,
  Project,
  ProjectOverview,
  ProjectRelease,
  ValidationResult,
  UpdateProjectRequest,
  TestLifecycleHookRequest,
  LifecycleHookTestResponse,
} from '@/types/generated'
import type { AuthorizationProvenance } from '@/types/generated/bindings/AuthorizationProvenance'
import type { MilestoneDefinitionRevision } from '@/types/generated/bindings/MilestoneDefinitionRevision'

// --- Projects ---

export function useProjectsQuery() {
  return useQuery({
    queryKey: qk.projects,
    queryFn: () => apiFetch<PaginatedResponse<Project>>('/projects'),
  })
}

export function useProjectsInfiniteQuery(limit = 20) {
  return useInfiniteQuery({
    queryKey: qk.projectPages(limit),
    queryFn: ({ pageParam }) =>
      apiFetch<PaginatedResponse<Project>>('/projects', {
        search: { cursor: pageParam as string | undefined, limit, include_total: true },
      }),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (last) => last.next_cursor ?? undefined,
  })
}

export function useProjectQuery(projectId: string) {
  return useQuery({
    queryKey: qk.project(projectId),
    queryFn: () => apiFetch<Project>(`/projects/${projectId}`),
  })
}

export function useProjectOverviewQuery(projectId: string) {
  return useQuery({
    queryKey: qk.projectOverview(projectId),
    queryFn: () => apiFetch<ProjectOverview>(`/projects/${projectId}/overview`),
    enabled: Boolean(projectId),
  })
}

export function useProjectReleaseQuery(projectId: string, releaseId: string) {
  return useQuery({
    queryKey: qk.projectRelease(projectId, releaseId),
    queryFn: () => apiFetch<ProjectRelease>(`/projects/${projectId}/releases/${releaseId}`),
    enabled: Boolean(projectId && releaseId),
  })
}

/**
 * A user-only release is bound to the exact readiness candidate the user
 * reviewed. Keep this input separate from the generated wire request because
 * JSON carries Rust i64 versions as numbers while ts-rs represents them as
 * bigint in the generated types.
 */
export type ProjectMilestoneReleaseInput = {
  projectId: string
  milestoneId: string
  expectedMilestoneVersion: number
  readinessSnapshotId: string
  readinessDigest: string
  idempotencyKey: string
  authorization: AuthorizationProvenance
}

export type ApproveMilestoneRevisionInput = {
  projectId: string
  milestoneId: string
  revisionId: string
  expectedMilestoneVersion: number
  idempotencyKey: string
  authorization: AuthorizationProvenance
}

/**
 * Approve a `draft` / `proposed` milestone definition revision as the
 * signed-in user. The compare-and-swap version is the milestone's, not the
 * revision's: the transition moves the milestone's current definition.
 */
export function useApproveMilestoneRevision() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: async (input: ApproveMilestoneRevisionInput) =>
      apiFetch<MilestoneDefinitionRevision>(
        `/projects/${input.projectId}/milestones/${input.milestoneId}/revisions/${input.revisionId}/transition`,
        {
          method: 'POST',
          body: JSON.stringify({
            mutation: {
              expected_version: input.expectedMilestoneVersion,
              expected_digest: null,
              idempotency_key: input.idempotencyKey,
              deduplication_key: null,
              authorization: input.authorization,
            },
            lifecycle: 'approved',
          }),
        },
      ),
    onSuccess: (_, input) => {
      void queryClient.invalidateQueries({ queryKey: qk.projectOverview(input.projectId) })
    },
  })
}

export function useReleaseProjectMilestone() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: async (input: ProjectMilestoneReleaseInput) =>
      apiFetch<ProjectRelease>(
        `/projects/${input.projectId}/milestones/${input.milestoneId}/release`,
        {
          method: 'POST',
          body: JSON.stringify({
            mutation: {
              expected_version: input.expectedMilestoneVersion,
              expected_digest: input.readinessDigest,
              idempotency_key: input.idempotencyKey,
              deduplication_key: null,
              authorization: input.authorization,
            },
            milestone_id: input.milestoneId,
            readiness_snapshot_id: input.readinessSnapshotId,
            readiness_digest: input.readinessDigest,
          }),
        },
      ),
    onSuccess: (release, input) => {
      void queryClient.invalidateQueries({ queryKey: qk.projectOverview(input.projectId) })
      void queryClient.invalidateQueries({ queryKey: qk.project(input.projectId) })
      invalidateAnalyticsQueries(queryClient, input.projectId)
      queryClient.setQueryData(qk.projectRelease(input.projectId, release.id), release)
    },
  })
}

export type RecordManualMilestoneCheckInput = {
  projectId: string
  milestoneId: string
  checkId: string
  definitionRevisionId: string
  charterRevisionId: string
  expectedCheckVersion: number
  status: 'pass' | 'fail'
  result: string
  inputDigest: string
  idempotencyKey: string
  authorization: AuthorizationProvenance
}

export function useRecordManualMilestoneCheck() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: async (input: RecordManualMilestoneCheckInput) => {
      return apiFetch<ValidationResult>(
        `/projects/${input.projectId}/milestones/${input.milestoneId}/checks/${input.checkId}/result`,
        {
          method: 'POST',
          body: JSON.stringify({
            mutation: {
              expected_version: input.expectedCheckVersion,
              expected_digest: null,
              idempotency_key: input.idempotencyKey,
              deduplication_key: null,
              authorization: input.authorization,
            },
            check_id: input.checkId,
            definition_revision_id: input.definitionRevisionId,
            status: input.status,
            result: input.result,
            input_digest: input.inputDigest,
            governing_revision_ids: [input.charterRevisionId],
          }),
        },
      )
    },
    onSuccess: (_, input) => {
      void queryClient.invalidateQueries({ queryKey: qk.projectOverview(input.projectId) })
    },
  })
}

export function useProjectAnalytics(projectId: string, from?: string, to?: string) {
  return useQuery({
    queryKey: qk.projectAnalytics(projectId, from, to),
    queryFn: () => getProjectAnalytics(projectId, from, to),
  })
}

export function useProjectHookRunsQuery(projectId: string, limit = 20) {
  return useInfiniteQuery({
    queryKey: qk.projectHookRunPages(projectId, limit),
    queryFn: ({ pageParam }) => listProjectHookRuns(projectId, pageParam, limit),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (last) => last.next_cursor ?? undefined,
  })
}

export function useCreateProject() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: CreateProjectRequest) =>
      apiFetch<Project>('/projects', {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    onSuccess: (project) => {
      void queryClient.invalidateQueries({ queryKey: qk.projects })
      void queryClient.invalidateQueries({ queryKey: ['agent-chats'] })
      void queryClient.invalidateQueries({ queryKey: ['product-genesis', 'active'] })
      if (project.execution_setup) {
        queryClient.setQueryData(qk.projectExecutionSetup(project.id), project.execution_setup)
      }
    },
  })
}

export function useUpdateProject() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ projectId, body }: { projectId: string; body: UpdateProjectRequest }) =>
      apiFetch<Project>(`/projects/${projectId}`, {
        method: 'PATCH',
        body: JSON.stringify(body),
      }),
    onSuccess: (project) => {
      void queryClient.invalidateQueries({ queryKey: qk.project(project.id) })
      void queryClient.invalidateQueries({ queryKey: qk.projects })
    },
  })
}

export function usePauseProject() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (projectId: string) =>
      apiFetch<Project>(`/projects/${projectId}/pause`, {
        method: 'POST',
      }),
    onSuccess: (project) => {
      void queryClient.invalidateQueries({ queryKey: qk.project(project.id) })
      void queryClient.invalidateQueries({ queryKey: qk.projects })
    },
  })
}

export function useResumeProject() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (projectId: string) =>
      apiFetch<Project>(`/projects/${projectId}/resume`, {
        method: 'POST',
      }),
    onSuccess: (project) => {
      void queryClient.invalidateQueries({ queryKey: qk.project(project.id) })
      void queryClient.invalidateQueries({ queryKey: qk.projects })
    },
  })
}

export function useRecheckProjectEnvironment() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: recheckProjectEnvironment,
    onSuccess: ({ project }) => {
      void queryClient.invalidateQueries({ queryKey: qk.project(project.id) })
      void queryClient.invalidateQueries({ queryKey: qk.projects })
    },
  })
}

export function useDeleteProject() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (projectId: string) =>
      apiFetch<void>(`/projects/${projectId}`, { method: 'DELETE' }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.projects })
    },
  })
}

export function useTestProjectLifecycleHook(projectId: string) {
  return useMutation({
    mutationFn: (body: TestLifecycleHookRequest) =>
      apiFetch<LifecycleHookTestResponse>(`/projects/${projectId}/hooks/test`, {
        method: 'POST',
        body: JSON.stringify(body),
      }),
  })
}
