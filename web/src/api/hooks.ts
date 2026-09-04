import { useEffect, useRef } from 'react'
import {
  type InfiniteData,
  useInfiniteQuery,
  useMutation,
  useQuery,
  useQueryClient,
} from '@tanstack/react-query'
import {
  ApiError,
  apiFetch,
  addDependency,
  addMember,
  searchUsers,
  createPat,
  deleteNotification,
  deletePat,
  getProjectAnalytics,
  getOperationsStatus,
  getSettings,
  getUnreadNotificationCount,
  deleteWorkflowTemplate,
  getWorkflowTemplate,
  listDependencies,
  listDependents,
  listMembers,
  listNotifications,
  listPats,
  listProjectHookRuns,
  listProjectAgents,
  listWorkflowTemplates,
  markAllNotificationsRead,
  markNotificationRead,
  recoverTask,
  refreshOperations,
  removeDependency,
  removeMember,
  saveWorkflowTemplate,
  updateMe,
  updateMemberRole,
  updateSettings,
} from '@/api/client'
import {
  cancelAgentInquiry,
  listAgentInquiries,
  listAgentInquiryLogs,
  type CancelAgentInquiryInput,
} from '@/api/agent-inquiries'
import { qk } from '@/api/query-keys'
import { getApiErrorCode } from '@/lib/api-error'
import { useAuthStore } from '@/stores/auth'
import type {
  Agent,
  AgentAvailability,
  AgentDiscoveredOptions,
  AssignRoleRequest,
  ClaimTaskRequest,
  Comment,
  CreateCommentRequest,
  DiffEnvelope,
  CreateAgentRequest,
  CreateProjectRequest,
  CreateRepoRequest,
  RepoSyncResponse,
  UpdateRepoRequest,
  CreateTaskRequest,
  Daemon,
  Execution,
  ExecutionUsage,
  ExecutorType,
  FollowUpRequest,
  LaunchExecutionRequest,
  LaunchExecutionResponse,
  LogEntry,
  McpConfigActionRequest,
  McpConfigResponse,
  OAuthApproveRequest,
  OAuthApproveResponse,
  OAuthAuthorizeContext,
  PaginatedResponse,
  Project,
  ProjectOverview,
  ProjectRelease,
  ValidationResult,
  Repo,
  Review,
  ReviewDecisionResponse,
  Task,
  TaskMediaResponse,
  TaskRoleAssignmentResponse,
  TransitionLogEntry,
  TransitionTaskResponse,
  TransitionTaskRequest,
  RejectReviewRequest,
  UpdateAgentRequest,
  SaveWorkflowTemplateRequest,
  RecoveryAction,
  UpdateProjectRequest,
  UpdateProjectWorkflowRequest,
  UpdateTaskRequest,
  TaskUsageSummary,
  TestLifecycleHookRequest,
  LifecycleHookTestResponse,
  SettingsResponse,
  UpdateSettingsRequest,
  WorkflowDefinition,
  Workspace,
  AgentInquiryResponse,
} from '@/types/generated'
import type { AuthorizationProvenance } from '@/types/generated/bindings/AuthorizationProvenance'
import type { MilestoneDefinitionRevision } from '@/types/generated/bindings/MilestoneDefinitionRevision'
import { recordUserInitiatedTransition } from '@/lib/notification-toast-suppression'

type TaskSearch = {
  cursor?: string
  limit?: number
  assignee_id?: string
  assignee_type?: string
  q?: string
  sort_by?: string
  sort_order?: 'asc' | 'desc'
  status?: string
  agent_id?: string
  include_archived?: boolean
  include_cancelled?: boolean
}

export type ExecutionLogsParams = {
  tail?: number
  from_sequence?: number
  limit?: number
}

export type ExecutionLogsResponse = {
  items: LogEntry[]
  has_more: boolean
  next_sequence?: number | null
}

export type CliProjectionItem = {
  daemon_id: string
  daemon_hostname: string
  daemon_status: string
  kind: string
  availability: string
  config_path: string | null
  version: string | null
  path: string | null
  agents: Array<{
    id: string
    name: string
    executor_type: string
    effective_status: string | null
  }>
}

export type CliProjectionResponse = {
  items: CliProjectionItem[]
}

export type PromptBuilderRegistryEntry = {
  id: string
  label: string
  compatible_role_hints: string[]
  description: string
}

function filterKey(input: TaskSearch): string {
  return JSON.stringify(input)
}

function searchParamsString(query: Partial<Record<string, string>>): string {
  const params = new URLSearchParams()
  for (const [key, value] of Object.entries(query)) {
    if (value !== undefined && value !== '') {
      params.set(key, value)
    }
  }
  return params.toString()
}

// --- OAuth ---

export function useOAuthAuthorizeContext(query: Partial<Record<string, string>>) {
  const queryString = searchParamsString(query)
  return useQuery({
    queryKey: ['oauth-authorize-context', JSON.stringify(query)],
    queryFn: () =>
      apiFetch<OAuthAuthorizeContext>(
        `/oauth/authorize/context${queryString ? `?${queryString}` : ''}`,
      ),
    enabled: Boolean(query.client_id && query.redirect_uri && query.response_type),
  })
}

export function useOAuthApprove() {
  return useMutation({
    mutationFn: (input: OAuthApproveRequest) =>
      apiFetch<OAuthApproveResponse>('/oauth/authorize/approve', {
        method: 'POST',
        body: JSON.stringify(input),
      }),
  })
}

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

// --- Tasks ---

export function useTasksQuery(projectId: string, search: TaskSearch) {
  return useInfiniteQuery({
    queryKey: qk.tasks(projectId, filterKey(search)),
    queryFn: ({ pageParam }) =>
      apiFetch<PaginatedResponse<Task>>(`/projects/${projectId}/tasks`, {
        search: { ...search, cursor: pageParam as string | undefined },
      }),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (last) => last.next_cursor ?? undefined,
  })
}

export function useAgentRecentTasks(agentId?: string, limit = 5) {
  const search: Omit<TaskSearch, 'cursor'> = {
    include_cancelled: true,
    limit,
    sort_by: 'updated_at',
    sort_order: 'desc',
  }
  return useQuery({
    queryKey: qk.agentTasks(agentId ?? '', filterKey(search)),
    queryFn: () =>
      apiFetch<PaginatedResponse<Task>>(`/agents/${agentId}/tasks`, {
        search,
      }),
    enabled: Boolean(agentId),
    select: (response) => response.items,
  })
}

export function useTaskQuery(taskId: string) {
  return useQuery({
    queryKey: qk.task(taskId),
    queryFn: () => apiFetch<Task>(`/tasks/${taskId}`),
  })
}

export function useCreateTask(projectId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: CreateTaskRequest) =>
      apiFetch<Task>(`/projects/${projectId}/tasks`, {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    onSuccess: (task) => {
      void queryClient.invalidateQueries({ queryKey: qk.projectTasks(projectId) })
      void queryClient.invalidateQueries({ queryKey: qk.projectOverview(projectId) })
      void queryClient.invalidateQueries({ queryKey: qk.task(task.id) })
    },
  })
}

export function useUpdateTask() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ taskId, body }: { taskId: string; body: UpdateTaskRequest }) =>
      apiFetch<Task>(`/tasks/${taskId}`, {
        method: 'PATCH',
        body: JSON.stringify(body),
      }),
    onSuccess: (task) => {
      void queryClient.invalidateQueries({ queryKey: qk.task(task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.projectTasks(task.project_id) })
    },
  })
}

type TransitionTaskMutation = {
  taskId: string
  body: TransitionTaskRequest
  currentStatus?: string
}

function transitionTask(taskId: string, body: TransitionTaskRequest) {
  return apiFetch<TransitionTaskResponse>(`/tasks/${taskId}/transition`, {
    method: 'POST',
    body: JSON.stringify(body),
  })
}

export function useTransitionTask() {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: async ({ taskId, body, currentStatus }: TransitionTaskMutation) => {
      try {
        return await transitionTask(taskId, body)
      } catch (error) {
        if (
          !(error instanceof ApiError) ||
          error.status !== 409 ||
          getApiErrorCode(error) !== 'version_conflict' ||
          !currentStatus
        ) {
          throw error
        }

        const latest = await apiFetch<Task>(`/tasks/${taskId}`)
        if (latest.status === body.status) {
          return { task: latest, review: null }
        }
        if (latest.status !== currentStatus) {
          throw error
        }
        return transitionTask(taskId, { ...body, version: latest.version })
      }
    },
    onMutate: ({ taskId }) => {
      recordUserInitiatedTransition(taskId)
    },
    onSuccess: (result) => {
      void queryClient.invalidateQueries({ queryKey: qk.task(result.task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.projectTasks(result.task.project_id) })
      void queryClient.invalidateQueries({ queryKey: qk.reviews(result.task.id) })
    },
  })
}

type GateDecisionRequest = {
  version: Task['version']
  reason?: string | null
}

type GateRejectDecisionRequest = {
  version: Task['version']
  reason: string
}

export function useApproveGate() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({
      taskId,
      stateName,
      body,
    }: {
      taskId: string
      stateName: string
      body: GateDecisionRequest
    }) =>
      apiFetch<Task>(`/tasks/${taskId}/gates/${stateName}/approve`, {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    onSuccess: (task) => {
      void queryClient.invalidateQueries({ queryKey: qk.task(task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.projectTasks(task.project_id) })
      void queryClient.invalidateQueries({ queryKey: qk.reviews(task.id) })
    },
  })
}

export function useRejectGate() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({
      taskId,
      stateName,
      body,
    }: {
      taskId: string
      stateName: string
      body: GateRejectDecisionRequest
    }) =>
      apiFetch<Task>(`/tasks/${taskId}/gates/${stateName}/reject`, {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    onSuccess: (task) => {
      void queryClient.invalidateQueries({ queryKey: qk.task(task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.projectTasks(task.project_id) })
      void queryClient.invalidateQueries({ queryKey: qk.reviews(task.id) })
    },
  })
}

export function useRecoverTask() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({
      taskId,
      action,
      reason,
      context,
    }: {
      taskId: string
      action: RecoveryAction
      reason?: string
      context?: string
    }) => recoverTask(taskId, action, reason, context),
    onSuccess: (task) => {
      void queryClient.invalidateQueries({ queryKey: qk.task(task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.taskDetail(task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.executions(task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.reviews(task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.transitions(task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.projectTasks(task.project_id) })
    },
  })
}

export function useTriggerReview() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (taskId: string) =>
      apiFetch<TransitionTaskResponse>(`/tasks/${taskId}/review`, {
        method: 'POST',
      }),
    onSuccess: (result) => {
      void queryClient.invalidateQueries({ queryKey: qk.task(result.task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.reviews(result.task.id) })
    },
  })
}

export function useReviewsQuery(taskId: string) {
  return useQuery({
    queryKey: qk.reviews(taskId),
    queryFn: () => apiFetch<Review[]>(`/tasks/${taskId}/reviews`),
    enabled: Boolean(taskId),
  })
}

export function useApproveReview() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (taskId: string) =>
      apiFetch<ReviewDecisionResponse>(`/tasks/${taskId}/review/approve`, {
        method: 'POST',
      }),
    onSuccess: (result) => {
      void queryClient.invalidateQueries({ queryKey: qk.task(result.task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.reviews(result.task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.projectTasks(result.task.project_id) })
    },
  })
}

export function useRejectReview() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ taskId, reason }: { taskId: string; reason?: string }) =>
      apiFetch<ReviewDecisionResponse>(`/tasks/${taskId}/review/reject`, {
        method: 'POST',
        body: JSON.stringify({ reason } satisfies RejectReviewRequest),
      }),
    onSuccess: (result) => {
      void queryClient.invalidateQueries({ queryKey: qk.task(result.task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.reviews(result.task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.projectTasks(result.task.project_id) })
    },
  })
}

export function useCommentsQuery(taskId: string) {
  return useQuery({
    queryKey: qk.comments(taskId),
    queryFn: async () => {
      const response = await apiFetch<PaginatedResponse<Comment>>(`/tasks/${taskId}/comments`, {
        search: { limit: 200, sort_by: 'created_at', sort_order: 'asc' },
      })
      return response.items
    },
    enabled: Boolean(taskId),
  })
}

export function useTaskMediaQuery(taskId: string) {
  return useQuery({
    queryKey: qk.taskMedia(taskId),
    queryFn: async () => {
      const response = await apiFetch<PaginatedResponse<TaskMediaResponse>>(
        `/tasks/${taskId}/media`,
        {
          search: { limit: 200, sort_by: 'created_at', sort_order: 'asc' },
        },
      )
      return response.items
    },
    enabled: Boolean(taskId),
  })
}

// --- Notifications ---

export function useNotificationsQuery(projectId?: string, read?: boolean) {
  return useQuery({
    queryKey: qk.notifications(projectId, read),
    queryFn: () => listNotifications({ project_id: projectId, read, limit: 20 }),
  })
}

export function useUnreadNotificationsCountQuery(projectId?: string) {
  return useQuery({
    queryKey: qk.notificationUnreadCount(projectId),
    queryFn: () => getUnreadNotificationCount(projectId),
  })
}

export function useMarkNotificationRead() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (notificationId: string) => markNotificationRead(notificationId),
    onSuccess: (notification) => {
      void queryClient.invalidateQueries({
        predicate: (query) =>
          String(query.queryKey[0]) === 'notifications' &&
          String(query.queryKey[1] ?? '') === notification.project_id,
      })
      void queryClient.invalidateQueries({
        queryKey: qk.notificationUnreadCount(notification.project_id),
      })
      void queryClient.invalidateQueries({ queryKey: qk.notificationUnreadCount() })
    },
  })
}

export function useMarkAllNotificationsRead() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (projectId?: string) => markAllNotificationsRead(projectId),
    onSuccess: (_, projectId) => {
      void queryClient.invalidateQueries({
        predicate: (query) =>
          String(query.queryKey[0]) === 'notifications' &&
          String(query.queryKey[1] ?? '') === (projectId ?? 'all'),
      })
      void queryClient.invalidateQueries({ queryKey: qk.notificationUnreadCount(projectId) })
      void queryClient.invalidateQueries({ queryKey: qk.notificationUnreadCount() })
    },
  })
}

export function useDeleteNotification() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ notificationId }: { notificationId: string }) =>
      deleteNotification(notificationId),
    onSuccess: () => {
      void queryClient.invalidateQueries({
        predicate: (query) => String(query.queryKey[0]) === 'notifications',
      })
    },
  })
}

export function useCreateComment() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ taskId, body }: { taskId: string; body: CreateCommentRequest }) =>
      apiFetch<Comment>(`/tasks/${taskId}/comments`, {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    onSuccess: (comment) => {
      void queryClient.invalidateQueries({ queryKey: qk.comments(comment.task_id) })
    },
  })
}

export function useUploadTaskMedia() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({
      taskId,
      file,
      authorName,
    }: {
      taskId: string
      file: File
      authorName?: string
    }) => {
      const formData = new FormData()
      formData.append('file', file)
      if (authorName) formData.append('author_name', authorName)
      return apiFetch<TaskMediaResponse>(`/tasks/${taskId}/media`, {
        method: 'POST',
        body: formData,
      })
    },
    onSuccess: (media) => {
      void queryClient.invalidateQueries({ queryKey: qk.taskMedia(media.task_id) })
    },
  })
}

export function useDeleteComment() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ commentId }: { taskId: string; commentId: string }) =>
      apiFetch<void>(`/comments/${commentId}`, { method: 'DELETE' }),
    onSuccess: (_, variables) => {
      void queryClient.invalidateQueries({ queryKey: qk.comments(variables.taskId) })
    },
  })
}

export function useCancelTask() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (taskId: string) =>
      apiFetch<Task>(`/tasks/${taskId}/cancel`, {
        method: 'POST',
      }),
    onSuccess: (task) => {
      void queryClient.invalidateQueries({ queryKey: qk.task(task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.projectTasks(task.project_id) })
    },
  })
}

export function useArchiveTask() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (taskId: string) =>
      apiFetch<Task>(`/tasks/${taskId}/archive`, {
        method: 'POST',
      }),
    onSuccess: (task) => {
      void queryClient.invalidateQueries({ queryKey: qk.task(task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.projectTasks(task.project_id) })
    },
  })
}

export function useAdvanceTask() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (taskId: string) =>
      apiFetch<Task>(`/tasks/${taskId}/advance`, {
        method: 'POST',
      }),
    onMutate: (taskId) => {
      recordUserInitiatedTransition(taskId)
    },
    onSuccess: (task) => {
      void queryClient.invalidateQueries({ queryKey: qk.task(task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.projectTasks(task.project_id) })
      void queryClient.invalidateQueries({ queryKey: qk.executions(task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.reviews(task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.transitions(task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.agents })
    },
  })
}

export function useDuplicateTask() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (taskId: string) =>
      apiFetch<Task>(`/tasks/${taskId}/duplicate`, {
        method: 'POST',
      }),
    onSuccess: (task) => {
      void queryClient.invalidateQueries({ queryKey: qk.projectTasks(task.project_id) })
    },
  })
}

export function useTaskWorkspace(taskId: string) {
  return useQuery({
    queryKey: qk.taskWorkspace(taskId),
    queryFn: () => apiFetch<Workspace>(`/tasks/${taskId}/workspace`),
    enabled: Boolean(taskId),
    retry: false,
  })
}

export function useResetTaskWorkspace() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (taskId: string) =>
      apiFetch<unknown>(`/tasks/${taskId}/workspace/reset`, {
        method: 'POST',
      }),
    onSuccess: (_, taskId) => {
      void queryClient.invalidateQueries({ queryKey: qk.task(taskId) })
    },
  })
}

export function useClaimTask() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ taskId, agentId }: { taskId: string; agentId: string }) =>
      apiFetch<Task>(`/tasks/${taskId}/claim`, {
        method: 'POST',
        body: JSON.stringify({ agent_id: agentId } satisfies ClaimTaskRequest),
      }),
    onSuccess: (task) => {
      void queryClient.invalidateQueries({ queryKey: qk.task(task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.projectTasks(task.project_id) })
      void queryClient.invalidateQueries({ queryKey: qk.agents })
    },
  })
}

export function useLaunchExecution() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ taskId, body }: { taskId: string; body: LaunchExecutionRequest }) =>
      apiFetch<LaunchExecutionResponse>(`/tasks/${taskId}/launch`, {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    onSuccess: (response) => {
      const task = response.data.task
      void queryClient.invalidateQueries({ queryKey: qk.task(task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.projectTasks(task.project_id) })
      void queryClient.invalidateQueries({ queryKey: qk.executions(task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.taskDiff(task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.agents })
    },
  })
}

export function useFollowUpExecution(executionId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: FollowUpRequest) =>
      apiFetch<LaunchExecutionResponse>(`/executions/${executionId}/follow-up`, {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    onSuccess: (response) => {
      const task = response.data.task
      const nextExecution = response.data.execution
      void queryClient.invalidateQueries({ queryKey: qk.task(task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.projectTasks(task.project_id) })
      void queryClient.invalidateQueries({ queryKey: qk.executions(task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.execution(executionId) })
      void queryClient.invalidateQueries({ queryKey: qk.execution(nextExecution.id) })
      void queryClient.invalidateQueries({ queryKey: qk.executionLogs(nextExecution.id) })
      void queryClient.invalidateQueries({ queryKey: qk.taskDiff(task.id) })
      void queryClient.invalidateQueries({ queryKey: qk.agents })
    },
  })
}

export function useTaskDiffQuery(taskId: string) {
  return useQuery({
    queryKey: qk.taskDiff(taskId),
    queryFn: () => apiFetch<DiffEnvelope>(`/tasks/${taskId}/diff`),
    enabled: Boolean(taskId),
  })
}

// --- Agents ---

const AGENTS_CACHE_STALE_TIME = 10 * 60 * 1000
const AGENTS_CACHE_GC_TIME = 60 * 60 * 1000

type AgentPagesData = InfiniteData<PaginatedResponse<Agent>, string | undefined>

function isPaginatedAgents(data: unknown): data is PaginatedResponse<Agent> {
  return (
    typeof data === 'object' && data !== null && Array.isArray((data as { items?: unknown }).items)
  )
}

function isAgentPagesData(data: unknown): data is AgentPagesData {
  return (
    typeof data === 'object' && data !== null && Array.isArray((data as { pages?: unknown }).pages)
  )
}

function findCachedAgent(
  queryClient: ReturnType<typeof useQueryClient>,
  agentId: string,
): Agent | undefined {
  const cachedAgent = queryClient.getQueryData<Agent>(qk.agent(agentId))
  if (cachedAgent) return cachedAgent

  for (const [, data] of queryClient.getQueriesData<unknown>({ queryKey: qk.agents })) {
    if (isPaginatedAgents(data)) {
      const agent = data.items.find((item) => item.id === agentId)
      if (agent) return agent
    }

    if (isAgentPagesData(data)) {
      for (const page of data.pages) {
        const agent = page.items.find((item) => item.id === agentId)
        if (agent) return agent
      }
    }
  }

  return undefined
}

export function useAgentsQuery() {
  return useQuery({
    queryKey: qk.agents,
    queryFn: () => apiFetch<PaginatedResponse<Agent>>('/agents', { search: { limit: 100 } }),
    staleTime: AGENTS_CACHE_STALE_TIME,
    gcTime: AGENTS_CACHE_GC_TIME,
  })
}

export function useAgentsInfiniteQuery(limit = 20) {
  return useInfiniteQuery({
    queryKey: qk.agentPages(limit),
    queryFn: ({ pageParam }) =>
      apiFetch<PaginatedResponse<Agent>>('/agents', {
        search: { cursor: pageParam as string | undefined, limit, include_total: true },
      }),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (last) => last.next_cursor ?? undefined,
  })
}

export function useAgentQuery(agentId?: string) {
  const queryClient = useQueryClient()
  return useQuery({
    queryKey: qk.agent(agentId ?? ''),
    queryFn: () => apiFetch<Agent>(`/agents/${agentId}`),
    enabled: Boolean(agentId),
    placeholderData: () => (agentId ? findCachedAgent(queryClient, agentId) : undefined),
    gcTime: AGENTS_CACHE_GC_TIME,
  })
}

export function useCreateAgent() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: CreateAgentRequest) =>
      apiFetch<Agent>('/agents', {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.agents })
    },
  })
}

export function useUpdateAgent() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ agentId, body }: { agentId: string; body: UpdateAgentRequest }) =>
      apiFetch<Agent>(`/agents/${agentId}`, {
        method: 'PATCH',
        body: JSON.stringify(body),
      }),
    onSuccess: (agent) => {
      void queryClient.invalidateQueries({ queryKey: qk.agent(agent.id) })
      void queryClient.invalidateQueries({ queryKey: qk.agents })
    },
  })
}

export function usePauseAgent() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (agentId: string) =>
      apiFetch<Agent>(`/agents/${agentId}/pause`, {
        method: 'POST',
      }),
    onSuccess: (agent) => {
      void queryClient.invalidateQueries({ queryKey: qk.agent(agent.id) })
      void queryClient.invalidateQueries({ queryKey: qk.agents })
    },
  })
}

export function useResumeAgent() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (agentId: string) =>
      apiFetch<Agent>(`/agents/${agentId}/resume`, {
        method: 'POST',
      }),
    onSuccess: (agent) => {
      void queryClient.invalidateQueries({ queryKey: qk.agent(agent.id) })
      void queryClient.invalidateQueries({ queryKey: qk.agents })
    },
  })
}

export function useDeleteAgent() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (agentId: string) =>
      apiFetch<void>(`/agents/${agentId}`, {
        method: 'DELETE',
      }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.agents })
    },
  })
}

export function useDuplicateAgent() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ agentId, name }: { agentId: string; name: string }) =>
      apiFetch<Agent>(`/agents/${agentId}/duplicate`, {
        method: 'POST',
        body: JSON.stringify({ name }),
      }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.agents })
    },
  })
}

export function useAgentDiscoveredOptions(agentId: string | null | undefined) {
  return useQuery({
    queryKey: qk.agentDiscoveredOptions(agentId ?? ''),
    queryFn: () => apiFetch<AgentDiscoveredOptions>(`/agents/${agentId}/discovered-options`),
    enabled: Boolean(agentId),
    staleTime: 5 * 60 * 1000,
  })
}

export function useAgentAvailability(agentId: string | null | undefined) {
  return useQuery({
    queryKey: qk.agentAvailability(agentId ?? ''),
    queryFn: () => apiFetch<AgentAvailability>(`/agents/${agentId}/availability`),
    enabled: Boolean(agentId),
    staleTime: 30 * 1000,
  })
}

export function useExecutorTypesQuery() {
  return useQuery({
    queryKey: qk.executorTypes,
    queryFn: () => apiFetch<ExecutorType[]>('/executor-types'),
  })
}

export function useDaemonsQuery(enabled = true) {
  return useQuery({
    queryKey: qk.daemons,
    queryFn: () => apiFetch<PaginatedResponse<Daemon>>('/daemons'),
    enabled,
  })
}

export function useOperationsStatusQuery() {
  return useQuery({
    queryKey: qk.operationsStatus,
    queryFn: getOperationsStatus,
    refetchInterval: 30_000,
  })
}

export function useRefreshOperationsMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: refreshOperations,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.operationsStatus })
    },
  })
}

export function useCliProjectionQuery() {
  return useQuery({
    queryKey: qk.clis,
    queryFn: () => apiFetch<CliProjectionResponse>('/clis'),
  })
}

function browserPublicBaseUrl(): string | undefined {
  if (typeof window === 'undefined') return undefined
  return window.location.origin
}

export function useMcpConfigQuery(agent: string, scope: string, projectId?: string) {
  return useQuery({
    queryKey: qk.mcpConfig(agent, scope, projectId),
    queryFn: () =>
      apiFetch<McpConfigResponse>('/config/mcp', {
        search: {
          agent,
          scope,
          public_base_url: browserPublicBaseUrl(),
          ...(projectId ? { project_id: projectId } : {}),
        },
      }),
  })
}

export function useUpdateMcpConfig() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: McpConfigActionRequest) =>
      apiFetch<McpConfigResponse>('/config/mcp', {
        method: 'POST',
        body: JSON.stringify({
          ...body,
          public_base_url: body.public_base_url ?? browserPublicBaseUrl(),
        }),
      }),
    onSuccess: (_data, variables) => {
      void queryClient.invalidateQueries({
        queryKey: qk.mcpConfig(variables.agent, variables.scope ?? 'project', variables.project_id),
      })
    },
  })
}

// --- Repos ---

export function useReposQuery(projectId: string, options?: { enabled?: boolean }) {
  return useQuery({
    queryKey: qk.repos(projectId),
    queryFn: () => apiFetch<PaginatedResponse<Repo>>(`/projects/${projectId}/repos`),
    enabled: options?.enabled ?? Boolean(projectId),
  })
}

export function useCreateRepo(projectId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: CreateRepoRequest) =>
      apiFetch<Repo>(`/projects/${projectId}/repos`, {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.repos(projectId) })
      void queryClient.invalidateQueries({ queryKey: qk.project(projectId) })
      void queryClient.invalidateQueries({ queryKey: qk.projects })
    },
  })
}

export function useUpdateRepo(repoId: string, projectId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: UpdateRepoRequest) =>
      apiFetch<Repo>(`/repos/${repoId}`, {
        method: 'PATCH',
        body: JSON.stringify(body),
      }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.repos(projectId) })
      void queryClient.invalidateQueries({ queryKey: qk.project(projectId) })
    },
  })
}

export function useSyncRepo(repoId: string, projectId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: () =>
      apiFetch<RepoSyncResponse>(`/repos/${repoId}/sync`, {
        method: 'POST',
      }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.repos(projectId) })
    },
  })
}

// --- Executions ---

export function useExecutionsQuery(taskId: string) {
  return useQuery({
    queryKey: qk.executions(taskId),
    queryFn: () => apiFetch<PaginatedResponse<Execution>>(`/tasks/${taskId}/executions`),
  })
}

export function useExecutionQuery(executionId: string) {
  return useQuery({
    queryKey: qk.execution(executionId),
    queryFn: () => apiFetch<Execution>(`/executions/${executionId}`),
    enabled: Boolean(executionId),
  })
}

export function useExecutionUsageQuery(executionId: string) {
  return useQuery({
    queryKey: qk.executionUsage(executionId),
    queryFn: () => apiFetch<ExecutionUsage[]>(`/executions/${executionId}/usage`),
  })
}

export function useTaskUsageQuery(taskId: string) {
  return useQuery({
    queryKey: qk.taskUsage(taskId),
    queryFn: () => apiFetch<TaskUsageSummary>(`/tasks/${taskId}/usage`),
  })
}

/**
 * Execution logs are returned as parsed JSONL entries from the backend.
 */
// --- Workflow ---

export function useWorkflowQuery(projectId: string) {
  return useQuery({
    queryKey: qk.workflow(projectId),
    queryFn: () => apiFetch<WorkflowDefinition>(`/projects/${projectId}/workflow`),
    enabled: Boolean(projectId),
  })
}

export function useUpdateWorkflow() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ projectId, body }: { projectId: string; body: UpdateProjectWorkflowRequest }) =>
      apiFetch<WorkflowDefinition>(`/projects/${projectId}/workflow`, {
        method: 'PUT',
        body: JSON.stringify(body),
      }),
    onSuccess: (_, vars) => {
      void queryClient.invalidateQueries({ queryKey: qk.workflow(vars.projectId) })
      void queryClient.invalidateQueries({ queryKey: qk.project(vars.projectId) })
    },
  })
}

export function useWorkflowPromptBuildersQuery() {
  return useQuery({
    queryKey: qk.workflowPromptBuilders,
    queryFn: () => apiFetch<PromptBuilderRegistryEntry[]>('/workflow/prompt-builders'),
  })
}

// --- Workflow Templates ---

export function useWorkflowTemplatesQuery() {
  return useQuery({
    queryKey: qk.workflowTemplates,
    queryFn: () => listWorkflowTemplates(),
  })
}

export function useWorkflowTemplateQuery(name: string) {
  return useQuery({
    queryKey: qk.workflowTemplate(name),
    queryFn: () => getWorkflowTemplate(name),
    enabled: Boolean(name),
  })
}

export function useSaveWorkflowTemplate() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ name, body }: { name: string; body: SaveWorkflowTemplateRequest }) =>
      saveWorkflowTemplate(name, body),
    onSuccess: (saved) => {
      void queryClient.invalidateQueries({ queryKey: qk.workflowTemplates })
      void queryClient.invalidateQueries({ queryKey: qk.workflowTemplate(saved.name) })
    },
  })
}

export function useDeleteWorkflowTemplate() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (name: string) => deleteWorkflowTemplate(name),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.workflowTemplates })
    },
  })
}

// --- Transition log ---

export function useTransitionLogQuery(taskId: string, enabled = true) {
  return useQuery({
    queryKey: qk.transitions(taskId),
    queryFn: async () => {
      const response = await apiFetch<{ items: TransitionLogEntry[] }>(
        `/tasks/${taskId}/transitions`,
      )
      return response.items
    },
    enabled: Boolean(taskId) && enabled,
  })
}

// --- Role assignments ---

export function useTaskRolesQuery(taskId: string, enabled = true) {
  return useQuery({
    queryKey: qk.taskRoles(taskId),
    queryFn: () => apiFetch<TaskRoleAssignmentResponse[]>(`/tasks/${taskId}/roles`),
    enabled: Boolean(taskId) && enabled,
  })
}

export function useAssignRole() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({
      taskId,
      roleName,
      body,
    }: {
      taskId: string
      roleName: string
      body: AssignRoleRequest
    }) =>
      apiFetch<TaskRoleAssignmentResponse>(`/tasks/${taskId}/roles/${roleName}`, {
        method: 'PUT',
        body: JSON.stringify(body),
      }),
    onSuccess: (assignment) => {
      void queryClient.invalidateQueries({ queryKey: qk.taskRoles(assignment.task_id) })
      void queryClient.invalidateQueries({ queryKey: qk.task(assignment.task_id) })
    },
  })
}

export type RemoveRoleRequestBody = {
  reset_workspace?: boolean
  reset_worktree?: boolean
}

export function useRemoveRole() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({
      taskId,
      roleName,
      body,
    }: {
      taskId: string
      roleName: string
      body?: RemoveRoleRequestBody
    }) =>
      apiFetch<void>(`/tasks/${taskId}/roles/${roleName}`, {
        method: 'DELETE',
        body: JSON.stringify(body ?? {}),
      }),
    onSuccess: (_, vars) => {
      void queryClient.invalidateQueries({ queryKey: qk.taskRoles(vars.taskId) })
      void queryClient.invalidateQueries({ queryKey: qk.task(vars.taskId) })
    },
  })
}

export async function getExecutionLogs(
  executionId: string,
  params?: ExecutionLogsParams,
): Promise<ExecutionLogsResponse> {
  const data = await apiFetch<ExecutionLogsResponse>(`/executions/${executionId}/logs`, {
    search: {
      tail: params?.tail,
      from_sequence: params?.from_sequence,
      limit: params?.limit,
    },
  })
  const response: ExecutionLogsResponse = {
    items: data.items ?? [],
    has_more: data.has_more ?? false,
  }
  if (data.next_sequence !== undefined) response.next_sequence = data.next_sequence
  return response
}

export function useExecutionLogs(
  executionId: string,
  params?: ExecutionLogsParams,
  isRunning?: boolean,
) {
  return useQuery({
    queryKey: [...qk.executionLogs(executionId), params ?? {}] as const,
    enabled: Boolean(executionId),
    refetchInterval: isRunning ? 3000 : false,
    queryFn: () => getExecutionLogs(executionId, params),
  })
}

export interface HookLogEntry {
  event: string
  hook_type: string
  command?: string
  plugin_name?: string
  duration_ms: number
  status: string
  exit_code?: number | null
  timeout?: boolean
  working_dir?: string
  stdout?: string
  stderr?: string
  error?: string
  reason?: string
}

export function getExecutionHookLogs(executionId: string): Promise<HookLogEntry[]> {
  return apiFetch<HookLogEntry[]>(`/executions/${executionId}/hook-logs`)
}

export function useExecutionHookLogs(executionId: string | undefined) {
  return useQuery({
    queryKey: qk.executionHookLogs(executionId ?? ''),
    enabled: !!executionId,
    queryFn: () => getExecutionHookLogs(executionId ?? ''),
  })
}

export function useSettingsQuery() {
  return useQuery({
    queryKey: qk.settings,
    queryFn: () => getSettings(),
  })
}

export function useUpdateSettings() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: UpdateSettingsRequest) => updateSettings(body),
    onSuccess: (data: SettingsResponse) => {
      queryClient.setQueryData(qk.settings, data)
    },
  })
}

// --- Integrations ---

export type IntegrationResponse = {
  id: string
  project_id: string
  platform: string
  base_url: string
  owner: string
  repo: string
  token_secret_ref: string
  poll_interval_secs: number
  sync_filter: Record<string, unknown>
  default_task_state: string | null
  default_assignee_type: string | null
  default_assignee_id: string | null
  enabled: boolean
  last_polled_at: string | null
  created_at: string
  updated_at: string
}

export type CreateIntegrationRequest = {
  platform: string
  base_url: string
  owner: string
  repo: string
  token_secret_ref: string
  poll_interval_secs?: number
  sync_filter?: Record<string, unknown>
  default_task_state?: string
  default_assignee_type?: string
  default_assignee_id?: string
  enabled?: boolean
}

export type PatchIntegrationRequest = {
  platform?: string
  base_url?: string
  owner?: string
  repo?: string
  token_secret_ref?: string
  poll_interval_secs?: number
  sync_filter?: Record<string, unknown>
  default_task_state?: string
  default_assignee_type?: string
  default_assignee_id?: string
  enabled?: boolean
}

export type SyncTriggerResponse = {
  imported: number
  skipped: number
  errors: number
}

export function useIntegrationQuery(projectId: string) {
  return useQuery({
    queryKey: qk.integration(projectId),
    queryFn: () => apiFetch<IntegrationResponse | null>(`/projects/${projectId}/integration`),
    retry: (failureCount, error) => {
      if (error instanceof ApiError && error.status === 404) return false
      return failureCount < 3
    },
  })
}

export function useCreateIntegration() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ projectId, body }: { projectId: string; body: CreateIntegrationRequest }) =>
      apiFetch<IntegrationResponse>(`/projects/${projectId}/integration`, {
        method: 'POST',
        body: JSON.stringify(body),
      }),
    onSuccess: (_, vars) => {
      void queryClient.invalidateQueries({ queryKey: qk.integration(vars.projectId) })
    },
  })
}

export function useUpdateIntegration() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ projectId, body }: { projectId: string; body: PatchIntegrationRequest }) =>
      apiFetch<IntegrationResponse>(`/projects/${projectId}/integration`, {
        method: 'PATCH',
        body: JSON.stringify(body),
      }),
    onSuccess: (_, vars) => {
      void queryClient.invalidateQueries({ queryKey: qk.integration(vars.projectId) })
    },
  })
}

export function useDeleteIntegration() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (projectId: string) =>
      apiFetch<void>(`/projects/${projectId}/integration`, { method: 'DELETE' }),
    onSuccess: (_, projectId) => {
      void queryClient.invalidateQueries({ queryKey: qk.integration(projectId) })
    },
  })
}

export function useTriggerSync() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (projectId: string) =>
      apiFetch<SyncTriggerResponse>(`/projects/${projectId}/integration/sync`, {
        method: 'POST',
      }),
    onSuccess: (_, projectId) => {
      void queryClient.invalidateQueries({ queryKey: qk.integration(projectId) })
    },
  })
}

// --- External Links ---

export type ExternalLinkResponse = {
  id: string
  task_id: string
  integration_id: string
  platform: string
  remote_owner: string
  remote_repo: string
  remote_issue_number: number
  remote_url: string
  global_id: string
  synced_at: string
  created_at: string
  updated_at: string
}

export function useExternalLinksQuery(taskId: string) {
  return useQuery({
    queryKey: qk.taskExternalLinks(taskId),
    queryFn: () => apiFetch<ExternalLinkResponse[]>(`/tasks/${taskId}/external-links`),
    enabled: Boolean(taskId),
  })
}

export function useCreateExternalLink() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ taskId, remoteIssueNumber }: { taskId: string; remoteIssueNumber: number }) =>
      apiFetch<ExternalLinkResponse>(`/tasks/${taskId}/external-links`, {
        method: 'POST',
        body: JSON.stringify({ remote_issue_number: remoteIssueNumber }),
      }),
    onSuccess: (link) => {
      void queryClient.invalidateQueries({ queryKey: qk.taskExternalLinks(link.task_id) })
      void queryClient.invalidateQueries({ queryKey: qk.task(link.task_id) })
    },
  })
}

export function useDeleteExternalLink() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ taskId, linkId }: { taskId: string; linkId: string }) =>
      apiFetch<void>(`/tasks/${taskId}/external-links/${linkId}`, {
        method: 'DELETE',
      }),
    onSuccess: (_, vars) => {
      void queryClient.invalidateQueries({ queryKey: qk.taskExternalLinks(vars.taskId) })
      void queryClient.invalidateQueries({ queryKey: qk.task(vars.taskId) })
    },
  })
}

export function useTaskDependenciesQuery(taskId: string) {
  return useQuery({
    queryKey: qk.taskDependencies(taskId),
    queryFn: () => listDependencies(taskId),
    enabled: Boolean(taskId),
  })
}

export function useTaskDependentsQuery(taskId: string) {
  return useQuery({
    queryKey: qk.taskDependents(taskId),
    queryFn: () => listDependents(taskId),
    enabled: Boolean(taskId),
  })
}

export function useAddDependency(taskId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (dependsOnId: string) => addDependency(taskId, dependsOnId),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.taskDependencies(taskId) })
    },
  })
}

export function useRemoveDependency(taskId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (dependsOnId: string) => removeDependency(taskId, dependsOnId),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.taskDependencies(taskId) })
    },
  })
}

// --- User Profile ---

export function useUpdateMe() {
  return useMutation({
    mutationFn: (body: { email?: string | null; display_name?: string | null }) => updateMe(body),
    onSuccess: (user) => useAuthStore.getState().updateUser(user),
  })
}

// --- Personal Access Tokens ---

export function usePatsQuery() {
  return useQuery({
    queryKey: qk.pats(),
    queryFn: () => listPats(),
  })
}

export function useCreatePat() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: { name: string; expires_at?: string | null }) => createPat(body),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.pats() })
    },
  })
}

export function useDeletePat() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (id: string) => deletePat(id),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.pats() })
    },
  })
}

// --- User Search ---

export function useUserSearch(query: string) {
  return useQuery({
    queryKey: ['users', 'search', query],
    queryFn: () => searchUsers(query),
    enabled: query.trim().length >= 2,
    staleTime: 30_000,
  })
}

// --- Project Members ---

export function useMembersQuery(projectId: string) {
  return useQuery({
    queryKey: qk.projectMembers(projectId),
    queryFn: () => listMembers(projectId),
    enabled: Boolean(projectId),
  })
}

export function useProjectAgentsQuery(projectId: string) {
  return useQuery({
    queryKey: qk.projectAgents(projectId),
    queryFn: () => listProjectAgents(projectId),
    enabled: Boolean(projectId),
  })
}

export function useAddMember(projectId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: { user_id: string; role: string }) => addMember(projectId, body),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.projectMembers(projectId) })
    },
  })
}

export function useUpdateMemberRole(projectId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ userId, body }: { userId: string; body: { role: string } }) =>
      updateMemberRole(projectId, userId, body),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.projectMembers(projectId) })
    },
  })
}

export function useRemoveMember(projectId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (userId: string) => removeMember(projectId, userId),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.projectMembers(projectId) })
    },
  })
}

// --- Agent Inquiries ---
// Ephemeral, read-only Main Agent sub-agent runs (`inquiry.run`). A run log,
// not a work item: the only mutation is cancel, and only while running.

const AGENT_INQUIRY_LIST_LIMIT = 50
const AGENT_INQUIRY_POLL_INTERVAL = 3_000
/** A running inquiry's activity (tool calls, reasoning, reply deltas) is followed at this cadence. */
const AGENT_INQUIRY_ACTIVITY_POLL_INTERVAL = 1_000
const AGENT_INQUIRY_LOG_PAGE_LIMIT = 1_000
const AGENT_INQUIRY_LOG_MAX_PAGES_PER_FETCH = 20

/**
 * Extend an already-loaded activity log with everything the server has
 * recorded since its last entry. An inquiry appends to one log with
 * increasing sequences, so a poll only ever transfers the new tail — same
 * idiom as `fetchAgentChatTurnLogsAfter`.
 */
export async function fetchAgentInquiryLogsAfter(
  inquiryId: string,
  previous: LogEntry[],
): Promise<LogEntry[]> {
  const items = [...previous]
  let from = items.length > 0 ? items[items.length - 1].sequence + 1 : 0
  for (let page = 0; page < AGENT_INQUIRY_LOG_MAX_PAGES_PER_FETCH; page += 1) {
    const response = await listAgentInquiryLogs(inquiryId, {
      from_sequence: from,
      limit: AGENT_INQUIRY_LOG_PAGE_LIMIT,
    })
    items.push(...response.items)
    if (!response.has_more || response.next_sequence === null) break
    from = response.next_sequence
  }
  return items
}

/**
 * The durable activity log of one inquiry: reasoning, tool calls with their
 * bounded results, and reply deltas. While `live`, it follows the inquiry at
 * the activity cadence; afterwards it is fetched once per mount so a reader
 * who watched it run sees its final tail without re-downloading the rest.
 */
export function useAgentInquiryLogsQuery(
  inquiryId: string | undefined,
  options: { live: boolean; enabled?: boolean },
) {
  const queryClient = useQueryClient()
  const queryKey = qk.agentInquiryLogs(inquiryId ?? 'none')
  const previous = useRef({ inquiryId, live: options.live })
  useEffect(() => {
    const wasRunning = previous.current.inquiryId === inquiryId && previous.current.live
    previous.current = { inquiryId, live: options.live }
    if (!inquiryId || !wasRunning || options.live) return

    const filter = { queryKey: qk.agentInquiryLogs(inquiryId) }
    // A poll already in flight may have read before completion. Cancel it
    // before fetching the terminal tail, including the initial empty read.
    // Closed rows stay invalidated and drain when opened again.
    void queryClient.cancelQueries(filter).then(() => queryClient.invalidateQueries(filter))
  }, [inquiryId, options.live, queryClient])

  return useQuery({
    queryKey,
    enabled: Boolean(inquiryId) && (options.enabled ?? true),
    refetchInterval: options.live ? AGENT_INQUIRY_ACTIVITY_POLL_INTERVAL : false,
    refetchOnMount: options.live ? true : 'always',
    staleTime: options.live ? 0 : Number.POSITIVE_INFINITY,
    queryFn: () =>
      fetchAgentInquiryLogsAfter(inquiryId!, queryClient.getQueryData<LogEntry[]>(queryKey) ?? []),
  })
}

export function useAgentInquiriesQuery(chatId: string | undefined, limit = AGENT_INQUIRY_LIST_LIMIT) {
  return useInfiniteQuery({
    queryKey: qk.agentInquiryPages(chatId ?? 'none', limit),
    queryFn: ({ pageParam }) => listAgentInquiries(chatId!, { cursor: pageParam, limit }),
    initialPageParam: undefined as string | undefined,
    getNextPageParam: (last) => last.next_cursor ?? undefined,
    enabled: Boolean(chatId),
    // Creation has no event notification. Keep discovering runs while this
    // list is mounted, including from an empty or all-terminal cache.
    refetchInterval: AGENT_INQUIRY_POLL_INTERVAL,
  })
}

export function useCancelAgentInquiryMutation(chatId: string | undefined) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, input }: { id: string; input: CancelAgentInquiryInput }) =>
      cancelAgentInquiry(id, input),
    onSuccess: (cancelled) => {
      if (!chatId) return
      // Settle the cancelled inquiry into every cached page immediately so
      // the Cancel action disappears without waiting on the next poll tick.
      queryClient.setQueriesData<InfiniteData<{ items: AgentInquiryResponse[] }> | undefined>(
        { queryKey: qk.agentInquiries(chatId) },
        (data) =>
          data && {
            ...data,
            pages: data.pages.map((page) => ({
              ...page,
              items: page.items.map((item) => (item.id === cancelled.id ? cancelled : item)),
            })),
          },
      )
      void queryClient.invalidateQueries({ queryKey: qk.agentInquiries(chatId) })
    },
  })
}
