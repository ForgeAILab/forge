// Task reviews, comments, media, execution actions, roles, links, and dependencies.

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  apiFetch,
  addDependency,
  listDependencies,
  listDependents,
  removeDependency,
} from '@/api/client'
import { qk } from '@/api/query-keys'
import type {
  AssignRoleRequest,
  ClaimTaskRequest,
  Comment,
  CreateCommentRequest,
  DiffEnvelope,
  FollowUpRequest,
  LaunchExecutionRequest,
  LaunchExecutionResponse,
  PaginatedResponse,
  Review,
  Task,
  TaskMediaResponse,
  TaskRoleAssignmentResponse,
  TransitionLogEntry,
  Workspace,
} from '@/types/generated'

export function useReviewsQuery(taskId: string, options: { enabled?: boolean } = {}) {
  return useQuery({
    queryKey: qk.reviews(taskId),
    queryFn: ({ signal }) => apiFetch<Review[]>(`/tasks/${taskId}/reviews`, { signal }),
    enabled: Boolean(taskId) && (options.enabled ?? true),
  })
}

export function useCommentsQuery(taskId: string, options: { enabled?: boolean } = {}) {
  return useQuery({
    queryKey: qk.comments(taskId),
    queryFn: async ({ signal }) => {
      const response = await apiFetch<PaginatedResponse<Comment>>(`/tasks/${taskId}/comments`, {
        search: { limit: 200, sort_by: 'created_at', sort_order: 'asc' },
        signal,
      })
      return response.items
    },
    enabled: Boolean(taskId) && (options.enabled ?? true),
  })
}

export function useTaskMediaQuery(taskId: string) {
  return useQuery({
    queryKey: qk.taskMedia(taskId),
    queryFn: async ({ signal }) => {
      const response = await apiFetch<PaginatedResponse<TaskMediaResponse>>(
        `/tasks/${taskId}/media`,
        {
          search: { limit: 200, sort_by: 'created_at', sort_order: 'asc' },
          signal,
        },
      )
      return response.items
    },
    enabled: Boolean(taskId),
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
    queryFn: ({ signal }) => apiFetch<Workspace>(`/tasks/${taskId}/workspace`, { signal }),
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

export function useTaskDiffQuery(taskId: string, options: { enabled?: boolean } = {}) {
  return useQuery({
    queryKey: qk.taskDiff(taskId),
    queryFn: ({ signal }) => apiFetch<DiffEnvelope>(`/tasks/${taskId}/diff`, { signal }),
    enabled: Boolean(taskId) && (options.enabled ?? true),
  })
}

// --- Transition log ---

export function useTransitionLogQuery(taskId: string, enabled = true) {
  return useQuery({
    queryKey: qk.transitions(taskId),
    queryFn: async ({ signal }) => {
      const response = await apiFetch<{ items: TransitionLogEntry[] }>(
        `/tasks/${taskId}/transitions`,
        { signal },
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
    queryFn: ({ signal }) =>
      apiFetch<TaskRoleAssignmentResponse[]>(`/tasks/${taskId}/roles`, { signal }),
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
    onSuccess: (_, dependsOnId) => {
      void queryClient.invalidateQueries({ queryKey: qk.taskDependencies(taskId) })
      void queryClient.invalidateQueries({ queryKey: qk.taskRelations(taskId) })
      void queryClient.invalidateQueries({ queryKey: qk.taskRelations(dependsOnId) })
    },
  })
}

export function useRemoveDependency(taskId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (dependsOnId: string) => removeDependency(taskId, dependsOnId),
    onSuccess: (_, dependsOnId) => {
      void queryClient.invalidateQueries({ queryKey: qk.taskDependencies(taskId) })
      void queryClient.invalidateQueries({ queryKey: qk.taskRelations(taskId) })
      void queryClient.invalidateQueries({ queryKey: qk.taskRelations(dependsOnId) })
    },
  })
}
