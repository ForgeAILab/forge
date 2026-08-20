import type {
  Agent,
  BranchListResponse,
  FsListResponse,
  NotificationResponse,
  OperationsRefreshResponse,
  OperatorStatusResponse,
  PaginatedResponse,
  ProjectAnalyticsResponse,
  ProjectMemberResponse,
  UpdateProfileRequest,
  UserResponse,
  UserSearchResult,
  RecoverTaskRequest,
  RecoveryAction,
  ReorderSubtasksRequest,
  SaveWorkflowTemplateRequest,
  SettingsResponse,
  Task,
  TokenResponse,
  UnreadCountResponse,
  UpdateSettingsRequest,
  WorkflowTemplateResponse,
  WorkflowTemplateSummary,
} from '@/types/generated'
import type { ProjectHookRunsResponse } from '@/types/generated/bindings/ProjectHookRunsResponse'
import { refreshAccess, RefreshUnavailableError, useAuthStore } from '@/stores/auth'

const API_BASE = '/api/v1'

type ApiFetchInit = RequestInit & {
  search?: Record<string, string | number | boolean | undefined>
}

export class ApiError extends Error {
  status: number
  requestId?: string

  constructor(message: string, status: number, requestId?: string) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.requestId = requestId
  }
}

async function apiResponse(path: string, init?: ApiFetchInit): Promise<Response> {
  const url = new URL(`${API_BASE}${path}`, window.location.origin)
  const { search, headers, ...fetchInit } = init ?? {}
  if (search) {
    for (const [key, value] of Object.entries(search)) {
      if (value !== undefined && value !== '') {
        url.searchParams.set(key, String(value))
      }
    }
  }

  const makeHeaders = (overrideToken?: string) => {
    const h = new Headers(headers)
    const isFormDataBody = typeof FormData !== 'undefined' && fetchInit.body instanceof FormData
    if (!h.has('content-type') && !isFormDataBody) {
      h.set('content-type', 'application/json')
    }
    const token = overrideToken ?? useAuthStore.getState().accessToken
    if (token && !h.has('authorization')) {
      h.set('authorization', `Bearer ${token}`)
    }
    return h
  }

  let response = await fetch(url, {
    ...fetchInit,
    headers: makeHeaders(),
  })

  if (response.status === 401) {
    let newToken: string
    try {
      newToken = await refreshAccess()
    } catch (error) {
      // A refresh that could not reach a verdict leaves the session intact —
      // bouncing to /login here would sign the user out on every server restart.
      if (error instanceof RefreshUnavailableError) {
        throw new ApiError(error.message, 503)
      }
      useAuthStore.getState().clearAuth()
      window.location.href = '/login'
      throw new ApiError('Unauthorized', 401)
    }
    response = await fetch(url, {
      ...fetchInit,
      headers: makeHeaders(newToken),
    })
  }

  if (!response.ok) {
    const text = await response.text()
    throw new ApiError(
      text || response.statusText,
      response.status,
      response.headers.get('x-request-id') ?? undefined,
    )
  }

  return response
}

export async function apiFetch<T>(path: string, init?: ApiFetchInit): Promise<T> {
  const response = await apiResponse(path, init)
  const text = await response.text()
  if (response.status === 204 || text.length === 0) {
    return undefined as T
  }

  return JSON.parse(text) as T
}

export async function apiFetchBlob(path: string, init?: ApiFetchInit): Promise<Blob> {
  const response = await apiResponse(path, init)
  return response.blob()
}

export function listFsEntries(path: string, daemonId: string): Promise<FsListResponse> {
  return apiFetch<FsListResponse>('/fs/list', {
    search: { path, daemon_id: daemonId },
  })
}

export function listBranches(path: string, daemonId: string): Promise<BranchListResponse> {
  return apiFetch<BranchListResponse>('/fs/branches', {
    search: { path, daemon_id: daemonId },
  })
}

export function listAgents(limit = 100, cursor?: string): Promise<PaginatedResponse<Agent>> {
  return apiFetch<PaginatedResponse<Agent>>('/agents', {
    search: { limit, cursor },
  })
}

export async function reorderSubtasks(taskId: string, body: ReorderSubtasksRequest): Promise<Task> {
  return apiFetch<Task>(`/tasks/${taskId}/subtasks/reorder`, {
    method: 'POST',
    body: JSON.stringify(body),
  })
}

export interface TaskDependency {
  task_id: string
  depends_on_id: string
  created_at: string
}

export function listDependencies(taskId: string): Promise<TaskDependency[]> {
  return apiFetch<TaskDependency[]>(`/tasks/${taskId}/dependencies`)
}

export function listDependents(taskId: string): Promise<TaskDependency[]> {
  return apiFetch<TaskDependency[]>(`/tasks/${taskId}/dependents`)
}

export function addDependency(taskId: string, dependsOnId: string): Promise<void> {
  return apiFetch<void>(`/tasks/${taskId}/dependencies`, {
    method: 'POST',
    body: JSON.stringify({ depends_on_id: dependsOnId }),
  })
}

export function removeDependency(taskId: string, dependsOnId: string): Promise<void> {
  return apiFetch<void>(`/tasks/${taskId}/dependencies/${dependsOnId}`, {
    method: 'DELETE',
  })
}

export async function recoverTask(
  taskId: string,
  action: RecoveryAction,
  reason?: string,
  context?: string,
): Promise<Task> {
  return apiFetch<Task>(`/tasks/${taskId}/recover`, {
    method: 'POST',
    body: JSON.stringify({
      action,
      reason: reason ?? null,
      context: context ?? null,
    } satisfies RecoverTaskRequest),
  })
}

export function listWorkflowTemplates(): Promise<WorkflowTemplateSummary[]> {
  return apiFetch<WorkflowTemplateSummary[]>('/workflow-templates')
}

export function getWorkflowTemplate(name: string): Promise<WorkflowTemplateResponse> {
  return apiFetch<WorkflowTemplateResponse>(`/workflow-templates/${name}`)
}

export function saveWorkflowTemplate(
  name: string,
  body: SaveWorkflowTemplateRequest,
): Promise<WorkflowTemplateResponse> {
  return apiFetch<WorkflowTemplateResponse>(`/workflow-templates/${name}`, {
    method: 'PUT',
    body: JSON.stringify(body),
  })
}

export async function deleteWorkflowTemplate(name: string): Promise<void> {
  return apiFetch<void>(`/workflow-templates/${name}`, { method: 'DELETE' })
}

export function listNotifications(params?: {
  project_id?: string
  read?: boolean
  cursor?: string
  limit?: number
}): Promise<PaginatedResponse<NotificationResponse>> {
  return apiFetch<PaginatedResponse<NotificationResponse>>('/notifications', {
    search: params,
  })
}

export function getUnreadNotificationCount(projectId?: string): Promise<UnreadCountResponse> {
  return apiFetch<UnreadCountResponse>('/notifications/unread-count', {
    search: { project_id: projectId },
  })
}

export function getOperationsStatus(): Promise<OperatorStatusResponse> {
  return apiFetch<OperatorStatusResponse>('/operations/status')
}

export function refreshOperations(): Promise<OperationsRefreshResponse> {
  return apiFetch<OperationsRefreshResponse>('/operations/refresh', { method: 'POST' })
}

export async function getProjectAnalytics(
  projectId: string,
  from?: string,
  to?: string,
): Promise<ProjectAnalyticsResponse> {
  const params = new URLSearchParams()
  if (from) params.set('from', from)
  if (to) params.set('to', to)
  const query = params.toString()
  return apiFetch<ProjectAnalyticsResponse>(
    `/projects/${projectId}/analytics${query ? `?${query}` : ''}`,
  )
}

export function listProjectHookRuns(
  projectId: string,
  cursor?: string,
  limit = 20,
): Promise<ProjectHookRunsResponse> {
  return apiFetch<ProjectHookRunsResponse>(`/projects/${projectId}/project_hook_runs`, {
    search: { cursor, limit },
  })
}

export function markNotificationRead(notificationId: string): Promise<NotificationResponse> {
  return apiFetch<NotificationResponse>(`/notifications/${notificationId}/read`, {
    method: 'PATCH',
  })
}

export function markAllNotificationsRead(projectId?: string): Promise<void> {
  return apiFetch<void>('/notifications/mark-all-read', {
    method: 'POST',
    search: { project_id: projectId },
  })
}

export function deleteNotification(notificationId: string): Promise<void> {
  return apiFetch<void>(`/notifications/${notificationId}`, {
    method: 'DELETE',
  })
}

export function getSettings(): Promise<SettingsResponse> {
  return apiFetch<SettingsResponse>('/settings')
}

export function updateSettings(body: UpdateSettingsRequest): Promise<SettingsResponse> {
  return apiFetch<SettingsResponse>('/settings', {
    method: 'PUT',
    body: JSON.stringify(body),
  })
}

// --- User Profile ---

export function updateMe(body: UpdateProfileRequest): Promise<UserResponse> {
  return apiFetch<UserResponse>('/auth/me', {
    method: 'PATCH',
    body: JSON.stringify(body),
  })
}

// --- Personal Access Tokens ---

export function listPats(): Promise<TokenResponse[]> {
  return apiFetch<TokenResponse[]>('/auth/tokens')
}

export function createPat(body: {
  name: string
  expires_at?: string | null
}): Promise<TokenResponse> {
  return apiFetch<TokenResponse>('/auth/tokens', {
    method: 'POST',
    body: JSON.stringify(body),
  })
}

export function deletePat(id: string): Promise<void> {
  return apiFetch<void>(`/auth/tokens/${id}`, { method: 'DELETE' })
}

// --- User Search ---

export function searchUsers(q: string): Promise<UserSearchResult[]> {
  return apiFetch<UserSearchResult[]>('/users/search', { search: { q, limit: 10 } })
}

// --- Project Members ---

export function listMembers(projectId: string): Promise<ProjectMemberResponse[]> {
  return apiFetch<ProjectMemberResponse[]>(`/projects/${projectId}/members`)
}

export function listProjectAgents(projectId: string): Promise<Agent[]> {
  return apiFetch<Agent[]>(`/projects/${projectId}/agents`)
}

export function addMember(
  projectId: string,
  body: { user_id: string; role: string },
): Promise<ProjectMemberResponse> {
  return apiFetch<ProjectMemberResponse>(`/projects/${projectId}/members`, {
    method: 'POST',
    body: JSON.stringify(body),
  })
}

export function updateMemberRole(
  projectId: string,
  userId: string,
  body: { role: string },
): Promise<ProjectMemberResponse> {
  return apiFetch<ProjectMemberResponse>(`/projects/${projectId}/members/${userId}`, {
    method: 'PATCH',
    body: JSON.stringify(body),
  })
}

export function removeMember(projectId: string, userId: string): Promise<void> {
  return apiFetch<void>(`/projects/${projectId}/members/${userId}`, { method: 'DELETE' })
}
