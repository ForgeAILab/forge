// Task queries, edits, transitions, and actions.

import {
  type InfiniteData,
  replaceEqualDeep,
  useInfiniteQuery,
  useMutation,
  useQuery,
  useQueryClient,
} from '@tanstack/react-query'
import {
  ApiError,
  apiFetch,
  fetchTaskList,
  retainTaskListEtag,
  applyTaskAction,
} from '@/api/client'
import { qk } from '@/api/query-keys'
import {
  TaskFieldConflictError,
  taskFieldValue,
  taskFieldValuesEqual,
  type TaskConflictField,
  type TaskConflictValue,
} from '@/api/task-update-conflict'
import {
  getApiErrorCode,
  isTaskBusy,
  isTransientApiError,
  taskBusyRetryAfterMs,
} from '@/lib/api-error'
import type {
  CreateTaskRequest,
  PaginatedResponse,
  Task,
  TaskDetailResponse,
  TaskRelationsResponse,
  TransitionTaskResponse,
  TransitionTaskRequest,
  TaskActionRequest,
  UpdateTaskRequest,
} from '@/types/generated'
import type { TasksResponse } from '@/types/generated'
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

function filterKey(input: TaskSearch): string {
  return JSON.stringify(input)
}

// --- Tasks ---

export function useTasksQuery(
  projectId: string,
  search: TaskSearch,
  options: { enabled?: boolean } = {},
) {
  const queryClient = useQueryClient()
  const queryKey = qk.tasks(projectId, filterKey(search))
  return useInfiniteQuery({
    queryKey,
    queryFn: ({ pageParam, signal }) => {
      const cached = queryClient.getQueryData<{ pages: TasksResponse[]; pageParams: unknown[] }>(
        queryKey,
      )
      const index = cached?.pageParams.findIndex((cursor) => cursor === pageParam) ?? -1
      return fetchTaskList(
        projectId,
        { ...search, cursor: pageParam },
        cached?.pages[index],
        signal,
      )
    },
    structuralSharing: (previous, incoming) => {
      const data = incoming as InfiniteData<TasksResponse>
      const shared = replaceEqualDeep(previous, data)
      data.pages.forEach((page, index) => retainTaskListEtag(page, shared.pages[index]))
      return shared
    },
    enabled: options.enabled ?? true,
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

function shouldRetryTaskDetailQuery(failureCount: number, error: unknown): boolean {
  return failureCount < 1 && isTransientApiError(error)
}

export function useTaskDetailQuery(taskId: string, options: { enabled?: boolean } = {}) {
  return useQuery({
    queryKey: qk.taskDetail(taskId),
    queryFn: ({ signal }) =>
      apiFetch<TaskDetailResponse>(`/tasks/${taskId}/detail`, {
        search: { include_total: true },
        signal,
      }),
    enabled: Boolean(taskId) && (options.enabled ?? true),
    retry: shouldRetryTaskDetailQuery,
  })
}

export function useTaskRelationsQuery(taskId: string, projectId: string, enabled = true) {
  return useQuery({
    queryKey: qk.taskRelations(taskId),
    queryFn: ({ signal }) =>
      apiFetch<TaskRelationsResponse>(`/tasks/${taskId}/relations`, { signal }),
    enabled: Boolean(taskId) && enabled,
    meta: { projectId },
    retry: shouldRetryTaskDetailQuery,
  })
}

export function useTaskQuery(taskId: string, options: { enabled?: boolean } = {}) {
  return useQuery({
    queryKey: qk.task(taskId),
    queryFn: ({ signal }) => apiFetch<Task>(`/tasks/${taskId}`, { signal }),
    enabled: Boolean(taskId) && (options.enabled ?? true),
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

type UpdateTaskMutation = {
  taskId: string
  body: UpdateTaskRequest
  expectedTask?: Task
}

const conflictAwareTaskFields: TaskConflictField[] = ['title', 'description', 'priority']

function requestedConflictFields(body: UpdateTaskRequest): TaskConflictField[] {
  return conflictAwareTaskFields.filter((field) => {
    const value = body[field]
    return value !== undefined && !(field === 'description' && value === null)
  })
}

function requestedTaskFieldValue(
  body: UpdateTaskRequest,
  field: TaskConflictField,
): TaskConflictValue {
  return body[field]
}

function cacheUpdatedTask(queryClient: ReturnType<typeof useQueryClient>, task: Task): void {
  queryClient.setQueryData(qk.task(task.id), task)
  queryClient.setQueryData<TaskDetailResponse>(qk.taskDetail(task.id), (current) =>
    current ? { ...current, task } : current,
  )
}

function isTaskVersionConflict(error: unknown): error is ApiError {
  return (
    error instanceof ApiError &&
    error.status === 409 &&
    getApiErrorCode(error) === 'version_conflict'
  )
}

export function useUpdateTask() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: async ({ taskId, body, expectedTask }: UpdateTaskMutation) => {
      const patch = (request: UpdateTaskRequest) =>
        apiFetch<Task>(`/tasks/${taskId}`, {
          method: 'PATCH',
          body: JSON.stringify(request),
        })

      try {
        return await patch(body)
      } catch (error) {
        const fields = requestedConflictFields(body)
        if (
          !isTaskVersionConflict(error) ||
          !expectedTask ||
          expectedTask.id !== taskId ||
          fields.length === 0
        ) {
          throw error
        }

        const latest = await apiFetch<Task>(`/tasks/${taskId}`)
        cacheUpdatedTask(queryClient, latest)
        void queryClient.invalidateQueries({ queryKey: qk.projectTasks(latest.project_id) })

        const conflictingFields = fields.filter((field) => {
          const currentValue = taskFieldValue(latest, field)
          const expectedValue = taskFieldValue(expectedTask, field)
          const requestedValue = requestedTaskFieldValue(body, field)
          return (
            !taskFieldValuesEqual(field, currentValue, expectedValue) &&
            !taskFieldValuesEqual(field, currentValue, requestedValue)
          )
        })
        if (conflictingFields.length > 0) {
          throw new TaskFieldConflictError(latest, conflictingFields)
        }

        const alreadyApplied = fields.every((field) =>
          taskFieldValuesEqual(
            field,
            taskFieldValue(latest, field),
            requestedTaskFieldValue(body, field),
          ),
        )
        if (alreadyApplied) return latest

        try {
          return await patch({ ...body, version: latest.version })
        } catch (retryError) {
          if (!isTaskVersionConflict(retryError)) throw retryError
          const newest = await apiFetch<Task>(`/tasks/${taskId}`)
          cacheUpdatedTask(queryClient, newest)
          void queryClient.invalidateQueries({ queryKey: qk.projectTasks(newest.project_id) })
          throw new TaskFieldConflictError(newest, fields, true)
        }
      }
    },
    onSuccess: (task) => {
      cacheUpdatedTask(queryClient, task)
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
    onError: (error, { taskId }) => refetchAfterTaskBusy(queryClient, error, taskId),
  })
}

/** A `task_busy` request stays queued; refetch the Task once its retry hint elapses. */
export function refetchAfterTaskBusy(
  queryClient: ReturnType<typeof useQueryClient>,
  error: unknown,
  taskId: string,
) {
  if (!isTaskBusy(error)) return
  const refetch = () => {
    for (const queryKey of [qk.task(taskId), qk.taskDetail(taskId)]) {
      void queryClient.invalidateQueries({ queryKey })
    }
  }
  refetch()
  setTimeout(refetch, taskBusyRetryAfterMs(error))
}

export function useTaskAction() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ taskId, ...request }: TaskActionRequest & { taskId: string }) =>
      applyTaskAction(taskId, request),
    onSuccess: (task) => {
      for (const queryKey of [
        qk.task(task.id),
        qk.taskDetail(task.id),
        qk.executions(task.id),
        qk.reviews(task.id),
        qk.transitions(task.id),
        qk.projectTasks(task.project_id),
      ]) {
        void queryClient.invalidateQueries({ queryKey })
      }
    },
    onError: (error, variables) => {
      void queryClient.invalidateQueries({ queryKey: qk.task(variables.taskId) })
      void queryClient.invalidateQueries({ queryKey: qk.taskDetail(variables.taskId) })
      refetchAfterTaskBusy(queryClient, error, variables.taskId)
    },
  })
}
