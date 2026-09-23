import { GitFork, Plus, X } from '@phosphor-icons/react'
import { useQuery } from '@tanstack/react-query'
import { useNavigate } from '@tanstack/react-router'
import { useEffect, useMemo, useState } from 'react'
import { toast } from 'sonner'
import { apiFetch } from '@/api/client'
import { useAddDependency, useRemoveDependency, useTaskRelationsQuery } from '@/api/hooks'
import { qk } from '@/api/query-keys'
import { TaskStatusBadge } from '@/components/task-controls'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { getApiErrorMessage } from '@/lib/api-error'
import type { PaginatedResponse, Task } from '@/types/generated'

const CANDIDATE_TASK_LIMIT = 30
const TASK_SEARCH_DEBOUNCE_MS = 250

interface TaskDependenciesPanelProps {
  task: Task
}

function useDependencyCandidatesQuery(projectId: string, search: string, enabled: boolean) {
  return useQuery({
    queryKey: [
      ...qk.projectTasks(projectId),
      'dependency-candidates',
      search,
      CANDIDATE_TASK_LIMIT,
    ],
    queryFn: ({ signal }) =>
      apiFetch<PaginatedResponse<Task>>(`/projects/${projectId}/tasks`, {
        search: {
          q: search || undefined,
          limit: CANDIDATE_TASK_LIMIT,
          include_cancelled: false,
        },
        signal,
      }),
    enabled: enabled && Boolean(projectId),
    retry: false,
    staleTime: 15_000,
  })
}

export function TaskDependenciesPanel({ task }: TaskDependenciesPanelProps) {
  const navigate = useNavigate()
  const relationsQuery = useTaskRelationsQuery(task.id, task.project_id)
  const addDependency = useAddDependency(task.id)
  const removeDependency = useRemoveDependency(task.id)

  const [showPicker, setShowPicker] = useState(false)
  const [search, setSearch] = useState('')
  const [debouncedSearch, setDebouncedSearch] = useState('')

  useEffect(() => {
    if (!showPicker) {
      setDebouncedSearch('')
      return
    }

    const timeout = window.setTimeout(
      () => setDebouncedSearch(search.trim()),
      TASK_SEARCH_DEBOUNCE_MS,
    )
    return () => window.clearTimeout(timeout)
  }, [search, showPicker])

  const candidatesQuery = useDependencyCandidatesQuery(
    task.project_id,
    debouncedSearch,
    showPicker && Boolean(relationsQuery.data),
  )

  const candidatePageTasks = candidatesQuery.data?.items ?? []
  const dependencyTasks = relationsQuery.data?.dependencies ?? []
  const missingDependencyIds = relationsQuery.data?.missing_dependency_ids ?? []
  const dependentTasks = relationsQuery.data?.dependents ?? []
  const depIds = useMemo(
    () => new Set(dependencyTasks.map((dependency) => dependency.id)),
    [dependencyTasks],
  )
  const dependentTaskIds = useMemo(
    () => new Set(dependentTasks.map((dependent) => dependent.id)),
    [dependentTasks],
  )
  const missingDependencies = useMemo(
    () => missingDependencyIds.filter((id) => !depIds.has(id)),
    [missingDependencyIds, depIds],
  )
  const hasDependencies = dependencyTasks.length > 0 || missingDependencies.length > 0

  const isTerminal = task.status === 'done' || task.status === 'cancelled'

  const candidateTasks = useMemo(() => {
    return candidatePageTasks.filter(
      (t) =>
        t.id !== task.id &&
        !depIds.has(t.id) &&
        !dependentTaskIds.has(t.id) &&
        t.status !== 'cancelled',
    )
  }, [candidatePageTasks, task.id, depIds, dependentTaskIds])

  const handleAdd = (dependsOnId: string) => {
    addDependency.mutate(dependsOnId, {
      onSuccess: () => {
        setShowPicker(false)
        setSearch('')
      },
      onError: (error) => {
        toast.error(getApiErrorMessage(error, 'Failed to add dependency'))
      },
    })
  }

  const handleRemove = (dependsOnId: string) => {
    removeDependency.mutate(dependsOnId, {
      onError: (error) => toast.error(getApiErrorMessage(error, 'Failed to remove dependency')),
    })
  }

  const openTaskDetail = (taskId: string) => {
    void navigate({ to: '/tasks/$taskId', params: { taskId } })
  }

  const isLoading = !relationsQuery.data && relationsQuery.isLoading
  const isSearchPending = showPicker && search.trim() !== debouncedSearch

  return (
    <div className="mt-4 border-t pt-4">
      <div className="mb-2 flex items-center justify-between">
        <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
          Dependencies
        </p>
        {!isTerminal && (
          <Button
            size="icon"
            variant="ghost"
            className="h-6 w-6"
            aria-label="Add dependency"
            onClick={() => {
              setShowPicker((v) => !v)
              setSearch('')
            }}
          >
            <Plus size={14} />
          </Button>
        )}
      </div>

      {showPicker && (
        <div className="mb-3 rounded-md border bg-background p-2">
          <input
            autoFocus
            type="text"
            placeholder="Search task titles or descriptions…"
            className="mb-1.5 w-full rounded border bg-muted/40 px-2 py-1 text-sm outline-none placeholder:text-muted-foreground focus:ring-1 focus:ring-ring"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === 'Escape') {
                setShowPicker(false)
                setSearch('')
              }
            }}
          />
          <div className="max-h-40 overflow-y-auto">
            {!relationsQuery.data && relationsQuery.isLoading ? (
              <div className="space-y-1 p-1">
                <Skeleton className="h-7 w-full" />
                <Skeleton className="h-7 w-full" />
              </div>
            ) : isSearchPending || candidatesQuery.isLoading ? (
              <div className="space-y-1 p-1">
                <Skeleton className="h-7 w-full" />
                <Skeleton className="h-7 w-full" />
              </div>
            ) : relationsQuery.isError && !relationsQuery.data ? (
              <div className="flex items-center justify-between gap-2 px-2 py-1.5">
                <p role="status" className="text-xs text-muted-foreground">
                  Couldn&apos;t load task relationships
                </p>
                <Button
                  size="sm"
                  variant="ghost"
                  className="h-6 px-2 text-xs"
                  onClick={() => void relationsQuery.refetch()}
                >
                  Retry
                </Button>
              </div>
            ) : candidatesQuery.isError ? (
              <div className="flex items-center justify-between gap-2 px-2 py-1.5">
                <p role="status" className="text-xs text-muted-foreground">
                  Couldn&apos;t load tasks
                </p>
                <Button
                  size="sm"
                  variant="ghost"
                  className="h-6 px-2 text-xs"
                  onClick={() => void candidatesQuery.refetch()}
                >
                  Retry
                </Button>
              </div>
            ) : candidateTasks.length === 0 ? (
              <p className="px-2 py-1.5 text-xs text-muted-foreground">No matching tasks</p>
            ) : (
              candidateTasks.map((t) => (
                <button
                  key={t.id}
                  type="button"
                  className="flex w-full cursor-pointer items-center gap-2 rounded px-2 py-1.5 text-left text-sm hover:bg-accent disabled:cursor-not-allowed disabled:opacity-50"
                  disabled={addDependency.isPending}
                  onClick={() => handleAdd(t.id)}
                >
                  <span className="min-w-0 flex-1 truncate">{t.title}</span>
                  <TaskStatusBadge status={t.status} />
                </button>
              ))
            )}
            {candidatesQuery.data?.has_more && !candidatesQuery.isError && (
              <p className="px-2 py-1.5 text-xs text-muted-foreground">
                Showing the first {CANDIDATE_TASK_LIMIT} results. Refine your search to see more.
              </p>
            )}
          </div>
          <Button
            size="sm"
            variant="ghost"
            className="mt-1 h-6 w-full text-xs text-muted-foreground"
            onClick={() => {
              setShowPicker(false)
              setSearch('')
            }}
          >
            Cancel
          </Button>
        </div>
      )}

      {isLoading ? (
        <div className="space-y-2">
          <Skeleton className="h-8 w-full" />
          <Skeleton className="h-8 w-full" />
        </div>
      ) : relationsQuery.isError && !relationsQuery.data ? (
        <div className="flex items-center justify-between gap-2 rounded-md border px-3 py-2">
          <p role="status" className="text-sm text-muted-foreground">
            Couldn&apos;t load task relationships
          </p>
          <Button size="sm" variant="ghost" onClick={() => void relationsQuery.refetch()}>
            Retry
          </Button>
        </div>
      ) : !hasDependencies ? (
        <p className="text-sm text-muted-foreground">No dependencies</p>
      ) : (
        <div className="space-y-1.5">
          {dependencyTasks.map((dep) => (
            <div
              key={dep.id}
              className="group flex items-center gap-2 rounded-md border bg-background px-2 py-1.5"
            >
              <button
                type="button"
                className="flex min-w-0 flex-1 cursor-pointer items-center gap-2 rounded-sm text-left transition-colors hover:text-primary focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                onClick={() => openTaskDetail(dep.id)}
              >
                <span className="min-w-0 flex-1 truncate text-sm">{dep.title}</span>
                <TaskStatusBadge status={dep.status} />
              </button>
              {!isTerminal && (
                <button
                  type="button"
                  aria-label={`Remove dependency on ${dep.title}`}
                  className="ml-0.5 shrink-0 rounded p-0.5 text-muted-foreground transition-colors hover:text-destructive"
                  disabled={removeDependency.isPending}
                  onClick={() => handleRemove(dep.id)}
                >
                  <X size={12} />
                </button>
              )}
            </div>
          ))}
          {missingDependencies.map((dependencyId) => (
            <div
              key={dependencyId}
              className="group flex items-center gap-2 rounded-md border bg-background px-2 py-1.5"
            >
              <button
                type="button"
                className="flex min-w-0 flex-1 cursor-pointer items-center gap-2 rounded-sm text-left transition-colors hover:text-primary focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                onClick={() => openTaskDetail(dependencyId)}
              >
                <span className="min-w-0 flex-1 truncate text-sm">{dependencyId}</span>
                <span className="shrink-0 rounded border px-1.5 py-0.5 text-[10px] uppercase text-muted-foreground">
                  Missing
                </span>
              </button>
              {!isTerminal && (
                <button
                  type="button"
                  aria-label={`Remove dependency on ${dependencyId}`}
                  className="ml-0.5 shrink-0 rounded p-0.5 text-muted-foreground transition-colors hover:text-destructive"
                  disabled={removeDependency.isPending}
                  onClick={() => handleRemove(dependencyId)}
                >
                  <X size={12} />
                </button>
              )}
            </div>
          ))}
        </div>
      )}

      {dependentTasks.length > 0 && (
        <div className="mt-3">
          <div className="mb-1.5 flex items-center gap-1 text-xs text-muted-foreground">
            <GitFork size={11} />
            <span className="uppercase tracking-wide font-medium">Blocking</span>
          </div>
          <div className="space-y-1.5">
            {dependentTasks.map((dep) => (
              <button
                key={dep.id}
                type="button"
                className="flex w-full cursor-pointer items-center gap-2 rounded-md border bg-muted/30 px-2 py-1.5 text-left transition-colors hover:border-border hover:bg-accent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                onClick={() => openTaskDetail(dep.id)}
              >
                <span className="min-w-0 flex-1 truncate text-sm text-muted-foreground">
                  {dep.title}
                </span>
                <TaskStatusBadge status={dep.status} />
              </button>
            ))}
          </div>
        </div>
      )}
    </div>
  )
}
