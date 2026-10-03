import { useMemo, useState } from 'react'
import { useNavigate } from '@tanstack/react-router'
import {
  useAgentsQuery,
  useArchiveTask,
  useAssignRole,
  useMembersQuery,
  useProjectAgentsQuery,
  useRemoveRole,
  useTasksQuery,
  useTransitionTask,
} from '@/api/hooks'
import { BulkCancelTasks, TaskRowActions } from '@/components/task-detail/task-list-actions'
import { ErrorBanner } from '@/components/error-banner'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import { Skeleton } from '@/components/ui/skeleton'
import {
  type AssigneeSelection,
  AgentAssigneeDropdown,
  TaskStatusDropdown,
} from '@/components/task-controls'
import { Avatar } from '@/components/ui/avatar'
import { AgentFilterGroup } from '@/components/agent-filter-group'
import { WorkflowHealthBadge } from '@/components/workflow-health-badge'
import { Funnel, Plus, UserCircle, X } from '@phosphor-icons/react'
import { cn } from '@/lib/cn'
import { toastApiError } from '@/lib/api-error'
import { productTerm } from '@/lib/i18n'
import { getBlockingAnnotation } from '@/lib/workflow-utils'
import type { TaskStatus } from '@/types/generated'
import type { TaskListItem as Task } from '@/types/generated'

export type TaskListSortBy = 'title' | 'status' | 'agent' | 'priority' | 'task_type' | 'updated_at'
export type TaskListSortOrder = 'asc' | 'desc'

const tableColumns: Array<{ key: TaskListSortBy; label: string }> = [
  { key: 'title', label: 'Title' },
  { key: 'priority', label: 'Pri' },
  { key: 'status', label: productTerm('phase') },
  { key: 'agent', label: 'Agent' },
  { key: 'updated_at', label: 'Updated' },
]

function formatDate(value: string): string {
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return date.toLocaleString()
}

function formatBlockingReason(value: string) {
  const withSpaces = value.replace(/_/g, ' ')
  return withSpaces.charAt(0).toUpperCase() + withSpaces.slice(1)
}

export function TaskListPage({
  projectId,
  sortBy,
  sortOrder,
  agentIds,
  blockedOnly,
  includeCancelled,
  includeArchived,
  priorityMin,
  priorityMax,
  onSortChange,
  onFilterChange,
}: {
  projectId: string
  sortBy: TaskListSortBy
  sortOrder: TaskListSortOrder
  agentIds: string[]
  blockedOnly: boolean
  includeCancelled: boolean
  includeArchived: boolean
  priorityMin?: number
  priorityMax?: number
  onSortChange: (sortBy: TaskListSortBy, sortOrder: TaskListSortOrder) => void
  onFilterChange: (patch: {
    agentIds?: string[]
    blockedOnly?: boolean
    includeCancelled?: boolean
    includeArchived?: boolean
    priorityMin?: number
    priorityMax?: number
  }) => void
}) {
  const navigate = useNavigate()
  const agentsQuery = useAgentsQuery()
  const { data: projectAgentsData } = useProjectAgentsQuery(projectId)
  const { data: membersData } = useMembersQuery(projectId)
  const archiveTask = useArchiveTask()
  const assignRole = useAssignRole()
  const removeRole = useRemoveRole()
  const transitionTask = useTransitionTask()
  const [selectedIds, setSelectedIds] = useState<Set<string>>(() => new Set())
  const [showMobileFilters, setShowMobileFilters] = useState(false)
  const agentUuids = agentIds.filter((id) => id !== 'user')
  const tasksQuery = useTasksQuery(projectId, {
    sort_by: sortBy,
    sort_order: sortOrder,
    agent_id: agentUuids.length > 0 ? agentUuids.join(',') : undefined,
    assignee_type: agentIds.includes('user') ? 'user' : undefined,
    include_cancelled: includeCancelled,
    include_archived: includeArchived,
  })

  const tasks = useMemo(
    () => tasksQuery.data?.pages.flatMap((page) => page.items) ?? [],
    [tasksQuery.data],
  )
  const visibleTasks = useMemo(
    () =>
      tasks.filter((task) => {
        if (blockedOnly && !task.blocked) return false
        if (priorityMin !== undefined && task.priority < priorityMin) return false
        if (priorityMax !== undefined && task.priority > priorityMax) return false
        return true
      }),
    [blockedOnly, priorityMin, priorityMax, tasks],
  )
  const agentNamesById = useMemo(
    () => new Map((agentsQuery.data?.items ?? []).map((agent) => [agent.id, agent.name])),
    [agentsQuery.data],
  )
  const agents = agentsQuery.data?.items ?? []
  const selectedTasks = useMemo(
    () => visibleTasks.filter((task) => selectedIds.has(task.id)),
    [selectedIds, visibleTasks],
  )
  const assignableSelected = selectedTasks.filter(
    (task) => task.status !== 'done' && task.status !== 'cancelled',
  )
  const allVisibleSelected =
    visibleTasks.length > 0 && visibleTasks.every((task) => selectedIds.has(task.id))

  const hasActiveFilters =
    agentIds.length > 0 ||
    blockedOnly ||
    includeCancelled ||
    includeArchived ||
    priorityMin !== undefined ||
    priorityMax !== undefined

  const activeFilterCount = [
    agentIds.length > 0,
    priorityMin !== undefined || priorityMax !== undefined,
    blockedOnly,
    includeCancelled,
    includeArchived,
  ].filter(Boolean).length

  const toggleSort = (key: TaskListSortBy) => {
    onSortChange(key, sortBy === key && sortOrder === 'asc' ? 'desc' : 'asc')
  }

  const toggleTask = (taskId: string, checked: boolean) => {
    setSelectedIds((current) => {
      const next = new Set(current)
      if (checked) next.add(taskId)
      else next.delete(taskId)
      return next
    })
  }

  const toggleAllVisible = (checked: boolean) => {
    setSelectedIds((current) => {
      const next = new Set(current)
      for (const task of visibleTasks) {
        if (checked) next.add(task.id)
        else next.delete(task.id)
      }
      return next
    })
  }

  const archiveSelected = () => {
    for (const task of selectedTasks) {
      archiveTask.mutate(task.id, {
        onError: (error) => toastApiError(error, 'Task archive failed'),
      })
    }
    setSelectedIds(new Set())
  }

  const assignSelected = (selection: AssigneeSelection) => {
    if (assignableSelected.length === 0) return
    for (const task of assignableSelected) {
      assignTask(task, selection)
    }
    setSelectedIds(new Set())
  }

  const assignTask = (task: { id: string }, selection: AssigneeSelection) => {
    if (selection.type === 'unassigned') {
      removeRole.mutate(
        { taskId: task.id, roleName: 'coder' },
        { onError: (error) => toastApiError(error, 'Agent assignment failed') },
      )
      return
    }
    assignRole.mutate(
      {
        taskId: task.id,
        roleName: 'coder',
        body:
          selection.type === 'agent'
            ? { assignee_type: 'agent', assignee_id: selection.agentId }
            : { assignee_type: 'user', assignee_id: selection.userId },
      },
      { onError: (error) => toastApiError(error, 'Agent assignment failed') },
    )
  }

  const moveTask = (task: Task, status: TaskStatus) => {
    transitionTask.mutate(
      { taskId: task.id, body: { status, version: task.version }, currentStatus: task.status },
      {
        onError: (error) => toastApiError(error, 'Status transition failed'),
      },
    )
  }

  const handleAgentClick = (clickedAgentId: string) => {
    const next = agentIds.includes(clickedAgentId)
      ? agentIds.filter((id) => id !== clickedAgentId)
      : [...agentIds, clickedAgentId]
    onFilterChange({ agentIds: next })
  }

  return (
    <div className="max-w-[1100px] space-y-4">
      <div className="flex flex-wrap items-center justify-between gap-3">
        <div>
          <h1 className="text-page font-semibold tracking-tight">Tasks</h1>
        </div>
        <Button
          className="h-8 gap-1.5 rounded-md px-3 text-ui"
          onClick={() =>
            navigate({
              to: '/projects/$projectId/board',
              params: { projectId },
              search: {},
            })
          }
        >
          <Plus size={14} weight="bold" />
          Quick create task
        </Button>
      </div>

      {tasksQuery.isError ? (
        <ErrorBanner
          error={tasksQuery.error}
          fallback="Tasks failed to load"
          onRetry={() => void tasksQuery.refetch()}
        />
      ) : null}

      {selectedTasks.length > 0 ? (
        <div className="flex flex-wrap items-center gap-2 rounded-md border bg-background p-3 shadow-soft">
          <span className="text-sm font-medium">{selectedTasks.length} selected</span>
          <BulkCancelTasks tasks={selectedTasks} onComplete={() => setSelectedIds(new Set())} />
          <Button
            disabled={archiveTask.isPending}
            size="sm"
            variant="outline"
            onClick={archiveSelected}
          >
            Archive selected
          </Button>
          <div className="w-64">
            <AgentAssigneeDropdown
              agents={projectAgentsData ?? []}
              members={membersData}
              disabled={
                assignableSelected.length === 0 || assignRole.isPending || removeRole.isPending
              }
              placeholder="Assign selected"
              value={{ type: 'unassigned' }}
              onChange={assignSelected}
            />
          </div>
        </div>
      ) : null}

      {/* Filter bar */}
      <div className="flex flex-wrap items-center gap-2">
        {/* Mobile filter toggle */}
        <button
          type="button"
          className={cn(
            'flex md:hidden h-8 cursor-pointer items-center gap-1.5 rounded-lg border px-2.5 text-xs font-medium transition-colors hover:bg-accent',
            hasActiveFilters || showMobileFilters
              ? 'border-foreground/20 bg-foreground/5 text-foreground'
              : 'text-muted-foreground',
          )}
          onClick={() => setShowMobileFilters((v) => !v)}
        >
          <Funnel size={14} />
          Filters
          {activeFilterCount > 0 && (
            <span className="flex h-4 w-4 items-center justify-center rounded-full bg-foreground text-micro text-background">
              {activeFilterCount}
            </span>
          )}
        </button>

        {/* Filter content */}
        <div
          className={cn(
            'flex flex-wrap items-center gap-2',
            showMobileFilters ? 'w-full md:w-auto' : 'hidden md:flex',
          )}
        >
          {agents.length > 0 && (
            <>
              <AgentFilterGroup
                agents={agents}
                selectedAgentIds={agentIds}
                onSelect={(ids) => onFilterChange({ agentIds: ids })}
              />
              <div className="h-4 w-px bg-border" />
            </>
          )}

          {[
            { key: 'blockedOnly' as const, label: 'Blocked', active: blockedOnly },
            { key: 'includeCancelled' as const, label: 'Cancelled', active: includeCancelled },
            { key: 'includeArchived' as const, label: 'Archived', active: includeArchived },
          ].map(({ key, label, active }) => (
            <button
              key={key}
              type="button"
              className={cn(
                'flex h-7 cursor-pointer items-center rounded-full px-2.5 text-xs font-medium transition-colors',
                active
                  ? 'bg-foreground/10 text-foreground ring-1 ring-inset ring-foreground/20'
                  : 'text-muted-foreground hover:bg-accent hover:text-foreground',
              )}
              onClick={() => onFilterChange({ [key]: !active })}
            >
              {label}
            </button>
          ))}

          <div className="h-4 w-px bg-border" />

          <div className="flex items-center gap-1.5">
            <span className="text-xs font-medium text-muted-foreground">Priority</span>
            <input
              className="h-7 w-16 rounded-lg border bg-background px-2 text-xs focus:outline-none focus:ring-2 focus:ring-ring"
              min={0}
              placeholder="Min"
              type="number"
              value={priorityMin ?? ''}
              onChange={(e) =>
                onFilterChange({ priorityMin: e.target.value === '' ? undefined : Number(e.target.value) })
              }
            />
            <span className="text-xs text-muted-foreground">–</span>
            <input
              className="h-7 w-16 rounded-lg border bg-background px-2 text-xs focus:outline-none focus:ring-2 focus:ring-ring"
              min={0}
              placeholder="Max"
              type="number"
              value={priorityMax ?? ''}
              onChange={(e) =>
                onFilterChange({ priorityMax: e.target.value === '' ? undefined : Number(e.target.value) })
              }
            />
          </div>

          {(priorityMin !== undefined || priorityMax !== undefined) && (
            <span className="flex items-center gap-1 rounded-full border border-border bg-muted/50 py-1 pl-2.5 pr-1.5 text-xs text-foreground">
              P{priorityMin ?? '0'}–{priorityMax ?? '∞'}
              <button
                type="button"
                className="cursor-pointer text-muted-foreground transition-colors hover:text-foreground"
                onClick={() => onFilterChange({ priorityMin: undefined, priorityMax: undefined })}
              >
                <X size={10} weight="bold" />
              </button>
            </span>
          )}

          {hasActiveFilters && (
            <button
              type="button"
              className="cursor-pointer text-xs text-muted-foreground transition-colors hover:text-foreground"
              onClick={() =>
                onFilterChange({
                  agentIds: [],
                  blockedOnly: false,
                  includeCancelled: false,
                  includeArchived: false,
                  priorityMin: undefined,
                  priorityMax: undefined,
                })
              }
            >
              Clear all
            </button>
          )}
        </div>
      </div>

      {tasksQuery.isLoading ? (
        <div className="space-y-2 rounded-md border bg-background p-4">
          <Skeleton className="h-8 w-full" />
          <Skeleton className="h-8 w-full" />
          <Skeleton className="h-8 w-2/3" />
        </div>
      ) : !tasksQuery.isError && visibleTasks.length === 0 ? (
        <div className="rounded-md border border-dashed p-8 text-center">
          <p className="text-sm font-medium">
            {blockedOnly ? 'No blocked tasks' : 'No tasks yet - create your first task'}
          </p>
          {!blockedOnly ? (
            <Button
              className="mt-4"
              onClick={() =>
                navigate({
                  to: '/projects/$projectId/board',
                  params: { projectId },
                  search: {},
                })
              }
            >
              Create task
            </Button>
          ) : null}
        </div>
      ) : (
        <div className="overflow-hidden rounded-lg border border-border-subtle bg-card shadow-soft">
          <div className="overflow-x-auto">
            <table className="w-full min-w-[900px] border-collapse text-ui">
              <thead className="bg-muted text-left">
                <tr>
                  <th className="w-10 px-3 py-[9px]">
                    <Checkbox
                      aria-label="Select all visible tasks"
                      checked={allVisibleSelected}
                      onChange={(event) => toggleAllVisible(event.target.checked)}
                    />
                  </th>
                  {tableColumns.map((column) => (
                    <th key={column.key} className="px-3 py-[9px]">
                      <button
                        className="inline-flex items-center gap-1 rounded-sm font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground hover:text-foreground"
                        type="button"
                        onClick={() => toggleSort(column.key)}
                      >
                        {column.label}
                        {sortBy === column.key ? (
                          <span aria-hidden="true" className="text-primary">
                            {sortOrder === 'asc' ? '↑' : '↓'}
                          </span>
                        ) : null}
                      </button>
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {visibleTasks.map((task) => {
                  const coderAssignment = task.role_assignments.find(
                    (assignment) => assignment.role_name === 'coder',
                  )
                  const coderAgentId =
                    coderAssignment?.assignee_type === 'agent' ? coderAssignment.assignee_id : null
                  const coderAgentName = coderAgentId
                    ? (agentNamesById.get(coderAgentId) ?? coderAgentId)
                    : null
                  const coderUserId =
                    coderAssignment?.assignee_type === 'user'
                      ? (coderAssignment.assignee_id ?? 'manual')
                      : null
                  const coderIsHuman = coderAssignment?.assignee_type === 'user'
                  const assigneeValue: AssigneeSelection = coderAgentId
                    ? { type: 'agent', agentId: coderAgentId }
                    : coderIsHuman
                      ? { type: 'user', userId: coderUserId ?? 'manual' }
                      : { type: 'unassigned' }
                  const isTerminal = task.status === 'done' || task.status === 'cancelled'
                  return (
                    <tr
                      key={task.id}
                      className="border-t border-border-subtle transition-colors hover:bg-muted/35"
                    >
                      {/* checkbox */}
                      <td className="w-10 px-3 py-[10px]">
                        <Checkbox
                          aria-label={`Select ${task.title}`}
                          checked={selectedIds.has(task.id)}
                          onChange={(event) => toggleTask(task.id, event.target.checked)}
                        />
                      </td>
                      {/* Title */}
                      <td className="px-3 py-[10px]">
                        <button
                          className="line-clamp-2 text-left font-medium hover:text-primary"
                          type="button"
                          onClick={() =>
                            navigate({ to: '/tasks/$taskId', params: { taskId: task.id } })
                          }
                        >
                          {task.title}
                        </button>
                        <TaskRowActions taskId={task.id} />
                      </td>
                      {/* Pri */}
                      <td className="w-14 px-3 py-[10px]">
                        <span className="rounded bg-muted px-1.5 py-0.5 font-mono text-micro font-medium text-muted-foreground">
                          P{task.priority}
                        </span>
                      </td>
                      {/* Phase */}
                      <td className="w-36 px-3 py-[10px]">
                        <div className="flex flex-col gap-1">
                          <TaskStatusDropdown
                            disabled={transitionTask.isPending}
                            status={task.status}
                            onChange={(status) => moveTask(task, status)}
                          />
                          {(() => {
                            const blockingAnnotation = getBlockingAnnotation(task)
                            const blockedReason =
                              task.blocked?.reason ?? blockingAnnotation?.blocking_reason
                            if (!blockedReason || task.status === 'cancelled') return null
                            return (
                              <span className="inline-flex items-center rounded bg-red-500/10 px-2 py-[3px] text-micro font-medium text-red-300">
                                {formatBlockingReason(blockedReason)}
                              </span>
                            )
                          })()}
                          {task.workflow_health ? (
                            <WorkflowHealthBadge health={task.workflow_health} compact />
                          ) : null}
                        </div>
                      </td>
                      {/* Agent */}
                      <td className="w-32 max-w-[128px] px-3 py-[10px]">
                        {!isTerminal ? (
                          <AgentAssigneeDropdown
                            agents={projectAgentsData ?? []}
                            members={membersData}
                            className="max-w-full"
                            disabled={assignRole.isPending || removeRole.isPending}
                            placeholder="Assign"
                            value={assigneeValue}
                            onChange={(selection) => assignTask(task, selection)}
                          />
                        ) : coderAgentId ? (
                          <button
                            type="button"
                            className="flex min-w-0 cursor-pointer items-center gap-1.5 rounded transition-colors hover:text-foreground"
                            title={`${coderAgentName} · Click to filter`}
                            onClick={() => handleAgentClick(coderAgentId)}
                          >
                            <Avatar
                              name={coderAgentName ?? 'Agent'}
                              seed={coderAgentId}
                              size="xs"
                              className="shrink-0"
                            />
                            <span className="truncate text-xs font-medium">{coderAgentName}</span>
                          </button>
                        ) : coderIsHuman ? (
                          <div className="flex min-w-0 items-center gap-1.5">
                            <UserCircle size={14} />
                            <span className="truncate text-xs font-medium">Human</span>
                          </div>
                        ) : (
                          <span className="italic text-muted-foreground">—</span>
                        )}
                      </td>
                      {/* Updated */}
                      <td className="w-32 px-3 py-[10px] text-muted-foreground">
                        {formatDate(task.updated_at)}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {tasksQuery.hasNextPage ? (
        <div className="flex justify-center">
          <Button
            disabled={tasksQuery.isFetchingNextPage}
            variant="outline"
            onClick={() => void tasksQuery.fetchNextPage()}
          >
            {tasksQuery.isFetchingNextPage ? 'Loading...' : 'Load More'}
          </Button>
        </div>
      ) : null}
    </div>
  )
}
