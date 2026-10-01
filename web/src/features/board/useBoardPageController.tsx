import type { MouseEvent } from 'react'
import { useEffect, useMemo, useRef, useState } from 'react'
import { useNavigate, useSearch } from '@tanstack/react-router'
import {
  useAgentsQuery,
  useAssignRole,
  useCreateTask,
  useTransitionTask,
  useWorkflowQuery,
} from '@/api/hooks'
import type { TaskCardMenuRenderer } from '@/components/kanban-task-card'
import { taskStatusTransitions } from '@/components/task-controls'
import { DropdownMenuItem } from '@/components/ui/dropdown-menu'
import { toastApiError } from '@/lib/api-error'
import {
  type ColumnGroup,
  deriveColumns,
  getValidDropColumns,
  groupByColumns,
  matchesFilters,
} from '@/lib/workflow-utils'
import { useFilterStore } from '@/stores/filters'
import type { WorkflowDefinition } from '@/types/generated'
import type { TaskListItem as Task } from '@/types/task-list'
import type { BoardFilterPatch } from './BoardToolbar'
import { orderingEligibility } from './board-ordering'
import { useBoardData } from './useBoardData'
import { useBoardDragSession } from './useBoardDragSession'

const DEFAULT_COLUMNS: ColumnGroup[] = [
  {
    primaryState: 'todo',
    columnName: 'Todo',
    states: ['todo'],
    stateLabels: { todo: 'Todo' },
    subStates: [],
    taskCount: 0,
    isTerminal: false,
    kind: 'initial',
    dotColor: 'bg-stone-500',
    accentColor: 'border-l-stone-500',
  },
  {
    primaryState: 'in_progress',
    columnName: 'In Progress',
    states: ['in_progress'],
    stateLabels: { in_progress: 'In Progress' },
    subStates: [],
    taskCount: 0,
    isTerminal: false,
    kind: 'active',
    dotColor: 'bg-orange-500',
    accentColor: 'border-l-orange-500',
  },
  {
    primaryState: 'review',
    columnName: 'Review',
    states: ['review'],
    stateLabels: { review: 'Review' },
    subStates: [],
    taskCount: 0,
    isTerminal: false,
    kind: 'gate',
    dotColor: 'bg-violet-400',
    accentColor: 'border-l-violet-400',
  },
  {
    primaryState: 'done',
    columnName: 'Done',
    states: ['done'],
    stateLabels: { done: 'Done' },
    subStates: [],
    taskCount: 0,
    isTerminal: true,
    kind: 'terminal',
    dotColor: 'bg-stone-500',
    accentColor: 'border-l-stone-500',
  },
]

type BoardSearch = {
  agentIds?: string
  priorityMax?: number
  priorityMin?: number
  q?: string
  task?: string
  blockedOnly?: boolean
  includeCancelled?: boolean
  includeArchived?: boolean
}

export function useBoardPageController(projectId: string) {
  const navigate = useNavigate({ from: '/projects/$projectId/board' })
  const search = useSearch({ from: '/projects/$projectId/board' }) as BoardSearch
  const filterQ = useFilterStore((state) => state.q)
  const filterAgentIds = useFilterStore((state) => state.agentIds)
  const filterPriorityMin = useFilterStore((state) => state.priorityMin)
  const filterPriorityMax = useFilterStore((state) => state.priorityMax)
  const filterBlockedOnly = useFilterStore((state) => state.blockedOnly)
  const filterIncludeCancelled = useFilterStore((state) => state.includeCancelled)
  const filterIncludeArchived = useFilterStore((state) => state.includeArchived)
  const setFilters = useFilterStore((state) => state.setFilters)
  const [agentPickerTaskId, setAgentPickerTaskId] = useState<string>()
  const [quickCreateOpen, setQuickCreateOpen] = useState(false)
  const [quickCreateTitle, setQuickCreateTitle] = useState('')
  const [quickCreateDescription, setQuickCreateDescription] = useState('')
  const [createDialogOpen, setCreateDialogOpen] = useState(false)
  const [selectedTaskId, setSelectedTaskId] = useState<string>()
  const [showMobileFilters, setShowMobileFilters] = useState(false)
  const [contextMenu, setContextMenu] = useState<{ task: Task; x: number; y: number }>()
  const searchInputRef = useRef<HTMLInputElement>(null)
  const quickCreateDescriptionRef = useRef<HTMLTextAreaElement>(null)
  const agentUuids = filterAgentIds.filter((id) => id !== 'user')
  const {
    query: tasksQuery,
    snapshot,
    refresh,
  } = useBoardData(projectId, {
    q: filterQ || undefined,
    agent_id: agentUuids.length > 0 ? agentUuids.join(',') : undefined,
    assignee_type: filterAgentIds.includes('user') ? 'user' : undefined,
    include_cancelled: filterIncludeCancelled || undefined,
    include_archived: filterIncludeArchived || undefined,
    limit: 200,
  })
  const workflowQuery = useWorkflowQuery(projectId)
  const agentsQuery = useAgentsQuery()
  const createTask = useCreateTask(projectId)
  const transitionTask = useTransitionTask()
  const assignRole = useAssignRole()

  const initialColumns = useMemo(
    () => visibleColumns(workflowQuery.data, snapshot.tasks, filterIncludeCancelled),
    [filterIncludeCancelled, snapshot.tasks, workflowQuery.data],
  )
  const columnStates = useMemo(
    () => Object.fromEntries(initialColumns.map((column) => [column.primaryState, column.states])),
    [initialColumns],
  )
  const dragSession = useBoardDragSession({ snapshot, columnStates, refresh })
  const tasks = dragSession.state.rendered.tasks
  const boardColumns = useMemo(
    () => visibleColumns(workflowQuery.data, tasks, filterIncludeCancelled),
    [filterIncludeCancelled, tasks, workflowQuery.data],
  )
  const filteredTasks = useMemo(
    () =>
      tasks.filter((task) =>
        matchesFilters(task, {
          priorityMin: filterPriorityMin,
          priorityMax: filterPriorityMax,
          types: [],
          blockedOnly: filterBlockedOnly,
        }),
      ),
    [filterBlockedOnly, filterPriorityMax, filterPriorityMin, tasks],
  )
  const grouped = useMemo(
    () => groupByColumns(filteredTasks, boardColumns),
    [boardColumns, filteredTasks],
  )
  const hasSearchOrFilters =
    Boolean(filterQ) ||
    filterAgentIds.length > 0 ||
    filterPriorityMin !== undefined ||
    filterPriorityMax !== undefined ||
    filterBlockedOnly ||
    filterIncludeCancelled ||
    filterIncludeArchived
  const ordering = orderingEligibility({
    hasSearchOrFilters,
    snapshot: dragSession.state.rendered,
    workflowAvailable: Boolean(workflowQuery.data),
    committing: dragSession.movePending,
  })
  const draggingTask = dragSession.state.draggableId
    ? tasks.find((task) => task.id === dragSession.state.draggableId)
    : undefined
  const validDropStatuses = draggingTask
    ? validTargetColumns(draggingTask.status, workflowQuery.data, boardColumns)
    : []
  const agentNamesById = useMemo(
    () => new Map((agentsQuery.data?.items ?? []).map((agent) => [agent.id, agent.name])),
    [agentsQuery.data],
  )

  useEffect(() => {
    setFilters({
      agentIds: search.agentIds ? search.agentIds.split(',').filter(Boolean) : [],
      priorityMax: typeof search.priorityMax === 'number' ? search.priorityMax : undefined,
      priorityMin: typeof search.priorityMin === 'number' ? search.priorityMin : undefined,
      q: search.q ?? '',
      blockedOnly: search.blockedOnly === true,
      includeCancelled: search.includeCancelled === true,
      includeArchived: search.includeArchived === true,
    })
  }, [
    search.agentIds,
    search.blockedOnly,
    search.includeArchived,
    search.includeCancelled,
    search.priorityMax,
    search.priorityMin,
    search.q,
    setFilters,
  ])

  useEffect(() => setSelectedTaskId(search.task), [search.task])

  useEffect(() => {
    const openCreateDialog = () => setCreateDialogOpen(true)
    const focusSearch = () => {
      searchInputRef.current?.focus()
      searchInputRef.current?.select()
    }
    window.addEventListener('forge:create-task', openCreateDialog)
    window.addEventListener('forge:focus-board-search', focusSearch)
    return () => {
      window.removeEventListener('forge:create-task', openCreateDialog)
      window.removeEventListener('forge:focus-board-search', focusSearch)
    }
  }, [])

  useEffect(() => {
    if (!contextMenu) return
    const close = () => setContextMenu(undefined)
    document.addEventListener('click', close)
    window.addEventListener('scroll', close, true)
    return () => {
      document.removeEventListener('click', close)
      window.removeEventListener('scroll', close, true)
    }
  }, [contextMenu])

  const setUrlFilters = (patch: BoardFilterPatch & { task?: string }) => {
    const nextAgentIds = 'agentIds' in patch ? (patch.agentIds ?? []) : filterAgentIds
    const next = {
      priorityMax: filterPriorityMax,
      priorityMin: filterPriorityMin,
      q: filterQ,
      task: search.task,
      blockedOnly: filterBlockedOnly,
      includeCancelled: filterIncludeCancelled,
      includeArchived: filterIncludeArchived,
      ...patch,
      agentIds: nextAgentIds,
    }
    setFilters({
      agentIds: next.agentIds,
      priorityMax: next.priorityMax,
      priorityMin: next.priorityMin,
      q: next.q ?? '',
      blockedOnly: Boolean(next.blockedOnly),
      includeCancelled: Boolean(next.includeCancelled),
      includeArchived: Boolean(next.includeArchived),
    })
    void navigate({
      search: () => ({
        agentIds: next.agentIds.length > 0 ? next.agentIds.join(',') : undefined,
        priorityMax: next.priorityMax,
        priorityMin: next.priorityMin,
        q: next.q || undefined,
        task: next.task || undefined,
        blockedOnly: next.blockedOnly || undefined,
        includeCancelled: next.includeCancelled || undefined,
        includeArchived: next.includeArchived || undefined,
      }),
    })
  }

  const openTaskDetail = (taskId: string) => {
    setSelectedTaskId(taskId)
    setUrlFilters({ task: taskId })
  }
  const closeTaskDetail = () => {
    setSelectedTaskId(undefined)
    setUrlFilters({ task: undefined })
  }
  const handleAgentClick = (agentId: string) => {
    setUrlFilters({
      agentIds: filterAgentIds.includes(agentId)
        ? filterAgentIds.filter((id) => id !== agentId)
        : [...filterAgentIds, agentId],
    })
  }
  const transitionTaskFromMenu = (task: Task, status: string) => {
    transitionTask.mutate(
      {
        taskId: task.id,
        body: { status, version: task.version },
        currentStatus: task.status,
      },
      { onError: (error) => toastApiError(error, 'Transition failed') },
    )
  }
  const renderTaskMenuItems: TaskCardMenuRenderer = (task) => (
    <>
      <DropdownMenuItem onClick={() => transitionTaskFromMenu(task, 'cancelled')}>
        Cancel
      </DropdownMenuItem>
      <DropdownMenuItem
        disabled={task.status === 'done' || task.status === 'cancelled'}
        onClick={() => setAgentPickerTaskId(task.id)}
      >
        Assign Agent
      </DropdownMenuItem>
      <DropdownMenuItem onClick={() => openTaskDetail(task.id)}>View Detail</DropdownMenuItem>
    </>
  )
  const assignAgent = (task: Task, agentId: string) => {
    if (!agentId) return
    assignRole.mutate(
      {
        taskId: task.id,
        roleName: 'coder',
        body: { assignee_type: 'agent', assignee_id: agentId },
      },
      {
        onError: (error) => toastApiError(error, 'Agent assignment failed'),
        onSuccess: () => setAgentPickerTaskId(undefined),
      },
    )
  }
  const submitQuickCreate = () => {
    const title = quickCreateTitle.trim()
    const description = quickCreateDescription.trim()
    if (!title || !description || createTask.isPending) return
    createTask.mutate(
      { title, description, task_type: 'task', priority: 0 },
      {
        onError: (error) => toastApiError(error, 'Task creation failed'),
        onSuccess: () => {
          setQuickCreateTitle('')
          setQuickCreateDescription('')
          setQuickCreateOpen(false)
        },
      },
    )
  }
  const cancelQuickCreate = () => {
    setQuickCreateTitle('')
    setQuickCreateDescription('')
    setQuickCreateOpen(false)
  }
  const handleLoadMore = () => {
    if (tasksQuery.hasNextPage && !tasksQuery.isFetchingNextPage) {
      void tasksQuery.fetchNextPage()
    }
  }
  const openContextMenu = (event: MouseEvent<HTMLElement>, task: Task) => {
    event.preventDefault()
    setContextMenu({ task, x: event.clientX, y: event.clientY })
  }

  return {
    agentsQuery,
    agentNamesById,
    agentPickerTaskId,
    assignAgent,
    assignRole,
    boardColumns,
    cancelQuickCreate,
    closeTaskDetail,
    contextMenu,
    createDialogOpen,
    createTask,
    dragSession,
    filterAgentIds,
    filterBlockedOnly,
    filterIncludeArchived,
    filterIncludeCancelled,
    filterPriorityMax,
    filterPriorityMin,
    filterQ,
    grouped,
    handleAgentClick,
    handleLoadMore,
    openContextMenu,
    openTaskDetail,
    ordering,
    quickCreateDescription,
    quickCreateDescriptionRef,
    quickCreateOpen,
    quickCreateTitle,
    renderTaskMenuItems,
    searchInputRef,
    selectedTaskId,
    setAgentPickerTaskId,
    setContextMenu,
    setCreateDialogOpen,
    setQuickCreateDescription,
    setQuickCreateOpen,
    setQuickCreateTitle,
    setShowMobileFilters,
    setUrlFilters,
    showMobileFilters,
    submitQuickCreate,
    tasks,
    tasksQuery,
    transitionTaskFromMenu,
    validDropStatuses,
  }
}

function visibleColumns(
  workflow: WorkflowDefinition | undefined,
  tasks: Task[],
  includeCancelled: boolean,
): ColumnGroup[] {
  const columns = workflow ? deriveColumns(workflow, tasks) : DEFAULT_COLUMNS
  const visible = columns.filter((column) => {
    if (['merging', 'merge_failed'].includes(column.primaryState)) return false
    if (column.primaryState === 'cancelled' && !includeCancelled) return false
    return true
  })
  if (!includeCancelled || visible.some((column) => column.primaryState === 'cancelled')) {
    return visible
  }
  const stripped = visible.map((column) => {
    if (!column.states.includes('cancelled')) return column
    const states = column.states.filter((state) => state !== 'cancelled')
    const stateSet = new Set(states)
    return {
      ...column,
      states,
      taskCount: tasks.filter((task) => stateSet.has(task.status)).length,
    }
  })
  stripped.push({
    primaryState: 'cancelled',
    columnName: 'Cancelled',
    states: ['cancelled'],
    stateLabels: { cancelled: 'Cancelled' },
    subStates: [],
    taskCount: tasks.filter((task) => task.status === 'cancelled').length,
    isTerminal: true,
    kind: 'terminal',
    dotColor: 'bg-zinc-400',
    accentColor: 'border-l-zinc-400',
  })
  return stripped
}

function validTargetColumns(
  status: string,
  workflow: WorkflowDefinition | undefined,
  columns: ColumnGroup[],
): string[] {
  if (workflow) return getValidDropColumns(workflow, status)
  return (taskStatusTransitions[status] ?? []).filter((target) =>
    columns.some((column) => column.primaryState === target),
  )
}
