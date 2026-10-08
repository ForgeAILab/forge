import type { TaskListItem as Task } from '@/types/generated'
import type {
  StateKind,
  TaskBlockingAnnotation,
  TaskExecutionObservability,
  TaskResponse,
  TaskType,
  WorkflowDefinition,
  WorkflowTrigger,
} from '@/types/generated'

export interface BoardColumn {
  primaryState: string
  columnName: string
  states: string[]
  stateLabels?: Record<string, string>
  isTerminal: boolean
  kind: StateKind | null
  dotColor: string
  accentColor: string
}

export interface ColumnGroup extends BoardColumn {
  subStates: string[]
  taskCount: number
}

export const taskTypes: TaskType[] = ['task', 'planning_task', 'discovery', 'sub_task']

export type BoardSearchTypes = string[] | string | undefined

export function normalizeTypes(types: BoardSearchTypes): TaskType[] {
  const values = Array.isArray(types) ? types : types ? [types] : []
  return values.filter((type): type is TaskType => taskTypes.includes(type as TaskType))
}

export function groupByColumns(tasks: Task[], cols: BoardColumn[]): Record<string, Task[]> {
  const result: Record<string, Task[]> = {}
  for (const col of cols) {
    result[col.primaryState] = tasks.filter((t) => col.states.includes(t.status))
  }
  return result
}

type TaskAnnotationSummary = Pick<TaskResponse, 'condition'> & {
  execution_observability?: Pick<TaskExecutionObservability, 'latest_execution_id'>
}

export function taskHasError(task: TaskAnnotationSummary): boolean {
  if (task.condition.details.failed || task.condition.details.blocked) return true
  if (!task.condition.details.diagnostic) return false
  return !isStaleBlockingAnnotation(task)
}

export function getTaskWorkflowWarning(
  task: TaskResponse,
): { title: string; message: string } | null {
  const remaining = task.plan_progress?.remaining ?? 0
  const latest = task.execution_observability
  if (
    task.status !== 'in_progress' ||
    remaining <= 0 ||
    latest?.active_execution_id ||
    latest?.latest_execution_status !== 'completed'
  ) {
    return null
  }

  const itemLabel = remaining === 1 ? 'item is' : 'items are'
  return {
    title: 'Plan checklist still open',
    message: `${remaining} checklist ${itemLabel} unchecked. The latest execution completed, but the task will stay in progress until the checklist is complete.`,
  }
}

/** The Task's blocked record. A failure record alone is not a block. */
export function blockedInterruption(task: TaskAnnotationSummary) {
  return task.condition.details.blocked ? task.condition.details.interruption : null
}

export function isTaskBlocked(task: TaskAnnotationSummary): boolean {
  if (task.condition.details.blocked) return true
  return Boolean(getBlockingAnnotation(task))
}

function rawBlockingAnnotation(task: TaskAnnotationSummary): TaskBlockingAnnotation | null {
  return task.condition.details.diagnostic
}

export function isStaleBlockingAnnotation(task: TaskAnnotationSummary): boolean {
  const annotation = rawBlockingAnnotation(task)
  if (!annotation?.blocked_execution_id) return false
  const latestExecutionId = task.execution_observability?.latest_execution_id
  return Boolean(latestExecutionId && latestExecutionId !== annotation.blocked_execution_id)
}

export function getStaleBlockingAnnotation(task: TaskAnnotationSummary): TaskBlockingAnnotation | null {
  const annotation = rawBlockingAnnotation(task)
  return annotation && isStaleBlockingAnnotation(task) ? annotation : null
}

export function getBlockingAnnotation(task: TaskAnnotationSummary): TaskBlockingAnnotation | null {
  const annotation = rawBlockingAnnotation(task)
  return annotation && !isStaleBlockingAnnotation(task) ? annotation : null
}

export function matchesFilters(
  task: Task,
  filters: { priorityMax?: number; priorityMin?: number; types: string[]; blockedOnly?: boolean },
): boolean {
  if (filters.blockedOnly && !task.condition.details.blocked) return false
  if (filters.priorityMin !== undefined && task.priority < filters.priorityMin) return false
  if (filters.priorityMax !== undefined && task.priority > filters.priorityMax) return false
  if (filters.types.length > 0 && !filters.types.includes(task.task_type)) return false
  return true
}

export function patchTaskIntoStatus(
  tasks: Task[],
  task: Task,
  toStatus: string,
  beforeTaskId?: string,
): Task[] {
  const updatedTask = { ...task, status: toStatus }
  const withoutTask = tasks.filter((candidate) => candidate.id !== task.id)
  if (!beforeTaskId) return [...withoutTask, updatedTask]
  const beforeIndex = withoutTask.findIndex((candidate) => candidate.id === beforeTaskId)
  if (beforeIndex === -1) return [...withoutTask, updatedTask]
  return [...withoutTask.slice(0, beforeIndex), updatedTask, ...withoutTask.slice(beforeIndex)]
}

export function isInitialKind(kind: StateKind | null | undefined): boolean {
  return kind === 'initial'
}

const KIND_COLORS: Record<StateKind, { dot: string; accent: string }> = {
  backlog: { dot: 'bg-stone-400', accent: 'border-l-stone-400' },
  initial: { dot: 'bg-stone-500', accent: 'border-l-stone-500' },
  active: { dot: 'bg-orange-500', accent: 'border-l-orange-500' },
  gate: { dot: 'bg-violet-400', accent: 'border-l-violet-400' },
  terminal: { dot: 'bg-stone-500', accent: 'border-l-stone-500' },
  custom: { dot: 'bg-red-500', accent: 'border-l-red-500' },
}

// Per-name overrides for known terminal sub-types
const NAME_DOT_OVERRIDES: Record<string, string> = {
  cancelled: 'bg-zinc-400',
  merge_failed: 'bg-red-500',
}

export function deriveColumns(workflow: WorkflowDefinition, tasks: Task[] = []): ColumnGroup[] {
  const seen = new Set<string>()
  const columnOrder: string[] = []
  const columnStates: Record<string, string[]> = {}

  for (const state of workflow.states) {
    if (!seen.has(state.column)) {
      seen.add(state.column)
      columnOrder.push(state.column)
      columnStates[state.column] = []
    }
    columnStates[state.column].push(state.name)
  }

  return columnOrder.map((col) => {
    const states = columnStates[col]
    const columnTasks = tasks.filter((task) => states.includes(task.status))
    const primaryState = primaryStateForColumn(workflow, col, states)
    const primaryDef = workflow.states.find((s) => s.name === primaryState)
    const stateLabels = Object.fromEntries(
      states.map((stateName) => {
        const state = workflow.states.find((candidate) => candidate.name === stateName)
        return [stateName, state?.display_name ?? formatStateLabel(stateName)]
      }),
    )
    const kind = primaryDef?.kind ?? null
    const kindColors = kind
      ? KIND_COLORS[kind]
      : { dot: 'bg-purple-500', accent: 'border-l-purple-500' }
    const dot = NAME_DOT_OVERRIDES[primaryState] ?? kindColors.dot
    const isTerminal = states.every(
      (s) => workflow.states.find((sd) => sd.name === s)?.kind === 'terminal',
    )
    return {
      primaryState,
      columnName: col,
      states,
      stateLabels,
      subStates: Array.from(new Set(columnTasks.map((task) => task.status))),
      taskCount: columnTasks.length,
      isTerminal,
      kind,
      dotColor: dot,
      accentColor: kindColors.accent,
    }
  })
}

function primaryStateForColumn(
  workflow: WorkflowDefinition,
  columnName: string,
  states: string[],
): string {
  const normalizedColumn = normalizeWorkflowLabel(columnName)
  return (
    states.find((stateName) => {
      const state = workflow.states.find((candidate) => candidate.name === stateName)
      return (
        normalizeWorkflowLabel(state?.display_name ?? '') === normalizedColumn ||
        normalizeWorkflowLabel(stateName) === normalizedColumn
      )
    }) ?? states[0]
  )
}

function normalizeWorkflowLabel(value: string): string {
  return value.replace(/_/g, ' ').trim().toLowerCase()
}

export function formatStateLabel(value: string): string {
  return value.replace(/_/g, ' ').replace(/\b\w/g, (char) => char.toUpperCase())
}

export type WorkflowEdge = {
  from: string
  to: string
  trigger: WorkflowTrigger
}

export function outgoingWorkflowEdges(
  workflow: WorkflowDefinition,
  fromState: string,
): WorkflowEdge[] {
  const state = workflow.states.find((candidate) => candidate.name === fromState)
  if (!state) return []
  const edges = Object.entries(state.triggers ?? {})
    .filter(
      (entry): entry is [WorkflowTrigger, NonNullable<(typeof state.triggers)[WorkflowTrigger]>] =>
        typeof entry[1]?.to === 'string',
    )
    .map(([trigger, definition]) => ({
      from: fromState,
      to: definition.to,
      trigger,
    }))
  if (
    !Object.prototype.hasOwnProperty.call(state.triggers ?? {}, 'accept') &&
    state.kind !== 'terminal'
  ) {
    const stateIndex = workflow.states.findIndex((candidate) => candidate.name === fromState)
    const nextState = stateIndex >= 0 ? workflow.states[stateIndex + 1] : undefined
    if (nextState) {
      edges.push({ from: fromState, to: nextState.name, trigger: 'accept' })
    }
  }
  return edges
}

export function workflowTriggerTargets(workflow: WorkflowDefinition, fromState: string): string[] {
  return outgoingWorkflowEdges(workflow, fromState).map((edge) => edge.to)
}

export function getValidDropColumns(workflow: WorkflowDefinition, fromState: string): string[] {
  const userAgentTargets = workflowTriggerTargets(workflow, fromState)

  const columns = deriveColumns(workflow)
  const result = new Set<string>()
  for (const target of userAgentTargets) {
    const col = columns.find((c) => c.states.includes(target))
    if (col) result.add(col.primaryState)
  }
  return Array.from(result)
}

export function getStateColors(
  stateName: string,
  kind?: StateKind | null,
): { dot: string; bg: string; text: string; accent: string } {
  const HARDCODED: Record<string, { dot: string; bg: string; text: string; accent: string }> = {
    todo: {
      dot: 'bg-stone-500',
      bg: 'bg-stone-100 dark:bg-stone-800/60',
      text: 'text-stone-600 dark:text-stone-400',
      accent: 'border-l-stone-500',
    },
    backlog: {
      dot: 'bg-stone-400',
      bg: 'bg-stone-50 dark:bg-stone-900/60',
      text: 'text-stone-500 dark:text-stone-400',
      accent: 'border-l-stone-400',
    },
    planning: {
      dot: 'bg-orange-400',
      bg: 'bg-orange-50 dark:bg-orange-500/10',
      text: 'text-orange-700 dark:text-orange-300',
      accent: 'border-l-orange-400',
    },
    in_progress: {
      dot: 'bg-orange-500',
      bg: 'bg-orange-50 dark:bg-orange-500/10',
      text: 'text-orange-700 dark:text-orange-300',
      accent: 'border-l-orange-500',
    },
    review: {
      dot: 'bg-violet-400',
      bg: 'bg-violet-50 dark:bg-violet-500/10',
      text: 'text-violet-700 dark:text-violet-300',
      accent: 'border-l-violet-400',
    },
    merging: {
      dot: 'bg-amber-400',
      bg: 'bg-amber-50 dark:bg-amber-500/10',
      text: 'text-amber-700 dark:text-amber-200',
      accent: 'border-l-amber-400',
    },
    merge_failed: {
      dot: 'bg-red-500',
      bg: 'bg-red-50 dark:bg-red-500/10',
      text: 'text-red-700 dark:text-red-300',
      accent: 'border-l-red-500',
    },
    blocked: {
      dot: 'bg-red-500',
      bg: 'bg-red-50 dark:bg-red-500/10',
      text: 'text-red-700 dark:text-red-300',
      accent: 'border-l-red-500',
    },
    done: {
      dot: 'bg-stone-500',
      bg: 'bg-stone-100 dark:bg-stone-800/60',
      text: 'text-stone-600 dark:text-stone-500',
      accent: 'border-l-stone-500',
    },
    cancelled: {
      dot: 'bg-stone-400',
      bg: 'bg-stone-100 dark:bg-stone-800/40',
      text: 'text-stone-500 dark:text-stone-500',
      accent: 'border-l-stone-400',
    },
  }

  if (HARDCODED[stateName]) return HARDCODED[stateName]

  if (kind) {
    const kindMap: Record<StateKind, { dot: string; bg: string; text: string; accent: string }> = {
      backlog: {
        dot: 'bg-stone-400',
        bg: 'bg-stone-50 dark:bg-stone-900/60',
        text: 'text-stone-500 dark:text-stone-400',
        accent: 'border-l-stone-400',
      },
      initial: {
        dot: 'bg-stone-500',
        bg: 'bg-stone-100 dark:bg-stone-800/60',
        text: 'text-stone-600 dark:text-stone-400',
        accent: 'border-l-stone-500',
      },
      active: {
        dot: 'bg-orange-500',
        bg: 'bg-orange-50 dark:bg-orange-500/10',
        text: 'text-orange-700 dark:text-orange-300',
        accent: 'border-l-orange-500',
      },
      gate: {
        dot: 'bg-violet-400',
        bg: 'bg-violet-50 dark:bg-violet-500/10',
        text: 'text-violet-700 dark:text-violet-300',
        accent: 'border-l-violet-400',
      },
      terminal: {
        dot: 'bg-stone-500',
        bg: 'bg-stone-100 dark:bg-stone-800/60',
        text: 'text-stone-600 dark:text-stone-500',
        accent: 'border-l-stone-500',
      },
      custom: {
        dot: 'bg-red-500',
        bg: 'bg-red-50 dark:bg-red-500/10',
        text: 'text-red-700 dark:text-red-300',
        accent: 'border-l-red-500',
      },
    }
    return kindMap[kind]
  }

  return {
    dot: 'bg-purple-500',
    bg: 'bg-purple-50 dark:bg-purple-950',
    text: 'text-purple-700 dark:text-purple-300',
    accent: 'border-l-purple-500',
  }
}

export function formatStateName(name: string): string {
  return name.replace(/_/g, ' ')
}

const ACTIVE_STATES = new Set(['in_progress', 'planning'])
const TERMINAL_STATES = new Set(['done', 'cancelled'])

export function isActiveStatus(status: string): boolean {
  return ACTIVE_STATES.has(status)
}

export function isTerminalStatus(status: string): boolean {
  return TERMINAL_STATES.has(status)
}
