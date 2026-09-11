import type { QueryClient } from '@tanstack/react-query'
import { qk } from '@/api/query-keys'
import { getApiErrorMessage } from '@/lib/api-error'
import type {
  Project,
  Review,
  Task,
  UpdateTaskRequest,
} from '@/types/generated'
import type { TaskMetadata } from '@/types/generated'
import type { AssigneeSelection } from '@/components/task-controls'

export type ParsedDiffFile = {
  path: string
  oldPath?: string
  newPath?: string
  hunks: string[]
}

export type TaskWithStateConfig = Task & {
  task_state_config?:
    | (TaskMetadata & Record<string, unknown>)
    | Record<string, unknown>
    | string
    | null
}

export type UpdateTaskRequestWithStateConfig = UpdateTaskRequest & {
  task_state_config: Record<string, unknown>
}

export const diffStatusStyles: Record<string, { label: string; className: string }> = {
  added: { label: 'A', className: 'bg-emerald-100 text-emerald-800' },
  modified: { label: 'M', className: 'bg-amber-100 text-amber-800' },
  deleted: { label: 'D', className: 'bg-red-100 text-red-800' },
  renamed: { label: 'R', className: 'bg-blue-100 text-blue-800' },
}

export function extractRunSuffix(title: string): string {
  const parts = title.split(' ')
  if (parts.length < 2) return ''
  const last = parts[parts.length - 1]
  return /^[a-z]+-\d{10,}-[a-z0-9]+$/.test(last) ? last : ''
}

export function stripRunSuffix(name: string, suffix: string): string {
  if (!suffix) return name
  const token = ` ${suffix}`
  return name.endsWith(token) ? name.slice(0, -token.length) : name
}

export function formatDate(value?: string | null): string {
  if (!value) return '-'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return date.toLocaleString()
}

export function getLatestReview(reviews: Review[]): Review | undefined {
  return reviews.reduce<Review | undefined>((latest, current) => {
    if (!latest) return current
    return current.attempt_number > latest.attempt_number ? current : latest
  }, undefined)
}

export function getErrorInfo(
  task: Task,
): { tone: 'timeout' | 'crash' | 'workspace'; message: string } | undefined {
  const annotation = task.error_annotation as Record<string, unknown> | null | undefined
  if (!annotation) return undefined
  // Annotations with blocking_reason are rendered by TaskBlockingBanner; skip here to avoid duplication.
  if (typeof annotation.blocking_reason === 'string' && annotation.blocking_reason) return undefined
  const type = typeof annotation.type === 'string' ? annotation.type : undefined
  if (type === 'workspace_reset_required' || type === 'workspace_error') {
    const message = typeof annotation.message === 'string' ? annotation.message : 'Workspace is unavailable'
    return { tone: 'workspace', message }
  }
  const kind = typeof annotation.kind === 'string' ? annotation.kind : undefined
  const message = typeof annotation.message === 'string' ? annotation.message : undefined
  if (!kind && !message) return undefined
  const tone = kind === 'crash' ? 'crash' : 'timeout'
  return { tone, message: message ?? (tone === 'crash' ? 'Task crashed' : 'Task timed out') }
}

export function getTaskDetailApiErrorMessage(error: unknown, fallback = 'Request failed'): string {
  return getApiErrorMessage(error, fallback)
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

export function isProject(value: unknown): value is Project {
  return isRecord(value) && typeof value.id === 'string' && isRecord(value.settings)
}

export function readTaskStateConfig(task: Task | undefined): Record<string, unknown> {
  const raw = (task as TaskWithStateConfig | undefined)?.task_state_config
  if (typeof raw === 'string') {
    try {
      const parsed: unknown = JSON.parse(raw)
      return isRecord(parsed) ? parsed : {}
    } catch {
      return {}
    }
  }
  return isRecord(raw) ? raw : {}
}

export function budgetValue(value: unknown): number | undefined {
  return typeof value === 'number' && Number.isInteger(value) && value >= 0 ? value : undefined
}

export function retryBudgetFromConfig(
  config: Record<string, unknown>,
  key: 'review' | 'merge_fix' | 'execution',
): string {
  const budgets = isRecord(config.retry_budgets) ? config.retry_budgets : undefined
  const value = budgetValue(budgets?.[key])
  return value === undefined ? '' : String(value)
}

function projectItems(value: unknown): Project[] {
  if (Array.isArray(value)) return value.filter(isProject)
  if (isRecord(value) && Array.isArray(value.items)) {
    return value.items.filter(isProject)
  }
  return []
}

export function getCachedProject(queryClient: QueryClient, projectId: string): Project | undefined {
  const direct = queryClient.getQueryData<Project>(qk.project(projectId))
  if (direct) return direct
  for (const [, data] of queryClient.getQueriesData({ queryKey: qk.projects })) {
    const project = projectItems(data).find((item) => item.id === projectId)
    if (project) return project
  }
  return undefined
}

export function assignmentSelection(
  assignment?: Task['role_assignments'][number],
): AssigneeSelection {
  if (!assignment) return { type: 'unassigned' }
  if (assignment.assignee_type === 'agent' && assignment.assignee_id) {
    return { type: 'agent', agentId: assignment.assignee_id }
  }
  if (assignment.assignee_type === 'user') return { type: 'user', userId: assignment.assignee_id ?? 'manual' }
  return { type: 'unassigned' }
}

export function assignmentAgentName(
  assignment: Task['role_assignments'][number] | undefined,
  agentName: (agentId?: string | null) => string | undefined,
): string | undefined {
  return assignment?.assignee_type === 'agent' && assignment.assignee_id
    ? (agentName(assignment.assignee_id) ?? assignment.assignee_id)
    : undefined
}

export function splitDiffIntoFiles(rawDiff: string): ParsedDiffFile[] {
  if (!rawDiff.trim()) return []
  const chunks = rawDiff.split(/^diff --git /m)
  const sections = chunks.slice(1).map((chunk) => `diff --git ${chunk}`)
  const parsed = sections
    .map((section): ParsedDiffFile | undefined => {
      const lines = section.split('\n')
      const oldPath = lines
        .find((line) => line.startsWith('--- '))
        ?.replace(/^---\s+/, '')
        .replace(/^a\//, '')
      const newPath = lines
        .find((line) => line.startsWith('+++ '))
        ?.replace(/^\+\+\+\s+/, '')
        .replace(/^b\//, '')
      const path = newPath && newPath !== '/dev/null' ? newPath : oldPath
      if (!path) return undefined

      if (!lines.some((line) => line.startsWith('@@ '))) return undefined
      return { path, oldPath, newPath, hunks: [section] }
    })
    .filter((file): file is ParsedDiffFile => Boolean(file))
  return parsed
}
