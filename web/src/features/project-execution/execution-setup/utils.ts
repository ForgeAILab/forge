// Execution setup action selection, formatting, and error utilities.

import { getApiErrorCode, isApiStatus } from '@/lib/api-error'
import type { ProjectExecutionSetupResponse, RetryAction } from '@/types/generated'

const SETUP_ACTIONS = new Set<RetryAction>([
  'refresh_and_retry',
  'select_worker',
  'select_independent_reviewer',
  'attach_repository',
  'retry_provisioning',
])

export type SetupAction =
  | 'refresh_and_retry'
  | 'select_worker'
  | 'select_independent_reviewer'
  | 'attach_repository'
  | 'retry_provisioning'

function isSetupAction(action: RetryAction | null | undefined): action is SetupAction {
  return action !== null && action !== undefined && SETUP_ACTIONS.has(action)
}

export function asNumber(value: number | bigint): number {
  return typeof value === 'bigint' ? Number(value) : value
}

export function humanize(value: string): string {
  return value.replaceAll('_', ' ').replace(/\b\w/g, (letter) => letter.toUpperCase())
}

export function bounded(value: string | null | undefined, fallback: string): string {
  const trimmed = value?.trim()
  if (!trimmed) return fallback
  return trimmed.length > 240 ? `${trimmed.slice(0, 237)}…` : trimmed
}

export function formatTimestamp(value: string | null | undefined): string | null {
  if (!value) return null
  const date = new Date(value)
  return Number.isNaN(date.getTime())
    ? value
    : date.toLocaleString([], { dateStyle: 'medium', timeStyle: 'short' })
}

export function isConflictError(error: unknown): boolean {
  return (
    isApiStatus(error, 409) ||
    isApiStatus(error, 412) ||
    ['version_conflict', 'digest_conflict'].includes(getApiErrorCode(error) ?? '')
  )
}

export function createIdempotencyKey(action: string, projectId: string): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID()
  }
  return `${action}:${projectId}:${Date.now()}:${Math.random().toString(36).slice(2)}`
}

export function nextSetupAction(data: ProjectExecutionSetupResponse): SetupAction | null {
  const availability = data.availability
  const refreshRequired =
    data.coordination_state === 'unavailable' ||
    data.execution_setup_state === 'unavailable' ||
    data.execution_gate === 'unavailable' ||
    [availability?.coordination, availability?.execution_setup, availability?.execution_gate].some(
      (status) => status?.availability !== undefined && status.availability !== 'current',
    )
  if (refreshRequired) return 'refresh_and_retry'
  if (isSetupAction(data.next_action)) return data.next_action
  const requirementAction = data.setup_requirements.find((requirement) =>
    isSetupAction(requirement.action),
  )?.action
  return isSetupAction(requirementAction) ? requirementAction : null
}

export function stateLabel(value: string): string {
  return humanize(value)
}

export function stateDescription(kind: 'coordination' | 'setup' | 'gate', value: string): string {
  if (kind === 'coordination') {
    if (value === 'ready') return 'Project Agent Chat is authorized and ready for planning turns.'
    if (value === 'setup_required') {
      return 'Project Agent Chat still needs its authorized binding; execution setup is shown separately.'
    }
    return 'Project Agent Chat is unavailable; Forge will not admit a turn.'
  }
  if (kind === 'setup') {
    if (value === 'ready') return 'The Project’s required execution setup is satisfied.'
    if (value === 'provisioning') {
      return 'Repository-backed execution is provisioning. No operational success is claimed yet.'
    }
    if (value === 'failed') {
      return 'Provisioning stopped; the saved failure remains visible until a bounded retry succeeds.'
    }
    if (value === 'unavailable') {
      return 'Forge could not verify execution setup. Refresh the readiness projection before acting.'
    }
    return 'The Project still needs a primary repository before repository-backed Tasks can run.'
  }
  if (value === 'unavailable') {
    return 'Forge could not verify the execution gate. Refresh the readiness projection before acting.'
  }
  if (value === 'active') return 'The approved Charter allows the Task workflow to run.'
  if (value === 'baseline_approval_required') {
    return 'This legacy planning status no longer blocks implementation; refresh Project state.'
  }
  if (value === 'reconciliation_required') {
    return 'Execution is paused until the recorded reconciliation requirement is resolved.'
  }
  return 'This legacy planning status no longer blocks implementation; refresh Project state.'
}

export function compactStateDescription(kind: 'coordination' | 'setup' | 'gate', value: string): string {
  if (kind === 'coordination' && value === 'ready') {
    return 'The Project Agent can plan and coordinate this Project.'
  }
  if (kind === 'setup' && value === 'ready') {
    return 'The repository is ready; Project defaults and Task assignments remain editable.'
  }
  if (kind === 'gate') {
    if (value === 'active') {
      return 'The Task workflow can run without another implementation approval.'
    }
    if (value === 'baseline_approval_required') {
      return 'Refresh this legacy planning status; it no longer blocks Tasks.'
    }
    if (value === 'pre_baseline_read_only') {
      return 'Refresh this legacy planning status; it no longer blocks Tasks.'
    }
    if (value === 'reconciliation_required') {
      return 'Review plan changes before repository work resumes.'
    }
  }
  return stateDescription(kind, value)
}
