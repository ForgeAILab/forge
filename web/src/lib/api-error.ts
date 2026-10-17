import { toast } from 'sonner'
import { ApiError } from '@/api/client'

export function getApiErrorMessage(error: unknown, fallback = 'Request failed'): string {
  if (error instanceof ApiError) {
    let message = error.message || fallback
    let requestId = error.requestId
    if (error.response?.message) message = error.response.message
    if (error.response?.request_id) requestId = error.response.request_id
    try {
      const parsed = JSON.parse(error.message) as {
        message?: unknown
        request_id?: unknown
      }
      if (typeof parsed.message === 'string' && parsed.message) {
        message = parsed.message
      }
      if (typeof parsed.request_id === 'string' && parsed.request_id) {
        requestId = parsed.request_id
      }
    } catch {
      // Non-JSON error bodies are already usable as-is.
    }
    const leadRefusal = getApiConflictDetails(error)?.refusal
    if (getApiErrorCode(error) === 'LEAD_OPERATION_REFUSED' && typeof leadRefusal === 'string') {
      message = leadRefusalMessages[leadRefusal] ?? message
    }
    const placementRejections = getPlacementRejectionMessage(error)
    return `${message}${placementRejections ? ` ${placementRejections}` : ''}${requestId ? ` Request ID: ${requestId}` : ''}`
  }
  if (error instanceof Error) return error.message
  return fallback
}

/** Public LeadRefusal::code() values in crates/services/src/lib.rs. */
const leadRefusalMessages: Record<string, string> = {
  outside_lead_subtree: 'The lead can act only on its root task and that root’s subtasks.',
  not_leading: 'This Agent is no longer running as the root’s lead.',
  wrong_target:
    'This lead operation targets the root’s planning run or one of its subtasks. Choose the required task.',
  subtask_started:
    'This subtask has started. The lead can no longer edit, cancel or reorder it through the breakdown operation.',
  lead_limit_reached:
    'The lead has reached the limit for this operation. The owner can review the root’s budget and recovery options.',
  action_not_offered:
    'This recovery action is not currently offered to the lead. Refresh the subtask’s actions.',
  lead_input_pending:
    'The lead is already waiting for an answer. Answer its open question in Mission Control.',
  lead_self_assignment: 'The lead cannot assign itself as a worker. Choose another Agent.',
}

export function getApiErrorCode(error: unknown): string | undefined {
  if (!(error instanceof ApiError)) return undefined
  if (error.code) return error.code
  try {
    const parsed = JSON.parse(error.message) as { code?: unknown }
    return typeof parsed.code === 'string' ? parsed.code : undefined
  } catch {
    return undefined
  }
}

export type ApiConflictDetails = Record<string, unknown>

function isRecord(value: unknown): value is ApiConflictDetails {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

/** Return server-provided conflict metadata without exposing arbitrary error bodies in the UI. */
export function getApiConflictDetails(error: unknown): ApiConflictDetails | undefined {
  if (!(error instanceof ApiError)) return undefined
  if (isRecord(error.details)) return error.details
  try {
    const parsed = JSON.parse(error.message) as { details?: unknown }
    return isRecord(parsed.details) ? parsed.details : undefined
  } catch {
    return undefined
  }
}

const placementFilterMessages: Record<string, string> = {
  owner_unreachable: 'Owner is offline or unreachable',
  daemon_upgrade_required:
    'Upgrade the daemon to the server release (protocol revision 7 or newer)',
  workspace_protocol_missing: 'Daemon lacks workspace protocol support',
  location_not_ready: 'Repository location is not ready',
  executor_unavailable: 'Executor is not installed, authenticated, or enabled',
  capability_missing: 'Executor lacks required capabilities',
  pin_mismatch: 'Location does not match the Agent’s pinned daemon',
  agent_capacity: 'Agent has no available capacity',
  machine_capacity: 'Machine has no available run capacity',
  native_backend_unsupported: 'Daemon workspaces require CLI Agents for all worktree roles',
  run_purpose_denied: 'Daemon policy denies a required hook, CI step, or environment setup',
  not_visible: 'Owner is not accessible to the Task owner',
  environment_not_ready: 'Project environment checks failed on this machine',
  environment_probe_pending: 'Project environment checks are pending on this machine',
  provision_failed:
    'Repository provisioning exhausted its retries; reconnect the machine or update Project placement settings to retry',
  environment_unverified:
    'This machine has no copy of the repository and no machine-scope checks can verify it before cloning',
}

function getPlacementRejectionMessage(error: ApiError): string | undefined {
  if (!['placement_unavailable', 'daemon_upgrade_required'].includes(getApiErrorCode(error) ?? ''))
    return undefined
  const candidates = getApiConflictDetails(error)?.rejected_candidates
  if (!Array.isArray(candidates)) return undefined
  if (candidates.length === 0) return 'No eligible repository locations.'

  return candidates
    .filter(isRecord)
    .map((candidate) => {
      const owner =
        candidate.owner_kind === 'server'
          ? typeof candidate.daemon_id === 'string'
            ? `Server via daemon ${candidate.daemon_id}`
            : 'Server'
          : typeof candidate.daemon_id === 'string'
            ? `Daemon ${candidate.daemon_id}`
            : 'Daemon'
      const location =
        typeof candidate.repo_location_id === 'string'
          ? `, location ${candidate.repo_location_id}`
          : ''
      const runtime =
        typeof candidate.runtime_id === 'string' ? `, runtime ${candidate.runtime_id}` : ''
      const codes = Array.isArray(candidate.filter_codes)
        ? candidate.filter_codes.filter((code): code is string => typeof code === 'string')
        : []
      const reasons = codes.map(
        (code) => `${placementFilterMessages[code] ?? code.replaceAll('_', ' ')} (${code})`,
      )
      return `${owner}${location}${runtime}: ${reasons.join('; ') || 'No rejection reason provided'}.`
    })
    .join(' ')
}

export const TASK_BUSY_QUEUED_MESSAGE = 'Queued; it will apply after the current step'

/**
 * `task_busy` (409) means the server accepted the request and keeps it queued
 * behind the Task's running step. It is not a failure and not a changed offer.
 */
export function isTaskBusy(error: unknown): boolean {
  return getApiErrorCode(error) === 'task_busy'
}

/** The server's retry hint for a `task_busy` response, in milliseconds. */
export function taskBusyRetryAfterMs(error: unknown): number {
  const retry = getApiConflictDetails(error)?.retry_after_ms
  return typeof retry === 'number' && retry > 0 ? retry : 0
}

/** Show the queued notice for `task_busy`; returns whether it handled the error. */
export function notifyTaskBusy(error: unknown): boolean {
  if (!isTaskBusy(error)) return false
  toast.info(TASK_BUSY_QUEUED_MESSAGE)
  return true
}

export function toastApiError(error: unknown, fallback?: string): void {
  if (notifyTaskBusy(error)) return
  toast.error(getApiErrorMessage(error, fallback))
}

export function isApiStatus(error: unknown, status: number): boolean {
  return error instanceof ApiError && error.status === status
}

/** Network errors and server failures may recover on retry; client errors and cancellations do not. */
export function isTransientApiError(error: unknown): boolean {
  if (error instanceof Error && error.name === 'AbortError') return false
  return (error instanceof ApiError && error.status >= 500) || error instanceof TypeError
}
