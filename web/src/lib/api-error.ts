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
    const placementRejections = getPlacementRejectionMessage(error)
    return `${message}${placementRejections ? ` ${placementRejections}` : ''}${requestId ? ` Request ID: ${requestId}` : ''}`
  }
  if (error instanceof Error) return error.message
  return fallback
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
    'Upgrade the daemon to the server release (protocol revision 3 or newer)',
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

export function toastApiError(error: unknown, fallback?: string): void {
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
