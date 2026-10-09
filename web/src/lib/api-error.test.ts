import { describe, expect, it, vi } from 'vitest'
import { toast } from 'sonner'
import { ApiError } from '@/api/client'
import {
  getApiErrorMessage,
  isTaskBusy,
  TASK_BUSY_QUEUED_MESSAGE,
  taskBusyRetryAfterMs,
  toastApiError,
} from './api-error'

describe('placement admission errors', () => {
  it('includes candidate reasons for daemon upgrade refusals', () => {
    const error = new ApiError('Upgrade the daemon', 409, undefined, {
      code: 'daemon_upgrade_required',
      message: 'Upgrade the daemon',
      request_id: '',
      details: {
        rejected_candidates: [
          {
            owner_kind: 'daemon',
            daemon_id: 'old-daemon',
            repo_location_id: 'checkout',
            filter_codes: ['daemon_upgrade_required', 'capability_missing'],
          },
        ],
      },
    })
    const message = getApiErrorMessage(error)
    expect(message).toContain('Daemon old-daemon, location checkout')
    expect(message).toContain(
      'Upgrade the daemon to the server release (protocol revision 6 or newer) (daemon_upgrade_required)',
    )
    expect(message).toContain('Executor lacks required capabilities (capability_missing)')
  })
  it('shows every candidate and filter code with a readable explanation', () => {
    const message = 'No compatible workspace owner'
    const error = new ApiError(message, 409, 'request-1', {
      code: 'placement_unavailable',
      message,
      request_id: 'request-1',
      details: {
        rejected_candidates: [
          {
            repo_location_id: 'mac-checkout',
            owner_kind: 'daemon',
            daemon_id: 'mac-daemon',
            runtime_id: 'mac-runtime',
            filter_codes: ['executor_unavailable', 'run_purpose_denied'],
          },
          {
            repo_location_id: 'server-checkout',
            owner_kind: 'server',
            daemon_id: null,
            runtime_id: null,
            filter_codes: ['agent_capacity'],
          },
        ],
      },
    })

    const result = getApiErrorMessage(error)
    expect(result).toContain(message)
    expect(result).toContain('Daemon mac-daemon, location mac-checkout, runtime mac-runtime')
    expect(result).toContain(
      'Executor is not installed, authenticated, or enabled (executor_unavailable)',
    )
    expect(result).toContain(
      'Daemon policy denies a required hook, CI step, or environment setup (run_purpose_denied)',
    )
    expect(result).toContain(
      'Server, location server-checkout: Agent has no available capacity (agent_capacity)',
    )
    expect(result).toContain('Request ID: request-1')
  })

  it('handles JSON error bodies and unfamiliar filter codes', () => {
    const error = new ApiError(
      JSON.stringify({
        code: 'placement_unavailable',
        message: 'Placement refused',
        details: {
          rejected_candidates: [
            {
              owner_kind: 'server',
              repo_location_id: 'location-1',
              filter_codes: ['new_owner_filter'],
            },
          ],
        },
      }),
      409,
    )

    expect(getApiErrorMessage(error)).toContain('new owner filter (new_owner_filter)')
  })

  it('explains when there are no candidate locations', () => {
    const error = new ApiError('No compatible owner', 409, undefined, {
      code: 'placement_unavailable',
      message: 'No compatible owner',
      request_id: '',
      details: { rejected_candidates: [] },
    })
    expect(getApiErrorMessage(error)).toContain('No eligible repository locations.')
  })

  it('does not expose arbitrary details or change unrelated errors', () => {
    const error = new ApiError('Version conflict', 409, undefined, {
      code: 'version_conflict',
      message: 'Version conflict',
      request_id: '',
      details: { rejected_candidates: ['private detail'] },
    })
    expect(getApiErrorMessage(error)).toBe('Version conflict')
    expect(getApiErrorMessage(new Error('Network unavailable'))).toBe('Network unavailable')
  })

  it('explains provision_failed as an actionable retry exhaustion', () => {
    const error = new ApiError('Placement refused', 409, undefined, {
      code: 'placement_unavailable',
      message: 'Placement refused',
      request_id: '',
      details: {
        rejected_candidates: [
          {
            owner_kind: 'daemon',
            daemon_id: 'connected-daemon',
            filter_codes: ['provision_failed'],
          },
        ],
      },
    })
    expect(getApiErrorMessage(error)).toContain('Repository provisioning exhausted its retries')
    expect(getApiErrorMessage(error)).toContain(
      'reconnect the machine or update Project placement settings',
    )
    expect(getApiErrorMessage(error)).not.toContain('offline')
  })

  it('ignores malformed candidate data without losing the server message', () => {
    const error = new ApiError('Placement refused', 409, undefined, {
      code: 'placement_unavailable',
      message: 'Placement refused',
      request_id: '',
      details: {
        rejected_candidates: [
          null,
          'private detail',
          { owner_kind: 'server', filter_codes: [null] },
        ],
      },
    })
    expect(getApiErrorMessage(error)).toBe(
      'Placement refused Server: No rejection reason provided.',
    )
  })
})

describe('task_busy', () => {
  const busy = () =>
    new ApiError('Task has pending steps; accepted work remains queued', 409, undefined, {
      code: 'task_busy',
      message: 'Task has pending steps; accepted work remains queued',
      request_id: '',
      details: { pending_steps: 2, retry_after_ms: 250, retry_hint: 'Refetch the Task' },
    })
  it('is an accepted, queued request with a retry hint', () => {
    expect(isTaskBusy(busy())).toBe(true)
    expect(taskBusyRetryAfterMs(busy())).toBe(250)
    expect(isTaskBusy(new ApiError('changed', 409, undefined))).toBe(false)
  })
  it('shows the queued notice instead of an error toast', () => {
    const info = vi.spyOn(toast, 'info').mockImplementation(() => '')
    const error = vi.spyOn(toast, 'error').mockImplementation(() => '')
    toastApiError(busy(), 'Transition failed')
    expect(info).toHaveBeenCalledWith(TASK_BUSY_QUEUED_MESSAGE)
    expect(error).not.toHaveBeenCalled()
    info.mockRestore()
    error.mockRestore()
  })
})
