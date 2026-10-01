import { describe, expect, it } from 'vitest'
import { ApiError } from '@/api/client'
import { getApiErrorMessage } from './api-error'

describe('placement admission errors', () => {
  it('includes candidate reasons for daemon upgrade refusals', () => {
    const error = new ApiError('Upgrade the daemon', 409, undefined, {
      code: 'daemon_upgrade_required',
      message: 'Upgrade the daemon',
      request_id: '',
      details: { rejected_candidates: [{
        owner_kind: 'daemon', daemon_id: 'old-daemon', repo_location_id: 'checkout',
        filter_codes: ['daemon_upgrade_required', 'capability_missing'],
      }] },
    })
    const message = getApiErrorMessage(error)
    expect(message).toContain('Daemon old-daemon, location checkout')
    expect(message).toContain('Upgrade the daemon to the server release (protocol revision 3 or newer) (daemon_upgrade_required)')
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
    expect(result).toContain('Executor is not installed, authenticated, or enabled (executor_unavailable)')
    expect(result).toContain('Daemon policy denies a required hook, CI step, or environment setup (run_purpose_denied)')
    expect(result).toContain('Server, location server-checkout: Agent has no available capacity (agent_capacity)')
    expect(result).toContain('Request ID: request-1')
  })

  it('handles JSON error bodies and unfamiliar filter codes', () => {
    const error = new ApiError(JSON.stringify({
      code: 'placement_unavailable',
      message: 'Placement refused',
      details: {
        rejected_candidates: [{
          owner_kind: 'server',
          repo_location_id: 'location-1',
          filter_codes: ['new_owner_filter'],
        }],
      },
    }), 409)

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

  it('ignores malformed candidate data without losing the server message', () => {
    const error = new ApiError('Placement refused', 409, undefined, {
      code: 'placement_unavailable',
      message: 'Placement refused',
      request_id: '',
      details: { rejected_candidates: [null, 'private detail', { owner_kind: 'server', filter_codes: [null] }] },
    })
    expect(getApiErrorMessage(error)).toBe('Placement refused Server: No rejection reason provided.')
  })
})
