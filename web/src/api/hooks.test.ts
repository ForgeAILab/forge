import { createElement } from 'react'
import { act, cleanup, renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { afterEach, describe, expect, it, vi } from 'vitest'

import {
  getExecutionHookLogs,
  getExecutionLogs,
  type ProjectMilestoneReleaseInput,
  useReleaseProjectMilestone,
} from './hooks'
import { useAuthStore } from '@/stores/auth'

describe('execution log API helpers', () => {
  afterEach(() => {
    cleanup()
    vi.restoreAllMocks()
    useAuthStore.getState().clearAuth()
    localStorage.clear()
  })

  it('loads execution logs through the authenticated API client', async () => {
    useAuthStore.setState({ accessToken: 'access-token', refreshToken: 'refresh-token' })
    const fetchMock = vi.spyOn(window, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ items: [], has_more: false }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    )

    await expect(getExecutionLogs('exec-1', { tail: 500 })).resolves.toEqual({
      items: [],
      has_more: false,
    })

    expect(fetchMock).toHaveBeenCalledTimes(1)
    const [url, init] = fetchMock.mock.calls[0]
    expect((url as URL).pathname).toBe('/api/v1/executions/exec-1/logs')
    expect((url as URL).searchParams.get('tail')).toBe('500')
    expect((init?.headers as Headers).get('authorization')).toBe('Bearer access-token')
  })

  it('loads hook logs without duplicating the API prefix', async () => {
    useAuthStore.setState({ accessToken: 'access-token', refreshToken: 'refresh-token' })
    const fetchMock = vi.spyOn(window, 'fetch').mockResolvedValue(
      new Response(JSON.stringify([]), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    )

    await expect(getExecutionHookLogs('exec-1')).resolves.toEqual([])

    expect(fetchMock).toHaveBeenCalledTimes(1)
    const [url, init] = fetchMock.mock.calls[0]
    expect((url as URL).pathname).toBe('/api/v1/executions/exec-1/hook-logs')
    expect((init?.headers as Headers).get('authorization')).toBe('Bearer access-token')
  })

  it('invalidates project analytics variants after releasing a milestone', async () => {
    useAuthStore.setState({ accessToken: 'access-token', refreshToken: 'refresh-token' })
    vi.spyOn(window, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ id: 'release-1' }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    )

    const queryClient = new QueryClient({
      defaultOptions: { mutations: { retry: false }, queries: { retry: false } },
    })
    const invalidateSpy = vi.spyOn(queryClient, 'invalidateQueries')
    const { result } = renderHook(() => useReleaseProjectMilestone(), {
      wrapper: ({ children }) =>
        createElement(QueryClientProvider, { client: queryClient }, children),
    })
    const input: ProjectMilestoneReleaseInput = {
      projectId: 'project-1',
      milestoneId: 'milestone-1',
      expectedMilestoneVersion: 2,
      readinessSnapshotId: 'readiness-1',
      readinessDigest: 'digest-1',
      idempotencyKey: 'idempotency-1',
      authorization: {
        principal: { kind: 'user', id: 'user-1', display_name: 'Test User' },
        authorization_basis: 'user_request',
        action: 'release_milestone',
        event_id: 'event-1',
        occurred_at: '2026-09-07T12:00:00Z',
      },
    }

    act(() => result.current.mutate(input))
    await waitFor(() => expect(result.current.isSuccess).toBe(true))

    expect(invalidateSpy).toHaveBeenCalledWith({
      queryKey: ['projects', 'project-1', 'analytics'],
    })
    expect(invalidateSpy).toHaveBeenCalledWith({ queryKey: ['analytics', 'usage'] })
  })
})
