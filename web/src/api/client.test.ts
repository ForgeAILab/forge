import { afterEach, describe, expect, it, vi } from 'vitest'

import { useAuthStore } from '@/stores/auth'

import { apiFetch } from './client'

describe('apiFetch', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('returns undefined for successful empty responses', async () => {
    vi.spyOn(window, 'fetch').mockResolvedValue(
      new Response(null, {
        status: 201,
        statusText: 'Created',
      }),
    )

    await expect(
      apiFetch<void>('/tasks/task-id/dependencies', { method: 'POST' }),
    ).resolves.toBeUndefined()
  })

  it('parses successful JSON responses', async () => {
    vi.spyOn(window, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({ ok: true }), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    )

    await expect(apiFetch<{ ok: boolean }>('/status')).resolves.toEqual({ ok: true })
  })
})

describe('apiFetch token refresh', () => {
  afterEach(() => {
    vi.restoreAllMocks()
    useAuthStore.getState().clearAuth()
  })

  const seedSession = () => {
    useAuthStore.setState({
      accessToken: 'expired-access-token',
      refreshToken: 'refresh-token',
      user: null,
    })
  }

  it('keeps the session when the refresh endpoint is unreachable', async () => {
    seedSession()
    vi.spyOn(window, 'fetch').mockImplementation(async (input) => {
      if (String(input).includes('/auth/refresh')) {
        throw new TypeError('Failed to fetch')
      }
      return new Response(null, { status: 401 })
    })

    await expect(apiFetch('/projects')).rejects.toMatchObject({ status: 503 })
    expect(useAuthStore.getState().refreshToken).toBe('refresh-token')
  })

  it('keeps the session when the refresh endpoint returns a server error', async () => {
    seedSession()
    vi.spyOn(window, 'fetch').mockImplementation(async (input) =>
      String(input).includes('/auth/refresh')
        ? new Response(null, { status: 502 })
        : new Response(null, { status: 401 }),
    )

    await expect(apiFetch('/projects')).rejects.toMatchObject({ status: 503 })
    expect(useAuthStore.getState().refreshToken).toBe('refresh-token')
  })

  it('clears the session when the refresh token itself is rejected', async () => {
    seedSession()
    vi.spyOn(window, 'fetch').mockImplementation(async (input) =>
      String(input).includes('/auth/refresh')
        ? new Response(null, { status: 401 })
        : new Response(null, { status: 401 }),
    )

    await expect(apiFetch('/projects')).rejects.toMatchObject({ status: 401 })
    expect(useAuthStore.getState().refreshToken).toBeNull()
  })
})
