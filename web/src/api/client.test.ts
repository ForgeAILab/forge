import { afterEach, describe, expect, it, vi } from 'vitest'

import { refreshAccess, useAuthStore } from '@/stores/auth'

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

describe('cookie-only API requests', () => {
  afterEach(() => {
    vi.restoreAllMocks()
    vi.unstubAllGlobals()
    useAuthStore.getState().clearAuth()
  })

  const seedCookieUser = () => useAuthStore.setState({
    accessToken: null,
    refreshToken: null,
    user: { id: 'cookie', email: 'cookie@example.com', display_name: null, is_admin: false, created_at: '' },
  })

  it('omits Authorization when there is no stored token', async () => {
    seedCookieUser()
    const fetchSpy = vi.spyOn(window, 'fetch').mockResolvedValue(new Response('{}'))
    await apiFetch('/projects')
    const headers = fetchSpy.mock.calls[0][1]?.headers as Headers
    expect(headers.has('authorization')).toBe(false)
  })

  it('refreshes with an empty object and retries using cookies without storing tokens', async () => {
    seedCookieUser()
    const fetchSpy = vi.spyOn(window, 'fetch')
      .mockResolvedValueOnce(new Response(null, { status: 401 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ access_token: 'new', refresh_token: 'new-refresh', token_type: 'Bearer', expires_in: 900 })))
      .mockResolvedValueOnce(new Response('{}'))
    await expect(apiFetch('/projects')).resolves.toEqual({})
    expect(fetchSpy.mock.calls[1]).toEqual(['/api/v1/auth/refresh', {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: '{}',
    }])
    expect((fetchSpy.mock.calls[2][1]?.headers as Headers).has('authorization')).toBe(false)
    expect(useAuthStore.getState().accessToken).toBeNull()
    expect(useAuthStore.getState().refreshToken).toBeNull()
  })

  it('shares a single cookie refresh across concurrent callers', async () => {
    seedCookieUser()
    const fetchSpy = vi.spyOn(window, 'fetch').mockResolvedValue(new Response(JSON.stringify({ access_token: 'new', refresh_token: 'new-refresh', token_type: 'Bearer', expires_in: 900 })))
    await expect(Promise.all([refreshAccess(), refreshAccess()])).resolves.toEqual([null, null])
    expect(fetchSpy).toHaveBeenCalledTimes(1)
  })

  it('replaces a stored access token after refreshing through the cookie', async () => {
    seedCookieUser()
    useAuthStore.setState({ accessToken: 'expired' })
    const fetchSpy = vi.spyOn(window, 'fetch').mockResolvedValue(new Response(JSON.stringify({ access_token: 'new', refresh_token: 'new-refresh', token_type: 'Bearer', expires_in: 900 })))
    await expect(refreshAccess()).resolves.toBe('new')
    expect(fetchSpy.mock.calls[0][1]?.body).toBe('{}')
    expect(useAuthStore.getState().accessToken).toBe('new')
  })

  it('clears the cookie-only user and goes to login after a rejected refresh', async () => {
    seedCookieUser()
    const location = { origin: 'http://localhost', href: '/projects' }
    vi.stubGlobal('window', { location })
    vi.stubGlobal('fetch', vi.fn(async () => new Response(null, { status: 401 })))
    await expect(apiFetch('/projects')).rejects.toMatchObject({ status: 401 })
    expect(useAuthStore.getState().user).toBeNull()
    expect(location.href).toBe('/login')
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
