import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { createMemoryHistory } from '@tanstack/react-router'
import { QueryClient } from '@tanstack/react-query'
import { restoreSession, useAuthStore } from '@/stores/auth'
import { createAppRouter, firstAuthorizedProjectId } from './router'

// F17 / 8.4.4: the root route redirected to `/projects/default/board` when
// the account had no Project — a fabricated ID that 404s on every
// Project-scoped fetch behind it. `firstAuthorizedProjectId` is the exact
// selection `indexRoute`'s `beforeLoad` redirects on; with no items it must
// return `undefined` so the caller falls back to `/chat`, never a literal
// `'default'`.
describe('firstAuthorizedProjectId', () => {
  it('returns the first authorized Project id when at least one exists', () => {
    expect(
      firstAuthorizedProjectId({
        items: [{ id: 'project-1' } as never, { id: 'project-2' } as never],
      }),
    ).toBe('project-1')
  })

  it('returns undefined rather than a fabricated default id when no Project remains', () => {
    expect(firstAuthorizedProjectId({ items: [] })).toBeUndefined()
  })
})

describe('cookie session auth guard', () => {
  beforeEach(() => {
    useAuthStore.setState({ accessToken: null, refreshToken: null, user: null, sessionChecked: false })
    localStorage.clear()
  })

  afterEach(() => {
    vi.restoreAllMocks()
    useAuthStore.getState().clearAuth()
  })

  it('restores an empty-storage session before loading a protected route without redirecting', async () => {
    const user = { id: 'cookie-user', email: 'cookie@example.com', display_name: null, is_admin: false, created_at: '' }
    const fetchSpy = vi.spyOn(window, 'fetch').mockResolvedValue(new Response(JSON.stringify(user)))
    const client = new QueryClient()
    const router = createAppRouter(client)
    router.update({ ...router.options, history: createMemoryHistory({ initialEntries: ['/account/profile'] }) })
    await router.load()

    expect(fetchSpy).toHaveBeenCalledExactlyOnceWith('/api/v1/auth/me')
    expect(router.state.location.pathname).toBe('/account/profile')
    expect(router.state.statusCode).toBe(200)
    expect(useAuthStore.getState().user).toEqual(user)
    expect(useAuthStore.getState().accessToken).toBeNull()
    expect(useAuthStore.getState().refreshToken).toBeNull()
    client.clear()
  })

  it('revives an expired access cookie once and signs in without storing tokens', async () => {
    const user = { id: 'cookie-user', email: 'cookie@example.com', display_name: null, is_admin: false, created_at: '' }
    const fetchSpy = vi.spyOn(window, 'fetch')
      .mockResolvedValueOnce(new Response(null, { status: 401 }))
      .mockResolvedValueOnce(new Response(JSON.stringify({ access_token: 'new', refresh_token: 'new-refresh', token_type: 'Bearer', expires_in: 900 })))
      .mockResolvedValueOnce(new Response(JSON.stringify(user)))
    const client = new QueryClient()
    const router = createAppRouter(client)
    router.update({ ...router.options, history: createMemoryHistory({ initialEntries: ['/account/profile'] }) })
    await router.load()
    await restoreSession()

    expect(fetchSpy.mock.calls).toEqual([
      ['/api/v1/auth/me'],
      ['/api/v1/auth/refresh', { method: 'POST' }],
      ['/api/v1/auth/me'],
    ])
    expect(router.state.location.pathname).toBe('/account/profile')
    expect(router.state.statusCode).toBe(200)
    expect(useAuthStore.getState()).toMatchObject({ user, accessToken: null, refreshToken: null, sessionChecked: true })
    expect(JSON.parse(localStorage.getItem('forge-auth')!).state).toMatchObject({ accessToken: null, refreshToken: null })
    client.clear()
  })

  it.each([401, 503, 'network'] as const)('clears a cached user and redirects to login when cookie revival fails: %s', async (failure) => {
    useAuthStore.setState({ user: { id: 'stale', email: 'old@example.com', display_name: null, is_admin: false, created_at: '' } })
    const fetchSpy = vi.spyOn(window, 'fetch')
      .mockResolvedValueOnce(new Response(null, { status: 401 }))
    if (failure === 'network') fetchSpy.mockRejectedValueOnce(new TypeError('Failed to fetch'))
    else fetchSpy.mockResolvedValueOnce(new Response(null, { status: failure }))
    const client = new QueryClient()
    const router = createAppRouter(client)
    router.update({ ...router.options, history: createMemoryHistory({ initialEntries: ['/account/profile'] }) })
    await router.load()
    await restoreSession()

    expect(fetchSpy.mock.calls).toEqual([
      ['/api/v1/auth/me'],
      ['/api/v1/auth/refresh', { method: 'POST' }],
    ])
    expect(router.state.location.pathname).toBe('/login')
    expect(useAuthStore.getState()).toMatchObject({ user: null, sessionChecked: true })
    client.clear()
  })

  it.each([401, 503])('redirects to login without a second refresh when the retried session lookup fails: %s', async (status) => {
    const fetchSpy = vi.spyOn(window, 'fetch')
      .mockResolvedValueOnce(new Response(null, { status: 401 }))
      .mockResolvedValueOnce(new Response('{}'))
      .mockResolvedValueOnce(new Response(null, { status }))
    const client = new QueryClient()
    const router = createAppRouter(client)
    router.update({ ...router.options, history: createMemoryHistory({ initialEntries: ['/account/profile'] }) })
    await router.load()
    await restoreSession()

    expect(fetchSpy.mock.calls).toEqual([
      ['/api/v1/auth/me'],
      ['/api/v1/auth/refresh', { method: 'POST' }],
      ['/api/v1/auth/me'],
    ])
    expect(router.state.location.pathname).toBe('/login')
    expect(useAuthStore.getState()).toMatchObject({ user: null, sessionChecked: true })
    client.clear()
  })

  it('keeps stored-token startup behavior without a cookie lookup', async () => {
    useAuthStore.setState({ accessToken: 'stored-token', refreshToken: 'stored-refresh' })
    const fetchSpy = vi.spyOn(window, 'fetch')
    const client = new QueryClient()
    const router = createAppRouter(client)
    router.update({ ...router.options, history: createMemoryHistory({ initialEntries: ['/account/profile'] }) })
    await router.load()
    expect(fetchSpy).not.toHaveBeenCalled()
    expect(router.state.location.pathname).toBe('/account/profile')
    client.clear()
  })
})
