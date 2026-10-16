import { create } from 'zustand'
import { createJSONStorage, persist } from 'zustand/middleware'
import type { AuthResponse, UserResponse } from '@/types/generated'

type AuthState = {
  accessToken: string | null
  refreshToken: string | null
  user: UserResponse | null
  sessionChecked: boolean
  setAuth: (auth: AuthResponse, user: UserResponse) => void
  updateTokens: (auth: AuthResponse) => void
  updateUser: (user: UserResponse) => void
  clearAuth: () => void
}

export const useAuthStore = create<AuthState>()(
  persist(
    (set) => ({
      accessToken: null,
      refreshToken: null,
      user: null,
      sessionChecked: false,
      setAuth: (auth, user) =>
        set({
          accessToken: auth.access_token,
          refreshToken: auth.refresh_token,
          user,
          sessionChecked: true,
        }),
      updateTokens: (auth) =>
        set({
          accessToken: auth.access_token,
          refreshToken: auth.refresh_token,
        }),
      updateUser: (user) => set({ user }),
      clearAuth: () =>
        set({
          accessToken: null,
          refreshToken: null,
          user: null,
          sessionChecked: true,
        }),
    }),
    {
      name: 'forge-auth',
      storage: createJSONStorage(() => window.localStorage),
      partialize: (state) => ({
        accessToken: state.accessToken,
        refreshToken: state.refreshToken,
        user: state.user,
      }),
    },
  ),
)

// Module-level singleton ensures concurrent 401s share one refresh request
let pendingRefresh: Promise<string | null> | null = null
let pendingSession: Promise<void> | null = null

/** Resolve an HttpOnly session before the router decides whether to sign in. */
export async function restoreSession(): Promise<void> {
  const { accessToken, sessionChecked } = useAuthStore.getState()
  if (accessToken || sessionChecked) return
  if (pendingSession) return pendingSession

  pendingSession = (async () => {
    const response = await fetch('/api/v1/auth/me')
    if (response.status === 401) {
      useAuthStore.getState().clearAuth()
      return
    }
    if (!response.ok) throw new Error(`Session lookup failed: HTTP ${response.status}`)
    const user = (await response.json()) as UserResponse
    useAuthStore.setState({ user, sessionChecked: true })
  })().finally(() => {
    pendingSession = null
  })
  return pendingSession
}

/**
 * A refresh failure the stored refresh token could still survive: the server was
 * unreachable or answered with something other than a rejection of the token
 * itself. Forge runs on the user's own machine and gets restarted constantly, so
 * discarding the session on one of these would sign the user out every restart.
 */
export class RefreshUnavailableError extends Error {
  constructor(cause: string) {
    super(`Token refresh unavailable: ${cause}`)
    this.name = 'RefreshUnavailableError'
  }
}

/** Only the server rejecting the refresh token itself invalidates the session. */
function refreshTokenRejected(status: number): boolean {
  return status === 400 || status === 401 || status === 403 || status === 422
}

export async function refreshAccess(): Promise<string | null> {
  if (pendingRefresh) return pendingRefresh

  const { accessToken, refreshToken, updateTokens, clearAuth } = useAuthStore.getState()
  pendingRefresh = (async (): Promise<string | null> => {
    let response: Response
    try {
      response = await fetch('/api/v1/auth/refresh', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(refreshToken ? { refresh_token: refreshToken } : {}),
      })
    } catch (error) {
      // The server is down or restarting. The refresh token is still good.
      throw new RefreshUnavailableError(error instanceof Error ? error.message : 'network error')
    }
    if (!response.ok) {
      if (!refreshTokenRejected(response.status)) {
        throw new RefreshUnavailableError(`HTTP ${response.status}`)
      }
      clearAuth()
      throw new Error('Token refresh failed')
    }
    const auth = (await response.json()) as AuthResponse
    // A cookie-only session stays cookie-only; password sign-in still persists
    // the JSON token pair using the existing localStorage behavior.
    if (accessToken || refreshToken) {
      updateTokens(auth)
      return auth.access_token
    }
    return null
  })().finally(() => {
    pendingRefresh = null
  })

  return pendingRefresh
}
