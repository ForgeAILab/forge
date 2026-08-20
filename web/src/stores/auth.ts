import { create } from 'zustand'
import { createJSONStorage, persist } from 'zustand/middleware'
import type { AuthResponse, UserResponse } from '@/types/generated'

type AuthState = {
  accessToken: string | null
  refreshToken: string | null
  user: UserResponse | null
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
      setAuth: (auth, user) =>
        set({
          accessToken: auth.access_token,
          refreshToken: auth.refresh_token,
          user,
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
let pendingRefresh: Promise<string> | null = null

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

export async function refreshAccess(): Promise<string> {
  if (pendingRefresh) return pendingRefresh

  const { refreshToken, updateTokens, clearAuth } = useAuthStore.getState()
  if (!refreshToken) {
    clearAuth()
    throw new Error('No refresh token')
  }

  pendingRefresh = (async (): Promise<string> => {
    let response: Response
    try {
      response = await fetch('/api/v1/auth/refresh', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ refresh_token: refreshToken }),
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
    updateTokens(auth)
    return auth.access_token
  })().finally(() => {
    pendingRefresh = null
  })

  return pendingRefresh
}
