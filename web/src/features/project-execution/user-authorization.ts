import { useAuthStore } from '@/stores/auth'
import type { AuthorizationProvenance } from '@/types/generated/bindings/AuthorizationProvenance'

export function newIdempotencyKey(prefix: string): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID()
  }
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2)}`
}

/**
 * The signed-in user's authorization for one user-only mutation. Every
 * approval, release, and attestation the web client sends carries this
 * provenance; the server validates the principal against the session.
 */
export function createUserAuthorization(
  action: string,
  authorizationBasis: string,
): AuthorizationProvenance {
  const user = useAuthStore.getState().user
  if (!user) throw new Error('Sign in again before completing this user-authorized action.')
  return {
    principal: { kind: 'user', id: user.id, display_name: user.display_name ?? null },
    authorization_basis: authorizationBasis,
    action,
    event_id: newIdempotencyKey(action),
    occurred_at: new Date().toISOString(),
  }
}
