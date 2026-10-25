// Product Genesis labels, authorization, and mutation utilities.

import { ApiError } from '@/api/client'
import { useAuthStore } from '@/stores/auth'
import type { ProductAgentSelection, ProjectCharterReadiness } from '../types'

export function lifecycleLabel(value: string): string {
  return value.replaceAll('_', ' ')
}

export function versionNumber(value: number | bigint): number {
  return typeof value === 'bigint' ? Number(value) : value
}

export function shortDigest(value: string): string {
  if (value.length <= 18) return value
  return `${value.slice(0, 10)}…${value.slice(-6)}`
}

function createEventId(prefix: string): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID()
  }
  return `${prefix}-${Date.now()}`
}

export function createAuthorization(action: string) {
  const user = useAuthStore.getState().user
  if (!user) throw new Error('Sign in again before approving a Charter.')
  return {
    principal: { kind: 'user' as const, id: user.id, display_name: user.display_name },
    authorization_basis: 'interactive_user_approval',
    action,
    event_id: createEventId(action),
    occurred_at: new Date().toISOString(),
  }
}

export function mutationId(prefix: string, identity: string): string {
  return `${prefix}:${identity}`
}

export function getApiErrorMessage(cause: unknown, fallback: string): string {
  if (cause instanceof ApiError) {
    if (cause.status === 409 || cause.status === 412) {
      return 'The Charter changed while this action was open. Refresh the exact revision and review it again.'
    }
    if (cause.status === 403) return 'This action is not authorized for the current account.'
    return cause.message || fallback
  }
  return cause instanceof Error ? cause.message : fallback
}

export function isConflict(cause: unknown): boolean {
  return cause instanceof ApiError && (cause.status === 409 || cause.status === 412)
}

export function agentLabel(agent: ProductAgentSelection | null): string {
  if (!agent) return 'No Project Agent selected'
  return agent.display_name ? `${agent.display_name} · ${agent.identity_id}` : agent.identity_id
}

export function readinessLabel(readiness: ProjectCharterReadiness | null): string {
  if (!readiness) return 'Not evaluated'
  return readiness.status === 'ready' ? 'Ready for approval' : 'Blocked'
}
