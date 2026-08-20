import { useCallback, useMemo, useRef, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'

import { useProjectQuery } from '@/api/hooks'
import { getApiErrorCode, getApiErrorMessage, isApiStatus } from '@/lib/api-error'
import { useAuthStore } from '@/stores/auth'
import type { ProductAgentSelection } from '@/types/generated/bindings/ProductAgentSelection'
import type { ProjectCharter } from '@/types/generated/bindings/ProjectCharter'
import type { ProjectCharterApproval } from '@/types/generated/bindings/ProjectCharterApproval'
import type { ProjectCharterRevision } from '@/types/generated/bindings/ProjectCharterRevision'

import { approveProjectCharterRevision, getProjectCharter } from './api'
import type { AuthorizationProvenance } from './types'

// Must match `APPROVAL_ACTION` in crates/api/src/routes/project_charters.rs;
// the server rejects a mismatch as an invalid authorization event (403).
const APPROVAL_ACTION = 'project_charter.approval'

export const projectCharterQueryKey = (projectId: string) =>
  ['projects', projectId, 'charter'] as const

/** ts-rs maps Rust i64 to bigint; the JSON wire carries plain numbers. */
function versionNumber(value: number | bigint): number {
  return typeof value === 'bigint' ? Number(value) : value
}

function createEventId(action: string): string {
  return typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function'
    ? crypto.randomUUID()
    : `${action}-${Date.now()}-${Math.random().toString(36).slice(2)}`
}

function createAuthorization(): AuthorizationProvenance {
  const user = useAuthStore.getState().user
  if (!user) throw new Error('Sign in again before approving a Charter.')
  return {
    principal: { kind: 'user', id: user.id, display_name: user.display_name ?? null },
    authorization_basis: 'interactive_user_approval',
    action: APPROVAL_ACTION,
    event_id: createEventId(APPROVAL_ACTION),
    occurred_at: new Date().toISOString(),
  } as AuthorizationProvenance
}

/**
 * A rejected approval always has a specific server-side reason — which
 * readiness gap, which stale version — and hiding it behind a generic
 * "something changed" line sends the reader looking in the wrong place. Only
 * the genuine optimistic-concurrency conflict gets prose of its own, because
 * there the useful instruction (re-review the refreshed revision) is not in
 * the server's message.
 */
export function projectCharterApprovalError(cause: unknown): string {
  if (isApiStatus(cause, 503)) return 'Forge is not reachable right now. Try again in a moment.'
  const code = getApiErrorCode(cause)
  if ((isApiStatus(cause, 409) || isApiStatus(cause, 412)) && code === 'version_conflict') {
    return 'The Charter or Project changed while this approval was open. Review the refreshed revision and approve again.'
  }
  return getApiErrorMessage(cause, 'The Charter revision could not be approved.')
}

export type ProjectCharterApprovalState = {
  /** The exact revision the user would approve, or null when there is none. */
  revision: ProjectCharterRevision | null
  charter: ProjectCharter | null
  /** Project Agent this approval binds the Project to. */
  agent: ProductAgentSelection | null
  /** Why approving is not possible yet, or null when it is. */
  blockedReason: string | null
  isPending: boolean
  error: string | null
  clearError: () => void
  approve: () => Promise<ProjectCharterApproval>
}

/**
 * Approval state for a Project's adoption/amendment Charter.
 *
 * Shared by the pinned Project Agent Chat card and the Project Overview
 * banner, so both surfaces approve the same exact revision through the same
 * REST contract and neither can drift into showing a different one.
 */
export function useProjectCharterApproval(projectId: string): ProjectCharterApprovalState {
  const queryClient = useQueryClient()
  const [error, setError] = useState<string | null>(null)
  // One idempotency key per (revision, version) attempt, so a retry after a
  // network hiccup replays the same approval instead of writing a second one.
  const attemptRef = useRef<{ fingerprint: string; key: string } | null>(null)

  const projectQuery = useProjectQuery(projectId)
  const charterQuery = useQuery({
    queryKey: projectCharterQueryKey(projectId),
    queryFn: () => getProjectCharter(projectId),
    refetchInterval: 15_000,
  })

  const charterData = charterQuery.data ?? null
  const charter = charterData?.charter ?? null
  const agent = charterData?.selected_project_agent ?? null

  // Only an unapproved revision is actionable. `current_draft_revision` is the
  // Charter's live head; once it is approved the Project is charter-backed and
  // this surface has nothing left to ask for.
  const revision = useMemo(() => {
    const draft = charterData?.current_draft_revision ?? null
    if (!draft) return null
    return draft.lifecycle === 'draft' || draft.lifecycle === 'proposed' ? draft : null
  }, [charterData])

  // The server refuses to approve a revision whose readiness is blocked, and
  // it already computed exactly which sections are missing. Show that before
  // the click instead of letting the user discover it as a rejection.
  const blockedReason = useMemo(() => {
    if (!revision || !charter) return null
    if (!agent) return 'No eligible Project Agent is bound — pick one in Agent Settings first.'
    const gaps = (revision.readiness?.gaps ?? []).filter((gap) => gap.blocking)
    if (revision.readiness?.status === 'blocked' && gaps.length > 0) {
      return `This revision is not ready to approve. Ask the Project Agent to fill in: ${gaps
        .map((gap) => gap.message)
        .join(' ')}`
    }
    return null
  }, [revision, charter, agent])

  const mutation = useMutation({
    mutationFn: async () => {
      const project = projectQuery.data
      if (!revision || !charter) throw new Error('The Charter is not loaded yet. Refresh and retry.')
      if (!agent) {
        throw new Error('No eligible Project Agent is bound — pick one in Agent Settings first.')
      }
      if (!project) throw new Error('Project details are still loading. Try again.')

      const expectedCharterVersion = versionNumber(charter.version)
      const expectedProjectVersion = versionNumber(project.version)
      const fingerprint = `${revision.id}:${expectedCharterVersion}:${expectedProjectVersion}`
      const attempt =
        attemptRef.current?.fingerprint === fingerprint
          ? attemptRef.current
          : { fingerprint, key: createEventId('project-charter-approval') }
      attemptRef.current = attempt

      return approveProjectCharterRevision(projectId, revision.id, {
        mutation: {
          expected_version: expectedCharterVersion,
          expected_digest: revision.content_digest,
          idempotency_key: attempt.key,
          deduplication_key: attempt.key,
          authorization: createAuthorization(),
        },
        charter_id: charter.id,
        revision_id: revision.id,
        content_digest: revision.content_digest,
        render_digest: revision.render_digest,
        expected_charter_version: expectedCharterVersion,
        expected_project_version: expectedProjectVersion,
        approved_project_name: revision.content.identity.working_name,
        approved_project_slug: revision.content.identity.slug_proposal,
        project_mode: revision.project_mode,
        selected_project_agent_identity_id: agent.identity_id,
        selected_project_agent_profile_revision_id: agent.profile_revision_id,
        selected_project_agent_operating_skill_revision: agent.operating_skill_revision,
        selected_project_agent_policy_digest: agent.policy_digest,
      })
    },
    onSuccess: () => {
      setError(null)
      attemptRef.current = null
      void queryClient.invalidateQueries({ queryKey: projectCharterQueryKey(projectId) })
      void queryClient.invalidateQueries({ queryKey: ['projects', projectId] })
      void queryClient.invalidateQueries({ queryKey: ['project-overview', projectId] })
      void queryClient.invalidateQueries({ queryKey: ['agent-chats'] })
    },
    onError: (cause) => {
      setError(projectCharterApprovalError(cause))
      // A conflict means someone moved the Charter or the Project; refetch so
      // the next attempt reviews the revision that actually exists now.
      void queryClient.invalidateQueries({ queryKey: projectCharterQueryKey(projectId) })
      void queryClient.invalidateQueries({ queryKey: ['projects', projectId] })
    },
  })

  const approve = useCallback(async () => {
    setError(null)
    return mutation.mutateAsync()
  }, [mutation])

  return {
    revision,
    charter,
    agent,
    blockedReason,
    isPending: mutation.isPending || projectQuery.isLoading,
    error,
    clearError: useCallback(() => setError(null), []),
    approve,
  }
}
