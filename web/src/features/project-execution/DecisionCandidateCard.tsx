import { useState } from 'react'
import { WarningCircle } from '@phosphor-icons/react'

import { Button } from '@/components/ui/button'
import { Label } from '@/components/ui/label'
import { Textarea } from '@/components/ui/textarea'
import { ConflictDetails } from '@/components/conflict-details'
import { useAuthStore } from '@/stores/auth'
import type { AuthorizationProvenance } from '@/types/generated/bindings/AuthorizationProvenance'
import type { PendingDecisionSummary } from '@/types/generated'

import type { ApproveDecisionCandidateWire, RejectDecisionCandidateWire } from './decision-api'
import {
  useApproveDecisionCandidateMutation,
  useRejectDecisionCandidateMutation,
} from './decision-hooks'

const APPROVE_ACTION = 'project.decision.candidate.approve'
const REJECT_ACTION = 'project.decision.candidate.reject'

function numberValue(value: number | bigint): number {
  return typeof value === 'bigint' ? Number(value) : value
}

function shortId(value: string): string {
  return value.length > 16 ? `${value.slice(0, 8)}…${value.slice(-6)}` : value
}

function humanize(value: string): string {
  return value.replaceAll('_', ' ').replace(/\b\w/g, (letter) => letter.toUpperCase())
}

function newIdempotencyKey(prefix: string): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID()
  }
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2)}`
}

function createUserAuthorization(
  action: string,
  authorizationBasis: string,
): AuthorizationProvenance {
  const user = useAuthStore.getState().user
  if (!user) throw new Error('Sign in again before acting on this decision.')
  return {
    principal: { kind: 'user', id: user.id, display_name: user.display_name ?? null },
    authorization_basis: authorizationBasis,
    action,
    event_id: newIdempotencyKey(action),
    occurred_at: new Date().toISOString(),
  }
}

/**
 * Project Overview previously exposed only a bare candidate UUID with no
 * question, alternatives, or approve/reject action (finding F15): the live
 * candidate had `options_json = []` while carrying a populated
 * `selected_outcome`. This renders the typed `pending_decisions` summary
 * (design D19) instead -- question, alternatives, the Project Agent's
 * recommendation, rationale, affected records, authority, and an
 * approve/reject action -- or, for a historical row that predates the D19
 * candidate-shape invariant, the exact reason it cannot be approved.
 */
export function DecisionCandidateCard({
  projectId,
  candidates,
}: {
  projectId: string
  candidates: PendingDecisionSummary[]
}) {
  const [resolved, setResolved] = useState<Set<string>>(new Set())
  const pending = candidates.filter((candidate) => !resolved.has(candidate.id))

  if (pending.length === 0) {
    return (
      <p className="text-xs text-muted-foreground">No pending decision proposals are recorded.</p>
    )
  }

  return (
    <ul className="space-y-3">
      {pending.map((candidate) => (
        <li key={candidate.id}>
          <PendingDecisionItem
            projectId={projectId}
            candidate={candidate}
            onResolved={() =>
              setResolved((current) => {
                const next = new Set(current)
                next.add(candidate.id)
                return next
              })
            }
          />
        </li>
      ))}
    </ul>
  )
}

function PendingDecisionItem({
  projectId,
  candidate,
  onResolved,
}: {
  projectId: string
  candidate: PendingDecisionSummary
  onResolved: () => void
}) {
  const approveMutation = useApproveDecisionCandidateMutation(projectId)
  const rejectMutation = useRejectDecisionCandidateMutation(projectId)
  const [reason, setReason] = useState('')
  const [showReject, setShowReject] = useState(false)

  const isMalformed = candidate.validity === 'malformed'
  const busy = approveMutation.isPending || rejectMutation.isPending
  const activeError = approveMutation.error ?? rejectMutation.error

  const affected = [
    ...candidate.affected_records.affected_artifact_refs.map(
      (ref) => `artifact ${shortId(ref.artifact_id)}`,
    ),
    ...candidate.affected_records.affected_task_ids.map((id) => `task ${shortId(id)}`),
    ...candidate.affected_records.affected_milestone_ids.map((id) => `milestone ${shortId(id)}`),
  ]

  function approve() {
    if (!candidate.approve_target || busy) return
    const input: ApproveDecisionCandidateWire = {
      mutation: {
        expected_version: numberValue(candidate.version),
        expected_digest: null,
        idempotency_key: newIdempotencyKey(`decision-approve-${candidate.id}`),
        deduplication_key: null,
        authorization: createUserAuthorization(
          APPROVE_ACTION,
          'interactive_user_decision_candidate_approval',
        ),
      },
    }
    approveMutation.mutate(
      { approveTargetPath: candidate.approve_target.path, input },
      { onSuccess: onResolved },
    )
  }

  function reject() {
    if (busy || reason.trim().length === 0) return
    const input: RejectDecisionCandidateWire = {
      mutation: {
        expected_version: numberValue(candidate.version),
        expected_digest: null,
        idempotency_key: newIdempotencyKey(`decision-reject-${candidate.id}`),
        deduplication_key: null,
        authorization: createUserAuthorization(
          REJECT_ACTION,
          'interactive_user_decision_candidate_rejection',
        ),
      },
      reason: reason.trim(),
    }
    rejectMutation.mutate(
      { rejectTargetPath: candidate.reject_target.path, input },
      { onSuccess: onResolved },
    )
  }

  const fieldId = `decision-candidate-${candidate.id}`

  return (
    <div className="rounded-md border border-ember-border bg-ember-surface p-3">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <p className="min-w-0 break-words text-sm font-medium text-foreground">
          {candidate.question}
        </p>
        <span className="inline-flex items-center rounded-full border px-2 py-0.5 font-mono text-micro font-semibold uppercase tracking-[0.08em] text-muted-foreground">
          required principal: {candidate.required_principal}
        </span>
      </div>

      {candidate.options.length > 0 ? (
        <p className="mt-2 break-words text-xs leading-5 text-muted-foreground">
          Alternatives: <span className="text-foreground">{candidate.options.join(' · ')}</span>
        </p>
      ) : null}
      {candidate.recommendation ? (
        <p className="mt-1 break-words text-xs leading-5 text-muted-foreground">
          Recommendation: <span className="text-foreground">{candidate.recommendation}</span>
        </p>
      ) : null}
      {candidate.rationale ? (
        <p className="mt-1 break-words text-xs leading-5 text-muted-foreground">
          Rationale: <span className="text-foreground">{candidate.rationale}</span>
        </p>
      ) : null}
      <p className="mt-1 break-all font-mono text-micro text-muted-foreground">
        Class: {humanize(candidate.decision_class)} · lifecycle {candidate.lifecycle} · version v
        {numberValue(candidate.version)}
      </p>
      {affected.length > 0 ? (
        <p className="mt-1 break-all font-mono text-micro text-muted-foreground">
          Affected: {affected.join(' · ')}
        </p>
      ) : null}
      <p className="mt-1 break-all font-mono text-micro text-muted-foreground">
        Proposed by {candidate.proposed_by.display_name ?? candidate.proposed_by.id} (
        {candidate.proposed_by.kind})
      </p>

      {isMalformed ? (
        <p className="mt-3 flex items-start gap-2 rounded-md border border-destructive/40 bg-destructive/5 px-3 py-2 text-xs text-destructive">
          <WarningCircle size={15} className="mt-0.5 shrink-0" aria-hidden />
          <span>
            This candidate cannot be approved: {candidate.invalid_reason}. Reject it to clear it.
          </span>
        </p>
      ) : null}

      {activeError ? <ConflictDetails error={activeError} fallbackAuthority="decision" /> : null}

      <div className="mt-3 flex flex-wrap items-center justify-end gap-2 border-t border-border-subtle pt-3">
        {showReject ? (
          <div className="w-full space-y-2">
            <Label htmlFor={`${fieldId}-reason`}>Rejection reason</Label>
            <Textarea
              id={`${fieldId}-reason`}
              value={reason}
              onChange={(event) => setReason(event.target.value)}
              placeholder="Explain why this candidate is rejected."
              rows={2}
            />
            <div className="flex justify-end gap-2">
              <Button variant="ghost" onClick={() => setShowReject(false)} disabled={busy}>
                Cancel
              </Button>
              <Button
                variant="destructive"
                onClick={reject}
                disabled={busy || reason.trim().length === 0}
              >
                {rejectMutation.isPending ? 'Rejecting…' : 'Reject'}
              </Button>
            </div>
          </div>
        ) : (
          <>
            <Button variant="ghost" onClick={() => setShowReject(true)} disabled={busy}>
              Reject
            </Button>
            <Button onClick={approve} disabled={busy || !candidate.approve_target}>
              {approveMutation.isPending ? 'Approving…' : 'Approve'}
            </Button>
          </>
        )}
      </div>
    </div>
  )
}
