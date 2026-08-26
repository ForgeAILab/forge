import { useRef, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ClipboardText } from '@phosphor-icons/react'

import { ApiError, apiFetch } from '@/api/client'
import { useProjectQuery } from '@/api/hooks'
import { qk } from '@/api/query-keys'
import { useProjectExecutionSetupQuery } from '@/features/project-execution/hooks'
import { useAuthStore } from '@/stores/auth'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/cn'
import { BaselineReviewSections } from '@/features/agent-chat/BaselineReviewSections'
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import type {
  AuthorizationProvenance,
  ExecutionBaselineApproval,
  ExecutionBaselineLifecycle,
  ExecutionBaselineRevision,
} from '@/types/generated'

// Generated ts-rs bindings type i64 as bigint; the wire carries plain JSON
// numbers (same convention product-genesis uses for optimistic versions).
type BaselineWire = {
  id: string
  project_id: string
  current_revision_id: string | null
  lifecycle: ExecutionBaselineLifecycle
  version: number
}

type RevisionWire = Omit<ExecutionBaselineRevision, 'revision_number'> & {
  revision_number: number
}

type ApprovalWire = Omit<ExecutionBaselineApproval, 'expected_project_version'> & {
  expected_project_version: number
}

type BaselineResponseWire = {
  baseline: BaselineWire
  current_revision: RevisionWire | null
  proposed_revision: RevisionWire | null
  approval: ApprovalWire | null
  integrity_issue?: {
    revision_id: string
    baseline_id: string
    field_path: string
    invalid_values: string[]
    diagnostic: string
    successor_revision_id: string | null
    conflict_id: string | null
    reconciliation_id: string | null
  } | null
}

// The atomic approve-and-activate response is receipt-first (D18/8.3.2): the
// identity fields are always populated once the command committed, and
// `projection` is a best-effort full baseline read that can be absent
// (`refresh_required: true`) without the command itself having failed.
type ApproveAndActivateResponseWire = {
  baseline_id: string
  revision_id: string
  approval_id: string
  content_digest: string
  render_digest: string
  projection: BaselineResponseWire | null
  refresh_required: boolean
}

const baselineQueryKey = (projectId: string) =>
  ['projects', projectId, 'execution-baseline'] as const

function createAuthorization(action: string): AuthorizationProvenance {
  const user = useAuthStore.getState().user
  if (!user) throw new Error('Sign in again before approving the execution baseline.')
  return {
    principal: { kind: 'user', id: user.id, display_name: user.display_name ?? null },
    authorization_basis: 'interactive_user_approval',
    action,
    event_id:
      typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function'
        ? crypto.randomUUID()
        : `${action}-${Date.now()}`,
    occurred_at: new Date().toISOString(),
  } as AuthorizationProvenance
}

function newIdempotencyKey(): string {
  return typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function'
    ? crypto.randomUUID()
    : `baseline-${Date.now()}-${Math.random().toString(36).slice(2)}`
}

async function fetchBaseline(projectId: string): Promise<BaselineResponseWire | null> {
  try {
    return await apiFetch<BaselineResponseWire>(`/projects/${projectId}/execution-baseline`)
  } catch (error) {
    if (error instanceof ApiError && error.status === 404) return null
    throw error
  }
}

/** Is the exact revision this click targeted already the Project's active
 * baseline? D18/8.3.2: before showing a conflict, check this first -- a
 * lost response, a double submit, or a race this exact command itself won
 * must all render success, never the stale-baseline failure F13 reported.
 * The server's own route performs the same check for the atomic command;
 * this is the client-side half of that contract, and it also covers the
 * separate "Start approved work" activation call. */
function isExactRevisionActive(baseline: BaselineResponseWire | null, revisionId: string) {
  return (
    baseline?.baseline.lifecycle === 'active' &&
    baseline.baseline.current_revision_id === revisionId
  )
}

function approvalErrorMessage(cause: unknown): string {
  if (cause instanceof ApiError) {
    if (cause.status === 409 || cause.status === 412) {
      return 'The baseline or Project changed while this approval was open. Review the refreshed revision and approve again.'
    }
    if (cause.status === 403) return 'This action is not authorized for the current account.'
    return cause.message
  }
  return cause instanceof Error ? cause.message : 'Approval failed.'
}

/**
 * Pinned approval card for the Project Agent Chat.
 *
 * When the agent proposes an execution-baseline revision, the approval is the
 * single gate that starts autonomous work — so the request surfaces here, in
 * the chat, with a review dialog and a one-click approve-and-activate that
 * writes the durable approval receipt through the normal REST contract.
 *
 * "Approve plan & start work" is one atomic, replay-exact server command
 * (D18/F13): approval, activation, governance promotion, receipt, and events
 * commit together, so a lost response can only ever replay the committed
 * success. "Start approved work" (an already-approved revision) stays the
 * separate exact replay-safe activation call.
 */
export function BaselineApprovalCard({
  projectId,
  className,
}: {
  projectId: string
  className?: string
}) {
  const queryClient = useQueryClient()
  const [reviewOpen, setReviewOpen] = useState(false)
  const [actionError, setActionError] = useState<string | null>(null)

  const projectQuery = useProjectQuery(projectId)
  const setupQuery = useProjectExecutionSetupQuery(projectId)
  const baselineQuery = useQuery({
    queryKey: baselineQueryKey(projectId),
    queryFn: () => fetchBaseline(projectId),
    refetchInterval: 15_000,
  })

  const data = baselineQuery.data
  const revision = data?.proposed_revision ?? data?.current_revision ?? null
  const approval =
    data?.approval && revision && data.approval.revision_id === revision.id ? data.approval : null
  const correctionRequired = Boolean(data?.integrity_issue)
  const step: 'approve' | 'approve_correction' | 'activate' | null = !revision
    ? null
    : revision.lifecycle === 'proposed'
      ? correctionRequired
        ? 'approve_correction'
        : 'approve'
      : revision.lifecycle === 'approved' && approval
        ? correctionRequired
          ? null
          : 'activate'
        : null

  // A newer draft can appear beside an already-active baseline (the Project
  // Agent starts drafting the next revision as soon as this one activates).
  // That draft is valid immutable history, not evidence anything failed --
  // it must never be silently swallowed into a blank card, and never read as
  // a failed approval (D18/8.3.2, F13).
  const unapprovedFutureDraft =
    !step &&
    data?.current_revision?.lifecycle === 'active' &&
    data.proposed_revision &&
    data.proposed_revision.lifecycle === 'draft' &&
    data.proposed_revision.id !== data.current_revision.id
      ? data.proposed_revision
      : null
  const correctionDraft =
    correctionRequired && data?.proposed_revision?.lifecycle === 'draft'
      ? data.proposed_revision
      : null
  const approvedCorrection =
    correctionRequired && revision?.lifecycle === 'approved' && approval ? revision : null

  // One stable idempotency key per (step, revision): reused across retries
  // of the exact same click so a lost response replays the committed
  // outcome instead of minting a second, unrelated command (D18/8.3.1). A
  // different revision or a different step is a genuinely different user
  // gesture and gets its own key.
  const idempotencyKeyRef = useRef<{ scope: string; key: string } | null>(null)
  const stableIdempotencyKey = (scope: string): string => {
    if (idempotencyKeyRef.current?.scope !== scope) {
      idempotencyKeyRef.current = { scope, key: newIdempotencyKey() }
    }
    return idempotencyKeyRef.current.key
  }

  const mutation = useMutation({
    mutationFn: async () => {
      if (!data || !revision || !step) throw new Error('No approvable baseline revision.')
      const project = projectQuery.data
      if (!project) throw new Error('Project details are still loading. Try again.')

      if (step === 'approve_correction') {
        const scope = `approve-correction:${revision.id}`
        await apiFetch<BaselineResponseWire>(
          `/projects/${projectId}/execution-baseline/${data.baseline.id}/revisions/${revision.id}/approve`,
          {
            method: 'POST',
            body: JSON.stringify({
              mutation: {
                expected_version: data.baseline.version,
                expected_digest: null,
                idempotency_key: stableIdempotencyKey(scope),
                deduplication_key: null,
                authorization: createAuthorization('project.execution_baseline.approve'),
              },
              revision_id: revision.id,
              content_digest: revision.content_digest,
              render_digest: revision.render_digest,
              expected_project_version: project.version,
            }),
          },
        )
        return { refreshRequired: false }
      }

      if (step === 'approve') {
        const scope = `approve-and-activate:${revision.id}`
        try {
          const result = await apiFetch<ApproveAndActivateResponseWire>(
            `/projects/${projectId}/execution-baseline/${data.baseline.id}/revisions/${revision.id}/approve-and-activate`,
            {
              method: 'POST',
              body: JSON.stringify({
                mutation: {
                  expected_version: project.version,
                  expected_digest: null,
                  idempotency_key: stableIdempotencyKey(scope),
                  deduplication_key: null,
                  authorization: createAuthorization(
                    'project.execution_baseline.approve_and_activate',
                  ),
                },
                revision_id: revision.id,
                content_digest: revision.content_digest,
                render_digest: revision.render_digest,
                expected_baseline_version: data.baseline.version,
              }),
            },
          )
          // "The requested active revision must render success even when a
          // follow-up refresh is needed" (8.3.2): a committed outcome with
          // no projection is still success, never an error.
          return { refreshRequired: result.refresh_required }
        } catch (cause) {
          if (cause instanceof ApiError && (cause.status === 409 || cause.status === 412)) {
            const refreshed = await fetchBaseline(projectId)
            if (isExactRevisionActive(refreshed, revision.id)) {
              return { refreshRequired: false }
            }
          }
          throw cause
        }
      }

      // step === 'activate': the already-approved "Start approved work"
      // gesture keeps using the separate exact replay-safe activation call.
      if (!approval)
        throw new Error('The approval receipt was not returned; refresh and try again.')
      const scope = `activate:${revision.id}`
      try {
        await apiFetch<BaselineResponseWire>(
          `/projects/${projectId}/execution-baseline/${data.baseline.id}/activate`,
          {
            method: 'POST',
            body: JSON.stringify({
              mutation: {
                expected_version: approval.expected_project_version,
                expected_digest: null,
                idempotency_key: stableIdempotencyKey(scope),
                deduplication_key: null,
                authorization: createAuthorization('project.execution_baseline.activate'),
              },
              baseline_id: data.baseline.id,
              revision_id: revision.id,
              approval_id: approval.id,
              expected_baseline_version: data.baseline.version,
              content_digest: revision.content_digest,
              render_digest: revision.render_digest,
            }),
          },
        )
        return { refreshRequired: false }
      } catch (cause) {
        if (cause instanceof ApiError && (cause.status === 409 || cause.status === 412)) {
          const refreshed = await fetchBaseline(projectId)
          if (isExactRevisionActive(refreshed, revision.id)) {
            return { refreshRequired: false }
          }
        }
        throw cause
      }
    },
    onSuccess: (result) => {
      setReviewOpen(false)
      setActionError(null)
      if (result.refreshRequired) {
        void queryClient.refetchQueries({ queryKey: baselineQueryKey(projectId) })
      } else {
        void queryClient.invalidateQueries({ queryKey: baselineQueryKey(projectId) })
      }
      void queryClient.invalidateQueries({ queryKey: ['projects', projectId] })
      void queryClient.invalidateQueries({ queryKey: qk.projectReconciliations(projectId) })
      void queryClient.invalidateQueries({ queryKey: ['agent-chats'] })
    },
    onError: (cause) => {
      setActionError(approvalErrorMessage(cause))
      void queryClient.invalidateQueries({ queryKey: baselineQueryKey(projectId) })
      void queryClient.invalidateQueries({ queryKey: ['projects', projectId] })
    },
  })

  // A revision can sit `approved` with a live approval receipt even while
  // the Project's execution gate is actually blocked by an outstanding
  // reconciliation (for example an invalid active baseline). Never show
  // this card's "approve/activate" copy in that state — it would read
  // exactly like the F12(b) bug this design forbids, offering a baseline
  // approval action for a blocker a baseline approval cannot resolve. The
  // canonical reconciliation review card is the one authorized recovery
  // path there (D16/D17).
  const reconciliationRequired = setupQuery.data?.execution_gate === 'reconciliation_required'

  if (!data || (reconciliationRequired && !correctionRequired)) return null

  if (!revision || !step) {
    if (correctionDraft) {
      return (
        <section
          className={cn(
            'mx-4 mt-3 min-w-0 scroll-mt-3 rounded-lg border border-ember-border bg-ember-surface px-3 py-3 sm:mx-6 sm:px-4',
            className,
          )}
          aria-label="Execution baseline correction draft"
        >
          <p className="font-mono text-micro font-semibold uppercase tracking-[0.1em] text-primary">
            Active plan repair · draft only
          </p>
          <p className="mt-1 text-sm font-medium text-foreground">
            Forge preserved the invalid active revision and prepared a typed correction draft.
          </p>
          <p className="mt-1 text-xs leading-5 text-muted-foreground">
            Unsupported values ({data.integrity_issue?.invalid_values.join(', ')}) were quarantined,
            not reinterpreted. Review and propose revision {correctionDraft.revision_number} before
            asking the user to approve it.
          </p>
        </section>
      )
    }
    if (approvedCorrection) {
      return (
        <section
          className={cn(
            'mx-4 mt-3 min-w-0 scroll-mt-3 rounded-lg border border-ember-border bg-ember-surface px-3 py-3 sm:mx-6 sm:px-4',
            className,
          )}
          aria-label="Approved execution baseline correction"
        >
          <p className="font-mono text-micro font-semibold uppercase tracking-[0.1em] text-primary">
            Correction approved · not active
          </p>
          <p className="mt-1 text-xs leading-5 text-muted-foreground">
            Revision {approvedCorrection.revision_number} is approved. Complete the reconciliation
            review below to atomically replace the invalid active revision and resume work.
          </p>
        </section>
      )
    }
    if (!unapprovedFutureDraft) return null
    return (
      <section
        className={cn(
          'mx-4 mt-3 min-w-0 scroll-mt-3 rounded-lg border border-border-subtle bg-muted/20 px-3 py-2 sm:mx-6',
          className,
        )}
        aria-label="Unapproved future baseline draft"
      >
        <p className="font-mono text-micro leading-5 text-muted-foreground">
          <span className="rounded-full border border-border bg-muted/30 px-2 py-0.5 uppercase tracking-[0.08em]">
            Draft — not active
          </span>{' '}
          A newer plan revision is being drafted. This is unapproved future work, not a sign that
          anything failed — the active plan above is still running.
        </p>
      </section>
    )
  }

  const stepLabel =
    step === 'approve'
      ? 'Approve plan & start work'
      : step === 'approve_correction'
        ? 'Approve corrected plan'
        : 'Start approved work'

  return (
    <section
      id="execution-approval"
      className={cn(
        'mx-4 mt-3 min-w-0 scroll-mt-3 rounded-lg border border-ember-border bg-card p-3 shadow-xs sm:mx-6 sm:p-4',
        className,
      )}
      aria-labelledby="baseline-approval-heading"
    >
      <div className="flex min-w-0 flex-wrap items-center justify-between gap-3">
        <div className="flex min-w-0 items-center gap-2">
          <ClipboardText size={16} className="text-primary" aria-hidden />
          <div className="min-w-0">
            <p className="font-mono text-micro font-semibold uppercase tracking-[0.1em] text-primary">
              {step === 'approve_correction' ? 'Plan repair · user approval' : 'Step 2 of 2 · Start building'}
            </p>
            <h2
              id="baseline-approval-heading"
              className="mt-1 text-sm font-semibold text-foreground"
            >
              {step === 'approve_correction'
                ? 'Approve the corrected implementation plan'
                : 'Approve the implementation plan'}
            </h2>
          </div>
          <span className="rounded-full border border-border bg-muted/30 px-2 py-0.5 font-mono text-micro uppercase tracking-[0.08em] text-muted-foreground">
            Revision {revision.revision_number} · {revision.lifecycle}
          </span>
        </div>
        <div className="flex shrink-0 items-center gap-2">
          <Button variant="outline" size="sm" onClick={() => setReviewOpen(true)}>
            Review
          </Button>
          <Button
            size="sm"
            disabled={mutation.isPending || projectQuery.isLoading}
            onClick={() => {
              setActionError(null)
              mutation.mutate()
            }}
          >
            {mutation.isPending ? 'Working…' : stepLabel}
          </Button>
        </div>
      </div>
      <p className="mt-1.5 max-w-2xl text-xs leading-5 text-muted-foreground">
        {step === 'approve_correction'
          ? 'This approval records the exact corrected successor but does not activate it. The reconciliation review remains the explicit step that replaces the preserved invalid revision.'
          : 'Your Project is already approved. This one action authorizes every Task covered by this exact plan to change the repository; Forge will not ask you to approve each Task.'}
      </p>
      {actionError ? (
        <p className="mt-2 text-xs leading-5 text-destructive" role="alert">
          {actionError}
        </p>
      ) : null}

      <Dialog open={reviewOpen} onOpenChange={setReviewOpen} ariaLabel="Review execution baseline">
        <DialogContent className="max-w-3xl">
          <DialogHeader>
            <DialogTitle>Execution baseline · revision {revision.revision_number}</DialogTitle>
          </DialogHeader>
          <div className="max-h-[60vh] overflow-auto rounded-md border border-border-subtle bg-muted/20 p-3">
            <BaselineReviewSections
              content={revision.content}
              previousContent={
                data.current_revision && data.current_revision.id !== revision.id
                  ? data.current_revision.content
                  : null
              }
              renderedView={revision.rendered_view}
              contentDigest={revision.content_digest}
            />
          </div>
          {actionError ? (
            <p className="mt-2 text-xs leading-5 text-destructive" role="alert">
              {actionError}
            </p>
          ) : null}
          <DialogFooter>
            <Button variant="outline" onClick={() => setReviewOpen(false)}>
              Cancel
            </Button>
            <Button
              disabled={mutation.isPending || projectQuery.isLoading}
              onClick={() => {
                setActionError(null)
                mutation.mutate()
              }}
            >
              {mutation.isPending ? 'Working…' : stepLabel}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </section>
  )
}
