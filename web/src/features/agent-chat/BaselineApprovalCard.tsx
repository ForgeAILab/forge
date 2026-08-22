import { useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ClipboardText } from '@phosphor-icons/react'

import { ApiError, apiFetch } from '@/api/client'
import { useProjectQuery } from '@/api/hooks'
import { useAuthStore } from '@/stores/auth'
import { Button } from '@/components/ui/button'
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
 */
export function BaselineApprovalCard({ projectId }: { projectId: string }) {
  const queryClient = useQueryClient()
  const [reviewOpen, setReviewOpen] = useState(false)
  const [actionError, setActionError] = useState<string | null>(null)

  const projectQuery = useProjectQuery(projectId)
  const baselineQuery = useQuery({
    queryKey: baselineQueryKey(projectId),
    queryFn: () => fetchBaseline(projectId),
    refetchInterval: 15_000,
  })

  const data = baselineQuery.data
  const revision = data?.proposed_revision ?? data?.current_revision ?? null
  const approval =
    data?.approval && revision && data.approval.revision_id === revision.id
      ? data.approval
      : null
  const step: 'approve' | 'activate' | null = !revision
    ? null
    : revision.lifecycle === 'proposed'
      ? 'approve'
      : revision.lifecycle === 'approved' && approval
        ? 'activate'
        : null

  const mutation = useMutation({
    mutationFn: async () => {
      if (!data || !revision || !step) throw new Error('No approvable baseline revision.')
      const project = projectQuery.data
      if (!project) throw new Error('Project details are still loading. Try again.')

      let baselineVersion = data.baseline.version
      let activationApproval: ApprovalWire | null = approval
      let projectVersion = project.version

      if (step === 'approve') {
        const approved = await apiFetch<BaselineResponseWire>(
          `/projects/${projectId}/execution-baseline/${data.baseline.id}/revisions/${revision.id}/approve`,
          {
            method: 'POST',
            body: JSON.stringify({
              mutation: {
                expected_version: baselineVersion,
                expected_digest: revision.content_digest,
                idempotency_key: newIdempotencyKey(),
                deduplication_key: null,
                authorization: createAuthorization('project.execution_baseline.approve'),
              },
              revision_id: revision.id,
              content_digest: revision.content_digest,
              render_digest: revision.render_digest,
              expected_project_version: projectVersion,
            }),
          },
        )
        baselineVersion = approved.baseline.version
        activationApproval =
          approved.approval && approved.approval.revision_id === revision.id
            ? approved.approval
            : null
      }

      if (!activationApproval) {
        throw new Error('The approval receipt was not returned; refresh and try again.')
      }
      // The receipt pins the Project version observed at approval time; the
      // activation CAS must use the same value.
      projectVersion = activationApproval.expected_project_version

      return apiFetch<BaselineResponseWire>(
        `/projects/${projectId}/execution-baseline/${data.baseline.id}/activate`,
        {
          method: 'POST',
          body: JSON.stringify({
            mutation: {
              expected_version: projectVersion,
              expected_digest: null,
              idempotency_key: newIdempotencyKey(),
              deduplication_key: null,
              authorization: createAuthorization('project.execution_baseline.activate'),
            },
            baseline_id: data.baseline.id,
            revision_id: revision.id,
            approval_id: activationApproval.id,
            expected_baseline_version: baselineVersion,
            content_digest: revision.content_digest,
            render_digest: revision.render_digest,
          }),
        },
      )
    },
    onSuccess: () => {
      setReviewOpen(false)
      setActionError(null)
      void queryClient.invalidateQueries({ queryKey: baselineQueryKey(projectId) })
      void queryClient.invalidateQueries({ queryKey: ['projects', projectId] })
      void queryClient.invalidateQueries({ queryKey: ['agent-chats'] })
    },
    onError: (cause) => {
      setActionError(approvalErrorMessage(cause))
      void queryClient.invalidateQueries({ queryKey: baselineQueryKey(projectId) })
      void queryClient.invalidateQueries({ queryKey: ['projects', projectId] })
    },
  })

  if (!data || !revision || !step) return null

  const stepLabel = step === 'approve' ? 'Approve & activate' : 'Activate'

  return (
    <section
      className="mx-4 mt-3 min-w-0 rounded-lg border border-ember-border bg-card p-3 shadow-xs sm:mx-6 sm:p-4"
      aria-labelledby="baseline-approval-heading"
    >
      <div className="flex min-w-0 flex-wrap items-center justify-between gap-3">
        <div className="flex min-w-0 items-center gap-2">
          <ClipboardText size={16} className="text-primary" aria-hidden />
          <h2 id="baseline-approval-heading" className="text-sm font-semibold text-foreground">
            Execution baseline awaiting your approval
          </h2>
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
        Approving this exact revision is the moment autonomous execution starts. Nothing runs
        until you approve and activate it.
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
            <pre className="whitespace-pre-wrap font-mono text-xs leading-5 text-foreground">
              {revision.rendered_view}
            </pre>
          </div>
          <p className="mt-2 font-mono text-micro uppercase tracking-[0.08em] text-muted-foreground">
            content digest {revision.content_digest.slice(0, 16)}…
          </p>
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
