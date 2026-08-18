import { useEffect, useRef, useState } from 'react'
import { Link } from '@tanstack/react-router'
import {
  ArrowClockwise,
  ArrowRight,
  CaretDown,
  CheckCircle,
  CircleNotch,
  FileText,
  Flask,
  GitDiff,
  ShieldCheck,
  WarningCircle,
  X,
} from '@phosphor-icons/react'
import { ApiError } from '@/api/client'
import { useAuthStore } from '@/stores/auth'
import { ConflictDetails } from '@/components/conflict-details'
import { Button } from '@/components/ui/button'
import { ErrorCard } from '@/components/chat/error-card'
import { LoadingState } from '@/components/chat/loading-state'
import {
  useApproveProductGenesisCharterRevisionMutation,
  useCancelProductGenesisMutation,
  useCreateProjectFromCharterApprovalMutation,
  useProductGenesisActiveQuery,
  useProductGenesisCharterQuery,
} from './hooks'
import type {
  ProductAgentSelection,
  ProductGenesisCharterResponse,
  ProductGenesisSession,
  ProjectCharterApproval,
  ProjectCharterReadiness,
  ProjectCharterRevision,
} from './types'
import { productGenesisVersion } from './types'

function lifecycleLabel(value: string): string {
  return value.replaceAll('_', ' ')
}

function versionNumber(value: number | bigint): number {
  return typeof value === 'bigint' ? Number(value) : value
}

function shortDigest(value: string): string {
  if (value.length <= 18) return value
  return `${value.slice(0, 10)}…${value.slice(-6)}`
}

function createEventId(prefix: string): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID()
  }
  return `${prefix}-${Date.now()}`
}

function createAuthorization(action: string) {
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

function mutationId(prefix: string, identity: string): string {
  return `${prefix}:${identity}`
}

function getApiErrorMessage(cause: unknown, fallback: string): string {
  if (cause instanceof ApiError) {
    if (cause.status === 409 || cause.status === 412) {
      return 'The Charter changed while this action was open. Refresh the exact revision and review it again.'
    }
    if (cause.status === 403) return 'This action is not authorized for the current account.'
    return cause.message || fallback
  }
  return cause instanceof Error ? cause.message : fallback
}

function isConflict(cause: unknown): boolean {
  return cause instanceof ApiError && (cause.status === 409 || cause.status === 412)
}

function agentLabel(agent: ProductAgentSelection | null): string {
  if (!agent) return 'No Project Agent selected'
  return agent.display_name ? `${agent.display_name} · ${agent.identity_id}` : agent.identity_id
}

function readinessLabel(readiness: ProjectCharterReadiness | null): string {
  if (!readiness) return 'Not evaluated'
  return readiness.status === 'ready' ? 'Ready for approval' : 'Blocked'
}

function RevisionDigestRow({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex min-w-0 items-start justify-between gap-3 border-b border-border-subtle py-2 last:border-b-0">
      <span className="shrink-0 text-xs text-muted-foreground">{label}</span>
      <code
        className="min-w-0 break-all text-right font-mono text-micro text-foreground"
        title={value}
      >
        {shortDigest(value)}
      </code>
    </div>
  )
}

function ReadinessPanel({ readiness }: { readiness: ProjectCharterReadiness | null }) {
  if (!readiness) {
    return (
      <section
        className="rounded-lg border border-border-subtle bg-muted/20 p-3"
        aria-labelledby="genesis-readiness-heading"
      >
        <div className="flex items-center gap-2">
          <WarningCircle size={16} className="text-warning" aria-hidden />
          <h3 id="genesis-readiness-heading" className="text-sm font-semibold text-foreground">
            Readiness not evaluated
          </h3>
        </div>
        <p className="mt-1 text-xs leading-5 text-muted-foreground">
          Forge has not published a readiness result for this revision yet. Approval remains
          disabled.
        </p>
      </section>
    )
  }

  const blockedGaps = readiness.gaps.filter((gap) => gap.blocking)
  const advisoryGaps = readiness.gaps.filter((gap) => !gap.blocking)
  const isReady = readiness.status === 'ready' && blockedGaps.length === 0

  return (
    <section
      className="rounded-lg border border-border-subtle bg-muted/20 p-3"
      aria-labelledby="genesis-readiness-heading"
    >
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="flex min-w-0 items-center gap-2">
          {isReady ? (
            <CheckCircle size={16} className="text-success" weight="fill" aria-hidden />
          ) : (
            <WarningCircle size={16} className="text-warning" aria-hidden />
          )}
          <div className="min-w-0">
            <h3 id="genesis-readiness-heading" className="text-sm font-semibold text-foreground">
              {readinessLabel(readiness)}
            </h3>
            <p className="text-micro text-muted-foreground">
              Policy {readiness.policy_revision} · evaluated{' '}
              {new Date(readiness.evaluated_at).toLocaleString()}
            </p>
          </div>
        </div>
        <code
          className="break-all font-mono text-micro text-muted-foreground"
          title={readiness.readiness_digest}
        >
          {shortDigest(readiness.readiness_digest)}
        </code>
      </div>
      {readiness.gaps.length > 0 ? (
        <ul className="mt-3 space-y-2" aria-label="Charter readiness gaps">
          {readiness.gaps.map((gap) => (
            <li
              key={`${gap.code}:${gap.section ?? ''}`}
              className="flex min-w-0 gap-2 text-xs leading-5"
            >
              <span
                className={gap.blocking ? 'mt-1 text-destructive' : 'mt-1 text-warning'}
                aria-hidden
              >
                •
              </span>
              <span className="min-w-0 text-foreground">
                <span className="font-mono text-micro uppercase tracking-[0.08em] text-muted-foreground">
                  {gap.blocking ? 'blocking' : 'advisory'} · {gap.kind.replaceAll('_', ' ')}
                </span>{' '}
                {gap.message}
                {gap.section ? (
                  <span className="text-muted-foreground"> · {gap.section}</span>
                ) : null}
              </span>
            </li>
          ))}
        </ul>
      ) : (
        <p className="mt-2 text-xs text-muted-foreground">No typed gaps were reported.</p>
      )}
      {advisoryGaps.length > 0 ? (
        <p className="mt-2 text-micro text-muted-foreground">
          {advisoryGaps.length} advisory item{advisoryGaps.length === 1 ? '' : 's'} remain visible
          for review.
        </p>
      ) : null}
    </section>
  )
}

function AgentSelectionPanel({ agent }: { agent: ProductAgentSelection | null }) {
  return (
    <section
      className="rounded-lg border border-border-subtle bg-muted/20 p-3"
      aria-labelledby="genesis-agent-heading"
    >
      <div className="flex items-center gap-2">
        <ShieldCheck size={16} className="text-primary" aria-hidden />
        <h3 id="genesis-agent-heading" className="text-sm font-semibold text-foreground">
          Selected Project Agent
        </h3>
      </div>
      {agent ? (
        <dl className="mt-3 grid min-w-0 gap-2 text-xs sm:grid-cols-2">
          <div className="min-w-0">
            <dt className="font-mono text-micro uppercase tracking-[0.08em] text-muted-foreground">
              Identity
            </dt>
            <dd className="mt-0.5 break-all text-foreground">{agentLabel(agent)}</dd>
          </div>
          <div className="min-w-0">
            <dt className="font-mono text-micro uppercase tracking-[0.08em] text-muted-foreground">
              Profile revision
            </dt>
            <dd className="mt-0.5 break-all font-mono text-micro text-foreground">
              {agent.profile_revision_id}
            </dd>
          </div>
          <div className="min-w-0">
            <dt className="font-mono text-micro uppercase tracking-[0.08em] text-muted-foreground">
              Operating skill
            </dt>
            <dd className="mt-0.5 break-all font-mono text-micro text-foreground">
              {agent.operating_skill_revision}
            </dd>
          </div>
          <div className="min-w-0">
            <dt className="font-mono text-micro uppercase tracking-[0.08em] text-muted-foreground">
              Policy digest
            </dt>
            <dd className="mt-0.5 break-all font-mono text-micro text-foreground">
              {shortDigest(agent.policy_digest)}
            </dd>
          </div>
        </dl>
      ) : (
        <p className="mt-2 text-xs leading-5 text-muted-foreground">
          Forge has not selected an eligible Project Agent revision set yet. Exact approval stays
          disabled until all responder revisions are present.
        </p>
      )}
    </section>
  )
}

function CharterContentSummary({ revision }: { revision: ProjectCharterRevision }) {
  const { content } = revision
  const lines = [
    ['Problem', content.problem_and_people.problem_or_opportunity],
    ['Primary outcome', content.core_experience.primary_outcome],
    ['Must-have outcomes', content.scope.must_have_outcomes.join(' · ')],
    ['Non-goals', content.scope.explicit_non_goals.join(' · ')],
    [
      'Success boundary',
      content.success.qualitative_outcome ?? content.success.success_signals.join(' · '),
    ],
  ].filter(([, value]) => value)

  return (
    <div className="grid gap-3 sm:grid-cols-2">
      {lines.map(([label, value]) => (
        <div
          key={label}
          className="min-w-0 rounded-md border border-border-subtle bg-background/50 p-2.5"
        >
          <p className="font-mono text-micro uppercase tracking-[0.08em] text-muted-foreground">
            {label}
          </p>
          <p className="mt-1 min-w-0 break-words text-xs leading-5 text-foreground">{value}</p>
        </div>
      ))}
    </div>
  )
}

function CharterReviewPanel({
  active,
  charterData,
  selectedRevisionId,
  onRevisionChange,
  approval,
  approvalPending,
  createPending,
  onApprove,
  onCreate,
  onRefresh,
  stale,
  conflict,
  conflictError,
}: {
  active: ProductGenesisSession
  charterData: ProductGenesisCharterResponse
  selectedRevisionId: string | null
  onRevisionChange: (revisionId: string) => void
  approval: ProjectCharterApproval | null
  approvalPending: boolean
  createPending: boolean
  onApprove: (revision: ProjectCharterRevision) => void
  onCreate: () => void
  onRefresh: () => void
  stale: boolean
  conflict: boolean
  conflictError: unknown
}) {
  const {
    charter,
    revisions,
    current_draft_revision: draft,
    selected_project_agent: agent,
  } = charterData
  const selectedRevision =
    revisions.find((revision) => revision.id === selectedRevisionId) ??
    draft ??
    revisions[0] ??
    null
  const isCurrentCandidate = Boolean(selectedRevision && draft && selectedRevision.id === draft.id)
  const readiness = selectedRevision?.readiness ?? null
  const canApprove = Boolean(
    charter &&
    selectedRevision &&
    isCurrentCandidate &&
    selectedRevision.lifecycle !== 'approved' &&
    readiness?.status === 'ready' &&
    agent &&
    !stale &&
    !conflict,
  )
  const projectId = active.project_id

  if (!charter || !selectedRevision) {
    return (
      <section
        className="basis-full rounded-lg border border-border-subtle bg-card p-4"
        aria-labelledby="genesis-charter-heading"
      >
        <div className="flex items-center gap-2">
          <FileText size={16} className="text-muted-foreground" aria-hidden />
          <h2 id="genesis-charter-heading" className="text-sm font-semibold text-foreground">
            Charter draft
          </h2>
        </div>
        <p className="mt-2 max-w-2xl text-xs leading-5 text-muted-foreground">
          The Main Agent has not saved a typed Charter revision yet. Continue the Main Chat to
          establish the project identity, outcome, boundaries, and success evidence. No Project
          exists from this Genesis session.
        </p>
        <p className="mt-2 font-mono text-micro uppercase tracking-[0.08em] text-warning">
          Approval unavailable · no exact revision
        </p>
      </section>
    )
  }

  return (
    <section
      className="basis-full min-w-0 rounded-lg border border-ember-border bg-card p-3 shadow-xs sm:p-4"
      aria-labelledby="genesis-charter-heading"
    >
      <header className="flex min-w-0 flex-wrap items-start justify-between gap-3 border-b border-border-subtle pb-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <FileText size={16} className="text-primary" aria-hidden />
            <h2 id="genesis-charter-heading" className="text-base font-semibold text-foreground">
              Project Charter
            </h2>
            <span className="rounded-full border border-border bg-muted/30 px-2 py-0.5 font-mono text-micro uppercase tracking-[0.08em] text-muted-foreground">
              Revision {versionNumber(selectedRevision.revision_number)} ·{' '}
              {lifecycleLabel(selectedRevision.lifecycle)}
            </span>
          </div>
          <p className="mt-1 max-w-2xl text-xs leading-5 text-muted-foreground">
            This is the exact revision Forge can approve. Chat history and memory inform discovery,
            but they do not become project truth until this revision is explicitly approved.
          </p>
        </div>
        <div className="flex min-w-0 items-center gap-2">
          <label className="flex min-w-0 items-center gap-2 text-xs text-muted-foreground">
            <span className="sr-only">Inspect Charter revision</span>
            <select
              value={selectedRevision.id}
              onChange={(event) => onRevisionChange(event.target.value)}
              className="max-w-[13rem] rounded-md border border-border bg-background px-2 py-1.5 text-xs text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
              aria-label="Inspect Charter revision"
            >
              {revisions.map((revision) => (
                <option key={revision.id} value={revision.id}>
                  Revision {versionNumber(revision.revision_number)} · {revision.lifecycle}
                </option>
              ))}
            </select>
          </label>
          <Button
            type="button"
            variant="ghost"
            size="sm"
            onClick={onRefresh}
            aria-label="Refresh Charter"
          >
            <ArrowClockwise size={14} aria-hidden />
            <span className="sr-only sm:not-sr-only">Refresh</span>
          </Button>
        </div>
      </header>

      <div className="mt-3 grid min-w-0 gap-3 lg:grid-cols-[minmax(0,1.45fr)_minmax(16rem,0.8fr)]">
        <div className="min-w-0 space-y-3">
          <div className="rounded-lg border border-border-subtle bg-background/30 p-3">
            <div className="flex flex-wrap items-start justify-between gap-2">
              <div className="min-w-0">
                <p className="font-mono text-micro uppercase tracking-[0.1em] text-primary">
                  {selectedRevision.content.identity.working_name}
                </p>
                <h3 className="mt-1 break-words text-lg font-semibold tracking-tight text-foreground">
                  {selectedRevision.content.identity.one_line_vision}
                </h3>
                <p className="mt-1 text-xs text-muted-foreground">
                  {selectedRevision.project_mode} mode · {selectedRevision.maturity} maturity ·
                  Charter version {versionNumber(charter.version)}
                </p>
              </div>
              <div
                className="flex shrink-0 items-center gap-1.5 rounded-md border border-border-subtle bg-muted/20 px-2 py-1 font-mono text-micro text-muted-foreground"
                title={selectedRevision.id}
              >
                <GitDiff size={13} aria-hidden />
                <span className="max-w-[9rem] break-all">{shortDigest(selectedRevision.id)}</span>
              </div>
            </div>
            <div className="mt-3">
              <p className="mb-2 font-mono text-micro uppercase tracking-[0.1em] text-muted-foreground">
                Material change summary
              </p>
              <p className="text-xs leading-5 text-foreground">
                {selectedRevision.provenance.material_diff ??
                  selectedRevision.provenance.change_summary ??
                  'Initial Charter revision; no prior revision to compare.'}
              </p>
            </div>
            <div className="mt-3">
              <CharterContentSummary revision={selectedRevision} />
            </div>
            <details className="mt-3 rounded-md border border-border-subtle bg-muted/10 p-2.5">
              <summary className="cursor-pointer text-xs font-medium text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">
                Inspect rendered Charter and semantic diff
              </summary>
              <pre className="mt-2 max-h-72 overflow-auto whitespace-pre-wrap break-words rounded-md bg-background p-2.5 font-mono text-micro leading-5 text-foreground">
                {selectedRevision.rendered_view || 'No rendered view was saved for this revision.'}
              </pre>
            </details>
            <div className="mt-3 rounded-md border border-border-subtle bg-muted/10 px-3 py-2">
              <RevisionDigestRow label="Content digest" value={selectedRevision.content_digest} />
              <RevisionDigestRow
                label="Rendered-view digest"
                value={selectedRevision.render_digest}
              />
              <RevisionDigestRow
                label="Schema / render"
                value={`${selectedRevision.schema_version} · ${selectedRevision.render_version}`}
              />
            </div>
          </div>
        </div>

        <div className="min-w-0 space-y-3">
          <ReadinessPanel readiness={readiness} />
          <AgentSelectionPanel agent={agent} />
          <section
            className="rounded-lg border border-ember-border bg-ember-surface p-3"
            aria-labelledby="genesis-approval-heading"
          >
            <div className="flex items-center gap-2">
              <ShieldCheck size={16} className="text-primary" aria-hidden />
              <h3 id="genesis-approval-heading" className="text-sm font-semibold text-foreground">
                Exact approval
              </h3>
            </div>
            <p className="mt-2 text-xs leading-5 text-muted-foreground">
              Approval records your explicit decision for this revision, both digests, the expected
              Charter version, and the selected Project Agent revision set.
            </p>
            {stale || conflict ? (
              <p
                className="mt-2 rounded-md border border-warning/40 bg-warning/10 px-2.5 py-2 text-xs leading-5 text-warning"
                role="status"
              >
              {conflict ? 'Conflict detected: server truth changed.' : 'This view is stale.'}{' '}
                Refresh before approving an exact revision.
              </p>
            ) : null}
            {conflict ? (
              <ConflictDetails error={conflictError} fallbackAuthority="Product Genesis Charter approval" />
            ) : null}
            <Button
              type="button"
              className="mt-3 w-full justify-center"
              onClick={() => onApprove(selectedRevision)}
              disabled={!canApprove || approvalPending}
              title={
                canApprove
                  ? 'Approve this exact Charter revision'
                  : 'Readiness, current revision, digests, and Project Agent selection must match Forge'
              }
            >
              {approvalPending ? (
                <CircleNotch size={15} className="animate-spin" aria-hidden />
              ) : (
                <CheckCircle size={15} aria-hidden />
              )}
              Approve exact Charter revision
            </Button>
            {!canApprove && !stale && !conflict ? (
              <p className="mt-2 text-micro leading-4 text-muted-foreground">
                Approval stays disabled until the displayed revision is current, readiness is ready,
                and all Project Agent revision identifiers are present.
              </p>
            ) : null}
            {approval ? (
              <div className="mt-3 border-t border-ember-border pt-3">
                <p className="font-mono text-micro uppercase tracking-[0.1em] text-muted-foreground">
                  Receipt · {approval.state}
                </p>
                <p className="mt-1 break-words text-xs text-foreground">
                  Revision {approval.charter_revision_id} ·{' '}
                  {shortDigest(approval.charter_content_digest)} ·{' '}
                  {shortDigest(approval.charter_render_digest)}
                </p>
                {approval.state === 'active' && !projectId ? (
                  <>
                    <p className="mt-2 text-xs leading-5 text-muted-foreground">
                      No Project exists yet. The next action creates the Project, Project Chat, and
                      bounded handoff atomically from this single-use receipt.
                    </p>
                    <Button
                      type="button"
                      variant="outline"
                      className="mt-3 w-full justify-center"
                      onClick={onCreate}
                      disabled={createPending}
                    >
                      {createPending ? (
                        <CircleNotch size={15} className="animate-spin" aria-hidden />
                      ) : (
                        <ArrowRight size={15} aria-hidden />
                      )}
                      Create Project and hand off
                    </Button>
                  </>
                ) : null}
                {approval.state === 'consumed' && projectId ? (
                  <p className="mt-2 text-xs text-success">
                    Receipt consumed by Project {projectId}.
                  </p>
                ) : null}
              </div>
            ) : null}
          </section>
        </div>
      </div>

      {active.lifecycle === 'handed_off' && projectId ? (
        <div className="mt-3 flex flex-wrap items-center justify-between gap-3 rounded-lg border border-success/40 bg-success/10 px-3 py-2.5">
          <div className="min-w-0">
            <p className="text-xs font-semibold text-foreground">Project handoff complete</p>
            <p className="mt-0.5 break-words text-xs text-muted-foreground">
              The Project Agent owns Project-local planning from the exact approved Charter. Main
              Agent discovery is complete.
            </p>
          </div>
          <Link
            to="/projects/$projectId/chat"
            params={{ projectId }}
            className="inline-flex shrink-0 items-center gap-1 text-xs font-medium text-primary underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          >
            Continue with Project Agent
            <ArrowRight size={13} aria-hidden />
          </Link>
        </div>
      ) : null}
    </section>
  )
}
/** Compact Genesis status chip for the chat header: lifecycle + cancel. */
export function ProductGenesisControls() {
  const activeQuery = useProductGenesisActiveQuery()
  const cancelMutation = useCancelProductGenesisMutation()
  const active = activeQuery.data?.session ?? null
  const [error, setError] = useState<string | null>(null)
  const projectId = active?.project_id ?? null

  async function cancel() {
    if (!active) return
    setError(null)
    try {
      await cancelMutation.mutateAsync({
        sessionId: active.id,
        input: {
          expected_version: productGenesisVersion(active),
          reason: 'cancelled_from_main_chat',
        },
      })
    } catch (cause) {
      setError(getApiErrorMessage(cause, 'Product Genesis could not be cancelled.'))
    }
  }

  if (activeQuery.isLoading) {
    return <LoadingState label="Loading Product Genesis…" compact />
  }
  if (activeQuery.isError || !active) {
    return null
  }

  return (
    <div className="flex min-w-0 items-center gap-2">
      <div className="flex min-w-0 flex-wrap items-center gap-2 rounded-lg border border-ember-border bg-ember-surface px-3 py-1.5">
        <Flask size={14} className="shrink-0 text-primary" aria-hidden />
        <span className="min-w-0 truncate text-xs text-foreground">
          Genesis · {lifecycleLabel(active.lifecycle)} · {active.maturity}
        </span>
        {projectId && active.lifecycle === 'handed_off' ? (
          <Link
            to="/projects/$projectId/chat"
            params={{ projectId }}
            className="inline-flex items-center gap-1 text-xs font-medium text-primary underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          >
            Continue with Project Agent
            <ArrowRight size={13} aria-hidden />
          </Link>
        ) : null}
        <Button
          type="button"
          variant="ghost"
          size="sm"
          onClick={() => void cancel()}
          disabled={cancelMutation.isPending || active.lifecycle === 'handed_off'}
          aria-label="Cancel Product Genesis"
          className="h-6 px-1.5"
        >
          {cancelMutation.isPending ? (
            <CircleNotch size={13} className="animate-spin" aria-hidden />
          ) : (
            <X size={13} aria-hidden />
          )}
          Cancel
        </Button>
      </div>
      {error ? (
        <p className="text-xs text-destructive" role="alert">
          {error}
        </p>
      ) : null}
    </div>
  )
}

/**
 * The Charter review as a compact chat-timeline card: one summary row with
 * the approval actions always visible, expandable into the full review
 * panel. Lives in the Main chat conversation instead of a dedicated area.
 */
export function ProductGenesisCharterCard() {
  const activeQuery = useProductGenesisActiveQuery()
  const active = activeQuery.data?.session ?? null
  const charterQuery = useProductGenesisCharterQuery(active?.id)
  const approveMutation = useApproveProductGenesisCharterRevisionMutation(active?.id)
  const createMutation = useCreateProjectFromCharterApprovalMutation()
  const [expanded, setExpanded] = useState(false)
  const [selectedRevisionId, setSelectedRevisionId] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [conflict, setConflict] = useState(false)
  const [conflictError, setConflictError] = useState<unknown>(null)
  const [createdProjectId, setCreatedProjectId] = useState<string | null>(null)
  const [lastApproval, setLastApproval] = useState<ProjectCharterApproval | null>(null)
  const approvalAttemptRef = useRef<{
    fingerprint: string
    idempotencyKey: string
    authorization: ReturnType<typeof createAuthorization>
  } | null>(null)
  const createAttemptRef = useRef<{
    fingerprint: string
    idempotencyKey: string
    authorization: ReturnType<typeof createAuthorization>
  } | null>(null)

  const charterData = charterQuery.data
  const approval = charterData?.approval ?? lastApproval
  const draftRevision = charterData?.current_draft_revision ?? null
  const selectedRevision =
    charterData?.revisions.find((revision) => revision.id === selectedRevisionId) ?? draftRevision
  const stale = Boolean(
    selectedRevision &&
    draftRevision &&
    selectedRevision.id !== draftRevision.id &&
    selectedRevision.lifecycle !== 'approved',
  )
  const projectId = createdProjectId ?? active?.project_id ?? null

  useEffect(() => {
    if (!charterData) return
    setSelectedRevisionId((current) =>
      current && charterData.revisions.some((revision) => revision.id === current)
        ? current
        : (charterData.current_draft_revision?.id ?? charterData.revisions[0]?.id ?? null),
    )
    if (charterData.approval) setLastApproval(charterData.approval)
  }, [charterData])

  useEffect(() => {
    if (!active) {
      setSelectedRevisionId(null)
      setLastApproval(null)
      setCreatedProjectId(null)
      setConflict(false)
      setConflictError(null)
      setExpanded(false)
      approvalAttemptRef.current = null
      createAttemptRef.current = null
    }
  }, [active])

  async function approve(revision: ProjectCharterRevision) {
    const charter = charterData?.charter
    if (!active || !charter || !charterData.selected_project_agent) return
    const agent = charterData.selected_project_agent
    setError(null)
    setConflict(false)
    setConflictError(null)
    try {
      const expectedCharterVersion = versionNumber(charter.version)
      const fingerprint = JSON.stringify({
        session_id: active.id,
        charter_id: charter.id,
        charter_version: expectedCharterVersion,
        revision_id: revision.id,
        content_digest: revision.content_digest,
        render_digest: revision.render_digest,
        project_mode: revision.project_mode,
        project_name: revision.content.identity.working_name,
        project_slug: revision.content.identity.slug_proposal,
        agent_identity_id: agent.identity_id,
        agent_profile_revision_id: agent.profile_revision_id,
        agent_operating_skill_revision: agent.operating_skill_revision,
        agent_policy_digest: agent.policy_digest,
        principal_id: useAuthStore.getState().user?.id ?? null,
      })
      const attempt =
        approvalAttemptRef.current?.fingerprint === fingerprint
          ? approvalAttemptRef.current
          : (() => {
              const idempotencyKey = mutationId(
                'product-genesis-charter-approval',
                `${active.id}:${revision.id}:${expectedCharterVersion}`,
              )
              const next = {
                fingerprint,
                idempotencyKey,
                authorization: createAuthorization('product_genesis.charter_approval'),
              }
              approvalAttemptRef.current = next
              return next
            })()
      const result = await approveMutation.mutateAsync({
        revisionId: revision.id,
        input: {
          mutation: {
            expected_version: expectedCharterVersion,
            expected_digest: revision.content_digest,
            idempotency_key: attempt.idempotencyKey,
            deduplication_key: attempt.idempotencyKey,
            authorization: attempt.authorization,
          },
          charter_id: charter.id,
          revision_id: revision.id,
          content_digest: revision.content_digest,
          render_digest: revision.render_digest,
          expected_charter_version: expectedCharterVersion,
          approved_project_name: revision.content.identity.working_name,
          approved_project_slug: revision.content.identity.slug_proposal,
          project_mode: revision.project_mode,
          selected_project_agent_identity_id: agent.identity_id,
          selected_project_agent_profile_revision_id: agent.profile_revision_id,
          selected_project_agent_operating_skill_revision: agent.operating_skill_revision,
          selected_project_agent_policy_digest: agent.policy_digest,
        },
      })
      setLastApproval(result)
    } catch (cause) {
      setConflict(isConflict(cause))
      setConflictError(cause)
      setError(getApiErrorMessage(cause, 'The exact Charter revision could not be approved.'))
    }
  }

  async function createProject() {
    if (!approval || approval.state !== 'active') return
    setError(null)
    setConflict(false)
    setConflictError(null)
    try {
      const principalId = useAuthStore.getState().user?.id ?? null
      const fingerprint = JSON.stringify({ approval_id: approval.id, principal_id: principalId })
      const attempt =
        createAttemptRef.current?.fingerprint === fingerprint
          ? createAttemptRef.current
          : (() => {
              const idempotencyKey = mutationId('product-genesis-project-create', approval.id)
              const next = {
                fingerprint,
                idempotencyKey,
                authorization: createAuthorization('product_genesis.create_project_from_approval'),
              }
              createAttemptRef.current = next
              return next
            })()
      const result = await createMutation.mutateAsync({
        approval_id: approval.id,
        idempotency_key: attempt.idempotencyKey,
        authorization: attempt.authorization,
      })
      setCreatedProjectId(result.project_id)
    } catch (cause) {
      setConflict(isConflict(cause))
      setConflictError(cause)
      setError(
        getApiErrorMessage(
          cause,
          'Project creation and handoff failed. The approval receipt remains replayable.',
        ),
      )
    }
  }

  function refreshCharter() {
    setError(null)
    setConflict(false)
    setConflictError(null)
    void charterQuery.refetch()
  }

  if (!active || activeQuery.isLoading || activeQuery.isError) return null
  if (!charterData || charterData.revisions.length === 0) return null

  const workingName =
    selectedRevision?.content.identity.working_name ??
    draftRevision?.content.identity.working_name ??
    'Charter'
  const vision = selectedRevision?.content.identity.one_line_vision ?? ''
  const revisionNumber = selectedRevision
    ? charterData.revisions.length -
      charterData.revisions.findIndex((revision) => revision.id === selectedRevision.id)
    : charterData.revisions.length
  const lifecycle = approval
    ? projectId
      ? 'project created'
      : 'approved'
    : (selectedRevision?.lifecycle ?? 'proposed')
  const canApprove = Boolean(
    !approval &&
    charterData.charter &&
    selectedRevision &&
    selectedRevision.lifecycle === 'proposed' &&
    !stale &&
    charterData.selected_project_agent,
  )

  return (
    <section
      aria-label={`Project Charter ${workingName}`}
      className="min-w-0 max-w-full overflow-hidden rounded-xl border border-ember-border/60 bg-card shadow-xs"
    >
      <button
        type="button"
        aria-expanded={expanded}
        onClick={() => setExpanded((current) => !current)}
        className="flex w-full min-w-0 items-center gap-2.5 px-4 py-3 text-left transition-colors hover:bg-muted/30"
      >
        <FileText size={16} className="shrink-0 text-primary" aria-hidden />
        <span className="min-w-0 truncate text-sm font-semibold text-foreground">
          Project Charter — {workingName}
        </span>
        <span className="shrink-0 rounded-full border border-border-subtle bg-muted/40 px-2 py-0.5 font-mono text-micro uppercase tracking-[0.08em] text-muted-foreground">
          rev {revisionNumber} · {lifecycleLabel(lifecycle)}
        </span>
        <CaretDown
          size={13}
          className={`ml-auto shrink-0 text-muted-foreground transition-transform ${expanded ? 'rotate-180' : ''}`}
          aria-hidden
        />
      </button>
      {vision ? (
        <p className="truncate px-4 pb-2 text-xs text-muted-foreground">
          {vision}
        </p>
      ) : null}
      <div className="flex flex-wrap items-center gap-2 border-t border-border-subtle px-4 py-2.5">
        {canApprove && selectedRevision ? (
          <Button
            type="button"
            size="sm"
            onClick={() => void approve(selectedRevision)}
            disabled={approveMutation.isPending}
          >
            {approveMutation.isPending ? (
              <CircleNotch size={13} className="animate-spin" aria-hidden />
            ) : (
              <ShieldCheck size={13} aria-hidden />
            )}
            Approve this revision
          </Button>
        ) : null}
        {approval && approval.state === 'active' && !projectId ? (
          <Button
            type="button"
            size="sm"
            onClick={() => void createProject()}
            disabled={createMutation.isPending}
          >
            {createMutation.isPending ? (
              <CircleNotch size={13} className="animate-spin" aria-hidden />
            ) : (
              <CheckCircle size={13} aria-hidden />
            )}
            Create project & hand off
          </Button>
        ) : null}
        {projectId ? (
          <Link
            to="/projects/$projectId/chat"
            params={{ projectId }}
            className="inline-flex items-center gap-1.5 text-xs font-medium text-primary underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          >
            Continue with the Project Agent
            <ArrowRight size={13} aria-hidden />
          </Link>
        ) : null}
        {!canApprove && !approval && !projectId ? (
          <span className="text-xs text-muted-foreground">
            {charterData.selected_project_agent
              ? 'Waiting for a proposed revision to approve.'
              : 'No eligible Project Agent is selected yet.'}
          </span>
        ) : null}
        <Button
          type="button"
          variant="ghost"
          size="sm"
          onClick={refreshCharter}
          className="ml-auto h-7 px-2 text-xs text-muted-foreground"
        >
          <ArrowClockwise size={13} aria-hidden />
          Refresh
        </Button>
      </div>
      {error ? (
        <div className="border-t border-border-subtle px-4 py-2.5">
          <ErrorCard
            title="Product Genesis error"
            description={error}
            severity={conflict ? 'conflict' : 'error'}
            action={conflict ? { label: 'Refresh', onClick: refreshCharter } : undefined}
            technicalDetails={conflictError}
          />
        </div>
      ) : null}
      {expanded ? (
        <div className="max-h-[60vh] overflow-y-auto border-t border-border-subtle p-3">
          <CharterReviewPanel
            active={active}
            charterData={charterData}
            selectedRevisionId={selectedRevisionId}
            onRevisionChange={setSelectedRevisionId}
            approval={approval}
            approvalPending={approveMutation.isPending}
            createPending={createMutation.isPending}
            onApprove={(revision) => void approve(revision)}
            onCreate={() => void createProject()}
            onRefresh={refreshCharter}
            stale={stale}
            conflict={conflict}
            conflictError={conflictError}
          />
        </div>
      ) : null}
    </section>
  )
}
