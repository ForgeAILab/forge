// Product Genesis Charter card and imperative handle.

import { forwardRef, useEffect, useImperativeHandle, useRef, useState } from 'react'
import { Link, useNavigate } from '@tanstack/react-router'
import {
  ArrowClockwise,
  ArrowRight,
  CaretDown,
  CheckCircle,
  CircleNotch,
  FileText,
  ShieldCheck,
} from '@phosphor-icons/react'
import { useAuthStore } from '@/stores/auth'
import { Button } from '@/components/ui/button'
import { ErrorCard } from '@/components/chat/error-card'
import {
  useApproveProductGenesisCharterRevisionMutation,
  useCreateProjectFromCharterApprovalMutation,
  useProductGenesisActiveQuery,
  useProductGenesisCharterQuery,
} from '../hooks'
import type { ProjectCharterApproval, ProjectCharterRevision } from '../types'
import {
  createAuthorization,
  versionNumber,
  mutationId,
  isConflict,
  getApiErrorMessage,
  lifecycleLabel,
} from './utils'
import { CharterReviewPanel } from './charter-review'

/** Imperative surface for typed approval ("approve" / the /approve command). */
export type ProductGenesisCharterCardHandle = {
  /** True when typing "approve" should be treated as an approval action. */
  canApproveTyped: () => boolean
  /** Approve the current proposed revision, then create the Project. */
  approveTyped: () => Promise<void>
}

/**
 * The Charter review as a compact pinned card above the Main Chat timeline:
 * one summary row with the approval actions always visible, expandable into
 * the full review panel. It stays in place while the conversation streams —
 * it never trails the newest message. The card always shows the current
 * revision (approved, else draft); revision history stays internal, exposed
 * only through the side-by-side compare toggle. The ref handle lets the chat
 * composer route a typed "approve" through the same exact-revision flow.
 */
export const ProductGenesisCharterCard = forwardRef<ProductGenesisCharterCardHandle>(
  function ProductGenesisCharterCard(_props, handleRef) {
    const navigate = useNavigate()
    const activeQuery = useProductGenesisActiveQuery()
    const active = activeQuery.data?.session ?? null
    const charterQuery = useProductGenesisCharterQuery(active?.id)
    const approveMutation = useApproveProductGenesisCharterRevisionMutation(active?.id)
    const createMutation = useCreateProjectFromCharterApprovalMutation()
    const [expanded, setExpanded] = useState(false)
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
    const selectedRevision =
      charterData?.current_approved_revision ??
      charterData?.current_draft_revision ??
      charterData?.revisions[0] ??
      null
    const previousRevision = selectedRevision
      ? (charterData?.revisions
          .filter(
            (revision) =>
              versionNumber(revision.revision_number) <
              versionNumber(selectedRevision.revision_number),
          )
          .sort((a, b) => versionNumber(b.revision_number) - versionNumber(a.revision_number))[0] ??
        null)
      : null
    const projectId = createdProjectId ?? active?.project_id ?? null

    useEffect(() => {
      if (charterData?.approval) setLastApproval(charterData.approval)
    }, [charterData])

    useEffect(() => {
      if (!active) {
        setLastApproval(null)
        setCreatedProjectId(null)
        setConflict(false)
        setConflictError(null)
        setExpanded(false)
        approvalAttemptRef.current = null
        createAttemptRef.current = null
      }
    }, [active])

    async function approve(revision: ProjectCharterRevision): Promise<ProjectCharterApproval> {
      const charter = charterData?.charter
      if (!active || !charter) throw new Error('The Charter is not loaded yet. Refresh and retry.')
      if (!charterData.selected_project_agent) {
        throw new Error('No eligible Project Agent exists — create one in Agents first.')
      }
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
        return result
      } catch (cause) {
        setConflict(isConflict(cause))
        setConflictError(cause)
        const message = getApiErrorMessage(
          cause,
          'The exact Charter revision could not be approved.',
        )
        setError(message)
        throw new Error(message)
      }
    }

    async function createProject(fromApproval?: ProjectCharterApproval): Promise<string> {
      const receipt = fromApproval ?? approval
      if (!receipt || receipt.state !== 'active') {
        throw new Error('No active approval receipt exists to create the Project from.')
      }
      setError(null)
      setConflict(false)
      setConflictError(null)
      try {
        const principalId = useAuthStore.getState().user?.id ?? null
        const fingerprint = JSON.stringify({ approval_id: receipt.id, principal_id: principalId })
        const attempt =
          createAttemptRef.current?.fingerprint === fingerprint
            ? createAttemptRef.current
            : (() => {
                const idempotencyKey = mutationId('product-genesis-project-create', receipt.id)
                const next = {
                  fingerprint,
                  idempotencyKey,
                  authorization: createAuthorization(
                    'product_genesis.create_project_from_approval',
                  ),
                }
                createAttemptRef.current = next
                return next
              })()
        const result = await createMutation.mutateAsync({
          approval_id: receipt.id,
          idempotency_key: attempt.idempotencyKey,
          authorization: attempt.authorization,
        })
        setCreatedProjectId(result.project_id)
        // 8.4.4: select and navigate to the returned Project Agent handoff
        // rather than leaving the user to notice and click the "Continue"
        // link below — the committed Project already exists, so staying put
        // is a dead end, not a safe default.
        void navigate({
          to: '/projects/$projectId/chat',
          params: { projectId: result.project_id },
        })
        return result.project_id
      } catch (cause) {
        setConflict(isConflict(cause))
        setConflictError(cause)
        const message = getApiErrorMessage(
          cause,
          'Project creation and handoff failed. The approval receipt remains replayable.',
        )
        setError(message)
        throw new Error(message)
      }
    }

    function refreshCharter() {
      setError(null)
      setConflict(false)
      setConflictError(null)
      void charterQuery.refetch()
    }

    useImperativeHandle(handleRef, () => ({
      canApproveTyped: () =>
        Boolean(
          active &&
          charterData &&
          !projectId &&
          (approval?.state === 'active' ||
            (!approval &&
              charterData.charter &&
              selectedRevision?.lifecycle === 'proposed' &&
              charterData.selected_project_agent)),
        ),
      approveTyped: async () => {
        if (!active || !charterData) {
          throw new Error('No Product Genesis Charter is active in this chat.')
        }
        if (projectId) {
          throw new Error('The Project already exists — continue in the Project chat.')
        }
        let receipt = approval
        if (!receipt) {
          if (
            !charterData.charter ||
            !selectedRevision ||
            selectedRevision.lifecycle !== 'proposed'
          ) {
            throw new Error('No proposed Charter revision is awaiting approval yet.')
          }
          receipt = await approve(selectedRevision)
        }
        if (receipt.state === 'active') {
          await createProject(receipt)
        } else if (receipt.state !== 'consumed') {
          throw new Error(`The approval receipt is ${receipt.state}. Refresh the Charter card.`)
        }
      },
    }))

    if (!active || activeQuery.isLoading || activeQuery.isError) return null
    if (!charterData || charterData.revisions.length === 0) return null

    const workingName = selectedRevision?.content.identity.working_name ?? 'Charter'
    const vision = selectedRevision?.content.identity.one_line_vision ?? ''
    const revisionNumber = selectedRevision
      ? versionNumber(selectedRevision.revision_number)
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
      charterData.selected_project_agent,
    )

    return (
      <section
        aria-label={`Project Charter ${workingName}`}
        className="mx-4 mt-3 min-w-0 max-w-full shrink-0 overflow-hidden rounded-xl border border-ember-border/60 bg-card shadow-xs sm:mx-5"
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
          <p className="truncate px-4 pb-2 text-xs text-muted-foreground">{vision}</p>
        ) : null}
        <div className="flex flex-wrap items-center gap-2 border-t border-border-subtle px-4 py-2.5">
          {canApprove && selectedRevision ? (
            <Button
              type="button"
              size="sm"
              onClick={() => void approve(selectedRevision).catch(() => undefined)}
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
              onClick={() => void createProject().catch(() => undefined)}
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
              {charterData.selected_project_agent ? (
                'Waiting for a proposed revision to approve.'
              ) : (
                <>
                  No eligible Project Agent exists —{' '}
                  <Link
                    to="/agents"
                    className="font-medium text-primary underline-offset-4 hover:underline"
                  >
                    create one in Agents
                  </Link>{' '}
                  first.
                </>
              )}
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
          <div className="max-h-[50vh] overflow-y-auto border-t border-border-subtle p-3">
            <CharterReviewPanel
              active={active}
              charterData={charterData}
              revision={selectedRevision}
              previousRevision={previousRevision}
              approval={approval}
              approvalPending={approveMutation.isPending}
              createPending={createMutation.isPending}
              onApprove={(revision) => void approve(revision).catch(() => undefined)}
              onCreate={() => void createProject().catch(() => undefined)}
              onRefresh={refreshCharter}
              conflict={conflict}
              conflictError={conflictError}
            />
          </div>
        ) : null}
      </section>
    )
  },
)
