/** Project Overview page composition. */

import { Link } from '@tanstack/react-router'
import { useRef, useState } from 'react'
import { ArrowUpRight, CheckCircle, ChatCircleDots } from '@phosphor-icons/react'
import {
  useProjectOverviewQuery,
  useRecordManualMilestoneCheck,
  useReleaseProjectMilestone,
} from '@/api/hooks'
import { ProjectFlowHeader } from '@/components/project-flow-status'
import { buttonClassName } from '@/components/ui/button-styles'
import { ProjectCharterAdoptionBanner } from '@/features/project-charter/ProjectCharterAdoptionBanner'
import { ProjectStageOrientation } from '@/features/project-workbench/ProjectStageOrientation'
import { ProjectExecutionSetupPanel } from '@/features/project-execution/ProjectExecutionSetupPanel'
import { ReconciliationReviewCard } from '@/features/project-execution/ReconciliationReviewCard'
import {
  createUserAuthorization,
  newIdempotencyKey,
} from '@/features/project-execution/user-authorization'
import { getApiErrorCode, getApiErrorMessage, isApiStatus } from '@/lib/api-error'
import { useAuthStore } from '@/stores/auth'
import type { AuthorizationProvenance } from '@/types/generated/bindings/AuthorizationProvenance'
import { count, formatDate, humanize, shortId, sha256Hex } from './project-overview/utils'
import type { ReleaseCandidate, ManualAttestationCandidate } from './project-overview/utils'
import { StatusLabel, SectionCard, MetricGrid, CheckSummary } from './project-overview/shared'
import {
  AcceptanceChecksPanel,
  ManualAttestationDialog,
  OutcomeCard,
} from './project-overview/outcomes'
import { DocumentFreshnessPanel, DecisionsAndRisks } from './project-overview/governance'
import { EvidenceGallery } from './project-overview/evidence'
import { ReleaseHistory, ReleaseReviewDialog } from './project-overview/releases'
import {
  ProjectionBanner,
  LoadingState,
  DeniedState,
  DeletedProjectRedirect,
  ErrorState,
} from './project-overview/states'
import { NextActionCard } from './project-overview/next-action'

export function ProjectOverviewPage({ projectId }: { projectId: string }) {
  const overviewQuery = useProjectOverviewQuery(projectId)
  const releaseMutation = useReleaseProjectMilestone()
  const attestationMutation = useRecordManualMilestoneCheck()
  const hasUser = Boolean(useAuthStore((state) => state.user))
  const releaseAttemptRef = useRef<{
    fingerprint: string
    key: string
    authorization: AuthorizationProvenance
  } | null>(null)
  const [releaseCandidate, setReleaseCandidate] = useState<ReleaseCandidate | null>(null)
  const [releaseError, setReleaseError] = useState<string | null>(null)
  const [releaseNotice, setReleaseNotice] = useState<string | null>(null)
  const [attestationCandidate, setAttestationCandidate] =
    useState<ManualAttestationCandidate | null>(null)
  const [attestationError, setAttestationError] = useState<string | null>(null)
  const [attestationNotice, setAttestationNotice] = useState<string | null>(null)

  function reviewRelease(candidate: ReleaseCandidate) {
    setReleaseError(null)
    setReleaseNotice(null)
    releaseMutation.reset?.()
    setReleaseCandidate(candidate)
  }

  async function executeRelease(candidate: ReleaseCandidate) {
    setReleaseError(null)
    try {
      const principalId = useAuthStore.getState().user?.id ?? 'anonymous'
      const fingerprint = `${principalId}:${candidate.milestoneId}:${candidate.snapshotId}:${candidate.digest}:${candidate.expectedMilestoneVersion}`
      const attempt =
        releaseAttemptRef.current?.fingerprint === fingerprint
          ? releaseAttemptRef.current
          : {
              fingerprint,
              key: newIdempotencyKey('project-milestone-release'),
              authorization: createUserAuthorization(
                'project.milestone.release',
                'interactive_user_release',
              ),
            }
      releaseAttemptRef.current = attempt
      const release = await releaseMutation.mutateAsync({
        projectId,
        milestoneId: candidate.milestoneId,
        expectedMilestoneVersion: candidate.expectedMilestoneVersion,
        readinessSnapshotId: candidate.snapshotId,
        readinessDigest: candidate.digest,
        idempotencyKey: attempt.key,
        authorization: attempt.authorization,
      })
      releaseAttemptRef.current = null
      setReleaseCandidate(null)
      setReleaseNotice(
        `Release recorded: ${release.snapshot.release_identity} is now immutable release truth.`,
      )
    } catch (error) {
      const code = getApiErrorCode(error)
      setReleaseError(
        (isApiStatus(error, 409) || isApiStatus(error, 412)) && code === 'version_conflict'
          ? 'The milestone changed while this release was open. Refresh the Overview, review the current readiness snapshot, and try again.'
          : getApiErrorMessage(
              error,
              'The release could not be recorded. Review the current readiness snapshot and try again.',
            ),
      )
      void overviewQuery.refetch()
    }
  }

  async function executeAttestation(
    candidate: ManualAttestationCandidate,
    status: 'pass' | 'fail',
    observation: string,
  ) {
    setAttestationError(null)
    try {
      if (!candidate.charterRevisionId) {
        throw new Error('The current milestone has no governing Charter revision.')
      }
      const inputDigest = await sha256Hex(
        JSON.stringify({
          check_id: candidate.check.id,
          definition_revision_id: candidate.definitionRevisionId,
          status,
          observation,
        }),
      )
      await attestationMutation.mutateAsync({
        projectId,
        milestoneId: candidate.milestoneId,
        checkId: candidate.check.id,
        definitionRevisionId: candidate.definitionRevisionId,
        charterRevisionId: candidate.charterRevisionId,
        expectedCheckVersion: count(candidate.check.version),
        status,
        result: observation,
        inputDigest,
        idempotencyKey: newIdempotencyKey('manual-attestation'),
        authorization: createUserAuthorization(
          'project.milestone.check.record',
          'interactive_user_attestation',
        ),
      })
      setAttestationCandidate(null)
      setAttestationNotice(
        `${humanize(status)} recorded for “${candidate.check.description}”. Required evidence remains separate.`,
      )
    } catch (error) {
      setAttestationError(
        getApiErrorMessage(
          error,
          'The result could not be recorded. Refresh the current check and try again.',
        ),
      )
      void overviewQuery.refetch()
    }
  }

  if (overviewQuery.isLoading) return <LoadingState />
  if (overviewQuery.isError) {
    if (isApiStatus(overviewQuery.error, 403)) return <DeniedState projectId={projectId} />
    if (isApiStatus(overviewQuery.error, 404))
      return <DeletedProjectRedirect projectId={projectId} />
    return (
      <ErrorState
        error={overviewQuery.error}
        onRetry={() => void overviewQuery.refetch()}
        projectId={projectId}
      />
    )
  }

  const overview = overviewQuery.data
  if (!overview || overview.projection_state === 'permission_denied')
    return <DeniedState projectId={projectId} />

  const setupRequired = overview.charter_state === 'charter_setup_required'
  const activeMilestones = overview.active_milestones
  const primary = activeMilestones.find(
    (item) => item.milestone.id === overview.primary_milestone_id,
  )
  // Before a Charter is adopted the Charter-derived panels have no source.
  // Hide each one that is also empty so the Overview shows what actually
  // exists -- Task progress, and the setup the Project still needs -- instead
  // of a column of "none recorded" placeholders. A Project that carries real
  // milestone, document, decision, evidence, or release records keeps
  // rendering them: this hides empty panels, never existing truth.
  const hideOutcome = setupRequired && activeMilestones.length === 0
  const hideValidation = hideOutcome && count(overview.check_summary.required_total) === 0
  const hideDocuments = setupRequired && overview.document_freshness.length === 0
  const hideDecisions =
    setupRequired &&
    overview.decisions.length === 0 &&
    overview.pending_decisions.length === 0 &&
    overview.risks.length === 0
  const hideEvidence = setupRequired && overview.evidence.length === 0
  const hideReleases = setupRequired && overview.releases.length === 0
  // The adoption banner already renders `charter_adoption` as a real control;
  // a second card restating it is noise.
  const hideNextAction = setupRequired && overview.next_action?.code === 'charter_adoption'
  const effectiveReleaseError =
    releaseError ??
    (releaseMutation.error
      ? getApiErrorMessage(
          releaseMutation.error,
          'The release could not be recorded. Review the current readiness snapshot and try again.',
        )
      : null)

  return (
    <div className="mx-auto flex w-full max-w-[1440px] min-w-0 flex-col gap-5">
      <header className="min-w-0">
        <div className="flex min-w-0 flex-wrap items-start justify-between gap-4">
          <div className="min-w-0">
            <p className="font-mono text-micro font-semibold uppercase tracking-[0.14em] text-muted-foreground">
              Project Overview
            </p>
            <h1 className="mt-2 break-words text-2xl font-semibold tracking-tight text-foreground sm:text-3xl">
              {overview.project_name}
            </h1>
            <div className="mt-3">
              <ProjectFlowHeader key={projectId} projectId={projectId} />
            </div>
            <p className="mt-2 max-w-3xl break-words text-sm leading-6 text-muted-foreground">
              {overview.vision}
            </p>
            <div className="mt-3 flex min-w-0 flex-wrap items-center gap-2">
              <StatusLabel status={overview.charter_state} />
              {overview.current_charter ? (
                <span className="break-all font-mono text-micro text-muted-foreground">
                  Charter r{count(overview.current_charter.revision_number)} ·{' '}
                  {shortId(overview.current_charter.content_digest)}
                </span>
              ) : (
                <span className="text-xs text-muted-foreground">No approved Charter revision</span>
              )}
              {overview.primary_milestone_id ? (
                <span className="break-all font-mono text-micro text-muted-foreground">
                  Primary milestone ID {overview.primary_milestone_id}
                </span>
              ) : null}
              {activeMilestones.map((item) => (
                <span
                  key={item.milestone.id}
                  className="max-w-full break-words rounded-md border border-border-subtle bg-muted px-2 py-1 text-xs text-foreground"
                >
                  {item.milestone.canonical_id} ·{' '}
                  {item.milestone.display_label ?? item.definition.content.name}
                </span>
              ))}
            </div>
          </div>
          <div className="flex shrink-0 flex-wrap gap-2">
            <Link
              to="/projects/$projectId/chat"
              params={{ projectId }}
              className={buttonClassName({ variant: 'outline' })}
            >
              <ChatCircleDots size={15} aria-hidden /> Project Agent Chat
            </Link>
            <Link to="/chat" className={buttonClassName({ variant: 'ghost' })}>
              Main Chat
            </Link>
          </div>
        </div>
      </header>

      <ProjectionBanner
        state={overview.projection_state}
        watermark={overview.source_event_watermark}
        onRetry={() => void overviewQuery.refetch()}
      />

      <ProjectStageOrientation overview={overview} />

      {releaseNotice ? (
        <div
          className="flex min-w-0 items-start gap-2 rounded-md border border-success/30 bg-success/10 p-3 text-sm"
          role="status"
        >
          <CheckCircle size={17} className="mt-0.5 shrink-0 text-success" aria-hidden />
          <p className="break-words text-foreground">{releaseNotice}</p>
        </div>
      ) : null}

      {attestationNotice ? (
        <div
          className="flex min-w-0 items-start gap-2 rounded-md border border-success/30 bg-success/10 p-3 text-sm"
          role="status"
        >
          <CheckCircle size={17} className="mt-0.5 shrink-0 text-success" aria-hidden />
          <p className="break-words text-foreground">{attestationNotice}</p>
        </div>
      ) : null}

      {setupRequired ? <ProjectCharterAdoptionBanner projectId={projectId} /> : null}

      <ReconciliationReviewCard projectId={projectId} />

      <div className="grid min-w-0 gap-5 xl:grid-cols-[minmax(0,1.45fr)_minmax(300px,0.75fr)]">
        <section
          className="order-3 min-w-0 space-y-5 xl:order-1 xl:col-start-1"
          aria-label="Project progress"
        >
          {hideOutcome ? null : (
            <div id="milestones" className="scroll-mt-24">
              <SectionCard
                title={primary ? 'Current outcome' : 'Current outcome setup'}
                eyebrow="Live Project progress"
                action={
                  <Link
                    to="/projects/$projectId/tasks"
                    params={{ projectId }}
                    search={{ sort_by: 'updated_at', sort_order: 'desc' }}
                    className="inline-flex items-center gap-1 text-xs font-medium text-primary hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  >
                    View Tasks <ArrowUpRight size={13} aria-hidden />
                  </Link>
                }
              >
                {activeMilestones.length === 0 ? (
                  <div className="rounded-md border border-dashed border-border bg-muted/30 p-4">
                    <p className="text-sm font-medium text-foreground">
                      No active milestone is defined yet.
                    </p>
                    <p className="mt-1 break-words text-xs leading-5 text-muted-foreground">
                      {overview.next_action?.title ??
                        'Continue in Project Agent Chat to define the first bounded outcome and acceptance checks.'}
                    </p>
                  </div>
                ) : (
                  <div className="space-y-4">
                    {activeMilestones.map((item) => (
                      <OutcomeCard
                        key={item.milestone.id}
                        item={item}
                        primary={item === primary}
                        projectionState={overview.projection_state}
                        hasUser={hasUser}
                        releasePending={releaseMutation.isPending}
                        releaseError={effectiveReleaseError}
                        onReviewRelease={reviewRelease}
                      />
                    ))}
                  </div>
                )}
              </SectionCard>
            </div>
          )}

          <div className={`grid min-w-0 gap-5 ${hideValidation ? '' : 'lg:grid-cols-2'}`}>
            <SectionCard
              title="Task progress"
              eyebrow="Authoritative workflow counts"
              action={
                hideOutcome ? (
                  <Link
                    to="/projects/$projectId/tasks"
                    params={{ projectId }}
                    search={{ sort_by: 'updated_at', sort_order: 'desc' }}
                    className="inline-flex items-center gap-1 text-xs font-medium text-primary hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  >
                    View Tasks <ArrowUpRight size={13} aria-hidden />
                  </Link>
                ) : undefined
              }
            >
              <MetricGrid counts={overview.task_counts} />
              <p className="mt-3 text-xs leading-5 text-muted-foreground">
                Counts come from linked Tasks. Forge does not infer a completion percentage from
                terminal work.
              </p>
            </SectionCard>
            {hideValidation ? null : (
              <div id="readiness" className="scroll-mt-24">
                <SectionCard title="Validation" eyebrow="Acceptance contract">
                  <CheckSummary summary={overview.check_summary} />
                  <AcceptanceChecksPanel
                    milestones={activeMilestones}
                    hasUser={hasUser}
                    pending={attestationMutation.isPending}
                    onReview={(candidate) => {
                      setAttestationError(null)
                      setAttestationNotice(null)
                      attestationMutation.reset?.()
                      setAttestationCandidate(candidate)
                    }}
                  />
                </SectionCard>
              </div>
            )}
          </div>
        </section>

        <aside
          className="contents xl:order-2 xl:col-start-2 xl:row-start-1 xl:block xl:min-w-0 xl:space-y-5"
          aria-label="Project Overview supporting information"
        >
          <div className="order-1 min-w-0 xl:order-none">
            <ProjectExecutionSetupPanel projectId={projectId} compact />
          </div>
          {hideNextAction ? null : (
            <div className="order-2 min-w-0 xl:order-none">
              <NextActionCard
                projectId={projectId}
                nextAction={overview.next_action}
                milestones={overview.active_milestones}
              />
            </div>
          )}
          {hideDocuments ? null : (
            <div id="documents" className="order-4 min-w-0 scroll-mt-24 xl:order-none">
              <DocumentFreshnessPanel
                projectId={projectId}
                documents={overview.document_freshness}
              />
            </div>
          )}
          {hideDecisions ? null : (
            <div id="decisions" className="order-5 min-w-0 scroll-mt-24 xl:order-none">
              <DecisionsAndRisks projectId={projectId} overview={overview} />
            </div>
          )}
          {hideEvidence ? null : (
            <div id="evidence" className="order-6 min-w-0 scroll-mt-24 xl:order-none">
              <EvidenceGallery projectId={projectId} evidence={overview.evidence} />
            </div>
          )}
          {hideReleases ? null : (
            <div id="releases" className="order-7 min-w-0 scroll-mt-24 xl:order-none">
              <ReleaseHistory projectId={projectId} releases={overview.releases} />
            </div>
          )}
        </aside>
      </div>

      <footer className="flex min-w-0 flex-wrap items-center gap-x-3 gap-y-1 border-t border-border-subtle pt-3 font-mono text-micro text-muted-foreground">
        <span>Projection {humanize(overview.projection_state)}</span>
        <span>Watermark {shortId(overview.source_event_watermark)}</span>
        <span>Generated {formatDate(overview.generated_at)}</span>
      </footer>
      <ReleaseReviewDialog
        candidate={releaseCandidate}
        open={releaseCandidate !== null}
        hasUser={hasUser}
        isPending={releaseMutation.isPending}
        error={effectiveReleaseError}
        onOpenChange={(open) => {
          if (!open && !releaseMutation.isPending) setReleaseCandidate(null)
        }}
        onConfirm={() => {
          if (releaseCandidate) void executeRelease(releaseCandidate)
        }}
      />
      <ManualAttestationDialog
        candidate={attestationCandidate}
        open={attestationCandidate !== null}
        pending={attestationMutation.isPending}
        error={
          attestationError ??
          (attestationMutation.error
            ? getApiErrorMessage(
                attestationMutation.error,
                'The result could not be recorded. Refresh the current check and try again.',
              )
            : null)
        }
        onOpenChange={(open) => {
          if (!open && !attestationMutation.isPending) setAttestationCandidate(null)
        }}
        onConfirm={(status, observation) => {
          if (attestationCandidate) {
            void executeAttestation(attestationCandidate, status, observation)
          }
        }}
      />
    </div>
  )
}
