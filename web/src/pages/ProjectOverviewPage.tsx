import { useQueryClient } from '@tanstack/react-query'
import { Link, useNavigate } from '@tanstack/react-router'
import type { ReactNode } from 'react'
import { useEffect, useRef, useState } from 'react'
import {
  ArrowClockwise,
  ArrowUpRight,
  CheckCircle,
  ChatCircleDots,
  CircleNotch,
  Clock,
  FileText,
  FilmStrip,
  ImageSquare,
  LockKey,
  Pulse,
  WarningCircle,
  XCircle,
} from '@phosphor-icons/react'
import { apiFetchBlob } from '@/api/client'
import {
  useProjectOverviewQuery,
  useRecordManualMilestoneCheck,
  useReleaseProjectMilestone,
} from '@/api/hooks'
import { ConflictDetails } from '@/components/conflict-details'
import { Button } from '@/components/ui/button'
import { buttonClassName } from '@/components/ui/button-styles'
import { Card } from '@/components/ui/card'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { ProjectCharterAdoptionBanner } from '@/features/project-charter/ProjectCharterAdoptionBanner'
import { ProjectStageOrientation } from '@/features/project-workbench/ProjectStageOrientation'
import { DocumentFreshnessDialog } from '@/features/project-documents/DocumentFreshnessDialog'
import { DecisionCandidateCard } from '@/features/project-execution/DecisionCandidateCard'
import { ProjectExecutionSetupPanel } from '@/features/project-execution/ProjectExecutionSetupPanel'
import { ReconciliationReviewCard } from '@/features/project-execution/ReconciliationReviewCard'
import { MilestoneRevisionApprovalControl } from '@/features/project-execution/MilestoneRevisionApprovalControl'
import {
  createUserAuthorization,
  newIdempotencyKey,
} from '@/features/project-execution/user-authorization'
import { getApiErrorCode, getApiErrorMessage, isApiStatus } from '@/lib/api-error'
import { useAuthStore } from '@/stores/auth'
import { clearDeletedProjectScope, resolveNextProjectId } from '@/stores/project-scope'
import type {
  AcceptanceCheckSummary,
  CharterRisk,
  DocumentFreshness,
  EvidenceAttachment,
  EvidenceAvailability,
  OverviewProjectionState,
  ProjectMilestoneOverview,
  ProjectNextAction,
  ProjectOverview,
  ProjectRelease,
  TaskProgressCounts,
} from '@/types/generated'
import type { AuthorizationProvenance } from '@/types/generated/bindings/AuthorizationProvenance'
import type { MilestoneAcceptanceCheckState } from '@/types/generated/bindings/MilestoneAcceptanceCheckState'
import { Label } from '@/components/ui/label'
import { Textarea } from '@/components/ui/textarea'

type CountValue = number | bigint

const EVIDENCE_AVAILABILITY_COPY: Record<EvidenceAvailability, string> = {
  available: 'Available proof',
  quarantined: 'Pending review',
  redacted: 'Redacted derivative',
  purged: 'Evidence unavailable',
}

function count(value: CountValue | undefined): number {
  return typeof value === 'bigint' ? Number(value) : (value ?? 0)
}

function formatDate(value: string | null | undefined): string {
  if (!value) return 'No date'
  const date = new Date(value)
  return Number.isNaN(date.getTime())
    ? value
    : date.toLocaleString([], { dateStyle: 'medium', timeStyle: 'short' })
}

function humanize(value: string | null | undefined): string {
  if (!value) return 'Unknown'
  return value.replaceAll('_', ' ').replace(/\b\w/g, (letter) => letter.toUpperCase())
}

function shortId(value: string | null | undefined): string {
  if (!value) return '—'
  return value.length > 16 ? `${value.slice(0, 8)}…${value.slice(-6)}` : value
}

function numberValue(value: number | bigint | null | undefined): number | null {
  if (value === null || value === undefined) return null
  const number = typeof value === 'bigint' ? Number(value) : value
  return Number.isFinite(number) ? number : null
}

async function sha256Hex(value: string): Promise<string> {
  const bytes = new TextEncoder().encode(value)
  const digest = await crypto.subtle.digest('SHA-256', bytes)
  return Array.from(new Uint8Array(digest), (byte) => byte.toString(16).padStart(2, '0')).join('')
}

type ReleaseCandidate = {
  milestoneId: string
  canonicalId: string
  label: string
  snapshotId: string
  digest: string
  expectedMilestoneVersion: number
  readinessExpectedMilestoneVersion: number
}

type ManualAttestationCandidate = {
  milestoneId: string
  milestoneLabel: string
  definitionRevisionId: string
  charterRevisionId: string
  check: MilestoneAcceptanceCheckState
}

function formatDuration(value: number | null): string {
  if (value === null || !Number.isFinite(value)) return 'Duration pending'
  const seconds = Math.max(0, Math.round(value))
  const minutes = Math.floor(seconds / 60)
  return `${minutes}:${String(seconds % 60).padStart(2, '0')}`
}

function statusClass(status: string): string {
  if (['released', 'pass', 'current', 'approved', 'available'].includes(status)) {
    return 'border-success/30 bg-success/10 text-success'
  }
  if (
    [
      'stale',
      'changes_pending',
      'reconciliation_required',
      'superseded',
      'waived',
      'quarantined',
      'ready_for_release',
    ].includes(status)
  ) {
    return 'border-warning/40 bg-warning/10 text-foreground'
  }
  if (
    ['failed', 'fail', 'blocked', 'invalidated', 'purged', 'redacted', 'error'].includes(status)
  ) {
    return 'border-destructive/30 bg-destructive/10 text-destructive'
  }
  return 'border-border-subtle bg-muted text-muted-foreground'
}

function StatusLabel({ status }: { status: string }) {
  return (
    <span
      className={`inline-flex max-w-full items-center rounded-full border px-2 py-0.5 font-mono text-micro font-semibold uppercase tracking-[0.08em] ${statusClass(status)}`}
    >
      {humanize(status)}
    </span>
  )
}

function SectionCard({
  title,
  eyebrow,
  children,
  className,
  action,
}: {
  title: string
  eyebrow?: string
  children: ReactNode
  className?: string
  action?: React.ReactNode
}) {
  return (
    <Card className={`min-w-0 border-border-subtle bg-card ${className ?? ''}`}>
      <div className="flex min-w-0 flex-wrap items-start justify-between gap-3 border-b border-border-subtle px-4 py-3 sm:px-5">
        <div className="min-w-0">
          {eyebrow ? (
            <p className="font-mono text-micro font-semibold uppercase tracking-[0.12em] text-muted-foreground">
              {eyebrow}
            </p>
          ) : null}
          <h2 className="mt-1 break-words text-sm font-semibold text-foreground">{title}</h2>
        </div>
        {action}
      </div>
      <div className="min-w-0 p-4 sm:p-5">{children}</div>
    </Card>
  )
}

function MetricGrid({ counts }: { counts: TaskProgressCounts }) {
  const metrics = [
    ['Total', counts.total],
    ['Backlog', counts.backlog],
    ['Active', counts.active],
    ['Review', counts.review],
    ['Blocked', counts.blocked],
    ['Terminal', counts.terminal],
  ] as const

  return (
    <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 xl:grid-cols-2">
      {metrics.map(([label, value]) => (
        <div
          key={label}
          className="min-w-0 rounded-md border border-border-subtle bg-muted/40 px-3 py-2"
        >
          <p className="text-xs text-muted-foreground">{label}</p>
          <p className="mt-1 font-mono text-lg font-semibold tabular-nums text-foreground">
            {count(value)}
          </p>
        </div>
      ))}
    </div>
  )
}

function CheckSummary({ summary }: { summary: AcceptanceCheckSummary }) {
  const checks = [
    ['Passed', summary.passed, 'pass'],
    ['Failed', summary.failed, 'fail'],
    ['Missing', summary.missing, 'missing'],
    ['Stale', summary.stale, 'stale'],
    ['Waived', summary.waived, 'waived'],
    ['Unavailable', summary.unavailable, 'unavailable'],
  ] as const

  return (
    <div>
      <div className="mb-3 flex flex-wrap items-baseline justify-between gap-2">
        <p className="text-xs text-muted-foreground">Required acceptance checks</p>
        <p className="font-mono text-xs text-foreground">
          {count(summary.passed)} / {count(summary.required_total)} passed
        </p>
      </div>
      <div className="grid grid-cols-2 gap-2 sm:grid-cols-3">
        {checks.map(([label, value, state]) => (
          <div key={label} className="min-w-0 rounded-md border border-border-subtle px-3 py-2">
            <div className="flex items-center gap-1.5">
              {state === 'pass' ? (
                <CheckCircle size={14} className="shrink-0 text-success" aria-hidden />
              ) : state === 'fail' ? (
                <XCircle size={14} className="shrink-0 text-destructive" aria-hidden />
              ) : (
                <CircleNotch size={14} className="shrink-0 text-muted-foreground" aria-hidden />
              )}
              <span className="min-w-0 truncate text-xs text-muted-foreground">{label}</span>
            </div>
            <p className="mt-1 font-mono text-lg font-semibold tabular-nums text-foreground">
              {count(value)}
            </p>
          </div>
        ))}
      </div>
    </div>
  )
}

function AcceptanceChecksPanel({
  milestones,
  hasUser,
  pending,
  onReview,
}: {
  milestones: ProjectMilestoneOverview[]
  hasUser: boolean
  pending: boolean
  onReview: (candidate: ManualAttestationCandidate) => void
}) {
  const checks = milestones.flatMap((item) =>
    (item.current_checks ?? []).map((check) => ({ item, check })),
  )

  if (checks.length === 0) {
    return (
      <p className="mt-4 border-t border-border-subtle pt-4 text-xs text-muted-foreground">
        No current acceptance-check definitions are available.
      </p>
    )
  }

  return (
    <ul className="mt-4 divide-y divide-border-subtle border-t border-border-subtle">
      {checks.map(({ item, check }) => {
        const evidenceRequirement = item.definition.content.evidence_requirements.find(
          (requirement) => requirement.id === check.id && requirement.required,
        )
        const evidenceAttached = item.evidence.some(
          (evidence) =>
            evidence.availability === 'available' &&
            evidence.acceptance_check_ids.includes(check.id),
        )
        const resultStatus = check.latest_result ?? 'missing'
        const evidenceKindLabel = evidenceRequirement?.evidence_kind
          ? `${humanize(evidenceRequirement.evidence_kind)} `
          : ''
        const canAttest =
          Boolean(evidenceRequirement) &&
          check.source_kind === 'manual' &&
          !['pass', 'waived'].includes(resultStatus) &&
          check.version > 0

        return (
          <li key={`${item.milestone.id}:${check.id}`} className="min-w-0 py-4 last:pb-0">
            <div className="flex min-w-0 flex-wrap items-start justify-between gap-3">
              <div className="min-w-0 flex-1">
                <div className="flex min-w-0 flex-wrap items-center gap-2">
                  <StatusLabel status={resultStatus} />
                  <span className="font-mono text-micro text-muted-foreground">
                    {humanize(check.source_kind)} · {check.required ? 'required' : 'optional'}
                  </span>
                </div>
                <p className="mt-2 break-words text-sm leading-6 text-foreground">
                  {check.description}
                </p>
                <p className="mt-1 break-all font-mono text-micro text-muted-foreground">
                  {item.milestone.canonical_id} · check {shortId(check.id)} · definition{' '}
                  {shortId(item.definition.id)} · check v{count(check.version)}
                </p>
                <p
                  className={`mt-2 text-xs ${
                    evidenceRequirement && evidenceAttached ? 'text-success' : 'text-warning'
                  }`}
                >
                  {evidenceRequirement
                    ? evidenceAttached
                      ? `Required ${evidenceKindLabel}evidence is attached.`
                      : `Required ${evidenceKindLabel}evidence is still missing.`
                    : 'No required evidence contract is linked to this check; the milestone must be revised before release.'}
                </p>
              </div>
              {canAttest ? (
                <Button
                  type="button"
                  size="sm"
                  variant="outline"
                  disabled={!hasUser || pending}
                  onClick={() =>
                    onReview({
                      milestoneId: item.milestone.id,
                      milestoneLabel: item.milestone.display_label ?? item.definition.content.name,
                      definitionRevisionId: item.definition.id,
                      charterRevisionId:
                        item.definition.content.charter_revision?.revision_id ?? '',
                      check,
                    })
                  }
                >
                  {!hasUser ? 'Sign in to attest' : pending ? 'Recording…' : 'Record attestation'}
                </Button>
              ) : null}
            </div>
            {check.source_kind !== 'manual' && !['pass', 'waived'].includes(resultStatus) ? (
              <p className="mt-2 text-xs text-muted-foreground">
                Waiting for the authoritative {humanize(check.source_kind).toLowerCase()} result;
                this cannot be replaced by a user attestation.
              </p>
            ) : null}
          </li>
        )
      })}
    </ul>
  )
}

function ManualAttestationDialog({
  candidate,
  open,
  pending,
  error,
  onOpenChange,
  onConfirm,
}: {
  candidate: ManualAttestationCandidate | null
  open: boolean
  pending: boolean
  error: string | null
  onOpenChange: (open: boolean) => void
  onConfirm: (status: 'pass' | 'fail', observation: string) => void
}) {
  const [status, setStatus] = useState<'pass' | 'fail' | null>(null)
  const [observation, setObservation] = useState('')

  useEffect(() => {
    if (open) {
      setStatus(null)
      setObservation('')
    }
  }, [open, candidate?.check.id])

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <DialogHeader>
          <DialogTitle>Record manual acceptance</DialogTitle>
          <DialogDescription>
            This writes an authoritative user result for one immutable check. It does not attach
            evidence and does not release the milestone.
          </DialogDescription>
        </DialogHeader>
        {candidate ? (
          <div className="space-y-4">
            <div className="rounded-md border border-border-subtle bg-muted/30 p-3">
              <p className="text-xs font-semibold text-foreground">{candidate.milestoneLabel}</p>
              <p className="mt-2 break-words text-sm leading-6 text-foreground">
                {candidate.check.description}
              </p>
              <p className="mt-2 break-all font-mono text-micro text-muted-foreground">
                check {candidate.check.id} · definition {candidate.definitionRevisionId} · v
                {count(candidate.check.version)}
              </p>
            </div>
            <fieldset>
              <legend className="text-xs font-medium text-foreground">Observed result</legend>
              <div className="mt-2 grid grid-cols-2 gap-2">
                <Button
                  type="button"
                  variant={status === 'pass' ? 'default' : 'outline'}
                  aria-pressed={status === 'pass'}
                  onClick={() => setStatus('pass')}
                >
                  <CheckCircle size={15} aria-hidden /> Pass
                </Button>
                <Button
                  type="button"
                  variant={status === 'fail' ? 'destructive' : 'outline'}
                  aria-pressed={status === 'fail'}
                  onClick={() => setStatus('fail')}
                >
                  <XCircle size={15} aria-hidden /> Fail
                </Button>
              </div>
            </fieldset>
            <div className="space-y-2">
              <Label htmlFor="manual-attestation-observation">Observation</Label>
              <Textarea
                id="manual-attestation-observation"
                value={observation}
                onChange={(event) => setObservation(event.target.value)}
                placeholder="Describe exactly what you observed. This note is not evidence."
                rows={4}
              />
            </div>
            {error ? (
              <p className="break-words text-xs leading-5 text-destructive" role="alert">
                {error}
              </p>
            ) : null}
          </div>
        ) : null}
        <DialogFooter>
          <Button
            type="button"
            variant="ghost"
            disabled={pending}
            onClick={() => onOpenChange(false)}
          >
            Cancel
          </Button>
          <Button
            type="button"
            disabled={!status || observation.trim().length < 3 || pending}
            aria-busy={pending}
            onClick={() => {
              if (status) onConfirm(status, observation.trim())
            }}
          >
            {pending ? 'Recording exact result…' : 'Record result'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function OutcomeCard({
  item,
  primary,
  projectionState,
  hasUser,
  releasePending,
  releaseError,
  onReviewRelease,
}: {
  item: ProjectMilestoneOverview
  primary: boolean
  projectionState: OverviewProjectionState
  hasUser: boolean
  releasePending: boolean
  releaseError: string | null
  onReviewRelease: (candidate: ReleaseCandidate) => void
}) {
  const content = item.definition.content
  const availableEvidenceCount = item.evidence.filter(
    (evidence) => evidence.availability === 'available',
  ).length
  const unavailableEvidenceCount = item.evidence.length - availableEvidenceCount
  const blockers = item.milestone.projection_reasons.filter((reason) =>
    ['blocked', 'stale', 'conflict', 'error'].some((term) =>
      `${reason.kind} ${reason.code}`.toLowerCase().includes(term),
    ),
  )
  const readiness = item.latest_readiness
  const freshness = item.readiness_freshness
  const readinessId = readiness?.id ?? null
  const readinessDigest = readiness?.readiness_digest ?? null
  const expectedMilestoneVersion = numberValue(item.milestone.version)
  const readinessExpectedMilestoneVersion = numberValue(readiness?.expected_milestone_version)
  const readinessIsFresh = readiness?.result === 'ready' && freshness?.status === 'current'
  const releaseCandidate =
    readinessId &&
    readinessDigest &&
    expectedMilestoneVersion !== null &&
    readinessExpectedMilestoneVersion !== null
      ? {
          milestoneId: item.milestone.id,
          canonicalId: item.milestone.canonical_id,
          label: item.milestone.display_label ?? content.name,
          snapshotId: readinessId,
          digest: readinessDigest,
          expectedMilestoneVersion,
          readinessExpectedMilestoneVersion,
        }
      : null

  return (
    <article className="min-w-0 rounded-lg border border-border-subtle bg-background p-4">
      <div className="flex min-w-0 flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex min-w-0 flex-wrap items-center gap-2">
            <p className="font-mono text-micro font-semibold uppercase tracking-[0.12em] text-muted-foreground">
              {item.milestone.canonical_id}
            </p>
            {primary ? <StatusLabel status="primary" /> : null}
            <StatusLabel status={item.milestone.lifecycle} />
          </div>
          <h3 className="mt-2 break-words text-base font-semibold text-foreground">
            {item.milestone.display_label ?? content.name}
          </h3>
        </div>
        {item.latest_readiness ? <StatusLabel status={item.latest_readiness.result} /> : null}
      </div>

      <p className="mt-3 break-words text-sm leading-6 text-foreground">{content.outcome}</p>

      <div className="mt-4 grid min-w-0 gap-3 sm:grid-cols-2">
        <ScopeList label="Included scope" values={content.included_scope} />
        <ScopeList label="Excluded scope" values={content.excluded_scope} muted />
      </div>

      {blockers.length > 0 ? (
        <div className="mt-4 rounded-md border border-warning/40 bg-warning/10 p-3" role="status">
          <div className="flex items-start gap-2">
            <WarningCircle size={16} className="mt-0.5 shrink-0 text-warning" aria-hidden />
            <div className="min-w-0">
              <p className="text-xs font-semibold text-foreground">Projection blockers</p>
              <ul className="mt-1 space-y-1 text-xs leading-5 text-muted-foreground">
                {blockers.map((reason) => (
                  <li key={`${reason.code}-${reason.message}`} className="break-words">
                    {reason.message}
                  </li>
                ))}
              </ul>
            </div>
          </div>
        </div>
      ) : null}

      {readiness ? (
        <div className="mt-4 rounded-md border border-border-subtle bg-muted/30 p-3">
          <div className="flex min-w-0 flex-wrap items-start justify-between gap-3">
            <div className="min-w-0">
              <p className="text-xs font-semibold text-foreground">Readiness candidate</p>
              <p className="mt-1 break-words text-xs text-muted-foreground">
                This is an immutable candidate for review. It does not release the milestone by
                itself.
              </p>
            </div>
            {releaseCandidate ? (
              <Button
                type="button"
                size="sm"
                disabled={
                  releasePending ||
                  !hasUser ||
                  !readinessIsFresh ||
                  projectionState !== 'current' ||
                  item.milestone.lifecycle === 'released'
                }
                aria-busy={releasePending}
                onClick={() => onReviewRelease(releaseCandidate)}
                aria-label={`Review exact release snapshot for ${item.milestone.canonical_id}`}
              >
                {releasePending
                  ? 'Releasing exact snapshot…'
                  : item.milestone.lifecycle === 'released'
                    ? 'Already released'
                    : !hasUser
                      ? 'Sign in to release'
                      : projectionState !== 'current' || !readinessIsFresh
                        ? 'Refresh before release'
                        : 'Release exact snapshot'}
              </Button>
            ) : null}
          </div>
          <dl className="mt-3 grid min-w-0 gap-2 border-t border-border-subtle pt-3 text-xs sm:grid-cols-3">
            <div className="min-w-0">
              <dt className="text-muted-foreground">Snapshot</dt>
              <dd
                className="break-all font-mono text-micro text-foreground"
                title={readinessId ?? undefined}
              >
                {shortId(readinessId)}
              </dd>
            </div>
            <div className="min-w-0">
              <dt className="text-muted-foreground">Digest</dt>
              <dd
                className="break-all font-mono text-micro text-foreground"
                title={readinessDigest ?? undefined}
              >
                {shortId(readinessDigest)}
              </dd>
            </div>
            <div className="min-w-0">
              <dt className="text-muted-foreground">Release CAS</dt>
              <dd className="font-mono text-micro text-foreground">
                {expectedMilestoneVersion === null
                  ? 'Not recorded'
                  : `v${expectedMilestoneVersion}`}
              </dd>
            </div>
            <div className="min-w-0 sm:col-span-3">
              <dt className="text-muted-foreground">Readiness captured against</dt>
              <dd className="font-mono text-micro text-muted-foreground">
                {readinessExpectedMilestoneVersion === null
                  ? 'Not recorded'
                  : `milestone v${readinessExpectedMilestoneVersion}`}
              </dd>
            </div>
          </dl>
          {!hasUser ? (
            <p className="mt-2 text-xs text-warning" role="status">
              Sign in again before releasing this exact snapshot.
            </p>
          ) : !readinessIsFresh ? (
            <p className="mt-2 text-xs text-warning" role="status">
              {freshness?.reason ??
                (freshness
                  ? `This candidate is ${humanize(freshness.status)}; refresh readiness before releasing.`
                  : 'Readiness freshness is unavailable; refresh the Overview before releasing.')}
            </p>
          ) : projectionState !== 'current' ? (
            <p className="mt-2 text-xs text-warning" role="status">
              The Overview projection is not current. Refresh it before releasing this exact
              snapshot.
            </p>
          ) : null}
          {releaseError ? (
            <p className="mt-2 break-words text-xs leading-5 text-destructive" role="alert">
              {releaseError}
            </p>
          ) : null}
        </div>
      ) : null}

      <div className="mt-4 grid gap-3 border-t border-border-subtle pt-3 sm:grid-cols-3">
        <div>
          <p className="text-xs text-muted-foreground">Milestone Tasks</p>
          <p className="mt-1 font-mono text-sm text-foreground">
            {count(item.task_counts.active)} active · {count(item.task_counts.blocked)} blocked ·{' '}
            {count(item.task_counts.terminal)} terminal
          </p>
        </div>
        <div>
          <p className="text-xs text-muted-foreground">Acceptance checks</p>
          <p className="mt-1 font-mono text-sm text-foreground">
            {count(item.check_summary.passed)} passed · {count(item.check_summary.failed)} failed ·{' '}
            {count(item.check_summary.missing)} missing
          </p>
        </div>
        <div>
          <p className="text-xs text-muted-foreground">Evidence coverage</p>
          <p className="mt-1 font-mono text-sm text-foreground">
            {availableEvidenceCount}/{item.evidence.length} available
          </p>
          {unavailableEvidenceCount > 0 ? (
            <p className="mt-1 text-xs text-warning">
              {unavailableEvidenceCount} attachment{unavailableEvidenceCount === 1 ? '' : 's'}{' '}
              unavailable
            </p>
          ) : null}
        </div>
      </div>
    </article>
  )
}

function ScopeList({
  label,
  values,
  muted = false,
}: {
  label: string
  values: string[]
  muted?: boolean
}) {
  return (
    <div className="min-w-0">
      <p className="text-xs font-medium text-muted-foreground">{label}</p>
      {values.length === 0 ? (
        <p className="mt-1 text-xs italic text-muted-foreground">None recorded</p>
      ) : (
        <ul
          className={`mt-1 space-y-1 text-xs leading-5 ${muted ? 'text-muted-foreground' : 'text-foreground'}`}
        >
          {values.slice(0, 4).map((value) => (
            <li key={value} className="break-words">
              {value}
            </li>
          ))}
          {values.length > 4 ? (
            <li className="font-mono text-micro text-muted-foreground">
              +{values.length - 4} more
            </li>
          ) : null}
        </ul>
      )}
    </div>
  )
}

function DocumentFreshnessPanel({
  projectId,
  documents,
}: {
  projectId: string
  documents: DocumentFreshness[]
}) {
  // A freshness row is a summary; the record behind it (approved text,
  // working text, the diff between them, and the revision history) opens in
  // place so "changes pending" is something the user can actually read.
  const [selected, setSelected] = useState<DocumentFreshness | null>(null)
  return (
    <SectionCard title="Document freshness" eyebrow="Canonical Project Documents">
      {documents.length === 0 ? (
        <EmptyInline text="No Project Documents are recorded yet. The Project Agent can propose a bounded document when the outcome needs one." />
      ) : (
        <ul className="divide-y divide-border-subtle">
          {documents.map((document) => (
            <li key={document.document_id} className="min-w-0 py-3 first:pt-0 last:pb-0">
              <button
                type="button"
                onClick={() => setSelected(document)}
                aria-label={`Inspect ${humanize(document.kind)} document`}
                className="-mx-1.5 flex w-[calc(100%+0.75rem)] min-w-0 items-start gap-2 rounded-md px-1.5 py-1 text-left transition-colors hover:bg-muted/40 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
              >
                <FileText size={16} className="mt-0.5 shrink-0 text-muted-foreground" aria-hidden />
                <div className="min-w-0 flex-1">
                  <div className="flex min-w-0 flex-wrap items-center gap-2">
                    <p className="break-words text-sm font-medium text-foreground">
                      {humanize(document.kind)}
                    </p>
                    <StatusLabel status={document.status} />
                  </div>
                  {document.approved_revision_id ? (
                    <p className="mt-1 break-all font-mono text-micro text-muted-foreground">
                      approved revision {shortId(document.approved_revision_id)} · digest{' '}
                      {shortId(document.approved_digest)}
                    </p>
                  ) : (
                    <p className="mt-1 text-xs text-muted-foreground">No approved revision yet.</p>
                  )}
                  {document.working_revision_id ? (
                    <p className="mt-1 break-all font-mono text-micro text-warning">
                      working {document.working_lifecycle ?? 'revision'}{' '}
                      {shortId(document.working_revision_id)}
                      {document.working_digest
                        ? ` · digest ${shortId(document.working_digest)}`
                        : ''}
                    </p>
                  ) : null}
                  {document.reason ? (
                    <p className="mt-1 break-words text-xs text-warning">{document.reason}</p>
                  ) : null}
                </div>
                <span className="inline-flex shrink-0 items-center gap-1 text-micro font-semibold text-primary">
                  Inspect <ArrowUpRight size={12} aria-hidden />
                </span>
              </button>
            </li>
          ))}
        </ul>
      )}
      <DocumentFreshnessDialog
        projectId={projectId}
        document={selected}
        open={selected !== null}
        onOpenChange={(open) => {
          if (!open) setSelected(null)
        }}
      />
    </SectionCard>
  )
}

function DecisionsAndRisks({
  projectId,
  overview,
}: {
  projectId: string
  overview: ProjectOverview
}) {
  const decisions = overview.decisions
  return (
    <SectionCard title="Decisions & risks" eyebrow="Authority Ledger">
      <div className="space-y-4">
        <div>
          <p className="text-xs font-medium text-muted-foreground">Pending proposals</p>
          <div className="mt-2">
            <DecisionCandidateCard projectId={projectId} candidates={overview.pending_decisions} />
          </div>
        </div>
        <div className="border-t border-border-subtle pt-4">
          <p className="text-xs font-medium text-muted-foreground">Decision log</p>
          {decisions.length === 0 ? (
            <EmptyInline text="No effective decisions are recorded in the current authority ledger." />
          ) : (
            <ul className="mt-2 space-y-2">
              {decisions.map((decision) => {
                // Keep the affected authority references bounded and typed; the full records remain
                // available from their canonical views.
                const affected = [
                  ...decision.affected_artifact_refs.map(
                    (ref) => `artifact ${shortId(ref.artifact_id)}`,
                  ),
                  ...decision.affected_task_ids.map((id) => `task ${shortId(id)}`),
                  ...decision.affected_milestone_ids.map((id) => `milestone ${shortId(id)}`),
                ]
                return (
                  <li
                    key={decision.id}
                    className="min-w-0 rounded-md border border-border-subtle bg-muted/30 px-3 py-2"
                  >
                    <div className="flex min-w-0 flex-wrap items-start justify-between gap-2">
                      <p className="break-words text-sm text-foreground">{decision.question}</p>
                      <StatusLabel status={decision.state} />
                    </div>
                    {decision.context ? (
                      <p className="mt-1 break-words text-xs leading-5 text-muted-foreground">
                        Context: <span className="text-foreground">{decision.context}</span>
                      </p>
                    ) : null}
                    {decision.options.length > 0 ? (
                      <p className="mt-1 break-words text-xs leading-5 text-muted-foreground">
                        Alternatives: {decision.options.join(' · ')}
                      </p>
                    ) : null}
                    <p className="mt-1 break-words text-xs text-muted-foreground">
                      Outcome: <span className="text-foreground">{decision.selected_outcome}</span>
                    </p>
                    <p className="mt-1 break-words text-xs leading-5 text-muted-foreground">
                      Rationale: <span className="text-foreground">{decision.rationale}</span>
                    </p>
                    <p className="mt-1 break-all font-mono text-micro text-muted-foreground">
                      Principal:{' '}
                      <span className="text-foreground">
                        {decision.decision_maker.display_name ?? decision.decision_maker.id}
                      </span>{' '}
                      · Class: {humanize(decision.decision_class)}
                    </p>
                    <p className="mt-1 break-all font-mono text-micro text-muted-foreground">
                      Decision ID {shortId(decision.id)}
                    </p>
                    {affected.length > 0 ? (
                      <p className="mt-1 break-all font-mono text-micro text-muted-foreground">
                        Affected: {affected.join(' · ')}
                      </p>
                    ) : null}
                  </li>
                )
              })}
            </ul>
          )}
        </div>
        <div className="border-t border-border-subtle pt-4">
          <p className="text-xs font-medium text-muted-foreground">Charter risks</p>
          {overview.risks.length === 0 ? (
            <EmptyInline text="No active risk is recorded in the current Charter projection." />
          ) : (
            <ul className="mt-2 space-y-3">
              {overview.risks.map((risk) => (
                <RiskRow key={risk.id} risk={risk} />
              ))}
            </ul>
          )}
        </div>
      </div>
    </SectionCard>
  )
}

function RiskRow({ risk }: { risk: CharterRisk }) {
  return (
    <li className="min-w-0 border-l-2 border-warning/50 pl-3">
      <p className="break-words text-sm text-foreground">{risk.description}</p>
      {risk.impact ? (
        <p className="mt-1 break-words text-xs text-muted-foreground">Impact: {risk.impact}</p>
      ) : null}
      {risk.treatment ? (
        <p className="mt-1 break-words text-xs text-muted-foreground">
          Treatment: {risk.treatment}
        </p>
      ) : null}
      <p className="mt-1 break-all font-mono text-micro text-muted-foreground">
        {risk.owner?.display_name ?? risk.owner?.id ?? 'Unassigned'} · {shortId(risk.id)}
      </p>
    </li>
  )
}

function EvidenceGallery({
  projectId,
  evidence,
}: {
  projectId: string
  evidence: EvidenceAttachment[]
}) {
  const availableCount = evidence.filter((item) => item.availability === 'available').length
  return (
    <SectionCard
      title="Evidence"
      eyebrow="Bounded proof media"
      action={
        <span className="font-mono text-micro text-muted-foreground">
          Coverage {availableCount}/{evidence.length} available
        </span>
      }
    >
      {evidence.length === 0 ? (
        <EmptyInline text="No evidence is attached to this Project projection yet. Evidence capture remains available from Tasks and Project Agent Chat." />
      ) : (
        <div
          className="min-w-0 overflow-x-auto overscroll-x-contain pb-1 snap-x snap-mandatory focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring min-[520px]:overflow-visible min-[520px]:snap-none"
          role="region"
          aria-label="Evidence gallery"
          tabIndex={0}
        >
          <div className="flex min-w-0 gap-3 min-[520px]:grid min-[520px]:grid-cols-2">
            {evidence.map((item) => (
              <div
                key={item.id}
                className="min-w-[min(18rem,calc(100vw-4rem))] shrink-0 snap-start min-[520px]:min-w-0 min-[520px]:shrink"
              >
                <EvidenceTile projectId={projectId} item={item} />
              </div>
            ))}
          </div>
        </div>
      )}
    </SectionCard>
  )
}

function EvidenceTile({ projectId, item }: { projectId: string; item: EvidenceAttachment }) {
  const [mediaUrl, setMediaUrl] = useState<string | null>(null)
  const [previewLoading, setPreviewLoading] = useState(false)
  const [previewError, setPreviewError] = useState(false)
  const [previewAttempt, setPreviewAttempt] = useState(0)
  const [duration, setDuration] = useState<number | null>(null)
  const [previewOpen, setPreviewOpen] = useState(false)
  const [textPreview, setTextPreview] = useState<string | null>(null)
  const [blobType, setBlobType] = useState<string | null>(null)
  const isVideo = item.kind === 'walkthrough_video'
  const hasVisualPreview = item.kind === 'screenshot' || isVideo
  const mediaPath = `/projects/${projectId}/media/${encodeURIComponent(item.asset_id)}`
  const icon = isVideo ? (
    <FilmStrip size={24} aria-hidden />
  ) : item.kind === 'screenshot' ? (
    <ImageSquare size={24} aria-hidden />
  ) : (
    <FileText size={24} aria-hidden />
  )
  const sourceTaskId = item.source_task_id ?? item.task_id
  const provenance = [
    sourceTaskId ? `Task ${shortId(sourceTaskId)}` : null,
    item.source_run_id ? `run ${shortId(item.source_run_id)}` : null,
    item.source_validation_id ? `validation ${shortId(item.source_validation_id)}` : null,
    item.author ? `uploaded by ${item.author.display_name ?? shortId(item.author.id)}` : null,
  ].filter((value): value is string => Boolean(value))
  const showPreview =
    item.availability === 'available' && hasVisualPreview && !previewError && Boolean(mediaUrl)

  useEffect(() => {
    let cancelled = false
    let objectUrl: string | null = null
    const shouldLoad = item.availability === 'available'

    setMediaUrl(null)
    setPreviewError(false)
    setPreviewLoading(shouldLoad)
    setDuration(null)
    setTextPreview(null)
    setBlobType(null)
    if (!shouldLoad) return

    void apiFetchBlob(mediaPath)
      .then((blob) => {
        if (cancelled) return
        if (typeof URL.createObjectURL !== 'function') {
          setPreviewError(true)
          return
        }
        objectUrl = URL.createObjectURL(blob)
        setMediaUrl(objectUrl)
        setBlobType(blob.type || null)
        // Text-like evidence (logs, reports) previews inline in the modal; the
        // bytes are already authorized and in hand, so read a bounded slice.
        const isTextLike =
          !hasVisualPreview && (blob.type.startsWith('text/') || blob.type === 'application/json')
        if (isTextLike && typeof blob.text === 'function') {
          void blob
            .text()
            .then((text) => {
              if (!cancelled) setTextPreview(text.slice(0, 20_000))
            })
            .catch(() => {})
        }
      })
      .catch(() => {
        if (!cancelled) setPreviewError(true)
      })
      .finally(() => {
        if (!cancelled) setPreviewLoading(false)
      })

    return () => {
      cancelled = true
      if (objectUrl) URL.revokeObjectURL(objectUrl)
    }
  }, [hasVisualPreview, item.availability, mediaPath, previewAttempt])

  function failPreview() {
    setPreviewError(true)
    if (mediaUrl) {
      URL.revokeObjectURL(mediaUrl)
      setMediaUrl(null)
    }
  }

  return (
    <article className="min-w-0 overflow-hidden rounded-md border border-border-subtle bg-background">
      <div className="flex aspect-video items-center justify-center border-b border-border-subtle bg-muted text-muted-foreground">
        {previewLoading ? (
          <div className="flex flex-col items-center gap-2 px-4 text-center">
            {icon}
            <p className="text-xs font-medium">
              Loading authorized {hasVisualPreview ? 'preview' : 'evidence file'}…
            </p>
            <p className="text-micro text-muted-foreground">
              Forge is fetching this evidence with your Project authorization.
            </p>
          </div>
        ) : showPreview ? (
          isVideo ? (
            <video
              src={mediaUrl ?? undefined}
              controls
              preload="metadata"
              poster="/logo.png"
              playsInline
              width="640"
              height="360"
              aria-label={item.caption}
              className="h-full w-full object-cover"
              onLoadedMetadata={(event) => setDuration(event.currentTarget.duration)}
              onError={failPreview}
            />
          ) : (
            <img
              src={mediaUrl ?? undefined}
              alt={item.caption}
              loading="lazy"
              width="640"
              height="360"
              className="h-full w-full object-cover"
              onError={failPreview}
            />
          )
        ) : (
          <div className="flex flex-col items-center gap-2 px-4 text-center">
            {icon}
            <p className="text-xs font-medium">
              {item.availability !== 'available'
                ? EVIDENCE_AVAILABILITY_COPY[item.availability]
                : isVideo
                  ? 'Video poster'
                  : item.kind === 'screenshot'
                    ? 'Image preview'
                    : 'Evidence file'}
            </p>
            <p className="text-micro text-muted-foreground">
              {previewError
                ? `${hasVisualPreview ? 'Preview' : 'File'} could not be loaded; metadata is preserved.`
                : item.availability !== 'available'
                  ? 'Metadata is retained, but this asset is not openable in the current state.'
                  : hasVisualPreview
                    ? 'Preview opens from the authorized asset'
                    : 'Open or download from the authorized asset'}
            </p>
            {previewError ? (
              <button
                type="button"
                className="mt-1 text-xs font-medium text-primary underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                onClick={() => setPreviewAttempt((attempt) => attempt + 1)}
              >
                Retry authorized asset
              </button>
            ) : null}
          </div>
        )}
      </div>
      <div className="min-w-0 p-3">
        <div className="flex min-w-0 flex-wrap items-start justify-between gap-2">
          <p className="min-w-0 break-words text-sm font-medium text-foreground">{item.caption}</p>
          <StatusLabel status={item.availability} />
        </div>
        <p className="mt-1 break-words text-xs text-muted-foreground">
          {humanize(item.kind)} · captured {formatDate(item.captured_at)}
        </p>
        {isVideo && item.availability === 'available' ? (
          <p className="mt-1 text-xs text-muted-foreground">
            {formatDuration(duration)} · explicit play controls; video never autoplays
          </p>
        ) : null}
        <p className="mt-1 break-all font-mono text-micro text-muted-foreground">
          asset {shortId(item.asset_id)} · checksum {shortId(item.checksum)}
        </p>
        <p className="mt-2 break-words text-xs text-muted-foreground">
          {provenance.length > 0 ? provenance.join(' · ') : 'Source provenance not recorded'}
        </p>
        {item.acceptance_check_ids.length > 0 ? (
          <p className="mt-2 break-all text-xs text-muted-foreground">
            Supports checks: {item.acceptance_check_ids.map(shortId).join(', ')}
          </p>
        ) : (
          <p className="mt-2 text-xs text-warning">
            No acceptance check linkage; not proof for a check.
          </p>
        )}
        <div className="mt-3 flex flex-wrap gap-2">
          {item.availability === 'available' ? (
            <>
              {mediaUrl ? (
                <>
                  <button
                    type="button"
                    onClick={() => setPreviewOpen(true)}
                    className="inline-flex items-center gap-1 rounded-md border border-input px-2 py-1 text-xs font-medium text-foreground transition-colors hover:bg-muted focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  >
                    Preview{' '}
                    {isVideo ? 'video' : item.kind === 'screenshot' ? 'image' : 'evidence file'}
                  </button>
                  <a
                    href={mediaUrl}
                    download
                    className="inline-flex items-center rounded-md px-2 py-1 text-xs font-medium text-primary transition-colors hover:bg-primary/10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  >
                    Download
                  </a>
                </>
              ) : (
                <span className="text-xs text-muted-foreground">
                  {previewLoading ? 'Loading authorized asset…' : 'Authorized asset unavailable'}
                </span>
              )}
            </>
          ) : (
            <span className="text-xs text-muted-foreground">
              {EVIDENCE_AVAILABILITY_COPY[item.availability]}
            </span>
          )}
        </div>
      </div>
      <Dialog
        open={previewOpen}
        onOpenChange={setPreviewOpen}
        ariaLabel={`Evidence preview: ${item.caption}`}
      >
        <DialogContent className="max-w-4xl">
          <DialogHeader>
            <DialogTitle>{item.caption}</DialogTitle>
            <DialogDescription>
              {humanize(item.kind)} · captured {formatDate(item.captured_at)} · asset{' '}
              {shortId(item.asset_id)}
            </DialogDescription>
          </DialogHeader>
          <div className="mt-3 max-h-[70vh] overflow-auto rounded-md border border-border-subtle bg-muted/20">
            {!mediaUrl ? (
              <p className="p-4 text-sm text-muted-foreground">Authorized asset unavailable.</p>
            ) : isVideo ? (
              <video
                src={mediaUrl}
                controls
                preload="metadata"
                playsInline
                aria-label={`${item.caption} preview`}
                className="mx-auto max-h-[70vh] w-full"
              />
            ) : item.kind === 'screenshot' ? (
              <img
                src={mediaUrl}
                alt={`${item.caption} preview`}
                className="mx-auto max-h-[70vh] w-auto"
              />
            ) : textPreview !== null ? (
              <pre className="whitespace-pre-wrap p-3 font-mono text-xs leading-5 text-foreground">
                {textPreview}
              </pre>
            ) : blobType === 'application/pdf' ? (
              <iframe src={mediaUrl} title={`${item.caption} preview`} className="h-[70vh] w-full" />
            ) : (
              <p className="p-4 text-sm text-muted-foreground">
                No inline preview for this file type. Open it in a new tab or download it.
              </p>
            )}
          </div>
          <DialogFooter className="mt-4 gap-2">
            {mediaUrl ? (
              <>
                <a
                  href={mediaUrl}
                  target="_blank"
                  rel="noreferrer"
                  className="inline-flex items-center gap-1 rounded-md border border-input px-3 py-1.5 text-xs font-medium text-foreground transition-colors hover:bg-muted focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                >
                  Open in new tab <ArrowUpRight size={13} aria-hidden />
                </a>
                <a
                  href={mediaUrl}
                  download
                  className="inline-flex items-center rounded-md px-3 py-1.5 text-xs font-medium text-primary transition-colors hover:bg-primary/10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                >
                  Download
                </a>
              </>
            ) : null}
            <Button type="button" onClick={() => setPreviewOpen(false)}>
              Close
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </article>
  )
}

function ReleaseHistory({
  releases,
  projectId,
}: {
  releases: ProjectRelease[]
  projectId: string
}) {
  return (
    <SectionCard title="Release history" eyebrow="Immutable released truth">
      {releases.length === 0 ? (
        <EmptyInline text="No immutable release snapshots exist yet. A readiness result is only a release candidate." />
      ) : (
        <ol className="space-y-3">
          {releases.map((release) => {
            const snapshot = release.snapshot
            return (
              <li
                key={release.id}
                className="min-w-0 rounded-md border border-border-subtle bg-background p-3"
              >
                <div className="flex min-w-0 flex-wrap items-start justify-between gap-3">
                  <div className="min-w-0">
                    <p className="break-words text-sm font-semibold text-foreground">
                      {snapshot.display_label ?? release.release_identity}
                    </p>
                    <p className="mt-1 font-mono text-micro text-muted-foreground">
                      {snapshot.milestone_canonical_id}-r{count(snapshot.release_revision)} ·{' '}
                      {formatDate(snapshot.released_at)}
                    </p>
                  </div>
                  <StatusLabel status="released" />
                </div>
                <p className="mt-3 break-words text-xs leading-5 text-muted-foreground">
                  {snapshot.summary}
                </p>
                <div className="mt-3 grid gap-2 border-t border-border-subtle pt-3 text-xs sm:grid-cols-2">
                  <p className="break-words text-muted-foreground">
                    Released by{' '}
                    <span className="text-foreground">
                      {snapshot.released_by.display_name ?? snapshot.released_by.id}
                    </span>
                  </p>
                  <p className="break-all font-mono text-micro text-muted-foreground">
                    digest {shortId(snapshot.snapshot_digest)} · {snapshot.evidence_pins.length}{' '}
                    evidence pin{snapshot.evidence_pins.length === 1 ? '' : 's'}
                  </p>
                </div>
                {snapshot.known_issues.length > 0 ? (
                  <p className="mt-2 break-words text-xs text-warning">
                    Known issues: {snapshot.known_issues.join(' · ')}
                  </p>
                ) : null}
                <Link
                  to="/projects/$projectId/releases/$releaseId"
                  params={{ projectId, releaseId: release.id }}
                  className="mt-3 inline-flex items-center gap-1 text-xs font-medium text-primary hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                >
                  Inspect immutable snapshot <ArrowUpRight size={13} aria-hidden />
                </Link>
              </li>
            )
          })}
        </ol>
      )}
    </SectionCard>
  )
}

function EmptyInline({ text }: { text: string }) {
  return <p className="mt-2 break-words text-xs leading-5 text-muted-foreground">{text}</p>
}

function ProjectionBanner({
  state,
  watermark,
  onRetry,
}: {
  state: OverviewProjectionState
  watermark: string
  onRetry: () => void
}) {
  if (state === 'current') return null
  const stale = state === 'stale'
  const loading = state === 'loading'
  return (
    <div
      className={`flex min-w-0 items-start gap-2 rounded-md border p-3 text-sm ${stale ? 'border-warning/40 bg-warning/10' : 'border-border-subtle bg-muted'}`}
      role={state === 'error' ? 'alert' : 'status'}
      aria-live="polite"
    >
      {loading ? (
        <CircleNotch size={17} className="mt-0.5 shrink-0 animate-spin" aria-hidden />
      ) : (
        <Pulse size={17} className="mt-0.5 shrink-0" aria-hidden />
      )}
      <div className="min-w-0">
        <p className="font-medium text-foreground">
          {stale
            ? 'Overview is stale'
            : loading
              ? 'Overview is refreshing'
              : `Overview projection ${humanize(state)}`}
        </p>
        <p className="mt-1 break-words text-xs text-muted-foreground">
          {stale
            ? `Cached progress is shown for inspection only; it is not current release truth. Source watermark ${shortId(watermark)}.`
            : 'Some projection sources are not current. Review the affected canonical records before treating this as ready or released.'}
        </p>
      </div>
      {state !== 'loading' ? (
        <Button
          type="button"
          size="sm"
          variant="outline"
          className="ml-auto shrink-0"
          onClick={onRetry}
        >
          <ArrowClockwise size={13} aria-hidden />
          {stale ? 'Refresh' : 'Retry'}
        </Button>
      ) : null}
    </div>
  )
}

function LoadingState() {
  return (
    <div
      className="mx-auto flex w-full max-w-[1440px] flex-col gap-5"
      aria-busy="true"
      role="status"
    >
      <div className="space-y-3">
        <div className="h-3 w-32 animate-pulse rounded bg-muted" />
        <div className="h-8 w-2/3 animate-pulse rounded bg-muted" />
        <div className="h-4 w-full max-w-2xl animate-pulse rounded bg-muted" />
      </div>
      <div className="grid min-w-0 gap-5 xl:grid-cols-[minmax(0,1.45fr)_minmax(300px,0.75fr)]">
        <div className="space-y-5">
          <div className="h-32 animate-pulse rounded-lg border border-border-subtle bg-muted" />
          <div className="h-72 animate-pulse rounded-lg border border-border-subtle bg-muted" />
        </div>
        <div className="space-y-5">
          <div className="h-56 animate-pulse rounded-lg border border-border-subtle bg-muted" />
          <div className="h-48 animate-pulse rounded-lg border border-border-subtle bg-muted" />
        </div>
      </div>
      Loading Project Overview…
    </div>
  )
}

function DeniedState({ projectId }: { projectId: string }) {
  return (
    <div className="mx-auto flex w-full max-w-2xl flex-col items-center gap-4 py-16 text-center">
      <div className="rounded-full border border-border-subtle bg-muted p-3 text-muted-foreground">
        <LockKey size={22} aria-hidden />
      </div>
      <div>
        <h1 className="text-lg font-semibold text-foreground">Project Overview access denied</h1>
        <p className="mt-2 text-sm leading-6 text-muted-foreground">
          This account is not authorized to read the Project Overview projection. Protected Project
          details and media are withheld.
        </p>
      </div>
      <div className="flex flex-wrap justify-center gap-2">
        <Link
          to="/projects/$projectId/chat"
          params={{ projectId }}
          className={buttonClassName({ variant: 'outline' })}
        >
          Open Project Agent Chat
        </Link>
        <Link
          to="/projects/$projectId/tasks"
          params={{ projectId }}
          search={{ sort_by: 'updated_at', sort_order: 'desc' }}
          className={buttonClassName({ variant: 'ghost' })}
        >
          View Tasks
        </Link>
      </div>
    </div>
  )
}

/**
 * The Project this route names is gone — deleted by this user elsewhere, by
 * another authorized member, or by this same delete just committing. F17 /
 * 8.4.4 requires external deletion and an authorized 404 to converge the
 * same way explicit deletion does: clear the deleted scope and land on
 * another authorized Project or Main Chat, not a dead Overview page.
 */
function DeletedProjectRedirect({ projectId }: { projectId: string }) {
  const queryClient = useQueryClient()
  const navigate = useNavigate()

  useEffect(() => {
    let cancelled = false
    clearDeletedProjectScope(queryClient, projectId)
    void resolveNextProjectId(queryClient, projectId).then((nextProjectId) => {
      if (cancelled) return
      void navigate(
        nextProjectId
          ? { to: '/projects/$projectId/board', params: { projectId: nextProjectId } }
          : { to: '/chat' },
      )
    })
    return () => {
      cancelled = true
    }
  }, [projectId, queryClient, navigate])

  return (
    <div
      className="mx-auto flex w-full max-w-2xl flex-col items-center gap-3 py-16 text-center"
      role="status"
      aria-live="polite"
    >
      <p className="text-sm text-muted-foreground">This Project no longer exists. Redirecting…</p>
    </div>
  )
}

function ErrorState({
  error,
  onRetry,
  projectId,
}: {
  error: unknown
  onRetry: () => void
  projectId: string
}) {
  const conflict = isApiStatus(error, 409)
  return (
    <div className="mx-auto flex w-full max-w-2xl flex-col items-center gap-4 py-16 text-center">
      <WarningCircle size={24} className="text-destructive" aria-hidden />
      <div>
        <h1 className="text-lg font-semibold text-foreground">
          {conflict ? 'Overview projection conflict' : 'Overview unavailable'}
        </h1>
        <p className="mt-2 text-sm leading-6 text-muted-foreground">
          {conflict
            ? 'The displayed projection changed while it was loading. Refresh to reconcile against current canonical records.'
            : 'Forge could not load this Project Overview. Existing Tasks and Project Agent Chat remain available.'}
        </p>
        {conflict ? (
          <ConflictDetails error={error} fallbackAuthority="Project Overview projection" />
        ) : null}
      </div>
      <div className="flex flex-wrap justify-center gap-2">
        <Button onClick={onRetry}>
          <ArrowClockwise size={15} aria-hidden /> Retry
        </Button>
        <Link
          to="/projects/$projectId/chat"
          params={{ projectId }}
          className={buttonClassName({ variant: 'outline' })}
        >
          Open Project Agent Chat
        </Link>
      </div>
    </div>
  )
}

function NextActionCard({
  projectId,
  nextAction,
  milestones,
}: {
  projectId: string
  nextAction: ProjectNextAction | null
  milestones: ProjectMilestoneOverview[]
}) {
  const action = nextAction
  const isReleaseAction = action?.action_kind === 'release'
  const approvalTarget =
    action?.code === 'milestone_definition_approval'
      ? (milestones.find((entry) => entry.definition.id === action.target_id) ?? null)
      : null
  return (
    <SectionCard title="Next action" eyebrow="User decision / action">
      <div className="flex min-w-0 items-start gap-3 rounded-md border border-ember-border bg-ember-surface p-3">
        <Clock size={18} className="mt-0.5 shrink-0 text-primary" aria-hidden />
        <div className="min-w-0">
          <p className="break-words text-sm font-medium text-foreground">
            {action?.title ?? 'No next action recorded'}
          </p>
          {action ? (
            <p className="mt-1 break-words text-xs leading-5 text-muted-foreground">
              {action.explanation}
            </p>
          ) : null}
          {action ? (
            <p className="mt-2 break-all font-mono text-micro text-muted-foreground">
              code {action.code} · {action.target_type} {shortId(action.target_id)}
            </p>
          ) : null}
          {action ? (
            <p className="mt-2 break-all font-mono text-micro text-muted-foreground">
              principal {action.required_principal}
              {action.expected_version !== null
                ? ` · expected version v${numberValue(action.expected_version) ?? '—'}`
                : null}
            </p>
          ) : null}
          {action ? (
            <p className="mt-2 break-all font-mono text-micro text-muted-foreground">
              operation {action.route_or_operation}
            </p>
          ) : null}
          {action ? (
            <p className="mt-2 text-micro font-semibold uppercase tracking-[0.08em] text-muted-foreground">
              {action.blocking ? 'Blocking action' : 'Recommended next action'}
            </p>
          ) : null}
          {approvalTarget && action ? (
            <MilestoneRevisionApprovalControl
              projectId={projectId}
              target={approvalTarget}
              expectedVersion={numberValue(action.expected_version)}
            />
          ) : isReleaseAction ? (
            <a
              href="#readiness"
              className="mt-3 inline-flex items-center gap-1 text-xs font-semibold text-primary hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              Review release readiness <ArrowUpRight size={13} aria-hidden />
            </a>
          ) : (
            <Link
              to="/projects/$projectId/chat"
              params={{ projectId }}
              className="mt-3 inline-flex items-center gap-1 text-xs font-semibold text-primary hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              Continue with Project Agent <ArrowUpRight size={13} aria-hidden />
            </Link>
          )}
        </div>
      </div>
    </SectionCard>
  )
}

function ReleaseReviewDialog({
  candidate,
  open,
  hasUser,
  isPending,
  error,
  onOpenChange,
  onConfirm,
}: {
  candidate: ReleaseCandidate | null
  open: boolean
  hasUser: boolean
  isPending: boolean
  error: string | null
  onOpenChange: (open: boolean) => void
  onConfirm: () => void
}) {
  if (!candidate) return null
  return (
    <Dialog open={open} onOpenChange={onOpenChange} ariaLabel="Review milestone release">
      <DialogContent className="max-w-xl">
        <DialogHeader>
          <DialogTitle>Review release · {candidate.canonicalId}</DialogTitle>
          <DialogDescription>
            Confirm the exact readiness snapshot Forge will record as immutable release truth. Forge
            never releases from a readiness result automatically.
          </DialogDescription>
        </DialogHeader>
        <dl className="mt-5 grid gap-3 rounded-md border border-border-subtle bg-muted/30 p-3 text-xs sm:grid-cols-2">
          <div className="min-w-0">
            <dt className="text-muted-foreground">Outcome</dt>
            <dd className="mt-1 break-words font-medium text-foreground">{candidate.label}</dd>
          </div>
          <div className="min-w-0">
            <dt className="text-muted-foreground">Release milestone version</dt>
            <dd className="mt-1 font-mono text-micro text-foreground">
              v{candidate.expectedMilestoneVersion}
            </dd>
          </div>
          <div className="min-w-0 sm:col-span-2">
            <dt className="text-muted-foreground">Readiness snapshot ID</dt>
            <dd className="mt-1 break-all font-mono text-micro text-foreground">
              {candidate.snapshotId}
            </dd>
          </div>
          <div className="min-w-0 sm:col-span-2">
            <dt className="text-muted-foreground">Readiness digest</dt>
            <dd className="mt-1 break-all font-mono text-micro text-foreground">
              {candidate.digest}
            </dd>
          </div>
          <div className="min-w-0 sm:col-span-2">
            <dt className="text-muted-foreground">Readiness captured against</dt>
            <dd className="mt-1 font-mono text-micro text-muted-foreground">
              milestone v{candidate.readinessExpectedMilestoneVersion}
            </dd>
          </div>
        </dl>
        {!hasUser ? (
          <p className="mt-3 text-xs text-warning" role="status">
            Sign in again before releasing this milestone. The server accepts only an authorization
            receipt for the current user.
          </p>
        ) : null}
        {error ? (
          <p className="mt-3 break-words text-xs leading-5 text-destructive" role="alert">
            {error}
          </p>
        ) : null}
        <DialogFooter className="mt-5 gap-2">
          <Button variant="outline" onClick={() => onOpenChange(false)} disabled={isPending}>
            Cancel
          </Button>
          <Button disabled={!hasUser || isPending} onClick={onConfirm}>
            {isPending ? 'Releasing exact snapshot…' : 'Confirm release'}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

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
