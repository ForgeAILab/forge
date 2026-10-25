// Project milestone outcomes and manual acceptance checks.

import { useEffect, useState } from 'react'
import { CheckCircle, WarningCircle, XCircle } from '@phosphor-icons/react'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import type { OverviewProjectionState, ProjectMilestoneOverview } from '@/types/generated'
import { Label } from '@/components/ui/label'
import { Textarea } from '@/components/ui/textarea'
import { count, humanize, shortId, numberValue } from './utils'
import type { ManualAttestationCandidate, ReleaseCandidate } from './utils'
import { StatusLabel } from './shared'

export function AcceptanceChecksPanel({
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

export function ManualAttestationDialog({
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

export function OutcomeCard({
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
