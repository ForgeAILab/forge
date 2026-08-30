import type { ReactNode } from 'react'
import {
  ArrowLeft,
  ArrowUpRight,
  CheckCircle,
  Fingerprint,
  LockKey,
  ShieldCheck,
  WarningCircle,
} from '@phosphor-icons/react'
import { Link } from '@tanstack/react-router'
import { useProjectReleaseQuery } from '@/api/hooks'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { isApiStatus } from '@/lib/api-error'
import type {
  ArtifactRef,
  EvidencePin,
  ProjectRelease,
  ReleaseDecisionReference,
  ReleaseTaskReference,
  ReleaseValidationReference,
} from '@/types/generated'

type CountValue = number | bigint

function count(value: CountValue): number {
  return typeof value === 'bigint' ? Number(value) : value
}

function shortId(value: string | null | undefined): string {
  if (!value) return '—'
  return value.length > 20 ? `${value.slice(0, 10)}…${value.slice(-7)}` : value
}

function formatDate(value: string): string {
  const date = new Date(value)
  return Number.isNaN(date.getTime())
    ? value
    : date.toLocaleString([], { dateStyle: 'medium', timeStyle: 'short' })
}

function humanize(value: string): string {
  return value.replaceAll('_', ' ').replace(/\b\w/g, (letter) => letter.toUpperCase())
}

function DetailCard({
  title,
  eyebrow,
  children,
}: {
  title: string
  eyebrow?: string
  children: ReactNode
}) {
  return (
    <Card className="min-w-0 border-border-subtle bg-card">
      <header className="border-b border-border-subtle px-4 py-3 sm:px-5">
        {eyebrow ? (
          <p className="font-mono text-micro font-semibold uppercase tracking-[0.12em] text-muted-foreground">
            {eyebrow}
          </p>
        ) : null}
        <h2 className="mt-1 break-words text-sm font-semibold text-foreground">{title}</h2>
      </header>
      <div className="min-w-0 p-4 sm:p-5">{children}</div>
    </Card>
  )
}

function DigestRow({ label, value }: { label: string; value: string | null | undefined }) {
  return (
    <div className="grid min-w-0 gap-1 border-b border-border-subtle py-2 last:border-b-0 sm:grid-cols-[10rem_minmax(0,1fr)] sm:gap-3">
      <dt className="text-xs text-muted-foreground">{label}</dt>
      <dd className="min-w-0 break-all font-mono text-micro text-foreground" title={value ?? undefined}>
        {value ?? '—'}
      </dd>
    </div>
  )
}

function ArtifactList({ label, refs }: { label: string; refs: ArtifactRef[] }) {
  return (
    <div>
      <p className="text-xs font-medium text-muted-foreground">{label}</p>
      {refs.length === 0 ? (
        <p className="mt-2 text-xs text-muted-foreground">None recorded.</p>
      ) : (
        <ul className="mt-2 space-y-2">
          {refs.map((ref) => (
            <li
              key={`${ref.artifact_id}:${ref.revision_id}`}
              className="min-w-0 rounded-md border border-border-subtle bg-background px-3 py-2"
            >
              <p className="break-all font-mono text-xs text-foreground">
                {shortId(ref.artifact_id)} · revision {shortId(ref.revision_id)}
              </p>
              <p className="mt-1 break-all font-mono text-micro text-muted-foreground">
                digest {shortId(ref.content_digest)}
                {ref.render_digest ? ` · render ${shortId(ref.render_digest)}` : ''}
              </p>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}

function DecisionList({ decisions }: { decisions: ReleaseDecisionReference[] }) {
  return (
    <div>
      <p className="text-xs font-medium text-muted-foreground">Included decisions</p>
      {decisions.length === 0 ? (
        <p className="mt-2 text-xs text-muted-foreground">No decisions were pinned.</p>
      ) : (
        <ul className="mt-2 space-y-2">
          {decisions.map((decision) => (
            <li key={decision.decision_id} className="min-w-0 rounded-md border border-border-subtle px-3 py-2">
              <div className="flex min-w-0 flex-wrap items-center gap-2">
                <span className="break-all font-mono text-xs text-foreground">
                  {shortId(decision.decision_id)}
                </span>
                <span className="rounded-full border border-border-subtle bg-muted px-2 py-0.5 font-mono text-micro uppercase tracking-[0.08em] text-muted-foreground">
                  {humanize(decision.state)}
                </span>
              </div>
              <p className="mt-1 break-all font-mono text-micro text-muted-foreground">
                digest {shortId(decision.digest)}
              </p>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}

function TaskList({ tasks }: { tasks: ReleaseTaskReference[] }) {
  return (
    <div>
      <p className="text-xs font-medium text-muted-foreground">Included Tasks</p>
      {tasks.length === 0 ? (
        <p className="mt-2 text-xs text-muted-foreground">No Tasks were pinned.</p>
      ) : (
        <ul className="mt-2 space-y-2">
          {tasks.map((task) => (
            <li key={`${task.task_id}:${String(task.task_version)}`} className="min-w-0 rounded-md border border-border-subtle px-3 py-2">
              <Link
                to="/tasks/$taskId"
                params={{ taskId: task.task_id }}
                className="inline-flex min-w-0 max-w-full items-center gap-1 break-all text-xs font-medium text-primary hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
              >
                <span className="break-all">{shortId(task.task_id)}</span>
                <ArrowUpRight size={13} className="shrink-0" aria-hidden />
              </Link>
              <p className="mt-1 break-words text-micro text-muted-foreground">
                {task.task_type} · {humanize(task.task_state)} · version {count(task.task_version)}
              </p>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}

function ValidationList({ validations }: { validations: ReleaseValidationReference[] }) {
  return (
    <div>
      <p className="text-xs font-medium text-muted-foreground">Validation outcomes</p>
      {validations.length === 0 ? (
        <p className="mt-2 text-xs text-muted-foreground">No validation outcomes were pinned.</p>
      ) : (
        <ul className="mt-2 space-y-2">
          {validations.map((validation) => (
            <li key={validation.validation_id} className="min-w-0 rounded-md border border-border-subtle px-3 py-2">
              <p className="break-all font-mono text-xs text-foreground">
                {shortId(validation.validation_id)} · {shortId(validation.result_digest)}
              </p>
              <p className="mt-1 break-words text-micro text-muted-foreground">
                {validation.principal.display_name ?? validation.principal.id} ·{' '}
                {formatDate(validation.evaluated_at)}
              </p>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}

function EvidencePinList({ pins }: { pins: EvidencePin[] }) {
  return (
    <div>
      <p className="text-xs font-medium text-muted-foreground">Pinned evidence</p>
      {pins.length === 0 ? (
        <p className="mt-2 text-xs text-muted-foreground">No evidence pins were recorded.</p>
      ) : (
        <ul className="mt-2 space-y-2">
          {pins.map((pin) => (
            <li key={pin.id} className="min-w-0 rounded-md border border-border-subtle px-3 py-2">
              <div className="flex min-w-0 flex-wrap items-center gap-2">
                <span className="break-all font-mono text-xs text-foreground">
                  {shortId(pin.asset_id)}
                </span>
                <span className="rounded-full border border-border-subtle bg-muted px-2 py-0.5 font-mono text-micro uppercase tracking-[0.08em] text-muted-foreground">
                  {humanize(pin.availability)}
                </span>
                <span className="rounded-full border border-border-subtle bg-muted px-2 py-0.5 font-mono text-micro uppercase tracking-[0.08em] text-muted-foreground">
                  release projection · {humanize(pin.availability_projection)}
                </span>
              </div>
              <p className="mt-1 break-all font-mono text-micro text-muted-foreground">
                pin {shortId(pin.id)} · attachment {shortId(pin.attachment_id)} · attachment digest{' '}
                {shortId(pin.attachment_digest)} · checksum {shortId(pin.asset_checksum)}
              </p>
              <p className="mt-1 break-words text-micro text-muted-foreground">
                {pin.task_media_id ? `task media ${shortId(pin.task_media_id)} · ` : ''}
                pinned {formatDate(pin.pinned_at)}
                {pin.stable_project_url ? ` · stable URL ${pin.stable_project_url}` : ''}
              </p>
              <p className="mt-1 break-words text-xs text-muted-foreground">
                {pin.availability === 'available'
                  ? 'This evidence remains pinned to the immutable release.'
                  : 'This evidence is retained as an immutable availability record.'}
              </p>
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}

function RepositoryReferenceList({ references }: { references: string[] }) {
  return (
    <div>
      <p className="text-xs font-medium text-muted-foreground">Repository references</p>
      {references.length === 0 ? (
        <p className="mt-2 text-xs text-muted-foreground">No repository references were pinned.</p>
      ) : (
        <ul className="mt-2 space-y-2">
          {references.map((reference) => (
            <li
              key={reference}
              className="min-w-0 break-all rounded-md border border-border-subtle bg-background px-3 py-2 font-mono text-micro text-foreground"
            >
              {reference}
            </li>
          ))}
        </ul>
      )}
    </div>
  )
}

function LoadingState() {
  return (
    <div className="mx-auto flex w-full max-w-[1100px] min-w-0 flex-col gap-5" role="status" aria-busy="true">
      <div className="h-3 w-28 animate-pulse rounded bg-muted" />
      <div className="h-8 w-2/3 max-w-xl animate-pulse rounded bg-muted" />
      <div className="h-4 w-full max-w-2xl animate-pulse rounded bg-muted" />
      <div className="grid gap-5 lg:grid-cols-[minmax(0,1.35fr)_minmax(18rem,0.65fr)]">
        <div className="h-72 animate-pulse rounded-lg border border-border-subtle bg-muted" />
        <div className="h-56 animate-pulse rounded-lg border border-border-subtle bg-muted" />
      </div>
      Loading release snapshot…
    </div>
  )
}

function ErrorState({
  error,
  projectId,
  onRetry,
}: {
  error: unknown
  projectId: string
  onRetry: () => void
}) {
  const denied = isApiStatus(error, 403)
  return (
    <div className="mx-auto flex w-full max-w-2xl flex-col items-center gap-4 py-16 text-center">
      {denied ? (
        <LockKey size={24} className="text-muted-foreground" aria-hidden />
      ) : (
        <WarningCircle size={24} className="text-destructive" aria-hidden />
      )}
      <div>
        <h1 className="text-lg font-semibold text-foreground">
          {denied ? 'Release snapshot access denied' : 'Release snapshot unavailable'}
        </h1>
        <p className="mt-2 text-sm leading-6 text-muted-foreground">
          {denied
            ? 'This account is not authorized to inspect this immutable release.'
            : 'Forge could not load the immutable release snapshot. Try again before treating the history entry as current.'}
        </p>
      </div>
      <div className="flex flex-wrap justify-center gap-2">
        {!denied ? (
          <Button type="button" onClick={onRetry}>
            <ArrowUpRight size={15} aria-hidden /> Retry
          </Button>
        ) : null}
        <Link to="/projects/$projectId/overview" params={{ projectId }}>
          <Button variant="outline">
            <ArrowLeft size={15} aria-hidden /> Project Overview
          </Button>
        </Link>
      </div>
    </div>
  )
}

function ReleaseHeader({ release }: { release: ProjectRelease }) {
  const snapshot = release.snapshot
  return (
    <header className="min-w-0">
      <Link
        to="/projects/$projectId/overview"
        params={{ projectId: release.project_id }}
        className="inline-flex items-center gap-1 text-xs font-medium text-primary hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        <ArrowLeft size={13} aria-hidden /> Project Overview
      </Link>
      <div className="mt-4 flex min-w-0 flex-wrap items-start justify-between gap-4">
        <div className="min-w-0">
          <p className="font-mono text-micro font-semibold uppercase tracking-[0.14em] text-muted-foreground">
            Immutable release snapshot
          </p>
          <h1 className="mt-2 break-words text-2xl font-semibold tracking-tight text-foreground sm:text-3xl">
            {snapshot.release_identity}
          </h1>
          <p className="mt-2 break-words text-sm leading-6 text-muted-foreground">
            {snapshot.display_label ?? 'Released Project outcome'} · {formatDate(snapshot.released_at)}
          </p>
        </div>
        <div className="flex shrink-0 items-center gap-2 rounded-md border border-success/30 bg-success/10 px-3 py-2 text-xs font-medium text-success">
          <CheckCircle size={15} aria-hidden /> Released · immutable
        </div>
      </div>
    </header>
  )
}

export function ProjectReleasePage({ projectId, releaseId }: { projectId: string; releaseId: string }) {
  const releaseQuery = useProjectReleaseQuery(projectId, releaseId)

  if (releaseQuery.isLoading) return <LoadingState />
  if (releaseQuery.isError || !releaseQuery.data) {
    return (
      <ErrorState
        error={releaseQuery.error}
        projectId={projectId}
        onRetry={() => void releaseQuery.refetch()}
      />
    )
  }

  const release = releaseQuery.data
  const snapshot = release.snapshot

  return (
    <div className="mx-auto flex w-full max-w-[1100px] min-w-0 flex-col gap-5">
      <ReleaseHeader release={release} />

      <div className="grid min-w-0 gap-5 lg:grid-cols-[minmax(0,1.35fr)_minmax(18rem,0.65fr)]">
        <section className="min-w-0 space-y-5" aria-label="Release snapshot details">
          <DetailCard title="Released outcome" eyebrow="Frozen at release time">
            <p className="break-words text-sm leading-6 text-foreground">{snapshot.summary}</p>
            <dl className="mt-4 divide-y divide-border-subtle rounded-md border border-border-subtle px-3">
              <DigestRow label="Release identity" value={snapshot.release_identity} />
              <DigestRow label="Milestone" value={snapshot.milestone_canonical_id} />
              <DigestRow
                label="Milestone definition revision"
                value={snapshot.milestone_definition_revision_id}
              />
              <DigestRow label="Milestone definition digest" value={snapshot.milestone_definition_digest} />
              <DigestRow
                label="Milestone version at release"
                value={String(count(snapshot.expected_milestone_version))}
              />
              <DigestRow label="Snapshot digest" value={snapshot.snapshot_digest} />
              <DigestRow label="Readiness digest" value={snapshot.readiness_digest} />
              <DigestRow label="Source watermark" value={snapshot.source_event_watermark} />
              <DigestRow label="Schema version" value={snapshot.schema_version} />
              <DigestRow label="Readiness snapshot" value={snapshot.readiness_snapshot_id} />
            </dl>
            {snapshot.changelog.length > 0 ? (
              <div className="mt-4">
                <p className="text-xs font-medium text-muted-foreground">Change summary</p>
                <ul className="mt-2 list-disc space-y-1 pl-5 text-xs leading-5 text-foreground">
                  {snapshot.changelog.map((entry) => (
                    <li key={entry} className="break-words">
                      {entry}
                    </li>
                  ))}
                </ul>
              </div>
            ) : null}
            {snapshot.known_issues.length > 0 ? (
              <div className="mt-4 rounded-md border border-warning/40 bg-warning/10 p-3" role="status">
                <p className="text-xs font-semibold text-foreground">Known issues at release</p>
                <ul className="mt-1 list-disc space-y-1 pl-5 text-xs leading-5 text-muted-foreground">
                  {snapshot.known_issues.map((issue) => (
                    <li key={issue} className="break-words">
                      {issue}
                    </li>
                  ))}
                </ul>
              </div>
            ) : null}
          </DetailCard>

          <DetailCard title="Frozen project references" eyebrow="Canonical provenance">
            <div className="space-y-5">
              <ArtifactList label="Approved Charter" refs={[snapshot.charter_revision]} />
              <ArtifactList label="Approved Project Documents" refs={snapshot.document_revisions} />
              <DecisionList decisions={snapshot.included_decisions} />
              <TaskList tasks={snapshot.included_tasks} />
              <ValidationList validations={snapshot.validation_results} />
              <RepositoryReferenceList references={snapshot.repository_references} />
            </div>
          </DetailCard>

          <DetailCard title="Evidence retained with this release" eyebrow="Pinned proof metadata">
            <EvidencePinList pins={snapshot.evidence_pins} />
          </DetailCard>
        </section>

        <aside className="min-w-0 space-y-5" aria-label="Release provenance">
          <DetailCard title="Released by" eyebrow="Principal-bound action">
            <div className="flex min-w-0 items-start gap-3">
              <ShieldCheck size={20} className="mt-0.5 shrink-0 text-success" aria-hidden />
              <div className="min-w-0">
                <p className="break-words text-sm font-medium text-foreground">
                  {snapshot.released_by.display_name ?? snapshot.released_by.id}
                </p>
                <p className="mt-1 break-all font-mono text-micro text-muted-foreground">
                  {snapshot.released_by.id}
                </p>
                <p className="mt-2 text-xs text-muted-foreground">{formatDate(snapshot.released_at)}</p>
                <dl className="mt-3 divide-y divide-border-subtle rounded-md border border-border-subtle px-3">
                  <DigestRow label="Authorization event" value={snapshot.authorization.event_id} />
                  <DigestRow
                    label="Authorization basis"
                    value={snapshot.authorization.authorization_basis}
                  />
                </dl>
              </div>
            </div>
          </DetailCard>

          <DetailCard title="Immutable identifiers" eyebrow="Inspect without mutation">
            <dl className="divide-y divide-border-subtle rounded-md border border-border-subtle px-3">
              <DigestRow label="Release record" value={release.id} />
              <DigestRow label="Project" value={release.project_id} />
              <DigestRow label="Milestone" value={release.milestone_id} />
              <DigestRow label="Release revision" value={String(count(snapshot.release_revision))} />
              <DigestRow label="Idempotency key" value={snapshot.idempotency_key} />
            </dl>
            <p className="mt-3 flex items-start gap-2 text-xs leading-5 text-muted-foreground">
              <Fingerprint size={15} className="mt-0.5 shrink-0" aria-hidden />
              This view is fetched from the authenticated Project release resource. It does not
              expose a public or unauthenticated API document.
            </p>
          </DetailCard>

          <DetailCard title="Release boundary" eyebrow="Forge snapshot semantics">
            <p className="text-xs leading-5 text-muted-foreground">
              This snapshot records the exact Charter, Documents, Tasks, validation, repository
              references, baseline, source watermark, evidence projections, waivers, and known
              issues at release time. Later Project changes update live Overview projections only;
              they do not rewrite this history.
            </p>
            {snapshot.waived_check_ids.length > 0 ? (
              <div className="mt-3 rounded-md border border-warning/40 bg-warning/10 p-3">
                <p className="text-xs font-semibold text-foreground">Waived checks</p>
                <p className="mt-1 break-all font-mono text-micro text-muted-foreground">
                  {snapshot.waived_check_ids.map(shortId).join(' · ')}
                </p>
              </div>
            ) : (
              <p className="mt-3 flex items-start gap-2 text-xs text-success">
                <CheckCircle size={14} className="mt-0.5 shrink-0" aria-hidden /> No waived checks
                recorded.
              </p>
            )}
          </DetailCard>
        </aside>
      </div>
    </div>
  )
}
