import type { AcceptanceEvidenceRequirement } from '@/types/generated/bindings/AcceptanceEvidenceRequirement'
import type { AdaptiveEnvelope } from '@/types/generated/bindings/AdaptiveEnvelope'
import type { ExecutionBaselineContent } from '@/types/generated/bindings/ExecutionBaselineContent'
import { cn } from '@/lib/cn'

const ADAPTIVE_OPERATION_LABELS: Record<string, string> = {
  split: 'Split a Task into smaller Tasks',
  sequence: 'Reorder/insert Tasks in sequence',
  replace: 'Replace a Task with a revised one',
}

type DiffStatus = 'added' | 'removed' | 'unchanged'

interface DiffEntry {
  key: string
  label: string
  status: DiffStatus
}

/**
 * Diff two string lists by exact value. Union in a stable order: every
 * current entry first (in its current order, marked `added` when it is new),
 * then any previous-only entry, marked `removed`. `undefined` previous means
 * "no prior revision to diff against" -- everything renders `unchanged`.
 */
function diffStringList(current: string[], previous: string[] | undefined): DiffEntry[] {
  if (!previous) return current.map((value) => ({ key: value, label: value, status: 'unchanged' }))
  const previousSet = new Set(previous)
  const currentSet = new Set(current)
  const entries: DiffEntry[] = current.map((value) => ({
    key: value,
    label: value,
    status: previousSet.has(value) ? 'unchanged' : 'added',
  }))
  for (const value of previous) {
    if (!currentSet.has(value)) entries.push({ key: `removed:${value}`, label: value, status: 'removed' })
  }
  return entries
}

function DiffBadge({ status }: { status: DiffStatus }) {
  if (status === 'unchanged') return null
  const isAdded = status === 'added'
  return (
    <span
      className={cn(
        'mr-1.5 inline-flex shrink-0 items-center rounded-full border px-1.5 py-0 text-[0.6rem] font-semibold uppercase tracking-[0.04em]',
        isAdded
          ? 'border-success/40 bg-success/10 text-success'
          : 'border-destructive/40 bg-destructive/10 text-destructive line-through decoration-1',
      )}
    >
      <span aria-hidden>{isAdded ? '+' : '−'}</span>
      <span className="sr-only">{isAdded ? 'Added' : 'Removed'}: </span>
    </span>
  )
}

function DiffListSection({
  title,
  entries,
  emptyLabel,
  mono = false,
}: {
  title: string
  entries: DiffEntry[]
  emptyLabel: string
  mono?: boolean
}) {
  return (
    <section aria-labelledby={`baseline-section-${slug(title)}`} className="min-w-0">
      <h3
        id={`baseline-section-${slug(title)}`}
        className="text-xs font-semibold uppercase tracking-[0.06em] text-muted-foreground"
      >
        {title}
      </h3>
      {entries.length === 0 ? (
        <p className="mt-1.5 text-xs italic text-muted-foreground">{emptyLabel}</p>
      ) : (
        <ul className="mt-1.5 flex flex-col gap-1">
          {entries.map((entry) => (
            <li
              key={entry.key}
              className={cn(
                'flex min-w-0 items-start gap-1 rounded-md border border-transparent px-2 py-1 text-xs leading-5',
                entry.status === 'added' && 'border-success/20 bg-success/5',
                entry.status === 'removed' && 'border-destructive/15 bg-destructive/5',
              )}
            >
              <DiffBadge status={entry.status} />
              <span
                className={cn(
                  'min-w-0 break-words text-foreground',
                  mono && 'font-mono',
                  entry.status === 'removed' && 'text-muted-foreground line-through',
                )}
              >
                {entry.label}
              </span>
            </li>
          ))}
        </ul>
      )}
    </section>
  )
}

function slug(value: string): string {
  return value.toLowerCase().replace(/[^a-z0-9]+/g, '-').replace(/(^-|-$)/g, '')
}

function evidenceDiff(
  current: AcceptanceEvidenceRequirement[],
  previous: AcceptanceEvidenceRequirement[] | undefined,
): { item: AcceptanceEvidenceRequirement; status: DiffStatus }[] {
  const previousById = new Map((previous ?? []).map((item) => [item.id, item]))
  const currentIds = new Set(current.map((item) => item.id))
  const entries: { item: AcceptanceEvidenceRequirement; status: DiffStatus }[] = current.map((item) => {
    const before = previousById.get(item.id)
    const status: DiffStatus =
      !previous || !before ? (previous ? 'added' : 'unchanged') : jsonEqual(before, item) ? 'unchanged' : 'added'
    return { item, status }
  })
  if (previous) {
    for (const item of previous) {
      if (!currentIds.has(item.id)) entries.push({ item, status: 'removed' })
    }
  }
  return entries
}

function jsonEqual(a: unknown, b: unknown): boolean {
  return JSON.stringify(a) === JSON.stringify(b)
}

function EvidenceSection({
  current,
  previous,
}: {
  current: AcceptanceEvidenceRequirement[]
  previous: AcceptanceEvidenceRequirement[] | undefined
}) {
  const entries = evidenceDiff(current, previous)
  return (
    <section aria-labelledby="baseline-section-evidence" className="min-w-0">
      <h3
        id="baseline-section-evidence"
        className="text-xs font-semibold uppercase tracking-[0.06em] text-muted-foreground"
      >
        Milestones &amp; acceptance checks
      </h3>
      {entries.length === 0 ? (
        <p className="mt-1.5 text-xs italic text-muted-foreground">
          No acceptance evidence requirements recorded.
        </p>
      ) : (
        <ul className="mt-1.5 flex flex-col gap-1.5">
          {entries.map(({ item, status }) => (
            <li
              key={item.id}
              className={cn(
                'rounded-md border border-border-subtle px-2 py-1.5 text-xs leading-5',
                status === 'added' && 'border-success/20 bg-success/5',
                status === 'removed' && 'border-destructive/15 bg-destructive/5',
              )}
            >
              <div className="flex min-w-0 items-start gap-1">
                <DiffBadge status={status} />
                <span
                  className={cn(
                    'min-w-0 break-words font-medium text-foreground',
                    status === 'removed' && 'text-muted-foreground line-through',
                  )}
                >
                  {item.description}
                </span>
              </div>
              <p className="mt-0.5 pl-0 text-micro text-muted-foreground">
                {item.required ? 'Required' : 'Optional'}
                {item.evidence_kind ? ` · ${item.evidence_kind}` : ''}
              </p>
            </li>
          ))}
        </ul>
      )}
    </section>
  )
}

function AdaptiveAuthoritySection({
  current,
  previous,
}: {
  current: AdaptiveEnvelope
  previous: AdaptiveEnvelope | undefined
}) {
  const operationEntries = diffStringList(current.allowed_task_operations, previous?.allowed_task_operations)
  return (
    <section aria-labelledby="baseline-section-adaptive" className="min-w-0">
      <h3
        id="baseline-section-adaptive"
        className="text-xs font-semibold uppercase tracking-[0.06em] text-muted-foreground"
      >
        Adaptive authority
      </h3>
      <p className="mt-1 text-xs leading-5 text-muted-foreground">
        Operations the Project Agent may perform on covered Tasks without asking again.
      </p>
      {operationEntries.length === 0 ? (
        <p className="mt-1.5 text-xs italic text-muted-foreground">
          No adaptive operations are granted -- the plan is fixed.
        </p>
      ) : (
        <ul className="mt-1.5 flex flex-wrap gap-1.5">
          {operationEntries.map((entry) => (
            <li
              key={entry.key}
              className={cn(
                'inline-flex items-center rounded-full border px-2 py-0.5 text-xs leading-5 text-foreground',
                entry.status === 'added' && 'border-success/40 bg-success/10',
                entry.status === 'removed' && 'border-destructive/30 bg-destructive/5 line-through',
                entry.status === 'unchanged' && 'border-border-subtle bg-muted/20',
              )}
            >
              <DiffBadge status={entry.status} />
              {ADAPTIVE_OPERATION_LABELS[entry.label] ?? entry.label}
            </li>
          ))}
        </ul>
      )}
      {current.forbidden_side_effects.length > 0 ? (
        <div className="mt-2">
          <DiffListSection
            title="Forbidden side effects"
            entries={diffStringList(current.forbidden_side_effects, previous?.forbidden_side_effects)}
            emptyLabel="None recorded."
          />
        </div>
      ) : null}
    </section>
  )
}

/**
 * Semantic Review view for one execution-baseline revision (design D19/F19).
 *
 * Replaces the raw preformatted `rendered_view` JSON blob with named
 * sections and, when a prior revision is available, an exact per-field
 * diff. This is presentation only: it renders the same exact approved
 * `content` the frozen `rendered_view`/digest already cover, and never
 * recomputes or overrides them.
 */
export function BaselineReviewSections({
  content,
  previousContent,
  renderedView,
  contentDigest,
}: {
  content: ExecutionBaselineContent
  previousContent?: ExecutionBaselineContent | null
  renderedView: string
  contentDigest: string
}) {
  const previous = previousContent ?? undefined
  return (
    <div className="flex min-w-0 flex-col gap-4">
      {previous ? (
        <p className="rounded-md border border-border-subtle bg-muted/20 px-2.5 py-1.5 text-micro text-muted-foreground">
          Comparing against the currently active revision. <span aria-hidden>+</span>
          <span className="sr-only">Added</span> is new in this revision;{' '}
          <span aria-hidden>{'−'}</span>
          <span className="sr-only">Removed</span> was dropped.
        </p>
      ) : null}
      <DiffListSection
        title="Intended outcomes"
        entries={diffStringList(content.adaptive_envelope.fixed_outcomes, previous?.adaptive_envelope.fixed_outcomes)}
        emptyLabel="No fixed outcomes recorded."
      />
      <DiffListSection
        title="Plan items"
        entries={diffStringList(content.plan_item_ids, previous?.plan_item_ids)}
        emptyLabel="No plan items recorded."
        mono
      />
      <DiffListSection
        title="Milestones"
        entries={diffStringList(content.milestone_ids, previous?.milestone_ids)}
        emptyLabel="No milestones recorded."
        mono
      />
      <EvidenceSection current={content.acceptance_evidence_matrix} previous={previous?.acceptance_evidence_matrix} />
      <AdaptiveAuthoritySection current={content.adaptive_envelope} previous={previous?.adaptive_envelope} />
      <DiffListSection
        title="Risks"
        entries={diffStringList(content.risk_classes, previous?.risk_classes)}
        emptyLabel="No named risk classes."
      />
      <DiffListSection
        title="Elevated actions"
        entries={diffStringList(content.elevated_operations, previous?.elevated_operations)}
        emptyLabel="No elevated actions."
      />
      <DiffListSection
        title="Exclusions"
        entries={diffStringList(content.exclusions, previous?.exclusions)}
        emptyLabel="Nothing explicitly excluded."
      />
      <DiffListSection
        title="Rollback & recovery"
        entries={diffStringList(content.rollback_and_recovery, previous?.rollback_and_recovery)}
        emptyLabel="No rollback steps recorded."
      />
      <details className="rounded-md border border-border-subtle">
        <summary className="cursor-pointer select-none rounded-md px-2.5 py-1.5 text-xs font-medium text-muted-foreground hover:bg-muted/30 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">
          Technical details (raw JSON)
        </summary>
        <div className="max-h-[40vh] overflow-auto border-t border-border-subtle bg-muted/20 p-3">
          <pre className="whitespace-pre-wrap font-mono text-xs leading-5 text-foreground">
            {renderedView}
          </pre>
        </div>
        <p className="border-t border-border-subtle px-2.5 py-1.5 font-mono text-micro uppercase tracking-[0.08em] text-muted-foreground">
          content digest {contentDigest.slice(0, 16)}…
        </p>
      </details>
    </div>
  )
}
