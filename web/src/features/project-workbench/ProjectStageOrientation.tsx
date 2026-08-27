import { Link } from '@tanstack/react-router'
import { Check, WarningCircle } from '@phosphor-icons/react'

import { cn } from '@/lib/cn'
import type { ExecutionBlockerProjection } from '@/types/generated/bindings/ExecutionBlockerProjection'
import type { ProjectOverview } from '@/types/generated/bindings/ProjectOverview'

export type ProjectWorkbenchStage = 'define' | 'plan' | 'build' | 'release'
type StageStatus = 'complete' | 'active' | 'blocked' | 'pending'

interface StageView {
  key: ProjectWorkbenchStage
  label: string
  status: StageStatus
  detail: string
  blocker: ExecutionBlockerProjection | null
}

const STAGE_LABELS: Record<ProjectWorkbenchStage, string> = {
  define: 'Define',
  plan: 'Plan',
  build: 'Build',
  release: 'Release',
}

function count(value: bigint | number): number {
  return typeof value === 'bigint' ? Number(value) : value
}

/**
 * Derive the Define -> Plan -> Build -> Release orientation entirely from
 * already-canonical server fields on `ProjectOverview` (design D17/8.5.1).
 * This performs no independent judgment -- it only reorganizes facts the
 * server already decided (Charter state, the execution gate, Task/check
 * counts, and Releases) into four named stops, plus the one canonical
 * `ExecutionBlockerProjection` when a blocker exists. It is navigation and
 * explanation only: it never computes a second workflow state, and a
 * blocker's `stage` (design D17's own closed vocabulary, reused verbatim
 * here -- `review` is shown scoped inside `build`) is always the server's
 * word, never re-derived.
 */
export function deriveProjectStageOrientation(overview: ProjectOverview): StageView[] {
  const executionSetup = overview.execution_setup ?? null
  const blocker = executionSetup?.execution_blocker ?? null
  const blockerStage: ProjectWorkbenchStage | null = blocker
    ? blocker.stage === 'review'
      ? 'build'
      : blocker.stage === 'define' ||
          blocker.stage === 'plan' ||
          blocker.stage === 'build' ||
          blocker.stage === 'release'
        ? blocker.stage
        : null
    : null

  const defineComplete = overview.charter_state !== 'charter_setup_required'
  const gate = executionSetup?.execution_gate ?? null
  const planActive = gate === 'pre_baseline_read_only' || gate === 'baseline_approval_required'
  const buildReached = gate === 'active' || gate === 'reconciliation_required'
  const planComplete = buildReached || overview.releases.length > 0

  const taskCounts = overview.task_counts
  const checks = overview.check_summary
  const buildStarted = buildReached && count(taskCounts.total) > 0
  const buildComplete =
    buildStarted &&
    count(taskCounts.terminal) === count(taskCounts.total) &&
    count(checks.failed) === 0 &&
    count(checks.missing) === 0
  const released = overview.releases.length > 0
  const latestRelease = overview.releases.at(-1) ?? null

  function status(computed: StageStatus, key: ProjectWorkbenchStage): StageStatus {
    return blockerStage === key ? 'blocked' : computed
  }

  const stages: StageView[] = [
    {
      key: 'define',
      label: STAGE_LABELS.define,
      status: status(defineComplete ? 'complete' : 'active', 'define'),
      detail: defineComplete
        ? overview.current_charter
          ? `Charter approved · revision ${count(overview.current_charter.revision_number)}`
          : 'Charter approved'
        : 'Charter setup required',
      blocker: blockerStage === 'define' ? blocker : null,
    },
    {
      key: 'plan',
      label: STAGE_LABELS.plan,
      status: status(planComplete ? 'complete' : planActive ? 'active' : 'pending', 'plan'),
      detail: planComplete
        ? 'Optional traceability · Tasks follow their workflow'
        : planActive
          ? 'Legacy traceability state · does not block Tasks'
          : 'Not started',
      blocker: blockerStage === 'plan' ? blocker : null,
    },
    {
      key: 'build',
      label: STAGE_LABELS.build,
      status: status(
        buildComplete ? 'complete' : buildStarted ? 'active' : buildReached ? 'active' : 'pending',
        'build',
      ),
      detail: buildStarted
        ? `${count(taskCounts.active)} active · ${count(taskCounts.review)} in review · ${count(taskCounts.terminal)}/${count(taskCounts.total)} done`
        : buildReached
          ? 'Ready to start Tasks'
          : 'Not started',
      blocker: blockerStage === 'build' ? blocker : null,
    },
    {
      key: 'release',
      label: STAGE_LABELS.release,
      status: status(released ? 'complete' : 'pending', 'release'),
      detail: released && latestRelease ? `Released · ${latestRelease.release_identity}` : 'Not released yet',
      blocker: blockerStage === 'release' ? blocker : null,
    },
  ]
  return stages
}

function StageIcon({ status }: { status: StageStatus }) {
  if (status === 'complete') return <Check size={12} weight="bold" aria-hidden />
  if (status === 'blocked') return <WarningCircle size={12} weight="fill" aria-hidden />
  return null
}

function stageDotClass(status: StageStatus): string {
  switch (status) {
    case 'complete':
      return 'border-success bg-success/15 text-success'
    case 'active':
      return 'border-primary bg-primary/10 text-primary'
    case 'blocked':
      return 'border-destructive bg-destructive/10 text-destructive'
    default:
      return 'border-border-subtle bg-muted/20 text-muted-foreground'
  }
}

/**
 * Persistent, server-derived Define -> Plan -> Build -> Release orientation
 * for the Project workbench (design D17/8.5.1, live-acceptance addendum).
 * Navigation and explanation only: it never becomes a second lifecycle,
 * workflow, or client truth store, and scoped reconciliation shows up
 * exactly at the stage the canonical `ExecutionBlockerProjection` names.
 */
export function ProjectStageOrientation({ overview }: { overview: ProjectOverview }) {
  const stages = deriveProjectStageOrientation(overview)
  const blocked = stages.find((stage) => stage.blocker)

  return (
    <nav
      aria-label="Project stage"
      className="min-w-0 rounded-xl border border-border-subtle bg-card px-3 py-3 sm:px-4"
    >
      <ol className="flex min-w-0 flex-wrap items-stretch gap-2 sm:flex-nowrap sm:gap-0">
        {stages.map((stage, index) => (
          <li key={stage.key} className="flex min-w-0 flex-1 items-center gap-2">
            <div className="flex min-w-0 flex-1 items-center gap-2">
              <span
                className={cn(
                  'flex h-6 w-6 shrink-0 items-center justify-center rounded-full border text-micro font-semibold',
                  stageDotClass(stage.status),
                )}
                aria-hidden
              >
                <StageIcon status={stage.status} />
                {stage.status === 'active' || stage.status === 'pending' ? index + 1 : null}
              </span>
              <div className="min-w-0">
                <p
                  className={cn(
                    'text-xs font-semibold',
                    stage.status === 'pending' ? 'text-muted-foreground' : 'text-foreground',
                  )}
                >
                  {stage.label}
                  <span className="sr-only">
                    {' '}
                    —{' '}
                    {stage.status === 'complete'
                      ? 'complete'
                      : stage.status === 'blocked'
                        ? 'blocked'
                        : stage.status === 'active'
                          ? 'in progress'
                          : 'not started'}
                  </span>
                </p>
                <p className="truncate text-micro text-muted-foreground">{stage.detail}</p>
              </div>
            </div>
            {index < stages.length - 1 ? (
              <span
                className="hidden h-px w-6 shrink-0 bg-border-subtle sm:block motion-reduce:transition-none"
                aria-hidden
              />
            ) : null}
          </li>
        ))}
      </ol>
      {blocked?.blocker ? (
        <div
          className="mt-3 flex min-w-0 items-start gap-2 rounded-md border border-destructive/30 bg-destructive/5 px-2.5 py-2"
          role="status"
        >
          <WarningCircle size={15} className="mt-0.5 shrink-0 text-destructive" aria-hidden />
          <div className="min-w-0">
            <p className="text-xs font-semibold text-foreground">
              {STAGE_LABELS[blocked.key]}: {blocked.blocker.headline}
            </p>
            <p className="mt-0.5 break-words text-xs leading-5 text-muted-foreground">
              {blocked.blocker.safe_explanation}
            </p>
            <Link
              to="/projects/$projectId/board"
              params={{ projectId: overview.project_id }}
              className="mt-1 inline-flex items-center text-xs font-medium text-primary underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              Review and resolve
            </Link>
          </div>
        </div>
      ) : null}
    </nav>
  )
}
