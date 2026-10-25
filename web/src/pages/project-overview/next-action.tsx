// Project Overview next action card.

import { Link } from '@tanstack/react-router'
import { ArrowUpRight, Clock } from '@phosphor-icons/react'
import { MilestoneRevisionApprovalControl } from '@/features/project-execution/MilestoneRevisionApprovalControl'
import type { ProjectMilestoneOverview, ProjectNextAction } from '@/types/generated'
import { shortId, numberValue } from './utils'
import { SectionCard } from './shared'

export function NextActionCard({
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
