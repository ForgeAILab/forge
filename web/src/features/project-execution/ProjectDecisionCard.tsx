import { Scales } from '@phosphor-icons/react'

import { useProjectOverviewQuery } from '@/api/hooks'
import { cn } from '@/lib/cn'
import type { ProjectOverview } from '@/types/generated'

import { DecisionCandidateCard } from './DecisionCandidateCard'
import { MilestoneRevisionApprovalControl } from './MilestoneRevisionApprovalControl'

function numberValue(value: number | bigint | null | undefined): number | null {
  if (value === null || value === undefined) return null
  const number = typeof value === 'bigint' ? Number(value) : value
  return Number.isFinite(number) ? number : null
}

/** The milestone whose definition revision `next_action` asks the user to approve. */
function milestoneApprovalTarget(overview: ProjectOverview) {
  const action = overview.next_action
  if (
    !action ||
    action.code !== 'milestone_definition_approval' ||
    action.required_principal !== 'user'
  ) {
    return null
  }
  const target = overview.active_milestones.find((entry) => entry.definition.id === action.target_id)
  return target ? { action, target } : null
}

/**
 * Everything the Project Agent is waiting on the user to decide, with the
 * decision itself one click away: the current milestone definition revision
 * when it needs approval, and any pending Decision proposals. Renders nothing
 * when there is nothing to decide. Deciding here records the decision and
 * wakes the Project Agent, so the user never has to repeat it as a message.
 */
export function ProjectDecisionCard({
  projectId,
  className,
}: {
  projectId: string
  className?: string
}) {
  const overviewQuery = useProjectOverviewQuery(projectId)
  const overview = overviewQuery.data
  if (!overview) return null
  const approval = milestoneApprovalTarget(overview)
  const pending = overview.pending_decisions
  if (!approval && pending.length === 0) return null

  return (
    <section
      aria-label="Needs your decision"
      className={cn('rounded-lg border border-ember-border bg-ember-surface p-3', className)}
    >
      <p className="inline-flex items-center gap-1.5 text-micro font-semibold uppercase tracking-[0.08em] text-muted-foreground">
        <Scales size={13} aria-hidden />
        Needs your decision
      </p>
      {approval ? (
        <div className="mt-2">
          <p className="break-words text-sm font-medium text-foreground">{approval.action.title}</p>
          <p className="mt-1 break-words text-xs leading-5 text-muted-foreground">
            {approval.action.explanation}
          </p>
          <MilestoneRevisionApprovalControl
            projectId={projectId}
            target={approval.target}
            expectedVersion={numberValue(approval.action.expected_version)}
          />
        </div>
      ) : null}
      {pending.length > 0 ? (
        <div className={cn(approval ? 'mt-3 border-t border-border-subtle pt-3' : 'mt-2')}>
          <DecisionCandidateCard projectId={projectId} candidates={pending} />
        </div>
      ) : null}
    </section>
  )
}
