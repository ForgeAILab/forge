import { useState } from 'react'
import { CheckCircle } from '@phosphor-icons/react'

import { useApproveMilestoneRevision } from '@/api/hooks'
import { Button } from '@/components/ui/button'
import { getApiErrorMessage } from '@/lib/api-error'
import type { ProjectMilestoneOverview } from '@/types/generated'

import { createUserAuthorization, newIdempotencyKey } from './user-authorization'

function numberValue(value: number | bigint | null | undefined): number | null {
  if (value === null || value === undefined) return null
  const number = typeof value === 'bigint' ? Number(value) : value
  return Number.isFinite(number) ? number : null
}

function humanize(value: string | null | undefined): string {
  if (!value) return 'Unknown'
  return value.replaceAll('_', ' ').replace(/\b\w/g, (letter) => letter.toUpperCase())
}

/** "6 acceptance checks · 5 task validation, 1 user attested" */
export function describeAcceptanceChecks(target: ProjectMilestoneOverview): string | null {
  const checks = target.definition.content.acceptance_checks
  if (checks.length === 0) return null
  const bySource = new Map<string, number>()
  for (const check of checks) {
    bySource.set(check.source_kind, (bySource.get(check.source_kind) ?? 0) + 1)
  }
  const breakdown = [...bySource.entries()]
    .map(([kind, count]) => `${count} ${humanize(kind).toLowerCase()}`)
    .join(', ')
  const noun = checks.length === 1 ? 'acceptance check' : 'acceptance checks'
  return bySource.size > 1 ? `${checks.length} ${noun} · ${breakdown}` : `${checks.length} ${noun}`
}

/**
 * The one user action behind `milestone_definition_approval`: approve the
 * milestone's current `draft`/`proposed` definition revision in place. Until
 * it is approved every downstream action is measured against an unapproved
 * contract. Approval is a recorded user decision, so it also wakes the
 * Project Agent to continue from it — the user never has to say "approve"
 * in the chat as well.
 */
export function MilestoneRevisionApprovalControl({
  projectId,
  target,
  expectedVersion,
}: {
  projectId: string
  target: ProjectMilestoneOverview
  expectedVersion: number | null
}) {
  const approval = useApproveMilestoneRevision()
  const [error, setError] = useState<string | null>(null)
  const revisionNumber = numberValue(target.definition.revision_number)
  const milestoneVersion = expectedVersion ?? numberValue(target.milestone.version)
  const checks = describeAcceptanceChecks(target)

  async function approve() {
    setError(null)
    approval.reset?.()
    if (milestoneVersion === null) {
      setError('The milestone version is unavailable; refresh and try again.')
      return
    }
    try {
      await approval.mutateAsync({
        projectId,
        milestoneId: target.milestone.id,
        revisionId: target.definition.id,
        expectedMilestoneVersion: milestoneVersion,
        idempotencyKey: newIdempotencyKey('milestone-revision-approve'),
        authorization: createUserAuthorization(
          'project.milestone.revision.transition',
          'interactive_user_approval',
        ),
      })
    } catch (caught) {
      setError(
        getApiErrorMessage(
          caught,
          'The definition revision could not be approved. Refresh and try again.',
        ),
      )
    }
  }

  return (
    <div className="mt-3 flex flex-col gap-2">
      <p className="text-xs leading-5 text-muted-foreground">
        {target.milestone.display_label ?? target.milestone.canonical_id}: definition revision{' '}
        {revisionNumber ?? '—'} is {humanize(target.definition.lifecycle)}.
      </p>
      {checks ? <p className="text-xs leading-5 text-muted-foreground">{checks}.</p> : null}
      {approval.isSuccess ? (
        <p className="inline-flex items-center gap-1.5 text-xs font-medium text-success" role="status">
          <CheckCircle size={14} aria-hidden />
          Approved. The Project Agent has been woken to continue from this decision.
        </p>
      ) : (
        <div>
          <Button size="sm" disabled={approval.isPending} onClick={() => void approve()}>
            {approval.isPending
              ? 'Approving…'
              : `Approve definition revision ${revisionNumber ?? ''}`.trim()}
          </Button>
        </div>
      )}
      {error ? (
        <p role="alert" className="break-words text-xs leading-5 text-destructive">
          {error}
        </p>
      ) : null}
    </div>
  )
}
