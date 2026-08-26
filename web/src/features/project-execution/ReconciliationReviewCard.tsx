import { useState } from 'react'
import { WarningCircle } from '@phosphor-icons/react'

import { ConflictDetails } from '@/components/conflict-details'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Label } from '@/components/ui/label'
import { getApiErrorMessage } from '@/lib/api-error'
import { useAuthStore } from '@/stores/auth'
import type {
  AuthorizationProvenance,
  ProjectReconciliation,
  ReconciliationReplacementRef,
  ReconciliationResolutionAction,
} from '@/types/generated'

import {
  useProjectReconciliationsQuery,
  useResolveProjectReconciliationMutation,
} from './reconciliation-hooks'
import type { ResolveProjectReconciliationWire } from './reconciliation-api'

const RESOLVE_AUTHORIZATION_BASIS = 'interactive_user_reconciliation_resolution'
const RESOLVE_OPERATION = 'project.reconciliation.resolve'

const ACTION_LABELS: Record<ReconciliationResolutionAction, string> = {
  retained: 'Keep the current work',
  revised: 'Use the recommended replacement',
  cancelled: 'Cancel the affected work',
  superseded: 'Replace and archive the current work',
  invalidated: 'Discard the conflicting change',
}

const REPLACEMENT_REQUIRED = new Set<ReconciliationResolutionAction>(['revised', 'superseded'])

function humanize(value: string): string {
  return value.replaceAll('_', ' ').replace(/\b\w/g, (letter) => letter.toUpperCase())
}

function numberValue(value: number | bigint): number {
  return typeof value === 'bigint' ? Number(value) : value
}

function formatTimestamp(value: string): string {
  const date = new Date(value)
  return Number.isNaN(date.getTime())
    ? value
    : date.toLocaleString([], { dateStyle: 'medium', timeStyle: 'short' })
}

function newIdempotencyKey(prefix: string): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID()
  }
  return `${prefix}-${Date.now()}-${Math.random().toString(36).slice(2)}`
}

function adaptiveOperation(description: string): string {
  return description.match(/operation ['‘]([^'’]+)['’]/i)?.[1]?.toLowerCase() ?? 'change'
}

function createUserAuthorization(action: string): AuthorizationProvenance {
  const user = useAuthStore.getState().user
  if (!user) throw new Error('Sign in again before resolving this reconciliation.')
  return {
    principal: { kind: 'user', id: user.id, display_name: user.display_name ?? null },
    authorization_basis: RESOLVE_AUTHORIZATION_BASIS,
    action,
    event_id: newIdempotencyKey(action),
    occurred_at: new Date().toISOString(),
  }
}

function recordRefLine(recordType: string, recordId: string, recordRevision: string): string {
  return `${humanize(recordType)} ${recordId} @ revision ${recordRevision}`
}

/**
 * A shared, scoped reconciliation record has no reachable product exit
 * anywhere else in Forge (finding F10) -- this is that exit. It renders
 * every Project reconciliation still in `required` state, and posts the
 * user's decision through the same service the REST/native surfaces share
 * (design D15). A successful resolution invalidates the Overview,
 * execution-setup, and Project queries so the product resumes on its own;
 * nothing here asks the user to reload the page or flip a phase control.
 */
export function ReconciliationReviewCard({ projectId }: { projectId: string }) {
  const { data, isLoading, isError, error, refetch } = useProjectReconciliationsQuery(projectId)
  const [resolved, setResolved] = useState<Record<string, ProjectReconciliation>>({})

  if (isLoading) return null
  if (isError) {
    return (
      <Card className="min-w-0 border-destructive/40 bg-card p-4 sm:p-5">
        <p className="flex items-center gap-2 text-sm font-medium text-destructive">
          <WarningCircle size={16} aria-hidden /> Reconciliation status is unavailable
        </p>
        <p className="mt-1 text-xs text-muted-foreground">{getApiErrorMessage(error)}</p>
      </Card>
    )
  }

  const items = data?.items ?? []
  // Once a resolve succeeds locally, drop it from the actionable list
  // immediately rather than waiting on the background refetch this
  // mutation's `onSettled` already triggered -- the user should never see a
  // reconciliation they just resolved still offering the same action form.
  const pending = items.filter((item) => item.allowed_actions.length > 0 && !(item.id in resolved))
  const recentlyResolved = Object.values(resolved)

  if (pending.length === 0 && recentlyResolved.length === 0) return null

  return (
    <Card
      className="min-w-0 border-border-subtle bg-card"
      aria-label="Project reconciliation review"
    >
      <div className="flex min-w-0 flex-wrap items-start justify-between gap-3 border-b border-border-subtle px-4 py-3 sm:px-5">
        <div className="min-w-0">
          <p className="font-mono text-micro font-semibold uppercase tracking-[0.12em] text-muted-foreground">
            Project decision
          </p>
          <h2 className="mt-1 break-words text-sm font-semibold text-foreground">
            Review a requested change
          </h2>
          <p className="mt-1 max-w-2xl text-xs leading-5 text-muted-foreground">
            Choose the outcome in plain language. Technical record details are available only if you
            need them.
          </p>
        </div>
      </div>
      <div className="min-w-0 space-y-4 p-4 sm:p-5">
        {pending.map((reconciliation) => (
          <ReconciliationItem
            key={reconciliation.id}
            projectId={projectId}
            reconciliation={reconciliation}
            onRefresh={() => void refetch()}
            onResolved={(result) => setResolved((current) => ({ ...current, [result.id]: result }))}
          />
        ))}
        {recentlyResolved.length > 0 ? (
          <div className="space-y-2 border-t border-border-subtle pt-4" aria-live="polite">
            <p className="text-xs font-semibold uppercase tracking-[0.08em] text-muted-foreground">
              Recently resolved
            </p>
            {recentlyResolved.map((reconciliation) => (
              <ResolvedResultSummary key={reconciliation.id} reconciliation={reconciliation} />
            ))}
          </div>
        ) : null}
      </div>
    </Card>
  )
}

function ResolvedResultSummary({ reconciliation }: { reconciliation: ProjectReconciliation }) {
  const resolution = reconciliation.resolution
  const title =
    reconciliation.conflict.conflict_code === 'invalid_active_baseline'
      ? 'Plan updated. Work can resume.'
      : reconciliation.conflict.conflict_code === 'adaptive_task_boundary_crossed'
        ? reconciliation.state === 'revised'
          ? 'Task change accepted. The Project Agent can retry.'
          : 'Task change rejected. The approved plan stays in place.'
        : 'Decision saved.'
  return (
    <div className="rounded-md border border-ember-border bg-ember-surface p-3 text-xs">
      <p className="font-medium text-foreground">{title}</p>
      {resolution ? (
        <details className="mt-2 rounded-md border border-border-subtle bg-background px-3 py-2">
          <summary className="cursor-pointer font-medium text-muted-foreground">
            Technical details
          </summary>
          <div className="mt-2 space-y-1 break-words">
            <p className="text-muted-foreground">
              By {resolution.principal.display_name ?? resolution.principal.id} at{' '}
              {formatTimestamp(resolution.occurred_at)}
            </p>
            <p className="text-foreground">Reason: {resolution.reason}</p>
            {resolution.replacement_ref ? (
              <p className="text-muted-foreground">
                Replacement:{' '}
                {recordRefLine(
                  resolution.replacement_ref.record_type,
                  resolution.replacement_ref.record_id,
                  resolution.replacement_ref.record_revision ?? 'unspecified',
                )}
              </p>
            ) : null}
            <p className="font-mono text-micro text-muted-foreground">
              {recordRefLine(
                reconciliation.affected.record_type,
                reconciliation.affected.record_id,
                reconciliation.affected.record_revision,
              )}{' '}
              · version v{numberValue(reconciliation.version)}
            </p>
          </div>
        </details>
      ) : null}
    </div>
  )
}

function ReconciliationItem({
  projectId,
  reconciliation,
  onRefresh,
  onResolved,
}: {
  projectId: string
  reconciliation: ProjectReconciliation
  onRefresh: () => void
  onResolved: (result: ProjectReconciliation) => void
}) {
  const mutation = useResolveProjectReconciliationMutation(projectId)
  const invalidBaselineCorrection =
    reconciliation.conflict.conflict_code === 'invalid_active_baseline'
  const adaptiveBoundary =
    reconciliation.conflict.conflict_code === 'adaptive_task_boundary_crossed'
  const suggestedReplacement = reconciliation.suggested_replacement_ref
  const options = reconciliation.allowed_actions
    .filter((candidate) => !REPLACEMENT_REQUIRED.has(candidate) || suggestedReplacement)
    .map((candidate) => ({ value: candidate, label: ACTION_LABELS[candidate] }))
  const [action, setAction] = useState<ReconciliationResolutionAction>(
    options[0]?.value ?? reconciliation.allowed_actions[0],
  )
  const [deferred, setDeferred] = useState(false)

  function submit(selectedAction: ReconciliationResolutionAction) {
    const replacementRequired = REPLACEMENT_REQUIRED.has(selectedAction)
    if (mutation.isPending || (replacementRequired && !suggestedReplacement)) return
    const replacement_ref: ReconciliationReplacementRef | null = replacementRequired
      ? suggestedReplacement
      : null
    const reason = invalidBaselineCorrection
      ? "Accepted Forge's recommended plan-format update and default Project Agent task authority."
      : adaptiveBoundary && selectedAction === 'revised'
        ? 'Accepted the corrected governing baseline and allowed the Project Agent to retry the blocked Task operation.'
        : adaptiveBoundary && selectedAction === 'retained'
          ? 'Kept the governing plan and rejected the earlier blocked Task operation.'
          : `User selected “${ACTION_LABELS[selectedAction]}” for ${humanize(reconciliation.conflict.conflict_code).toLowerCase()}.`
    const input: ResolveProjectReconciliationWire = {
      mutation: {
        expected_version: numberValue(reconciliation.version),
        expected_digest: null,
        idempotency_key: newIdempotencyKey(`reconciliation-resolve-${reconciliation.id}`),
        deduplication_key: null,
        authorization: createUserAuthorization(RESOLVE_OPERATION),
      },
      action: selectedAction,
      replacement_ref,
      reason,
    }
    mutation.mutate(
      { reconciliationId: reconciliation.id, input },
      {
        onSuccess: (result) => {
          onResolved(result)
        },
      },
    )
  }

  const fieldId = `reconciliation-${reconciliation.id}`

  if (invalidBaselineCorrection) {
    return (
      <div className="rounded-md border border-ember-border bg-ember-surface p-3 sm:p-4">
        <div className="min-w-0">
          <p className="text-sm font-semibold text-foreground">
            Update how the Project Agent manages work
          </p>
          <p className="mt-1 max-w-3xl text-sm leading-6 text-foreground">
            This Project uses an older plan format. Accepting replaces that plan record with
            Forge&apos;s corrected version and lets the Project Agent split, reorder, and replace
            in-scope Tasks. Work resumes automatically.
          </p>
          <p className="mt-2 text-xs leading-5 text-muted-foreground">
            This does not change the approved Charter, switch your Project Agent, or release the
            Project.
          </p>
        </div>

        <TechnicalDetails reconciliation={reconciliation} />

        {deferred ? (
          <div
            className="mt-3 rounded-md border border-border-subtle bg-background p-3"
            role="status"
          >
            <p className="text-xs text-foreground">
              No change was made. Repository work remains paused until you accept the update.
            </p>
            <Button className="mt-2" variant="outline" size="sm" onClick={() => setDeferred(false)}>
              Review again
            </Button>
          </div>
        ) : (
          <div className="mt-4 flex flex-col-reverse gap-2 sm:flex-row sm:justify-end">
            <Button
              variant="outline"
              onClick={() => setDeferred(true)}
              disabled={mutation.isPending}
            >
              Reject for now
            </Button>
            {suggestedReplacement ? (
              <Button
                onClick={() => submit('revised')}
                disabled={mutation.isPending}
                aria-describedby={`${fieldId}-accept-help`}
              >
                {mutation.isPending ? 'Applying update…' : 'Accept update & resume work'}
              </Button>
            ) : (
              <Button onClick={onRefresh} disabled={mutation.isPending}>
                Refresh recommended update
              </Button>
            )}
          </div>
        )}
        <p
          id={`${fieldId}-accept-help`}
          className="mt-2 text-xs text-muted-foreground sm:text-right"
        >
          One click approves and applies the correction; there is no second plan approval.
        </p>

        {mutation.isError ? (
          <div className="mt-3">
            <ConflictDetails error={mutation.error} fallbackAuthority="reconciliation" />
          </div>
        ) : null}
      </div>
    )
  }

  if (adaptiveBoundary) {
    const operation = adaptiveOperation(reconciliation.conflict.description)
    const operationLabel =
      operation === 'replace'
        ? 'Task replacement'
        : operation === 'split'
          ? 'Task split'
          : operation === 'sequence'
            ? 'Task order change'
            : 'Task change'

    return (
      <div className="rounded-md border border-ember-border bg-ember-surface p-3 sm:p-4">
        <div className="min-w-0">
          <p className="text-sm font-semibold text-foreground">
            Allow the Project Agent to retry this {operationLabel.toLowerCase()}?
          </p>
          <p className="mt-1 max-w-3xl text-sm leading-6 text-foreground">
            Forge blocked the earlier {operationLabel.toLowerCase()} under the old plan rules.
            Accepting clears that block and lets the Project Agent retry it under the corrected
            plan.
          </p>
          <p className="mt-2 text-xs leading-5 text-muted-foreground">
            {operation === 'replace'
              ? 'This replaces the in-scope Task. It does not switch the Project Agent assigned to this Project.'
              : 'This changes only the in-scope Tasks. It does not switch the Project Agent assigned to this Project.'}
          </p>
        </div>

        <TechnicalDetails reconciliation={reconciliation} />

        <div className="mt-4 flex flex-col-reverse gap-2 sm:flex-row sm:justify-end">
          <Button
            variant="outline"
            onClick={() => submit('retained')}
            disabled={mutation.isPending}
          >
            Reject change
          </Button>
          <Button
            onClick={() => submit('revised')}
            disabled={mutation.isPending || !suggestedReplacement}
            aria-describedby={`${fieldId}-adaptive-help`}
          >
            {mutation.isPending ? 'Saving decision…' : 'Accept & let agent retry'}
          </Button>
        </div>
        <p
          id={`${fieldId}-adaptive-help`}
          className="mt-2 text-xs text-muted-foreground sm:text-right"
        >
          {suggestedReplacement
            ? 'The corrected plan is active. This decision resumes the affected Task automatically.'
            : 'Accept the plan-format update first; then this action becomes available.'}
        </p>

        {mutation.isError ? (
          <div className="mt-3">
            <ConflictDetails error={mutation.error} fallbackAuthority="reconciliation" />
          </div>
        ) : null}
      </div>
    )
  }

  return (
    <div className="rounded-md border border-ember-border bg-ember-surface p-3 sm:p-4">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="min-w-0">
          <p className="break-words text-sm font-semibold text-foreground">
            {reconciliation.conflict.description}
          </p>
          <p className="mt-1 text-xs leading-5 text-muted-foreground">
            Choose what Forge should keep. The Project Agent cannot make this decision silently.
          </p>
        </div>
      </div>

      <TechnicalDetails reconciliation={reconciliation} />

      <div className="mt-4 space-y-3 border-t border-border-subtle pt-3">
        <div>
          <Label htmlFor={`${fieldId}-action`}>What should Forge do?</Label>
          <select
            id={`${fieldId}-action`}
            className="flex h-9 w-full rounded-md border border-input bg-background px-3 py-2 text-ui"
            value={action}
            onChange={(event) => setAction(event.target.value as ReconciliationResolutionAction)}
            aria-label={`Resolution action for ${reconciliation.conflict.conflict_code}`}
          >
            {options.map((option) => (
              <option key={option.value} value={option.value}>
                {option.label}
              </option>
            ))}
          </select>
        </div>

        {mutation.isError ? (
          <ConflictDetails error={mutation.error} fallbackAuthority="reconciliation" />
        ) : null}

        <div className="flex justify-end">
          {options.length > 0 ? (
            <Button onClick={() => submit(action)} disabled={mutation.isPending}>
              {mutation.isPending ? 'Saving decision…' : 'Confirm decision'}
            </Button>
          ) : (
            <Button onClick={onRefresh}>Refresh available choices</Button>
          )}
        </div>
      </div>
    </div>
  )
}

function TechnicalDetails({ reconciliation }: { reconciliation: ProjectReconciliation }) {
  return (
    <details className="mt-3 rounded-md border border-border-subtle bg-background px-3 py-2 text-xs">
      <summary className="cursor-pointer font-medium text-muted-foreground">
        Technical details
      </summary>
      <div className="mt-3 space-y-3">
        <p className="break-words font-mono text-micro text-muted-foreground">
          {reconciliation.conflict.conflict_code} · {reconciliation.conflict.domain} · version v
          {numberValue(reconciliation.version)}
        </p>
        <dl className="grid grid-cols-1 gap-2 sm:grid-cols-2">
          <div>
            <dt className="font-semibold uppercase tracking-[0.06em] text-muted-foreground">
              Current rule
            </dt>
            <dd className="mt-0.5 break-words text-foreground">
              {recordRefLine(
                reconciliation.governing.record_type,
                reconciliation.governing.record_id,
                reconciliation.governing.record_revision,
              )}
            </dd>
          </div>
          <div>
            <dt className="font-semibold uppercase tracking-[0.06em] text-muted-foreground">
              Affected work
            </dt>
            <dd className="mt-0.5 break-words text-foreground">
              {recordRefLine(
                reconciliation.affected.record_type,
                reconciliation.affected.record_id,
                reconciliation.affected.record_revision,
              )}
            </dd>
          </div>
        </dl>
        {reconciliation.conflict.affected_paths.length > 0 ? (
          <div>
            <p className="font-semibold uppercase tracking-[0.06em] text-muted-foreground">
              Fields involved
            </p>
            <ul className="mt-1 list-inside list-disc break-words text-foreground">
              {reconciliation.conflict.affected_paths.map((path) => (
                <li key={path}>{path}</li>
              ))}
            </ul>
          </div>
        ) : null}
      </div>
    </details>
  )
}
