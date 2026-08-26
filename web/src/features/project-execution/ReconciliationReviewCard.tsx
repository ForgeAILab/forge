import { useState } from 'react'
import { WarningCircle } from '@phosphor-icons/react'

import { ConflictDetails } from '@/components/conflict-details'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Textarea } from '@/components/ui/textarea'
import { getApiErrorMessage } from '@/lib/api-error'
import { useAuthStore } from '@/stores/auth'
import type {
  AuthorizationProvenance,
  ProjectReconciliation,
  ReconciliationReplacementRef,
  ReconciliationResolutionAction,
} from '@/types/generated'

import { useProjectReconciliationsQuery, useResolveProjectReconciliationMutation } from './reconciliation-hooks'
import type { ResolveProjectReconciliationWire } from './reconciliation-api'

const RESOLVE_AUTHORIZATION_BASIS = 'interactive_user_reconciliation_resolution'
const RESOLVE_OPERATION = 'project.reconciliation.resolve'

const ACTION_LABELS: Record<ReconciliationResolutionAction, string> = {
  retained: 'Retain the governing record',
  revised: 'Replace with a revised record',
  cancelled: 'Cancel the affected record',
  superseded: 'Supersede with a replacement',
  invalidated: 'Invalidate the affected record',
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
  const { data, isLoading, isError, error } = useProjectReconciliationsQuery(projectId)
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
  const pending = items.filter(
    (item) => item.allowed_actions.length > 0 && !(item.id in resolved),
  )
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
            Canonical conflict
          </p>
          <h2 className="mt-1 break-words text-sm font-semibold text-foreground">
            Reconciliation review
          </h2>
        </div>
      </div>
      <div className="min-w-0 space-y-4 p-4 sm:p-5">
        {pending.map((reconciliation) => (
          <ReconciliationItem
            key={reconciliation.id}
            projectId={projectId}
            reconciliation={reconciliation}
            onResolved={(result) =>
              setResolved((current) => ({ ...current, [result.id]: result }))
            }
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
  return (
    <div className="rounded-md border border-ember-border bg-ember-surface p-3 text-xs">
      <p className="font-medium text-foreground">
        {humanize(reconciliation.state)} · {recordRefLine(
          reconciliation.affected.record_type,
          reconciliation.affected.record_id,
          reconciliation.affected.record_revision,
        )}
      </p>
      {resolution ? (
        <>
          <p className="mt-1 text-muted-foreground">
            By {resolution.principal.display_name ?? resolution.principal.id} at{' '}
            {formatTimestamp(resolution.occurred_at)}
          </p>
          <p className="mt-1 text-foreground">Reason: {resolution.reason}</p>
          {resolution.replacement_ref ? (
            <p className="mt-1 text-muted-foreground">
              Replacement:{' '}
              {recordRefLine(
                resolution.replacement_ref.record_type,
                resolution.replacement_ref.record_id,
                resolution.replacement_ref.record_revision ?? 'unspecified',
              )}
            </p>
          ) : null}
        </>
      ) : null}
      <p className="mt-1 font-mono text-micro text-muted-foreground">
        version v{numberValue(reconciliation.version)}
      </p>
    </div>
  )
}

function ReconciliationItem({
  projectId,
  reconciliation,
  onResolved,
}: {
  projectId: string
  reconciliation: ProjectReconciliation
  onResolved: (result: ProjectReconciliation) => void
}) {
  const mutation = useResolveProjectReconciliationMutation(projectId)
  const options = reconciliation.allowed_actions.map((action) => ({
    value: action,
    label: ACTION_LABELS[action],
  }))
  const [action, setAction] = useState<ReconciliationResolutionAction>(
    reconciliation.allowed_actions[0],
  )
  const [reason, setReason] = useState('')
  const [replacementType, setReplacementType] = useState('')
  const [replacementId, setReplacementId] = useState('')
  const [replacementRevision, setReplacementRevision] = useState('')
  const invalidBaselineCorrection =
    reconciliation.conflict.conflict_code === 'invalid_active_baseline'
  const replacementTypeValue = invalidBaselineCorrection
    ? (reconciliation.suggested_replacement_ref?.record_type ?? '')
    : replacementType
  const replacementIdValue = invalidBaselineCorrection
    ? (reconciliation.suggested_replacement_ref?.record_id ?? '')
    : replacementId
  const replacementRevisionValue = invalidBaselineCorrection
    ? (reconciliation.suggested_replacement_ref?.record_revision ?? '')
    : replacementRevision

  const replacementRequired = REPLACEMENT_REQUIRED.has(action)
  const reasonValid = reason.trim().length > 0
  const replacementValid =
    !replacementRequired ||
    (replacementTypeValue.trim().length > 0 && replacementIdValue.trim().length > 0)
  const canSubmit = reasonValid && replacementValid && !mutation.isPending

  function submit() {
    if (!canSubmit) return
    const replacement_ref: ReconciliationReplacementRef | null = replacementRequired
      ? {
          record_type: replacementTypeValue.trim(),
          record_id: replacementIdValue.trim(),
          record_revision: replacementRevisionValue.trim() || null,
        }
      : null
    const input: ResolveProjectReconciliationWire = {
      mutation: {
        expected_version: numberValue(reconciliation.version),
        expected_digest: null,
        idempotency_key: newIdempotencyKey(`reconciliation-resolve-${reconciliation.id}`),
        deduplication_key: null,
        authorization: createUserAuthorization(RESOLVE_OPERATION),
      },
      action,
      replacement_ref,
      reason: reason.trim(),
    }
    mutation.mutate(
      { reconciliationId: reconciliation.id, input },
      {
        onSuccess: (result) => {
          onResolved(result)
          setReason('')
          setReplacementType('')
          setReplacementId('')
          setReplacementRevision('')
        },
      },
    )
  }

  const fieldId = `reconciliation-${reconciliation.id}`

  return (
    <div className="rounded-md border border-ember-border bg-ember-surface p-3">
      <div className="flex flex-wrap items-start justify-between gap-2">
        <div className="min-w-0">
          <p className="break-words text-sm font-medium text-foreground">
            {reconciliation.conflict.description}
          </p>
          <p className="mt-1 font-mono text-micro text-muted-foreground">
            {reconciliation.conflict.conflict_code} · {reconciliation.conflict.domain}
          </p>
        </div>
        <span className="inline-flex items-center rounded-full border px-2 py-0.5 font-mono text-micro font-semibold uppercase tracking-[0.08em] text-muted-foreground">
          required principal: {reconciliation.required_principal}
        </span>
      </div>

      <dl className="mt-3 grid grid-cols-1 gap-2 text-xs sm:grid-cols-2">
        <div>
          <dt className="font-semibold uppercase tracking-[0.06em] text-muted-foreground">
            Governing record
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
            Affected record
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
        <div className="mt-3 text-xs">
          <p className="font-semibold uppercase tracking-[0.06em] text-muted-foreground">
            Impact
          </p>
          <ul className="mt-0.5 list-inside list-disc break-words text-foreground">
            {reconciliation.conflict.affected_paths.map((path) => (
              <li key={path}>{path}</li>
            ))}
          </ul>
        </div>
      ) : null}

      <p className="mt-3 font-mono text-micro text-muted-foreground">
        state {reconciliation.state} · version v{numberValue(reconciliation.version)}
      </p>

      <div className="mt-4 space-y-3 border-t border-border-subtle pt-3">
        <div>
          <Label htmlFor={`${fieldId}-action`}>Resolution</Label>
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

        {replacementRequired ? (
          <div>
            {invalidBaselineCorrection ? (
              <p className="mb-2 text-xs leading-5 text-muted-foreground">
                {reconciliation.suggested_replacement_ref
                  ? 'Forge verified the exact approved successor below. Resolving will activate it in the same transaction.'
                  : 'Approve a typed successor revision first. Forge will fill the exact replacement here once its user approval receipt is active.'}
              </p>
            ) : null}
            <div className="grid grid-cols-1 gap-2 sm:grid-cols-3">
              <div>
                <Label htmlFor={`${fieldId}-replacement-type`}>Replacement type</Label>
                <Input
                  id={`${fieldId}-replacement-type`}
                  value={replacementTypeValue}
                  onChange={(event) => setReplacementType(event.target.value)}
                  placeholder="task"
                  readOnly={invalidBaselineCorrection}
                />
              </div>
              <div>
                <Label htmlFor={`${fieldId}-replacement-id`}>Replacement id</Label>
                <Input
                  id={`${fieldId}-replacement-id`}
                  value={replacementIdValue}
                  onChange={(event) => setReplacementId(event.target.value)}
                  readOnly={invalidBaselineCorrection}
                />
              </div>
              <div>
                <Label htmlFor={`${fieldId}-replacement-revision`}>Replacement revision</Label>
                <Input
                  id={`${fieldId}-replacement-revision`}
                  value={replacementRevisionValue}
                  onChange={(event) => setReplacementRevision(event.target.value)}
                  placeholder="optional"
                  readOnly={invalidBaselineCorrection}
                />
              </div>
            </div>
          </div>
        ) : null}

        <div>
          <Label htmlFor={`${fieldId}-reason`}>Reason</Label>
          <Textarea
            id={`${fieldId}-reason`}
            value={reason}
            onChange={(event) => setReason(event.target.value)}
            placeholder="Explain why this resolution is correct."
            rows={3}
          />
        </div>

        {mutation.isError ? (
          <ConflictDetails error={mutation.error} fallbackAuthority="reconciliation" />
        ) : null}

        <div className="flex justify-end">
          <Button onClick={submit} disabled={!canSubmit}>
            {mutation.isPending ? 'Resolving…' : 'Resolve'}
          </Button>
        </div>
      </div>
    </div>
  )
}
