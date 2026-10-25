// Mission Control formatting and attention action utilities.

import { actionForPost } from '@/components/task-detail/task-action-buttons'
import type { Offer } from '@/types/generated'
import type { RetryTurnAction } from '@/types/generated/bindings/RetryTurnAction'
import type { AttentionItem } from '@/features/federation/types'

export function formatDate(value: string | null | undefined): string {
  if (!value) return 'No date'
  const date = new Date(value)
  return Number.isNaN(date.getTime())
    ? value
    : date.toLocaleString([], { dateStyle: 'medium', timeStyle: 'short' })
}

export function humanize(value: string | null | undefined): string {
  if (!value) return 'Unknown'
  return value.replaceAll('_', ' ').replace(/\b\w/g, (letter) => letter.toUpperCase())
}

export function attentionTaskId(item: AttentionItem): string | null {
  if (item.scope_type === 'task' && item.scope_id.trim()) return item.scope_id

  const entityType = item.details?.entity_type
  const entityId = item.details?.entity_id
  if (entityType === 'task' && typeof entityId === 'string' && entityId.trim()) {
    return entityId
  }
  return null
}

export function attentionActionLabel(value: string | null | undefined): string {
  const normalized = value?.trim().toLowerCase().replaceAll(' ', '_')
  if (normalized === 'inspect_run') return 'Inspect run'
  if (normalized === 'inspect_ci') return 'Inspect CI'
  return humanize(value)
}

export function isInspectRunAction(value: string | null | undefined): boolean {
  return value?.trim().toLowerCase().replaceAll(' ', '_') === 'inspect_run'
}

export function count(value: bigint | number): number {
  return typeof value === 'bigint' ? Number(value) : value
}

export function attentionRetryAction(
  item: AttentionItem,
): (Omit<RetryTurnAction, 'expected_version'> & { expected_version: number }) | null {
  if (item.lifecycle === 'resolved') return null
  const candidate = item.details.retry_action
  if (!candidate || typeof candidate !== 'object' || Array.isArray(candidate)) return null
  const action = candidate as Record<string, unknown>
  if (
    action.kind !== 'retry_turn' ||
    typeof action.chat_id !== 'string' ||
    typeof action.turn_id !== 'string' ||
    typeof action.expected_version !== 'number' ||
    !Number.isSafeInteger(action.expected_version) ||
    action.expected_version < 0
  ) {
    return null
  }
  return {
    kind: action.kind,
    chat_id: action.chat_id,
    turn_id: action.turn_id,
    expected_version: action.expected_version,
  }
}

export function attentionTone(item: AttentionItem): string {
  if (
    [
      'validation_failed',
      'run_stalled',
      'retry_exhausted',
      'execution_failed',
      'runtime_offline',
    ].includes(item.category)
  ) {
    return 'border-destructive/30 bg-destructive/5'
  }
  if (
    [
      'human_input_required',
      'review_risk',
      'review_needs_owner',
      'budget_threshold',
      'commitment_overdue',
      'conflict_hotspot',
      'progress_warning',
      'flaky_check',
      'base_check_failing',
    ].includes(item.category)
  ) {
    return 'border-warning/30 bg-warning/5'
  }
  return 'border-border-subtle bg-card'
}

export type ReviewActionAccess = { loading: boolean; canWrite: boolean; isOwner: boolean }

export function reviewOffers(offers: Offer[], access: ReviewActionAccess): Offer[] {
  if (!access.canWrite) return []
  return offers.flatMap((offer) => {
    // The Task action API accepts human offers from Project writers. Only
    // approval with an override additionally requires Project owner access.
    if (!offer.authority.includes('owner')) return []
    if (access.isOwner || offer.action.verb !== 'approve') return [offer]
    const action = actionForPost(offer, offer.action)
    if (action.verb === 'approve' && action.override) return []
    return [
      {
        ...offer,
        parameters: offer.parameters.map((parameter) =>
          parameter.name === 'override' && parameter.boolean_values?.includes(true)
            ? { ...parameter, boolean_values: [false] }
            : parameter,
        ),
      },
    ]
  })
}
