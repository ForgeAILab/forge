import type { RetryAction } from '@/types/generated'

export type ExecutionBlockerRouteTo = '/projects/$projectId/chat' | '/projects/$projectId/overview'

export type ExecutionBlockerNavTarget = {
  label: string
  to: ExecutionBlockerRouteTo
  hash?: string
  /**
   * `primary` is the strong call-to-action for a blocker that needs the
   * viewer's authority right now (approve, reconcile). `link` is a plain
   * navigation link for a blocker that only needs the viewer to continue
   * planning or setup elsewhere.
   */
  variant: 'primary' | 'link'
}

/**
 * Where the one permitted `ExecutionBlockerProjection.next_action` (D17)
 * routes the viewer. This is the single place every surface picks a route
 * for a `RetryAction`, so a Task-scoped or Project-wide reconciliation
 * always lands on the exact reconciliation review card
 * (`ReconciliationReviewCard`, mounted on the Project overview route)
 * instead of a dead end or a re-derived approval link (D16/8.2.6).
 *
 * `refresh_and_retry`, `use_new_idempotency_key`, `retry_after`, and
 * `correct_input` have no navigation target here — those are retry/refresh
 * controls the caller renders itself against its own query, not a Link.
 */
const NAV_TARGETS: Partial<Record<RetryAction, ExecutionBlockerNavTarget>> = {
  complete_setup: {
    label: 'Finish build setup',
    to: '/projects/$projectId/chat',
    hash: 'project-execution-status',
    variant: 'link',
  },
  attach_repository: {
    label: 'Attach repository',
    to: '/projects/$projectId/chat',
    hash: 'project-execution-status',
    variant: 'link',
  },
  select_worker: {
    label: 'Select a Worker',
    to: '/projects/$projectId/chat',
    hash: 'project-execution-status',
    variant: 'link',
  },
  select_independent_reviewer: {
    label: 'Select an independent reviewer',
    to: '/projects/$projectId/chat',
    hash: 'project-execution-status',
    variant: 'link',
  },
  retry_provisioning: {
    label: 'Retry provisioning',
    to: '/projects/$projectId/chat',
    hash: 'project-execution-status',
    variant: 'link',
  },
  reauthorize: {
    label: 'Approve plan & start work',
    to: '/projects/$projectId/chat',
    hash: 'execution-approval',
    variant: 'primary',
  },
  repropose: {
    label: 'Plan execution baseline',
    to: '/projects/$projectId/chat',
    variant: 'link',
  },
  resolve_reconciliation: {
    label: 'Review current plan',
    to: '/projects/$projectId/overview',
    variant: 'primary',
  },
}

export function executionBlockerNavTarget(
  action: RetryAction | null | undefined,
): ExecutionBlockerNavTarget | null {
  if (!action) return null
  return NAV_TARGETS[action] ?? null
}
