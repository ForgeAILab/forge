// Execution setup action controls and mutations.

import { useRef } from 'react'
import { Link } from '@tanstack/react-router'
import {
  ArrowClockwise,
  ArrowUpRight,
  CircleNotch,
  ShieldCheck,
  UserCircle,
  WarningCircle,
} from '@phosphor-icons/react'
import { ConflictDetails } from '@/components/conflict-details'
import { getApiErrorMessage } from '@/lib/api-error'
import { Button } from '@/components/ui/button'
import type { ProjectExecutionSetupResponse } from '@/types/generated'
import { executionBlockerNavTarget } from '../executionBlockerNav'
import {
  useAttachPrimaryRepositoryMutation,
  useRetryProvisioningMutation,
  useSelectIndependentReviewerMutation,
  useSelectWorkerMutation,
} from '../hooks'
import { type SetupAction, asNumber, isConflictError, createIdempotencyKey } from './utils'
import { ActionSelect, RepositorySelect } from './selectors'

export function ActionArea({
  action,
  data,
  projectId,
  workerSelection,
  reviewerSelection,
  repositorySelection,
  setWorkerSelection,
  setReviewerSelection,
  setRepositorySelection,
  workerMutation,
  reviewerMutation,
  repositoryMutation,
  retryMutation,
  onRefresh,
  compact = false,
}: {
  action: SetupAction | null
  data: ProjectExecutionSetupResponse
  projectId: string
  workerSelection: string
  reviewerSelection: string
  repositorySelection: string
  setWorkerSelection: (value: string) => void
  setReviewerSelection: (value: string) => void
  setRepositorySelection: (value: string) => void
  workerMutation: ReturnType<typeof useSelectWorkerMutation>
  reviewerMutation: ReturnType<typeof useSelectIndependentReviewerMutation>
  repositoryMutation: ReturnType<typeof useAttachPrimaryRepositoryMutation>
  retryMutation: ReturnType<typeof useRetryProvisioningMutation>
  onRefresh: () => void
  compact?: boolean
}) {
  const attemptKeys = useRef<Record<string, { fingerprint: string; key: string }>>({})
  const blocker = data.execution_blocker
  const expectedProjectVersion = asNumber(data.project_version)
  const reviewerOptions = data.eligible_reviewers
  const mutationError =
    action === 'select_worker'
      ? workerMutation.error
      : action === 'select_independent_reviewer'
        ? reviewerMutation.error
        : action === 'attach_repository'
          ? repositoryMutation.error
          : retryMutation.error
  const conflict = isConflictError(mutationError)
  const allReady =
    !conflict &&
    data.coordination_state === 'ready' &&
    data.execution_setup_state === 'ready' &&
    data.execution_gate === 'active'

  if (allReady) return null

  const idempotencyKey = (actionName: string, fingerprint: string): string => {
    const previous = attemptKeys.current[actionName]
    if (previous?.fingerprint === fingerprint) return previous.key
    const key = createIdempotencyKey(actionName, projectId)
    attemptKeys.current[actionName] = { fingerprint, key }
    return key
  }

  const submitWorker = () => {
    if (!workerSelection) return
    workerMutation.mutate({
      identity_id: workerSelection,
      expected_project_version: expectedProjectVersion,
      idempotency_key: idempotencyKey(
        'select-worker',
        `${expectedProjectVersion}:${workerSelection}`,
      ),
    })
  }
  const submitReviewer = () => {
    if (!reviewerSelection) return
    reviewerMutation.mutate({
      identity_id: reviewerSelection,
      expected_project_version: expectedProjectVersion,
      idempotency_key: idempotencyKey(
        'select-independent-reviewer',
        `${expectedProjectVersion}:${reviewerSelection}`,
      ),
    })
  }
  const submitRepository = () => {
    if (!repositorySelection) return
    repositoryMutation.mutate({
      repo_id: repositorySelection,
      expected_project_version: expectedProjectVersion,
      idempotency_key: idempotencyKey(
        'attach-primary-repository',
        `${expectedProjectVersion}:${repositorySelection}`,
      ),
    })
  }
  const submitRetry = () => {
    const operation = data.provisioning
    if (!operation) return
    retryMutation.mutate({
      expected_operation_version: asNumber(operation.version),
      idempotency_key: idempotencyKey(
        'retry-provisioning',
        `${operation.id}:${asNumber(operation.version)}`,
      ),
    })
  }

  const actionBody = (() => {
    if (conflict) {
      return (
        <Button
          type="button"
          size="sm"
          variant="outline"
          className="w-full sm:w-auto"
          onClick={onRefresh}
        >
          <ArrowClockwise size={14} aria-hidden /> Refresh readiness
        </Button>
      )
    }

    if (action === 'refresh_and_retry') {
      return (
        <Button
          type="button"
          size="sm"
          variant="outline"
          className="w-full sm:w-auto"
          onClick={onRefresh}
        >
          <ArrowClockwise size={14} aria-hidden /> Refresh readiness
        </Button>
      )
    }

    if (data.coordination_state !== 'ready') {
      const coordinationAction =
        data.coordination_state === 'setup_required'
          ? 'Configure Project Agent'
          : 'Restore Project Agent binding'
      return (
        <Link
          to="/agents"
          search={{ project: projectId }}
          className="inline-flex w-full max-w-full justify-center items-center gap-1.5 text-xs font-semibold text-primary underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring sm:w-auto"
        >
          {coordinationAction} <ArrowUpRight size={13} aria-hidden />
        </Link>
      )
    }

    if (action === 'select_worker') {
      if (data.eligible_workers.length === 0) {
        return (
          <Link
            to="/agents"
            search={{ project: projectId }}
            className="inline-flex w-full max-w-full justify-center items-center gap-1.5 text-xs font-semibold text-primary underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring sm:w-auto"
          >
            Create Worker <ArrowUpRight size={13} aria-hidden />
          </Link>
        )
      }
      return (
        <ActionSelect
          id="execution-worker"
          label="Worker"
          value={workerSelection}
          options={data.eligible_workers}
          onChange={setWorkerSelection}
          buttonLabel="Select Worker"
          pending={workerMutation.isPending}
          onSubmit={submitWorker}
          icon={<UserCircle size={14} aria-hidden />}
          compact={compact}
        />
      )
    }

    if (action === 'select_independent_reviewer') {
      if (reviewerOptions.length === 0) {
        return (
          <Link
            to="/agents"
            search={{ project: projectId }}
            className="inline-flex w-full max-w-full justify-center items-center gap-1.5 text-xs font-semibold text-primary underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring sm:w-auto"
          >
            Create reviewer <ArrowUpRight size={13} aria-hidden />
          </Link>
        )
      }
      return (
        <ActionSelect
          id="execution-independent-reviewer"
          label="Reviewer"
          value={reviewerSelection}
          options={reviewerOptions}
          onChange={setReviewerSelection}
          buttonLabel="Select reviewer"
          pending={reviewerMutation.isPending}
          onSubmit={submitReviewer}
          icon={<ShieldCheck size={14} aria-hidden />}
          compact={compact}
        />
      )
    }

    if (action === 'attach_repository') {
      return (
        <RepositorySelect
          projectId={projectId}
          value={repositorySelection}
          onChange={setRepositorySelection}
          pending={repositoryMutation.isPending}
          onSubmit={submitRepository}
          compact={compact}
        />
      )
    }

    // Any remaining legacy or reconciliation projection routes
    // through the one canonical `execution_blocker.next_action` instead of
    // re-deriving a link from `execution_gate` here. This is also what
    // fixes the dead end a `reconciliation_required` gate used to fall
    // into: it now lands on the exact reconciliation review card instead of
    // a generic "keep planning" link (D16/8.2.6).
    if (data.execution_setup_state === 'ready') {
      const navTarget = executionBlockerNavTarget(blocker?.next_action)
      if (navTarget) {
        const className =
          navTarget.variant === 'primary'
            ? 'inline-flex w-full max-w-full items-center justify-center gap-1.5 rounded-md bg-primary px-3 py-2 text-xs font-semibold text-primary-foreground shadow-xs transition-[color,background-color,transform] hover:brightness-95 active:translate-y-px focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 sm:w-auto'
            : 'inline-flex w-full max-w-full justify-center items-center gap-1.5 text-xs font-semibold text-primary underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring sm:w-auto'
        return (
          <Link
            to={navTarget.to}
            params={{ projectId }}
            hash={navTarget.hash}
            className={className}
          >
            {navTarget.label} <ArrowUpRight size={13} aria-hidden />
          </Link>
        )
      }
    }

    if (data.execution_setup_state === 'provisioning') {
      return (
        <Button
          type="button"
          size="sm"
          variant="outline"
          className="w-full sm:w-auto"
          onClick={onRefresh}
        >
          <ArrowClockwise size={14} aria-hidden /> Refresh provisioning status
        </Button>
      )
    }

    if (action === 'retry_provisioning' || data.provisioning?.retryable) {
      return (
        <Button
          type="button"
          size="sm"
          className="w-full sm:w-auto"
          onClick={submitRetry}
          disabled={retryMutation.isPending}
        >
          {retryMutation.isPending ? (
            <CircleNotch
              size={14}
              className="animate-spin motion-reduce:animate-none"
              aria-hidden
            />
          ) : (
            <ArrowClockwise size={14} aria-hidden />
          )}
          {retryMutation.isPending ? 'Retrying…' : 'Retry provisioning'}
        </Button>
      )
    }

    if (data.execution_setup_state === 'failed') {
      return (
        <Link
          to="/projects/$projectId/settings"
          params={{ projectId }}
          className="inline-flex w-full max-w-full justify-center items-center gap-1.5 text-xs font-semibold text-primary underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring sm:w-auto"
        >
          Review setup configuration <ArrowUpRight size={13} aria-hidden />
        </Link>
      )
    }

    return (
      <Link
        to="/projects/$projectId/chat"
        params={{ projectId }}
        className="inline-flex w-full max-w-full justify-center items-center gap-1.5 text-xs font-semibold text-primary underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring sm:w-auto"
      >
        Continue planning in Project Chat <ArrowUpRight size={13} aria-hidden />
      </Link>
    )
  })()

  const actionSummary = conflict
    ? 'The Project setup changed while this action was open. Refresh the current authority before trying again.'
    : action === 'refresh_and_retry'
      ? 'Refresh the readiness projection before taking a setup action.'
      : data.coordination_state === 'setup_required'
        ? 'Configure the Project Agent before admitting a coordination turn.'
        : data.coordination_state === 'unavailable'
          ? 'Restore the Project Agent binding before admitting a coordination turn.'
          : action === 'select_worker'
            ? 'Select or create a Worker before repository-backed execution can proceed.'
            : action === 'select_independent_reviewer'
              ? 'Select any enabled configured Agent. The Worker and reviewer may be the same Agent.'
              : action === 'attach_repository'
                ? 'Attach the Project’s primary repository explicitly.'
                : action === 'retry_provisioning'
                  ? data.execution_setup_state === 'provisioning'
                    ? 'Recheck provisioning without claiming success before the server confirms it.'
                    : 'Retry the recorded provisioning operation; Forge will report if its retry budget is exhausted.'
                  : // The one canonical blocker's own explanation, not a
                    // re-derivation from `execution_gate` — this is what
                    // keeps a reconciliation from ever reading like a
                    // baseline-approval prompt here (F12b/D17).
                    (blocker?.safe_explanation ??
                    'Execution setup is complete; continue with the current planning gate.')

  return (
    <div className="min-w-0 rounded-md border border-ember-border bg-ember-surface p-3">
      <div className="flex min-w-0 items-start gap-2">
        <WarningCircle size={17} className="mt-0.5 shrink-0 text-primary" aria-hidden />
        <div className="min-w-0 flex-1">
          <p className="text-xs font-semibold uppercase tracking-[0.08em] text-foreground">
            Next action
          </p>
          <p className="mt-1 break-words text-sm text-foreground">{actionSummary}</p>
          <div className="mt-3">{actionBody}</div>
          {retryMutation.isPending ? (
            <p
              className="mt-3 text-xs leading-5 text-muted-foreground"
              role="status"
              aria-live="polite"
            >
              Retrying provisioning… The server has not confirmed a ready result yet.
            </p>
          ) : null}
          {mutationError ? (
            <div className="mt-3 break-words text-xs text-destructive" role="alert">
              <p>
                {conflict
                  ? 'The setup action was not applied because the current Project authority changed.'
                  : getApiErrorMessage(mutationError, 'The setup action could not be completed.')}
              </p>
              {conflict ? (
                <ConflictDetails
                  error={mutationError}
                  fallbackAuthority="Project execution setup"
                />
              ) : null}
            </div>
          ) : null}
        </div>
      </div>
    </div>
  )
}
