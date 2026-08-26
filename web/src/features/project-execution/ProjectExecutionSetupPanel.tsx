import { useEffect, useMemo, useRef, useState, type ReactNode } from 'react'
import { Link } from '@tanstack/react-router'
import {
  ArrowClockwise,
  ArrowUpRight,
  CheckCircle,
  CircleNotch,
  GitBranch,
  ShieldCheck,
  UserCircle,
  WarningCircle,
} from '@phosphor-icons/react'

import { useReposQuery } from '@/api/hooks'
import { ConflictDetails } from '@/components/conflict-details'
import { getApiErrorCode, getApiErrorMessage, isApiStatus } from '@/lib/api-error'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import type {
  ExecutionPrincipalResponse,
  ProjectExecutionSetupResponse,
  RetryAction,
} from '@/types/generated'

import { executionBlockerNavTarget } from './executionBlockerNav'
import {
  useAttachPrimaryRepositoryMutation,
  useProjectExecutionSetupQuery,
  useRetryProvisioningMutation,
  useSelectIndependentReviewerMutation,
  useSelectWorkerMutation,
} from './hooks'

const SETUP_ACTIONS = new Set<RetryAction>([
  'refresh_and_retry',
  'select_worker',
  'select_independent_reviewer',
  'attach_repository',
  'retry_provisioning',
])

type SetupAction =
  | 'refresh_and_retry'
  | 'select_worker'
  | 'select_independent_reviewer'
  | 'attach_repository'
  | 'retry_provisioning'

function isSetupAction(action: RetryAction | null | undefined): action is SetupAction {
  return action !== null && action !== undefined && SETUP_ACTIONS.has(action)
}

function asNumber(value: number | bigint): number {
  return typeof value === 'bigint' ? Number(value) : value
}

function humanize(value: string): string {
  return value.replaceAll('_', ' ').replace(/\b\w/g, (letter) => letter.toUpperCase())
}

function bounded(value: string | null | undefined, fallback: string): string {
  const trimmed = value?.trim()
  if (!trimmed) return fallback
  return trimmed.length > 240 ? `${trimmed.slice(0, 237)}…` : trimmed
}

function formatTimestamp(value: string | null | undefined): string | null {
  if (!value) return null
  const date = new Date(value)
  return Number.isNaN(date.getTime())
    ? value
    : date.toLocaleString([], { dateStyle: 'medium', timeStyle: 'short' })
}

function isConflictError(error: unknown): boolean {
  return (
    isApiStatus(error, 409) ||
    isApiStatus(error, 412) ||
    ['version_conflict', 'digest_conflict'].includes(getApiErrorCode(error) ?? '')
  )
}

function createIdempotencyKey(action: string, projectId: string): string {
  if (typeof crypto !== 'undefined' && typeof crypto.randomUUID === 'function') {
    return crypto.randomUUID()
  }
  return `${action}:${projectId}:${Date.now()}:${Math.random().toString(36).slice(2)}`
}

function nextSetupAction(data: ProjectExecutionSetupResponse): SetupAction | null {
  const availability = data.availability
  const refreshRequired =
    data.coordination_state === 'unavailable' ||
    data.execution_setup_state === 'unavailable' ||
    data.execution_gate === 'unavailable' ||
    [availability?.coordination, availability?.execution_setup, availability?.execution_gate].some(
      (status) => status?.availability !== undefined && status.availability !== 'current',
    )
  if (refreshRequired) return 'refresh_and_retry'
  if (isSetupAction(data.next_action)) return data.next_action
  const requirementAction = data.setup_requirements.find((requirement) =>
    isSetupAction(requirement.action),
  )?.action
  return isSetupAction(requirementAction) ? requirementAction : null
}

function stateLabel(value: string): string {
  return humanize(value)
}

function stateDescription(kind: 'coordination' | 'setup' | 'gate', value: string): string {
  if (kind === 'coordination') {
    if (value === 'ready') return 'Project Agent Chat is authorized and ready for planning turns.'
    if (value === 'setup_required') {
      return 'Project Agent Chat still needs its authorized binding; execution setup is shown separately.'
    }
    return 'Project Agent Chat is unavailable; Forge will not admit a turn.'
  }
  if (kind === 'setup') {
    if (value === 'ready') return 'The Project’s required execution setup is satisfied.'
    if (value === 'provisioning') {
      return 'Repository-backed execution is provisioning. No operational success is claimed yet.'
    }
    if (value === 'failed') {
      return 'Provisioning stopped; the saved failure remains visible until a bounded retry succeeds.'
    }
    if (value === 'unavailable') {
      return 'Forge could not verify execution setup. Refresh the readiness projection before acting.'
    }
    return 'Worker, independent reviewer, or primary repository setup is still missing.'
  }
  if (value === 'unavailable') {
    return 'Forge could not verify the execution gate. Refresh the readiness projection before acting.'
  }
  if (value === 'active') return 'An approved execution baseline is active.'
  if (value === 'baseline_approval_required') {
    return 'Execution setup is ready; an execution baseline still needs approval.'
  }
  if (value === 'reconciliation_required') {
    return 'Execution is paused until the recorded reconciliation requirement is resolved.'
  }
  return 'Planning remains available, but execution is read-only until a baseline is active.'
}

function compactStateDescription(kind: 'coordination' | 'setup' | 'gate', value: string): string {
  if (kind === 'coordination' && value === 'ready') {
    return 'The Project Agent can plan and coordinate this Project.'
  }
  if (kind === 'setup' && value === 'ready') {
    return 'The Worker, reviewer, and repository are ready.'
  }
  if (kind === 'gate') {
    if (value === 'active') {
      return 'Approved Tasks can run without another approval.'
    }
    if (value === 'baseline_approval_required') {
      return 'Approve the implementation plan once to start every covered Task.'
    }
    if (value === 'pre_baseline_read_only') {
      return 'The Project Agent still needs to prepare the implementation plan.'
    }
    if (value === 'reconciliation_required') {
      return 'Review plan changes before repository work resumes.'
    }
  }
  return stateDescription(kind, value)
}

function StateIcon({ value }: { value: string }) {
  if (value === 'ready' || value === 'active') {
    return <CheckCircle size={17} className="shrink-0 text-success" aria-hidden />
  }
  if (value === 'provisioning') {
    return (
      <CircleNotch
        size={17}
        className="shrink-0 animate-spin motion-reduce:animate-none text-primary"
        aria-hidden
      />
    )
  }
  if (value === 'failed' || value === 'unavailable' || value === 'reconciliation_required') {
    return <WarningCircle size={17} className="shrink-0 text-warning" aria-hidden />
  }
  return <CircleNotch size={17} className="shrink-0 text-muted-foreground" aria-hidden />
}

function StatePill({ value }: { value: string }) {
  return (
    <span className="inline-flex max-w-full items-center rounded-full border border-border-subtle bg-muted px-2 py-0.5 font-mono text-micro font-semibold uppercase tracking-[0.08em] text-muted-foreground">
      {stateLabel(value)}
    </span>
  )
}

function ReadinessRow({
  label,
  value,
  description,
  compact = false,
}: {
  label: string
  value: string
  description: string
  compact?: boolean
}) {
  return (
    <li
      className={`flex min-w-0 items-start rounded-md border border-border-subtle bg-background px-3 ${compact ? 'gap-2 py-2.5' : 'gap-3 py-3'}`}
    >
      <StateIcon value={value} />
      <div className="min-w-0 flex-1">
        <div className="flex min-w-0 flex-wrap items-center gap-2">
          <p className="text-sm font-semibold text-foreground">{label}</p>
          <StatePill value={value} />
        </div>
        <p className="mt-1 break-words text-xs leading-5 text-muted-foreground">{description}</p>
      </div>
    </li>
  )
}

function PrincipalOption({ principal }: { principal: ExecutionPrincipalResponse }) {
  const details = [principal.provider, principal.model].filter(Boolean).join(' · ')
  return (
    <option value={principal.identity_id}>
      {principal.name}
      {details ? ` · ${details}` : ''}
    </option>
  )
}

function ActionSelect({
  id,
  label,
  value,
  options,
  onChange,
  buttonLabel,
  pending,
  onSubmit,
  icon,
  compact = false,
}: {
  id: string
  label: string
  value: string
  options: ExecutionPrincipalResponse[]
  onChange: (value: string) => void
  buttonLabel: string
  pending: boolean
  onSubmit: () => void
  icon: ReactNode
  compact?: boolean
}) {
  return (
    <div className="min-w-0">
      <label htmlFor={id} className="text-xs font-medium text-foreground">
        {label}
      </label>
      <div
        className={`mt-2 flex min-w-0 flex-col gap-2 ${compact ? '' : 'xl:flex-row xl:items-center'}`}
      >
        <select
          id={id}
          value={value}
          onChange={(event) => onChange(event.target.value)}
          className="min-w-0 flex-1 rounded-md border border-input bg-background px-3 py-2 text-sm text-foreground shadow-xs outline-none transition-colors focus-visible:ring-2 focus-visible:ring-ring"
          aria-describedby={`${id}-hint`}
          disabled={pending}
        >
          <option value="">Select a principal…</option>
          {options.map((principal) => (
            <PrincipalOption key={principal.identity_id} principal={principal} />
          ))}
        </select>
        <Button
          type="button"
          size="sm"
          className={`w-full shrink-0 ${compact ? '' : 'sm:w-auto'}`}
          onClick={onSubmit}
          disabled={!value || pending}
        >
          {pending ? (
            <CircleNotch
              size={14}
              className="animate-spin motion-reduce:animate-none"
              aria-hidden
            />
          ) : (
            icon
          )}
          {pending ? 'Saving…' : buttonLabel}
        </Button>
      </div>
      <p id={`${id}-hint`} className="mt-2 text-xs leading-5 text-muted-foreground">
        Forge checks current eligibility and project version again when you submit.
      </p>
    </div>
  )
}

function RepositorySelect({
  projectId,
  value,
  onChange,
  pending,
  onSubmit,
  compact = false,
}: {
  projectId: string
  value: string
  onChange: (value: string) => void
  pending: boolean
  onSubmit: () => void
  compact?: boolean
}) {
  const reposQuery = useReposQuery(projectId, { enabled: true })
  const repos = reposQuery.data?.items ?? []

  if (reposQuery.isLoading) {
    return (
      <p className="flex items-center gap-2 text-xs text-muted-foreground" role="status">
        <CircleNotch size={14} className="animate-spin motion-reduce:animate-none" aria-hidden />{' '}
        Loading repositories…
      </p>
    )
  }

  if (reposQuery.isError) {
    return (
      <div className="flex min-w-0 flex-wrap items-center gap-3">
        <p className="min-w-0 break-words text-xs text-muted-foreground">
          Forge could not load repositories for this Project.
        </p>
        <Button
          type="button"
          size="sm"
          variant="outline"
          className="w-full sm:w-auto"
          onClick={() => void reposQuery.refetch()}
        >
          <ArrowClockwise size={14} aria-hidden /> Retry repository list
        </Button>
      </div>
    )
  }

  if (repos.length === 0) {
    return (
      <Link
        to="/projects/$projectId/settings"
        params={{ projectId }}
        className="inline-flex w-full max-w-full justify-center items-center gap-1.5 text-xs font-semibold text-primary underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring sm:w-auto"
      >
        Create or attach repository <ArrowUpRight size={13} aria-hidden />
      </Link>
    )
  }

  return (
    <div className="min-w-0">
      <label htmlFor="execution-primary-repository" className="text-xs font-medium text-foreground">
        Primary repository
      </label>
      <div
        className={`mt-2 flex min-w-0 flex-col gap-2 ${compact ? '' : 'xl:flex-row xl:items-center'}`}
      >
        <select
          id="execution-primary-repository"
          value={value}
          onChange={(event) => onChange(event.target.value)}
          className="min-w-0 flex-1 rounded-md border border-input bg-background px-3 py-2 text-sm text-foreground shadow-xs outline-none transition-colors focus-visible:ring-2 focus-visible:ring-ring"
          aria-describedby="execution-primary-repository-hint"
          disabled={pending}
        >
          <option value="">Select a repository…</option>
          {repos.map((repo) => (
            <option key={repo.id} value={repo.id}>
              {repo.name} · {repo.default_branch}
            </option>
          ))}
        </select>
        <Button
          type="button"
          size="sm"
          className={`w-full shrink-0 ${compact ? '' : 'sm:w-auto'}`}
          onClick={onSubmit}
          disabled={!value || pending}
        >
          {pending ? (
            <CircleNotch
              size={14}
              className="animate-spin motion-reduce:animate-none"
              aria-hidden
            />
          ) : (
            <GitBranch size={14} aria-hidden />
          )}
          {pending ? 'Saving…' : 'Attach repository'}
        </Button>
      </div>
      <p
        id="execution-primary-repository-hint"
        className="mt-2 text-xs leading-5 text-muted-foreground"
      >
        The repository is an explicit binding; its presence does not infer Task intent.
      </p>
    </div>
  )
}

function ActionArea({
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
  const reviewerOptions = data.eligible_reviewers.filter(
    (principal) => principal.identity_id !== data.worker?.identity_id,
  )
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
          label="Independent reviewer"
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

    // Every remaining gate (baseline approval, an unstarted implementation
    // plan, or an outstanding reconciliation — including one the Project's
    // execution_gate cannot itself distinguish from the others) routes
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
              ? 'Select an independent reviewer distinct from the Worker.'
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

export function ProjectExecutionSetupPanel({
  projectId,
  compact = false,
}: {
  projectId: string
  compact?: boolean
}) {
  const setupQuery = useProjectExecutionSetupQuery(projectId)
  const workerMutation = useSelectWorkerMutation(projectId)
  const reviewerMutation = useSelectIndependentReviewerMutation(projectId)
  const repositoryMutation = useAttachPrimaryRepositoryMutation(projectId)
  const retryMutation = useRetryProvisioningMutation(projectId)
  const [workerSelection, setWorkerSelection] = useState('')
  const [reviewerSelection, setReviewerSelection] = useState('')
  const [repositorySelection, setRepositorySelection] = useState('')
  const data = setupQuery.data
  const action = useMemo(() => (data ? nextSetupAction(data) : null), [data])

  useEffect(() => {
    if (!data) return
    if (
      data.eligible_workers.length > 0 &&
      !data.eligible_workers.some((item) => item.identity_id === workerSelection)
    ) {
      setWorkerSelection(data.eligible_workers[0]?.identity_id ?? '')
    }
    const reviewers = data.eligible_reviewers.filter(
      (item) => item.identity_id !== data.worker?.identity_id,
    )
    if (reviewers.length > 0 && !reviewers.some((item) => item.identity_id === reviewerSelection)) {
      setReviewerSelection(reviewers[0]?.identity_id ?? '')
    }
  }, [data, reviewerSelection, workerSelection])

  if (setupQuery.isLoading) {
    return (
      <section className="min-w-0" aria-label="Execution readiness">
        <Card
          className="min-w-0 border-border-subtle bg-card p-4 sm:p-5"
          role="status"
          aria-busy="true"
        >
          <div className="flex items-center gap-2 text-sm font-medium text-foreground">
            <CircleNotch
              size={17}
              className="animate-spin motion-reduce:animate-none text-primary"
              aria-hidden
            />
            Loading execution readiness…
          </div>
          <div className={`mt-4 grid gap-2 ${compact ? '' : 'xl:grid-cols-3'}`} aria-hidden>
            <div
              className={`${compact ? 'h-12' : 'h-16'} animate-pulse motion-reduce:animate-none rounded-md bg-muted`}
            />
            <div
              className={`${compact ? 'h-12' : 'h-16'} animate-pulse motion-reduce:animate-none rounded-md bg-muted`}
            />
            <div
              className={`${compact ? 'h-12' : 'h-16'} animate-pulse motion-reduce:animate-none rounded-md bg-muted`}
            />
          </div>
        </Card>
      </section>
    )
  }

  if (setupQuery.isError || !data) {
    return (
      <section className="min-w-0" aria-label="Execution readiness">
        <Card className="min-w-0 border-destructive/30 bg-destructive/5 p-4 sm:p-5" role="alert">
          <div className="flex min-w-0 items-start gap-3">
            <WarningCircle size={18} className="mt-0.5 shrink-0 text-destructive" aria-hidden />
            <div className="min-w-0 flex-1">
              <h2 className="text-sm font-semibold text-foreground">
                Execution readiness unavailable
              </h2>
              <p className="mt-1 break-words text-xs leading-5 text-muted-foreground">
                Forge could not load the setup projection. Existing Project planning remains
                separate and no execution success is shown.
              </p>
              <Button
                type="button"
                size="sm"
                variant="outline"
                className="mt-3 w-full sm:w-auto"
                onClick={() => void setupQuery.refetch()}
              >
                <ArrowClockwise size={14} aria-hidden /> Retry readiness
              </Button>
            </div>
          </div>
        </Card>
      </section>
    )
  }

  const provisioning = data.provisioning
  const requirementSummary = data.setup_requirements.slice(0, 3).map((requirement) => {
    if (requirement.role === 'worker') return 'Worker'
    if (requirement.role === 'independent_reviewer') return 'independent reviewer'
    if (requirement.requirement_type === 'repository') return 'primary repository'
    if (requirement.requirement_type === 'provisioning') return 'provisioning'
    return humanize(requirement.requirement_type)
  })
  const contentSpacing = compact ? 'gap-2' : 'gap-4'
  const headingId = `execution-readiness-${projectId}`

  return (
    <section
      id="project-execution-status"
      className="min-w-0 scroll-mt-4"
      aria-labelledby={headingId}
    >
      <p className="sr-only" role="status" aria-live="polite">
        Execution readiness updated: coordination {stateLabel(data.coordination_state)}, execution
        setup {stateLabel(data.execution_setup_state)}, execution gate{' '}
        {stateLabel(data.execution_gate)}.
      </p>
      <Card
        className={`min-w-0 border-border-subtle bg-card ${compact ? 'p-4' : 'p-4 shadow-sm sm:p-5'}`}
      >
        <div className="flex min-w-0 flex-wrap items-start justify-between gap-3">
          <div className="min-w-0">
            <p className="font-mono text-micro font-semibold uppercase tracking-[0.12em] text-muted-foreground">
              Project execution
            </p>
            <h2
              id={headingId}
              className={`mt-1 break-words font-semibold text-foreground ${compact ? 'text-sm' : 'text-base'}`}
            >
              {compact ? 'Ready to build?' : 'Execution readiness'}
            </h2>
            <p className="mt-1 max-w-3xl break-words text-xs leading-5 text-muted-foreground">
              {compact
                ? data.execution_gate === 'baseline_approval_required'
                  ? 'Your Project is approved. One final approval starts all work covered by the plan.'
                  : data.execution_gate === 'active'
                    ? 'Approved work can run without another Task-by-Task approval.'
                    : 'See what Forge still needs before implementation can start.'
                : 'Coordination, repository setup, and the execution baseline are independent gates. A ready Project Agent Chat does not imply operational execution.'}
            </p>
          </div>
        </div>

        <ul
          aria-label="Execution status gates"
          className={`mt-4 grid min-w-0 ${contentSpacing} ${compact ? '' : 'xl:grid-cols-3'}`}
        >
          <ReadinessRow
            label={compact ? 'Project Agent' : 'Coordination'}
            value={data.coordination_state}
            description={
              compact
                ? compactStateDescription('coordination', data.coordination_state)
                : stateDescription('coordination', data.coordination_state)
            }
            compact={compact}
          />
          <ReadinessRow
            label={compact ? 'Build setup' : 'Execution setup'}
            value={data.execution_setup_state}
            description={
              compact
                ? compactStateDescription('setup', data.execution_setup_state)
                : stateDescription('setup', data.execution_setup_state)
            }
            compact={compact}
          />
          <ReadinessRow
            label={compact ? 'Permission to build' : 'Execution gate'}
            value={data.execution_gate}
            description={
              compact
                ? compactStateDescription('gate', data.execution_gate)
                : stateDescription('gate', data.execution_gate)
            }
            compact={compact}
          />
        </ul>

        {data.execution_setup_state === 'provisioning' && provisioning ? (
          <div
            className="mt-4 rounded-md border border-warning/40 bg-warning/10 p-3"
            role="status"
            aria-live="polite"
          >
            <div className="flex min-w-0 items-start gap-2">
              <CircleNotch
                size={17}
                className="mt-0.5 shrink-0 animate-spin motion-reduce:animate-none text-warning"
                aria-hidden
              />
              <div className="min-w-0">
                <p className="text-xs font-semibold text-foreground">
                  {['retrying', 'retry_wait', 'queued'].includes(provisioning.status.toLowerCase())
                    ? 'Retrying provisioning'
                    : 'Provisioning in progress'}
                </p>
                <p className="mt-1 break-words text-xs leading-5 text-muted-foreground">
                  Checkpoint {bounded(provisioning.current_checkpoint, 'pending')} · attempt{' '}
                  {asNumber(provisioning.attempt_count)} of {asNumber(provisioning.max_attempts)}.
                  The Project is not executable until the server reports ready.
                </p>
                <dl
                  className={`mt-3 grid min-w-0 gap-x-4 gap-y-2 text-micro ${compact ? '' : 'xl:grid-cols-2'}`}
                >
                  <div className="min-w-0">
                    <dt className="font-mono uppercase tracking-[0.08em] text-muted-foreground">
                      Operation
                    </dt>
                    <dd
                      className="mt-1 break-all font-mono text-foreground"
                      title={provisioning.id}
                    >
                      {provisioning.id}
                    </dd>
                  </div>
                  <div className="min-w-0">
                    <dt className="font-mono uppercase tracking-[0.08em] text-muted-foreground">
                      Status
                    </dt>
                    <dd className="mt-1 break-words font-mono text-foreground">
                      {stateLabel(provisioning.status)}
                    </dd>
                  </div>
                  {formatTimestamp(provisioning.next_retry_at) ? (
                    <div className="min-w-0 sm:col-span-2">
                      <dt className="font-mono uppercase tracking-[0.08em] text-muted-foreground">
                        Next retry
                      </dt>
                      <dd className="mt-1 break-words text-foreground">
                        {formatTimestamp(provisioning.next_retry_at)}
                      </dd>
                    </div>
                  ) : null}
                </dl>
              </div>
            </div>
          </div>
        ) : null}

        {data.execution_setup_state === 'failed' && provisioning ? (
          <div
            className="mt-4 rounded-md border border-destructive/30 bg-destructive/5 p-3"
            role="alert"
          >
            <div className="flex min-w-0 items-start gap-2">
              <WarningCircle size={17} className="mt-0.5 shrink-0 text-destructive" aria-hidden />
              <div className="min-w-0">
                <p className="text-xs font-semibold text-foreground">Provisioning failed</p>
                <p className="mt-1 break-words text-xs leading-5 text-muted-foreground">
                  {bounded(
                    provisioning.last_error_message,
                    'The server recorded a provisioning failure without a user-facing detail.',
                  )}
                </p>
              </div>
            </div>
          </div>
        ) : null}

        {requirementSummary.length > 0 ? (
          <p className="mt-4 break-words text-xs leading-5 text-muted-foreground">
            Setup requirements: {requirementSummary.join(' · ')}
            {data.setup_requirements.length > requirementSummary.length
              ? ' · more recorded by Forge'
              : ''}
          </p>
        ) : null}

        <div className="mt-4">
          <ActionArea
            action={action}
            data={data}
            projectId={projectId}
            workerSelection={workerSelection}
            reviewerSelection={reviewerSelection}
            repositorySelection={repositorySelection}
            setWorkerSelection={setWorkerSelection}
            setReviewerSelection={setReviewerSelection}
            setRepositorySelection={setRepositorySelection}
            workerMutation={workerMutation}
            reviewerMutation={reviewerMutation}
            repositoryMutation={repositoryMutation}
            retryMutation={retryMutation}
            onRefresh={() => void setupQuery.refetch()}
            compact={compact}
          />
        </div>
      </Card>
    </section>
  )
}
