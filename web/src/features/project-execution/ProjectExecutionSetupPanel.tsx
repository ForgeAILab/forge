import { useEffect, useMemo, useState } from 'react'
import { ArrowClockwise, CircleNotch, WarningCircle } from '@phosphor-icons/react'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import {
  useAttachPrimaryRepositoryMutation,
  useProjectExecutionSetupQuery,
  useRetryProvisioningMutation,
  useSelectIndependentReviewerMutation,
  useSelectWorkerMutation,
} from './hooks'
import {
  nextSetupAction,
  humanize,
  stateLabel,
  compactStateDescription,
  stateDescription,
  bounded,
  asNumber,
  formatTimestamp,
} from './execution-setup/utils'
import { ReadinessRow } from './execution-setup/readiness'
import { ActionArea } from './execution-setup/actions'

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
                  ? 'Your Project is approved. This optional plan adds traceability and does not start Tasks.'
                  : data.execution_gate === 'active'
                    ? 'Charter-backed Tasks follow their workflow without another implementation approval.'
                    : 'See what Forge still needs before implementation can start.'
                : 'Project Agent coordination and repository setup are independent. The approved Charter authorizes Tasks; an execution baseline is optional traceability.'}
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
            label={compact ? 'Task workflow' : 'Implementation authority'}
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
