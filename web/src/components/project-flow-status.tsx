import { useEffect, useState } from 'react'
import { useProjectQuery, useRecheckProjectEnvironment } from '@/api/hooks'
import { Button } from '@/components/ui/button'
import { getApiErrorMessage } from '@/lib/api-error'
import type { Project } from '@/types/generated'
import type { ProjectSlots } from '@/types/generated/bindings/ProjectSlots'

export function ProjectSlotUsage({ slots }: { slots: ProjectSlots }) {
  return (
    <div className="space-y-1 text-xs text-muted-foreground">
      <p>
        Active {slots.active}
        {slots.limit === 0 ? ' (no limit)' : `/${slots.limit}`} · Parked {slots.parked} · Queued{' '}
        {slots.queued}
      </p>
      {slots.limit > 0 && slots.parked >= 2 * slots.limit ? (
        <p className="font-medium text-warning">waiting on you: {slots.parked} parked</p>
      ) : null}
    </div>
  )
}

export function ProjectEnvironmentPauseNotice({ project }: { project: Project }) {
  const recheck = useRecheckProjectEnvironment()
  const [now, setNow] = useState(Date.now)
  const pause = project.environment_pause

  useEffect(() => {
    if (project.system_pause_reason !== 'environment_not_ready') return
    const timer = window.setInterval(() => setNow(Date.now()), 30_000)
    return () => window.clearInterval(timer)
  }, [project.system_pause_reason])

  if (
    project.system_pause_reason !== 'environment_not_ready' &&
    !recheck.data &&
    !recheck.isError
  ) {
    return null
  }

  const nextCheckAt = pause ? Date.parse(pause.next_check_at) : NaN
  const minutes = Math.max(0, Math.ceil((nextCheckAt - now) / 60_000))

  return (
    <section className="min-w-0 space-y-2 rounded-lg border border-warning/30 bg-warning/10 p-3">
      {project.system_pause_reason === 'environment_not_ready' ? (
        <div className="flex flex-wrap items-center justify-between gap-2">
          <p className="break-words text-xs" role="status">
            <span className="font-semibold">Environment paused</span>
            {pause?.checks.length ? ` · ${pause.checks.join(', ')}` : ''}
            {Number.isFinite(nextCheckAt) ? (
              <span title={new Date(nextCheckAt).toLocaleString()}>
                {minutes > 0 ? ` · next check in ${minutes}m` : ' · next check due now'}
              </span>
            ) : null}
          </p>
          <Button
            size="sm"
            variant="outline"
            disabled={recheck.isPending}
            onClick={() => recheck.mutate(project.id)}
          >
            {recheck.isPending ? 'Checking…' : 'Check now'}
          </Button>
        </div>
      ) : null}
      {pause?.output && project.system_pause_reason === 'environment_not_ready' ? (
        <details className="text-xs">
          <summary className="cursor-pointer rounded focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">
            Output tail
          </summary>
          <pre className="mt-2 max-h-36 overflow-auto whitespace-pre-wrap break-all rounded bg-muted p-2 font-mono">
            {pause.output}
          </pre>
        </details>
      ) : null}
      {recheck.isError ? (
        <p role="alert" className="break-words text-xs text-destructive">
          {getApiErrorMessage(recheck.error, 'Environment check failed')}
        </p>
      ) : null}
      {recheck.data ? (
        <div className="space-y-2 text-xs" role="status">
          <p>
            {recheck.data.project.system_pause_reason === 'environment_not_ready'
              ? 'Environment is still paused.'
              : recheck.data.project.paused
                ? 'Checks finished. Project remains paused.'
                : recheck.data.checks.some((check) => !check.passed)
                  ? 'Some environment checks failed.'
                  : 'Checks passed. Project is resumed.'}
          </p>
          {recheck.data.checks.map((check) => (
            <div key={check.name}>
              <p className={check.passed ? 'text-success' : 'text-destructive'}>
                {check.name}: {check.passed ? 'Passed' : 'Failed'}
                {check.exit_code !== null ? ` (exit ${check.exit_code})` : ''}
              </p>
              {check.output_tail ? (
                <details>
                  <summary className="cursor-pointer rounded focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">
                    {check.name} output
                  </summary>
                  <pre className="mt-2 max-h-36 overflow-auto whitespace-pre-wrap break-all rounded bg-muted p-2 font-mono">
                    {check.output_tail}
                  </pre>
                </details>
              ) : null}
            </div>
          ))}
        </div>
      ) : null}
    </section>
  )
}

export function ProjectFlowHeader({ projectId }: { projectId: string }) {
  const { data: project } = useProjectQuery(projectId)
  if (!project) return null
  return (
    <div className="min-w-0 shrink-0 space-y-2">
      <ProjectSlotUsage slots={project.slots} />
      <ProjectEnvironmentPauseNotice key={project.id} project={project} />
    </div>
  )
}
