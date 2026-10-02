import { useEffect, useState } from 'react'
import { useRecheckProjectEnvironment } from '@/api/hooks'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { getApiErrorMessage } from '@/lib/api-error'
import { SettingsSection } from './SettingsSection'
import type { ProjectEnvironmentReadiness as Readiness } from '@/types/generated/bindings/ProjectEnvironmentReadiness'

function CheckTime({ value, now }: { value: string | null; now: number }) {
  if (!value) return <>—</>
  const date = new Date(value)
  if (!Number.isFinite(date.getTime())) return <>—</>
  const seconds = Math.round((date.getTime() - now) / 1000)
  const magnitude = Math.abs(seconds)
  const [amount, unit] =
    magnitude < 60
      ? ([seconds, 'second'] as const)
      : magnitude < 3600
        ? ([Math.round(seconds / 60), 'minute'] as const)
        : magnitude < 86400
          ? ([Math.round(seconds / 3600), 'hour'] as const)
          : ([Math.round(seconds / 86400), 'day'] as const)
  return (
    <time dateTime={value} title={date.toLocaleString()} className="whitespace-nowrap">
      {new Intl.RelativeTimeFormat(undefined, { numeric: 'auto', style: 'short' }).format(
        amount,
        unit,
      )}
    </time>
  )
}

export function ProjectEnvironmentReadiness({
  projectId,
  rows,
  isLoading = false,
  hasRepository = true,
}: {
  projectId: string
  rows: Readiness[]
  isLoading?: boolean
  hasRepository?: boolean
}) {
  const recheck = useRecheckProjectEnvironment()
  const [tick, setNow] = useState(Date.now)
  // A check that just finished is newer than the last 30-second tick; without
  // this it would read as being in the future.
  const now = rows.reduce((latest, row) => {
    const checked = row.checked_at ? new Date(row.checked_at).getTime() : Number.NaN
    return Number.isFinite(checked) && checked > latest ? checked : latest
  }, tick)
  useEffect(() => {
    if (!rows.length) return
    const timer = window.setInterval(() => setNow(Date.now()), 30_000)
    return () => window.clearInterval(timer)
  }, [rows.length])
  return (
    <SettingsSection
      layout="stacked"
      title="Machine readiness"
      description="Environment checks are recorded separately for each machine."
    >
      {isLoading ? (
        <Skeleton className="h-24 w-full" />
      ) : rows.length === 0 ? (
        <p className="text-sm text-muted-foreground">
          {hasRepository
            ? 'No environment readiness recorded. Machines appear after configured checks run.'
            : 'Environment checks cannot run until a repository is added to this Project.'}
        </p>
      ) : (
        <div
          className="min-w-0 max-w-full overflow-x-auto rounded-md border border-border-subtle focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          tabIndex={0}
          role="region"
          aria-label="Machine readiness table"
        >
          <table className="w-full text-left text-xs" aria-label="Machine environment readiness">
            <thead className="text-muted-foreground">
              <tr>
                {['Machine', 'Status', 'Failing checks', 'Checked', 'Next check', 'Action'].map(
                  (label) => (
                    <th key={label} scope="col" className="whitespace-nowrap p-2 font-medium">
                      {label}
                    </th>
                  ),
                )}
              </tr>
            </thead>
            <tbody>
              {rows.map((row) => (
                <tr key={row.machine.id} className="border-t border-border-subtle align-top">
                  <th scope="row" className="p-2 font-medium">
                    <span className="whitespace-nowrap">{row.machine.name}</span>
                    <span className="mt-1 block whitespace-nowrap text-muted-foreground">
                      {row.scope_covered === 'full' ? 'Full checks' : 'Machine checks'}
                    </span>
                  </th>
                  <td className="p-2">
                    <Badge variant="outline" className="whitespace-nowrap">
                      {row.status.replace('_', ' ')}
                    </Badge>
                  </td>
                  <td className="w-full min-w-40 p-2">
                    {row.failing_checks.length ? (
                      row.failing_checks.map((check) => (
                        <details key={check.name}>
                          <summary className="cursor-pointer rounded text-destructive focus-visible:ring-2 focus-visible:ring-ring">
                            {check.name}
                          </summary>
                          <pre className="max-w-64 whitespace-pre-wrap break-all font-mono">
                            {check.output_tail}
                          </pre>
                        </details>
                      ))
                    ) : row.status === 'not_ready' ? (
                      <p className="whitespace-pre-wrap break-words">
                        {row.output_tail || 'Launch environment failed'}
                      </p>
                    ) : (
                      '—'
                    )}
                  </td>
                  <td className="p-2">
                    <CheckTime value={row.checked_at} now={now} />
                  </td>
                  <td className="p-2">
                    <CheckTime value={row.next_check_at} now={now} />
                  </td>
                  <td className="p-2">
                    <Button
                      className="whitespace-nowrap"
                      size="sm"
                      variant="outline"
                      disabled={recheck.isPending}
                      aria-label={`Check now on ${row.machine.name}`}
                      onClick={() => recheck.mutate({ projectId, machine: row.machine.id })}
                    >
                      {recheck.isPending && recheck.variables?.machine === row.machine.id
                        ? 'Checking…'
                        : 'Check now'}
                    </Button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
      {recheck.isPending ? (
        <p role="status" className="mt-2 text-xs">
          Checking {rows.find((row) => row.machine.id === recheck.variables?.machine)?.machine.name}
          …
        </p>
      ) : null}
      {recheck.isError ? (
        <p role="alert" className="mt-2 text-xs text-destructive">
          {getApiErrorMessage(recheck.error, 'Could not check machine')}
        </p>
      ) : null}
      {recheck.data?.machines.map((machine) =>
        machine.error ? (
          <p key={machine.machine.id} role="alert" className="mt-2 text-xs text-destructive">
            {machine.machine.name}: {machine.error}
          </p>
        ) : (
          <p key={machine.machine.id} role="status" className="mt-2 text-xs">
            {machine.machine.name}:{' '}
            {machine.checks.every((check) => check.passed) ? 'Checks passed' : 'Checks failed'}
          </p>
        ),
      )}
    </SettingsSection>
  )
}
