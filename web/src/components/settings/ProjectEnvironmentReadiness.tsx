import { useRecheckProjectEnvironment } from '@/api/hooks'
import { Button } from '@/components/ui/button'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { getApiErrorMessage } from '@/lib/api-error'
import { SettingsSection } from './SettingsSection'
import type { ProjectEnvironmentReadiness as Readiness } from '@/types/generated/bindings/ProjectEnvironmentReadiness'

function CheckTime({ value }: { value: string | null }) {
  return value ? <time dateTime={value}>{new Date(value).toLocaleString()}</time> : <>—</>
}

export function ProjectEnvironmentReadiness({
  projectId,
  rows,
  isLoading = false,
}: {
  projectId: string
  rows: Readiness[]
  isLoading?: boolean
}) {
  const recheck = useRecheckProjectEnvironment()
  return (
    <SettingsSection
      title="Machine readiness"
      description="Environment checks are recorded separately for each machine."
    >
      {isLoading ? (
        <Skeleton className="h-24 w-full" />
      ) : rows.length === 0 ? (
        <p className="text-sm text-muted-foreground">
          No environment readiness recorded. Machines appear after configured checks run.
        </p>
      ) : (
        <div className="overflow-x-auto">
          <table className="w-full text-left text-xs" aria-label="Machine environment readiness">
            <thead className="text-muted-foreground">
              <tr>
                {['Machine', 'Status', 'Failing checks', 'Checked', 'Next check', 'Action'].map(
                  (label) => (
                    <th key={label} scope="col" className="p-2 font-medium">
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
                    <span className="break-words">{row.machine.name}</span>
                    <span className="mt-1 block text-muted-foreground">
                      {row.scope_covered === 'full' ? 'Full checks' : 'Machine checks'}
                    </span>
                  </th>
                  <td className="p-2">
                    <Badge variant="outline">{row.status.replace('_', ' ')}</Badge>
                  </td>
                  <td className="min-w-40 max-w-64 p-2">
                    {row.failing_checks.length ? (
                      row.failing_checks.map((check) => (
                        <details key={check.name}>
                          <summary className="cursor-pointer rounded text-destructive focus-visible:ring-2 focus-visible:ring-ring">
                            {check.name}
                          </summary>
                          <pre className="whitespace-pre-wrap break-all font-mono">
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
                    <CheckTime value={row.checked_at} />
                  </td>
                  <td className="p-2">
                    <CheckTime value={row.next_check_at} />
                  </td>
                  <td className="p-2">
                    <Button
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
