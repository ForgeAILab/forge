import { useTaskFindingsQuery } from '@/api/task-findings'
import { ErrorBanner } from '@/components/error-banner'
import { Badge } from '@/components/ui/badge'
import { Skeleton } from '@/components/ui/skeleton'
import { isTransientApiError } from '@/lib/api-error'
import type { ReviewFindingEntry } from '@/types/generated/bindings/ReviewFindingEntry'

export function FindingDetails({
  finding,
}: {
  finding: Pick<ReviewFindingEntry, 'where' | 'expected' | 'observed'>
}) {
  const fields = [
    ['Where', finding.where],
    ['Expected', finding.expected],
    ['Observed', finding.observed],
  ] as const
  if (!fields.some(([, value]) => value)) return null
  return (
    <details className="mt-2 text-xs">
      <summary className="cursor-pointer rounded-sm text-muted-foreground hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">
        Finding details
      </summary>
      <dl className="mt-2 space-y-2">
        {fields.map(([label, value]) =>
          value ? (
            <div key={label}>
              <dt className="font-medium">{label}</dt>
              <dd className="whitespace-pre-wrap break-words text-muted-foreground">{value}</dd>
            </div>
          ) : null,
        )}
      </dl>
    </details>
  )
}

export function ReviewFindingsLedger({ taskId }: { taskId: string }) {
  const findings = useTaskFindingsQuery(taskId)
  // The server orders ids naturally before its cap; one order everywhere.
  const entries = findings.data?.items ?? []
  return (
    <section
      aria-label="Finding ledger"
      className="min-w-0 space-y-3 rounded-lg border bg-card p-4"
    >
      <h3 className="text-sm font-medium">Finding ledger</h3>
      {findings.isError ? (
        <ErrorBanner
          error={findings.error}
          fallback="Findings failed to load"
          onRetry={() => void findings.refetch()}
          showRetry={isTransientApiError(findings.error)}
        />
      ) : findings.isPending ? (
        <div role="status" aria-label="Loading findings">
          <Skeleton className="h-20 w-full" />
        </div>
      ) : entries.length === 0 ? (
        <p className="text-sm text-muted-foreground">No findings in this review window.</p>
      ) : (
        <div
          role="region"
          aria-label="Finding ledger table"
          tabIndex={0}
          className="overflow-x-auto rounded-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
        >
          <table className="w-full text-left text-xs">
            <caption className="sr-only">Current review-window findings, ordered by id</caption>
            <thead className="border-b text-muted-foreground">
              <tr>
                {['ID', 'Source', 'Severity', 'Status', 'Streak', 'Summary'].map((label) => (
                  <th key={label} scope="col" className="p-2 font-medium">
                    {label}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {entries.map((entry) => (
                <tr key={entry.id} className="border-b last:border-b-0">
                  <th scope="row" className="p-2 align-top font-mono font-medium">
                    {entry.id}
                  </th>
                  <td className="p-2 align-top">{entry.source}</td>
                  <td className="p-2 align-top">{entry.severity}</td>
                  <td className="p-2 align-top">
                    <Badge variant="outline">{entry.status}</Badge>
                  </td>
                  <td className="p-2 align-top">{entry.streak}</td>
                  <td className="min-w-48 p-2 align-top">
                    <p className="break-words">{entry.summary}</p>
                    <p className="mt-1 text-muted-foreground">
                      First seen: attempt {entry.first_attempt} · Last checked: attempt{' '}
                      {entry.last_checked_attempt}
                    </p>
                    <FindingDetails finding={entry} />
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </section>
  )
}
