import type { ReviewConformance } from '@/types/generated/bindings/ReviewConformance'
import { Badge } from '@/components/ui/badge'

const STATUS_LABELS = {
  not_assessed: 'Not assessed',
  passed: 'Passed',
  failed: 'Failed',
  unverified: 'Unverified',
} as const

export function ReviewConformancePanel({ conformance }: { conformance?: ReviewConformance }) {
  const status = conformance?.status ?? 'not_assessed'
  const contract = conformance?.contract
  const usesTaskScopedPolicy = contract?.policy === 'forge.review-conformance/2'
  return (
    <section
      aria-label="Task review conformance"
      className="min-w-0 space-y-3 rounded-lg border bg-card p-4"
    >
      <div className="flex flex-wrap items-center justify-between gap-2">
        <h3 className="text-sm font-medium">
          {contract && !contract.context.charter_revision_id
            ? 'Task conformance (no approved Charter)'
            : 'Task conformance to Charter'}
        </h3>
        <Badge variant="outline">{STATUS_LABELS[status]}</Badge>
      </div>
      {status === 'not_assessed' && (
        <p className="text-sm text-muted-foreground">
          This review has no recorded Charter assessment. Its review outcome is preserved.
        </p>
      )}
      {conformance?.reason && <p className="break-words text-sm">{conformance.reason}</p>}
      {contract && usesTaskScopedPolicy && (
        <p className="text-xs text-muted-foreground">
          This review covered {contract.context.requirements.length} Task-scoped{' '}
          {contract.context.requirements.length === 1 ? 'requirement' : 'requirements'}.
          {contract.context.deferred_requirement_count > 0 && (
            <>
              {' '}
              {contract.context.deferred_requirement_count} Project{' '}
              {contract.context.deferred_requirement_count === 1
                ? 'requirement remains'
                : 'requirements remain'}{' '}
              for milestone readiness.
            </>
          )}
        </p>
      )}
      {contract && !usesTaskScopedPolicy && (
        <p className="text-xs text-muted-foreground">
          This historical review used the previous whole-Project scope (
          {contract.context.requirements.length}{' '}
          {contract.context.requirements.length === 1 ? 'requirement' : 'requirements'}). Run a
          fresh review to use Task-scoped v2.
        </p>
      )}
      {contract && (
        <details className="group text-sm">
          <summary className="cursor-pointer rounded-sm text-muted-foreground hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">
            Requirements and evidence
          </summary>
          <div className="mt-3 space-y-3">
            <dl className="space-y-1 text-xs">
              <div>
                <dt className="font-medium">Charter revision</dt>
                <dd className="break-all font-mono">
                  {contract.context.charter_revision_id ?? 'Legacy project — no approved Charter'}
                </dd>
              </div>
              <div>
                <dt className="font-medium">Reviewed commit</dt>
                <dd className="break-all font-mono">{contract.commit_sha}</dd>
              </div>
              <div>
                <dt className="font-medium">Contract</dt>
                <dd className="break-all font-mono">{contract.digest}</dd>
              </div>
            </dl>
            {conformance?.assessment?.requirements.map((item) => {
              const requirement = contract.context.requirements.find(
                (r) => r.id === item.requirement_id,
              )
              return (
                <div key={item.requirement_id} className="space-y-1 border-t pt-3">
                  <p className="break-words font-medium">
                    {requirement?.text ?? item.requirement_id}
                  </p>
                  <p className="text-xs text-muted-foreground">
                    {item.disposition.replaceAll('_', ' ')}
                  </p>
                  <p className="break-words">{item.rationale}</p>
                  <ul className="space-y-1 text-xs text-muted-foreground">
                    {item.evidence.map((evidence) => (
                      <li
                        key={
                          evidence.kind === 'file'
                            ? `${evidence.commit_sha}:${evidence.path}:${evidence.start_line}:${evidence.end_line}`
                            : evidence.check_id
                        }
                        className="break-all font-mono"
                      >
                        {evidence.kind === 'file'
                          ? `${evidence.path}:${evidence.start_line}–${evidence.end_line} @ ${evidence.commit_sha.slice(0, 12)}`
                          : `Check: ${evidence.check_id}`}
                      </li>
                    ))}
                  </ul>
                </div>
              )
            })}
            {conformance?.assessment?.findings.map((finding) => (
              <div
                key={`${finding.blocking}:${finding.expected}:${finding.actual}`}
                className="space-y-1 border-t pt-3"
              >
                <p className="font-medium">{finding.blocking ? 'Blocking finding' : 'Finding'}</p>
                <p className="break-words">Expected: {finding.expected}</p>
                <p className="break-words">Actual: {finding.actual}</p>
              </div>
            ))}
            {conformance?.checks.map((check) => (
              <details key={check.check_id} className="border-t pt-3">
                <summary className="cursor-pointer break-words">
                  {check.check_id} · exit {check.exit_code}
                </summary>
                <pre className="mt-2 whitespace-pre-wrap break-all rounded-md bg-muted p-2 text-xs">
                  {check.command}
                  {'\n'}
                  {check.output}
                </pre>
              </details>
            ))}
          </div>
        </details>
      )}
    </section>
  )
}
