import type { ReviewConformance } from '@/types/generated/bindings/ReviewConformance'
import { Badge } from '@/components/ui/badge'
import { ChatMarkdown } from '@/components/chat/chat-markdown'
import { FindingDetails } from './ReviewFindingsLedger'
import { TaskEvidencePreview } from '@/components/task-detail/task-comments-panel'

const STATUS_LABELS = {
  not_assessed: 'Not assessed',
  passed: 'Passed',
  failed: 'Failed',
  blocked: 'Blocked by environment',
  unverified: 'Unverified',
} as const

export function ReviewConformancePanel({
  conformance,
  verify,
  taskId,
}: {
  conformance?: ReviewConformance
  verify?: unknown
  taskId?: string
}) {
  const status = conformance?.status ?? 'not_assessed'
  const contract = conformance?.contract
  const usesCurrentTaskScopedPolicy = contract?.policy === 'forge.review-conformance/3'
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
      {status === 'blocked' && (
        <p className="text-sm text-muted-foreground">
          The reviewer could not reach a verdict in its environment. Fix the environment (for
          example add review setup steps), then re-run the review.
        </p>
      )}
      {status === 'not_assessed' && (
        <p className="text-sm text-muted-foreground">
          This review has no recorded Charter assessment. Its review outcome is preserved.
        </p>
      )}
      {conformance?.reason && <p className="break-words text-sm">{conformance.reason}</p>}
      {contract && usesCurrentTaskScopedPolicy && (
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
      {contract && !usesCurrentTaskScopedPolicy && (
        <p className="text-xs text-muted-foreground">
          {contract.policy === 'forge.review-conformance/1' ? (
            <>
              This historical review used the previous whole-Project scope (
              {contract.context.requirements.length}{' '}
              {contract.context.requirements.length === 1 ? 'requirement' : 'requirements'}).{' '}
            </>
          ) : (
            <>
              This review used obsolete conformance policy{' '}
              <code className="font-mono">{contract.policy}</code>. Its result is retained for
              history.{' '}
            </>
          )}
          Run a fresh review to use the current Task-scoped policy.
        </p>
      )}
      {Boolean(conformance?.assessment?.findings?.length) && (
        <section aria-label="Attempt findings" className="space-y-2 border-t pt-3">
          <h4 className="text-sm font-medium">Attempt findings</h4>
          <ul className="space-y-3">
            {conformance?.assessment?.findings?.map((finding, index) => (
              <li key={index} className="text-sm">
                <p className="break-words">
                  <span className="font-mono">
                    {finding.id ?? 'New finding (Forge assigns ID)'}
                  </span>
                  {' · '}
                  {finding.severity ?? 'blocking'}
                  {' · '}
                  {finding.summary}
                </p>
                <FindingDetails
                  finding={{
                    where: finding.where ?? null,
                    expected: finding.expected ?? null,
                    observed: finding.observed ?? null,
                  }}
                />
              </li>
            ))}
          </ul>
        </section>
      )}
      {Boolean(conformance?.assessment?.resolved?.length) && (
        <p className="break-words text-sm">
          Resolved in this attempt: {conformance?.assessment?.resolved?.join(', ')}
        </p>
      )}
      <ReviewVerifyBlock verify={verify} taskId={taskId} />
      {(contract || conformance?.assessment?.report || Boolean(conformance?.checks.length)) && (
        <details className="group text-sm">
          <summary className="cursor-pointer rounded-sm text-muted-foreground hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">
            Review and evidence
          </summary>
          <div className="mt-3 space-y-3">
            {contract && (
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
            )}
            {conformance?.assessment?.report && (
              <div className="min-w-0 border-t pt-3">
                <ChatMarkdown text={conformance.assessment.report} />
              </div>
            )}
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

function record(value: unknown): Record<string, unknown> | undefined {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : undefined
}

function ReviewVerifyBlock({ verify, taskId }: { verify: unknown; taskId?: string }) {
  const results = record(verify)
  if (!results || typeof results.status !== 'string') return null
  const checks = Array.isArray(results.checks)
    ? results.checks.flatMap((value) => {
        const check = record(value)
        return check && typeof check.name === 'string' && typeof check.result === 'string'
          ? [check]
          : []
      })
    : []
  const evidence = [
    ...new Set(
      checks.flatMap((check) =>
        Array.isArray(check.evidence)
          ? check.evidence.filter((id): id is string => typeof id === 'string')
          : [],
      ),
    ),
  ]
  return (
    <section aria-label="Verify results" className="space-y-2 border-t pt-3">
      <h4 className="text-sm font-medium">Verify: {results.status}</h4>
      {typeof results.reason === 'string' && (
        <p className="break-words text-sm">{results.reason}</p>
      )}
      {checks.length > 0 && (
        <ul className="space-y-2 text-sm">
          {checks.map((check, index) => (
            <li key={index} className="break-words">
              {String(check.name)} · {String(check.result)}
              {typeof check.summary === 'string' ? ` — ${check.summary}` : ''}
            </li>
          ))}
        </ul>
      )}
      <p className="text-xs text-muted-foreground">{evidence.length} evidence</p>
      {taskId && evidence.length > 0 && <TaskEvidencePreview taskId={taskId} mediaIds={evidence} />}
    </section>
  )
}
