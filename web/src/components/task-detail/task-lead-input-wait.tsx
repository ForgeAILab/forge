import { Link } from '@tanstack/react-router'
import { useMissionControlQuery } from '@/features/federation/hooks'
import { isLeadInput, leadQuestion } from '@/components/lead-input-answer'
import { getApiErrorMessage } from '@/lib/api-error'

/** Read the same authoritative question and link to its Mission Control form. */
export function TaskLeadInputWait({ taskId }: { taskId: string }) {
  const query = useMissionControlQuery()
  const item = query.data?.needs_attention.find(
    (item) =>
      isLeadInput(item) &&
      item.lifecycle !== 'resolved' &&
      ((item.scope_type === 'task' && item.scope_id === taskId) || item.details.task_id === taskId),
  )
  return (
    <div className="space-y-2 text-xs">
      <p className="text-muted-foreground">The lead is waiting for your answer.</p>
      {query.isLoading ? <p role="status">Loading the lead’s question…</p> : null}
      {query.isError ? (
        <p role="alert" className="text-destructive">
          {getApiErrorMessage(query.error, 'Could not load the lead’s question')}
        </p>
      ) : item ? (
        <p className="whitespace-pre-wrap break-words text-foreground">
          {leadQuestion(item) ?? 'The lead’s question is unavailable. Refresh Mission Control.'}
        </p>
      ) : !query.isLoading ? (
        <p className="text-muted-foreground">Refresh Mission Control to load the question.</p>
      ) : null}
      <Link
        to="/mission-control"
        hash={item ? `attention-${item.id}` : undefined}
        className="inline-block rounded text-primary underline underline-offset-2 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        Answer the lead in Mission Control
      </Link>
    </div>
  )
}
