// Mission Control attention cards and answer controls.

import { useState } from 'react'
import { Link } from '@tanstack/react-router'
import { ArrowUpRight, Clock, WarningCircle } from '@phosphor-icons/react'
import { Button } from '@/components/ui/button'
import { Textarea } from '@/components/ui/textarea'
import { LeadInputAnswer, isLeadInput } from '@/components/lead-input-answer'
import { TaskActionButtons } from '@/components/task-detail/task-action-buttons'
import type { Offer } from '@/types/generated'
import { useMembersQuery, useProjectQuery } from '@/api/hooks'
import { useAuthStore } from '@/stores/auth'
import { useAgentChatTurnsQuery, useRetryAgentChatTurnMutation } from '@/features/agent-chat/hooks'
import {
  useAnswerEscalationMutation,
  useResolveAttentionMutation,
} from '@/features/federation/hooks'
import type { AttentionItem } from '@/features/federation/types'
import { StateBadge } from '@/features/federation/components'
import {
  type ReviewActionAccess,
  attentionTaskId,
  reviewOffers,
  attentionRetryAction,
  attentionTone,
  humanize,
  isInspectRunAction,
  attentionActionLabel,
} from './utils'

function ReviewOwnerActions({ item, access }: { item: AttentionItem; access: ReviewActionAccess }) {
  const taskId = attentionTaskId(item)
  const task = item.details.task as { task_version?: number } | null | undefined
  const recovery = item.details.recovery as { actions?: Offer[] } | null | undefined
  if (!taskId) return null
  return (
    <div className="mt-3 space-y-2">
      <Link
        to="/tasks/$taskId/$tab"
        params={{ taskId, tab: 'review' }}
        className="inline-flex min-h-8 items-center gap-1 rounded-sm text-xs underline underline-offset-2 hover:text-primary focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        Open review
        <ArrowUpRight size={14} aria-hidden />
      </Link>
      {!access.loading && !access.canWrite && item.lifecycle !== 'resolved' ? (
        <p className="text-xs text-muted-foreground">Owner action needed</p>
      ) : null}
      {access.canWrite &&
      item.lifecycle !== 'resolved' &&
      typeof task?.task_version === 'number' &&
      Array.isArray(recovery?.actions) ? (
        <TaskActionButtons
          taskId={taskId}
          version={task.task_version}
          offers={recovery.actions}
          transformOffers={(offers) => reviewOffers(offers, access)}
        />
      ) : null}
    </div>
  )
}

function CiIncidentDetails({ item }: { item: AttentionItem }) {
  if (!['flaky_check', 'base_check_failing'].includes(item.category)) return null
  const details = item.details
  const tasks = Array.isArray(details.task_ids)
    ? details.task_ids.filter((id): id is string => typeof id === 'string')
    : []
  const triage =
    details.triage && typeof details.triage === 'object'
      ? (details.triage as Record<string, unknown>)
      : null
  const failures = Array.isArray(details.base_failures)
    ? details.base_failures.filter((s): s is string => typeof s === 'string')
    : []
  const signature = typeof details.signature === 'string' ? details.signature : ''
  const reason = typeof triage?.reason === 'string' ? triage.reason : ''
  return (
    <div className="mt-3 space-y-2 text-xs">
      {typeof details.command === 'string' ? (
        <code className="block whitespace-pre-wrap break-words">{details.command}</code>
      ) : null}
      {typeof details.count === 'number' ? (
        <p className="text-muted-foreground">
          {details.count} occurrences · {tasks.length} tasks
        </p>
      ) : null}
      {typeof details.base_commit === 'string' ? (
        <p className="break-words text-muted-foreground">
          Base: <code>{details.base_commit}</code>
        </p>
      ) : null}
      {reason ? <p className="break-words text-muted-foreground">{reason}</p> : null}
      {signature ? <p className="break-words font-mono">{signature}</p> : null}
      {failures.length ? (
        <details>
          <summary className="cursor-pointer focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">
            {failures.length} base failures
          </summary>
          <ul className="mt-2 space-y-1">
            {failures.map((failure) => (
              <li className="break-words font-mono" key={failure}>
                {failure}
              </li>
            ))}
          </ul>
        </details>
      ) : null}
      <div className="flex flex-wrap gap-2">
        {tasks.map((taskId) => (
          <Link
            key={taskId}
            to="/tasks/$taskId/$tab"
            params={{ taskId, tab: 'executions' }}
            className="rounded-sm underline underline-offset-2 hover:text-primary focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            aria-label={`Inspect CI for task ${taskId}`}
          >
            Task {taskId.slice(0, 8)}
          </Link>
        ))}
      </div>
    </div>
  )
}

function escalationOf(item: AttentionItem): { id: string; need: string } | null {
  if (item.recommended_action !== 'answer_escalation' || item.scope_type !== 'project') return null
  const id = item.details.escalation_id
  const need = item.details.need
  return typeof id === 'string' && id
    ? { id, need: typeof need === 'string' ? need : item.summary }
    : null
}

/** The owner's answer to a Project Agent escalation; it wakes the Agent. */
function EscalationAnswer({
  item,
  escalation,
}: {
  item: AttentionItem
  escalation: { id: string; need: string }
}) {
  const [answer, setAnswer] = useState('')
  const submit = useAnswerEscalationMutation()
  const inputId = `escalation-answer-${item.id}`
  return (
    <div className="mt-3 space-y-2">
      <p className="whitespace-pre-wrap text-xs leading-5 text-foreground">{escalation.need}</p>
      <label htmlFor={inputId} className="sr-only">
        Answer
      </label>
      <Textarea
        id={inputId}
        value={answer}
        maxLength={4096}
        placeholder="Answer the Project Agent"
        disabled={submit.isPending || submit.isSuccess}
        onChange={(event) => setAnswer(event.target.value)}
      />
      <Button
        variant="outline"
        size="sm"
        disabled={!answer.trim() || submit.isPending || submit.isSuccess}
        onClick={() =>
          submit.mutate({ projectId: item.scope_id, escalationId: escalation.id, answer })
        }
      >
        {submit.isPending ? 'Answering…' : 'Answer'}
      </Button>
      {submit.error ? (
        <p className="text-xs text-destructive" role="alert">
          {submit.error.message}
        </p>
      ) : null}
    </div>
  )
}

type LeadAnswerDraft = { answer: string; position: number }

export type LeadAnswerDrafts = Record<string, LeadAnswerDraft>

export function WithdrawnLeadDraft({
  id,
  answer,
  onDismiss,
}: {
  id: string
  answer: string
  onDismiss: () => void
}) {
  const [copied, setCopied] = useState(false)
  const [copyError, setCopyError] = useState(false)
  return (
    <article
      id={`attention-${id}`}
      className="space-y-2 rounded-lg border border-border-subtle bg-muted p-4"
      role="status"
    >
      <p className="whitespace-pre-wrap break-words text-sm text-muted-foreground">
        This question was withdrawn; your draft: <span className="text-foreground">{answer}</span>
      </p>
      <div className="flex flex-wrap gap-2">
        <Button
          variant="outline"
          size="sm"
          onClick={async () => {
            try {
              await navigator.clipboard.writeText(answer)
              setCopied(true)
              setCopyError(false)
            } catch {
              setCopyError(true)
            }
          }}
        >
          {copied ? 'Copied' : 'Copy draft'}
        </Button>
        <Button variant="outline" size="sm" onClick={onDismiss}>
          Dismiss draft
        </Button>
      </div>
      {copyError ? (
        <p className="text-xs text-muted-foreground">
          Could not copy. Select your draft to copy it.
        </p>
      ) : null}
    </article>
  )
}

type AttentionCardProps = {
  item: AttentionItem
  answer: string
  onAnswerChange: (answer: string) => void
  onAnswered: () => void
}

export function AttentionCard(props: AttentionCardProps) {
  return props.item.category === 'review_needs_owner' ? (
    <ReviewOwnerAttentionCard {...props} />
  ) : (
    <AttentionCardContent {...props} />
  )
}

function ReviewOwnerAttentionCard(props: AttentionCardProps) {
  const user = useAuthStore((state) => state.user)
  const project = useProjectQuery(props.item.scope_id)
  const members = useMembersQuery(props.item.scope_id)
  const member = members.data?.find((entry) => entry.user_id === user?.id)
  const ownerId = project.data?.owner_id
  const loading = project.isLoading || members.isLoading
  const ready = Boolean(
    user && !loading && project.data && !project.isError && members.data && !members.isError,
  )
  // Match ProjectAccess: explicit membership takes precedence over owner_id;
  // legacy ownerless Projects grant owner rights to every signed-in user.
  const isOwner =
    ready && (ownerId === null || (member ? member.role === 'owner' : ownerId === user?.id))
  const canWrite = ready && (isOwner || member?.role === 'admin' || member?.role === 'member')
  return <AttentionCardContent {...props} reviewAccess={{ loading, canWrite, isOwner }} />
}

function AttentionCardContent({
  item,
  answer,
  onAnswerChange,
  onAnswered,
  reviewAccess,
}: AttentionCardProps & { reviewAccess?: ReviewActionAccess }) {
  const canResolve = reviewAccess?.canWrite ?? true
  const isProgressWarning = item.category === 'progress_warning'
  const taskId = isProgressWarning ? attentionTaskId(item) : null
  const retryAction = attentionRetryAction(item)
  const retryTurn = useRetryAgentChatTurnMutation(retryAction?.chat_id)
  const resolve = useResolveAttentionMutation()
  const escalation = escalationOf(item)
  const turns = useAgentChatTurnsQuery(retryAction?.chat_id)
  const anotherTurnLive =
    turns.data?.some((turn) =>
      ['queued', 'leased', 'retry_wait', 'awaiting_input'].includes(turn.status),
    ) ?? false
  return (
    <article
      id={`attention-${item.id}`}
      className={`rounded-lg border p-4 ${attentionTone(item)}`}
      role={isProgressWarning ? 'status' : undefined}
      aria-live={isProgressWarning ? 'polite' : undefined}
    >
      <div className="flex items-start gap-3">
        {isProgressWarning ? (
          <Clock size={18} className="mt-0.5 shrink-0 text-warning" aria-hidden />
        ) : (
          <WarningCircle size={18} className="mt-0.5 shrink-0 text-warning" aria-hidden />
        )}
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            <h3 className="break-words text-sm font-semibold text-foreground">
              {isProgressWarning ? 'Waiting for semantic progress' : item.summary}
            </h3>
            <StateBadge status={item.lifecycle} />
            {canResolve && item.lifecycle !== 'resolved' && !isLeadInput(item) ? (
              <Button
                variant="ghost"
                size="sm"
                className="ml-auto"
                disabled={resolve.isPending || resolve.isSuccess}
                onClick={() => resolve.mutate({ id: item.id, expectedVersion: item.version })}
              >
                {resolve.isPending ? 'Resolving…' : 'Resolve'}
              </Button>
            ) : null}
          </div>
          <p className="mt-1 text-xs leading-5 text-muted-foreground">
            {isProgressWarning ? (
              <>
                <span>{item.summary}</span> · owner health is reported separately
              </>
            ) : (
              humanize(item.category)
            )}
          </p>
          <CiIncidentDetails item={item} />
          {reviewAccess ? <ReviewOwnerActions item={item} access={reviewAccess} /> : null}
          <p className="mt-3 font-mono text-micro text-muted-foreground">
            {humanize(item.scope_type)} · {item.scope_id.slice(0, 8)} · priority {item.priority} ·
            event {item.source_event_id.slice(0, 8)}
          </p>
          {item.recommended_action && item.category !== 'review_needs_owner' ? (
            <div className="mt-3 flex min-w-0 flex-wrap items-center gap-x-2 gap-y-2 text-xs font-medium text-foreground">
              <span>Next: </span>
              {retryAction ? (
                <Button
                  variant="outline"
                  size="sm"
                  disabled={
                    retryTurn.isPending ||
                    retryTurn.isSuccess ||
                    turns.isLoading ||
                    turns.isError ||
                    anotherTurnLive
                  }
                  onClick={() =>
                    retryTurn.mutate({
                      turnId: retryAction.turn_id,
                      input: {
                        expected_version: retryAction.expected_version,
                        idempotency_key: `agent-chat-turn-retry:${retryAction.turn_id}:${retryAction.expected_version}`,
                      },
                    })
                  }
                >
                  {retryTurn.isPending ? 'Retrying…' : 'Retry turn'}
                </Button>
              ) : isProgressWarning && taskId && isInspectRunAction(item.recommended_action) ? (
                <Link
                  to="/tasks/$taskId/$tab"
                  params={{ taskId, tab: 'executions' }}
                  className="inline-flex min-h-8 w-full items-center justify-center gap-1 rounded-md border border-warning/40 bg-card px-2.5 py-1.5 text-xs font-semibold text-foreground transition-colors hover:border-warning hover:bg-warning/10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 sm:w-auto"
                  aria-label="Inspect run"
                >
                  {attentionActionLabel(item.recommended_action)}
                  <ArrowUpRight size={14} aria-hidden />
                </Link>
              ) : (
                <span>{attentionActionLabel(item.recommended_action)}</span>
              )}
            </div>
          ) : null}
          {escalation && item.lifecycle !== 'resolved' ? (
            <EscalationAnswer item={item} escalation={escalation} />
          ) : null}
          {isLeadInput(item) ? (
            <LeadInputAnswer
              item={item}
              answer={answer}
              onAnswerChange={onAnswerChange}
              onAnswered={onAnswered}
            />
          ) : null}
          {retryTurn.error ? (
            <p className="mt-2 text-xs text-destructive" role="alert">
              {retryTurn.error.message}
            </p>
          ) : null}
          {resolve.error ? (
            <p className="mt-2 text-xs text-destructive" role="alert">
              {resolve.error.message}
            </p>
          ) : null}
        </div>
      </div>
    </article>
  )
}
