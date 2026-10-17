import { useState, type ReactNode } from 'react'
import { Link } from '@tanstack/react-router'
import {
  ArrowClockwise,
  ArrowUpRight,
  Brain,
  CheckCircle,
  Clock,
  Command,
  Gauge,
  Gavel,
  Pulse,
  Question,
  WarningCircle,
} from '@phosphor-icons/react'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Textarea } from '@/components/ui/textarea'
import { LeadInputAnswer, isLeadInput } from '@/components/lead-input-answer'
import { useAgentQuery } from '@/api/hooks'
import {
  useAgentChatsQuery,
  useAgentChatTurnsQuery,
  useRetryAgentChatTurnMutation,
} from '@/features/agent-chat/hooks'
import type { AgentChatEntry } from '@/features/agent-chat/types'
import type { RetryTurnAction } from '@/types/generated/bindings/RetryTurnAction'
import {
  useAnswerEscalationMutation,
  useMissionControlQuery,
  useResolveAttentionMutation,
} from '@/features/federation/hooks'
import type {
  AgentHealthItem,
  AttentionConsumerHealth,
  AttentionItem,
  CoordinationActivityItem,
  MissionControlResponse,
  MissionControlWorkItem,
  OutcomeItem,
} from '@/features/federation/types'
import {
  EmptyPanel,
  ErrorPanel,
  LoadingPanel,
  PageHeader,
  SectionKicker,
  StateBadge,
  StatusDot,
} from '@/features/federation/components'

function formatDate(value: string | null | undefined): string {
  if (!value) return 'No date'
  const date = new Date(value)
  return Number.isNaN(date.getTime())
    ? value
    : date.toLocaleString([], { dateStyle: 'medium', timeStyle: 'short' })
}

function humanize(value: string | null | undefined): string {
  if (!value) return 'Unknown'
  return value.replaceAll('_', ' ').replace(/\b\w/g, (letter) => letter.toUpperCase())
}

function attentionTaskId(item: AttentionItem): string | null {
  if (item.scope_type === 'task' && item.scope_id.trim()) return item.scope_id

  const entityType = item.details?.entity_type
  const entityId = item.details?.entity_id
  if (entityType === 'task' && typeof entityId === 'string' && entityId.trim()) {
    return entityId
  }
  return null
}

function attentionActionLabel(value: string | null | undefined): string {
  const normalized = value?.trim().toLowerCase().replaceAll(' ', '_')
  if (normalized === 'inspect_run') return 'Inspect run'
  if (normalized === 'inspect_ci') return 'Inspect CI'
  return humanize(value)
}

function isInspectRunAction(value: string | null | undefined): boolean {
  return value?.trim().toLowerCase().replaceAll(' ', '_') === 'inspect_run'
}

function count(value: bigint | number): number {
  return typeof value === 'bigint' ? Number(value) : value
}

function attentionRetryAction(
  item: AttentionItem,
): (Omit<RetryTurnAction, 'expected_version'> & { expected_version: number }) | null {
  if (item.lifecycle === 'resolved') return null
  const candidate = item.details.retry_action
  if (!candidate || typeof candidate !== 'object' || Array.isArray(candidate)) return null
  const action = candidate as Record<string, unknown>
  if (
    action.kind !== 'retry_turn' ||
    typeof action.chat_id !== 'string' ||
    typeof action.turn_id !== 'string' ||
    typeof action.expected_version !== 'number' ||
    !Number.isSafeInteger(action.expected_version) ||
    action.expected_version < 0
  ) {
    return null
  }
  return {
    kind: action.kind,
    chat_id: action.chat_id,
    turn_id: action.turn_id,
    expected_version: action.expected_version,
  }
}

function attentionTone(item: AttentionItem): string {
  if (
    [
      'validation_failed',
      'run_stalled',
      'retry_exhausted',
      'execution_failed',
      'runtime_offline',
    ].includes(item.category)
  ) {
    return 'border-destructive/30 bg-destructive/5'
  }
  if (
    [
      'human_input_required',
      'review_risk',
      'budget_threshold',
      'commitment_overdue',
      'conflict_hotspot',
      'progress_warning',
      'flaky_check',
      'base_check_failing',
    ].includes(item.category)
  ) {
    return 'border-warning/30 bg-warning/5'
  }
  return 'border-border-subtle bg-card'
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
type LeadAnswerDrafts = Record<string, LeadAnswerDraft>

function WithdrawnLeadDraft({
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

function AttentionCard({
  item,
  answer,
  onAnswerChange,
  onAnswered,
}: {
  item: AttentionItem
  answer: string
  onAnswerChange: (answer: string) => void
  onAnswered: () => void
}) {
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
            <h3 className="text-sm font-semibold text-foreground">
              {isProgressWarning ? 'Waiting for semantic progress' : item.summary}
            </h3>
            <StateBadge status={item.lifecycle} />
            {item.lifecycle !== 'resolved' && !isLeadInput(item) ? (
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
          <p className="mt-3 font-mono text-micro text-muted-foreground">
            {humanize(item.scope_type)} · {item.scope_id.slice(0, 8)} · priority {item.priority} ·
            event {item.source_event_id.slice(0, 8)}
          </p>
          {item.recommended_action ? (
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

function AgentHealthCard({ item, scopeLabel }: { item: AgentHealthItem; scopeLabel: string }) {
  const status = item.connection_status ?? item.identity_status
  return (
    <article className="rounded-lg border border-border-subtle bg-card p-4">
      <div className="flex items-start justify-between gap-3">
        <div className="flex min-w-0 items-center gap-2">
          <StatusDot status={status} />
          <h3 className="truncate text-sm font-semibold text-foreground">{item.name}</h3>
        </div>
        <StateBadge status={status} />
      </div>
      <p className="mt-2 text-xs font-medium text-primary">{scopeLabel}</p>
      <div className="mt-3 grid gap-2 text-xs sm:grid-cols-2">
        <div>
          <p className="text-muted-foreground">Provider</p>
          <p className="mt-1 truncate font-mono text-foreground">
            {item.provider ?? item.backend_kind ?? 'Native'}
          </p>
        </div>
        <div>
          <p className="text-muted-foreground">Model</p>
          <p className="mt-1 truncate font-mono text-foreground">
            {item.model ?? 'Profile pending'}
          </p>
        </div>
        <div>
          <p className="text-muted-foreground">Sessions</p>
          <p className="mt-1 font-mono text-foreground">{item.active_session_count} active</p>
        </div>
        <div>
          <p className="text-muted-foreground">Project scopes</p>
          <p className="mt-1 font-mono text-foreground">{item.project_count}</p>
        </div>
      </div>
      <p className="mt-3 border-t border-border-subtle pt-2 font-mono text-micro text-muted-foreground">
        {item.paused ? 'Paused' : humanize(item.identity_status)} · last activity{' '}
        {formatDate(item.last_activity_at)}
      </p>
    </article>
  )
}

function BindingScopeRow({ entry }: { entry: AgentChatEntry }) {
  const isMain = entry.kind === 'main'
  const to = isMain ? '/chat' : '/projects/$projectId/chat'
  const label = isMain ? 'Global · Main' : (entry.project_name ?? 'Project Agent')
  const identity = entry.identity_name ?? 'No identity selected'
  const status = entry.binding_state === 'active' ? entry.chat_status : entry.binding_state

  return (
    <Link
      to={to}
      params={isMain ? undefined : { projectId: entry.project_id ?? '' }}
      className="flex min-w-0 items-center justify-between gap-3 px-4 py-3 transition-colors hover:bg-muted/30 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring"
    >
      <div className="min-w-0">
        <p className="truncate text-sm font-medium text-foreground">{label}</p>
        <p className="mt-1 truncate text-xs text-muted-foreground">
          {identity} · {isMain ? 'account-owned timeline' : 'Project-owned timeline'}
        </p>
      </div>
      <div className="flex shrink-0 items-center gap-2">
        {count(entry.pending_turn_count) > 0 ? (
          <span className="font-mono text-micro text-muted-foreground">
            {count(entry.pending_turn_count)} pending
          </span>
        ) : null}
        <StateBadge status={status} label={humanize(status)} />
        <ArrowUpRight size={15} className="text-muted-foreground" aria-hidden />
      </div>
    </Link>
  )
}

function BindingScopes({
  entries,
  isLoading,
  isError,
  onRetry,
}: {
  entries: AgentChatEntry[]
  isLoading: boolean
  isError: boolean
  onRetry: () => void
}) {
  if (isLoading) {
    return (
      <Card className="border-border-subtle bg-card p-4" role="status" aria-live="polite">
        Loading Main and Project Agent bindings…
      </Card>
    )
  }
  if (isError) {
    return (
      <ErrorPanel
        title="Agent binding projection unavailable"
        description="Mission Control could not load the current Main and Project Agent scopes. Retry before relying on the roster."
        onRetry={onRetry}
      />
    )
  }
  if (entries.length === 0) {
    return (
      <EmptyPanel
        title="No bound Agent Chat scopes"
        description="Connect and bind a Main or Project Agent to make its durable timeline visible here. Unbound identities remain in Agent settings."
        icon={<Brain size={19} />}
      />
    )
  }
  return (
    <ProjectionSection
      title="Main and Project Agent bindings"
      count={entries.length}
      icon={<Brain size={16} />}
    >
      <div className="divide-y divide-border-subtle">
        {entries.map((entry) => (
          <BindingScopeRow key={entry.chat_id} entry={entry} />
        ))}
      </div>
    </ProjectionSection>
  )
}

function WorkRow({ item }: { item: MissionControlWorkItem }) {
  return (
    <Link
      to="/tasks/$taskId"
      params={{ taskId: item.task_id }}
      className="flex items-center justify-between gap-3 px-4 py-3 transition-colors hover:bg-muted/30 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring"
    >
      <div className="min-w-0">
        <p className="truncate text-sm font-medium text-foreground">{item.title}</p>
        <p className="mt-1 text-xs text-muted-foreground">
          Project {item.project_id.slice(0, 8)} · {item.primary_action}
        </p>
      </div>
      <div className="flex shrink-0 items-center gap-2">
        <StateBadge status={item.status} />
        <ArrowUpRight size={15} className="text-muted-foreground" aria-hidden />
      </div>
    </Link>
  )
}

function OutcomeRow({ item }: { item: OutcomeItem }) {
  return (
    <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border-subtle px-4 py-3 last:border-b-0">
      <div className="min-w-0">
        <p className="truncate text-sm font-medium text-foreground">{item.title}</p>
        <p className="mt-1 text-xs text-muted-foreground">
          Project {item.project_id.slice(0, 8)} · {item.outcome}
        </p>
      </div>
      <span className="font-mono text-micro text-muted-foreground">
        {formatDate(item.occurred_at)}
      </span>
    </div>
  )
}

function ActivityMetadata({ label, value }: { label: string; value: string }) {
  return (
    <div className="min-w-0">
      <dt className="font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
        {label}
      </dt>
      <dd className="mt-1 min-w-0 break-all font-mono text-micro text-foreground" title={value}>
        {value}
      </dd>
    </div>
  )
}

function CoordinationActivityRow({ item }: { item: CoordinationActivityItem }) {
  const lead = useAgentQuery(item.actor_type === 'task_lead' ? item.actor_id : undefined)
  const actor =
    item.actor_type === 'task_lead'
      ? `Lead · ${lead.data?.name ?? item.actor_id}`
      : `${humanize(item.actor_type)} · ${item.actor_id}`
  const isDirectCommand = item.activity_kind === 'direct_command'
  const kindLabel = isDirectCommand ? 'Direct command receipt' : 'Approval action'
  return (
    <article
      className={`min-w-0 rounded-lg border p-4 ${
        isDirectCommand
          ? 'border-ember-border bg-ember-surface'
          : 'border-border-subtle bg-background'
      }`}
    >
      <div className="flex min-w-0 flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex min-w-0 flex-wrap items-center gap-2">
            <span className="rounded-full border border-border-subtle bg-card px-2 py-0.5 font-mono text-micro font-semibold uppercase tracking-[0.7px] text-muted-foreground">
              {kindLabel}
            </span>
            <StateBadge status={item.status} label={humanize(item.status)} />
            <span className="font-mono text-micro text-muted-foreground">
              policy {humanize(item.policy_result)}
            </span>
          </div>
          <h4 className="mt-2 break-all text-sm font-semibold text-foreground">{item.operation}</h4>
          <p className="mt-1 text-xs leading-5 text-muted-foreground">
            {isDirectCommand
              ? 'Committed through the shared command boundary with a replayable receipt.'
              : 'Authorization remains a separate step from the eventual domain command.'}
          </p>
        </div>
        <time
          dateTime={item.occurred_at}
          className="shrink-0 font-mono text-micro text-muted-foreground"
        >
          {formatDate(item.occurred_at)}
        </time>
      </div>
      <dl className="mt-4 grid min-w-0 gap-x-4 gap-y-3 border-t border-border-subtle pt-3 xl:grid-cols-4">
        <ActivityMetadata label="Actor" value={actor} />
        <ActivityMetadata label="Scope" value={`${humanize(item.scope_type)} · ${item.scope_id}`} />
        <ActivityMetadata label="Input digest" value={item.input_digest} />
        <ActivityMetadata label="Correlation" value={item.correlation_id} />
      </dl>
      {item.outcome ? (
        <p className="mt-3 border-t border-border-subtle pt-2 font-mono text-micro text-muted-foreground">
          Outcome recorded; payload details are withheld from Mission Control.
        </p>
      ) : null}
    </article>
  )
}

function CoordinationActivityGroup({
  id,
  title,
  description,
  items,
  icon,
  emptyLabel,
}: {
  id: string
  title: string
  description: string
  items: CoordinationActivityItem[]
  icon: ReactNode
  emptyLabel: string
}) {
  return (
    <section
      aria-labelledby={id}
      className="min-w-0 rounded-lg border border-border-subtle bg-card p-3 sm:p-4"
    >
      <div className="flex min-w-0 flex-wrap items-start justify-between gap-3">
        <div className="flex min-w-0 items-start gap-2">
          <span className="mt-0.5 shrink-0 text-primary" aria-hidden>
            {icon}
          </span>
          <div className="min-w-0">
            <h3 id={id} className="text-sm font-semibold text-foreground">
              {title}
            </h3>
            <p className="mt-1 max-w-2xl text-xs leading-5 text-muted-foreground">{description}</p>
          </div>
        </div>
        <span className="rounded-full bg-muted px-2 py-0.5 font-mono text-micro text-muted-foreground">
          {items.length}
        </span>
      </div>
      {items.length > 0 ? (
        <div className="mt-3 space-y-2">
          {items.map((item) => (
            <CoordinationActivityRow key={item.id} item={item} />
          ))}
        </div>
      ) : (
        <p className="mt-3 rounded-md border border-dashed border-border-subtle bg-muted/30 px-3 py-3 text-xs text-muted-foreground">
          {emptyLabel}
        </p>
      )}
    </section>
  )
}

function CoordinationActivitySection({
  items,
  allScopesQuiet,
}: {
  items: CoordinationActivityItem[]
  allScopesQuiet: boolean
}) {
  const directCommands = items.filter((item) => item.activity_kind === 'direct_command')
  const approvalActions = items.filter((item) => item.activity_kind === 'approval_action')

  return (
    <ProjectionSection
      title="Coordination activity"
      count={items.length}
      icon={<Command size={16} />}
    >
      {items.length === 0 ? (
        <div className="p-4" role="status" aria-live="polite">
          <p className="text-sm font-medium text-foreground">No coordination activity recorded</p>
          <p className="mt-1 max-w-2xl text-xs leading-5 text-muted-foreground">
            {allScopesQuiet
              ? 'All scopes are quiet. Durable direct-command receipts and approval actions will appear here after the server commits them.'
              : 'No durable direct-command receipts or approval actions are currently projected.'}
          </p>
        </div>
      ) : (
        <div className="space-y-4 p-3 sm:p-4">
          <CoordinationActivityGroup
            id="mission-control-direct-command-receipts"
            title="Durable direct-command receipts"
            description="Commands admitted by policy and committed with a durable, replayable receipt."
            items={directCommands}
            icon={<CheckCircle size={16} />}
            emptyLabel="No direct-command receipts are currently projected."
          />
          <CoordinationActivityGroup
            id="mission-control-approval-actions"
            title="Pending and approved approval actions"
            description="Approval-required operations retain their source provenance; approval does not apply the domain effect by itself."
            items={approvalActions}
            icon={<Gavel size={16} />}
            emptyLabel="No pending or approved approval actions are currently projected."
          />
        </div>
      )}
    </ProjectionSection>
  )
}

function Capacity({ data }: { data: MissionControlResponse }) {
  const capacity = data.capacity
  return (
    <Card className="border-border-subtle bg-card p-4">
      <div className="flex items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <Gauge size={17} className="text-primary" aria-hidden />
          <SectionKicker>Runtime capacity</SectionKicker>
        </div>
        <StateBadge
          status={capacity.healthy ? 'healthy' : 'attention'}
          label={capacity.healthy ? 'Healthy' : 'Attention'}
        />
      </div>
      <div className="mt-3 grid grid-cols-3 gap-3">
        <div>
          <p className="font-mono text-xl font-semibold tabular-nums text-foreground">
            {capacity.active_executions}
          </p>
          <p className="text-micro text-muted-foreground">executions</p>
        </div>
        <div>
          <p className="font-mono text-xl font-semibold tabular-nums text-foreground">
            {capacity.queued_tasks}
          </p>
          <p className="text-micro text-muted-foreground">queued</p>
        </div>
        <div>
          <p className="font-mono text-xl font-semibold tabular-nums text-foreground">
            {capacity.active_sessions}
          </p>
          <p className="text-micro text-muted-foreground">sessions</p>
        </div>
      </div>
    </Card>
  )
}

function ConsumerHealth({ health }: { health: AttentionConsumerHealth | null }) {
  if (!health)
    return (
      <Card className="border-border-subtle bg-card p-4">
        <div className="flex items-center gap-2">
          <Pulse size={17} className="text-muted-foreground" aria-hidden />
          <SectionKicker>Projection consumer</SectionKicker>
        </div>
        <p className="mt-3 text-xs text-muted-foreground">
          No consumer health projection returned.
        </p>
      </Card>
    )
  const status = health.stale || health.last_error_code ? 'attention' : 'healthy'
  const label = health.stale ? 'Stale' : health.last_error_code ? 'Attention' : 'Healthy'
  return (
    <Card className="border-border-subtle bg-card p-4">
      <div className="flex items-center justify-between gap-3">
        <div className="flex items-center gap-2">
          <Pulse size={17} className="text-primary" aria-hidden />
          <SectionKicker>Projection consumer</SectionKicker>
        </div>
        <StateBadge status={status} label={label} />
      </div>
      <p className="mt-3 text-sm font-medium text-foreground">{health.consumer_name}</p>
      <div className="mt-2 grid gap-2 text-xs sm:grid-cols-2">
        <div>
          <p className="text-muted-foreground">Last sequence</p>
          <p className="mt-1 font-mono text-foreground">{health.last_sequence}</p>
        </div>
      </div>
      <p className="mt-3 border-t border-border-subtle pt-2 font-mono text-micro text-muted-foreground">
        Last success {formatDate(health.last_success_at)}
        {health.last_error_code ? ` · ${health.last_error_code}` : ''}
      </p>
      {health.last_error_message ? (
        <p className="mt-2 text-xs text-muted-foreground">{health.last_error_message}</p>
      ) : null}
    </Card>
  )
}

function ProjectionSection({
  title,
  count,
  children,
  icon,
}: {
  title: string
  count: number
  children: ReactNode
  icon: ReactNode
}) {
  return (
    <section className="overflow-hidden rounded-xl border border-border-subtle bg-card shadow-card">
      <header className="flex items-center justify-between gap-3 border-b border-border-subtle px-4 py-3">
        <div className="flex items-center gap-2">
          <span className="text-primary">{icon}</span>
          <h2 className="font-mono text-micro font-semibold uppercase tracking-[0.9px] text-muted-foreground">
            {title}
          </h2>
        </div>
        <span className="rounded-full bg-muted px-2 py-0.5 font-mono text-micro text-muted-foreground">
          {count}
        </span>
      </header>
      {children}
    </section>
  )
}

function MissionContent({
  data,
  chatEntries,
  chatIsLoading,
  chatIsError,
  onRetryChats,
  leadDrafts,
  onLeadDraftChange,
  onDismissLeadDraft,
}: {
  data: MissionControlResponse
  chatEntries: AgentChatEntry[]
  chatIsLoading: boolean
  chatIsError: boolean
  onRetryChats: () => void
  leadDrafts: LeadAnswerDrafts
  onLeadDraftChange: (id: string, answer: string, position: number) => void
  onDismissLeadDraft: (id: string) => void
}) {
  const attention = data.needs_attention
  const humanInput = attention.filter((item) => item.category === 'human_input_required')
  const humanInputRows: { id: string; item: AttentionItem | null }[] = humanInput.map((item) => ({
    id: item.id,
    item,
  }))
  const attentionIds = new Set(attention.map((item) => item.id))
  const withdrawnDrafts = Object.entries(leadDrafts)
    .filter(([id, draft]) => !attentionIds.has(id) && draft.answer.trim())
    .sort(([, a], [, b]) => a.position - b.position)
  for (const [id, draft] of withdrawnDrafts) {
    humanInputRows.splice(draft.position, 0, { id, item: null })
  }
  const renderAttentionCard = (item: AttentionItem, position: number) => (
    <AttentionCard
      key={item.id}
      item={item}
      answer={leadDrafts[item.id]?.answer ?? ''}
      onAnswerChange={(answer) => onLeadDraftChange(item.id, answer, position)}
      onAnswered={() => onDismissLeadDraft(item.id)}
    />
  )
  const commitments = attention.filter((item) => item.category === 'commitment_overdue')
  const otherAttention = attention.filter(
    (item) => item.category !== 'human_input_required' && item.category !== 'commitment_overdue',
  )
  const boundIdentityIds = new Set(
    chatEntries.flatMap((entry) => (entry.identity_id ? [entry.identity_id] : [])),
  )
  const relevantAgentHealth = data.agent_health.filter(
    (item) => boundIdentityIds.has(item.identity_id) || item.active_session_count > 0,
  )
  const scopeForIdentity = new Map(
    chatEntries
      .filter((entry) => entry.identity_id)
      .map((entry) => [
        entry.identity_id as string,
        entry.kind === 'main' ? 'Main Agent' : `${entry.project_name ?? 'Project'} Agent`,
      ]),
  )
  const coordinationActivity = data.coordination_activity
  const hasAny =
    attention.length +
      withdrawnDrafts.length +
      data.review_ready.length +
      data.active_work.length +
      relevantAgentHealth.length +
      data.recent_outcomes.length +
      coordinationActivity.length >
    0

  return (
    <div className="space-y-5">
      <BindingScopes
        entries={chatEntries}
        isLoading={chatIsLoading}
        isError={chatIsError}
        onRetry={onRetryChats}
      />
      <div className="grid gap-4 md:grid-cols-2 xl:grid-cols-4">
        <Card className="border-border-subtle bg-card p-4">
          <SectionKicker>Needs attention</SectionKicker>
          <p className="mt-2 font-mono text-2xl font-semibold text-foreground">
            {attention.length}
          </p>
          <p className="mt-1 text-xs text-muted-foreground">
            Server-authored actionable conditions
          </p>
        </Card>
        <Card className="border-border-subtle bg-card p-4">
          <SectionKicker>Review ready</SectionKicker>
          <p className="mt-2 font-mono text-2xl font-semibold text-foreground">
            {data.review_ready.length}
          </p>
          <p className="mt-1 text-xs text-muted-foreground">Work awaiting review</p>
        </Card>
        <Card className="border-border-subtle bg-card p-4">
          <SectionKicker>Active work</SectionKicker>
          <p className="mt-2 font-mono text-2xl font-semibold text-foreground">
            {data.active_work.length}
          </p>
          <p className="mt-1 text-xs text-muted-foreground">Current assigned execution</p>
        </Card>
        <Capacity data={data} />
      </div>
      <div className="grid gap-5 xl:grid-cols-2">
        <ConsumerHealth health={data.consumer_health} />
        {relevantAgentHealth.length > 0 ? (
          <ProjectionSection
            title="Relevant Task agent health"
            count={relevantAgentHealth.length}
            icon={<Pulse size={16} />}
          >
            <div className="grid gap-2 p-3 md:grid-cols-2">
              {relevantAgentHealth.map((item) => (
                <AgentHealthCard
                  key={item.identity_id}
                  item={item}
                  scopeLabel={scopeForIdentity.get(item.identity_id) ?? 'Relevant Task agent'}
                />
              ))}
            </div>
          </ProjectionSection>
        ) : null}
      </div>
      <CoordinationActivitySection items={coordinationActivity} allScopesQuiet={!hasAny} />
      {otherAttention.length > 0 ? (
        <ProjectionSection
          title="Needs attention"
          count={otherAttention.length}
          icon={<WarningCircle size={16} />}
        >
          <div className="space-y-2 p-3">{otherAttention.map(renderAttentionCard)}</div>
        </ProjectionSection>
      ) : null}
      {humanInputRows.length > 0 ? (
        <ProjectionSection
          title="Questions / human input"
          count={humanInput.length}
          icon={<Question size={16} />}
        >
          <div className="space-y-2 p-3">
            {humanInputRows.map((row, position) =>
              row.item ? (
                renderAttentionCard(row.item, position)
              ) : (
                <WithdrawnLeadDraft
                  key={row.id}
                  id={row.id}
                  answer={leadDrafts[row.id].answer}
                  onDismiss={() => onDismissLeadDraft(row.id)}
                />
              ),
            )}
          </div>
        </ProjectionSection>
      ) : null}
      {commitments.length > 0 ? (
        <ProjectionSection
          title="Commitment alerts"
          count={commitments.length}
          icon={<Clock size={16} />}
        >
          <div className="space-y-2 p-3">{commitments.map(renderAttentionCard)}</div>
        </ProjectionSection>
      ) : null}
      {data.review_ready.length > 0 ? (
        <ProjectionSection
          title="Review-ready work"
          count={data.review_ready.length}
          icon={<CheckCircle size={16} />}
        >
          <div className="divide-y divide-border-subtle">
            {data.review_ready.map((item) => (
              <WorkRow key={item.task_id} item={item} />
            ))}
          </div>
        </ProjectionSection>
      ) : null}
      {data.active_work.length > 0 ? (
        <ProjectionSection
          title="Active work"
          count={data.active_work.length}
          icon={<Pulse size={16} />}
        >
          <div className="divide-y divide-border-subtle">
            {data.active_work.map((item) => (
              <WorkRow key={item.task_id} item={item} />
            ))}
          </div>
        </ProjectionSection>
      ) : null}
      {data.recent_outcomes.length > 0 ? (
        <ProjectionSection
          title="Recent outcomes"
          count={data.recent_outcomes.length}
          icon={<CheckCircle size={16} />}
        >
          <div>
            {data.recent_outcomes.map((item) => (
              <OutcomeRow key={`${item.task_id}:${item.occurred_at}`} item={item} />
            ))}
          </div>
        </ProjectionSection>
      ) : null}
    </div>
  )
}

export function MissionControlPage() {
  const query = useMissionControlQuery()
  const chatsQuery = useAgentChatsQuery()
  const [leadDrafts, setLeadDrafts] = useState<LeadAnswerDrafts>({})
  const dismissLeadDraft = (id: string) => {
    setLeadDrafts((drafts) => {
      const next = { ...drafts }
      delete next[id]
      return next
    })
  }
  const computedAt = query.data?.computed_at ? Date.parse(query.data.computed_at) : Number.NaN
  const isStale = Boolean(
    query.data &&
    (query.data.consumer_health?.stale ||
      !Number.isFinite(computedAt) ||
      Date.now() - computedAt > 60_000),
  )
  return (
    <div className="min-h-full space-y-6 p-5 lg:p-8">
      <PageHeader
        eyebrow="Mission Control"
        title="What needs your attention?"
        description="A read-only operational projection across authorized account and project scopes."
        actions={
          <>
            <Button
              variant="outline"
              size="sm"
              onClick={() => {
                void query.refetch()
                void chatsQuery.refetch()
              }}
              disabled={query.isFetching || chatsQuery.isFetching}
            >
              <ArrowClockwise
                size={14}
                className={query.isFetching || chatsQuery.isFetching ? 'animate-spin' : ''}
                aria-hidden
              />
              Refresh
            </Button>
            <Link
              to="/agents"
              className="inline-flex h-8 items-center gap-1.5 rounded-md border border-input bg-card px-3 text-xs font-medium text-foreground transition-colors hover:bg-muted focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              <Brain size={14} aria-hidden />
              Agent settings
            </Link>
          </>
        }
      />
      {isStale ? (
        <div
          className="flex items-center gap-2 rounded-md border border-warning/30 bg-warning/10 px-3 py-2 text-xs text-foreground"
          role="status"
        >
          <WarningCircle size={15} className="text-warning" aria-hidden />
          Projection is stale. Refreshing from authoritative records may take a moment.
        </div>
      ) : null}
      {query.isLoading ? <LoadingPanel label="Loading Mission Control projection" /> : null}
      {query.isError ? (
        <ErrorPanel
          title="Mission Control projection unavailable"
          description="The attention read model is unavailable. No client-side state is synthesized from raw events."
          onRetry={() => void query.refetch()}
        />
      ) : null}
      {query.data ? (
        <MissionContent
          data={query.data}
          chatEntries={chatsQuery.data?.items ?? []}
          chatIsLoading={chatsQuery.isLoading}
          chatIsError={chatsQuery.isError}
          onRetryChats={() => void chatsQuery.refetch()}
          leadDrafts={leadDrafts}
          onLeadDraftChange={(id, answer, position) =>
            setLeadDrafts((drafts) => ({ ...drafts, [id]: { answer, position } }))
          }
          onDismissLeadDraft={dismissLeadDraft}
        />
      ) : null}
      {query.data ? (
        <p className="flex items-center gap-2 font-mono text-micro text-muted-foreground">
          <Clock size={12} aria-hidden />
          Computed {formatDate(query.data.computed_at)} · projections refresh from committed events
        </p>
      ) : null}
    </div>
  )
}
