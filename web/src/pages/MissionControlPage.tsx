import type { ReactNode } from 'react'
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
import { useAgentChatsQuery } from '@/features/agent-chat/hooks'
import type { AgentChatEntry } from '@/features/agent-chat/types'
import { useMissionControlQuery } from '@/features/federation/hooks'
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
  return humanize(value)
}

function isInspectRunAction(value: string | null | undefined): boolean {
  return value?.trim().toLowerCase().replaceAll(' ', '_') === 'inspect_run'
}

function count(value: bigint | number): number {
  return typeof value === 'bigint' ? Number(value) : value
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
      'progress_warning',
    ].includes(item.category)
  ) {
    return 'border-warning/30 bg-warning/5'
  }
  return 'border-border-subtle bg-card'
}

function AttentionCard({ item }: { item: AttentionItem }) {
  const isProgressWarning = item.category === 'progress_warning'
  const taskId = isProgressWarning ? attentionTaskId(item) : null
  return (
    <article
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
          <p className="mt-3 font-mono text-micro text-muted-foreground">
            {humanize(item.scope_type)} · {item.scope_id.slice(0, 8)} · priority {item.priority} ·
            event {item.source_event_id.slice(0, 8)}
          </p>
          {item.recommended_action ? (
            <div className="mt-3 flex min-w-0 flex-wrap items-center gap-x-2 gap-y-2 text-xs font-medium text-foreground">
              <span>Next: </span>
              {isProgressWarning && taskId && isInspectRunAction(item.recommended_action) ? (
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
        <ActivityMetadata label="Actor" value={`${humanize(item.actor_type)} · ${item.actor_id}`} />
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
          <p className="text-muted-foreground">Processed events</p>
          <p className="mt-1 font-mono text-foreground">{health.processed_events}</p>
        </div>
        <div>
          <p className="text-muted-foreground">Last sequence</p>
          <p className="mt-1 font-mono text-foreground">{health.last_sequence}</p>
        </div>
      </div>
      <p className="mt-3 border-t border-border-subtle pt-2 font-mono text-micro text-muted-foreground">
        Last success {formatDate(health.last_success_at)}
        {health.last_error_code ? ` · ${health.last_error_code}` : ''}
      </p>
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
}: {
  data: MissionControlResponse
  chatEntries: AgentChatEntry[]
  chatIsLoading: boolean
  chatIsError: boolean
  onRetryChats: () => void
}) {
  const attention = data.needs_attention
  const humanInput = attention.filter((item) => item.category === 'human_input_required')
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
          <div className="space-y-2 p-3">
            {otherAttention.map((item) => (
              <AttentionCard key={item.id} item={item} />
            ))}
          </div>
        </ProjectionSection>
      ) : null}
      {humanInput.length > 0 ? (
        <ProjectionSection
          title="Questions / human input"
          count={humanInput.length}
          icon={<Question size={16} />}
        >
          <div className="space-y-2 p-3">
            {humanInput.map((item) => (
              <AttentionCard key={item.id} item={item} />
            ))}
          </div>
        </ProjectionSection>
      ) : null}
      {commitments.length > 0 ? (
        <ProjectionSection
          title="Commitment alerts"
          count={commitments.length}
          icon={<Clock size={16} />}
        >
          <div className="space-y-2 p-3">
            {commitments.map((item) => (
              <AttentionCard key={item.id} item={item} />
            ))}
          </div>
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
