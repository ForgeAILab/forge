// Mission Control Agent, binding, capacity, and consumer health sections.

import { Link } from '@tanstack/react-router'
import { ArrowUpRight, Brain, Gauge, Pulse } from '@phosphor-icons/react'
import { Card } from '@/components/ui/card'
import type { AgentChatEntry } from '@/features/agent-chat/types'
import type {
  AgentHealthItem,
  AttentionConsumerHealth,
  MissionControlResponse,
} from '@/features/federation/types'
import {
  EmptyPanel,
  ErrorPanel,
  SectionKicker,
  StateBadge,
  StatusDot,
} from '@/features/federation/components'
import { humanize, formatDate, count } from './utils'
import { ProjectionSection } from './shared'

export function AgentHealthCard({
  item,
  scopeLabel,
}: {
  item: AgentHealthItem
  scopeLabel: string
}) {
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

export function BindingScopes({
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

export function Capacity({ data }: { data: MissionControlResponse }) {
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

export function ConsumerHealth({ health }: { health: AttentionConsumerHealth | null }) {
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
