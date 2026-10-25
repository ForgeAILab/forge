// Mission Control coordination activity sections.

import type { ReactNode } from 'react'
import { CheckCircle, Command, Gavel } from '@phosphor-icons/react'
import { useAgentQuery } from '@/api/hooks'
import type { CoordinationActivityItem } from '@/features/federation/types'
import { StateBadge } from '@/features/federation/components'
import { humanize, formatDate } from './utils'
import { ProjectionSection } from './shared'

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

export function CoordinationActivitySection({
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
