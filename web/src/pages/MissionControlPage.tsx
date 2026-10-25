import { useState } from 'react'
import { Link } from '@tanstack/react-router'
import {
  ArrowClockwise,
  Brain,
  CheckCircle,
  Clock,
  Pulse,
  Question,
  WarningCircle,
} from '@phosphor-icons/react'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { useAgentChatsQuery } from '@/features/agent-chat/hooks'
import type { AgentChatEntry } from '@/features/agent-chat/types'
import { useMissionControlQuery } from '@/features/federation/hooks'
import type { AttentionItem, MissionControlResponse } from '@/features/federation/types'
import {
  ErrorPanel,
  LoadingPanel,
  PageHeader,
  SectionKicker,
} from '@/features/federation/components'
import {
  type LeadAnswerDrafts,
  AttentionCard,
  WithdrawnLeadDraft,
} from './mission-control/attention'
import { BindingScopes, Capacity, ConsumerHealth, AgentHealthCard } from './mission-control/health'
import { ProjectionSection } from './mission-control/shared'
import { CoordinationActivitySection } from './mission-control/activity'
import { WorkRow, OutcomeRow } from './mission-control/work'
import { formatDate } from './mission-control/utils'

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
