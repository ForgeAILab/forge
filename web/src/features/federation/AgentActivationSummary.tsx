import {
  ArrowCounterClockwise,
  ChatCircleDots,
  GitBranch,
  Lightning,
} from '@phosphor-icons/react'
import type { AgentChatEntry } from '@/features/agent-chat/types'
import type { FederatedAgent } from '@/features/federation/types'
import { StateBadge } from '@/features/federation/components'

interface ActivationPath {
  id: string
  title: string
  status: string
  description: string
  budget: string
  icon: typeof ChatCircleDots
}

function scopeLabel(entry: AgentChatEntry): string {
  return entry.kind === 'main' ? 'Main Chat' : (entry.project_name ?? 'Project Agent Chat')
}

function activeAgentChatScopes(
  agentId: string,
  entries: AgentChatEntry[],
): string[] {
  const scopes: string[] = []
  for (const entry of entries) {
    if (entry.identity_id === agentId && entry.binding_state === 'active') {
      scopes.push(scopeLabel(entry))
    }
  }
  return scopes
}

export function AgentActivationSummary({
  agent,
  chatEntries,
}: {
  agent: FederatedAgent
  chatEntries: AgentChatEntry[]
}) {
  const scopes = activeAgentChatScopes(agent.id, chatEntries)
  const scopeList = scopes.join(', ')
  const effectiveStatus = agent.effective_status ?? agent.status
  const available = !agent.paused && ['active', 'busy', 'idle'].includes(effectiveStatus)
  const paths: ActivationPath[] = [
    {
      id: 'chat-message',
      title: 'Chat message',
      status: scopes.length > 0 ? 'configured' : 'not_bound',
      description:
        scopes.length > 0
          ? `A user message in ${scopeList} admits a new turn.`
          : 'No Main or Project Chat currently routes messages to this Agent.',
      budget: 'Does not use the background wake budget.',
      icon: ChatCircleDots,
    },
    {
      id: 'project-handoff',
      title: 'Project creation handoff',
      status: 'selection_based',
      description:
        'Runs once when this Agent is selected for a newly approved Project creation. Replacing a binding does not replay an old handoff.',
      budget: 'Does not use the background wake budget.',
      icon: GitBranch,
    },
    {
      id: 'attention-wake',
      title: 'Background attention wake',
      status: scopes.length > 0 ? 'configured' : 'not_bound',
      description:
        scopes.length > 0
          ? `Incidents in ${scopeList} can admit a turn after Forge checks binding, availability, deduplication, cooldown, and policy.`
          : 'A current Main or Project binding is required before an Attention incident can wake this Agent.',
      budget: 'Uses the binding wake budget per rolling hour.',
      icon: Lightning,
    },
    {
      id: 'task-workflow',
      title: 'Task workflow assignment',
      status: 'assignment_based',
      description:
        'Runs when a Task assigns this Agent as Worker or reviewer and the Task workflow reaches that role. Task assignment overrides Project defaults.',
      budget: 'Uses Task execution capacity, not the background wake budget.',
      icon: ArrowCounterClockwise,
    },
  ]

  return (
    <section aria-labelledby={`agent-activation-${agent.id}`}>
      <div className="mb-3 flex flex-wrap items-start justify-between gap-3">
        <div>
          <h3
            id={`agent-activation-${agent.id}`}
            className="font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground"
          >
            When this agent runs
          </h3>
          <p className="mt-1 text-xs leading-5 text-muted-foreground">
            Settings are frozen when a new turn or Task execution is admitted. Later edits apply
            to the next admission.
          </p>
        </div>
        <StateBadge
          status={available ? 'active' : effectiveStatus}
          label={available ? 'Eligible for new work' : agent.paused ? 'Agent disabled' : 'Unavailable'}
        />
      </div>

      <ul className="divide-y divide-border-subtle overflow-hidden rounded-md border border-border-subtle bg-card">
        {paths.map((path) => {
          const Icon = path.icon
          return (
            <li key={path.id} className="flex min-w-0 gap-3 px-3 py-3">
              <span className="flex h-8 w-8 shrink-0 items-center justify-center rounded-md bg-muted text-muted-foreground">
                <Icon size={16} aria-hidden />
              </span>
              <div className="min-w-0 flex-1">
                <div className="flex flex-wrap items-center gap-2">
                  <p className="text-sm font-medium text-foreground">{path.title}</p>
                  <span className="rounded bg-muted px-2 py-1 font-mono text-micro uppercase tracking-[0.8px] text-muted-foreground">
                    {path.status.replaceAll('_', ' ')}
                  </span>
                </div>
                <p className="mt-1 text-xs leading-5 text-muted-foreground">{path.description}</p>
                <p className="mt-1 font-mono text-micro leading-5 text-muted-foreground">
                  {path.budget}
                </p>
              </div>
            </li>
          )
        })}
      </ul>

      <details className="mt-3 rounded-md border border-border-subtle bg-muted/20 px-3 py-2">
        <summary className="cursor-pointer text-xs font-medium text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">
          Technical event names
        </summary>
        <div className="mt-3 space-y-3 text-xs leading-5 text-muted-foreground">
          <div>
            <p className="font-medium text-foreground">Turn admission triggers</p>
            <p className="mt-1 font-mono break-words">
              user_message · genesis_continuation · main_project_handoff · autonomous_wake ·
              baseline_activation (optional planning traceability)
            </p>
          </div>
          <div>
            <p className="font-medium text-foreground">Attention-producing events</p>
            <p className="mt-1">
              Task transitions to blocked, review, failed, or done; execution failure,
              cancellation, stall, or progress warning; validation/review/retry incidents;
              runtime unavailability; required questions/interactions; budget thresholds; and
              overdue commitments.
            </p>
          </div>
          <p>
            Event subscriptions are server-defined today. The stored binding subscriptions value
            is not currently a user-configurable per-event filter.
          </p>
        </div>
      </details>
    </section>
  )
}
