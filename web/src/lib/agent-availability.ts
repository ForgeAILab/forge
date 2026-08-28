const USABLE_AGENT_STATUSES = new Set(['active', 'busy', 'idle'])

export type AgentAvailabilityProjection = {
  status: string
  effective_status?: string | null
  paused?: boolean
}

/**
 * Whether Forge may offer an Agent for a new assignment or binding.
 *
 * `effective_status` includes provider, credential, connection, runtime, and
 * capacity state. Project-scoped legacy projections may omit it, so `idle`
 * remains the safe base-status fallback.
 */
export function isAgentUsable(agent: AgentAvailabilityProjection): boolean {
  if (agent.paused) return false
  return USABLE_AGENT_STATUSES.has(agent.effective_status ?? agent.status)
}

export function usableAgents<T extends AgentAvailabilityProjection>(agents: readonly T[]): T[] {
  return agents.filter(isAgentUsable)
}
