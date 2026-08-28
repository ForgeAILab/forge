import { describe, expect, it } from 'vitest'
import { isAgentUsable, usableAgents } from './agent-availability'

describe('agent availability', () => {
  it.each(['active', 'busy', 'idle'])('allows %s agents for new selections', (status) => {
    expect(isAgentUsable({ status: 'idle', effective_status: status, paused: false })).toBe(true)
  })

  it.each([
    'source_disabled',
    'paused',
    'connection_degraded',
    'connection_unavailable',
    'daemon_offline',
    'daemon_unavailable',
    'deactivated',
    'error',
    'offline',
  ])('hides %s agents from new selections', (status) => {
    expect(isAgentUsable({ status: 'idle', effective_status: status, paused: false })).toBe(false)
  })

  it('treats the explicit pause flag as unavailable', () => {
    expect(isAgentUsable({ status: 'idle', effective_status: 'active', paused: true })).toBe(false)
  })

  it('filters a mixed roster without mutating it', () => {
    const agents = [
      { id: 'usable', status: 'idle', effective_status: 'active', paused: false },
      {
        id: 'disabled-provider',
        status: 'idle',
        effective_status: 'source_disabled',
        paused: false,
      },
    ]

    expect(usableAgents(agents).map((agent) => agent.id)).toEqual(['usable'])
    expect(agents).toHaveLength(2)
  })
})
