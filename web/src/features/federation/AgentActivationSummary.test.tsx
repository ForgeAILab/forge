import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import type { AgentChatEntry } from '@/features/agent-chat/types'
import type { FederatedAgent } from '@/features/federation/types'
import { AgentActivationSummary } from './AgentActivationSummary'

const agent: FederatedAgent = {
  id: 'agent-1',
  name: 'Project Operator',
  description: null,
  profile_id: 'profile-1',
  backend_kind: 'native',
  executor_type: 'embedded',
  provider: 'openai',
  model: 'gpt-test',
  reasoning_effort: null,
  permission_policy: 'scoped_proposals',
  prompt_template: null,
  capabilities: [],
  config_json: {},
  credential_handle_id: 'credential-1',
  daemon_id: null,
  max_concurrent_tasks: 1,
  status: 'idle',
  active_task_count: 0,
  effective_status: 'active',
  total_runs: 0,
  avg_duration_ms: null,
  success_rate: null,
  total_input_tokens: 0,
  total_output_tokens: 0,
  total_cache_read_tokens: 0,
  total_cache_write_tokens: 0,
  total_tokens: 0,
  total_cost_usd: null,
  is_default: false,
  paused: false,
  owner_id: 'user-1',
  visibility: 'private',
  version: 1,
  created_at: '2026-08-27T00:00:00Z',
  updated_at: '2026-08-27T00:00:00Z',
}

const chatEntries: AgentChatEntry[] = [
  {
    chat_id: 'main-chat',
    kind: 'main',
    project_id: null,
    project_name: null,
    identity_id: agent.id,
    identity_name: agent.name,
    binding_state: 'active',
    chat_status: 'ready',
    unread_count: 0n,
    pending_turn_count: 0n,
    last_message_at: null,
  },
  {
    chat_id: 'project-chat',
    kind: 'project',
    project_id: 'project-1',
    project_name: 'Todo',
    identity_id: agent.id,
    identity_name: agent.name,
    binding_state: 'active',
    chat_status: 'ready',
    unread_count: 0n,
    pending_turn_count: 0n,
    last_message_at: null,
  },
]

describe('AgentActivationSummary', () => {
  it('lists the active chat scopes and separates every admission path', () => {
    render(<AgentActivationSummary agent={agent} chatEntries={chatEntries} />)

    expect(screen.getByText('When this agent runs')).toBeTruthy()
    expect(screen.getByText('Eligible for new work')).toBeTruthy()
    expect(screen.getByText('Chat message')).toBeTruthy()
    expect(screen.getByText('Project creation handoff')).toBeTruthy()
    expect(screen.getByText('Background attention wake')).toBeTruthy()
    expect(screen.getByText('Task workflow assignment')).toBeTruthy()
    expect(screen.getByText(/A user message in Main Chat, Todo/)).toBeTruthy()
    expect(screen.getByText(/Uses the binding wake budget per rolling hour/)).toBeTruthy()
  })

  it('does not treat inactive bindings as trigger scopes', () => {
    const inactive = chatEntries.map((entry) => ({ ...entry, binding_state: 'replaced' as const }))

    render(
      <AgentActivationSummary
        agent={{ ...agent, paused: true }}
        chatEntries={inactive}
      />,
    )

    expect(screen.getByText('Agent disabled')).toBeTruthy()
    expect(screen.getAllByText('not bound')).toHaveLength(2)
    expect(screen.getByText(/No Main or Project Chat currently routes messages/)).toBeTruthy()
  })
})
