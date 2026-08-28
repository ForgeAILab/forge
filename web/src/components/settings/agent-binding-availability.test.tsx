import { fireEvent, render, screen, within } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import type { FederatedAgent } from '@/features/federation/types'
import { MainAgentBindingCard } from './MainAgentBindingCard'
import { ProjectAgentTab } from './ProjectAgentTab'

const refetch = vi.fn()

vi.mock('@tanstack/react-router', () => ({
  Link: ({ children }: { children: React.ReactNode }) => <a>{children}</a>,
}))

vi.mock('@/features/federation/hooks', () => ({
  isVersionConflict: () => false,
  useMainAgentBindingQuery: () => ({
    data: undefined,
    error: undefined,
    isLoading: false,
    isError: false,
    refetch,
  }),
  useSetMainAgentBindingMutation: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useProjectAgentBindingQuery: () => ({
    data: undefined,
    error: undefined,
    isLoading: false,
    isError: false,
    refetch,
  }),
  useSetProjectAgentBindingMutation: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useFederatedAgentsQuery: () => ({
    data: { items: agents, has_more: false },
    isLoading: false,
    isError: false,
    refetch,
  }),
}))

function agent(id: string, name: string, effectiveStatus: string): FederatedAgent {
  return {
    id,
    name,
    description: null,
    profile_id: `profile-${id}`,
    backend_kind: 'native',
    executor_type: 'embedded',
    provider: 'openai',
    model: 'gpt-test',
    reasoning_effort: null,
    permission_policy: null,
    prompt_template: null,
    capabilities: [],
    config_json: {},
    credential_handle_id: `credential-${id}`,
    daemon_id: null,
    max_concurrent_tasks: 1,
    status: 'idle',
    active_task_count: 0,
    effective_status: effectiveStatus,
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
    created_at: '2026-08-28T00:00:00Z',
    updated_at: '2026-08-28T00:00:00Z',
  }
}

const agents = [
  agent('usable', 'Usable Agent', 'active'),
  agent('disabled-source', 'Disabled Provider Agent', 'source_disabled'),
]

function expectOnlyUsableAgentInOpenListbox() {
  const listbox = within(screen.getByRole('listbox'))
  expect(listbox.getByRole('option', { name: /Usable Agent/ })).toBeTruthy()
  expect(listbox.queryByRole('option', { name: /Disabled Provider Agent/ })).toBeNull()
}

describe('agent binding availability', () => {
  it('offers only usable agents for the Main Agent binding', () => {
    render(<MainAgentBindingCard agents={agents} onConnect={vi.fn()} />)

    fireEvent.click(screen.getByRole('button', { name: 'Main Agent' }))
    expectOnlyUsableAgentInOpenListbox()
  })

  it('offers only usable agents for the Project Agent binding', () => {
    render(<ProjectAgentTab projectId="project-1" projectName="Project One" />)

    fireEvent.click(screen.getByRole('button', { name: 'Project Agent' }))
    expectOnlyUsableAgentInOpenListbox()
  })
})
