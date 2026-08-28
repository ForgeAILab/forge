import { fireEvent, render, screen, within } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { AgentAssigneeDropdown } from './task-controls'
import type { Agent } from '@/types/generated'

function agent(
  id: string,
  name: string,
  effectiveStatus: string,
  overrides: Partial<Agent> = {},
): Agent {
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
    ...overrides,
  }
}

describe('AgentAssigneeDropdown', () => {
  it('offers only agents that can accept new work', () => {
    const onChange = vi.fn()
    render(
      <AgentAssigneeDropdown
        agents={[
          agent('usable', 'Usable Agent', 'active'),
          agent('disabled-source', 'Disabled Provider Agent', 'source_disabled'),
          agent('paused', 'Paused Agent', 'active', { paused: true }),
        ]}
        onChange={onChange}
      />,
    )

    fireEvent.click(screen.getByRole('button', { name: 'Assign agent' }))
    const listbox = within(screen.getByRole('listbox'))

    expect(listbox.getByRole('option', { name: /Usable Agent/ })).toBeTruthy()
    expect(listbox.queryByRole('option', { name: /Disabled Provider Agent/ })).toBeNull()
    expect(listbox.queryByRole('option', { name: /Paused Agent/ })).toBeNull()
  })

  it('keeps an existing unavailable assignment visible without offering it again', () => {
    render(
      <AgentAssigneeDropdown
        agents={[agent('disabled-source', 'Disabled Provider Agent', 'source_disabled')]}
        value={{ type: 'agent', agentId: 'disabled-source' }}
        onChange={vi.fn()}
      />,
    )

    expect(screen.getByText('Disabled Provider Agent')).toBeTruthy()
    fireEvent.click(screen.getByRole('button', { name: 'Assign agent' }))
    expect(
      within(screen.getByRole('listbox')).queryByRole('option', {
        name: /Disabled Provider Agent/,
      }),
    ).toBeNull()
  })
})
