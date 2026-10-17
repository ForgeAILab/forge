import { fireEvent, render, screen, within } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import {
  AgentAssigneeDropdown,
  TaskStatusBadge,
  TaskStatusDropdown,
  getAvailableTaskTransitions,
} from './task-controls'
import { leadCandidateDisabledReason } from '@/lib/root-lead'
import type { Agent } from '@/types/generated'
import { emptyUsage } from '@/test-utils/usage'

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
    active_assigned_task_count: 0,
    running_execution_count: 0,
    effective_status: effectiveStatus,
    avg_duration_ms: null,
    success_rate: null,
    usage: emptyUsage,
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

  it('lists a CLI Agent in the Lead picker disabled with the reason, and offers no person', () => {
    const onChange = vi.fn()
    render(
      <AgentAssigneeDropdown
        agents={[
          agent('native', 'Native Agent', 'active'),
          agent('cli', 'Codex CLI', 'active', { backend_kind: 'cli', executor_type: 'codex' }),
        ]}
        roleLabel="Lead"
        agentsOnly
        agentDisabledReason={leadCandidateDisabledReason}
        onChange={onChange}
      />,
    )

    fireEvent.click(screen.getByRole('button', { name: 'Assign Lead' }))
    const listbox = within(screen.getByRole('listbox'))
    const cli = listbox.getByRole('option', { name: /Codex CLI/ }) as HTMLButtonElement
    expect(cli.disabled).toBe(true)
    expect(cli.textContent).toContain('A lead must be a native Agent')
    expect(listbox.queryByRole('option', { name: /Human/ })).toBeNull()

    fireEvent.click(cli)
    expect(onChange).not.toHaveBeenCalled()
    fireEvent.click(listbox.getByRole('option', { name: /Native Agent/ }))
    expect(onChange).toHaveBeenCalledWith({ type: 'agent', agentId: 'native' })
  })

  it('still lets the lead be removed', () => {
    const onChange = vi.fn()
    render(
      <AgentAssigneeDropdown
        agents={[agent('native', 'Native Agent', 'active')]}
        value={{ type: 'agent', agentId: 'native' }}
        roleLabel="Lead"
        agentsOnly
        agentDisabledReason={leadCandidateDisabledReason}
        onChange={onChange}
      />,
    )
    fireEvent.click(screen.getByRole('button', { name: 'Assign Lead' }))
    fireEvent.click(within(screen.getByRole('listbox')).getByRole('option', { name: /Unassigned/ }))
    expect(onChange).toHaveBeenCalledWith({ type: 'unassigned' })
  })
})

describe('lead_check as a task status', () => {
  it('offers the gate exits as manual transitions and is itself no target of another status', () => {
    expect(getAvailableTaskTransitions('lead_check')).toEqual(['done', 'in_progress', 'cancelled'])
    for (const from of ['todo', 'in_progress', 'review', 'merging', 'merge_failed']) {
      expect(getAvailableTaskTransitions(from)).not.toContain('lead_check')
    }
  })

  it('renders the status with the working colours, not the unknown-status fallback', () => {
    render(<TaskStatusBadge status="lead_check" />)
    const badge = screen.getByText('lead check')
    expect(badge.className).toContain('orange')
    expect(badge.className).not.toContain('purple')
  })

  it('offers a status menu from lead_check instead of a dead badge', () => {
    render(<TaskStatusDropdown status="lead_check" onChange={vi.fn()} />)
    expect(screen.getByRole('button', { name: /from lead check/ })).toBeTruthy()
  })
})
