import { fireEvent, render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { AgentMachines } from './AgentMachines'
import type { FederatedAgent } from './types'
const state = vi.hoisted(() => ({ admin: true, mutate: vi.fn(), invalidate: vi.fn() }))
vi.mock('@/api/hooks', () => ({
  useUpdateAgent: () => ({ mutate: state.mutate, isPending: false, isError: false }),
}))
vi.mock('@/stores/auth', () => ({ useAuthStore: () => state.admin }))
vi.mock('@tanstack/react-query', () => ({
  useQueryClient: () => ({ invalidateQueries: state.invalidate }),
}))
const agent = {
  id: 'agent-1',
  version: 3,
  daemon_id: 'daemon-mac',
  runnable_on: {
    count: 1,
    machines: [
      {
        id: 'runtime-mac',
        name: 'Mac mini',
        owner_kind: 'daemon',
        daemon_id: 'daemon-mac',
        runtime_id: 'runtime-mac',
      },
    ],
  },
} as FederatedAgent
beforeEach(() => {
  state.admin = true
  state.mutate.mockReset()
})
describe('Agent machines', () => {
  it('shows runnable identities and lets an admin clear the pin', () => {
    render(<AgentMachines agent={agent} />)
    expect(screen.getByText('Mac mini')).toBeTruthy()
    expect(screen.getByText('Pin: daemon-mac')).toBeTruthy()
    fireEvent.click(screen.getByRole('button', { name: 'Clear pin' }))
    expect(state.mutate.mock.calls[0][0]).toEqual({
      agentId: 'agent-1',
      body: { version: 3, daemon_id: null },
    })
    state.mutate.mock.calls[0][1].onSuccess()
    expect(state.invalidate).toHaveBeenCalledWith({ queryKey: ['federated-agents'] })
  })
  it('shows a count and hides pin controls for a non-admin', () => {
    state.admin = false
    render(<AgentMachines agent={{ ...agent, runnable_on: { count: 2 } }} />)
    expect(screen.getByText('2 machines')).toBeTruthy()
    expect(screen.queryByRole('button')).toBeNull()
    expect(screen.queryByText(/Pin:/)).toBeNull()
  })
  it('warns when the pinned machine cannot run the executor', () => {
    render(<AgentMachines agent={{ ...agent, runnable_on: { count: 0, machines: [] } }} />)
    expect(screen.getByRole('status').textContent).toContain('cannot run on any machine')
    expect(screen.getByText(/pinned machine cannot run/)).toBeTruthy()
  })
})
