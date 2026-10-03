import { fireEvent, render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { AgentMachines } from './AgentMachines'
import type { FederatedAgent } from './types'
const state = vi.hoisted(() => ({
  admin: true,
  mutate: vi.fn(),
  invalidate: vi.fn(),
  query: vi.fn(),
  daemon: undefined as
    | { id: string; machine_id: string; hostname: string; status: string }
    | undefined,
}))
vi.mock('@/api/hooks', () => ({
  useUpdateAgent: () => ({ mutate: state.mutate, isPending: false, isError: false }),
}))
vi.mock('@/stores/auth', () => ({ useAuthStore: () => state.admin }))
vi.mock('@tanstack/react-query', () => ({
  useQueryClient: () => ({ invalidateQueries: state.invalidate }),
  useQuery: (options: unknown) => {
    state.query(options)
    return { data: state.daemon, isPending: false }
  },
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
  state.query.mockReset()
  state.daemon = undefined
})
describe('Agent machines', () => {
  it('shows runnable identities and lets an admin clear the pin', () => {
    render(<AgentMachines agent={agent} />)
    expect(screen.getByText('Mac mini')).toBeTruthy()
    expect(screen.getByText('Pinned to Mac mini').getAttribute('title')).toBe('daemon-mac')
    expect(state.query.mock.calls[0][0].enabled).toBe(false)
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
    expect(screen.queryByText(/Pinned to/)).toBeNull()
    expect(state.query.mock.calls[0][0].enabled).toBe(false)
  })
  it('warns when the pinned machine cannot run the executor', () => {
    render(<AgentMachines agent={{ ...agent, runnable_on: { count: 0, machines: [] } }} />)
    expect(screen.getByRole('status').textContent).toContain('cannot run on any machine')
    expect(screen.getByText(/Not currently runnable/)).toBeTruthy()
    expect(screen.getByText(/executor is unavailable or disabled/)).toBeTruthy()
  })
  it('names an offline pin even when it is absent from runnable_on', () => {
    state.daemon = {
      id: 'daemon-mac',
      machine_id: 'machine-mac',
      hostname: 'Mac mini',
      status: 'offline',
    }
    render(<AgentMachines agent={{ ...agent, runnable_on: { count: 0, machines: [] } }} />)
    expect(screen.getByText('Pinned to Mac mini').getAttribute('title')).toBe('daemon-mac')
    expect(screen.getByText(/Offline/)).toBeTruthy()
    expect(state.query.mock.calls[0][0].enabled).toBe(true)
  })
  it('uses Server host for a pin to the embedded daemon', () => {
    state.daemon = {
      id: 'daemon-mac',
      machine_id: 'embedded:local-host',
      hostname: 'local-host',
      status: 'online',
    }
    render(
      <AgentMachines
        agent={{
          ...agent,
          runnable_on: {
            count: 1,
            machines: [
              {
                id: 'server',
                name: 'Server host',
                owner_kind: 'server',
                daemon_id: null,
                runtime_id: null,
              },
            ],
          },
        }}
      />,
    )
    expect(screen.getByText('Pinned to Server host')).toBeTruthy()
    expect(screen.queryByText(/Not currently runnable/)).toBeNull()
  })
})
