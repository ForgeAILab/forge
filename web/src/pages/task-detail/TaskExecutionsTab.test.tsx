import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { describe, expect, it, vi } from 'vitest'
import type { Execution } from '@/types/generated'
import { TaskExecutionsTab } from './TaskExecutionsTab'
const { apiFetch } = vi.hoisted(() => ({ apiFetch: vi.fn() }))
vi.mock('@/api/client', () => ({ apiFetch }))
vi.mock('@/api/hooks', () => ({ useTaskAction: () => ({ mutate: vi.fn(), isPending: false }) }))
vi.mock('@tanstack/react-router', () => ({ useNavigate: () => vi.fn() }))
describe('independent execution stops', () => {
  it('stops the running role and side session separately', async () => {
    apiFetch.mockResolvedValue({})
    const runs = ['role-run', 'side-session'].map(
      (id) =>
        ({
          id,
          task_id: 't',
          status: 'running',
          role: id === 'side-session' ? 'interactive' : 'coder',
          parent_execution_id: null,
          agent_session_id: id,
          summary: id,
          created_at: '2026-10-02T00:00:00Z',
        }) as Execution,
    )
    render(
      <QueryClientProvider client={new QueryClient()}>
        <TaskExecutionsTab
          taskId="t"
          version={4}
          offers={[]}
          executions={runs}
          isLoading={false}
          agentName={() => 'Worker'}
          formatDate={() => 'Today'}
        />
      </QueryClientProvider>,
    )
    expect(screen.getByText('Coder Session')).toBeTruthy()
    expect(screen.getByText('Interactive Session')).toBeTruthy()
    const stops = screen.getAllByRole('button', { name: 'Stop' })
    expect(stops).toHaveLength(2)
    fireEvent.click(stops[0])
    await waitFor(() => expect(apiFetch).toHaveBeenCalledTimes(1))
    await waitFor(() => expect((stops[1] as HTMLButtonElement).disabled).toBe(false))
    fireEvent.click(stops[1])
    await waitFor(() => expect(apiFetch).toHaveBeenCalledTimes(2))
    expect(new Set(apiFetch.mock.calls.map((call) => call[0]))).toEqual(
      new Set(['/executions/role-run/stop', '/executions/side-session/stop']),
    )
    expect(apiFetch.mock.calls.every((call) => call[1].body === '{}')).toBe(true)
  })
  it('shows separate stops when two running executions share a displayed chain', async () => {
    apiFetch.mockReset()
    apiFetch.mockResolvedValue({})
    const runs = [
      {
        id: 'parent',
        status: 'completed',
        parent_execution_id: null,
        created_at: '2026-10-01T00:00:00Z',
      },
      {
        id: 'role-run',
        status: 'running',
        parent_execution_id: 'parent',
        created_at: '2026-10-02T00:00:00Z',
      },
      {
        id: 'side-session',
        status: 'running',
        parent_execution_id: 'parent',
        created_at: '2026-10-02T01:00:00Z',
      },
    ].map(
      (run) =>
        ({
          ...run,
          task_id: 't',
          agent_id: 'worker',
          role: 'coder',
          agent_session_id: run.id,
          summary: run.id,
        }) as Execution,
    )
    render(
      <QueryClientProvider client={new QueryClient()}>
        <TaskExecutionsTab
          taskId="t"
          version={4}
          offers={[]}
          executions={runs}
          isLoading={false}
          agentName={() => 'Worker'}
          formatDate={() => 'Today'}
        />
      </QueryClientProvider>,
    )
    const stops = screen.getAllByRole('button', { name: /Stop turn/ })
    expect(stops).toHaveLength(2)
    fireEvent.click(stops[0])
    await waitFor(() => expect((stops[1] as HTMLButtonElement).disabled).toBe(false))
    fireEvent.click(stops[1])
    await waitFor(() => expect(apiFetch).toHaveBeenCalledTimes(2))
    expect(new Set(apiFetch.mock.calls.map((call) => call[0]))).toEqual(
      new Set(['/executions/role-run/stop', '/executions/side-session/stop']),
    )
  })

  it('shows an offered fresh retry even before any execution was created', () => {
    render(
      <QueryClientProvider client={new QueryClient()}>
        <TaskExecutionsTab
          taskId="t"
          version={4}
          offers={[
            {
              action: { verb: 'retry', fresh_session: true },
              parameters: [{ name: 'fresh_session', required: false, boolean_values: [true] }],
              label: 'Retry from fresh session',
              reason: 'dispatch_failed',
              authority: ['owner'],
              target_execution_id: null,
              propagates: false,
            },
          ]}
          executions={[]}
          isLoading={false}
          agentName={() => 'Worker'}
          formatDate={() => 'Today'}
        />
      </QueryClientProvider>,
    )
    expect(screen.getByRole('button', { name: 'Retry from fresh session' })).toBeTruthy()
  })
})
