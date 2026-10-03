import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { TaskInteractiveLaunch } from './task-interactive-launch'
const { mutate } = vi.hoisted(() => ({ mutate: vi.fn() }))
vi.mock('@/api/hooks', () => ({ useLaunchExecution: () => ({ mutate, isPending: false }) }))
vi.mock('@tanstack/react-router', () => ({ useNavigate: () => vi.fn() }))
vi.mock('@/pages/task-detail/TaskLaunchDialog', () => ({
  TaskLaunchDialog: ({
    onSubmit,
  }: {
    onSubmit: (config: { agentId: string; overrides: null }, summary: string) => void
  }) => (
    <button
      onClick={() => onSubmit({ agentId: 'worker', overrides: null }, 'Investigate the review')}
    >
      Launch interactive session
    </button>
  ),
}))
describe('interactive resource entry point', () => {
  it('opens the retained launch dialog and forwards its input', () => {
    render(
      <QueryClientProvider client={new QueryClient()}>
        <TaskInteractiveLaunch taskId="t" />
      </QueryClientProvider>,
    )
    expect(mutate).not.toHaveBeenCalled()
    fireEvent.click(screen.getByRole('button', { name: 'Open interactive' }))
    fireEvent.click(screen.getByRole('button', { name: 'Launch interactive session' }))
    expect(mutate.mock.calls[0][0]).toEqual({
      taskId: 't',
      body: { agent_id: 'worker', summary: 'Investigate the review', overrides: null },
    })
  })
})
