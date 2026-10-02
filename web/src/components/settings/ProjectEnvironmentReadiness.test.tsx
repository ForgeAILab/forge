import { fireEvent, render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { ProjectEnvironmentReadiness } from './ProjectEnvironmentReadiness'
import type { ProjectEnvironmentReadiness as Readiness } from '@/types/generated/bindings/ProjectEnvironmentReadiness'
const mutation = vi.hoisted(() => ({
  mutate: vi.fn(),
  isPending: false,
  isError: false,
  error: null as Error | null,
  data: undefined,
  variables: { machine: 'server' },
}))
vi.mock('@/api/hooks', () => ({ useRecheckProjectEnvironment: () => mutation }))
const row: Readiness = {
  machine: {
    id: 'server',
    name: 'Server host',
    owner_kind: 'server',
    daemon_id: null,
    runtime_id: null,
  },
  status: 'not_ready',
  failing_checks: [{ name: 'cargo', output_tail: 'command not found' }],
  output_tail: 'command not found',
  scope_covered: 'full',
  checked_at: '2026-10-02T00:00:00Z',
  next_check_at: '2026-10-02T00:10:00Z',
}
beforeEach(() => {
  mutation.mutate.mockReset()
  mutation.isPending = false
  mutation.isError = false
  mutation.error = null
})
describe('Machine readiness', () => {
  it('shows each machine, checks, times and a targeted action', () => {
    render(<ProjectEnvironmentReadiness projectId="project-1" rows={[row]} />)
    expect(screen.getByRole('table', { name: 'Machine environment readiness' })).toBeTruthy()
    expect(screen.getByText('not ready')).toBeTruthy()
    expect(screen.getByText('command not found')).toBeTruthy()
    expect(document.querySelectorAll('time').length).toBe(2)
    fireEvent.click(screen.getByRole('button', { name: 'Check now on Server host' }))
    expect(mutation.mutate).toHaveBeenCalledWith({ projectId: 'project-1', machine: 'server' })
  })
  it('shows an empty state when no checks have recorded readiness', () => {
    render(<ProjectEnvironmentReadiness projectId="project-1" rows={[]} />)
    expect(screen.getByText(/No environment readiness recorded/)).toBeTruthy()
    expect(screen.queryByRole('table')).toBeNull()
  })
  it('names a pending machine, disables Check now and reports errors', () => {
    mutation.isPending = true
    mutation.isError = true
    mutation.error = new Error('Machine unreachable')
    render(<ProjectEnvironmentReadiness projectId="project-1" rows={[row]} />)
    expect(screen.getByRole('button').hasAttribute('disabled')).toBe(true)
    expect(screen.getByRole('status').textContent).toContain('Server host')
    expect(screen.getByRole('alert').textContent).toContain('Machine unreachable')
  })
})
