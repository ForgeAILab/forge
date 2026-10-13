import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { TaskPlacementDiagnostics } from './task-placement-diagnostics'
const machine = {
  id: 'runtime-mac',
  name: 'Mac mini',
  owner_kind: 'daemon' as const,
  daemon_id: 'daemon-mac',
  runtime_id: 'runtime-mac',
}
describe('Placement diagnostics', () => {
  it('names a failed machine and its checks', () => {
    render(
      <TaskPlacementDiagnostics
        diagnostics={[
          { machine, filter_codes: ['environment_not_ready'], failing_checks: ['cargo'] },
        ]}
      />,
    )
    expect(screen.getByText('Mac mini: Environment not ready (cargo)')).toBeTruthy()
  })
  it('names the pending probe and renders the machine cap through the same panel', () => {
    render(
      <TaskPlacementDiagnostics
        diagnostics={[
          { machine, filter_codes: ['environment_probe_pending'], failing_checks: [] },
          { machine, filter_codes: ['machine_capacity'], failing_checks: [] },
        ]}
      />,
    )
    expect(screen.getByText('Checking machine Mac mini…')).toBeTruthy()
    expect(screen.getByText('Mac mini: Machine run capacity reached')).toBeTruthy()
  })
  it('labels exhausted provisioning without calling the connected machine offline', () => {
    render(
      <TaskPlacementDiagnostics
        diagnostics={[{ machine, filter_codes: ['provision_failed'], failing_checks: [] }]}
      />,
    )
    expect(
      screen.getByText(
        'Mac mini: Repository provisioning failed; reconnect or update placement settings to retry',
      ),
    ).toBeTruthy()
    expect(screen.queryByText(/offline/i)).toBeNull()
  })
  it('has no empty status panel', () => {
    render(<TaskPlacementDiagnostics diagnostics={[]} />)
    expect(screen.queryByRole('status')).toBeNull()
  })
  it('explains the refusal of a native lead on a daemon workspace with the two ways out', () => {
    const refusal = { machine, filter_codes: ['native_backend_unsupported'], failing_checks: [] }
    const { unmount } = render(<TaskPlacementDiagnostics diagnostics={[refusal]} hasLead />)
    expect(screen.getByText('Mac mini: Daemon workspaces run CLI Agents only')).toBeTruthy()
    const action = screen.getByTestId('lead-placement-refusal').textContent ?? ''
    expect(action).toContain('Remove the lead')
    expect(action).toContain('runs on the server')
    unmount()

    // Without a lead the refusal is about another role: no lead advice.
    render(<TaskPlacementDiagnostics diagnostics={[refusal]} />)
    expect(screen.getByText('Mac mini: Daemon workspaces run CLI Agents only')).toBeTruthy()
    expect(screen.queryByTestId('lead-placement-refusal')).toBeNull()
  })
  it('gives no lead advice for another placement refusal on a root with a lead', () => {
    render(
      <TaskPlacementDiagnostics
        diagnostics={[{ machine, filter_codes: ['machine_capacity'], failing_checks: [] }]}
        hasLead
      />,
    )
    expect(screen.queryByTestId('lead-placement-refusal')).toBeNull()
  })
})
