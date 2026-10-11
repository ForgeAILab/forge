import { fireEvent, render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { ForgeSettingsPage } from '@/pages/ForgeSettingsPage'
import { MachineRunCapacity } from '@/pages/DaemonsPage'
import type { Daemon } from '@/types/generated'

const mocks = vi.hoisted(() => ({ saveSettings: vi.fn(), saveLimit: vi.fn(), admin: true }))
vi.mock('@/api/hooks', () => ({
  useSettingsQuery: vi.fn().mockReturnValue({ data: { config_path: '/forge.yaml', restart_required: false, settings: [
    { key: 'server.bind', value: '127.0.0.1:8080', effective_value: '127.0.0.1:8080', restart_required: false },
    { key: 'server.usage_index_budget_mb', value: null, effective_value: 128, restart_required: false },
    { key: 'server.max_concurrent_runs', value: null, effective_value: 4, restart_required: false },
  ] }, isLoading: false }),
  useUpdateSettings: () => ({ mutate: mocks.saveSettings, isPending: false }),
  useUpdateDaemonRunLimit: () => ({ mutate: mocks.saveLimit, isPending: false }),
}))
vi.mock('@/stores/auth', () => ({ useAuthStore: (selector: (state: unknown) => unknown) => selector({ user: { is_admin: mocks.admin } }) }))
vi.mock('@tanstack/react-router', () => ({ Link: ({ children }: { children: React.ReactNode }) => <span>{children}</span> }))
vi.mock('sonner', () => ({ toast: { error: vi.fn(), success: vi.fn() } }))

const daemon: Daemon = { id: 'daemon', owner_id: null, machine_id: 'remote', hostname: 'Remote', os: 'linux', arch: 'x64',
  status: 'online', detected_clis: [], labels: {}, version: 3, created_at: '', updated_at: '',
  max_concurrent_runs: 6, run_limit: 2, effective_max_concurrent_runs: 2 }

beforeEach(() => { vi.clearAllMocks(); mocks.admin = true })
describe('machine capacity settings', () => {
  it('shows automatic capacity and saves a live cap or automatic reset', () => {
    render(<ForgeSettingsPage />)
    expect(screen.getByText('Automatic (4)')).toBeTruthy()
    const input = screen.getByLabelText('Max concurrent runs')
    fireEvent.change(input, { target: { value: '2' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))
    expect(mocks.saveSettings.mock.calls[0][0].server.max_concurrent_runs).toBe(2)
    fireEvent.change(input, { target: { value: '' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))
    expect(mocks.saveSettings.mock.calls[1][0].server.max_concurrent_runs).toBeNull()
  })
  it('saves a version-checked admin limit and allows clearing it', () => {
    render(<MachineRunCapacity daemon={daemon} />)
    expect(screen.getByText(/Reported cap:/).textContent).toContain('6')
    expect(screen.getByText(/Effective cap:/).textContent).toContain('2')
    fireEvent.change(screen.getByLabelText('Admin limit'), { target: { value: '1' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save limit' }))
    expect(mocks.saveLimit.mock.calls[0][0]).toEqual({ id: 'daemon', version: 3, run_limit: 1 })
    fireEvent.change(screen.getByLabelText('Admin limit'), { target: { value: '' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save limit' }))
    expect(mocks.saveLimit.mock.calls[1][0].run_limit).toBeNull()
  })
  it('keeps an edited limit when a periodic report advances the version', () => {
    const view = render(<MachineRunCapacity daemon={daemon} />)
    fireEvent.change(screen.getByLabelText('Admin limit'), { target: { value: '1' } })
    view.rerender(<MachineRunCapacity daemon={{ ...daemon, version: 4 }} />)
    fireEvent.click(screen.getByRole('button', { name: 'Save limit' }))
    expect(mocks.saveLimit.mock.calls[0][0]).toEqual({ id: 'daemon', version: 4, run_limit: 1 })
  })
  it('shows a read-only admin limit to non-admins', () => {
    mocks.admin = false
    render(<MachineRunCapacity daemon={daemon} />)
    expect(screen.getByText('Admin limit: 2')).toBeTruthy()
    expect(screen.queryByRole('button', { name: 'Save limit' })).toBeNull()
  })
})

describe('usage index budget setting', () => {
  it('saves an explicit MiB budget, zero, and a default reset', () => {
    render(<ForgeSettingsPage />)
    expect(screen.getByText('In effect: 128 MiB')).toBeTruthy()
    const input = screen.getByLabelText('Usage index memory budget')
    for (const value of ['64', '0', '']) {
      fireEvent.change(input, { target: { value } })
      fireEvent.click(screen.getByRole('button', { name: 'Save' }))
      expect(mocks.saveSettings.mock.calls.at(-1)?.[0].server.usage_index_budget_mb).toBe(value === '' ? null : Number(value))
    }
  })
  it('rejects invalid budgets without saving', () => {
    render(<ForgeSettingsPage />)
    for (const value of ['-1', '1.5', '4294967296']) {
      fireEvent.change(screen.getByLabelText('Usage index memory budget'), { target: { value } })
      fireEvent.click(screen.getByRole('button', { name: 'Save' }))
    }
    expect(mocks.saveSettings).not.toHaveBeenCalled()
  })
})
