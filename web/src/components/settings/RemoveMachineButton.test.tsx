import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { ApiError, apiFetch } from '@/api/client'
import { RemoveMachineButton } from './RemoveMachineButton'
import { useAuthStore } from '@/stores/auth'
import type { Daemon } from '@/types/generated'

vi.mock('@/api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/client')>()),
  apiFetch: vi.fn(),
}))
vi.mock('sonner', () => ({ toast: { success: vi.fn() } }))

const daemon: Daemon = {
  id: 'dead',
  machine_id: 'workstation',
  hostname: 'Lost workstation',
  os: 'linux',
  arch: 'x86_64',
  owner_id: 'owner',
  status: 'offline',
  max_concurrent_runs: 4,
  run_limit: null,
  effective_max_concurrent_runs: 4,
  detected_clis: [],
  labels: {},
  version: 1,
  created_at: '2026-10-05T00:00:00Z',
  updated_at: '2026-10-05T00:00:00Z',
}
const result = {
  id: 'dead',
  hostname: 'Lost workstation',
  pending_remote_cancels_cleared: 1,
  cleanup_records_cleared: 0,
  provisioning_attempts_cleared: 0,
  readiness_records_cleared: 0,
  placements_failed: 2,
  tasks_to_replace: 2,
  agents_retired: 1,
  tasks_queued: 3,
}
const preview = { id: 'dead', tasks_to_replace: 2, agents_to_retire: 1 }
const removalCalls = () =>
  vi.mocked(apiFetch).mock.calls.filter(([, init]) => init?.method === 'DELETE')

function mount(machine = daemon, onRemoved = vi.fn()) {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  })
  const invalidate = vi.spyOn(client, 'invalidateQueries')
  const view = render(
    <QueryClientProvider client={client}>
      <RemoveMachineButton daemon={machine} onRemoved={onRemoved} />
    </QueryClientProvider>,
  )
  return { ...view, client, invalidate, onRemoved }
}

beforeEach(() => {
  vi.clearAllMocks()
  useAuthStore.setState({
    user: {
      id: 'owner',
      email: 'owner@example.com',
      display_name: null,
      is_admin: false,
      created_at: '2026-10-05T00:00:00Z',
    },
  })
  vi.mocked(apiFetch).mockImplementation(async (path: string) =>
    path.endsWith('/removal') ? preview : result,
  )
})

describe('machine removal', () => {
  it('requires confirmation, preserves cancel, and sends DELETE with cache refresh', async () => {
    const { onRemoved, invalidate } = mount()
    fireEvent.click(screen.getByRole('button', { name: 'Remove' }))
    expect(screen.getByRole('dialog')).toBeTruthy()
    expect(screen.getByRole('heading', { name: 'Remove Lost workstation?' })).toBeTruthy()
    expect(screen.getByText(/Execution\s+history keeps/)).toBeTruthy()
    expect(removalCalls()).toHaveLength(0)
    fireEvent.click(screen.getByRole('button', { name: 'Cancel' }))
    expect(removalCalls()).toHaveLength(0)
    fireEvent.click(screen.getByRole('button', { name: 'Remove' }))
    fireEvent.click(screen.getByRole('button', { name: 'Remove machine' }))
    await waitFor(() => expect(onRemoved).toHaveBeenCalledOnce())
    expect(apiFetch).toHaveBeenCalledWith('/daemons/dead', { method: 'DELETE' })
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ['daemons'] })
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ['operations', 'status'] })
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ['tasks'] })
  })

  it('disables connected machines with an accessible stop-daemon hint', () => {
    mount({ ...daemon, status: 'online' })
    const button = screen.getByRole('button', { name: 'Remove' })
    expect(button).toHaveProperty('disabled', true)
    const hint = document.getElementById(button.getAttribute('aria-describedby') ?? '')
    expect(hint?.textContent).toBe('Stop the daemon before removing this machine.')
    fireEvent.click(button)
    expect(screen.queryByRole('dialog')).toBeNull()
    expect(apiFetch).not.toHaveBeenCalled()
  })

  it('disables the embedded machine even when offline', () => {
    mount({ ...daemon, machine_id: 'embedded:server:linux:x86_64' })
    expect(screen.getByRole('button', { name: 'Remove' })).toHaveProperty('disabled', true)
    expect(screen.getByText('The embedded server machine cannot be removed.')).toBeTruthy()
  })

  it('states how many Tasks will be re-placed and that unpushed work is abandoned', async () => {
    mount()
    fireEvent.click(screen.getByRole('button', { name: 'Remove' }))
    expect(apiFetch).toHaveBeenCalledWith('/daemons/dead/removal')
    await waitFor(() =>
      expect(screen.getByRole('status').textContent).toBe(
        '2 Tasks have a workspace on this machine and will be re-placed on another machine. 1 Agent pinned to it will be retired.',
      ),
    )
    expect(screen.getByText(/that was not pushed is abandoned/)).toBeTruthy()
    expect(removalCalls()).toHaveLength(0)
  })

  it('offers removal to an administrator and hides it from any other user', () => {
    const other = {
      id: 'other',
      email: 'other@example.com',
      display_name: null,
      is_admin: false,
      created_at: '2026-10-05T00:00:00Z',
    }
    useAuthStore.setState({ user: other })
    const first = mount()
    expect(screen.queryByRole('button', { name: 'Remove' })).toBeNull()
    first.unmount()
    useAuthStore.setState({ user: { ...other, is_admin: true } })
    mount()
    expect(screen.getByRole('button', { name: 'Remove' })).toHaveProperty('disabled', false)
  })

  it('shows a racing connected conflict and refreshes the machine query', async () => {
    vi.mocked(apiFetch).mockImplementation(async (path: string) => {
      if (path.endsWith('/removal')) return preview
      throw new ApiError('Stop the daemon first', 409)
    })
    const { onRemoved, invalidate } = mount()
    fireEvent.click(screen.getByRole('button', { name: 'Remove' }))
    fireEvent.click(screen.getByRole('button', { name: 'Remove machine' }))
    await waitFor(() => expect(screen.getByRole('alert').textContent).toBe('Stop the daemon first'))
    expect(screen.getByRole('dialog')).toBeTruthy()
    expect(onRemoved).not.toHaveBeenCalled()
    expect(invalidate).toHaveBeenCalledWith({ queryKey: ['daemons'] })
  })

  it('disables both dialog actions while removal is pending', async () => {
    let resolve: (value: typeof result) => void = () => {}
    vi.mocked(apiFetch).mockImplementation((path: string) =>
      path.endsWith('/removal')
        ? Promise.resolve(preview)
        : new Promise((done) => {
            resolve = done
          }),
    )
    mount()
    fireEvent.click(screen.getByRole('button', { name: 'Remove' }))
    fireEvent.click(screen.getByRole('button', { name: 'Remove machine' }))
    await waitFor(() =>
      expect(screen.getByRole('button', { name: 'Removing…' })).toHaveProperty('disabled', true),
    )
    expect(screen.getByRole('button', { name: 'Cancel' })).toHaveProperty('disabled', true)
    resolve(result)
    await waitFor(() => expect(screen.queryByRole('dialog')).toBeNull())
  })
})
