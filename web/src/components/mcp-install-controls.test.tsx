import { render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { apiFetch } from '@/api/client'
import { McpInstallControls } from './mcp-install-controls'
import { useAuthStore } from '@/stores/auth'

vi.mock('@/api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/client')>()),
  apiFetch: vi.fn(),
}))
vi.mock('sonner', () => ({ toast: { success: vi.fn(), error: vi.fn() } }))

const user = {
  id: 'member',
  email: 'member@example.com',
  display_name: null,
  is_admin: false,
  created_at: '2026-10-05T00:00:00Z',
}

const configCalls = () =>
  vi.mocked(apiFetch).mock.calls.filter(([path]) => path === '/config/mcp')

function mount() {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  })
  return render(
    <QueryClientProvider client={client}>
      <McpInstallControls scope="project" projectId="project-1" />
    </QueryClientProvider>,
  )
}

beforeEach(() => {
  vi.clearAllMocks()
  vi.mocked(apiFetch).mockImplementation(async (path: string) =>
    path === '/clis'
      ? { items: [] }
      : {
          installed: false,
          url: null,
          expected_url: 'http://forge.test/mcp?project_id=project-1',
          config_path: '/srv/repo/.claude/settings.json',
          agents: ['claude', 'cursor', 'codex'],
        },
  )
})

describe('MCP install controls', () => {
  it('gives a non-administrator the URL without reading or offering server configuration', () => {
    useAuthStore.setState({ user })
    mount()
    expect(screen.getByText(`${window.location.origin}/mcp?project_id=project-1`)).toBeTruthy()
    expect(screen.getByText(/requires a Forge administrator/)).toBeTruthy()
    expect(screen.queryByRole('button', { name: 'Install' })).toBeNull()
    expect(screen.queryByText('/srv/repo/.claude/settings.json')).toBeNull()
    expect(configCalls()).toHaveLength(0)
  })

  it('reads server configuration and offers installs to an administrator', async () => {
    useAuthStore.setState({ user: { ...user, is_admin: true } })
    mount()
    await waitFor(() => expect(configCalls()).toHaveLength(3))
    await waitFor(() =>
      expect(screen.getByText('http://forge.test/mcp?project_id=project-1')).toBeTruthy(),
    )
    expect(screen.getAllByRole('button', { name: 'Install' }).length).toBeGreaterThan(0)
  })
})
