import { fireEvent, render, screen, waitFor } from '@testing-library/react'
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

const configCalls = () => vi.mocked(apiFetch).mock.calls.filter(([path]) => path === '/config/mcp')

function mount(scope: 'project' | 'local' | 'user' = 'project', projectId = 'project-1') {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  })
  return render(
    <QueryClientProvider client={client}>
      <McpInstallControls scope={scope} projectId={scope === 'user' ? undefined : projectId} />
    </QueryClientProvider>,
  )
}

beforeEach(() => {
  vi.clearAllMocks()
  vi.mocked(apiFetch).mockImplementation(async (path: string) =>
    path === '/clis'
      ? { items: [] }
      : path === '/config/mcp/endpoint'
        ? { url: 'https://public.forge.test/mcp?project_id=project-1' }
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
  it('gives a non-administrator the configured public URL without reading server configuration', async () => {
    useAuthStore.setState({ user })
    mount()
    const url = 'https://public.forge.test/mcp?project_id=project-1'
    expect(await screen.findByText(url)).toBeTruthy()
    expect(apiFetch).toHaveBeenCalledWith('/config/mcp/endpoint', {
      search: { scope: 'project', project_id: 'project-1' },
    })
    const writeText = vi.fn().mockResolvedValue(undefined)
    Object.defineProperty(navigator, 'clipboard', { configurable: true, value: { writeText } })
    fireEvent.click(screen.getByRole('button', { name: 'Copy to clipboard' }))
    expect(writeText).toHaveBeenCalledWith(url)
    expect(screen.getByText(/requires a Forge administrator/)).toBeTruthy()
    expect(screen.queryByRole('button', { name: 'Install' })).toBeNull()
    expect(screen.queryByText('/srv/repo/.claude/settings.json')).toBeNull()
    expect(configCalls()).toHaveLength(0)
  })

  it.each(['local', 'user'] as const)('reads the server endpoint in %s scope', async (scope) => {
    useAuthStore.setState({ user })
    vi.mocked(apiFetch).mockResolvedValue({ url: 'https://public.forge.test/mcp' })
    mount(scope)
    expect(await screen.findByText('https://public.forge.test/mcp')).toBeTruthy()
    expect(apiFetch).toHaveBeenCalledWith('/config/mcp/endpoint', {
      search: scope === 'user' ? { scope } : { scope, project_id: 'project-1' },
    })
    expect(configCalls()).toHaveLength(0)
  })

  it('does not offer a browser-derived URL while loading or after a refused read', async () => {
    useAuthStore.setState({ user })
    let reject!: (error: Error) => void
    vi.mocked(apiFetch).mockImplementation((path) =>
      path === '/clis'
        ? Promise.resolve({ items: [] })
        : new Promise((_resolve, fail) => {
            reject = fail
          }),
    )
    mount()
    expect(screen.queryByRole('button', { name: 'Copy to clipboard' })).toBeNull()
    reject(new Error('not_found'))
    expect(await screen.findByText('Unable to load MCP URL.')).toBeTruthy()
    expect(screen.queryByText(`${window.location.origin}/mcp?project_id=project-1`)).toBeNull()
    expect(screen.queryByRole('button', { name: 'Copy to clipboard' })).toBeNull()
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
