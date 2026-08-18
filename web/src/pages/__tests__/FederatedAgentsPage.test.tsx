import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { fireEvent, render, screen, within } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { FederatedAgentsPage } from '@/pages/FederatedAgentsPage'
import type { AgentChatEntry } from '@/features/agent-chat/types'
import type { FederatedAgent } from '@/features/federation/types'
import type {
  ProviderAuthorizationOperationResponse,
  ProviderEntryResponse,
} from '@/types/generated'

const createEmbeddedAgent = vi.fn().mockResolvedValue({})
const registerHarnessAgent = vi.fn().mockResolvedValue({})
const connectProfile = vi.fn().mockResolvedValue({})
const createProviderEntry = vi.fn()
const removeProviderEntry = vi.fn().mockResolvedValue({ provider_revocation: 'succeeded' })
const cancelAuthorization = vi.fn().mockResolvedValue({})
const updateAgent = vi.fn().mockResolvedValue({})
const testProviderEntry = vi.fn().mockResolvedValue({
  status: 'ok',
  latency_ms: 123,
  message: null,
  checked_at: '2026-08-15T00:00:00Z',
})

vi.mock('@/features/federation/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/features/federation/api')>()),
  testProviderEntry: (id: string) => testProviderEntry(id) as Promise<unknown>,
}))
let authorizationOperation: ProviderAuthorizationOperationResponse | undefined
const startAuthorization = vi.fn().mockImplementation(async () => {
  authorizationOperation = {
    id: 'authorization-1',
    provider: 'openai',
    method: 'device_oauth',
    state: 'polling',
    authorization_url: 'https://auth.example/device',
    user_code: 'ABCD-EFGH',
    expires_at: '2026-08-14T16:00:00Z',
    poll_interval_seconds: 5,
    credential_handle_id: null,
    error_code: null,
    error_message: null,
    version: 1,
    created_at: '2026-08-14T15:50:00Z',
    updated_at: '2026-08-14T15:50:00Z',
    completed_at: null,
  }
  return authorizationOperation
})

vi.mock('@tanstack/react-router', () => ({
  Link: ({ children }: { children: React.ReactNode }) => <a>{children}</a>,
  useSearch: () => ({}),
}))
vi.mock('@/stores/auth', () => ({
  useAuthStore: (selector: (state: { user: null }) => unknown) => selector({ user: null }),
}))
// The Projects section on the Bindings tab lists every project; keep it empty here.
vi.mock('@/api/hooks', () => ({
  useProjectsQuery: () => ({ data: { items: [] }, isLoading: false, isError: false, refetch: vi.fn() }),
  useUpdateAgent: () => ({ mutateAsync: updateAgent, isPending: false }),
}))
// The agent dialogs reuse the launch-dialog selectors, which need discovered
// options to offer models and reasoning efforts.
vi.mock('@/hooks/useDiscoveredOptions', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useDiscoveredOptions')>()),
  useDiscoveredOptions: () => ({
    data: {
      models: [
        {
          id: 'gpt-5.2-codex',
          displayName: 'GPT-5.2 Codex',
          provider: 'OpenAI',
          reasoningOptions: ['medium', 'high'],
        },
      ],
      reasoningOptions: [
        { id: 'medium', label: 'Medium' },
        { id: 'high', label: 'High' },
      ],
      permissionPolicies: ['auto', 'supervised', 'plan'],
    },
    isLoading: false,
    isFetching: false,
    isError: false,
  }),
}))
vi.mock('@/features/federation/hooks', () => ({
  isVersionConflict: () => false,
  useFederatedAgentsQuery: () => ({
    data: { items: [agent, cliAgent], has_more: false },
    isLoading: false,
    isError: false,
    refetch: vi.fn(),
  }),
  useCreateEmbeddedAgentMutation: () => ({ mutateAsync: createEmbeddedAgent, isPending: false }),
  useRegisterHarnessAgentMutation: () => ({
    mutateAsync: registerHarnessAgent,
    isPending: false,
  }),
  useConnectEmbeddedProfileMutation: () => ({
    mutateAsync: connectProfile,
    isPending: false,
  }),
  useAgentProviderCapabilitiesQuery: () => ({
    data: {
      items: [
        {
          provider: 'openai',
          display_name: 'OpenAI',
          default_base_url: 'https://api.openai.com/v1',
          default_model: 'gpt-5',
          model_discovery: true,
          credential_methods: [
            {
              method: 'api_key',
              action_label: 'Use OpenAI API key',
              support_level: 'stable',
              configured: true,
              setup_guidance: null,
              boundary_note: 'OpenAI Platform credentials.',
              runtimes: [
                { runtime: 'direct', support_level: 'stable', reason: null },
                { runtime: 'codex', support_level: 'stable', reason: null },
              ],
            },
            {
              method: 'browser_oauth',
              action_label: 'Continue with ChatGPT',
              support_level: 'experimental',
              configured: true,
              setup_guidance: null,
              boundary_note: 'Direct ChatGPT adapter.',
              runtimes: [
                { runtime: 'direct', support_level: 'experimental', reason: null },
                {
                  runtime: 'codex',
                  support_level: 'unavailable',
                  reason:
                    "OAuth handoff into the Codex CLI is not supported; use the CLI's own login",
                },
              ],
            },
            {
              method: 'device_oauth',
              action_label: 'Use ChatGPT device code',
              support_level: 'experimental',
              configured: true,
              setup_guidance: null,
              boundary_note: 'Device-code fallback.',
              runtimes: [{ runtime: 'direct', support_level: 'experimental', reason: null }],
            },
          ],
        },
      ],
    },
    isLoading: false,
    isError: false,
    refetch: vi.fn(),
  }),
  useProvidersQuery: () => ({
    data: { items: providerEntries, cli_runtimes: cliRuntimes },
    isLoading: false,
    isError: false,
    refetch: vi.fn(),
  }),
  useProviderUsageQuery: () => ({
    data: {
      id: 'credential-1',
      provider: 'openai',
      source: 'probe',
      windows: [
        { id: 'primary', used_percent: 42, window_minutes: 300, resets_at: '2026-08-16T06:00:00Z' },
      ],
      fetched_at: '2026-08-16T00:00:00Z',
    },
    isLoading: false,
    isError: false,
    isFetching: false,
    refetch: vi.fn(),
  }),
  useProjectAgentBindingQuery: () => ({
    data: undefined,
    isLoading: false,
    isError: false,
    refetch: vi.fn(),
  }),
  useSetProjectAgentBindingMutation: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useCreateProviderEntryMutation: () => ({ mutateAsync: createProviderEntry, isPending: false }),
  useRenameProviderEntryMutation: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useRemoveProviderEntryMutation: () => ({ mutateAsync: removeProviderEntry, isPending: false }),
  useProviderAuthorizationQuery: () => ({ data: authorizationOperation }),
  useStartProviderAuthorizationMutation: () => ({
    mutateAsync: startAuthorization,
    isPending: false,
  }),
  useCancelProviderAuthorizationMutation: () => ({
    mutateAsync: cancelAuthorization,
    isPending: false,
  }),
  useMainAgentBindingQuery: () => ({
    data: undefined,
    isLoading: false,
    isError: false,
    refetch: vi.fn(),
  }),
  useSetMainAgentBindingMutation: () => ({ mutateAsync: vi.fn(), isPending: false }),
}))
vi.mock('@/features/agent-chat/hooks', () => ({
  useAgentChatsQuery: () => ({
    data: { items: chatEntries },
    isLoading: false,
    isError: false,
    refetch: vi.fn(),
  }),
}))

const chatEntries: AgentChatEntry[] = [
  {
    chat_id: 'main-chat',
    kind: 'main',
    project_id: null,
    project_name: null,
    identity_id: 'agent-1',
    identity_name: 'Forge Guide',
    binding_state: 'active',
    chat_status: 'ready',
    unread_count: 0n,
    pending_turn_count: 0n,
    last_message_at: null,
  },
]

const providerEntries: ProviderEntryResponse[] = [
  {
    id: 'credential-1',
    provider: 'openai',
    label: 'Personal ChatGPT',
    credential_method: 'oauth_bundle',
    status: 'configured',
    base_url: 'https://chatgpt.com/backend-api/codex',
    provider_account_id: 'acct-123',
    used_by: [{ agent_id: 'agent-1', agent_name: 'Forge Guide', runtime: 'direct' }],
    last_used_at: '2026-08-14T12:00:00Z',
    version: 1,
    created_at: '2026-08-12T11:00:00Z',
    updated_at: '2026-08-12T12:00:00Z',
  },
  {
    id: 'credential-2',
    provider: 'openai',
    label: 'Work key',
    credential_method: 'api_key',
    status: 'configured',
    base_url: 'https://api.openai.com/v1',
    provider_account_id: null,
    used_by: [],
    last_used_at: null,
    version: 1,
    created_at: '2026-08-12T11:00:00Z',
    updated_at: '2026-08-12T12:00:00Z',
  },
]

const cliRuntimes = [
  {
    kind: 'claude_code',
    daemon_id: 'daemon-1',
    daemon_hostname: 'forge-host',
    daemon_status: 'online',
    availability: 'unauthenticated',
    version: '2.1.0',
    login_hint: 'Run `claude` on the host and complete its login',
    used_by: [],
  },
  {
    kind: 'codex',
    daemon_id: 'daemon-1',
    daemon_hostname: 'forge-host',
    daemon_status: 'online',
    availability: 'authenticated',
    version: '0.9.4',
    login_hint: null,
    used_by: [],
  },
]

const agent: FederatedAgent = {
  id: 'agent-1',
  name: 'Forge Guide',
  description: 'A bounded account assistant.',
  profile_id: 'profile-1',
  backend_kind: 'native',
  executor_type: 'embedded',
  provider: 'openai',
  model: 'gpt-test',
  reasoning_effort: null,
  permission_policy: 'scoped_proposals',
  prompt_template: null,
  capabilities: ['read_account'],
  config_json: {},
  credential_handle_id: 'credential-1',
  daemon_id: null,
  max_concurrent_tasks: 1,
  status: 'idle',
  active_task_count: 0,
  effective_status: 'ready',
  total_runs: 4,
  avg_duration_ms: 500,
  success_rate: 1,
  total_input_tokens: 400,
  total_output_tokens: 100,
  total_cache_read_tokens: 50,
  total_cache_write_tokens: 20,
  total_tokens: 570,
  total_cost_usd: 0.05,
  is_default: false,
  paused: false,
  owner_id: 'user-1',
  visibility: 'private',
  version: 1,
  created_at: '2026-08-12T11:00:00Z',
  updated_at: '2026-08-12T12:00:00Z',
}

const cliAgent: FederatedAgent = {
  id: 'agent-2',
  name: 'Codex Runner',
  description: 'A CLI-harness worker.',
  profile_id: 'profile-2',
  backend_kind: 'harness',
  executor_type: 'codex',
  provider: 'openai',
  model: 'gpt-5-codex',
  reasoning_effort: 'medium',
  permission_policy: 'scoped_proposals',
  prompt_template: null,
  capabilities: [],
  config_json: {},
  credential_handle_id: 'credential-2',
  daemon_id: null,
  max_concurrent_tasks: 1,
  status: 'idle',
  active_task_count: 0,
  effective_status: 'ready',
  total_runs: 0,
  avg_duration_ms: null,
  success_rate: null,
  total_input_tokens: 0,
  total_output_tokens: 0,
  total_cache_read_tokens: 0,
  total_cache_write_tokens: 0,
  total_tokens: 0,
  total_cost_usd: null,
  is_default: false,
  paused: false,
  owner_id: 'user-1',
  visibility: 'private',
  version: 3,
  created_at: '2026-08-12T11:00:00Z',
  updated_at: '2026-08-12T12:00:00Z',
}

function renderPage() {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false },
    },
  })
  render(
    <QueryClientProvider client={queryClient}>
      <FederatedAgentsPage />
    </QueryClientProvider>,
  )
}

function openProvidersTab() {
  fireEvent.click(screen.getByRole('tab', { name: /providers/i }))
}

describe('FederatedAgentsPage', () => {
  beforeEach(() => {
    authorizationOperation = undefined
    createEmbeddedAgent.mockClear()
    registerHarnessAgent.mockClear()
    connectProfile.mockClear()
    createProviderEntry.mockClear()
    createProviderEntry.mockResolvedValue(providerEntries[1])
    removeProviderEntry.mockClear()
    startAuthorization.mockClear()
    cancelAuthorization.mockClear()
    updateAgent.mockClear()
  })

  it('defaults to the agents roster and keeps bindings on their own tab', () => {
    renderPage()
    expect(screen.getByRole('tab', { name: /agents/i }).getAttribute('aria-selected')).toBe('true')
    expect(screen.getByText('Forge Guide')).toBeTruthy()
    expect(screen.queryByText('Main and Project Agent bindings')).toBeNull()
    fireEvent.click(screen.getByRole('tab', { name: /bindings/i }))
    expect(
      screen.getByRole('tab', { name: /bindings/i }).getAttribute('aria-selected'),
    ).toBe('true')
    expect(screen.getByText('Main and Project Agent bindings')).toBeTruthy()
    expect(screen.getByText('Global · Main')).toBeTruthy()
    // The Projects section is always present on the Bindings tab, never gated on `?project=`.
    expect(screen.getByText('Project Agent bindings')).toBeTruthy()
  })

  it('shows an agent\'s settings in the panel and edits a direct agent inline', async () => {
    renderPage()
    fireEvent.click(screen.getByText('Forge Guide'))
    // The panel shows the agent's settings directly — no profile list, no dialog.
    expect(screen.queryByText(/profiles/i)).toBeNull()
    expect(screen.getByText('Harness')).toBeTruthy()
    // The save bar stays disabled until something actually changes.
    const save = screen.getByRole('button', { name: /save settings/i }) as HTMLButtonElement
    expect(save.disabled).toBe(true)
    fireEvent.change(screen.getByLabelText('Model'), { target: { value: 'gpt-6' } })
    expect(save.disabled).toBe(false)
    fireEvent.click(save)
    await vi.waitFor(() =>
      expect(connectProfile).toHaveBeenCalledWith({
        identityId: 'agent-1',
        input: expect.objectContaining({
          credential_id: 'credential-1',
          model: 'gpt-6',
          version: 1,
        }),
      }),
    )
    expect(updateAgent).not.toHaveBeenCalled()
  })

  it('discards inline edits back to the agent\'s current settings', () => {
    renderPage()
    fireEvent.click(screen.getByText('Forge Guide'))
    fireEvent.change(screen.getByLabelText('Model'), { target: { value: 'gpt-6' } })
    fireEvent.click(screen.getByRole('button', { name: /discard/i }))
    expect((screen.getByLabelText('Model') as HTMLInputElement).value).toBe('gpt-test')
    expect(connectProfile).not.toHaveBeenCalled()
  })

  it('edits a CLI-harness agent inline through the core agent PATCH', async () => {
    renderPage()
    fireEvent.click(screen.getByText('Codex Runner'))

    // The panel reuses the launch-dialog selectors: model and reasoning come
    // from discovery, policy from the shared PolicySelector.
    fireEvent.click(screen.getByLabelText('Model'))
    fireEvent.click(within(screen.getByRole('listbox')).getByRole('option', { name: /GPT-5\.2 Codex/i }))
    fireEvent.click(screen.getByLabelText('Reasoning'))
    fireEvent.click(within(screen.getByRole('listbox')).getByRole('option', { name: /High/ }))
    fireEvent.click(screen.getByLabelText('Policy'))
    fireEvent.click(within(screen.getByRole('listbox')).getByRole('option', { name: /Supervised/ }))
    fireEvent.click(screen.getByRole('button', { name: /save settings/i }))

    await vi.waitFor(() =>
      expect(updateAgent).toHaveBeenCalledWith({
        agentId: 'agent-2',
        body: {
          name: 'Codex Runner',
          description: 'A CLI-harness worker.',
          model: 'gpt-5.2-codex',
          reasoning_effort: 'high',
          permission_policy: 'supervised',
          prompt_template: null,
          version: 3,
        },
      }),
    )
    expect(connectProfile).not.toHaveBeenCalled()
  })

  it('lists provider entries and CLI runtimes with usage on the providers tab', () => {
    renderPage()
    openProvidersTab()
    expect(screen.getByText('Personal ChatGPT')).toBeTruthy()
    expect(screen.getByText('Work key')).toBeTruthy()
    expect(screen.getByRole('button', { name: /used by 1 agent/i })).toBeTruthy()
    expect(screen.getByText(/Claude Code harness/)).toBeTruthy()
    expect(screen.getByText(/Run `claude` on the host/)).toBeTruthy()
    expect(screen.getAllByText(/42% used · resets/).length).toBeGreaterThan(0)
  })

  it('creates an API-key provider entry step by step and tests the connection', async () => {
    renderPage()
    openProvidersTab()
    fireEvent.click(screen.getByRole('button', { name: /add provider/i }))
    const wizard = within(screen.getByRole('dialog'))
    expect(wizard.getByText(/step 1 of 4/i)).toBeTruthy()
    fireEvent.click(wizard.getByRole('button', { name: /OpenAI/i }))
    expect(wizard.getByText(/step 2 of 4/i)).toBeTruthy()
    fireEvent.click(wizard.getByRole('button', { name: /use openai api key/i }))
    expect(wizard.getByText(/step 3 of 4/i)).toBeTruthy()
    fireEvent.change(wizard.getByLabelText('API key'), { target: { value: 'secret-value' } })
    fireEvent.click(wizard.getByRole('button', { name: /^add provider$/i }))
    await vi.waitFor(() =>
      expect(createProviderEntry).toHaveBeenCalledWith(
        expect.objectContaining({ provider: 'openai', credential: 'secret-value' }),
      ),
    )
    expect(createEmbeddedAgent).not.toHaveBeenCalled()
    expect(registerHarnessAgent).not.toHaveBeenCalled()
    expect(await screen.findByText(/is connected\. No agent was created\./)).toBeTruthy()
    await vi.waitFor(() => expect(testProviderEntry).toHaveBeenCalledWith('credential-2'))
    expect(await screen.findByText(/Provider responding · 123 ms/)).toBeTruthy()
    expect(
      screen.getByRole('button', { name: /create an agent with this provider/i }),
    ).toBeTruthy()
  })

  it('renders and cancels a device authorization operation without exposing tokens', async () => {
    renderPage()
    openProvidersTab()
    fireEvent.click(screen.getByRole('button', { name: /add provider/i }))
    const wizard = within(screen.getByRole('dialog'))
    fireEvent.click(wizard.getByRole('button', { name: /OpenAI/i }))
    fireEvent.click(wizard.getByRole('button', { name: /use chatgpt device code/i }))
    fireEvent.click(wizard.getByRole('button', { name: /start authorization/i }))

    expect(await screen.findByText('ABCD-EFGH')).toBeTruthy()
    expect(screen.queryByText(/access_token|refresh_token/i)).toBeNull()
    fireEvent.click(screen.getByRole('button', { name: /cancel authorization/i }))
    await vi.waitFor(() =>
      expect(cancelAuthorization).toHaveBeenCalledWith({
        id: 'authorization-1',
        input: { expected_version: 1 },
      }),
    )
  })

  it('starts only one provider authorization for a repeated form submission', () => {
    startAuthorization.mockReturnValueOnce(new Promise(() => {}))
    renderPage()
    openProvidersTab()
    fireEvent.click(screen.getByRole('button', { name: /add provider/i }))
    const wizard = within(screen.getByRole('dialog'))
    fireEvent.click(wizard.getByRole('button', { name: /OpenAI/i }))
    fireEvent.click(wizard.getByRole('button', { name: /use chatgpt device code/i }))
    const form = screen.getByRole('button', { name: /start authorization/i }).closest('form')
    expect(form).toBeTruthy()
    fireEvent.submit(form!)
    fireEvent.submit(form!)

    expect(startAuthorization).toHaveBeenCalledTimes(1)
  })

  it('warns with dependent agents before disconnecting an entry', async () => {
    renderPage()
    openProvidersTab()
    const disconnectButtons = screen.getAllByRole('button', { name: /^disconnect$/i })
    fireEvent.click(disconnectButtons[0])
    expect(screen.getByText(/1 agent reference this entry \(Forge Guide\)/)).toBeTruthy()
    const confirm = screen
      .getByRole('alertdialog')
      .querySelector('button') as HTMLButtonElement
    fireEvent.click(confirm)
    await vi.waitFor(() =>
      expect(removeProviderEntry).toHaveBeenCalledWith({ handleId: 'credential-1', version: 1 }),
    )
  })

  it('creates a direct agent from a provider entry through the two-step wizard', async () => {
    renderPage()
    fireEvent.click(screen.getAllByRole('button', { name: /new agent/i })[0])
    const wizard = within(screen.getByRole('dialog'))
    // Step 1 groups direct provider entries and CLI harnesses (with versions).
    expect(wizard.getByText('Claude Code harness')).toBeTruthy()
    expect(wizard.getByText('v2.1.0')).toBeTruthy()
    fireEvent.click(wizard.getByRole('button', { name: /Openai · Work key/i }))
    fireEvent.change(wizard.getByLabelText('Agent name'), { target: { value: 'Main guide' } })
    fireEvent.change(wizard.getByLabelText('Model'), { target: { value: 'gpt-5' } })
    fireEvent.click(wizard.getByRole('button', { name: /create agent/i }))
    await vi.waitFor(() =>
      expect(createEmbeddedAgent).toHaveBeenCalledWith(
        expect.objectContaining({
          name: 'Main guide',
          credential_id: 'credential-2',
          model: 'gpt-5',
        }),
      ),
    )
  })

  it('creates a harness agent, optionally powered by a compatible provider entry', async () => {
    renderPage()
    fireEvent.click(screen.getAllByRole('button', { name: /new agent/i })[0])
    const wizard = within(screen.getByRole('dialog'))
    fireEvent.click(wizard.getByRole('button', { name: /Codex CLI harness/i }))
    fireEvent.change(wizard.getByLabelText('Agent name'), { target: { value: 'Codex worker' } })
    // Only entries the capability catalog admits for this harness are offered.
    fireEvent.click(wizard.getByLabelText('Harness credential'))
    fireEvent.click(within(screen.getByRole('listbox')).getByRole('option', { name: /Work key/i }))
    fireEvent.click(wizard.getByRole('button', { name: /create agent/i }))
    await vi.waitFor(() =>
      expect(registerHarnessAgent).toHaveBeenCalledWith(
        expect.objectContaining({
          name: 'Codex worker',
          executor_type: 'codex',
          credential_id: 'credential-2',
        }),
      ),
    )
  })
})
