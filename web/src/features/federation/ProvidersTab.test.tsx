import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type {
  PricingCatalogStatus,
  PricingSettings,
  ProviderEntryResponse,
  SubjectPricingResponse,
} from '@/types/generated'
import { PricingCatalogStatusPanel, ProvidersTab } from './ProvidersTab'

const mocks = vi.hoisted(() => ({
  catalogStatus: {} as PricingCatalogStatus,
  providerPricing: { settings: null } as SubjectPricingResponse,
  refreshCatalog: vi.fn(),
  refreshCatalogStatus: vi.fn(),
  updateProviderPricing: vi.fn(),
  resetProviderPricing: vi.fn(),
  updateCliRuntimePricing: vi.fn(),
  resetCliRuntimePricing: vi.fn(),
  testProviderEntry: vi.fn(),
}))

vi.mock('@/features/federation/api', () => ({
  testProviderEntry: mocks.testProviderEntry,
}))

vi.mock('@/features/federation/hooks', () => ({
  federationQueryKeys: {
    credentials: ['agent-providers', 'credentials'],
    providerUsage: (id: string) => ['agent-providers', id, 'usage'],
    pricingCatalogStatus: ['agent-providers', 'pricing-catalog', 'status'],
  },
  isVersionConflict: (error: unknown) =>
    typeof error === 'object' && error !== null && (error as { status?: unknown }).status === 409,
  useAgentProviderCapabilitiesQuery: () => ({
    data: { items: [] },
    isLoading: false,
    isError: false,
  }),
  useCancelProviderAuthorizationMutation: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useCreateProviderEntryMutation: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useProviderAuthorizationQuery: () => ({ data: undefined }),
  useProviderUsageQuery: () => ({
    data: undefined,
    isLoading: false,
    isError: false,
    isFetching: false,
    refetch: vi.fn(),
  }),
  usePricingCatalogStatusQuery: () => ({
    data: mocks.catalogStatus,
    isLoading: false,
    isError: false,
    refetch: mocks.refreshCatalogStatus,
  }),
  usePricingCatalogModelsQuery: () => ({
    data: { items: [], has_more: false, next_cursor: null },
    isLoading: false,
    isError: false,
  }),
  useRefreshPricingCatalogMutation: () => ({ mutateAsync: mocks.refreshCatalog, isPending: false }),
  useProviderPricingQuery: () => ({
    data: mocks.providerPricing,
    isLoading: false,
    isError: false,
    refetch: vi.fn(),
  }),
  useCliRuntimePricingQuery: () => ({
    data: { settings: null },
    isLoading: false,
    isError: false,
    refetch: vi.fn(),
  }),
  useAgentPricingQuery: () => ({
    data: undefined,
    isLoading: false,
    isError: false,
    refetch: vi.fn(),
  }),
  useUpdateProviderPricingMutation: () => ({
    mutateAsync: mocks.updateProviderPricing,
    isPending: false,
  }),
  useResetProviderPricingMutation: () => ({
    mutateAsync: mocks.resetProviderPricing,
    isPending: false,
  }),
  useUpdateCliRuntimePricingMutation: () => ({
    mutateAsync: mocks.updateCliRuntimePricing,
    isPending: false,
  }),
  useResetCliRuntimePricingMutation: () => ({
    mutateAsync: mocks.resetCliRuntimePricing,
    isPending: false,
  }),
  useUpdateAgentPricingMutation: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useResetAgentPricingMutation: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useRemoveProviderEntryMutation: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useRenameProviderEntryMutation: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useSetCliRuntimeAvailabilityMutation: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useSetProviderEntryAvailabilityMutation: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useStartProviderAuthorizationMutation: () => ({ mutateAsync: vi.fn(), isPending: false }),
}))

const providerEntry: ProviderEntryResponse = {
  id: 'provider-1',
  provider: 'openai',
  label: 'Personal subscription',
  credential_method: 'oauth_bundle',
  status: 'configured',
  enabled: true,
  base_url: 'https://chatgpt.com/backend-api',
  provider_account_id: 'account-1',
  used_by: [],
  last_used_at: null,
  health: null,
  version: 2,
  created_at: '2026-08-01T00:00:00Z',
  updated_at: '2026-08-01T00:00:00Z',
}

const discounted: PricingSettings = {
  mode: 'discount',
  discount_percent: '20',
  fixed_rates: null,
  catalog_provider_id: 'zai',
  catalog_model_id: null,
  version: 3,
}

function renderPanel(entry = providerEntry) {
  render(
    <QueryClientProvider client={new QueryClient()}>
      <ProvidersTab
        entries={[entry]}
        cliRuntimes={[]}
        isLoading={false}
        isError={false}
        onRetry={vi.fn()}
        routeSearch={{}}
        onShowAgents={vi.fn()}
        onCreateAgentWithProvider={vi.fn()}
      />
    </QueryClientProvider>,
  )
}

beforeEach(() => {
  mocks.catalogStatus = {
    state: 'fresh',
    active_snapshot_id: 'snapshot-1',
    revision: 'revision-1',
    etag: 'etag-1',
    fetched_at: '2026-09-01T00:00:00Z',
    last_checked_at: '2026-09-01T00:00:00Z',
    stale_after: '2026-09-08T00:00:00Z',
    last_error_code: null,
  }
  mocks.providerPricing = { settings: null }
  mocks.refreshCatalog.mockReset().mockResolvedValue(mocks.catalogStatus)
  mocks.refreshCatalogStatus.mockReset().mockResolvedValue(mocks.catalogStatus)
  mocks.updateProviderPricing.mockReset().mockResolvedValue({ settings: discounted })
  mocks.resetProviderPricing.mockReset().mockResolvedValue({ settings: null })
  mocks.testProviderEntry.mockReset().mockResolvedValue({
    status: 'ok', latency_ms: 12, message: null, checked_at: '2026-09-23T00:00:00Z',
  })
})

afterEach(() => {
  cleanup()
  vi.clearAllMocks()
})

describe('pricing catalog status', () => {
  it.each([
    ['absent', 'No pricing catalog has been loaded', 'status'],
    ['fresh', 'Pricing catalog is fresh', 'status'],
    ['stale', 'Pricing catalog is stale', 'status'],
    ['refresh_failed', 'Pricing catalog refresh failed', 'alert'],
  ] as const)('renders the %s state with last-good metadata', (state, copy, role) => {
    mocks.catalogStatus = {
      ...mocks.catalogStatus,
      state,
      active_snapshot_id: state === 'absent' ? null : 'last-good-snapshot',
      revision: state === 'absent' ? null : 'last-good-revision',
      last_error_code: state === 'refresh_failed' ? 'upstream_unavailable' : null,
    }
    render(
      <QueryClientProvider client={new QueryClient()}>
        <PricingCatalogStatusPanel />
      </QueryClientProvider>,
    )
    expect(screen.getByRole(role).textContent).toContain(copy)
    if (state !== 'absent') expect(screen.getByText('last-good-snapshot')).toBeTruthy()
  })

  it('keeps last-known-good data visible when refresh fails', async () => {
    mocks.catalogStatus = {
      ...mocks.catalogStatus,
      state: 'stale',
      active_snapshot_id: 'snapshot-lkg',
    }
    mocks.refreshCatalog.mockRejectedValueOnce(new Error('upstream unavailable'))
    render(
      <QueryClientProvider client={new QueryClient()}>
        <PricingCatalogStatusPanel />
      </QueryClientProvider>,
    )
    fireEvent.click(screen.getByRole('button', { name: 'Refresh catalog' }))
    expect((await screen.findByRole('alert')).textContent).toContain('upstream unavailable')
    expect(screen.getByText('snapshot-lkg')).toBeTruthy()
  })
})

describe('provider pricing adjustment', () => {
  it('shows the current summary and provider-wide scope', () => {
    mocks.providerPricing = { settings: discounted }
    renderPanel()
    const section = screen.getByRole('region', { name: 'Pricing' })
    expect(within(section).getByText('20% off list price')).toBeTruthy()
    expect(within(section).getByText('Price as models.dev provider: zai')).toBeTruthy()
    expect(
      within(section).getByText('Applies to every agent using this provider. Agents can override.'),
    ).toBeTruthy()
  })

  it('saves a discount with the current settings version', async () => {
    mocks.providerPricing = { settings: discounted }
    renderPanel()
    const section = within(screen.getByRole('region', { name: 'Pricing' }))
    fireEvent.click(section.getByRole('button', { name: 'Edit' }))
    fireEvent.change(section.getByLabelText('Discount percent'), { target: { value: '12.5' } })
    fireEvent.click(section.getByRole('button', { name: 'Save' }))
    await waitFor(() =>
      expect(mocks.updateProviderPricing).toHaveBeenCalledWith({
        subjectId: 'provider-1',
        input: {
          mode: 'discount',
          discount_percent: '12.5',
          fixed_rates: null,
          catalog_provider_id: 'zai',
          catalog_model_id: null,
          expected_version: 3,
        },
      }),
    )
  })

  it('resets to list price using DELETE with the settings version', async () => {
    mocks.providerPricing = { settings: discounted }
    renderPanel()
    const section = within(screen.getByRole('region', { name: 'Pricing' }))
    fireEvent.click(section.getByRole('button', { name: 'Edit' }))
    fireEvent.click(section.getByRole('button', { name: 'Reset to list price' }))
    await waitFor(() =>
      expect(mocks.resetProviderPricing).toHaveBeenCalledWith({
        subjectId: 'provider-1',
        version: 3,
      }),
    )
  })
})

describe('provider health', () => {
  it('shows a timed backoff and redacted failure instead of Connected', () => {
    renderPanel({
      ...providerEntry,
      health: {
        status: 'backoff',
        consecutive_failures: 2,
        last_error_kind: 'rate_limited',
        last_error_message: 'Provider returned HTTP 429',
        last_failure_at: '2026-09-23T00:00:00Z',
        backoff_until: '2099-01-01T00:00:00Z',
      },
    })
    expect(screen.getByText('Backing off')).toBeTruthy()
    expect(screen.getByText(/Provider returned HTTP 429/)).toBeTruthy()
    expect(screen.queryByText('Connected')).toBeNull()
  })

  it('shows an auth error and runs a live connection test from the card', async () => {
    renderPanel({
      ...providerEntry,
      health: {
        status: 'error',
        consecutive_failures: 1,
        last_error_kind: 'auth',
        last_error_message: 'Provider rejected the credential',
        last_failure_at: '2026-09-23T00:00:00Z',
        backoff_until: null,
      },
    })
    expect(screen.getByText('Error')).toBeTruthy()
    fireEvent.click(screen.getByRole('button', { name: 'Test provider connection' }))
    await waitFor(() => expect(mocks.testProviderEntry).toHaveBeenCalledWith('provider-1'))
    expect(await screen.findByText('Connection test passed · 12 ms')).toBeTruthy()
  })
})
