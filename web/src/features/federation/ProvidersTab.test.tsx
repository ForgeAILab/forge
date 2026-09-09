import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type {
  CatalogModelRate,
  PricingBinding,
  PricingCatalogStatus,
  ProviderEntryResponse,
  ProviderPricing,
} from '@/types/generated'
import { PricingCatalogStatusPanel, ProvidersTab } from './ProvidersTab'

const mocks = vi.hoisted(() => ({
  catalogStatus: {
    state: 'fresh',
    active_snapshot_id: 'snapshot-1',
    revision: 'revision-1',
    etag: 'etag-1',
    fetched_at: '2026-09-01T00:00:00Z',
    last_checked_at: '2026-09-01T00:00:00Z',
    stale_after: '2026-09-08T00:00:00Z',
    last_error_code: null,
  } as PricingCatalogStatus,
  catalogModels: [] as CatalogModelRate[],
  providerPricing: {
    subject_id: 'provider-1',
    subject_revision_digest: 'subject-revision-1',
    version: 3,
    bindings: [],
  } satisfies ProviderPricing,
  refreshCatalog: vi.fn(),
  refreshCatalogStatus: vi.fn(),
  providerPricingRefetch: vi.fn(),
  replaceProviderPricing: vi.fn(),
  replaceCliRuntimePricing: vi.fn(),
}))

vi.mock('@/features/federation/api', () => ({
  testProviderEntry: vi.fn().mockResolvedValue({ status: 'ok', latency_ms: 12, message: null }),
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
  useCliRuntimePricingQuery: () => ({
    data: mocks.providerPricing,
    isLoading: false,
    isError: false,
    refetch: mocks.providerPricingRefetch,
  }),
  useCreateProviderEntryMutation: () => ({ mutateAsync: vi.fn(), isPending: false }),
  usePricingCatalogModelsQuery: () => ({
    data: { items: mocks.catalogModels, has_more: false, next_cursor: null },
    isLoading: false,
    isError: false,
    refetch: vi.fn(),
  }),
  usePricingCatalogStatusQuery: () => ({
    data: mocks.catalogStatus,
    isLoading: false,
    isError: false,
    refetch: mocks.refreshCatalogStatus,
  }),
  useProviderAuthorizationQuery: () => ({ data: undefined }),
  useProviderPricingQuery: () => ({
    data: mocks.providerPricing,
    isLoading: false,
    isError: false,
    refetch: mocks.providerPricingRefetch,
  }),
  useProviderUsageQuery: () => ({
    data: undefined,
    isLoading: false,
    isError: false,
    isFetching: false,
    refetch: vi.fn(),
  }),
  useRefreshPricingCatalogMutation: () => ({
    mutateAsync: mocks.refreshCatalog,
    isPending: false,
  }),
  useRemoveProviderEntryMutation: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useRenameProviderEntryMutation: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useReplaceCliRuntimePricingMutation: () => ({
    mutateAsync: mocks.replaceCliRuntimePricing,
    isPending: false,
  }),
  useReplaceProviderPricingMutation: () => ({
    mutateAsync: mocks.replaceProviderPricing,
    isPending: false,
  }),
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
  version: 2,
  created_at: '2026-08-01T00:00:00Z',
  updated_at: '2026-08-01T00:00:00Z',
}

function bindingFixture(overrides: Partial<PricingBinding> = {}): PricingBinding {
  return {
    id: 'binding-1',
    runtime_model: 'shared-model',
    subject_revision_digest: 'subject-revision-1',
    source_kind: 'models_dev_catalog',
    catalog_provider_id: 'openai',
    catalog_model_id: 'shared-model',
    catalog_rate_revision_id: 'rate-revision-1',
    manual_rates: null,
    effective_at: '2026-09-01T00:00:00Z',
    retired_at: null,
    version: 1,
    ...overrides,
  }
}

function renderPanel() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  })
  return render(
    <QueryClientProvider client={queryClient}>
      <ProvidersTab
        entries={[providerEntry]}
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

function openPricingDialog() {
  renderPanel()
  const configure = screen.getByRole('button', { name: 'Configure pricing' })
  configure.focus()
  fireEvent.click(configure)
  return {
    configure,
    dialog: screen.getByRole('dialog'),
  }
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
  mocks.catalogModels = []
  mocks.providerPricing = {
    subject_id: 'provider-1',
    subject_revision_digest: 'subject-revision-1',
    version: 3,
    bindings: [],
  }
  mocks.refreshCatalog.mockReset().mockResolvedValue(mocks.catalogStatus)
  mocks.refreshCatalogStatus.mockReset().mockResolvedValue(mocks.catalogStatus)
  mocks.providerPricingRefetch.mockReset().mockResolvedValue({ isError: false, error: null })
  mocks.replaceProviderPricing.mockReset().mockResolvedValue(mocks.providerPricing)
  mocks.replaceCliRuntimePricing.mockReset().mockResolvedValue(mocks.providerPricing)
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
    if (state !== 'absent') {
      expect(
        screen.getByText(state === 'refresh_failed' ? 'last-good-snapshot' : 'last-good-snapshot'),
      ).toBeTruthy()
      expect(screen.getAllByText('2026-09-01T00:00:00Z').length).toBeGreaterThanOrEqual(1)
    }
  })

  it('keeps the last-known-good state visible when an explicit refresh fails', async () => {
    mocks.catalogStatus = {
      ...mocks.catalogStatus,
      state: 'stale',
      active_snapshot_id: 'snapshot-lkg',
      revision: 'revision-lkg',
    }
    mocks.refreshCatalog.mockRejectedValueOnce(
      new Error('upstream response body must not be rendered'),
    )
    render(
      <QueryClientProvider client={new QueryClient()}>
        <PricingCatalogStatusPanel />
      </QueryClientProvider>,
    )

    fireEvent.click(screen.getByRole('button', { name: 'Refresh catalog' }))
    expect((await screen.findByRole('alert')).textContent).toContain(
      'upstream response body must not be rendered',
    )
    expect(screen.getByText('snapshot-lkg')).toBeTruthy()
    expect(mocks.refreshCatalogStatus).toHaveBeenCalled()
  })
})

describe('provider pricing configuration', () => {
  it('shows subscription caveat and separates quota Usage from monetary Pricing', () => {
    renderPanel()

    expect(screen.getByText('Pricing')).toBeTruthy()
    expect(screen.getByText(/separate from quota Usage and Rate limits/i)).toBeTruthy()
    expect(screen.getByText(/subscription, private contract, or custom runtime/i)).toBeTruthy()
    expect(screen.queryByText(/^Rate$/i)).toBeNull()
  })

  it('saves an explicit zero manual rate without converting the decimal to a number', async () => {
    mocks.providerPricing = { ...mocks.providerPricing, subject_id: 'provider-1' }
    const { dialog } = openPricingDialog()
    fireEvent.click(within(dialog).getByLabelText('Manual override'))
    fireEvent.change(within(dialog).getByLabelText('Runtime model ID'), {
      target: { value: 'gpt-free' },
    })
    fireEvent.change(within(dialog).getByLabelText('Input · USD per 1M tokens'), {
      target: { value: '0' },
    })
    fireEvent.change(within(dialog).getByLabelText('Output · USD per 1M tokens'), {
      target: { value: '0' },
    })
    fireEvent.click(within(dialog).getByRole('button', { name: 'Add model pricing' }))
    fireEvent.click(within(dialog).getByRole('button', { name: 'Save pricing' }))

    await waitFor(() => expect(mocks.replaceProviderPricing).toHaveBeenCalled())
    const call = mocks.replaceProviderPricing.mock.calls[0][0] as {
      subjectId: string
      input: {
        bindings: Array<{ runtime_model: string; manual_rates: Record<string, unknown> | null }>
      }
    }
    expect(call.subjectId).toBe('provider-1')
    expect(call.input.bindings[0]).toMatchObject({ runtime_model: 'gpt-free' })
    expect(call.input.bindings[0].manual_rates).toMatchObject({
      input: { currency: 'USD', decimal_per_million: '0' },
      output: { currency: 'USD', decimal_per_million: '0' },
      cache_read: null,
      cache_write: null,
    })
    expect(
      screen
        .getAllByRole('status')
        .some((status) => /Pricing saved/i.test(status.textContent ?? '')),
    ).toBe(true)
  })

  it('preserves slash-containing catalog model IDs in the binding body', async () => {
    mocks.catalogModels = [
      {
        snapshot_id: 'snapshot-1',
        rate_revision_id: 'rate-revision-1',
        provider_id: 'openrouter',
        model_id: 'qwen/foo',
        rates: {
          input: { currency: 'USD', decimal_per_million: '0.25' },
          output: { currency: 'USD', decimal_per_million: '0.75' },
          cache_read: null,
          cache_write: null,
        },
        tiers: null,
        source_last_updated: null,
        source_kind: 'models_dev_catalog',
      },
    ]
    expect(mocks.catalogModels[0].snapshot_id).not.toBe(mocks.catalogModels[0].rate_revision_id)
    const { dialog } = openPricingDialog()
    fireEvent.change(within(dialog).getByLabelText('Runtime model ID'), {
      target: { value: 'qwen/foo' },
    })
    const catalogSelect = within(dialog).getByLabelText('Catalog provider/model')
    const option = within(catalogSelect).getByRole('option', { name: 'openrouter / qwen/foo' })
    fireEvent.change(catalogSelect, { target: { value: option.getAttribute('value') } })
    fireEvent.click(within(dialog).getByRole('button', { name: 'Add model pricing' }))
    fireEvent.click(within(dialog).getByRole('button', { name: 'Save pricing' }))

    await waitFor(() => expect(mocks.replaceProviderPricing).toHaveBeenCalled())
    const input = mocks.replaceProviderPricing.mock.calls[0][0].input as {
      bindings: Array<Record<string, unknown>>
    }
    expect(input.bindings[0]).toMatchObject({
      runtime_model: 'qwen/foo',
      catalog_provider_id: 'openrouter',
      catalog_model_id: 'qwen/foo',
      catalog_rate_revision_id: 'rate-revision-1',
      source_kind: 'models_dev_catalog',
    })
  })

  it('publishes catalog and manual bindings together, then retires only the manual override', async () => {
    mocks.catalogModels = [
      {
        snapshot_id: 'snapshot-1',
        rate_revision_id: 'rate-revision-1',
        provider_id: 'openai',
        model_id: 'shared-model',
        rates: {
          input: { currency: 'USD', decimal_per_million: '0.25' },
          output: { currency: 'USD', decimal_per_million: '0.75' },
          cache_read: null,
          cache_write: null,
        },
        tiers: null,
        source_last_updated: null,
        source_kind: 'models_dev_catalog',
      },
    ]
    const catalogBinding = bindingFixture()
    const manualBinding = bindingFixture({
      id: 'binding-2',
      source_kind: 'manual_override',
      catalog_provider_id: null,
      catalog_model_id: null,
      catalog_rate_revision_id: null,
      manual_rates: {
        input: { currency: 'USD', decimal_per_million: '1' },
        output: null,
        cache_read: null,
        cache_write: null,
      },
    })
    mocks.replaceProviderPricing
      .mockResolvedValueOnce({
        ...mocks.providerPricing,
        version: 4,
        bindings: [catalogBinding, manualBinding],
      })
      .mockResolvedValueOnce({
        ...mocks.providerPricing,
        version: 5,
        bindings: [catalogBinding],
      })

    const { dialog } = openPricingDialog()
    fireEvent.change(within(dialog).getByLabelText('Runtime model ID'), {
      target: { value: 'shared-model' },
    })
    const catalogSelect = within(dialog).getByLabelText('Catalog provider/model')
    const catalogOption = within(catalogSelect).getByRole('option', {
      name: 'openai / shared-model',
    })
    fireEvent.change(catalogSelect, { target: { value: catalogOption.getAttribute('value') } })
    fireEvent.click(within(dialog).getByRole('button', { name: 'Add model pricing' }))

    fireEvent.click(within(dialog).getByLabelText('Manual override'))
    fireEvent.change(within(dialog).getByLabelText('Runtime model ID'), {
      target: { value: 'shared-model' },
    })
    fireEvent.change(within(dialog).getByLabelText('Input · USD per 1M tokens'), {
      target: { value: '1' },
    })
    fireEvent.click(within(dialog).getByRole('button', { name: 'Add model pricing' }))
    fireEvent.click(within(dialog).getByRole('button', { name: 'Save pricing' }))

    await waitFor(() => expect(mocks.replaceProviderPricing).toHaveBeenCalledTimes(1))
    const firstInput = mocks.replaceProviderPricing.mock.calls[0][0].input as {
      bindings: Array<Record<string, unknown>>
    }
    expect(firstInput.bindings).toHaveLength(2)
    expect(firstInput.bindings.map((binding) => binding.source_kind).sort()).toEqual([
      'manual_override',
      'models_dev_catalog',
    ])

    const manualRow = within(dialog)
      .getAllByRole('row')
      .find((row) => row.textContent?.includes('Manual override'))
    expect(manualRow).toBeTruthy()
    fireEvent.click(within(manualRow!).getByRole('button', { name: 'Retire shared-model' }))
    fireEvent.click(within(dialog).getByRole('button', { name: 'Save pricing' }))

    await waitFor(() => expect(mocks.replaceProviderPricing).toHaveBeenCalledTimes(2))
    const secondInput = mocks.replaceProviderPricing.mock.calls[1][0].input as {
      bindings: Array<Record<string, unknown>>
    }
    expect(secondInput.bindings).toHaveLength(1)
    expect(secondInput.bindings[0]).toMatchObject({
      runtime_model: 'shared-model',
      source_kind: 'models_dev_catalog',
      catalog_rate_revision_id: 'rate-revision-1',
    })
    expect(within(dialog).getByText('models.dev catalog')).toBeTruthy()
  })

  it('marks invalid negative/exponent/over-precise rates and never submits them', () => {
    const { dialog } = openPricingDialog()
    fireEvent.click(within(dialog).getByLabelText('Manual override'))
    fireEvent.change(within(dialog).getByLabelText('Runtime model ID'), {
      target: { value: 'invalid-rate-model' },
    })
    const input = within(dialog).getByLabelText('Input · USD per 1M tokens')
    fireEvent.change(input, { target: { value: '1000000' } })
    expect(input.getAttribute('aria-invalid')).toBe('false')
    for (const value of ['-1', '1e3', '0.1234567890', '1000000.000000001', '1000001']) {
      fireEvent.change(input, { target: { value } })
      expect(input.getAttribute('aria-invalid')).toBe('true')
    }
    fireEvent.click(within(dialog).getByRole('button', { name: 'Add model pricing' }))
    expect(mocks.replaceProviderPricing).not.toHaveBeenCalled()
  })

  it('preserves a draft after a 409 and offers Refresh without overwriting it', async () => {
    mocks.replaceProviderPricing.mockRejectedValueOnce({ status: 409 })
    const { dialog } = openPricingDialog()
    fireEvent.click(within(dialog).getByLabelText('Manual override'))
    fireEvent.change(within(dialog).getByLabelText('Runtime model ID'), {
      target: { value: 'draft-model' },
    })
    fireEvent.change(within(dialog).getByLabelText('Input · USD per 1M tokens'), {
      target: { value: '0.000001' },
    })
    fireEvent.click(within(dialog).getByRole('button', { name: 'Add model pricing' }))
    fireEvent.click(within(dialog).getByRole('button', { name: 'Save pricing' }))

    const alert = await screen.findByRole('alert')
    expect(alert.textContent).toMatch(/draft is preserved/i)
    expect(within(dialog).getByText('draft-model')).toBeTruthy()
    fireEvent.click(within(alert).getByRole('button', { name: 'Refresh' }))
    await waitFor(() => expect(mocks.providerPricingRefetch).toHaveBeenCalled())
    expect(within(dialog).getByText('draft-model')).toBeTruthy()
  })

  it('returns focus to the originating provider card after closing the dialog', async () => {
    const { dialog, configure } = openPricingDialog()
    await waitFor(() => expect(document.activeElement).not.toBe(configure))
    fireEvent.click(within(dialog).getByRole('button', { name: 'Cancel' }))
    await waitFor(() => expect(document.activeElement).toBe(configure))
  })

  it('exposes a caption, scoped headers, wrapped model IDs, and status semantics', () => {
    const { dialog } = openPricingDialog()
    expect(within(dialog).getByRole('table')).toBeTruthy()
    expect(
      within(dialog).getByText(
        'Configured exact model pricing bindings and per-million-token rates',
      ),
    ).toBeTruthy()
    const headers = within(dialog).getAllByRole('columnheader')
    expect(headers.length).toBeGreaterThan(4)
    for (const header of headers) {
      if (!header.textContent) continue
      expect(header.getAttribute('scope')).toBe('col')
    }
    expect(screen.getByRole('status')).toBeTruthy()
  })
})
