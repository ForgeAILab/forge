import { afterEach, describe, expect, it, vi } from 'vitest'
import {
  deleteAgentPricing,
  deleteCliRuntimePricing,
  deleteProviderPricing,
  getAgentPricing,
  getCliRuntimePricing,
  getContextManifest,
  getPricingCatalogStatus,
  listContextManifests,
  listPricingCatalogModels,
  refreshPricingCatalog,
  updateAgentPricing,
  updateCliRuntimePricing,
  updateProviderPricing,
} from './api'

const { apiFetch } = vi.hoisted(() => ({ apiFetch: vi.fn() }))

vi.mock('@/api/client', () => ({ apiFetch }))

describe('context-manifest API adapter', () => {
  afterEach(() => {
    apiFetch.mockReset()
  })

  it('uses the identity-scoped authorized discovery route', async () => {
    apiFetch.mockResolvedValueOnce({ items: [{ id: 'manifest-1' }], has_more: false })

    await expect(
      listContextManifests({ identity_id: 'identity-1', context_scope_id: 'scope-1' }),
    ).resolves.toEqual([{ id: 'manifest-1' }])
    expect(apiFetch).toHaveBeenCalledWith('/agents/identity-1/context-manifests', {
      search: { context_scope_id: 'scope-1', limit: 50 },
    })
  })

  it('keeps detail lookup scoped by identity and context scope', async () => {
    apiFetch.mockResolvedValueOnce({ id: 'manifest-1' })

    await expect(
      getContextManifest('manifest-1', {
        identity_id: 'identity-1',
        context_scope_id: 'scope-1',
      }),
    ).resolves.toEqual({ id: 'manifest-1' })
    expect(apiFetch).toHaveBeenCalledWith('/context-manifests/manifest-1', {
      search: { identity_id: 'identity-1', context_scope_id: 'scope-1' },
    })
  })
})

describe('provider pricing API adapter', () => {
  afterEach(() => {
    apiFetch.mockReset()
  })

  it('loads catalog status and posts an explicit idempotent refresh', async () => {
    apiFetch.mockResolvedValueOnce({ state: 'fresh' })
    await expect(getPricingCatalogStatus()).resolves.toEqual({ state: 'fresh' })
    expect(apiFetch).toHaveBeenLastCalledWith('/providers/pricing-catalog/status')

    apiFetch.mockResolvedValueOnce({ state: 'fresh', revision: 'rev-2' })
    await expect(refreshPricingCatalog({ idempotency_key: 'pricing-refresh-1' })).resolves.toEqual({
      state: 'fresh',
      revision: 'rev-2',
    })
    expect(apiFetch).toHaveBeenLastCalledWith('/providers/pricing-catalog/refresh', {
      method: 'POST',
      body: JSON.stringify({ idempotency_key: 'pricing-refresh-1' }),
    })
  })

  it('keeps catalog model search and cursors as query values', async () => {
    apiFetch.mockResolvedValueOnce({ items: [], has_more: false, next_cursor: null })

    await listPricingCatalogModels({
      limit: 25,
      cursor: 'opaque/next',
      provider_id: 'openrouter',
      query: 'qwen/foo',
    })

    expect(apiFetch).toHaveBeenCalledWith('/providers/pricing-catalog/models', {
      search: {
        limit: 25,
        cursor: 'opaque/next',
        provider_id: 'openrouter',
        query: 'qwen/foo',
      },
    })
  })

  it('puts and resets a provider adjustment with its version', async () => {
    const input = {
      expected_version: 4,
      mode: 'discount' as const,
      discount_percent: '20',
      fixed_rates: null,
      catalog_provider_id: 'openrouter',
      catalog_model_id: null,
    }
    apiFetch.mockResolvedValueOnce({ settings: { ...input, version: 5 } })

    await updateProviderPricing('provider-1', input)

    expect(apiFetch).toHaveBeenCalledWith('/providers/provider-1/pricing', {
      method: 'PUT',
      body: JSON.stringify(input),
    })
    apiFetch.mockResolvedValueOnce({ settings: null })
    await deleteProviderPricing('provider-1', 5)
    expect(apiFetch).toHaveBeenLastCalledWith('/providers/provider-1/pricing', {
      method: 'DELETE',
      search: { version: 5 },
    })
  })

  it('encodes CLI runtime identifiers for get, put, and delete', async () => {
    const pricing = { settings: null }
    apiFetch.mockResolvedValueOnce(pricing)
    await expect(getCliRuntimePricing('daemon/1', 'claude_code')).resolves.toBe(pricing)
    expect(apiFetch).toHaveBeenLastCalledWith(
      '/providers/cli-runtimes/daemon%2F1/claude_code/pricing',
    )

    apiFetch.mockResolvedValueOnce(pricing)
    const input = {
      expected_version: 0,
      mode: 'list' as const,
      discount_percent: null,
      fixed_rates: null,
      catalog_provider_id: 'anthropic',
      catalog_model_id: null,
    }
    await updateCliRuntimePricing('daemon/1', 'claude_code', input)
    expect(apiFetch).toHaveBeenLastCalledWith(
      '/providers/cli-runtimes/daemon%2F1/claude_code/pricing',
      { method: 'PUT', body: JSON.stringify(input) },
    )
    await deleteCliRuntimePricing('daemon/1', 'claude_code', 1)
    expect(apiFetch).toHaveBeenLastCalledWith(
      '/providers/cli-runtimes/daemon%2F1/claude_code/pricing',
      { method: 'DELETE', search: { version: 1 } },
    )
  })

  it('uses agent pricing routes and preserves pinned slash model IDs', async () => {
    const input = {
      expected_version: 0,
      mode: 'list' as const,
      discount_percent: null,
      fixed_rates: null,
      catalog_provider_id: 'openrouter',
      catalog_model_id: 'qwen/foo',
    }
    apiFetch.mockResolvedValue({ agent_id: 'agent-1' })
    await getAgentPricing('agent/1')
    expect(apiFetch).toHaveBeenLastCalledWith('/agents/agent%2F1/pricing')
    await updateAgentPricing('agent/1', input)
    expect(apiFetch).toHaveBeenLastCalledWith('/agents/agent%2F1/pricing', {
      method: 'PUT',
      body: JSON.stringify(input),
    })
    await deleteAgentPricing('agent/1', 2)
    expect(apiFetch).toHaveBeenLastCalledWith('/agents/agent%2F1/pricing', {
      method: 'DELETE',
      search: { version: 2 },
    })
  })
})
