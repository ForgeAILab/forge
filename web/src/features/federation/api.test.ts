import { afterEach, describe, expect, it, vi } from 'vitest'
import {
  getCliRuntimePricing,
  getContextManifest,
  getPricingCatalogStatus,
  listContextManifests,
  listPricingCatalogModels,
  refreshPricingCatalog,
  replaceCliRuntimePricing,
  replaceProviderPricing,
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

  it('uses exact provider model IDs in the request body, including slash IDs', async () => {
    const input = {
      expected_version: 4,
      idempotency_key: 'pricing-save-1',
      subject_revision_digest: 'subject-rev',
      bindings: [
        {
          runtime_model: 'qwen/foo',
          source_kind: 'models_dev_catalog' as const,
          catalog_provider_id: 'openrouter',
          catalog_model_id: 'qwen/foo',
          catalog_rate_revision_id: 'rate-1',
          manual_rates: null,
        },
      ],
    }
    apiFetch.mockResolvedValueOnce({ subject_id: 'provider-1', version: 5, bindings: [] })

    await replaceProviderPricing('provider-1', input)

    expect(apiFetch).toHaveBeenCalledWith('/providers/provider-1/pricing', {
      method: 'PUT',
      body: JSON.stringify(input),
    })
  })

  it('encodes runtime route identifiers while keeping model IDs in the body', async () => {
    const pricing = { subject_id: 'daemon-1', version: 1, bindings: [] }
    apiFetch.mockResolvedValueOnce(pricing)
    await expect(getCliRuntimePricing('daemon/1', 'claude_code')).resolves.toBe(pricing)
    expect(apiFetch).toHaveBeenLastCalledWith(
      '/providers/cli-runtimes/daemon%2F1/claude_code/pricing',
    )

    apiFetch.mockResolvedValueOnce(pricing)
    await replaceCliRuntimePricing('daemon/1', 'claude_code', {
      expected_version: 0,
      idempotency_key: 'runtime-pricing-1',
      subject_revision_digest: 'subject-rev',
      bindings: [],
    })
    expect(apiFetch).toHaveBeenLastCalledWith(
      '/providers/cli-runtimes/daemon%2F1/claude_code/pricing',
      expect.objectContaining({ method: 'PUT' }),
    )
  })
})
