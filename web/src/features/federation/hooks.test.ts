import { describe, expect, it } from 'vitest'
import { federationQueryKeys } from './hooks'

describe('provider pricing query keys', () => {
  it('keeps catalog status, model searches, provider bindings, and CLI bindings distinct', () => {
    const catalogQuery = {
      limit: 25,
      cursor: 'opaque/next',
      provider_id: 'openrouter',
      query: 'qwen/foo',
    }

    expect(federationQueryKeys.pricingCatalogStatus).toEqual([
      'agent-providers',
      'pricing-catalog',
      'status',
    ])
    expect(federationQueryKeys.pricingCatalogModels(catalogQuery)).toEqual([
      'agent-providers',
      'pricing-catalog',
      'models',
      catalogQuery,
    ])
    expect(federationQueryKeys.providerPricing('provider/with-slash')).toEqual([
      'agent-providers',
      'provider/with-slash',
      'pricing',
    ])
    expect(federationQueryKeys.cliRuntimePricing('daemon/with-slash', 'claude_code')).toEqual([
      'agent-providers',
      'cli-runtime',
      'daemon/with-slash',
      'claude_code',
      'pricing',
    ])
  })
})
