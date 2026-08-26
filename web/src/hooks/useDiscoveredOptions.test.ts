import { describe, expect, it } from 'vitest'
import { getReasoningOptionsForModel, normalizeDiscoveredOptions } from './useDiscoveredOptions'

describe('normalizeDiscoveredOptions', () => {
  it('uses adapter-provided reasoning efforts for each model', () => {
    const options = normalizeDiscoveredOptions({
      models: ['gpt-5.6-sol', 'gpt-5.6-luna'],
      cli_specific: {
        reasoning_efforts: ['low', 'medium', 'high', 'xhigh', 'max', 'ultra'],
        model_reasoning_efforts: {
          'gpt-5.6-sol': ['low', 'medium', 'high', 'xhigh', 'max', 'ultra'],
          'gpt-5.6-luna': ['low', 'medium', 'high', 'xhigh', 'max'],
        },
      },
    })

    expect(getReasoningOptionsForModel(options, 'gpt-5.6-sol').map((entry) => entry.id)).toEqual([
      'low',
      'medium',
      'high',
      'xhigh',
      'max',
      'ultra',
    ])
    expect(getReasoningOptionsForModel(options, 'gpt-5.6-luna').map((entry) => entry.id)).toEqual([
      'low',
      'medium',
      'high',
      'xhigh',
      'max',
    ])
  })

  it('preserves an explicit empty effort list for models without reasoning controls', () => {
    const options = normalizeDiscoveredOptions({
      models: ['claude-fable-5', 'claude-haiku-4-5'],
      cli_specific: {
        reasoning_efforts: ['low', 'medium', 'high', 'xhigh', 'max', 'ultracode'],
        model_reasoning_efforts: {
          'claude-fable-5': ['low', 'medium', 'high', 'xhigh', 'max', 'ultracode'],
          'claude-haiku-4-5': [],
        },
      },
    })

    expect(getReasoningOptionsForModel(options, 'claude-haiku-4-5')).toEqual([])
    expect(getReasoningOptionsForModel(options, 'claude-fable-5').at(-1)).toEqual({
      id: 'ultracode',
      label: 'Ultracode',
    })
  })

  it('keeps the legacy shared effort fallback when adapters omit per-model metadata', () => {
    const options = normalizeDiscoveredOptions({ models: ['custom-model'] })

    expect(getReasoningOptionsForModel(options, 'custom-model').map((entry) => entry.id)).toEqual([
      'low',
      'medium',
      'high',
    ])
    expect(options.modelConfigs).toEqual({})
  })

  it('preserves Smith model-to-provider config for harness registration', () => {
    const options = normalizeDiscoveredOptions({
      models: ['gpt-5.6-terra', 'gemini-3.6-flash'],
      cli_specific: {
        model_providers: {
          'gpt-5.6-terra': 'chatgpt',
          'gemini-3.6-flash': 'google',
        },
      },
    })

    expect(options.modelConfigs).toEqual({
      'gpt-5.6-terra': { provider: 'chatgpt' },
      'gemini-3.6-flash': { provider: 'google' },
    })
  })
})
