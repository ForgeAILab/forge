import { describe, expect, it } from 'vitest'
import {
  ENVIRONMENT_EXAMPLE,
  environmentTextFromSettings,
  parseEnvironmentText,
} from './environment-utils'

describe('environment settings text', () => {
  it('shows nothing for an empty or missing environment', () => {
    expect(environmentTextFromSettings({})).toBe('')
    expect(environmentTextFromSettings({ environment: { env: {}, assets: [], checks: [] } })).toBe(
      '',
    )
  })

  it('round-trips a declared environment', () => {
    const environment = {
      env: { GODOT_BIN: '/opt/godot' },
      assets: [],
      checks: [{ name: 'godot', command: 'godot --version', roles: [], timeout_seconds: 120 }],
    }
    const text = environmentTextFromSettings({ environment })
    expect(parseEnvironmentText(text)).toEqual({ ok: true, value: environment })
  })

  it('keeps the interval in its separate field instead of the JSON editor', () => {
    const settings = {
      environment: { env: { TOOL: 'tool' }, assets: [], checks: [], recheck_interval_seconds: 600 },
    }
    expect(parseEnvironmentText(environmentTextFromSettings(settings))).toEqual({
      ok: true,
      value: { env: { TOOL: 'tool' }, assets: [], checks: [] },
    })
    expect(settings.environment.recheck_interval_seconds).toBe(600)
  })

  it('clears on empty text and refuses non-objects', () => {
    expect(parseEnvironmentText('  ')).toEqual({ ok: true, value: null })
    expect(parseEnvironmentText('[]')).toMatchObject({ ok: false })
    expect(parseEnvironmentText('{ nope')).toMatchObject({ ok: false })
  })

  it('does not recommend disabling the browser sandbox', () => {
    expect(ENVIRONMENT_EXAMPLE).not.toContain('--no-sandbox')
  })
})
