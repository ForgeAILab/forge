import { describe, expect, it } from 'vitest'
import { environmentTextFromSettings, parseEnvironmentText } from './EnvironmentTab'

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

  it('clears on empty text and refuses non-objects', () => {
    expect(parseEnvironmentText('  ')).toEqual({ ok: true, value: null })
    expect(parseEnvironmentText('[]')).toMatchObject({ ok: false })
    expect(parseEnvironmentText('{ nope')).toMatchObject({ ok: false })
  })
})
