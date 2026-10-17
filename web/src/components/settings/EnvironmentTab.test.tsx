import { fireEvent, render, screen } from '@testing-library/react'
import { EnvironmentTab } from './EnvironmentTab'
import { describe, expect, it, vi } from 'vitest'
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

describe('environment scope and provisioning controls', () => {
  it('validates scope and defaults an existing check to workspace', () => {
    expect(
      parseEnvironmentText('{"checks":[{"name":"x","command":"true","scope":"host"}]}'),
    ).toMatchObject({ ok: false })
    expect(parseEnvironmentText('{"checks":[{"name":"x","command":"true"}]}')).toMatchObject({
      ok: true,
    })
  })

  it('updates scope without losing check fields and exposes provisioning policy', async () => {
    const change = vi.fn()
    const provision = vi.fn()
    render(
      <EnvironmentTab
        projectIsLoading={false}
        canSave
        isSaving={false}
        environmentText={JSON.stringify({
          env: { X: 'value' },
          checks: [
            { name: 'cargo', command: 'cargo --version', roles: ['coder'], timeout_seconds: 10 },
          ],
        })}
        provision="when_verified"
        onProvisionChange={provision}
        recheckMinutes="10"
        recheckIntervalError={null}
        saveError={null}
        onRecheckMinutesChange={vi.fn()}
        onEnvironmentTextChange={change}
        onSave={vi.fn()}
      />,
    )
    fireEvent.click(screen.getByRole('button', { name: 'cargo check scope' }))
    fireEvent.click(screen.getByRole('option', { name: 'Machine — toolchains, disk or services' }))
    expect(JSON.parse(change.mock.calls[0][0])).toEqual({
      env: { X: 'value' },
      checks: [
        {
          name: 'cargo',
          command: 'cargo --version',
          roles: ['coder'],
          timeout_seconds: 10,
          scope: 'machine',
        },
      ],
    })
    fireEvent.click(screen.getByRole('button', { name: 'Provision a repository location' }))
    fireEvent.click(screen.getByRole('option', { name: 'Never' }))
    expect(provision).toHaveBeenCalledWith('never')
  })
})
