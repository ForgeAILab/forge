import { useEffect, useState } from 'react'

export const ENVIRONMENT_EXAMPLE = `{
  "env": { "GODOT_BIN": "/opt/godot/4.7.2/godot" },
  "assets": [
    { "source": "/srv/assets/limezu", "target": "assets/vendor/limezu" }
  ],
  "checks": [
    { "name": "godot", "command": "\\"$GODOT_BIN\\" --headless --version" },
    { "name": "browser", "command": "chromium --headless=new --dump-dom about:blank >/dev/null", "roles": ["reviewer"] }
  ]
}`

/** Serialize the stored environment for editing; empty when nothing is declared. */
export function environmentTextFromSettings(settings: unknown): string {
  const environment =
    typeof settings === 'object' && settings !== null
      ? (settings as Record<string, unknown>).environment
      : undefined
  if (typeof environment !== 'object' || environment === null) return ''
  const record = environment as Record<string, unknown>
  const isEmpty = (value: unknown) =>
    value === undefined ||
    (Array.isArray(value) && value.length === 0) ||
    (typeof value === 'object' && value !== null && Object.keys(value).length === 0)
  if (isEmpty(record.env) && isEmpty(record.assets) && isEmpty(record.checks)) return ''
  return JSON.stringify(environment, null, 2)
}

/** Keep the environment draft isolated from the other Project settings form. */
export function useProjectEnvironmentText(settings: unknown) {
  const [text, setText] = useState('')

  useEffect(() => {
    setText(environmentTextFromSettings(settings))
  }, [settings])

  return [text, setText] as const
}

/** Parse the editor text; `null` clears the environment, a string is an error message. */
export function parseEnvironmentText(
  text: string,
): { ok: true; value: Record<string, unknown> | null } | { ok: false; error: string } {
  if (!text.trim()) return { ok: true, value: null }
  try {
    const value: unknown = JSON.parse(text)
    if (typeof value !== 'object' || value === null || Array.isArray(value)) {
      return { ok: false, error: 'Environment must be a JSON object' }
    }
    return { ok: true, value: value as Record<string, unknown> }
  } catch (error) {
    return {
      ok: false,
      error: `Environment is not valid JSON: ${error instanceof Error ? error.message : String(error)}`,
    }
  }
}
