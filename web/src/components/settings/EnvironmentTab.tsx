import { SettingsSection } from '@/components/settings/SettingsSection'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { Textarea } from '@/components/ui/textarea'

export const ENVIRONMENT_EXAMPLE = `{
  "env": { "GODOT_BIN": "/opt/godot/4.7.2/godot" },
  "assets": [
    { "source": "/srv/assets/limezu", "target": "assets/vendor/limezu" }
  ],
  "checks": [
    { "name": "godot", "command": "\\"$GODOT_BIN\\" --headless --version" },
    { "name": "browser", "command": "chromium --headless=new --no-sandbox --dump-dom about:blank >/dev/null", "roles": ["reviewer"] }
  ]
}`

interface EnvironmentTabProps {
  projectIsLoading: boolean
  canSave: boolean
  isSaving: boolean
  environmentText: string
  onEnvironmentTextChange: (text: string) => void
  onSave: () => void
}

/** Edits `settings.environment`: host env vars, worktree assets, and preflight checks. */
export function EnvironmentTab({
  projectIsLoading,
  canSave,
  isSaving,
  environmentText,
  onEnvironmentTextChange,
  onSave,
}: EnvironmentTabProps) {
  return (
    <div>
      <SettingsSection
        title="Execution environment"
        description="Applied immediately before every Task execution. `env` reaches agents, review checks, and hooks; `assets` are copied into the worktree when absent; a failing `check` parks the Task as environment not ready before an agent run is spent."
      >
        {projectIsLoading ? (
          <Skeleton className="h-64 w-full" />
        ) : (
          <div className="space-y-3">
            <Textarea
              aria-label="Project environment JSON"
              className="min-h-72 font-mono text-xs"
              spellCheck={false}
              value={environmentText}
              placeholder={ENVIRONMENT_EXAMPLE}
              onChange={(event) => onEnvironmentTextChange(event.target.value)}
            />
            <p className="text-xs text-muted-foreground">
              Checks run with <code>bash -lc</code> in the worktree; <code>roles</code> limits a
              check to those execution roles (empty means all). Asset sources are absolute host
              paths; targets are relative to the worktree.
            </p>
          </div>
        )}
      </SettingsSection>
      <div className="flex justify-end pt-4">
        <Button disabled={!canSave || isSaving} onClick={onSave}>
          {isSaving ? 'Saving…' : 'Save environment'}
        </Button>
      </div>
    </div>
  )
}

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
