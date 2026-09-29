import { SettingsSection } from '@/components/settings/SettingsSection'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { Textarea } from '@/components/ui/textarea'
import { ENVIRONMENT_EXAMPLE, parseEnvironmentText } from './environment-utils'

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
  const parsed = parseEnvironmentText(environmentText)
  const validationError = parsed.ok ? null : parsed.error

  return (
    <div>
      <SettingsSection
        title="Execution environment"
        description="Applied immediately before Task execution. `env` reaches local and remote agents, review checks, and hooks; local runs copy `assets` into the worktree and run `checks` before an agent run is spent."
      >
        {projectIsLoading ? (
          <Skeleton className="h-64 w-full" />
        ) : (
          <div className="space-y-3">
            <Textarea
              aria-label="Project environment JSON"
              aria-describedby={
                validationError
                  ? 'project-environment-help project-environment-error'
                  : 'project-environment-help'
              }
              aria-invalid={validationError ? true : undefined}
              className="min-h-72 font-mono text-xs"
              spellCheck={false}
              value={environmentText}
              placeholder={ENVIRONMENT_EXAMPLE}
              onChange={(event) => onEnvironmentTextChange(event.target.value)}
            />
            {validationError ? (
              <p id="project-environment-error" role="alert" className="text-xs text-destructive">
                {validationError}
              </p>
            ) : null}
            <p id="project-environment-help" className="text-xs text-muted-foreground">
              Checks run with <code>bash -lc</code> in the worktree; <code>roles</code> limits a
              check to those execution roles (empty means all). Asset sources are absolute host
              paths; targets are relative to the worktree. Project settings are stored as plain text
              and readable through the API, so use a credential provider—not <code>env</code>
              —for secrets.
            </p>
          </div>
        )}
      </SettingsSection>
      <div className="flex justify-end pt-4">
        <Button disabled={!canSave || isSaving || Boolean(validationError)} onClick={onSave}>
          {isSaving ? 'Saving…' : 'Save environment'}
        </Button>
      </div>
    </div>
  )
}
