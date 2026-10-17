import { SettingsSection } from '@/components/settings/SettingsSection'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select } from '@/components/ui/select'
import { Textarea } from '@/components/ui/textarea'
import { ENVIRONMENT_EXAMPLE, parseEnvironmentText } from './environment-utils'

interface EnvironmentTabProps {
  projectIsLoading: boolean
  canSave: boolean
  isSaving: boolean
  environmentText: string
  provision: 'when_verified' | 'never'
  onProvisionChange: (value: 'when_verified' | 'never') => void
  recheckMinutes: string
  recheckIntervalError: string | null
  saveError: string | null
  onRecheckMinutesChange: (value: string) => void
  onEnvironmentTextChange: (text: string) => void
  onSave: () => void
}

/** Edits `settings.environment`: host env vars, worktree assets, and preflight checks. */
export function EnvironmentTab({
  projectIsLoading,
  canSave,
  isSaving,
  environmentText,
  provision,
  onProvisionChange,
  recheckMinutes,
  recheckIntervalError,
  saveError,
  onRecheckMinutesChange,
  onEnvironmentTextChange,
  onSave,
}: EnvironmentTabProps) {
  const parsed = parseEnvironmentText(environmentText)
  const validationError = parsed.ok ? null : parsed.error
  const checks = parsed.ok && Array.isArray(parsed.value?.checks) ? parsed.value.checks : []
  const setScope = (index: number, scope: string) => {
    if (!parsed.ok || !parsed.value) return
    onEnvironmentTextChange(
      JSON.stringify(
        {
          ...parsed.value,
          checks: checks.map((check, i) =>
            i === index && typeof check === 'object' && check !== null
              ? { ...check, scope }
              : check,
          ),
        },
        null,
        2,
      ),
    )
  }

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
            {checks.map((check, index) => {
              if (typeof check !== 'object' || check === null) return null
              const record = check as Record<string, unknown>
              const name = typeof record.name === 'string' ? record.name : `Check ${index + 1}`
              return (
                <div key={index} className="space-y-2">
                  <Label htmlFor={`environment-scope-${index}`}>{name}: check scope</Label>
                  <Select
                    id={`environment-scope-${index}`}
                    aria-label={`${name} check scope`}
                    value={record.scope === 'machine' ? 'machine' : 'workspace'}
                    options={[
                      { value: 'workspace', label: 'Workspace — requires a checkout' },
                      { value: 'machine', label: 'Machine — toolchains, disk or services' },
                    ]}
                    onChange={(scope) => setScope(index, scope)}
                  />
                </div>
              )
            })}
            <p id="project-environment-help" className="text-xs text-muted-foreground">
              Checks must be read-only and run with <code>bash -lc</code>. Machine checks can run in
              an empty directory before code is copied; workspace checks require a checkout;{' '}
              <code>roles</code> limits a check to those execution roles (empty means all). Asset
              sources are absolute host paths; targets are relative to the worktree. Project
              settings are stored as plain text and readable through the API, so use a credential
              provider—not <code>env</code>
              —for secrets.
            </p>
          </div>
        )}
      </SettingsSection>
      <SettingsSection
        title="Repository provisioning"
        description="Allow a managed clone on another machine only after its machine checks pass."
      >
        {projectIsLoading ? (
          <Skeleton className="h-10 w-full" />
        ) : (
          <div className="space-y-2">
            <Label htmlFor="project-placement-provision">Provision a repository location</Label>
            <Select
              id="project-placement-provision"
              aria-label="Provision a repository location"
              value={provision}
              options={[
                { value: 'when_verified', label: 'When verified' },
                { value: 'never', label: 'Never' },
              ]}
              onChange={(value) => onProvisionChange(value as 'when_verified' | 'never')}
            />
            <p className="text-xs text-muted-foreground">
              Requires a repository remote and at least one passing machine check. Manually
              registered locations remain available.
            </p>
          </div>
        )}
      </SettingsSection>
      <SettingsSection
        title="Environment re-check interval"
        description="Automatically retry failing checks while the project is environment-paused."
      >
        {projectIsLoading ? (
          <Skeleton className="h-10 w-full" />
        ) : (
          <div className="space-y-2">
            <Label htmlFor="project-environment-recheck-minutes">
              Environment re-check interval (minutes)
            </Label>
            <Input
              id="project-environment-recheck-minutes"
              type="number"
              min={1}
              max={1440}
              step="any"
              value={recheckMinutes}
              onChange={(event) => onRecheckMinutesChange(event.target.value)}
              aria-invalid={Boolean(recheckIntervalError)}
              aria-describedby={
                recheckIntervalError
                  ? 'project-environment-recheck-help project-environment-recheck-error'
                  : 'project-environment-recheck-help'
              }
            />
            <p id="project-environment-recheck-help" className="text-xs text-muted-foreground">
              Forge re-runs failing environment checks at this interval and resumes the project
              automatically when they pass. Default: 10 minutes.
            </p>
            {recheckIntervalError ? (
              <p
                id="project-environment-recheck-error"
                role="alert"
                className="text-xs text-destructive"
              >
                {recheckIntervalError}
              </p>
            ) : null}
          </div>
        )}
      </SettingsSection>
      {saveError ? (
        <p role="alert" className="mt-2 text-xs text-destructive">
          {saveError}
        </p>
      ) : null}
      <div className="flex justify-end pt-4">
        <Button
          disabled={
            !canSave || isSaving || Boolean(validationError) || Boolean(recheckIntervalError)
          }
          onClick={onSave}
        >
          {isSaving ? 'Saving…' : 'Save environment'}
        </Button>
      </div>
    </div>
  )
}
