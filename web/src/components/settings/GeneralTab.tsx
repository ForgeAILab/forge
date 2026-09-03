import { useRouterState } from '@tanstack/react-router'
import { useMembersQuery, useProjectAgentsQuery } from '@/api/hooks'
import { CiStepsEditor } from '@/components/ci-steps-editor'
import { SettingsSection } from '@/components/settings/SettingsSection'
import {
  assigneeSelectionFromValue,
  assigneeValueFromSelection,
} from '@/components/settings/project-settings-utils'
import { AgentAssigneeDropdown } from '@/components/task-controls'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select } from '@/components/ui/select'
import { Skeleton } from '@/components/ui/skeleton'
import { Switch } from '@/components/ui/switch'
import type { Agent, RoleDefinition } from '@/types/generated'

interface GeneralTabProps {
  projectIsLoading: boolean
  canSave: boolean
  isSaving: boolean
  paused: boolean
  pausedAt?: string | null
  systemPauseReason?: string | null
  pausePending: boolean
  name: string
  ciSteps: string[]
  defaultRoleSelections: Record<string, string>
  roles: RoleDefinition[]
  workflowIsLoading: boolean
  agents: Agent[]
  agentsIsLoading: boolean
  agentsIsError: boolean
  automaticRecoveryEnabled: boolean
  automaticRecoveryAgentId: string
  onNameChange: (v: string) => void
  onTogglePaused: () => void
  onCiStepsChange: (steps: string[]) => void
  onDefaultRoleSelectionsChange: (selections: Record<string, string>) => void
  onAutomaticRecoveryEnabledChange: (enabled: boolean) => void
  onAutomaticRecoveryAgentIdChange: (agentId: string) => void
  onSave: () => void
}

export function GeneralTab({
  projectIsLoading,
  canSave,
  isSaving,
  paused,
  pausedAt,
  systemPauseReason,
  pausePending,
  name,
  ciSteps,
  defaultRoleSelections,
  roles,
  workflowIsLoading,
  agents,
  agentsIsLoading,
  agentsIsError,
  automaticRecoveryEnabled,
  automaticRecoveryAgentId,
  onNameChange,
  onTogglePaused,
  onCiStepsChange,
  onDefaultRoleSelectionsChange,
  onAutomaticRecoveryEnabledChange,
  onAutomaticRecoveryAgentIdChange,
  onSave,
}: GeneralTabProps) {
  const params = useRouterState({
    select: (state) => state.matches.at(-1)?.params as { projectId?: string } | undefined,
  })
  const projectId = params?.projectId ?? ''
  const projectAgentsQuery = useProjectAgentsQuery(projectId)
  const { data: membersData } = useMembersQuery(projectId)
  const projectAgentsData = projectAgentsQuery.data
  const roleAssignmentsLoading = workflowIsLoading || projectAgentsQuery.isLoading
  const agentOptions = agents.map((agent) => ({
    value: agent.id,
    label: agent.name,
  }))

  return (
    <>
      <div className="mb-8">
        <h2 className="text-page font-semibold tracking-tight">General</h2>
        <p className="mt-1 text-sm text-muted-foreground">
          Project identity, CI configuration, and role defaults.
        </p>
      </div>
      {projectIsLoading ? (
        <div className="space-y-4">
          <Skeleton className="h-10 w-full" />
          <Skeleton className="h-32 w-full" />
        </div>
      ) : (
        <>
          <SettingsSection
            title="Project name"
            description="Shown in the sidebar and in CLI output."
          >
            <Input
              id="project-name"
              className="max-w-xs"
              value={name}
              onChange={(e) => onNameChange(e.target.value)}
            />
          </SettingsSection>
          <SettingsSection
            title="Project availability"
            description="Pause dispatch for this project without changing tasks or settings."
          >
            <div className="flex items-center gap-3">
              <Button
                size="sm"
                variant="outline"
                disabled={pausePending || !canSave}
                onClick={onTogglePaused}
              >
                {pausePending ? 'Saving...' : paused ? 'Resume project' : 'Pause project'}
              </Button>
              {paused && pausedAt ? (
                <span className="text-xs text-muted-foreground">
                  Paused {new Date(pausedAt).toLocaleString()}
                </span>
              ) : null}
            </div>
            {paused && systemPauseReason === 'missing_repository' ? (
              <p className="mt-2 max-w-md text-xs leading-5 text-muted-foreground">
                Paused automatically: this Project has no repository yet. It resumes on its own
                once one is attached; resuming it here will not let Tasks run without one.
              </p>
            ) : null}
          </SettingsSection>
          <SettingsSection
            title="CI steps"
            description="Shell commands run during review. Applied to all tasks in this project by default."
          >
            <CiStepsEditor steps={ciSteps} onChange={onCiStepsChange} />
          </SettingsSection>
          {(roleAssignmentsLoading || roles.length > 0) && (
            <SettingsSection
              title="Default role assignments"
              description="Pre-assign agents or users to each role when a task is created."
            >
              {roleAssignmentsLoading ? (
                <Skeleton className="h-10 w-full" />
              ) : (
                <div className="space-y-3">
                  {roles.map((role) => (
                    <div key={role.name} className="rounded-md border p-3">
                      <div className="space-y-1">
                        <p className="text-sm font-medium">
                          {role.display_name || role.name}
                          {role.description && (
                            <span className="ml-1.5 text-xs font-normal text-muted-foreground">
                              — {role.description}
                            </span>
                          )}
                        </p>
                        <AgentAssigneeDropdown
                          agents={projectAgentsData ?? []}
                          members={membersData}
                          disabled={projectAgentsQuery.isLoading}
                          placeholder="Unassigned"
                          roleLabel={role.display_name || role.name}
                          value={assigneeSelectionFromValue(defaultRoleSelections[role.name])}
                          onChange={(selection) =>
                            onDefaultRoleSelectionsChange({
                              ...defaultRoleSelections,
                              [role.name]: assigneeValueFromSelection(selection),
                            })
                          }
                        />
                      </div>
                    </div>
                  ))}
                </div>
              )}
              {projectAgentsQuery.isError && <p className="text-xs text-destructive">Unable to load agents</p>}
            </SettingsSection>
          )}
          <SettingsSection
            title="Automatic recovery agent"
            description="Before an exhausted review retry window blocks a task, dispatch one configured agent as a final recovery attempt."
          >
            <div className="space-y-3">
              <div className="flex items-center gap-3">
                <Switch
                  id="automatic-recovery"
                  checked={automaticRecoveryEnabled}
                  disabled={agentsIsLoading}
                  onChange={(event) =>
                    onAutomaticRecoveryEnabledChange((event.target as HTMLInputElement).checked)
                  }
                />
                <Label htmlFor="automatic-recovery" className="cursor-pointer text-sm">
                  {automaticRecoveryEnabled ? 'Enabled' : 'Disabled'}
                </Label>
              </div>
              <Select
                value={automaticRecoveryAgentId}
                options={agentOptions}
                placeholder={agentsIsLoading ? 'Loading agents...' : 'Select recovery agent'}
                disabled={!automaticRecoveryEnabled || agentsIsLoading || agentOptions.length === 0}
                onChange={onAutomaticRecoveryAgentIdChange}
              />
              {agentsIsError && <p className="text-xs text-destructive">Unable to load agents</p>}
            </div>
          </SettingsSection>
          <div className="flex justify-end py-6">
            <Button disabled={isSaving || !canSave} onClick={onSave}>
              {isSaving ? 'Saving...' : 'Save'}
            </Button>
          </div>
        </>
      )}
    </>
  )
}
