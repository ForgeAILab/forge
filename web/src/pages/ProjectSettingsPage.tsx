import { useEffect, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { Link, useNavigate } from '@tanstack/react-router'
import type { Icon } from '@phosphor-icons/react'
import {
  ChartBar,
  CloudArrowDown,
  Database,
  FlowArrow,
  FolderOpen,
  Gear,
  GitBranch,
  Lightning,
  Pause,
  Plugs,
  Users,
  WarningOctagon,
} from '@phosphor-icons/react'
import { toast } from 'sonner'
import {
  useAgentsQuery,
  useCreateRepo,
  useDaemonsQuery,
  useDeleteProject,
  usePauseProject,
  useProjectQuery,
  useReposQuery,
  useResumeProject,
  useUpdateProject,
  useWorkflowQuery,
} from '@/api/hooks'
import { normalizeCiSteps } from '@/components/ci-steps-editor'
import { ErrorBanner } from '@/components/error-banner'
import { McpInstallControls } from '@/components/mcp-install-controls'
import { AnalyticsTab } from '@/components/settings/AnalyticsTab'
import { DangerTab } from '@/components/settings/DangerTab'
import { GeneralTab } from '@/components/settings/GeneralTab'
import { HooksTab } from '@/components/settings/HooksTab'
import { MembersTab } from '@/components/settings/MembersTab'
import { RepoDialog } from '@/components/settings/RepoDialog'
import { ReposTab } from '@/components/settings/ReposTab'
import { SettingsSection } from '@/components/settings/SettingsSection'
import { WorkflowTab } from '@/components/settings/WorkflowTab'
import {
  ciStepsFromReviewConfig,
  isRecord,
  lifecycleHooksFromSettings,
  settingsErrorMessage,
} from '@/components/settings/project-settings-utils'
import { emptyRepoForm, type RepoFormState } from '@/components/settings/RepoForm'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { getApiErrorMessage } from '@/lib/api-error'
import { cn } from '@/lib/cn'
import { productTerm } from '@/lib/i18n'
import { useAuthStore } from '@/stores/auth'
import { clearDeletedProjectScope, resolveNextProjectId } from '@/stores/project-scope'
import type { DefaultRoleAssignment, LifecycleHooks } from '@/types/generated'

export type ProjectSettingsTab =
  | 'general'
  | 'repos'
  | 'members'
  | 'mcp'
  | 'hooks'
  | 'analytics'
  | 'workflow'
  | 'danger'

const SETTINGS_TABS: Array<{
  id: ProjectSettingsTab
  label: string
  icon: Icon
  danger?: boolean
}> = [
  { id: 'general', label: 'General', icon: Gear },
  { id: 'repos', label: 'Repos', icon: GitBranch },
  { id: 'members', label: 'Members', icon: Users },
  { id: 'mcp', label: 'MCP', icon: Plugs },
  { id: 'hooks', label: 'Hooks', icon: Lightning },
  { id: 'analytics', label: 'Analytics', icon: ChartBar },
  { id: 'workflow', label: 'Workflow', icon: FlowArrow },
  { id: 'danger', label: 'Danger zone', icon: WarningOctagon, danger: true },
]

export function isProjectSettingsTab(value: string | undefined): value is ProjectSettingsTab {
  return SETTINGS_TABS.some((tab) => tab.id === value)
}

export function ProjectSettingsPage({
  projectId,
  initialTab = 'general',
}: {
  projectId: string
  initialTab?: ProjectSettingsTab
}) {
  const projectQuery = useProjectQuery(projectId)
  const workflowQuery = useWorkflowQuery(projectId)
  const updateProject = useUpdateProject()
  const deleteProject = useDeleteProject()
  const pauseProject = usePauseProject()
  const resumeProject = useResumeProject()
  const navigate = useNavigate()
  const queryClient = useQueryClient()

  const agentsQuery = useAgentsQuery()

  const [deletingProject, setDeletingProject] = useState(false)

  // Form state (all needed by saveProject)
  const [name, setName] = useState('')
  const [ciSteps, setCiSteps] = useState<string[]>([])
  const [lifecycleHooks, setLifecycleHooks] = useState<LifecycleHooks>({})
  const [defaultRoleSelections, setDefaultRoleSelections] = useState<Record<string, string>>({})
  const [automaticRecoveryEnabled, setAutomaticRecoveryEnabled] = useState(false)
  const [automaticRecoveryAgentId, setAutomaticRecoveryAgentId] = useState('')

  const project = projectQuery.data
  const roles = workflowQuery.data?.roles ?? []
  const agents = agentsQuery.data?.items ?? []

  useEffect(() => {
    if (!project) return
    const timeout = window.setTimeout(() => {
      setName(project.name)
      setCiSteps(ciStepsFromReviewConfig(project.default_review_config))
      setLifecycleHooks(lifecycleHooksFromSettings(project.settings))
      const rawAssignments = project.settings?.default_role_assignments
      const assignments: DefaultRoleAssignment[] = Array.isArray(rawAssignments)
        ? rawAssignments
        : []
      const selections: Record<string, string> = {}
      for (const a of assignments) {
        if (a.assignee_type === 'agent' && a.assignee_id) {
          selections[a.role_name] = `agent:${a.assignee_id}`
        } else if (a.assignee_type === 'user' && a.assignee_id === 'human') {
          selections[a.role_name] = 'manual'
        } else if (a.assignee_type === 'user' && a.assignee_id) {
          selections[a.role_name] = `user:${a.assignee_id}`
        }
      }
      setDefaultRoleSelections(selections)
      const rawRecovery = isRecord(project.settings?.automatic_recovery)
        ? project.settings.automatic_recovery
        : {}
      setAutomaticRecoveryEnabled(rawRecovery.enabled === true)
      setAutomaticRecoveryAgentId(
        typeof rawRecovery.agent_id === 'string' ? rawRecovery.agent_id : '',
      )
    }, 0)
    return () => window.clearTimeout(timeout)
  }, [project])

  const saveProject = () => {
    if (!project || updateProject.isPending) return
    const nextName = name.trim()
    if (!nextName) {
      toast.error('Project name is required')
      return
    }
    for (const hooks of Object.values(lifecycleHooks)) {
      for (const hook of hooks ?? []) {
        if (hook.type !== 'script') continue
        if (!hook.command.trim()) {
          toast.error('Script command is required')
          return
        }
        if (!Number.isInteger(hook.timeout_seconds) || hook.timeout_seconds < 1) {
          toast.error('Script timeout must be 1 or greater')
          return
        }
      }
    }
    if (automaticRecoveryEnabled && !automaticRecoveryAgentId) {
      toast.error('Automatic recovery requires an agent')
      return
    }
    const settingsWithoutRolePrompts: Record<string, unknown> = {
      ...(isRecord(project.settings) ? project.settings : {}),
    }
    delete settingsWithoutRolePrompts.role_prompts
    const defaultRoleAssignmentsList: DefaultRoleAssignment[] = roles.flatMap(
      (role): DefaultRoleAssignment[] => {
        const sel = defaultRoleSelections[role.name] ?? 'unassigned'
        if (sel === 'unassigned') return []
        if (sel === 'manual')
          return [{ role_name: role.name, assignee_type: 'user', assignee_id: 'human' }]
        if (sel.startsWith('user:'))
          return [
            {
              role_name: role.name,
              assignee_type: 'user',
              assignee_id: sel.slice('user:'.length),
            },
          ]
        return [
          {
            role_name: role.name,
            assignee_type: 'agent',
            assignee_id: sel.slice('agent:'.length),
          },
        ]
      },
    )
    const nextSettings: Record<string, unknown> = {
      ...settingsWithoutRolePrompts,
      default_role_assignments: defaultRoleAssignmentsList,
      lifecycle_hooks: lifecycleHooks,
      automatic_recovery: {
        enabled: automaticRecoveryEnabled,
        agent_id: automaticRecoveryEnabled ? automaticRecoveryAgentId : null,
        max_attempts: 1,
      },
    }
    updateProject.mutate(
      {
        projectId,
        body: {
          version: project.version,
          name: nextName,
          settings: nextSettings,
          default_review_config: {
            ci_steps: normalizeCiSteps(ciSteps),
            review_prompt: null,
          },
        },
      },
      {
        onError: (error) => toast.error(settingsErrorMessage(error, 'Project update failed')),
        onSuccess: () => toast.success('Project settings saved'),
      },
    )
  }

  const toggleProjectPaused = () => {
    if (!project) return
    const mutation = project.paused ? resumeProject : pauseProject
    mutation.mutate(project.id, {
      onError: (error) =>
        toast.error(
          getApiErrorMessage(
            error,
            project.paused ? 'Project resume failed' : 'Project pause failed',
          ),
        ),
    })
  }

  return (
    <div className="flex min-h-[calc(100dvh-7rem)] max-h-[calc(100dvh-7rem)] flex-col gap-0 overflow-hidden rounded-xl border border-border-subtle bg-card shadow-card lg:flex-row">
      {/* Settings sidebar */}
      <aside className="flex w-full shrink-0 flex-col border-b bg-background lg:w-56 lg:border-b-0 lg:border-r">
        <div className="border-b px-4 py-3">
          <p className="font-mono text-micro font-semibold uppercase tracking-[1px] text-muted-foreground">
            Settings
          </p>
          <div className="mt-0.5 flex min-w-0 items-center gap-1.5">
            <p className="min-w-0 truncate text-sm font-semibold text-foreground">
              {project?.name ?? '…'}
            </p>
            {project?.paused ? (
              <Pause size={12} className="shrink-0 text-muted-foreground" weight="fill" />
            ) : null}
          </div>
          <p className="truncate font-mono text-[11px] text-muted-foreground">{projectId}</p>
        </div>
        <nav className="flex flex-1 gap-0.5 overflow-x-auto p-2 lg:flex-col">
          {SETTINGS_TABS.map((tab) => {
            const TabIcon = tab.icon
            return (
              <Link
                key={tab.id}
                to={
                  tab.id === 'general'
                    ? '/projects/$projectId/settings'
                    : '/projects/$projectId/settings/$tab'
                }
                params={{ projectId, tab: tab.id }}
                className={cn(
                  'relative flex w-auto shrink-0 items-center gap-2.5 rounded-lg px-2.5 py-[7px] text-[13px] leading-none font-medium text-left transition-colors lg:w-full lg:shrink',
                  initialTab === tab.id
                    ? 'bg-[var(--ember-surface)] text-sidebar-active-foreground before:absolute before:left-0 before:top-1/2 before:-translate-y-1/2 before:h-4 before:w-[3px] before:rounded-r-full before:bg-primary'
                    : tab.danger
                      ? 'text-destructive/70 hover:bg-destructive/5 hover:text-destructive'
                      : 'text-sidebar-foreground hover:bg-accent/50 hover:text-foreground',
                )}
              >
                <TabIcon size={16} />
                {tab.label}
              </Link>
            )
          })}
        </nav>
      </aside>

      {/* Content area */}
      <div className="min-h-0 flex-1 overflow-y-auto px-4 py-5 sm:px-6 lg:px-8 lg:py-6">
        <div className="max-w-[760px]">
          {projectQuery.isError && (
            <ErrorBanner
              error={projectQuery.error}
              fallback="Project failed to load"
              onRetry={() => void projectQuery.refetch()}
            />
          )}

          {initialTab === 'general' && (
            <GeneralTab
              projectIsLoading={projectQuery.isLoading}
              canSave={Boolean(project)}
              isSaving={updateProject.isPending}
              paused={project?.paused ?? false}
              pausedAt={project?.paused_at}
              pausePending={pauseProject.isPending || resumeProject.isPending}
              name={name}
              ciSteps={ciSteps}
              defaultRoleSelections={defaultRoleSelections}
              roles={roles}
              workflowIsLoading={workflowQuery.isLoading}
              agents={agents}
              agentsIsLoading={agentsQuery.isLoading}
              agentsIsError={agentsQuery.isError}
              automaticRecoveryEnabled={automaticRecoveryEnabled}
              automaticRecoveryAgentId={automaticRecoveryAgentId}
              onNameChange={setName}
              onTogglePaused={toggleProjectPaused}
              onCiStepsChange={setCiSteps}
              onDefaultRoleSelectionsChange={setDefaultRoleSelections}
              onAutomaticRecoveryEnabledChange={setAutomaticRecoveryEnabled}
              onAutomaticRecoveryAgentIdChange={setAutomaticRecoveryAgentId}
              onSave={saveProject}
            />
          )}

          {initialTab === 'repos' && <ReposTab project={project} projectId={projectId} />}

          {initialTab === 'members' && <MembersTab projectId={projectId} />}

          {initialTab === 'mcp' && <ProjectMcpTab projectId={projectId} />}

          {initialTab === 'hooks' && (
            <HooksTab
              project={project}
              projectId={projectId}
              projectIsLoading={projectQuery.isLoading}
              canSave={Boolean(project)}
              isSaving={updateProject.isPending}
              lifecycleHooks={lifecycleHooks}
              onLifecycleHooksChange={setLifecycleHooks}
              onSave={saveProject}
            />
          )}

          {initialTab === 'analytics' && <AnalyticsTab projectId={projectId} />}

          {initialTab === 'workflow' && (
            <WorkflowTab
              projectId={projectId}
              workflowTemplateName={project?.workflow_template_name ?? undefined}
            />
          )}

          {initialTab === 'danger' && <DangerTab onDeleteClick={() => setDeletingProject(true)} />}
        </div>
      </div>

      <Dialog open={deletingProject} onOpenChange={setDeletingProject}>
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Delete project</DialogTitle>
            <DialogDescription>
              Are you sure you want to delete <strong>{project?.name}</strong>? This action cannot
              be undone.
            </DialogDescription>
          </DialogHeader>
          <DialogFooter className="mt-4">
            <Button variant="outline" onClick={() => setDeletingProject(false)}>
              Cancel
            </Button>
            <Button
              disabled={deleteProject.isPending}
              variant="destructive"
              onClick={() => {
                deleteProject.mutate(projectId, {
                  onSuccess: async () => {
                    toast.success('Project deleted')
                    // F17 / 8.4.4: clear every deleted-scope cache, chat
                    // state, and the persisted selection, then land on
                    // another authorized Project or Main Chat — never a
                    // stale Project-scoped page, never the fabricated
                    // `default` id `/` used to fall back to.
                    clearDeletedProjectScope(queryClient, projectId)
                    const nextProjectId = await resolveNextProjectId(queryClient, projectId)
                    void navigate(
                      nextProjectId
                        ? {
                            to: '/projects/$projectId/board',
                            params: { projectId: nextProjectId },
                          }
                        : { to: '/chat' },
                    )
                  },
                  onError: (error) => {
                    toast.error(
                      error instanceof Error
                        ? getApiErrorMessage(error)
                        : 'Failed to delete project',
                    )
                  },
                })
              }}
            >
              {deleteProject.isPending ? 'Deleting...' : 'Delete'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}

function ProjectMcpTab({ projectId }: { projectId: string }) {
  const isAdmin = useAuthStore((s) => Boolean(s.user?.is_admin))
  const projectQuery = useProjectQuery(projectId)
  const reposQuery = useReposQuery(projectId)
  const daemonsQuery = useDaemonsQuery(isAdmin)
  const createRepo = useCreateRepo(projectId)

  const [repoDialogOpen, setRepoDialogOpen] = useState(false)
  const [repoForm, setRepoForm] = useState<RepoFormState>(emptyRepoForm)
  const [selectedDaemonId, setSelectedDaemonId] = useState<string | undefined>(undefined)

  const project = projectQuery.data
  const repos = reposQuery.data?.items ?? []
  const daemons = daemonsQuery.data?.items ?? []
  const primaryRepo = repos.find((r) => r.id === project?.primary_repo_id)
  const activeDaemonId = daemons.length === 1 ? daemons[0]?.id : selectedDaemonId

  useEffect(() => {
    if (!selectedDaemonId) return
    if (daemons.length > 1 && daemons.some((daemon) => daemon.id === selectedDaemonId)) return
    setSelectedDaemonId(undefined)
  }, [daemons, selectedDaemonId])

  const hasLocalRepo = primaryRepo?.local_path != null

  const openAddLocalRepo = () => {
    setRepoForm({ ...emptyRepoForm, source_mode: 'local' })
    setRepoDialogOpen(true)
  }

  const openAddRemoteRepo = () => {
    setRepoForm({ ...emptyRepoForm, source_mode: 'remote' })
    setRepoDialogOpen(true)
  }

  const submitRepo = (nextForm = repoForm) => {
    const localPath = nextForm.local_path.trim()
    const remoteUrlInput = nextForm.remote_url.trim()
    if (nextForm.source_mode === 'local' && !localPath) {
      toast.error('Local repo path is required')
      return
    }
    if (nextForm.source_mode === 'remote' && !remoteUrlInput) {
      toast.error('Remote URL is required')
      return
    }
    const remoteUrl =
      nextForm.source_mode === 'local' ? remoteUrlInput || localPath : remoteUrlInput
    createRepo.mutate(
      {
        remote_url: remoteUrl,
        name: nextForm.name.trim() || null,
        local_path: nextForm.source_mode === 'local' ? localPath : null,
        default_branch: nextForm.default_branch.trim() || 'main',
        work_mode: nextForm.work_mode,
        pr_provider: nextForm.work_mode === 'pull_request' ? nextForm.pr_provider : null,
        pr_provider_config: null,
      },
      {
        onError: (error) => toast.error(settingsErrorMessage(error, 'Repository creation failed')),
        onSuccess: () => {
          setRepoDialogOpen(false)
          setRepoForm(emptyRepoForm)
        },
      },
    )
  }

  const isLoading = projectQuery.isLoading || reposQuery.isLoading

  return (
    <>
      <div className="mb-8">
        <h2 className="text-page font-semibold tracking-tight">MCP</h2>
        <p className="mt-1 text-sm text-muted-foreground">
          Project-scoped Model Context Protocol configuration.
        </p>
      </div>

      {!isLoading && !hasLocalRepo ? (
        <div className="rounded-lg border border-dashed p-8 text-center">
          <div className="mx-auto mb-4 flex h-11 w-11 items-center justify-center rounded-full bg-muted">
            <Database size={20} className="text-muted-foreground" />
          </div>
          <p className="font-medium">No local repository configured</p>
          <p className="mt-1.5 text-sm text-muted-foreground">
            MCP config files are installed inside the project repository.
            {primaryRepo
              ? ' The primary repository does not have a local path set.'
              : ' Add a local repository to enable MCP setup.'}
          </p>
          {daemons.length > 1 && (
            <div className="mt-4 flex items-center justify-center gap-2">
              <label className="text-sm text-muted-foreground">
                Browse via {productTerm('runtime').toLowerCase()}:
              </label>
              <select
                className="h-8 rounded-md border border-border bg-background px-2 text-sm text-foreground focus:outline-none focus:ring-1 focus:ring-ring"
                value={activeDaemonId ?? ''}
                onChange={(e) => setSelectedDaemonId(e.target.value || undefined)}
              >
                {daemons.map((d) => (
                  <option key={d.id} value={d.id}>
                    {d.hostname} {d.status !== 'online' ? '(offline)' : ''}
                  </option>
                ))}
              </select>
            </div>
          )}
          <div className="mt-4 flex items-center justify-center gap-2">
            {daemons.length > 0 ? (
              <Button size="sm" variant="outline" onClick={openAddLocalRepo}>
                <FolderOpen size={14} className="mr-1.5" />
                Add Local Repo
              </Button>
            ) : null}
            <Button size="sm" variant="outline" onClick={openAddRemoteRepo}>
              <CloudArrowDown size={14} className="mr-1.5" />
              Add Remote Repo
            </Button>
            <Link
              to="/projects/$projectId/settings/$tab"
              params={{ projectId, tab: 'repos' }}
              className="inline-flex h-8 items-center gap-1.5 rounded-md border border-border bg-background px-3 text-sm font-medium text-foreground hover:bg-accent/50 transition-colors"
            >
              <GitBranch size={14} />
              Go to Repos
            </Link>
          </div>
        </div>
      ) : (
        <SettingsSection
          title="Project MCP"
          description="Connect MCP-compatible clients to this project's Forge endpoint."
        >
          <McpInstallControls scope="project" projectId={projectId} />
        </SettingsSection>
      )}

      <RepoDialog
        form={repoForm}
        open={repoDialogOpen}
        pending={createRepo.isPending}
        daemons={daemons}
        daemonId={activeDaemonId}
        onDaemonChange={setSelectedDaemonId}
        title={repoForm.source_mode === 'remote' ? 'Add Remote Repository' : 'Add Local Repository'}
        onOpenChange={setRepoDialogOpen}
        onSubmit={submitRepo}
        onUpdate={setRepoForm}
      />
    </>
  )
}
