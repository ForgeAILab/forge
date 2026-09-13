import { useEffect, useState, type ReactNode } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { Link, useNavigate, useRouterState } from '@tanstack/react-router'
import {
  ChatCircleDots,
  Kanban,
  Key,
  Sun,
  Moon,
  CaretUpDown,
  Plus,
  Pause,
  Check,
  DotsThree,
  SignOut,
  UserCircle,
} from '@phosphor-icons/react'
import { useTranslation } from 'react-i18next'
import { useAgentsQuery, useCreateProject, useProjectsInfiniteQuery } from '@/api/hooks'
import { logoutApi } from '@/api/auth'
import { Avatar } from '@/components/ui/avatar'
import { NotificationCenter } from '@/components/notification-center'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { cn } from '@/lib/cn'
import { usableAgents } from '@/lib/agent-availability'
import { useLayoutStore } from '@/stores/layout'
import { useAuthStore } from '@/stores/auth'
import { clearDeletedProjectScope, resolveNextProjectId } from '@/stores/project-scope'
import type { Agent } from '@/types/generated/api'
import { navigationItemsForSection, type AppShellNavItem } from '@/components/app-shell-navigation'

const PROJECTS_PAGE_SIZE = 20

function ProjectSwitcher({ projectId }: { projectId: string | undefined }) {
  const { t } = useTranslation()
  const navigate = useNavigate()
  const projectsQuery = useProjectsInfiniteQuery(PROJECTS_PAGE_SIZE)
  const createProject = useCreateProject()
  const agentsQuery = useAgentsQuery()
  const [createOpen, setCreateOpen] = useState(false)
  const [newName, setNewName] = useState('')
  const [selectedAgentId, setSelectedAgentId] = useState('')
  const [error, setError] = useState('')

  const projects = projectsQuery.data?.pages.flatMap((page) => page.items) ?? []
  const currentProject = projects.find((p) => p.id === projectId)
  const availableAgents = usableAgents(agentsQuery.data?.items ?? [])

  const fetchNextProjectsPage = () => {
    if (projectsQuery.hasNextPage && !projectsQuery.isFetchingNextPage) {
      void projectsQuery.fetchNextPage()
    }
  }

  const handleProjectsScroll = (event: React.UIEvent<HTMLDivElement>) => {
    const target = event.currentTarget
    if (target.scrollHeight - target.scrollTop - target.clientHeight < 48) {
      fetchNextProjectsPage()
    }
  }

  const renderProjectMenuItems = () => (
    <div className="max-h-[70vh] overflow-y-auto pr-1" onScroll={handleProjectsScroll}>
      {projects.map((p) => (
        <DropdownMenuItem
          key={p.id}
          onClick={() =>
            void navigate({ to: '/projects/$projectId/board', params: { projectId: p.id } })
          }
        >
          <Avatar name={p.name} seed={p.id} size="xs" className="mr-2 rounded" />
          <span className="min-w-0 flex-1 truncate text-left" title={p.name}>
            {p.name}
          </span>
          {p.paused ? (
            <Pause size={12} className="ml-1 shrink-0 text-muted-foreground" weight="fill" />
          ) : null}
          {p.id === projectId && <Check size={14} className="ml-1 shrink-0 text-success" />}
        </DropdownMenuItem>
      ))}
      {projectsQuery.hasNextPage || projectsQuery.isFetchingNextPage ? (
        <DropdownMenuItem
          keepOpen
          disabled={projectsQuery.isFetchingNextPage}
          className="justify-center text-xs text-muted-foreground"
          onClick={fetchNextProjectsPage}
        >
          {projectsQuery.isFetchingNextPage ? 'Loading...' : 'Load more'}
        </DropdownMenuItem>
      ) : null}
    </div>
  )

  async function handleCreate(e: React.FormEvent) {
    e.preventDefault()
    const name = newName.trim()
    if (!name) {
      setError(t('projectSwitcher.nameRequired'))
      return
    }
    const selectedAgent = availableAgents.find((agent) => agent.id === selectedAgentId)
    try {
      const created = await createProject.mutateAsync({
        name,
        project_agent_identity_id: selectedAgent?.id ?? null,
        project_agent_profile_id: selectedAgent?.profile_id ?? null,
      })
      setCreateOpen(false)
      setNewName('')
      setSelectedAgentId('')
      setError('')
      void navigate({ to: '/projects/$projectId/board', params: { projectId: created.id } })
    } catch {
      setError(t('projectSwitcher.createFailed'))
    }
  }

  return (
    <div className="min-w-0">
      <DropdownMenu className="block w-full">
        <DropdownMenuTrigger
          className="flex h-9 w-full min-w-0 items-center gap-2 overflow-hidden rounded-lg border border-border-subtle bg-card px-2.5 text-ui shadow-xs transition-[border-color,background-color,box-shadow] hover:border-border hover:bg-muted/40 hover:shadow-soft focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          aria-label={t('projectSwitcher.switchProject')}
        >
          <Avatar
            name={currentProject?.name ?? 'P'}
            seed={projectId ?? 'default'}
            size="sm"
            className="shrink-0 rounded-md"
          />
          <span
            className="min-w-0 flex-1 truncate text-left font-medium text-foreground"
            title={currentProject?.name ?? t('projectSwitcher.selectProject')}
          >
            {currentProject?.name ?? t('projectSwitcher.selectProject')}
          </span>
          {currentProject?.paused ? (
            <Pause size={12} className="shrink-0 text-muted-foreground" weight="fill" />
          ) : null}
          <CaretUpDown size={14} className="shrink-0 text-muted-foreground" />
        </DropdownMenuTrigger>
        <DropdownMenuContent align="start" side="bottom" className="w-60">
          {renderProjectMenuItems()}
          {projects.length > 0 && <DropdownMenuSeparator />}
          <DropdownMenuItem onClick={() => setCreateOpen(true)}>
            <Plus size={14} className="mr-2" />
            <span className="flex-1 text-left">{t('projectSwitcher.createProject')}</span>
          </DropdownMenuItem>
        </DropdownMenuContent>
      </DropdownMenu>

      <CreateProjectDialog
        open={createOpen}
        onOpenChange={(v) => {
          setCreateOpen(v)
          if (!v) {
            setNewName('')
            setSelectedAgentId('')
            setError('')
          }
        }}
        name={newName}
        onNameChange={setNewName}
        agents={availableAgents}
        agentsLoading={agentsQuery.isLoading}
        selectedAgentId={selectedAgentId}
        onAgentChange={setSelectedAgentId}
        error={error}
        loading={createProject.isPending}
        onSubmit={handleCreate}
      />
    </div>
  )
}

function CreateProjectDialog({
  open,
  onOpenChange,
  name,
  onNameChange,
  agents,
  agentsLoading,
  selectedAgentId,
  onAgentChange,
  error,
  loading,
  onSubmit,
}: {
  open: boolean
  onOpenChange: (v: boolean) => void
  name: string
  onNameChange: (v: string) => void
  agents: Agent[]
  agentsLoading: boolean
  selectedAgentId: string
  onAgentChange: (v: string) => void
  error: string
  loading: boolean
  onSubmit: (e: React.FormEvent) => void
}) {
  const { t } = useTranslation()
  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent>
        <form onSubmit={onSubmit}>
          <DialogHeader>
            <DialogTitle>{t('projectSwitcher.createProject')}</DialogTitle>
          </DialogHeader>
          <div className="my-4 space-y-4">
            <p className="rounded-md border border-border-subtle bg-muted/20 px-3 py-2 text-xs leading-5 text-muted-foreground">
              Generic Project creation is available for human/API setup and starts in{' '}
              <span className="font-mono text-micro">charter_setup_required</span>. Use Product
              Genesis in the Main Chat when this Project needs a Charter-backed handoff.
            </p>
            <div className="space-y-2">
              <Label htmlFor="project-name">{t('projectSwitcher.projectName')}</Label>
              <Input
                id="project-name"
                value={name}
                onChange={(e) => onNameChange(e.target.value)}
                placeholder={t('projectSwitcher.projectNamePlaceholder')}
                autoFocus
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="project-agent">
                {t('projectSwitcher.projectAgent')}
                <span className="ml-1 font-normal text-muted-foreground">
                  {t('projectSwitcher.optional')}
                </span>
              </Label>
              <select
                id="project-agent"
                value={selectedAgentId}
                onChange={(event) => onAgentChange(event.target.value)}
                disabled={agentsLoading}
                className="h-9 w-full rounded-md border border-input bg-background px-3 text-sm text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring disabled:opacity-50"
              >
                <option value="">
                  {agentsLoading ? t('common.loading') : t('projectSwitcher.selectProjectAgent')}
                </option>
                {agents.map((agent) => (
                  <option key={agent.id} value={agent.id}>
                    {agent.name} · {agent.executor_type}
                    {agent.model ? ` · ${agent.model}` : ''}
                  </option>
                ))}
              </select>
              <p className="text-xs text-muted-foreground">{t('projectSwitcher.agentHint')}</p>
            </div>
            {error && <p className="text-sm text-destructive">{error}</p>}
          </div>
          <DialogFooter>
            <button
              type="button"
              className="cursor-pointer rounded-md border px-3 py-1.5 text-sm transition-colors hover:bg-accent"
              onClick={() => onOpenChange(false)}
            >
              {t('common.cancel')}
            </button>
            <button
              type="submit"
              disabled={loading}
              className="cursor-pointer rounded-md bg-primary px-3 py-1.5 text-sm text-primary-foreground transition-colors hover:bg-primary/90 disabled:opacity-50"
            >
              {loading ? t('common.loading') : t('common.create')}
            </button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
  )
}

function UserMenu() {
  const navigate = useNavigate()
  const user = useAuthStore((s) => s.user)
  const { refreshToken, clearAuth } = useAuthStore()

  async function handleLogout() {
    if (refreshToken) {
      try {
        await logoutApi({ refresh_token: refreshToken })
      } catch {
        // Server-side revocation failure is non-fatal; still clear local state
      }
    }
    clearAuth()
    void navigate({ to: '/login', search: { redirect: undefined } })
  }

  const label = user?.display_name ?? user?.email ?? 'Account'
  const initial = label[0]?.toUpperCase() ?? 'U'

  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        className="flex h-8 w-8 cursor-pointer items-center justify-center rounded-full bg-primary/10 text-sm font-semibold text-primary transition-colors hover:bg-primary/20 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
        aria-label="User menu"
        title={label}
      >
        {user ? initial : <UserCircle size={18} />}
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-52">
        {user && (
          <>
            <div className="px-3 py-2">
              {user.display_name && (
                <p className="truncate text-sm font-medium text-foreground">{user.display_name}</p>
              )}
              <p className="truncate text-xs text-muted-foreground">{user.email}</p>
            </div>
            <DropdownMenuSeparator />
          </>
        )}
        <DropdownMenuItem onClick={() => void navigate({ to: '/account' })}>
          <Key size={14} className="mr-2" />
          Account settings
        </DropdownMenuItem>
        <DropdownMenuSeparator />
        <DropdownMenuItem
          onClick={() => void handleLogout()}
          className="text-destructive focus:bg-destructive/10 focus:text-destructive"
        >
          <SignOut size={14} className="mr-2" />
          Sign out
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

function MoreNavigation({ projectId, isAdmin }: { projectId?: string; isAdmin: boolean }) {
  const { t } = useTranslation()
  const navigate = useNavigate()
  const projectItems = navigationItemsForSection('project')
  const workspaceItems = navigationItemsForSection('workspace').filter((item) => {
    if (item.key === 'daemons' || item.key === 'operations' || item.key === 'forgeSettings') {
      return isAdmin
    }
    return true
  })

  const goTo = (item: AppShellNavItem) => {
    switch (item.key) {
      case 'overview':
        if (projectId) void navigate({ to: '/projects/$projectId/overview', params: { projectId } })
        break
      case 'tasks':
        if (projectId)
          void navigate({
            to: '/projects/$projectId/tasks',
            params: { projectId },
            search: { sort_by: 'updated_at', sort_order: 'desc' },
          })
        break
      case 'agentWorkspace':
        if (projectId) void navigate({ to: '/projects/$projectId/chat', params: { projectId } })
        break
      case 'settings':
        if (projectId) void navigate({ to: '/projects/$projectId/settings', params: { projectId } })
        break
      case 'agentSettings':
        void navigate({ to: '/agents' })
        break
      case 'missionControl':
        void navigate({ to: '/mission-control' })
        break
      case 'daemons':
        void navigate({ to: '/daemons' })
        break
      case 'operations':
        void navigate({ to: '/operations' })
        break
      case 'forgeSettings':
        void navigate({ to: '/settings' })
        break
      default:
        break
    }
  }

  const renderMenuItem = (item: AppShellNavItem) => {
    const Icon = item.icon
    const unavailable = item.section === 'project' && !projectId
    return (
      <DropdownMenuItem key={item.key} disabled={unavailable} onClick={() => goTo(item)}>
        <Icon size={15} className="mr-2 shrink-0 text-muted-foreground" />
        <span>{t(`appShell.navigation.${item.key}`)}</span>
      </DropdownMenuItem>
    )
  }

  return (
    <DropdownMenu>
      <DropdownMenuTrigger
        aria-label="More navigation"
        className="flex h-8 w-8 cursor-pointer items-center justify-center rounded-lg border border-input bg-card text-muted-foreground shadow-xs transition-[background-color,color,transform] hover:bg-muted hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring active:scale-95"
      >
        <DotsThree size={17} weight="bold" />
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-60">
        <p className="px-2 pb-1 pt-1.5 font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
          {t('appShell.navigation.project', 'Project')}
        </p>
        {projectItems.map(renderMenuItem)}
        <DropdownMenuSeparator />
        <p className="px-2 pb-1 pt-1.5 font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
          {t('appShell.navigation.workspace', 'Workspace')}
        </p>
        {workspaceItems.map(renderMenuItem)}
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

function ForgeBrand() {
  return (
    <>
      <img src="/logo.png" alt="" className="h-7 w-7 rounded-lg" />
      <span className="hidden text-sm font-semibold tracking-tight text-foreground sm:inline">
        Forge
      </span>
    </>
  )
}

export function AppShell({ children }: { children: ReactNode }) {
  const { t } = useTranslation()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const isAdmin = useAuthStore((s) => Boolean(s.user?.is_admin))
  const theme = useLayoutStore((s) => s.theme)
  const setTheme = useLayoutStore((s) => s.setTheme)
  const params = useRouterState({
    select: (state) => state.matches.at(-1)?.params as { projectId?: string } | undefined,
  })
  const projectsQuery = useProjectsInfiniteQuery(PROJECTS_PAGE_SIZE)
  const storedProjectId = useLayoutStore((s) => s.selectedProjectId)
  const setSelectedProjectId = useLayoutStore((s) => s.setSelectedProjectId)
  const routeProjectId = params?.projectId
  const pathname = useRouterState({ select: (state) => state.location.pathname })
  const isBoardRoute = /^\/projects\/[^/]+\/board$/.test(pathname)
  // Chat pins its header and composer; only the message timeline scrolls.
  const isChatRoute = pathname === '/chat' || /^\/projects\/[^/]+\/chat$/.test(pathname)
  const firstProjectId = projectsQuery.data?.pages[0]?.items[0]?.id
  const projectId = routeProjectId ?? storedProjectId ?? firstProjectId

  useEffect(() => {
    if (routeProjectId && routeProjectId !== storedProjectId) {
      setSelectedProjectId(routeProjectId)
    }
  }, [routeProjectId, storedProjectId, setSelectedProjectId])

  // F17 / 8.4.4: converge immediately when a Project is deleted elsewhere
  // while its route is open here, the same way an explicit delete does.
  // `web/src/api/sse.ts` dispatches this on `project.deleted`; it is a
  // latency optimization only — a lost/late frame still converges once the
  // next Project-scoped fetch 404s (`DeletedProjectRedirect` on Project
  // Overview).
  useEffect(() => {
    function handleProjectDeleted(event: Event) {
      const detail = (event as CustomEvent<{ entity_id?: string }>).detail
      const deletedProjectId = detail?.entity_id
      if (!deletedProjectId) return
      clearDeletedProjectScope(queryClient, deletedProjectId)
      if (routeProjectId !== deletedProjectId) return
      void resolveNextProjectId(queryClient, deletedProjectId).then((nextProjectId) => {
        void navigate(
          nextProjectId
            ? { to: '/projects/$projectId/board', params: { projectId: nextProjectId } }
            : { to: '/chat' },
        )
      })
    }
    window.addEventListener('forge:project-deleted', handleProjectDeleted)
    return () => window.removeEventListener('forge:project-deleted', handleProjectDeleted)
  }, [queryClient, navigate, routeProjectId])

  const primaryTabClass =
    'relative inline-flex h-9 min-w-0 flex-1 items-center justify-center gap-2 rounded-md px-4 text-ui font-medium text-muted-foreground transition-[background-color,color,box-shadow,transform] hover:bg-card/70 hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1 active:scale-[0.98] md:flex-none'
  const primaryTabActiveClass =
    'bg-card text-foreground shadow-xs ring-1 ring-inset ring-ember-border'

  return (
    <div
      className="flex h-[100dvh] min-h-[100svh] flex-col overflow-hidden bg-background"
      data-shell-mode="topbar"
    >
      <a
        href="#main-content"
        className="sr-only focus:not-sr-only focus:absolute focus:left-4 focus:top-4 focus:z-50 focus:rounded-md focus:bg-background focus:px-3 focus:py-2 focus:text-sm focus:ring-2 focus:ring-ring"
      >
        Skip to main content
      </a>
      <header className="shrink-0 border-b border-border-subtle bg-background">
        <div className="flex min-h-14 flex-wrap items-center gap-2 px-3 py-2 sm:px-4 lg:px-5">
          <div className="flex min-w-0 flex-1 items-center gap-2 md:flex-none">
            {projectId ? (
              <Link
                to="/projects/$projectId/board"
                params={{ projectId }}
                aria-label="Open Kanban"
                className="flex shrink-0 items-center gap-2 rounded-lg focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
              >
                <ForgeBrand />
              </Link>
            ) : (
              <Link
                to="/chat"
                aria-label="Open Main Chat"
                className="flex shrink-0 items-center gap-2 rounded-lg focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
              >
                <ForgeBrand />
              </Link>
            )}
            <div className="min-w-0 flex-1 md:w-56 md:flex-none">
              <ProjectSwitcher projectId={projectId} />
            </div>
          </div>

          <nav
            className="order-3 flex w-full items-center gap-1 rounded-lg border border-border-subtle bg-muted/40 p-1 md:order-none md:w-auto"
            aria-label="Primary navigation"
            data-primary-navigation
          >
            {projectId ? (
              <Link
                to="/projects/$projectId/board"
                params={{ projectId }}
                activeOptions={{ exact: true }}
                className={primaryTabClass}
                activeProps={{ className: primaryTabActiveClass }}
                aria-label="Kanban"
              >
                <Kanban size={16} />
                <span>{t('appShell.navigation.board')}</span>
              </Link>
            ) : (
              <button
                type="button"
                disabled
                title="Select or create a Project to open Kanban"
                className={cn(primaryTabClass, 'cursor-not-allowed opacity-50')}
              >
                <Kanban size={16} />
                <span>{t('appShell.navigation.board')}</span>
              </button>
            )}
            <Link
              to="/chat"
              activeOptions={{ exact: true }}
              className={primaryTabClass}
              activeProps={{ className: primaryTabActiveClass }}
              aria-label="Main Chat"
            >
              <ChatCircleDots size={16} />
              <span>{t('appShell.navigation.mainChat')}</span>
            </Link>
          </nav>

          <div className="ml-auto flex shrink-0 items-center gap-2">
            <NotificationCenter projectId={projectId} />
            <MoreNavigation projectId={projectId} isAdmin={isAdmin} />
            <button
              type="button"
              className="flex h-8 w-8 cursor-pointer items-center justify-center rounded-lg border border-input bg-card text-muted-foreground shadow-xs transition-[background-color,color,transform] hover:bg-muted hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring active:scale-95"
              onClick={() => setTheme(theme === 'light' ? 'dark' : 'light')}
              aria-label={t('appShell.toggleTheme')}
            >
              {theme === 'light' ? <Moon size={15} /> : <Sun size={15} />}
            </button>
            <UserMenu />
          </div>
        </div>
      </header>

      <main
        id="main-content"
        className={cn(
          'min-h-0 flex-1 bg-background p-3 sm:p-4 lg:p-5',
          isBoardRoute || isChatRoute ? 'overflow-hidden' : 'overflow-auto',
        )}
      >
        {children}
      </main>
    </div>
  )
}
