import {
  lazy,
  Suspense,
  useCallback,
  useEffect,
  useRef,
  useState,
  type ComponentType,
  type ReactNode,
} from 'react'
import { Link, useNavigate, useRouterState } from '@tanstack/react-router'
import type { IconWeight } from '@phosphor-icons/react'
import {
  List,
  ChatCircleDots,
  Robot,
  Kanban,
  ChartLineUp,
  Gear,
  Key,
  Sliders,
  Sun,
  Moon,
  CaretUpDown,
  Plus,
  Pause,
  Pulse,
  Check,
  Desktop,
  SidebarSimple,
  MagnifyingGlass,
  SignOut,
  UserCircle,
} from '@phosphor-icons/react'
import { useTranslation } from 'react-i18next'
import { useAgentsQuery, useCreateProject, useProjectsInfiniteQuery } from '@/api/hooks'
import { logoutApi } from '@/api/auth'
import { Avatar } from '@/components/ui/avatar'
import { ChatLauncher } from '@/components/chat/chat-launcher'
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
import { useLayoutStore } from '@/stores/layout'
import { useAuthStore } from '@/stores/auth'
import type { Agent } from '@/types/generated/api'

const CommandPalette = lazy(() =>
  import('@/components/command-palette').then((module) => ({
    default: module.CommandPalette,
  })),
)

type NavItem = {
  to: string
  key:
    | 'overview'
    | 'board'
    | 'tasks'
    | 'mainChat'
    | 'agentWorkspace'
    | 'agentSettings'
    | 'missionControl'
    | 'daemons'
    | 'operations'
    | 'settings'
    | 'forgeSettings'
  icon: ComponentType<{ size?: string | number; weight?: IconWeight }>
  section: 'main' | 'project' | 'global'
}

const navItems: NavItem[] = [
  { to: '/projects/$projectId/overview', key: 'overview', icon: ChartLineUp, section: 'project' },
  { to: '/projects/$projectId/board', key: 'board', icon: Kanban, section: 'project' },
  { to: '/projects/$projectId/tasks', key: 'tasks', icon: List, section: 'project' },
  {
    to: '/projects/$projectId/chat',
    key: 'agentWorkspace',
    icon: ChatCircleDots,
    section: 'project',
  },
  { to: '/projects/$projectId/settings', key: 'settings', icon: Gear, section: 'project' },
  { to: '/chat', key: 'mainChat', icon: ChatCircleDots, section: 'main' },
  { to: '/agents', key: 'agentSettings', icon: Robot, section: 'global' },
  { to: '/mission-control', key: 'missionControl', icon: Pulse, section: 'global' },
  { to: '/daemons', key: 'daemons', icon: Desktop, section: 'global' },
  { to: '/operations', key: 'operations', icon: Pulse, section: 'global' },
  { to: '/settings', key: 'forgeSettings', icon: Sliders, section: 'global' },
]

export function navigationItemsForSection(section: NavItem['section']) {
  return navItems.filter((item) => item.section === section)
}

const DRAWER_FOCUSABLE_SELECTOR =
  'a[href],button:not([disabled]),input:not([disabled]),select:not([disabled]),textarea:not([disabled]),[tabindex]:not([tabindex="-1"])'

export function getDrawerFocusableElements(container: HTMLElement): HTMLElement[] {
  return Array.from(container.querySelectorAll<HTMLElement>(DRAWER_FOCUSABLE_SELECTOR)).filter(
    (element) => !element.hasAttribute('aria-hidden'),
  )
}

function isDrawerOwnedOverlayTarget(target: EventTarget | null): boolean {
  return (
    target instanceof HTMLElement &&
    Boolean(target.closest('[role="dialog"], [data-drawer-owned-overlay]'))
  )
}

/** Keep keyboard focus within the open mobile/tablet navigation drawer. */
export function trapDrawerFocus(event: KeyboardEvent, container: HTMLElement): boolean {
  if (event.key !== 'Tab') return false

  const focusable = getDrawerFocusableElements(container)
  if (focusable.length === 0) {
    event.preventDefault()
    container.focus()
    return true
  }

  const activeElement = document.activeElement as HTMLElement | null
  const activeIndex = activeElement ? focusable.indexOf(activeElement) : -1
  const wrapsBackward = event.shiftKey && (activeIndex <= 0 || activeIndex === -1)
  const wrapsForward =
    !event.shiftKey && (activeIndex === -1 || activeIndex === focusable.length - 1)

  if (wrapsBackward || wrapsForward) {
    event.preventDefault()
    const next = event.shiftKey ? focusable[focusable.length - 1] : focusable[0]
    next.focus()
    return true
  }

  return false
}

const PROJECTS_PAGE_SIZE = 20

function ProjectSwitcher({
  projectId,
  collapsed,
}: {
  projectId: string | undefined
  collapsed: boolean
}) {
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
  const availableAgents = (agentsQuery.data?.items ?? []).filter((agent) => !agent.paused)

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

  if (collapsed) {
    return (
      <div className="mb-1 flex min-w-0 justify-center px-1">
        <DropdownMenu>
          <DropdownMenuTrigger
            className="flex items-center justify-center rounded-lg transition-opacity hover:opacity-80"
            aria-label={t('projectSwitcher.switchProject')}
            title={currentProject?.name ?? t('projectSwitcher.selectProject')}
          >
            <Avatar name={currentProject?.name ?? 'P'} seed={projectId ?? 'default'} size="md" />
          </DropdownMenuTrigger>
          <DropdownMenuContent align="start" side="bottom" className="w-52">
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

  return (
    <div className="mb-1 min-w-0 px-2">
      <DropdownMenu className="block w-full">
        <DropdownMenuTrigger className="flex w-full min-w-0 items-center gap-2.5 overflow-hidden rounded-lg px-2.5 py-2 text-ui transition-colors hover:bg-sidebar-hover">
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

function NavSection({
  label,
  collapsed,
  children,
}: {
  label: string
  collapsed: boolean
  children: ReactNode
}) {
  if (collapsed) {
    return <div className="space-y-0.5 px-1">{children}</div>
  }
  return (
    <div>
      <p className="mb-1 px-4 font-mono text-micro font-semibold uppercase tracking-[1.2px] text-muted-foreground">
        {label}
      </p>
      <div className="space-y-0.5 px-2">{children}</div>
    </div>
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

export function AppShell({ children }: { children: ReactNode }) {
  const { t } = useTranslation()
  const isAdmin = useAuthStore((s) => Boolean(s.user?.is_admin))
  const sidebarCollapsed = useLayoutStore((s) => s.sidebarCollapsed)
  const setSidebarCollapsed = useLayoutStore((s) => s.setSidebarCollapsed)
  const theme = useLayoutStore((s) => s.theme)
  const setTheme = useLayoutStore((s) => s.setTheme)
  const shellMode = useShellMode()
  const [railExpanded, setRailExpanded] = useState(false)
  const [drawerOpen, setDrawerOpen] = useState(false)
  const menuButtonRef = useRef<HTMLButtonElement>(null)
  const drawerRef = useRef<HTMLElement>(null)
  const params = useRouterState({
    select: (state) => state.matches.at(-1)?.params as { projectId?: string } | undefined,
  })
  const projectsQuery = useProjectsInfiniteQuery(PROJECTS_PAGE_SIZE)
  const storedProjectId = useLayoutStore((s) => s.selectedProjectId)
  const setSelectedProjectId = useLayoutStore((s) => s.setSelectedProjectId)
  const routeProjectId = params?.projectId
  const pathname = useRouterState({ select: (state) => state.location.pathname })
  const isGlobalChatRoute = pathname === '/chat'
  const isBoardRoute = /^\/projects\/[^/]+\/board$/.test(pathname)
  // Chat pins its header and composer; only the message timeline scrolls.
  const isChatRoute = pathname === '/chat' || /^\/projects\/[^/]+\/chat$/.test(pathname)
  const firstProjectId = projectsQuery.data?.pages[0]?.items[0]?.id
  const projectId = routeProjectId ?? storedProjectId ?? firstProjectId
  const effectiveCollapsed =
    shellMode === 'full' ? sidebarCollapsed : shellMode === 'rail' ? !railExpanded : false

  const closeDrawer = useCallback((restoreFocus = true) => {
    setDrawerOpen(false)
    if (restoreFocus) requestAnimationFrame(() => menuButtonRef.current?.focus())
  }, [])

  useEffect(() => {
    if (routeProjectId && routeProjectId !== storedProjectId) {
      setSelectedProjectId(routeProjectId)
    }
  }, [routeProjectId, storedProjectId, setSelectedProjectId])

  useEffect(() => {
    setRailExpanded(false)
    if (shellMode !== 'overlay') setDrawerOpen(false)
  }, [shellMode])

  useEffect(() => {
    if (!drawerOpen || shellMode !== 'overlay') return
    const drawer = drawerRef.current
    if (!drawer) return

    const focusFirst = () => {
      getDrawerFocusableElements(drawer)[0]?.focus()
    }
    const focusFrame = requestAnimationFrame(focusFirst)
    let disposed = false
    const onKeyDown = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        event.preventDefault()
        closeDrawer()
        return
      }
      if (isDrawerOwnedOverlayTarget(document.activeElement)) return
      trapDrawerFocus(event, drawer)
    }
    const onFocusIn = (event: FocusEvent) => {
      if (drawer.contains(event.target as Node) || isDrawerOwnedOverlayTarget(event.target)) return
      requestAnimationFrame(() => {
        if (!disposed && drawerOpen && !drawer.contains(document.activeElement)) focusFirst()
      })
    }
    document.addEventListener('keydown', onKeyDown)
    document.addEventListener('focusin', onFocusIn)
    return () => {
      disposed = true
      cancelAnimationFrame(focusFrame)
      document.removeEventListener('keydown', onKeyDown)
      document.removeEventListener('focusin', onFocusIn)
    }
  }, [closeDrawer, drawerOpen, shellMode])

  const projectNavItems = navigationItemsForSection('project')
  const globalNavItems = navigationItemsForSection('global').filter((item) => {
    if (
      item.key === 'daemons' ||
      item.key === 'operations' ||
      item.key === 'forgeSettings'
    ) {
      return isAdmin
    }
    return true
  })

  const renderNavLink = (item: NavItem) => {
    const Icon = item.icon
    const isProjectRoute = item.section === 'project'

    if (isProjectRoute && !projectId) {
      return (
        <button
          key={item.key}
          className={cn(
            'flex w-full items-center gap-2.5 rounded-lg px-2.5 py-2 text-[13px] leading-none text-muted-foreground/50',
            effectiveCollapsed && 'justify-center px-0',
          )}
          disabled
          type="button"
        >
          <Icon size={16} />
          {!effectiveCollapsed && <span>{t(`appShell.navigation.${item.key}`)}</span>}
        </button>
      )
    }

    const linkProps = isProjectRoute
      ? { to: item.to, params: { projectId: projectId! } }
      : { to: item.to }

    return (
      <Link
        key={item.key}
        {...linkProps}
        aria-label={t(`appShell.navigation.${item.key}`)}
        onClick={() => {
          if (shellMode === 'overlay') closeDrawer(false)
        }}
        className={cn(
          'group relative flex items-center gap-2.5 rounded-lg px-2.5 py-[7px] text-[13px] leading-none font-medium transition-colors hover:bg-sidebar-hover hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-1',
          effectiveCollapsed && 'justify-center px-0',
        )}
        inactiveProps={{
          className: 'text-sidebar-foreground',
        }}
        activeProps={{
          className:
            'bg-[var(--ember-surface)] text-sidebar-active-foreground before:absolute before:left-0 before:top-1/2 before:-translate-y-1/2 before:h-4 before:w-[3px] before:rounded-r-full before:bg-primary',
        }}
      >
        <Icon size={16} />
        {!effectiveCollapsed && <span>{t(`appShell.navigation.${item.key}`)}</span>}
      </Link>
    )
  }

  return (
    <div
      className="flex h-[100dvh] min-h-[100svh] overflow-hidden bg-background"
      data-shell-mode={shellMode}
    >
      <a
        href="#main-content"
        className="sr-only focus:not-sr-only focus:absolute focus:left-4 focus:top-4 focus:z-50 focus:rounded-md focus:bg-background focus:px-3 focus:py-2 focus:text-sm focus:ring-2 focus:ring-ring"
      >
        Skip to main content
      </a>

      {shellMode === 'overlay' && drawerOpen ? (
        <button
          type="button"
          aria-label="Close navigation"
          className="fixed inset-0 z-40 cursor-default bg-black/35 backdrop-blur-[1px]"
          onClick={() => closeDrawer()}
        />
      ) : null}

      {shellMode !== 'overlay' || drawerOpen ? (
        <aside
          ref={drawerRef}
          id="forge-navigation-drawer"
          aria-label="Primary navigation"
          role={shellMode === 'overlay' ? 'dialog' : undefined}
          aria-modal={shellMode === 'overlay' ? true : undefined}
          data-navigation-state={effectiveCollapsed ? 'collapsed' : 'expanded'}
          className={cn(
            'flex shrink-0 flex-col border-r border-sidebar-border bg-sidebar motion-safe:transition-[width,transform] motion-safe:duration-200',
            effectiveCollapsed ? 'w-14' : 'w-60',
            shellMode === 'overlay' && 'fixed inset-y-0 left-0 z-50 shadow-float',
          )}
        >
          {/* Sidebar header */}
          <div
            className={cn(
              'flex h-14 shrink-0 items-center border-b border-sidebar-border',
              effectiveCollapsed ? 'justify-center px-1' : 'justify-between px-3',
            )}
          >
            {!effectiveCollapsed && (
              <div className="flex items-center gap-2">
                <img src="/logo.png" alt="Forge" className="h-7 w-7 rounded-lg" />
                <span className="text-sm font-semibold tracking-tight text-foreground">Forge</span>
              </div>
            )}
            <button
              type="button"
              className="flex h-7 w-7 cursor-pointer items-center justify-center rounded-md text-muted-foreground transition-colors hover:bg-sidebar-hover hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
              onClick={() => {
                if (shellMode === 'full') setSidebarCollapsed(!sidebarCollapsed)
                else if (shellMode === 'rail') setRailExpanded((expanded) => !expanded)
                else closeDrawer()
              }}
              aria-label={effectiveCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}
            >
              <SidebarSimple size={16} weight={effectiveCollapsed ? 'regular' : 'fill'} />
            </button>
          </div>

          {/* Navigation */}
          <nav className="flex flex-1 flex-col gap-4 overflow-y-auto py-3">
            {/* Project switcher */}
            <ProjectSwitcher projectId={projectId} collapsed={effectiveCollapsed} />

            {/* Account-level Main Chat */}
            <div className={effectiveCollapsed ? 'px-1' : 'px-2'}>
              {navigationItemsForSection('main').map(renderNavLink)}
            </div>

            {/* Project nav */}
            <NavSection
              label={t('appShell.navigation.project', 'Project')}
              collapsed={effectiveCollapsed}
            >
              {projectNavItems.map(renderNavLink)}
            </NavSection>

            {/* Global nav */}
            <NavSection
              label={t('appShell.navigation.workspace', 'Workspace')}
              collapsed={effectiveCollapsed}
            >
              {globalNavItems.map(renderNavLink)}
            </NavSection>
          </nav>
        </aside>
      ) : null}

      {/* Main content */}
      <div className="flex min-h-0 flex-1 flex-col overflow-hidden">
        {/* Header */}
        <header className="flex h-14 shrink-0 items-center justify-between gap-2 border-b border-sidebar-border bg-background px-3 sm:px-4 lg:px-5">
          <div className="flex min-w-0 items-center gap-2">
            {shellMode === 'overlay' ? (
              <button
                ref={menuButtonRef}
                type="button"
                aria-controls="forge-navigation-drawer"
                aria-expanded={drawerOpen}
                aria-label="Open navigation"
                className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg border border-input bg-card text-muted-foreground hover:bg-muted hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                onClick={() => setDrawerOpen(true)}
              >
                <List size={17} />
              </button>
            ) : null}
            {projectId && !isGlobalChatRoute ? (
              <Suspense
                fallback={
                  <button
                    className="flex min-w-0 items-center gap-2 rounded-lg border border-input bg-card px-3 py-1.5 text-ui text-muted-foreground opacity-50"
                    disabled
                    type="button"
                  >
                    <MagnifyingGlass size={14} />
                    <span>{t('commandPalette.button')}</span>
                  </button>
                }
              >
                <CommandPalette projectId={projectId} />
              </Suspense>
            ) : (
              <button
                className="flex min-w-0 items-center gap-2 rounded-lg border border-input bg-card px-3 py-1.5 text-ui text-muted-foreground opacity-50"
                disabled
                type="button"
              >
                <MagnifyingGlass size={14} />
                <span>{t('commandPalette.button')}</span>
              </button>
            )}
          </div>
          <div className="flex items-center gap-2">
            <NotificationCenter projectId={projectId} />
            <button
              type="button"
              className="flex h-8 w-8 cursor-pointer items-center justify-center rounded-lg border border-input bg-card text-muted-foreground transition-colors hover:bg-muted hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
              onClick={() => setTheme(theme === 'light' ? 'dark' : 'light')}
              aria-label={t('appShell.toggleTheme')}
            >
              {theme === 'light' ? <Moon size={15} /> : <Sun size={15} />}
            </button>
            <UserMenu />
          </div>
        </header>
        <main
          id="main-content"
          className={cn(
            'min-h-0 flex-1 bg-card p-3 sm:p-4 lg:p-5',
            isBoardRoute || isChatRoute ? 'overflow-hidden' : 'overflow-auto',
          )}
        >
          {children}
        </main>
      </div>
      <ChatLauncher />
    </div>
  )
}

type ShellMode = 'full' | 'rail' | 'overlay'

function shellModeForWidth(width: number): ShellMode {
  if (width >= 1440) return 'full'
  if (width >= 1024) return 'rail'
  return 'overlay'
}

function useShellMode(): ShellMode {
  const [mode, setMode] = useState<ShellMode>(() =>
    typeof window === 'undefined' ? 'full' : shellModeForWidth(window.innerWidth),
  )
  useEffect(() => {
    const update = () => setMode(shellModeForWidth(window.innerWidth))
    window.addEventListener('resize', update)
    return () => window.removeEventListener('resize', update)
  }, [])
  return mode
}
