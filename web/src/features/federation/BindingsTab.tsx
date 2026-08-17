import { useEffect, useMemo, useState, type ReactNode } from 'react'
import { Link } from '@tanstack/react-router'
import { ArrowUpRight, Folder, Globe, MagnifyingGlass } from '@phosphor-icons/react'
import { useProjectsQuery } from '@/api/hooks'
import { MainAgentBindingCard } from '@/components/settings/MainAgentBindingCard'
import { ProjectAgentTab } from '@/components/settings/ProjectAgentTab'
import { Input } from '@/components/ui/input'
import { cn } from '@/lib/cn'
import type { AgentChatEntry } from '@/features/agent-chat/types'
import type { FederatedAgent } from '@/features/federation/types'
import {
  ErrorPanel,
  LoadingPanel,
  SectionKicker,
  StateBadge,
  StatusDot,
} from '@/features/federation/components'
import type { Project } from '@/types/generated'
import { humanize } from './format'

/** The scope id of the single Global (Main Agent) scope. */
const MAIN_SCOPE = 'main'

function mainChatEntry(entries: AgentChatEntry[]): AgentChatEntry | undefined {
  return entries.find((entry) => entry.kind === 'main')
}

function projectChatEntry(
  entries: AgentChatEntry[],
  projectId: string,
): AgentChatEntry | undefined {
  return entries.find((entry) => entry.kind === 'project' && entry.project_id === projectId)
}

/** An active binding reports chat status; anything else reports its binding state. */
function scopeStatus(entry: AgentChatEntry | undefined): string {
  if (!entry) return 'setup_required'
  return entry.binding_state === 'active' ? entry.chat_status : entry.binding_state
}

/** Compact pill naming the bound agent, or "Not configured". */
function AgentScopeBadge({ entry }: { entry: AgentChatEntry | undefined }) {
  const identity = (entry?.binding_state === 'active' ? entry.identity_name : null) ?? ''
  const configured = identity.length > 0
  return (
    <span
      className={cn(
        'ml-auto inline-flex max-w-[10rem] shrink-0 items-center gap-1.5 rounded-full border px-2 py-0.5 text-micro font-medium',
        configured
          ? 'border-success/30 bg-success/10 text-success'
          : 'border-border bg-muted text-muted-foreground',
      )}
      title={configured ? identity : 'Not configured'}
    >
      <StatusDot status={scopeStatus(entry)} />
      <span className="truncate">{configured ? identity : 'Not configured'}</span>
    </span>
  )
}

/** One selectable scope in the roster, styled like the Agents tab's RosterRow. */
function ScopeRow({
  icon,
  title,
  subtitle,
  selected,
  onSelect,
  children,
}: {
  icon: ReactNode
  title: string
  subtitle: string
  selected: boolean
  onSelect: () => void
  children?: ReactNode
}) {
  return (
    <button
      type="button"
      onClick={onSelect}
      aria-current={selected}
      className={cn(
        'relative flex w-full items-center gap-3 rounded-lg px-3 py-2.5 text-left transition-colors',
        selected
          ? 'border border-primary/20 bg-primary/8 text-foreground before:absolute before:left-0 before:top-1/2 before:-translate-y-1/2 before:h-4 before:w-[3px] before:rounded-r-full before:bg-primary'
          : 'border border-transparent text-foreground hover:bg-accent/50',
      )}
    >
      <div className="flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-ember-surface text-primary">
        {icon}
      </div>
      <div className="min-w-0 flex-1">
        <p className="truncate text-sm font-medium">{title}</p>
        <p className="mt-0.5 truncate text-xs text-muted-foreground">{subtitle}</p>
      </div>
      {children}
    </button>
  )
}

function ScopeSectionHeader({ kicker, title }: { kicker: string; title: string }) {
  return (
    <div className="px-3 pb-1 pt-2">
      <SectionKicker>{kicker}</SectionKicker>
      <p className="mt-0.5 text-xs font-medium text-foreground">{title}</p>
    </div>
  )
}

function ScopeDetailHeader({
  icon,
  kicker,
  title,
  children,
}: {
  icon: ReactNode
  kicker: string
  title: string
  children?: ReactNode
}) {
  return (
    <header className="flex shrink-0 items-start gap-4 border-b border-border-subtle px-6 py-4">
      <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-lg bg-ember-surface text-primary">
        {icon}
      </div>
      <div className="min-w-0 flex-1">
        <SectionKicker>{kicker}</SectionKicker>
        <div className="mt-0.5 flex flex-wrap items-center gap-2">
          <h2 className="truncate text-lg font-semibold text-foreground">{title}</h2>
          {children}
        </div>
      </div>
    </header>
  )
}

function BoundAgentScopes({
  entries,
  isLoading,
  isError,
  onRetry,
}: {
  entries: AgentChatEntry[]
  isLoading: boolean
  isError: boolean
  onRetry: () => void
}) {
  if (isLoading) return <LoadingPanel label="Loading Main and Project Agent bindings" />
  if (isError) {
    return (
      <ErrorPanel
        title="Agent binding projection unavailable"
        description="Forge could not load the current Main and Project Agent scopes. Retry before relying on this view."
        onRetry={onRetry}
      />
    )
  }
  return (
    <section
      aria-labelledby="bound-agent-scopes-heading"
      className="overflow-hidden rounded-xl border border-border-subtle bg-card shadow-soft"
    >
      <header className="border-b border-border-subtle px-4 py-4 sm:px-5">
        <SectionKicker>Bound agent scopes</SectionKicker>
        <h2 id="bound-agent-scopes-heading" className="mt-1 text-lg font-semibold text-foreground">
          Main and Project Agent bindings
        </h2>
        <p className="mt-1 max-w-2xl text-sm leading-6 text-muted-foreground">
          These are the only durable chat owners. Task Workers and reviewers appear in Task detail,
          while unbound agents stay in the Agents tab.
        </p>
      </header>
      {entries.length > 0 ? (
        <div className="divide-y divide-ember-border">
          {entries.map((entry) => {
            const isMain = entry.kind === 'main'
            const label = isMain ? 'Global · Main' : (entry.project_name ?? 'Project Agent')
            const identity = entry.identity_name ?? 'Setup required'
            const status =
              entry.binding_state === 'active' ? entry.chat_status : entry.binding_state
            return (
              <div
                key={entry.chat_id}
                className="flex min-w-0 items-center justify-between gap-3 px-4 py-3"
              >
                <div className="min-w-0">
                  <p className="truncate text-sm font-medium text-foreground">{label}</p>
                  <p className="mt-1 truncate text-xs text-muted-foreground">
                    {identity} · {isMain ? 'account-owned timeline' : 'Project-owned timeline'}
                  </p>
                </div>
                <div className="flex shrink-0 items-center gap-3">
                  <StateBadge status={status} label={humanize(status)} />
                  {isMain ? (
                    <Link
                      to="/chat"
                      className="inline-flex items-center gap-1 text-xs font-medium text-primary hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                    >
                      Open chat
                      <ArrowUpRight size={13} aria-hidden />
                    </Link>
                  ) : (
                    <Link
                      to="/projects/$projectId/chat"
                      params={{ projectId: entry.project_id ?? '' }}
                      className="inline-flex items-center gap-1 text-xs font-medium text-primary hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                    >
                      Open chat
                      <ArrowUpRight size={13} aria-hidden />
                    </Link>
                  )}
                </div>
              </div>
            )
          })}
        </div>
      ) : (
        <p className="px-4 py-4 text-sm text-muted-foreground">
          No Main or Project Agent binding is visible yet. Create an agent and choose its owning
          scope below.
        </p>
      )}
    </section>
  )
}

/** Right-hand detail for the Global scope: the one Main Agent binding. */
function MainScopeDetail({
  agents,
  chatEntry,
  onConnect,
}: {
  agents: FederatedAgent[]
  chatEntry: AgentChatEntry | undefined
  onConnect: () => void
}) {
  const status = scopeStatus(chatEntry)
  return (
    <div className="flex min-w-0 flex-1 flex-col overflow-hidden">
      <ScopeDetailHeader
        icon={<Globe size={20} weight="duotone" aria-hidden />}
        kicker="Global"
        title="Main Agent"
      >
        <StateBadge status={status} label={humanize(status)} />
      </ScopeDetailHeader>
      <div className="flex-1 overflow-y-auto px-6 py-5">
        <MainAgentBindingCard agents={agents} onConnect={onConnect} />
      </div>
    </div>
  )
}

/** Right-hand detail for one Project's agent binding. */
function ProjectScopeDetail({
  project,
  chatEntry,
}: {
  project: Project
  chatEntry: AgentChatEntry | undefined
}) {
  return (
    <div className="flex min-w-0 flex-1 flex-col overflow-hidden">
      <ScopeDetailHeader
        icon={<Folder size={20} weight="duotone" aria-hidden />}
        kicker="Projects"
        title={project.name}
      >
        <AgentScopeBadge entry={chatEntry} />
      </ScopeDetailHeader>
      <div className="flex-1 overflow-y-auto px-6 py-5">
        <ProjectAgentTab projectId={project.id} projectName={project.name} />
      </div>
    </div>
  )
}

/**
 * Bindings tab: a Runtimes-style master/detail — the scope roster (Global Main
 * Agent plus every Project, never gated behind a URL param) on the left, the
 * selected scope's binding configuration on the right, and the read-only
 * chat-scope projection below.
 */
export function BindingsTab({
  agents,
  chatEntries,
  chatsLoading,
  chatsError,
  onRetryChats,
  onConnect,
  urlProjectId,
}: {
  agents: FederatedAgent[]
  chatEntries: AgentChatEntry[]
  chatsLoading: boolean
  chatsError: boolean
  onRetryChats: () => void
  onConnect: () => void
  urlProjectId?: string
}) {
  const projectsQuery = useProjectsQuery()
  const projects = projectsQuery.data?.items ?? []
  const [query, setQuery] = useState('')
  const [selectedScope, setSelectedScope] = useState<string>(urlProjectId ?? MAIN_SCOPE)

  // A `?project={id}` deep link wins while that project exists.
  useEffect(() => {
    if (urlProjectId && projects.some((project) => project.id === urlProjectId)) {
      setSelectedScope(urlProjectId)
    }
  }, [urlProjectId, projects])

  const normalized = query.trim().toLowerCase()
  const mainEntry = mainChatEntry(chatEntries)
  const mainMatches =
    !normalized ||
    ['Main Agent', 'Global', mainEntry?.identity_name ?? ''].some((value) =>
      value.toLowerCase().includes(normalized),
    )
  const matchingProjects = useMemo(
    () =>
      projects.filter((project) => {
        if (!normalized) return true
        const identity = projectChatEntry(chatEntries, project.id)?.identity_name ?? ''
        return [project.name, identity].some((value) => value.toLowerCase().includes(normalized))
      }),
    [chatEntries, normalized, projects],
  )

  const selectedProject = projects.find((project) => project.id === selectedScope) ?? null
  const effectiveScope =
    selectedScope === MAIN_SCOPE || selectedProject ? selectedScope : MAIN_SCOPE
  const showGlobal = mainMatches
  const showProjects = normalized === '' || matchingProjects.length > 0

  return (
    <div
      role="tabpanel"
      id="agent-settings-panel-bindings"
      aria-labelledby="agent-settings-tab-bindings"
      className="space-y-6"
    >
      <div className="flex h-[calc(100vh-17rem)] min-h-[520px] gap-0 overflow-hidden rounded-xl border border-border-subtle bg-card shadow-card">
        <div className="flex w-80 shrink-0 flex-col border-r border-border-subtle bg-background">
          <header className="flex shrink-0 items-center justify-between border-b border-border-subtle px-4 py-3">
            <div>
              <p className="font-mono text-micro font-semibold uppercase tracking-[1px] text-muted-foreground">
                Scopes
              </p>
              <p className="mt-0.5 text-[11px] text-muted-foreground">
                {projects.length + 1} total
              </p>
            </div>
          </header>
          <div className="shrink-0 border-b border-border-subtle px-3 py-2.5">
            <label className="relative block">
              <span className="sr-only">Search scopes</span>
              <MagnifyingGlass
                size={14}
                className="pointer-events-none absolute left-2.5 top-1/2 -translate-y-1/2 text-muted-foreground"
                aria-hidden
              />
              <Input
                className="pl-8"
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                placeholder="Search scopes"
              />
            </label>
          </div>
          <div className="flex-1 overflow-y-auto p-1.5">
            {projectsQuery.isLoading ? <LoadingPanel label="Loading projects" /> : null}
            {projectsQuery.isError ? (
              <div className="p-2">
                <ErrorPanel
                  title="Projects unavailable"
                  description="Forge could not load the project list. Retry before configuring a Project Agent."
                  onRetry={() => void projectsQuery.refetch()}
                />
              </div>
            ) : null}
            {!projectsQuery.isLoading && !projectsQuery.isError ? (
              <>
                {showGlobal ? (
                  <div className="space-y-0.5">
                    <ScopeSectionHeader kicker="Global" title="Main Agent binding" />
                    <ScopeRow
                      icon={<Globe size={16} weight="duotone" aria-hidden />}
                      title="Main Agent"
                      subtitle={
                        mainEntry?.identity_name
                          ? `Bound to ${mainEntry.identity_name}`
                          : 'No identity bound yet'
                      }
                      selected={effectiveScope === MAIN_SCOPE}
                      onSelect={() => setSelectedScope(MAIN_SCOPE)}
                    >
                      <AgentScopeBadge entry={mainEntry} />
                    </ScopeRow>
                  </div>
                ) : null}
                {showProjects ? (
                  <div className="mt-4 space-y-0.5">
                    <ScopeSectionHeader kicker="Projects" title="Project Agent bindings" />
                    {matchingProjects.map((project) => (
                      <ScopeRow
                        key={project.id}
                        icon={<Folder size={16} weight="duotone" aria-hidden />}
                        title={project.name}
                        subtitle="Project-owned timeline"
                        selected={effectiveScope === project.id}
                        onSelect={() => setSelectedScope(project.id)}
                      >
                        <AgentScopeBadge entry={projectChatEntry(chatEntries, project.id)} />
                      </ScopeRow>
                    ))}
                    {matchingProjects.length === 0 ? (
                      <p className="px-3 py-3 text-xs text-muted-foreground">
                        No projects exist yet.
                      </p>
                    ) : null}
                  </div>
                ) : null}
                {!showGlobal && !showProjects ? (
                  <p className="px-2 py-6 text-center text-xs text-muted-foreground">
                    No matching scopes
                  </p>
                ) : null}
              </>
            ) : null}
          </div>
        </div>
        <div className="flex min-w-0 flex-1 flex-col overflow-hidden">
          {effectiveScope === MAIN_SCOPE ? (
            <MainScopeDetail agents={agents} chatEntry={mainEntry} onConnect={onConnect} />
          ) : selectedProject ? (
            <ProjectScopeDetail
              project={selectedProject}
              chatEntry={projectChatEntry(chatEntries, selectedProject.id)}
            />
          ) : null}
        </div>
      </div>

      <BoundAgentScopes
        entries={chatEntries}
        isLoading={chatsLoading}
        isError={chatsError}
        onRetry={onRetryChats}
      />
    </div>
  )
}
