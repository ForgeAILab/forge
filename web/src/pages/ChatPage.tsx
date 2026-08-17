import { useEffect, useMemo } from 'react'
import { Link } from '@tanstack/react-router'
import { ArrowUpRight, ChatCircleDots } from '@phosphor-icons/react'
import { Avatar } from '@/components/ui/avatar'
import { ErrorPanel, LoadingPanel, StatusDot } from '@/features/federation/components'
import { AgentChatTimeline } from '@/components/chat/agent-chat-timeline'
import { ChatSetupRequired } from '@/components/chat/chat-setup-required'
import { ProjectAgentWorkbench } from '@/components/chat/project-agent-workbench'
import { ProductGenesisControls } from '@/features/product-genesis/ProductGenesisControls'
import {
  useProductGenesisActiveQuery,
  useStartProductGenesisMutation,
} from '@/features/product-genesis/hooks'
import {
  useAgentChatQuery,
  useAgentChatsQuery,
  useCancelAgentChatTurnMutation,
  useSendAgentChatMessageMutation,
} from '@/features/agent-chat/hooks'
import type { AgentChatEntry } from '@/features/agent-chat/types'
import { useChatSelection } from '@/stores/chat'
import { useProjectsInfiniteQuery } from '@/api/hooks'

const PROJECTS_PAGE_SIZE = 100

function projectNameFor(
  projectId: string | null | undefined,
  projects: Array<{ id: string; name: string }>,
): string {
  if (!projectId) return 'Project'
  return projects.find((project) => project.id === projectId)?.name ?? 'Project'
}

function isBindingReady(entry: AgentChatEntry | undefined): boolean {
  return Boolean(entry && entry.binding_state === 'active' && entry.chat_status === 'ready')
}

export function handoffProjectIdsForScope(
  projectId: string | undefined,
  entries: AgentChatEntry[],
): string[] {
  if (projectId) return [projectId]
  return entries.flatMap((entry) => (entry.project_id ? [entry.project_id] : []))
}

function ScopeAffordances({ projectId, ready }: { projectId?: string; ready?: boolean }) {
  const affordances = projectId
    ? ['Project decisions', 'Commitments', 'Task management', 'Delivery outcomes', 'Provenance']
    : ['Discovery', 'Web research', 'Bounded portfolio summaries', 'Project handoffs']

  return (
    <section
      aria-label={projectId ? 'Project Agent capabilities' : 'Main Agent capabilities'}
      className="mt-4 flex min-w-0 flex-wrap items-center gap-1.5"
    >
      <span className="mr-1 text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
        {projectId ? 'Project scope' : 'Main scope'}
      </span>
      {affordances.map((affordance) => (
        <span
          key={affordance}
          className="rounded-full border border-border-subtle bg-muted/40 px-2 py-1 text-micro text-muted-foreground"
        >
          {affordance === 'Web research'
            ? `${affordance} · ${ready === undefined ? 'checking' : ready ? 'policy-controlled' : 'setup required'}`
            : affordance}
        </span>
      ))}
    </section>
  )
}

const PROJECT_RECORD_LINKS = [
  { label: 'Milestones', section: 'milestones' },
  { label: 'Documents', section: 'documents' },
  { label: 'Decisions', section: 'decisions' },
  { label: 'Evidence', section: 'evidence' },
  { label: 'Readiness', section: 'readiness' },
  { label: 'Releases', section: 'releases' },
] as const

export function ProjectRecordNavigation({ projectId }: { projectId: string }) {
  const projectPath = `/projects/${encodeURIComponent(projectId)}`

  return (
    <nav
      aria-label="Project records"
      className="flex min-w-0 flex-wrap items-center justify-end gap-1.5"
    >
      <span className="mr-1 font-mono text-micro font-semibold uppercase tracking-[0.1em] text-muted-foreground">
        Project records
      </span>
      <a
        href={`${projectPath}/tasks?sort_by=updated_at&sort_order=desc`}
        className="rounded-full border border-border-subtle bg-muted/30 px-2.5 py-1 text-xs font-medium text-foreground transition-colors hover:border-primary/40 hover:bg-ember-surface hover:text-primary focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
      >
        Tasks
      </a>
      {PROJECT_RECORD_LINKS.map(({ label, section }) => (
        <a
          key={section}
          href={`${projectPath}/overview#${section}`}
          className="rounded-full border border-border-subtle bg-muted/30 px-2.5 py-1 text-xs font-medium text-foreground transition-colors hover:border-primary/40 hover:bg-ember-surface hover:text-primary focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
        >
          {label}
        </a>
      ))}
    </nav>
  )
}

export function ChatPage({ projectId }: { projectId?: string }) {
  const chatsQuery = useAgentChatsQuery()
  const projectsQuery = useProjectsInfiniteQuery(PROJECTS_PAGE_SIZE)
  const setGlobalChat = useChatSelection((state) => state.setGlobalChat)
  const setProjectChat = useChatSelection((state) => state.setProjectChat)
  const entries = chatsQuery.data?.items ?? []
  const projects = useMemo(
    () => projectsQuery.data?.pages.flatMap((page) => page.items) ?? [],
    [projectsQuery.data],
  )
  const globalSource = entries.find((entry) => entry.kind === 'main')
  const projectSources = entries.filter((entry) => entry.kind === 'project')
  const activeSource = projectId
    ? projectSources.find((entry) => entry.project_id === projectId)
    : globalSource
  const activeChatId = activeSource?.chat_id
  const chatQuery = useAgentChatQuery(activeChatId)
  const sendMutation = useSendAgentChatMessageMutation(chatQuery.data?.id)
  const cancelMutation = useCancelAgentChatTurnMutation(chatQuery.data?.id)
  const activeGenesisQuery = useProductGenesisActiveQuery()
  const startGenesisMutation = useStartProductGenesisMutation()
  const hasActiveGenesis = Boolean(activeGenesisQuery.data?.session)
  const activeAgentName = activeSource?.identity_name ?? undefined
  const activeProjectName = projectNameFor(projectId, projects)
  const activeReady = Boolean(
    chatQuery.data &&
    chatQuery.data.status === 'ready' &&
    activeSource &&
    isBindingReady(activeSource),
  )
  const activeStatusKnown = Boolean(activeSource || chatQuery.data)
  const chatNeedsSetup = Boolean(
    activeSource?.binding_state === 'setup_required' || chatQuery.data?.status === 'setup_required',
  )
  const chatUnavailable = Boolean(activeSource && !chatNeedsSetup && !isBindingReady(activeSource))
  const activeState =
    chatsQuery.isLoading || (activeChatId !== undefined && chatQuery.isLoading)
      ? 'loading'
      : chatsQuery.isError || chatQuery.isError || chatUnavailable
        ? 'unavailable'
        : chatNeedsSetup || !activeSource
          ? 'setup_required'
          : 'ready'

  useEffect(() => {
    if (!chatQuery.data) return
    if (projectId) setProjectChat(projectId, chatQuery.data)
    else setGlobalChat(chatQuery.data)
  }, [chatQuery.data, projectId, setGlobalChat, setProjectChat])

  async function sendMessage(content: string) {
    if (!chatQuery.data) throw new Error('This Agent Chat is not ready yet.')
    const admitted = await sendMutation.mutateAsync({ content, dedupe_key: null })
    if (admitted.turn_job) {
      useChatSelection.getState().setPendingTurns(chatQuery.data.id, [admitted.turn_job])
    }
  }

  async function cancelTurn(turnId: string, expectedVersion: number) {
    await cancelMutation.mutateAsync({
      turnId,
      input: {
        expected_version: expectedVersion,
        idempotency_key: `agent-chat-turn-cancel:${turnId}:${expectedVersion}`,
      },
    })
  }

  const pageTitle = projectId ? `${activeProjectName} Agent Workspace` : 'Main Chat'
  const pageDescription = projectId
    ? 'Shape Project records and Tasks beside this Project’s durable Agent timeline. Repository work remains Task-scoped.'
    : 'The account’s single Main Agent timeline for discovery, organization, and explicit handoff.'

  return (
    <div className="flex min-h-0 min-h-full flex-col overflow-hidden rounded-xl border border-border-subtle bg-background shadow-xs">
      <header className="flex shrink-0 flex-wrap items-start justify-between gap-4 border-b border-border-subtle px-4 py-4 sm:px-5">
        <div className="min-w-0">
          <p className="font-mono text-micro uppercase tracking-[0.18em] text-primary">
            {projectId ? 'Project Agent' : 'Account Agent'}
          </p>
          <h1 className="mt-1 truncate text-xl font-semibold tracking-tight text-foreground sm:text-2xl">
            {pageTitle}
          </h1>
          <p className="mt-1 max-w-2xl text-sm leading-6 text-muted-foreground">
            {pageDescription}
          </p>
          <ScopeAffordances
            projectId={projectId}
            ready={activeStatusKnown ? activeReady : undefined}
          />
        </div>
        <div className="flex shrink-0 items-center gap-2 rounded-lg border border-border-subtle bg-muted/20 px-3 py-2">
          <ChatCircleDots size={15} className="text-primary" aria-hidden />
          <span className="text-xs text-muted-foreground">One durable timeline</span>
        </div>
        {!projectId ? <ProductGenesisControls /> : null}
      </header>
      <div className="flex min-h-0 flex-1 flex-col">
        <ProjectAgentWorkbench projectId={projectId}>
        <section
          className="flex min-h-0 min-w-0 flex-1 flex-col"
          aria-label={`${pageTitle} timeline`}
        >
          <header className="flex shrink-0 flex-wrap items-center justify-between gap-3 border-b border-border-subtle px-4 py-3 sm:px-5">
            <div className="flex min-w-0 items-center gap-3">
              <Avatar
                name={activeAgentName ?? (projectId ? activeProjectName : 'Main')}
                seed={activeChatId ?? projectId ?? 'main'}
                size="sm"
              />
              <div className="min-w-0">
                <p className="truncate text-sm font-semibold text-foreground">
                  {activeAgentName ?? (projectId ? 'Project Agent' : 'Main Agent')}
                </p>
                <p className="flex items-center gap-1.5 text-xs text-muted-foreground">
                  <StatusDot status={activeState} />
                  {activeState === 'ready'
                    ? 'Ready for one finite turn'
                    : activeState === 'loading'
                      ? 'Loading authorized chat'
                      : activeState === 'unavailable'
                        ? 'Unavailable'
                        : 'Setup required'}
                </p>
              </div>
            </div>
            <div className="flex min-w-0 flex-wrap items-center justify-end gap-3">
              {projectId ? (
                <>
                  <ProjectRecordNavigation projectId={projectId} />
                  <Link
                    to="/agents"
                    search={{ project: projectId }}
                    className="inline-flex items-center gap-1 text-xs font-medium text-primary underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  >
                    Agent settings
                    <ArrowUpRight size={13} aria-hidden />
                  </Link>
                </>
              ) : null}
            </div>
          </header>
          {chatsQuery.isLoading || (activeChatId && chatQuery.isLoading) ? (
            <LoadingPanel label="Loading Agent Chat" />
          ) : chatsQuery.isError ? (
            <ErrorPanel
              title="Agent Chat unavailable"
              description="Forge could not load the authorized chat switcher. No chat is created or forked while it is unavailable."
              onRetry={() => void chatsQuery.refetch()}
            />
          ) : chatQuery.isError ? (
            <ErrorPanel
              title="Chat details unavailable"
              description="Forge could not load this existing Agent Chat. Try again before admitting a turn."
              onRetry={() => void chatQuery.refetch()}
            />
          ) : chatUnavailable ? (
            <div className="flex min-h-0 flex-1 items-center justify-center overflow-y-auto p-4 sm:p-6">
              <section
                className="mx-auto flex w-full max-w-xl flex-col items-start rounded-xl border border-destructive/30 bg-destructive/5 p-5 sm:p-6"
                role="alert"
                aria-labelledby="chat-unavailable-heading"
              >
                <h2
                  id="chat-unavailable-heading"
                  className="text-base font-semibold text-foreground"
                >
                  Agent Chat unavailable
                </h2>
                <p className="mt-2 text-sm leading-6 text-muted-foreground">
                  This durable chat exists, but its owning Agent binding is currently paused,
                  replaced, revoked, or archived. Forge will not admit a turn until the authorized
                  binding is restored.
                </p>
                {projectId ? (
                  <Link
                    to="/agents"
                    search={{ project: projectId }}
                    className="mt-5 inline-flex items-center gap-1.5 text-xs font-medium text-primary underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  >
                    Open Agent settings <ArrowUpRight size={13} aria-hidden />
                  </Link>
                ) : (
                  <Link
                    to="/agents"
                    search={{ tab: 'bindings' }}
                    className="mt-5 inline-flex items-center gap-1.5 text-xs font-medium text-primary underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  >
                    Open Agent settings <ArrowUpRight size={13} aria-hidden />
                  </Link>
                )}
              </section>
            </div>
          ) : chatNeedsSetup ? (
            <div className="flex min-h-0 flex-1 items-center justify-center overflow-y-auto p-4 sm:p-6">
              <ChatSetupRequired projectId={projectId} />
            </div>
          ) : chatQuery.data ? (
            <AgentChatTimeline
              chat={chatQuery.data}
              agentName={activeAgentName}
              projectId={projectId}
              handoffProjectIds={handoffProjectIdsForScope(projectId, projectSources)}
              isSending={sendMutation.isPending}
              onSend={sendMessage}
              onCancelTurn={cancelTurn}
              onStartGenesis={
                !projectId && !hasActiveGenesis
                  ? async (initialIdea) => {
                      await startGenesisMutation.mutateAsync({
                        initial_idea: initialIdea || null,
                        maturity: 'mvp',
                        preferred_project_agent_identity_id: null,
                      })
                    }
                  : undefined
              }
              isStartingGenesis={startGenesisMutation.isPending}
            />
          ) : (
            <div className="flex min-h-0 flex-1 items-center justify-center overflow-y-auto p-4 sm:p-6">
              <ChatSetupRequired projectId={projectId} />
            </div>
          )}
        </section>
        </ProjectAgentWorkbench>
      </div>
    </div>
  )
}
