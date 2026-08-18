import { useCallback, useEffect, useMemo } from 'react'
import { Link } from '@tanstack/react-router'
import { ArrowUpRight, GearSix } from '@phosphor-icons/react'
import { Avatar } from '@/components/ui/avatar'
import { ErrorPanel, LoadingPanel, StatusDot } from '@/features/federation/components'
import { AgentChatTimeline, type ChatCommand } from '@/components/chat/agent-chat-timeline'
import { ChatSetupRequired } from '@/components/chat/chat-setup-required'
import {
  ProductGenesisCharterCard,
  ProductGenesisControls,
} from '@/features/product-genesis/ProductGenesisControls'
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

  const startGenesis = useCallback(
    async (idea: string) => {
      await startGenesisMutation.mutateAsync({
        initial_idea: idea || null,
        maturity: 'mvp',
        preferred_project_agent_identity_id: null,
      })
    },
    [startGenesisMutation],
  )
  const commands = useMemo<ChatCommand[]>(
    () =>
      !projectId && !hasActiveGenesis
        ? [
            {
              name: 'start-product',
              description: 'Start Product Genesis — draft a Charter from your idea',
              run: startGenesis,
            },
          ]
        : [],
    [projectId, hasActiveGenesis, startGenesis],
  )

  async function cancelTurn(turnId: string, expectedVersion: number) {
    await cancelMutation.mutateAsync({
      turnId,
      input: {
        expected_version: expectedVersion,
        idempotency_key: `agent-chat-turn-cancel:${turnId}:${expectedVersion}`,
      },
    })
  }

  return (
    <div className="flex h-full min-h-0 flex-col overflow-hidden rounded-xl border border-border-subtle bg-background shadow-xs">
      <header className="flex shrink-0 flex-wrap items-center justify-between gap-3 border-b border-border-subtle px-4 py-3 sm:px-5">
        <div className="flex min-w-0 items-center gap-3">
          <Avatar
            name={activeAgentName ?? (projectId ? activeProjectName : 'Main')}
            seed={activeChatId ?? projectId ?? 'main'}
            size="sm"
          />
          <div className="min-w-0">
            <h1 className="truncate text-sm font-semibold text-foreground">
              {activeAgentName ?? (projectId ? 'Project Agent' : 'Main Agent')}
            </h1>
            <p className="flex items-center gap-1.5 text-xs text-muted-foreground">
              <StatusDot status={activeState} />
              <span className="truncate">
                {projectId ? activeProjectName : 'Main Chat'}
                {activeState === 'ready'
                  ? ''
                  : activeState === 'loading'
                    ? ' · Loading'
                    : activeState === 'unavailable'
                      ? ' · Unavailable'
                      : ' · Setup required'}
              </span>
            </p>
          </div>
        </div>
        <div className="flex shrink-0 items-center gap-2">
          {!projectId ? <ProductGenesisControls /> : null}
          {projectId ? (
            <Link
              to="/projects/$projectId/settings"
              params={{ projectId }}
              className="inline-flex items-center gap-1.5 rounded-lg border border-border-subtle px-2.5 py-1.5 text-xs font-medium text-muted-foreground transition-colors hover:bg-muted/40 hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              <GearSix size={13} aria-hidden />
              Project settings
            </Link>
          ) : null}
        </div>
      </header>
      <div className="flex min-h-0 flex-1 flex-col">
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
              <h2 id="chat-unavailable-heading" className="text-base font-semibold text-foreground">
                Agent Chat unavailable
              </h2>
              <p className="mt-2 text-sm leading-6 text-muted-foreground">
                This durable chat exists, but its owning Agent binding is currently paused,
                replaced, revoked, or archived. Forge will not admit a turn until the authorized
                binding is restored.
              </p>
              <Link
                to="/agents"
                search={projectId ? { project: projectId } : { tab: 'bindings' }}
                className="mt-5 inline-flex items-center gap-1.5 text-xs font-medium text-primary underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
              >
                Open Agent settings <ArrowUpRight size={13} aria-hidden />
              </Link>
            </section>
          </div>
        ) : chatNeedsSetup || !chatQuery.data ? (
          <div className="flex min-h-0 flex-1 items-center justify-center overflow-y-auto p-4 sm:p-6">
            <ChatSetupRequired projectId={projectId} />
          </div>
        ) : (
          <AgentChatTimeline
            chat={chatQuery.data}
            agentName={activeAgentName}
            projectId={projectId}
            handoffProjectIds={handoffProjectIdsForScope(projectId, projectSources)}
            isSending={sendMutation.isPending}
            onSend={sendMessage}
            onCancelTurn={cancelTurn}
            commands={commands}
            footer={!projectId ? <ProductGenesisCharterCard /> : undefined}
          />
        )}
      </div>
    </div>
  )
}
