import { useState } from 'react'
import { Check, Copy } from '@phosphor-icons/react'
import { toast } from 'sonner'
import { useCliProjectionQuery, useMcpConfigQuery, useUpdateMcpConfig } from '@/api/hooks'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { cn } from '@/lib/cn'
import { getApiErrorMessage } from '@/lib/api-error'
import { useAuthStore } from '@/stores/auth'
import type { McpConfigActionRequest } from '@/types/generated'

type McpInstallScope = 'project' | 'local' | 'user'
type McpInstallAgent = 'claude' | 'cursor' | 'codex'

const MCP_CLIENTS: Record<
  McpInstallAgent,
  { label: string; configPath: string; cliKind?: string }
> = {
  claude: {
    label: 'Claude Code',
    configPath: '.claude/settings.json',
    cliKind: 'claude_code',
  },
  codex: { label: 'Codex CLI', configPath: '.codex/config.toml', cliKind: 'codex' },
  cursor: { label: 'Cursor', configPath: '.cursor/mcp.json' },
}

const CLIENT_ORDER: McpInstallAgent[] = ['claude', 'codex', 'cursor']

function availableMcpClients(cliKinds: Set<string>): McpInstallAgent[] {
  const clients = CLIENT_ORDER.filter((agent) => {
    const cliKind = MCP_CLIENTS[agent].cliKind
    return !cliKind || cliKinds.has(cliKind)
  })
  return clients.length > 0 ? clients : CLIENT_ORDER
}

function stripTokenQueryParam(url: string): string {
  try {
    const isAbsolute = /^[a-zA-Z][a-zA-Z\d+\-.]*:/.test(url)
    const parsed = new URL(url, window.location.origin)
    parsed.searchParams.delete('token')
    return isAbsolute ? parsed.toString() : `${parsed.pathname}${parsed.search}${parsed.hash}`
  } catch {
    return url
  }
}

function getCliCommand(agent: McpInstallAgent, mcpUrl: string): string {
  switch (agent) {
    case 'claude':
      return `claude mcp add forge "${mcpUrl}"`
    case 'codex':
      return `[mcp_servers.forge]\nurl = "${mcpUrl}"`
    case 'cursor':
      return JSON.stringify({ mcpServers: { forge: { type: 'http', url: mcpUrl } } }, null, 2)
  }
}

// The address this browser reaches Forge on; the server derives the same URL
// when no public base URL is configured.
function browserMcpUrl(scope: McpInstallScope, projectId?: string): string {
  const base = `${window.location.origin}/mcp`
  return scope === 'project' && projectId
    ? `${base}?project_id=${encodeURIComponent(projectId)}`
    : base
}

function getCommandLabel(agent: McpInstallAgent): string {
  switch (agent) {
    case 'claude':
      return 'CLI command'
    case 'codex':
      return 'Add to .codex/config.toml'
    case 'cursor':
      return 'Add to .cursor/mcp.json'
  }
}

function CopyButton({ text, className }: { text: string; className?: string }) {
  const [copied, setCopied] = useState(false)

  const handleCopy = () => {
    void navigator.clipboard.writeText(text)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  return (
    <button
      type="button"
      onClick={handleCopy}
      className={cn(
        'shrink-0 rounded p-1 text-muted-foreground transition-colors hover:text-foreground cursor-pointer',
        className,
      )}
      aria-label="Copy to clipboard"
    >
      {copied ? <Check size={14} /> : <Copy size={14} />}
    </button>
  )
}

export function McpInstallControls({
  scope,
  projectId,
  compact = false,
}: {
  scope: McpInstallScope
  projectId?: string
  compact?: boolean
}) {
  const clisQuery = useCliProjectionQuery()
  const cliKinds = new Set(
    (clisQuery.data?.items ?? [])
      .filter((item) => item.availability !== 'missing')
      .map((item) => item.kind),
  )
  const supportedAgents = availableMcpClients(cliKinds)
  // Server-side MCP configuration is administrator-only: other users get the
  // URL to configure their own client, without reading the server's files.
  const isAdmin = useAuthStore((state) => state.user?.is_admin === true)

  const claudeQuery = useMcpConfigQuery('claude', scope, projectId, isAdmin)
  const cursorQuery = useMcpConfigQuery('cursor', scope, projectId, isAdmin)
  const codexQuery = useMcpConfigQuery('codex', scope, projectId, isAdmin)
  const mutation = useUpdateMcpConfig()
  const [pendingAgent, setPendingAgent] = useState<McpInstallAgent | 'all' | null>(null)

  if (!isAdmin) {
    const manualUrl = browserMcpUrl(scope, projectId)
    return (
      <div className={cn('space-y-3', compact && 'mt-3')}>
        <div>
          <p className="mb-1.5 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
            MCP Server URL
          </p>
          <div className="flex items-center gap-2 rounded-md border bg-muted/40 px-3 py-2">
            <code className="min-w-0 flex-1 truncate font-mono text-sm text-foreground">
              {manualUrl}
            </code>
            <CopyButton text={manualUrl} />
          </div>
        </div>
        <p className="text-xs text-muted-foreground">
          Use this URL to configure your MCP client. Installing MCP configuration on the Forge
          server requires a Forge administrator.
        </p>
      </div>
    )
  }

  const queries = { claude: claudeQuery, cursor: cursorQuery, codex: codexQuery } as const

  const expectedUrl =
    claudeQuery.data?.expected_url ??
    cursorQuery.data?.expected_url ??
    codexQuery.data?.expected_url
  const installedUrl =
    CLIENT_ORDER.map((agent) => queries[agent].data).find((data) => data?.installed && data.url)
      ?.url ?? null
  const rawDisplayUrl = installedUrl ?? expectedUrl
  const displayUrl = rawDisplayUrl ? stripTokenQueryParam(rawDisplayUrl) : rawDisplayUrl
  const installableAgents = CLIENT_ORDER.filter(
    (agent) =>
      supportedAgents.includes(agent) &&
      queries[agent].data !== undefined &&
      !queries[agent].data?.installed,
  )

  const handleAction = (agent: McpInstallAgent, action: McpConfigActionRequest['action']) => {
    setPendingAgent(agent)
    mutation.mutate(
      { agent, scope, project_id: projectId, action },
      {
        onSuccess: () => {
          toast.success(action === 'install' ? 'MCP server installed' : 'MCP server removed')
          setPendingAgent(null)
        },
        onError: (error) => {
          toast.error(getApiErrorMessage(error, `Failed to ${action} MCP server`))
          setPendingAgent(null)
        },
      },
    )
  }

  const handleInstallMissing = async () => {
    if (installableAgents.length === 0) return
    setPendingAgent('all')
    try {
      for (const agent of installableAgents) {
        await mutation.mutateAsync({ agent, scope, project_id: projectId, action: 'install' })
      }
      toast.success('MCP servers installed')
    } catch (error) {
      toast.error(getApiErrorMessage(error, 'Failed to install MCP servers'))
    } finally {
      setPendingAgent(null)
    }
  }

  return (
    <div className={cn('space-y-5', compact && 'mt-3')}>
      {/* MCP Server URL */}
      <div>
        <p className="mb-1.5 text-xs font-semibold uppercase tracking-wide text-muted-foreground">
          MCP Server URL
        </p>
        <div className="flex items-center gap-2 rounded-md border bg-muted/40 px-3 py-2">
          {displayUrl !== undefined ? (
            <>
              <code className="min-w-0 flex-1 truncate font-mono text-sm text-foreground">
                {displayUrl}
              </code>
              <CopyButton text={displayUrl} />
            </>
          ) : (
            <Skeleton className="h-5 w-72" />
          )}
        </div>
        <p className="mt-1 text-xs text-muted-foreground">
          Use this URL to configure any MCP-compatible client manually.
        </p>
      </div>

      {/* Per-client rows */}
      {installableAgents.length > 1 && (
        <div className="mb-2 flex items-center justify-end">
          <Button
            size="sm"
            variant="outline"
            disabled={pendingAgent !== null || mutation.isPending}
            onClick={() => void handleInstallMissing()}
          >
            {pendingAgent === 'all' ? 'Installing…' : 'Install missing'}
          </Button>
        </div>
      )}
      <div className="divide-y rounded-md border" role="group" aria-label="MCP client">
        {CLIENT_ORDER.map((agent) => {
          const query = queries[agent]
          const isSupported = supportedAgents.includes(agent)
          const isRowPending = pendingAgent === agent
          const isBulkInstalling = pendingAgent === 'all' && installableAgents.includes(agent)
          const isDisabled = pendingAgent !== null || query.isLoading
          const rawAgentUrl =
            query.data?.installed && query.data.url ? query.data.url : (installedUrl ?? expectedUrl)
          const agentUrl = rawAgentUrl ? stripTokenQueryParam(rawAgentUrl) : rawAgentUrl
          const cliCommand = agentUrl ? getCliCommand(agent, agentUrl) : null
          const commandLabel = getCommandLabel(agent)

          return (
            <div key={agent} className={cn('px-4 py-4', compact && 'px-3 py-3')}>
              <div className="flex items-start justify-between gap-3">
                <div className="min-w-0 flex-1">
                  <div className="flex flex-wrap items-center gap-2">
                    <span className="text-sm font-medium">{MCP_CLIENTS[agent].label}</span>
                    {query.isLoading ? (
                      <Skeleton className="h-5 w-20" />
                    ) : query.data?.installed ? (
                      <Badge variant="default">Installed</Badge>
                    ) : (
                      <Badge variant="secondary">Not installed</Badge>
                    )}
                    {!isSupported && (
                      <Badge variant="outline" className="text-muted-foreground">
                        Not detected
                      </Badge>
                    )}
                  </div>
                  <p className="mt-0.5 break-all font-mono text-xs text-muted-foreground">
                    {query.data?.config_path ?? MCP_CLIENTS[agent].configPath}
                  </p>
                </div>
                <div className="shrink-0">
                  {query.data?.installed ? (
                    <Button
                      size="sm"
                      variant="outline"
                      disabled={isDisabled}
                      onClick={() => handleAction(agent, 'uninstall')}
                    >
                      {isRowPending ? 'Removing…' : 'Uninstall'}
                    </Button>
                  ) : (
                    <Button
                      size="sm"
                      disabled={isDisabled}
                      onClick={() => handleAction(agent, 'install')}
                    >
                      {isRowPending || isBulkInstalling ? 'Installing…' : 'Install'}
                    </Button>
                  )}
                </div>
              </div>

              {!compact && cliCommand && (
                <div className="mt-3">
                  <p className="mb-1 text-xs text-muted-foreground">{commandLabel}</p>
                  <div className="flex items-start gap-2 rounded-md bg-muted/60 px-3 py-2">
                    <code className="min-w-0 flex-1 whitespace-pre-wrap break-all font-mono text-xs text-foreground">
                      {cliCommand}
                    </code>
                    <CopyButton text={cliCommand} />
                  </div>
                </div>
              )}
            </div>
          )
        })}
      </div>
    </div>
  )
}
