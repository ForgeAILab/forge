import { useEffect, useMemo, useState } from 'react'
import { Link } from '@tanstack/react-router'
import { Check, Copy, Desktop, PencilSimple, Plus, X } from '@phosphor-icons/react'
import { toast } from 'sonner'
import {
  useAgentsQuery,
  useCreatePat,
  useDaemonsQuery,
  useSettingsQuery,
  useUpdateSettings,
} from '@/api/hooks'
import { ErrorBanner } from '@/components/error-banner'
import { McpInstallControls } from '@/components/mcp-install-controls'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Skeleton } from '@/components/ui/skeleton'
import { getApiErrorMessage } from '@/lib/api-error'
import { cn } from '@/lib/cn'
import { productTerm } from '@/lib/i18n'
import type { Agent, Daemon, ForgeSettingResponse } from '@/types/generated'

const EMPTY_DAEMONS: Daemon[] = []
const EMPTY_AGENTS: Agent[] = []

function formatDate(value?: string | null): string {
  if (!value) return '—'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  return date.toLocaleString()
}

function formatRelative(value?: string | null): string {
  if (!value) return '—'
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return value
  const diff = Date.now() - date.getTime()
  const seconds = Math.floor(diff / 1000)
  if (seconds < 5) return 'now'
  if (seconds < 60) return `${seconds}s ago`
  const minutes = Math.floor(seconds / 60)
  if (minutes < 60) return `${minutes}m ago`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ago`
  return date.toLocaleDateString()
}

type CliAvailability = 'authenticated' | 'installed' | 'available' | 'not_found' | string

const availabilityConfig: Record<CliAvailability, { dot: string; label: string }> = {
  authenticated: { dot: 'bg-orange-500', label: 'Authenticated' },
  installed: { dot: 'bg-violet-400', label: 'Installed' },
  available: { dot: 'bg-violet-400', label: 'Available' },
  not_found: { dot: 'bg-stone-400', label: 'Not found' },
}

function availabilityOf(availability: string) {
  return availabilityConfig[availability] ?? { dot: 'bg-zinc-400', label: availability }
}

function defaultServerUrl(): string {
  if (typeof window === 'undefined') return 'http://127.0.0.1:8080'
  return window.location.origin
}

function shellQuote(value: string): string {
  if (/^[A-Za-z0-9_/:.,@%+=~$-]+$/.test(value)) return value
  return `'${value.replaceAll("'", "'\\''")}'`
}

export function DaemonsPage({ selectedDaemonId }: { selectedDaemonId?: string }) {
  const daemonsQuery = useDaemonsQuery()
  const agentsQuery = useAgentsQuery()
  const [linkDialogOpen, setLinkDialogOpen] = useState(false)

  const daemons = daemonsQuery.data?.items ?? EMPTY_DAEMONS
  const agents = agentsQuery.data?.items ?? EMPTY_AGENTS

  const selectedDaemon = useMemo(
    () => daemons.find((d) => d.id === selectedDaemonId),
    [daemons, selectedDaemonId],
  )

  const agentsByDaemon = useMemo(() => {
    const map = new Map<string, Agent[]>()
    for (const agent of agents) {
      if (!agent.daemon_id) continue
      const list = map.get(agent.daemon_id) ?? []
      list.push(agent)
      map.set(agent.daemon_id, list)
    }
    return map
  }, [agents])

  return (
    <div className="flex h-[calc(100vh-7rem)] gap-0 overflow-hidden rounded-xl border border-border-subtle bg-card shadow-card">
      {/* Left panel */}
      <div className="flex w-60 shrink-0 flex-col border-r border-border-subtle bg-background">
        <header className="flex shrink-0 items-center justify-between border-b px-4 py-3">
          <div>
            <p className="font-mono text-micro font-semibold uppercase tracking-[1px] text-muted-foreground">
              {productTerm('runtime', daemons.length)}
            </p>
            <p className="mt-0.5 text-[11px] text-muted-foreground">
              {daemons.filter((d) => d.status === 'online').length}/{daemons.length} online
            </p>
          </div>
          <Button
            variant="ghost"
            size="icon-sm"
            aria-label={`Link ${productTerm('runtime')}`}
            title={`Link ${productTerm('runtime')}`}
            onClick={() => setLinkDialogOpen(true)}
          >
            <Plus size={14} weight="bold" />
          </Button>
        </header>

        <div className="flex-1 overflow-y-auto">
          {daemonsQuery.isError ? (
            <div className="p-3">
              <ErrorBanner
                error={daemonsQuery.error}
                fallback={`Failed to load ${productTerm('runtime', 0).toLowerCase()}`}
                onRetry={() => void daemonsQuery.refetch()}
              />
            </div>
          ) : daemonsQuery.isLoading ? (
            <div className="space-y-1 p-2">
              {[0, 1, 2].map((i) => (
                <div key={i} className="rounded-lg p-3">
                  <Skeleton className="h-4 w-2/3" />
                  <Skeleton className="mt-2 h-3 w-full" />
                </div>
              ))}
            </div>
          ) : daemons.length === 0 ? (
            <div className="p-6 text-center">
              <div className="mx-auto mb-3 flex h-10 w-10 items-center justify-center rounded-full bg-muted">
                <Desktop size={20} className="text-muted-foreground" />
              </div>
              <p className="text-sm font-medium">No {productTerm('runtime', 0).toLowerCase()}</p>
              <p className="mt-1 text-xs text-muted-foreground">
                Start a {productTerm('runtime').toLowerCase()} to make it appear here
              </p>
            </div>
          ) : (
            <div className="space-y-0.5 p-1.5">
              {daemons.map((daemon) => {
                const isSelected = selectedDaemonId === daemon.id
                const online = daemon.status === 'online'
                const daemonAgents = agentsByDaemon.get(daemon.id) ?? []
                return (
                  <Link
                    key={daemon.id}
                    to="/daemons/$daemonId"
                    params={{ daemonId: daemon.id }}
                    className={cn(
                      'relative flex w-full cursor-pointer items-start gap-3 rounded-lg px-3 py-2.5 text-left transition-colors',
                      isSelected
                        ? 'border border-primary/20 bg-primary/8 text-foreground before:absolute before:left-0 before:top-1/2 before:-translate-y-1/2 before:h-4 before:w-[3px] before:rounded-r-full before:bg-primary'
                        : 'border border-transparent text-foreground hover:bg-accent/50',
                    )}
                  >
                    <div className="relative mt-0.5 shrink-0">
                      <Desktop size={18} className="text-muted-foreground" />
                      <span
                        className={cn(
                          'absolute -bottom-0.5 -right-0.5 h-2 w-2 rounded-full border border-card',
                          online ? 'bg-orange-500' : 'bg-stone-400',
                        )}
                      />
                    </div>
                    <div className="min-w-0 flex-1">
                      <p className="truncate text-sm font-medium">
                        {daemon.hostname || daemon.machine_id}
                      </p>
                      <div className="mt-0.5 flex items-center gap-1.5 text-xs text-muted-foreground">
                        <span className="truncate">
                          {daemon.os} · {daemon.arch}
                        </span>
                      </div>
                      <div className="mt-0.5 flex items-center gap-1.5 text-xs text-muted-foreground">
                        <span>
                          {daemonAgents.length} agent{daemonAgents.length !== 1 ? 's' : ''}
                        </span>
                        {daemon.last_report_at && (
                          <>
                            <span>·</span>
                            <span>{formatRelative(daemon.last_report_at)}</span>
                          </>
                        )}
                      </div>
                    </div>
                  </Link>
                )
              })}
            </div>
          )}
        </div>
      </div>

      {/* Right panel */}
      <div className="flex flex-1 flex-col overflow-hidden">
        {selectedDaemon ? (
          <DaemonDetail
            daemon={selectedDaemon}
            agents={agentsByDaemon.get(selectedDaemon.id) ?? []}
          />
        ) : (
          <EmptyPanel />
        )}
      </div>
      <DaemonLinkDialog open={linkDialogOpen} onOpenChange={setLinkDialogOpen} />
    </div>
  )
}

function DaemonLinkDialog({
  open,
  onOpenChange,
}: {
  open: boolean
  onOpenChange: (open: boolean) => void
}) {
  const [serverUrl, setServerUrl] = useState(defaultServerUrl)
  const [workspaceRoot, setWorkspaceRoot] = useState('$HOME/.forge/workspaces')
  const [linkToken, setLinkToken] = useState<string | null>(null)
  const [tokenError, setTokenError] = useState('')
  const [copied, setCopied] = useState(false)
  const createPat = useCreatePat()

  useEffect(() => {
    if (!open) return
    setServerUrl(defaultServerUrl())
    setLinkToken(null)
    setTokenError('')
    setCopied(false)
  }, [open])

  const command = useMemo(() => {
    if (!linkToken) return ''
    const server = serverUrl.trim() || defaultServerUrl()
    const root = workspaceRoot.trim() || '$HOME/.forge/workspaces'
    return `forge-ctl --server ${shellQuote(server)} daemon link --token ${shellQuote(linkToken)} --workspace-root ${shellQuote(root)}`
  }, [serverUrl, workspaceRoot, linkToken])

  function handleCopy() {
    if (!command) return
    void navigator.clipboard.writeText(command).then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), 1600)
    })
  }

  async function handleCreateToken() {
    setTokenError('')
    setCopied(false)
    try {
      const result = await createPat.mutateAsync({
        name: `${productTerm('runtime')} link ${new Date().toLocaleString()}`,
        expires_at: null,
      })
      if (!result.token) {
        throw new Error('Token was not returned')
      }
      setLinkToken(result.token)
      toast.success(`${productTerm('runtime')} link token created`)
    } catch (error) {
      const message = getApiErrorMessage(
        error,
        `Failed to create ${productTerm('runtime').toLowerCase()} link token`,
      )
      setTokenError(message)
      toast.error(message)
    }
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-xl">
        <DialogHeader>
          <DialogTitle>Link {productTerm('runtime').toLowerCase()}</DialogTitle>
          <DialogDescription>
            Create a personal link token, then run the command on the machine that should report
            local CLIs, browse paths, and run agent work for Forge.
          </DialogDescription>
        </DialogHeader>

        <div className="mt-5 space-y-4">
          <div className="grid gap-3 sm:grid-cols-2">
            <div className="space-y-1.5">
              <Label htmlFor="daemon-link-server">Server URL</Label>
              <Input
                id="daemon-link-server"
                value={serverUrl}
                onChange={(event) => setServerUrl(event.target.value)}
              />
            </div>
            <div className="space-y-1.5">
              <Label htmlFor="daemon-link-workspace">Workspace root</Label>
              <Input
                id="daemon-link-workspace"
                value={workspaceRoot}
                onChange={(event) => setWorkspaceRoot(event.target.value)}
              />
            </div>
          </div>

          <div className="rounded-lg border bg-muted/40">
            <div className="flex items-center justify-between border-b px-3 py-2">
              <span className="font-mono text-[11px] uppercase tracking-[0.8px] text-muted-foreground">
                Command
              </span>
              <button
                type="button"
                className="rounded p-1 text-muted-foreground transition-colors hover:text-foreground disabled:cursor-not-allowed disabled:opacity-50 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                aria-label={`Copy ${productTerm('runtime').toLowerCase()} link command`}
                onClick={handleCopy}
                disabled={!command}
              >
                {copied ? <Check size={14} /> : <Copy size={14} />}
              </button>
            </div>
            <pre className="max-h-40 overflow-x-auto whitespace-pre-wrap break-all p-3 font-mono text-xs text-foreground">
              {command || 'Create a link token to generate the command.'}
            </pre>
          </div>
          {tokenError ? <p className="text-xs text-destructive">{tokenError}</p> : null}
        </div>

        <DialogFooter className="mt-5">
          {!linkToken ? (
            <Button
              onClick={() => {
                void handleCreateToken()
              }}
              disabled={createPat.isPending}
            >
              {createPat.isPending ? 'Creating...' : 'Create token'}
            </Button>
          ) : null}
          <Button variant="outline" onClick={() => onOpenChange(false)}>
            Close
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function RestartBadge({ setting }: { setting: ForgeSettingResponse | undefined }) {
  if (!setting?.restart_required) return null
  return (
    <span className="ml-1.5 inline-flex items-center rounded-full bg-amber-100 px-1.5 py-0.5 font-mono text-[10px] font-medium text-amber-700 dark:bg-amber-900/30 dark:text-amber-400">
      restart required
    </span>
  )
}

function EmptyPanel() {
  return (
    <div className="flex flex-1 items-center justify-center">
      <div className="text-center">
        <div className="mx-auto mb-4 flex h-12 w-12 items-center justify-center rounded-full bg-muted">
          <Desktop size={24} className="text-muted-foreground" />
        </div>
        <p className="text-sm font-medium">Select a {productTerm('runtime').toLowerCase()}</p>
        <p className="mt-1 text-xs text-muted-foreground">
          Choose a {productTerm('runtime').toLowerCase()} from the list to view its details
        </p>
      </div>
    </div>
  )
}

function WorkspaceSection() {
  const settingsQuery = useSettingsQuery()
  const updateSettings = useUpdateSettings()
  const [editing, setEditing] = useState(false)
  const [root, setRoot] = useState('')
  const [cleanupDelay, setCleanupDelay] = useState('')

  const settings = settingsQuery.data?.settings ?? []
  const getSetting = (key: string) => settings.find((s) => s.key === key)

  useEffect(() => {
    if (!settings.length) return
    const get = (key: string) => settings.find((s) => s.key === key)?.value
    setRoot(String(get('workspace.root') ?? ''))
    setCleanupDelay(String(get('workspace.cleanup_delay_seconds') ?? ''))
  }, [settings]) // eslint-disable-line react-hooks/exhaustive-deps

  function handleCancel() {
    const get = (key: string) => settings.find((s) => s.key === key)?.value
    setRoot(String(get('workspace.root') ?? ''))
    setCleanupDelay(String(get('workspace.cleanup_delay_seconds') ?? ''))
    setEditing(false)
  }

  function handleSave() {
    const delay = Number(cleanupDelay)
    if (cleanupDelay && (!Number.isInteger(delay) || delay < 1)) {
      toast.error('Cleanup delay must be a positive integer')
      return
    }
    updateSettings.mutate(
      {
        workspace: {
          root: root.trim() || null,
          cleanup_delay_seconds: cleanupDelay ? delay : null,
        },
      },
      {
        onSuccess: () => {
          toast.success('Workspace settings saved')
          setEditing(false)
        },
        onError: (err) => toast.error(getApiErrorMessage(err, 'Failed to save workspace settings')),
      },
    )
  }

  const rootSetting = getSetting('workspace.root')
  const cleanupDelaySetting = getSetting('workspace.cleanup_delay_seconds')

  const displayRoot = root || '(default)'
  const displayDelay = cleanupDelay ? `${cleanupDelay} sec` : '(default)'

  return (
    <section>
      <div className="mb-3 flex items-center justify-between">
        <h3 className="font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
          Workspace
        </h3>
        {editing ? (
          <div className="flex items-center gap-1.5">
            <button
              type="button"
              onClick={handleCancel}
              className="flex cursor-pointer items-center gap-1 rounded px-2 py-1 text-xs text-muted-foreground transition-colors hover:bg-accent hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
            >
              <X size={11} />
              Cancel
            </button>
            <Button
              size="sm"
              className="h-6 px-2 text-xs"
              disabled={updateSettings.isPending}
              onClick={handleSave}
            >
              {updateSettings.isPending ? 'Saving…' : 'Save'}
            </Button>
          </div>
        ) : (
          <button
            type="button"
            onClick={() => setEditing(true)}
            className="flex cursor-pointer items-center gap-1 rounded px-2 py-1 text-xs text-muted-foreground transition-colors hover:bg-accent hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          >
            <PencilSimple size={11} />
            Edit
          </button>
        )}
      </div>

      {settingsQuery.isLoading ? (
        <div className="overflow-hidden rounded-lg border">
          <div className="flex items-center justify-between px-4 py-2.5">
            <Skeleton className="h-3 w-28" />
            <Skeleton className="h-5 w-32" />
          </div>
          <div className="flex items-center justify-between border-t px-4 py-2.5">
            <Skeleton className="h-3 w-36" />
            <Skeleton className="h-5 w-16" />
          </div>
        </div>
      ) : (
        <div className="overflow-hidden rounded-lg border">
          {/* workspace.root row */}
          <div
            className={cn('flex items-center justify-between px-4', editing ? 'py-2' : 'py-2.5')}
          >
            <div className="flex items-center gap-1.5">
              <span className="font-mono text-[11px] text-muted-foreground">workspace.root</span>
              {editing && <RestartBadge setting={rootSetting} />}
            </div>
            {editing ? (
              <Input
                id="ws-root"
                aria-label="Workspace root directory"
                className="h-7 w-56 font-mono text-xs"
                placeholder="/path/to/workspaces"
                value={root}
                onChange={(e) => setRoot(e.target.value)}
              />
            ) : (
              <code className="rounded bg-muted px-2 py-0.5 font-mono text-[11px] text-foreground">
                {displayRoot}
              </code>
            )}
          </div>

          {/* cleanup_delay_seconds row */}
          <div
            className={cn(
              'flex items-center justify-between border-t px-4',
              editing ? 'py-2' : 'py-2.5',
            )}
          >
            <div className="flex items-center gap-1.5">
              <span className="font-mono text-[11px] text-muted-foreground">
                cleanup_delay_seconds
              </span>
              {editing && <RestartBadge setting={cleanupDelaySetting} />}
            </div>
            {editing ? (
              <div className="flex items-center gap-1.5">
                <Input
                  id="ws-cleanup"
                  aria-label="Workspace cleanup delay in seconds"
                  type="number"
                  min={1}
                  className="h-7 w-20 font-mono text-xs"
                  value={cleanupDelay}
                  onChange={(e) => setCleanupDelay(e.target.value)}
                />
                <span className="font-mono text-[11px] text-muted-foreground">sec</span>
              </div>
            ) : (
              <code className="rounded bg-muted px-2 py-0.5 font-mono text-[11px] text-foreground">
                {displayDelay}
              </code>
            )}
          </div>
        </div>
      )}
    </section>
  )
}

function DaemonDetail({ daemon, agents }: { daemon: Daemon; agents: Agent[] }) {
  const online = daemon.status === 'online'
  const labels = daemon.labels ?? {}
  const hasLabels = Object.keys(labels).length > 0

  const configRows: Array<{ key: string; value: string }> = [
    { key: 'machine_id', value: daemon.machine_id },
    { key: 'hostname', value: daemon.hostname ?? '—' },
    { key: 'os', value: daemon.os },
    { key: 'arch', value: daemon.arch },
    ...(daemon.agent_version ? [{ key: 'agent_version', value: daemon.agent_version }] : []),
  ]

  return (
    <div className="flex flex-1 flex-col overflow-y-auto">
      <header className="flex shrink-0 items-center gap-4 border-b px-6 py-4">
        <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-lg bg-muted">
          <Desktop size={20} className="text-muted-foreground" />
        </div>
        <div className="min-w-0 flex-1">
          <h2 className="truncate text-lg font-semibold">{daemon.hostname || daemon.machine_id}</h2>
          <p className="truncate font-mono text-[12px] text-muted-foreground">
            {daemon.machine_id}
          </p>
        </div>
        <span
          className={cn(
            'rounded-full px-3 py-1 text-[12px] font-medium',
            online ? 'bg-orange-500/12 text-orange-400' : 'bg-stone-500/15 text-stone-400',
          )}
        >
          {online ? 'Online' : 'Offline'}
        </span>
      </header>

      <div className="flex-1 space-y-6 px-6 py-5">
        {/* Stats grid */}
        <div className="grid grid-cols-4 gap-2.5">
          {[
            { label: 'Status', value: online ? 'Online' : 'Offline' },
            { label: 'Agents', value: agents.length },
            { label: 'CLIs', value: daemon.detected_clis.length },
            { label: 'Last seen', value: formatRelative(daemon.last_report_at) },
          ].map((stat) => (
            <div key={stat.label} className="rounded-lg border bg-muted/40 px-3.5 py-3">
              <p className="mb-2 font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
                {stat.label}
              </p>
              <p className="font-mono text-xl font-semibold tabular-nums text-foreground">
                {stat.value}
              </p>
            </div>
          ))}
        </div>

        {/* System */}
        <section>
          <h3 className="mb-3 font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
            System
          </h3>
          <div className="overflow-hidden rounded-lg border">
            {configRows.map((row, i) => (
              <div
                key={row.key}
                className={cn('flex items-center justify-between px-4 py-2.5', i > 0 && 'border-t')}
              >
                <span className="font-mono text-[11px] text-muted-foreground">{row.key}</span>
                <code className="rounded bg-muted px-2 py-0.5 font-mono text-[11px] text-foreground">
                  {row.value}
                </code>
              </div>
            ))}
          </div>
        </section>

        {/* Workspace */}
        <WorkspaceSection />

        {/* Detected CLIs */}
        <section>
          <h3 className="mb-3 font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
            Detected CLIs
          </h3>
          {daemon.detected_clis.length === 0 ? (
            <p className="text-sm text-muted-foreground">No CLIs detected</p>
          ) : (
            <div className="overflow-hidden rounded-lg border">
              <table className="w-full text-sm">
                <thead>
                  <tr className="border-b bg-muted/40">
                    <th className="px-4 py-2 text-left font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
                      Kind
                    </th>
                    <th className="px-4 py-2 text-left font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
                      Status
                    </th>
                    <th className="px-4 py-2 text-left font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
                      Version
                    </th>
                    <th className="px-4 py-2 text-left font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
                      Path
                    </th>
                  </tr>
                </thead>
                <tbody className="divide-y">
                  {daemon.detected_clis.map((cli, i) => {
                    const avail = availabilityOf(cli.availability)
                    return (
                      <tr key={i} className="transition-colors hover:bg-muted/20">
                        <td className="px-4 py-2.5 font-medium">{cli.kind}</td>
                        <td className="px-4 py-2.5">
                          <div className="flex items-center gap-1.5">
                            <span className={cn('h-1.5 w-1.5 rounded-full', avail.dot)} />
                            <span className="text-xs text-muted-foreground">{avail.label}</span>
                          </div>
                        </td>
                        <td className="px-4 py-2.5 font-mono text-xs text-muted-foreground">
                          {cli.version ?? '—'}
                        </td>
                        <td className="max-w-xs px-4 py-2.5 font-mono text-xs text-muted-foreground">
                          <span className="block truncate" title={cli.path ?? undefined}>
                            {cli.path ?? '—'}
                          </span>
                        </td>
                      </tr>
                    )
                  })}
                </tbody>
              </table>
            </div>
          )}
        </section>

        {/* MCP */}
        <section>
          <h3 className="mb-3 font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
            MCP Servers
          </h3>
          <McpInstallControls scope="user" compact />
        </section>

        {/* Agents */}
        <section>
          <h3 className="mb-3 font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
            Agents ({agents.length})
          </h3>
          {agents.length === 0 ? (
            <p className="text-sm text-muted-foreground">
              No agents registered on this {productTerm('runtime').toLowerCase()}
            </p>
          ) : (
            <div className="flex flex-wrap gap-2">
              {agents.map((agent) => (
                <div
                  key={agent.id}
                  className="flex items-center gap-2 rounded-lg border bg-muted/40 px-3 py-2 text-sm"
                >
                  <span
                    className={cn(
                      'h-2 w-2 rounded-full',
                      agent.status === 'idle'
                        ? 'bg-orange-500'
                        : agent.status === 'busy'
                          ? 'bg-amber-400'
                          : agent.status === 'error'
                            ? 'bg-red-500'
                            : 'bg-stone-400',
                    )}
                  />
                  <span className="font-medium">{agent.name}</span>
                  <span className="font-mono text-xs text-muted-foreground">
                    {agent.running_execution_count ?? 0}/{agent.max_concurrent_tasks}
                  </span>
                </div>
              ))}
            </div>
          )}
        </section>

        {/* Labels */}
        {hasLabels && (
          <section>
            <h3 className="mb-3 font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
              Labels
            </h3>
            <div className="flex flex-wrap gap-1.5">
              {Object.entries(labels).map(([k, v]) => (
                <span
                  key={k}
                  className="rounded-md border bg-muted/40 px-2 py-0.5 font-mono text-xs"
                >
                  {k}={v}
                </span>
              ))}
            </div>
          </section>
        )}

        {/* Timestamps */}
        <section>
          <h3 className="mb-3 font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
            Timestamps
          </h3>
          <div className="overflow-hidden rounded-lg border">
            {[
              { key: 'registered', value: formatDate(daemon.created_at) },
              { key: 'updated', value: formatDate(daemon.updated_at) },
            ].map((row, i) => (
              <div
                key={row.key}
                className={cn('flex items-center justify-between px-4 py-2.5', i > 0 && 'border-t')}
              >
                <span className="font-mono text-[11px] text-muted-foreground">{row.key}</span>
                <code className="rounded bg-muted px-2 py-0.5 font-mono text-[11px] text-foreground">
                  {row.value}
                </code>
              </div>
            ))}
          </div>
        </section>
      </div>
    </div>
  )
}
