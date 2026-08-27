import { useEffect, useMemo, useRef, useState } from 'react'
import { CaretRight, Key, MagnifyingGlass, Plus, Robot, ShieldCheck, TerminalWindow } from '@phosphor-icons/react'
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
import { Select } from '@/components/ui/select'
import { Textarea } from '@/components/ui/textarea'
import { ModelSelector } from '@/components/execution-config/ModelSelector'
import { PolicySelector } from '@/components/execution-config/PolicySelector'
import { ReasoningSelector } from '@/components/execution-config/ReasoningSelector'
import { getReasoningOptionsForModel, useDiscoveredOptions } from '@/hooks/useDiscoveredOptions'
import { cn } from '@/lib/cn'
import type { AgentChatEntry } from '@/features/agent-chat/types'
import {
  useAgentProviderCapabilitiesQuery,
  useCreateEmbeddedAgentMutation,
  useRegisterHarnessAgentMutation,
} from '@/features/federation/hooks'
import type { FederatedAgent } from '@/features/federation/types'
import type { CliRuntimeEntryResponse, ProviderEntryResponse } from '@/types/generated'
import { EmptyPanel, ErrorPanel, LoadingPanel, StatusDot } from '@/features/federation/components'
import { AgentDetailPanel } from './AgentDetailPanel'
import { DEFAULT_CEILING, humanize, runtimeDisplayNames, runtimeOptionsForEntry } from './format'

/** The chosen harness: a direct provider entry, or a CLI runtime kind. */
type HarnessChoice =
  | { kind: 'direct'; entryId: string }
  | { kind: 'cli'; runtime: string }

/**
 * Two-step creation: choose the harness (direct or CLI), then set the agent's
 * defaults — model, reasoning, policy, prompt, description.
 */
export function NewAgentDialog({
  open,
  onClose,
  entries,
  cliRuntimes,
  preselectedEntryId,
  onAddProvider,
}: {
  open: boolean
  onClose: () => void
  entries: ProviderEntryResponse[]
  cliRuntimes: CliRuntimeEntryResponse[]
  preselectedEntryId: string | null
  onAddProvider: () => void
}) {
  const capabilities = useAgentProviderCapabilitiesQuery()
  const createEmbedded = useCreateEmbeddedAgentMutation()
  const registerHarness = useRegisterHarnessAgentMutation()
  const [harness, setHarness] = useState<HarnessChoice | null>(null)
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [model, setModel] = useState('')
  const [reasoningEffort, setReasoningEffort] = useState('')
  const [permissionPolicy, setPermissionPolicy] = useState<string | null>(null)
  const [systemPrompt, setSystemPrompt] = useState('')
  const [credentialId, setCredentialId] = useState('')
  const [error, setError] = useState<string>()
  const inFlight = useRef(false)

  useEffect(() => {
    if (!open) return
    setHarness(preselectedEntryId ? { kind: 'direct', entryId: preselectedEntryId } : null)
    setName('')
    setDescription('')
    setModel('')
    setReasoningEffort('')
    setPermissionPolicy(null)
    setSystemPrompt('')
    setCredentialId('')
    setError(undefined)
  }, [open, preselectedEntryId])

  const activeEntries = entries.filter((entry) => entry.status === 'configured' && entry.enabled)
  const selectedEntry =
    harness?.kind === 'direct'
      ? (activeEntries.find((entry) => entry.id === harness.entryId) ?? null)
      : null
  const cliRuntime = harness?.kind === 'cli' ? harness.runtime : null
  const capability = selectedEntry
    ? capabilities.data?.items.find((item) => item.provider === selectedEntry.provider)
    : undefined
  const step: 1 | 2 = harness ? 2 : 1
  const discovered = useDiscoveredOptions(null, cliRuntime)
  const reasoningOptionsForModel = useMemo(
    () => getReasoningOptionsForModel(discovered.data, model.trim() ? model : null),
    [discovered.data, model],
  )
  const uniqueCliRuntimes = cliRuntimes.filter(
    (runtimeEntry, index, all) =>
      runtimeEntry.enabled
      && all.findIndex((candidate) => candidate.enabled && candidate.kind === runtimeEntry.kind) === index,
  )
  /** Entries whose provider can power the chosen CLI harness via injection. */
  const harnessCredentialEntries = useMemo(() => {
    if (!cliRuntime) return []
    return activeEntries.filter((entry) =>
      runtimeOptionsForEntry(capabilities.data?.items, entry).some(
        (option) => option.runtime === cliRuntime && option.support_level !== 'unavailable',
      ),
    )
  }, [activeEntries, capabilities.data?.items, cliRuntime])

  useEffect(() => {
    if (step === 2 && selectedEntry && !model) {
      setModel(capability?.default_model ?? '')
    }
  }, [capability?.default_model, model, selectedEntry, step])

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (inFlight.current || !harness) return
    if (!name.trim()) {
      setError('A name is required.')
      return
    }
    inFlight.current = true
    setError(undefined)
    try {
      if (harness.kind === 'direct' && selectedEntry) {
        if (!model.trim()) {
          setError('A model is required for a direct agent.')
          return
        }
        await createEmbedded.mutateAsync({
          name: name.trim(),
          description: description.trim() ? description.trim() : null,
          credential_id: selectedEntry.id,
          model: model.trim(),
          system_prompt: systemPrompt.trim() ? systemPrompt.trim() : null,
          account_permission_ceiling: DEFAULT_CEILING,
          tool_policy: DEFAULT_CEILING,
        })
      } else if (harness.kind === 'cli') {
        await registerHarness.mutateAsync({
          name: name.trim(),
          description: description.trim() ? description.trim() : null,
          executor_type: harness.runtime,
          model: model.trim() ? model.trim() : null,
          reasoning_effort: reasoningEffort.trim() ? reasoningEffort.trim() : null,
          permission_policy: permissionPolicy,
          prompt_template: systemPrompt.trim() ? systemPrompt.trim() : null,
          credential_id: credentialId || null,
          config_json: discovered.data?.modelConfigs?.[model.trim()] ?? {},
        })
      }
      onClose()
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'The agent could not be created.')
    } finally {
      inFlight.current = false
    }
  }

  return (
    <Dialog open={open} onOpenChange={(next) => !next && onClose()}>
      <DialogContent className="max-w-2xl">
        <DialogHeader>
          <p className="font-mono text-micro font-semibold uppercase tracking-[1px] text-muted-foreground">
            New agent · step {step} of 2
          </p>
          <DialogTitle className="mt-1">
            {step === 1 ? 'Choose the harness' : 'Set the agent defaults'}
          </DialogTitle>
          <DialogDescription>
            {step === 1
              ? 'An agent runs directly on the built-in runtime, or through a CLI harness.'
              : 'These defaults apply everywhere the agent is bound. You can change them any time.'}
          </DialogDescription>
        </DialogHeader>

        {step === 1 ? (
          <div className="mt-5 space-y-4">
            {activeEntries.length === 0 && uniqueCliRuntimes.length === 0 ? (
              <EmptyPanel
                title="No harness available"
                description="Add a provider first, or authenticate a CLI on a connected runtime."
                icon={<Key size={19} />}
              />
            ) : null}
            {activeEntries.length > 0 ? (
              <div className="space-y-2">
                <p className="font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
                  Direct · built-in runtime
                </p>
                {activeEntries.map((entry) => (
                  <button
                    key={entry.id}
                    type="button"
                    className="flex w-full items-center justify-between gap-3 rounded-md border border-border-subtle bg-card px-3 py-2 text-left hover:border-ember-border"
                    onClick={() => setHarness({ kind: 'direct', entryId: entry.id })}
                  >
                    <div className="min-w-0">
                      <p className="truncate text-sm font-medium text-foreground">
                        {humanize(entry.provider)} · {entry.label}
                      </p>
                      <p className="mt-0.5 text-xs text-muted-foreground">
                        {entry.credential_method === 'oauth_bundle' ? 'OAuth login' : 'API key'} · used
                        by {entry.used_by.length}
                      </p>
                    </div>
                    <CaretRight size={15} className="shrink-0 text-muted-foreground" aria-hidden />
                  </button>
                ))}
              </div>
            ) : null}
            {uniqueCliRuntimes.length > 0 ? (
              <div className="space-y-2">
                <p className="font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
                  CLI harness
                </p>
                {uniqueCliRuntimes.map((runtimeEntry) => (
                  <button
                    key={runtimeEntry.kind}
                    type="button"
                    className="flex w-full items-center justify-between gap-3 rounded-md border border-border-subtle bg-card px-3 py-2 text-left hover:border-ember-border"
                    onClick={() => setHarness({ kind: 'cli', runtime: runtimeEntry.kind })}
                  >
                    <div className="flex min-w-0 items-center gap-2.5">
                      <TerminalWindow size={16} className="shrink-0 text-primary" aria-hidden />
                      <div className="min-w-0">
                        <p className="truncate text-sm font-medium text-foreground">
                          {runtimeDisplayNames[runtimeEntry.kind] ?? humanize(runtimeEntry.kind)}
                          {runtimeEntry.version ? (
                            <span className="ml-2 font-mono text-xs text-muted-foreground">
                              v{runtimeEntry.version}
                            </span>
                          ) : null}
                        </p>
                        <p className="mt-0.5 text-xs text-muted-foreground">
                          {humanize(runtimeEntry.availability)} · CLI-managed or provider credential
                        </p>
                      </div>
                    </div>
                    <CaretRight size={15} className="shrink-0 text-muted-foreground" aria-hidden />
                  </button>
                ))}
              </div>
            ) : null}
            <DialogFooter className="gap-2">
              <Button type="button" variant="outline" onClick={onAddProvider}>
                <Plus size={15} aria-hidden />
                Add a provider
              </Button>
            </DialogFooter>
          </div>
        ) : null}

        {step === 2 ? (
          <form onSubmit={submit} className="mt-5 space-y-4">
            <p className="text-xs text-muted-foreground">
              Harness:{' '}
              <strong className="text-foreground">
                {selectedEntry
                  ? `Direct · ${humanize(selectedEntry.provider)} · ${selectedEntry.label}`
                  : (runtimeDisplayNames[cliRuntime ?? ''] ?? humanize(cliRuntime))}
              </strong>
            </p>
            <div className="grid gap-4 sm:grid-cols-2">
              <div className="space-y-2">
                <Label htmlFor="agent-name">Agent name</Label>
                <Input
                  id="agent-name"
                  value={name}
                  onChange={(event) => setName(event.target.value)}
                  placeholder="Forge assistant"
                  required
                />
              </div>
              {cliRuntime ? (
                <ModelSelector
                  id="agent-model"
                  models={discovered.data?.models ?? []}
                  recentModelIds={[]}
                  value={model.trim() ? model : null}
                  isLoading={discovered.isFetching}
                  hasError={discovered.isError}
                  onChange={(next) => {
                    setModel(next ?? '')
                    setReasoningEffort('')
                  }}
                />
              ) : (
                <div className="space-y-2">
                  <Label htmlFor="agent-model">Model</Label>
                  <Input
                    id="agent-model"
                    value={model}
                    onChange={(event) => setModel(event.target.value)}
                    required
                  />
                </div>
              )}
              {cliRuntime ? (
                <>
                  <ReasoningSelector
                    id="agent-reasoning"
                    options={reasoningOptionsForModel}
                    value={reasoningEffort.trim() ? reasoningEffort : null}
                    isLoading={discovered.isFetching}
                    hasError={discovered.isError}
                    onChange={(next) => setReasoningEffort(next ?? '')}
                  />
                  <PolicySelector
                    id="agent-policy"
                    value={permissionPolicy}
                    onChange={setPermissionPolicy}
                  />
                  <div className="space-y-2 sm:col-span-2">
                    <Label htmlFor="agent-credential">Credential</Label>
                    <Select
                      id="agent-credential"
                      value={credentialId}
                      onChange={setCredentialId}
                      options={[
                        { value: '', label: 'CLI-managed login' },
                        ...harnessCredentialEntries.map((entry) => ({
                          value: entry.id,
                          label: `${humanize(entry.provider)} · ${entry.label}`,
                        })),
                      ]}
                      aria-label="Harness credential"
                    />
                  </div>
                </>
              ) : null}
              <div className="space-y-2 sm:col-span-2">
                <Label htmlFor="agent-description">Description</Label>
                <Input
                  id="agent-description"
                  value={description}
                  onChange={(event) => setDescription(event.target.value)}
                  placeholder="What this agent is for — other agents use this to pick it"
                />
              </div>
              <div className="space-y-2 sm:col-span-2">
                <Label htmlFor="agent-prompt">System prompt (optional)</Label>
                <Textarea
                  id="agent-prompt"
                  value={systemPrompt}
                  onChange={(event) => setSystemPrompt(event.target.value)}
                  placeholder="A bounded role for this agent"
                  rows={3}
                />
              </div>
            </div>
            {error ? (
              <p role="alert" className="text-xs text-destructive">
                {error}
              </p>
            ) : null}
            <DialogFooter className="gap-2">
              <Button type="button" variant="ghost" onClick={() => setHarness(null)}>
                Back
              </Button>
              <Button type="submit" disabled={createEmbedded.isPending || registerHarness.isPending}>
                <ShieldCheck size={15} aria-hidden />
                {createEmbedded.isPending || registerHarness.isPending ? 'Creating…' : 'Create agent'}
              </Button>
            </DialogFooter>
          </form>
        ) : null}
      </DialogContent>
    </Dialog>
  )
}

function RosterRow({
  agent,
  selected,
  onSelect,
}: {
  agent: FederatedAgent
  selected: boolean
  onSelect: () => void
}) {
  const runtime = agent.executor_type === 'embedded' ? 'direct' : agent.executor_type
  return (
    <button
      type="button"
      onClick={onSelect}
      aria-current={selected}
      className={cn(
        'relative flex w-full items-start gap-3 rounded-lg px-3 py-2.5 text-left transition-colors',
        selected
          ? 'border border-primary/20 bg-primary/8 text-foreground before:absolute before:left-0 before:top-1/2 before:-translate-y-1/2 before:h-4 before:w-[3px] before:rounded-r-full before:bg-primary'
          : 'border border-transparent text-foreground hover:bg-accent/50',
      )}
    >
      <div className="relative mt-0.5 flex h-8 w-8 shrink-0 items-center justify-center rounded-lg bg-ember-surface text-primary">
        <Robot size={16} weight="duotone" aria-hidden />
        <StatusDot
          status={agent.effective_status ?? agent.status}
          className="absolute -bottom-0.5 -right-0.5 border border-card"
        />
      </div>
      <div className="min-w-0 flex-1">
        <p className="truncate text-sm font-medium">{agent.name}</p>
        <p className="mt-0.5 truncate text-xs text-muted-foreground">
          {runtimeDisplayNames[runtime] ?? humanize(runtime)} · {agent.model ?? 'model not set'}
        </p>
      </div>
    </button>
  )
}

function EmptyDetailPanel() {
  return (
    <div className="flex flex-1 items-center justify-center">
      <div className="text-center">
        <div className="mx-auto mb-4 flex h-12 w-12 items-center justify-center rounded-full bg-muted">
          <Robot size={24} className="text-muted-foreground" />
        </div>
        <p className="text-sm font-medium">Select an agent</p>
        <p className="mt-1 text-xs text-muted-foreground">
          Choose an agent from the list to view and edit its settings
        </p>
      </div>
    </div>
  )
}

/** Agents tab: a Runtimes-style master/detail — roster on the left, one agent's detail on the right. */
export function AgentsTab({
  agents,
  entries,
  chatEntries,
  isLoading,
  isError,
  onRetry,
  selectedId,
  onSelect,
  providerFilter,
  onProviderFilterChange,
  onNewAgent,
}: {
  agents: FederatedAgent[]
  entries: ProviderEntryResponse[]
  chatEntries: AgentChatEntry[]
  isLoading: boolean
  isError: boolean
  onRetry: () => void
  selectedId: string | null
  onSelect: (id: string | null) => void
  providerFilter: string
  onProviderFilterChange: (value: string) => void
  onNewAgent: () => void
}) {
  const [query, setQuery] = useState('')
  const [statusFilter, setStatusFilter] = useState('all')

  const providerOptions = useMemo(
    () => [
      { value: 'all', label: 'All providers' },
      ...Array.from(new Set(agents.flatMap((agent) => (agent.provider ? [agent.provider] : []))))
        .sort()
        .map((provider) => ({ value: provider, label: humanize(provider) })),
    ],
    [agents],
  )
  const statusOptions = useMemo(
    () => [
      { value: 'all', label: 'All statuses' },
      ...Array.from(new Set(agents.map((agent) => agent.status)))
        .sort()
        .map((status) => ({ value: status, label: humanize(status) })),
    ],
    [agents],
  )
  const filteredAgents = useMemo(() => {
    const normalized = query.trim().toLowerCase()
    return agents.filter((agent) => {
      if (providerFilter !== 'all' && agent.provider !== providerFilter) return false
      if (statusFilter !== 'all' && agent.status !== statusFilter) return false
      if (!normalized) return true
      return [agent.name, agent.description, agent.executor_type, agent.provider, agent.model, agent.status]
        .filter(Boolean)
        .some((value) => String(value).toLowerCase().includes(normalized))
    })
  }, [agents, providerFilter, query, statusFilter])

  const selectedAgent = agents.find((agent) => agent.id === selectedId) ?? null

  return (
    <div
      role="tabpanel"
      id="agent-settings-panel-agents"
      aria-labelledby="agent-settings-tab-agents"
    >
      {isLoading ? <LoadingPanel label="Loading agent roster" /> : null}
      {isError ? (
        <ErrorPanel
          title="Agent roster unavailable"
          onRetry={onRetry}
          description="The agent roster is unavailable. Existing Agent Chat history remains server-authoritative."
        />
      ) : null}
      {!isLoading && !isError && agents.length === 0 ? (
        <EmptyPanel
          title="No agents yet"
          description="1. Connect a provider. 2. Create an agent on it — directly or through a CLI harness."
          icon={<Robot size={19} />}
          action={
            <Button onClick={onNewAgent}>
              <Plus size={15} aria-hidden />
              Get started
            </Button>
          }
        />
      ) : null}
      {!isLoading && !isError && agents.length > 0 ? (
        <div className="flex h-[calc(100vh-17rem)] min-h-[520px] gap-0 overflow-hidden rounded-xl border border-border-subtle bg-card shadow-card">
          <div className="flex w-80 shrink-0 flex-col border-r border-border-subtle bg-background">
            <header className="flex shrink-0 items-center justify-between border-b border-border-subtle px-4 py-3">
              <div>
                <p className="font-mono text-micro font-semibold uppercase tracking-[1px] text-muted-foreground">
                  Agents
                </p>
                <p className="mt-0.5 text-[11px] text-muted-foreground">{agents.length} total</p>
              </div>
              <Button
                variant="ghost"
                size="icon-sm"
                aria-label="New agent"
                title="New agent"
                onClick={onNewAgent}
              >
                <Plus size={14} weight="bold" />
              </Button>
            </header>
            <div className="shrink-0 space-y-2 border-b border-border-subtle px-3 py-2.5">
              <label className="relative block">
                <span className="sr-only">Search agents</span>
                <MagnifyingGlass
                  size={14}
                  className="pointer-events-none absolute left-2.5 top-1/2 -translate-y-1/2 text-muted-foreground"
                  aria-hidden
                />
                <Input
                  className="pl-8"
                  value={query}
                  onChange={(event) => setQuery(event.target.value)}
                  placeholder="Search agents"
                />
              </label>
              <div className="grid grid-cols-2 gap-1.5">
                <Select
                  value={providerFilter}
                  options={providerOptions}
                  onChange={onProviderFilterChange}
                  aria-label="Filter agents by provider"
                />
                <Select
                  value={statusFilter}
                  options={statusOptions}
                  onChange={setStatusFilter}
                  aria-label="Filter agents by status"
                />
              </div>
            </div>
            <div className="flex-1 overflow-y-auto p-1.5">
              {filteredAgents.length === 0 ? (
                <p className="px-2 py-6 text-center text-xs text-muted-foreground">
                  No matching agents
                </p>
              ) : (
                <div className="space-y-0.5">
                  {filteredAgents.map((agent) => (
                    <RosterRow
                      key={agent.id}
                      agent={agent}
                      selected={agent.id === selectedId}
                      onSelect={() => onSelect(agent.id === selectedId ? null : agent.id)}
                    />
                  ))}
                </div>
              )}
            </div>
          </div>
          <div className="flex flex-1 flex-col overflow-hidden">
            {selectedAgent ? (
              <AgentDetailPanel
                agent={selectedAgent}
                entries={entries}
                chatEntries={chatEntries}
              />
            ) : (
              <EmptyDetailPanel />
            )}
          </div>
        </div>
      ) : null}
    </div>
  )
}
