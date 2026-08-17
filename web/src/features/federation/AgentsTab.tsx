import { useEffect, useMemo, useRef, useState } from 'react'
import { CaretRight, Key, MagnifyingGlass, Plus, Robot, ShieldCheck } from '@phosphor-icons/react'
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
import { EmptyPanel, ErrorPanel, LoadingPanel, StateBadge, StatusDot } from '@/features/federation/components'
import { AgentDetailPanel } from './AgentDetailPanel'
import { DEFAULT_CEILING, humanize, runtimeDisplayNames, runtimeOptionsForEntry } from './format'

type WizardRuntime = { runtime: string; support_level: string; reason: string | null }

/** Three-step registration: authentication source → runtime → configure. */
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
  const [entryId, setEntryId] = useState<string | null>(preselectedEntryId)
  const [cliKind, setCliKind] = useState<string | null>(null)
  const [runtime, setRuntime] = useState<string | null>(null)
  const [name, setName] = useState('')
  const [description, setDescription] = useState('')
  const [model, setModel] = useState('')
  const [reasoningEffort, setReasoningEffort] = useState('')
  const [permissionPolicy, setPermissionPolicy] = useState<string | null>(null)
  const [systemPrompt, setSystemPrompt] = useState('')
  const [error, setError] = useState<string>()
  const inFlight = useRef(false)

  useEffect(() => {
    if (!open) return
    setEntryId(preselectedEntryId)
    setCliKind(null)
    setRuntime(null)
    setName('')
    setDescription('')
    setModel('')
    setReasoningEffort('')
    setPermissionPolicy(null)
    setSystemPrompt('')
    setError(undefined)
  }, [open, preselectedEntryId])

  const activeEntries = entries.filter((entry) => entry.status === 'configured')
  const selectedEntry = activeEntries.find((entry) => entry.id === entryId) ?? null
  const runtimeOptions: WizardRuntime[] = selectedEntry
    ? runtimeOptionsForEntry(capabilities.data?.items, selectedEntry)
    : cliKind
      ? [{ runtime: cliKind, support_level: 'stable', reason: null }]
      : []
  const capability = selectedEntry
    ? capabilities.data?.items.find((item) => item.provider === selectedEntry.provider)
    : undefined
  const step: 1 | 2 | 3 = !selectedEntry && !cliKind ? 1 : !runtime ? 2 : 3
  const isHarness = Boolean(runtime && runtime !== 'direct')
  const discovered = useDiscoveredOptions(null, isHarness ? runtime : null)
  const reasoningOptionsForModel = useMemo(
    () => getReasoningOptionsForModel(discovered.data, model.trim() ? model : null),
    [discovered.data, model],
  )

  useEffect(() => {
    if (step === 3 && selectedEntry && !model) {
      setModel(capability?.default_model ?? '')
    }
  }, [capability?.default_model, model, selectedEntry, step])

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (inFlight.current || !runtime) return
    if (!name.trim()) {
      setError('A name is required.')
      return
    }
    inFlight.current = true
    setError(undefined)
    try {
      if (runtime === 'direct' && selectedEntry) {
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
      } else {
        await registerHarness.mutateAsync({
          name: name.trim(),
          description: description.trim() ? description.trim() : null,
          executor_type: runtime,
          model: model.trim() ? model.trim() : null,
          reasoning_effort: reasoningEffort.trim() ? reasoningEffort.trim() : null,
          permission_policy: permissionPolicy,
          credential_id: selectedEntry?.id ?? null,
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
            New agent · step {step} of 3
          </p>
          <DialogTitle className="mt-1">
            {step === 1
              ? 'Choose an authentication source'
              : step === 2
                ? 'Choose a runtime'
                : 'Configure the agent'}
          </DialogTitle>
          <DialogDescription>
            {step === 1
              ? 'Pick the provider entry or CLI-managed runtime this agent authenticates with.'
              : step === 2
                ? 'Compatibility comes from the server capability catalog.'
                : 'Creation publishes an immutable profile. Bindings stay unchanged until you assign them.'}
          </DialogDescription>
        </DialogHeader>

        {step === 1 ? (
          <div className="mt-5 space-y-3">
            {activeEntries.length === 0 && cliRuntimes.length === 0 ? (
              <EmptyPanel
                title="No authentication sources"
                description="Add a provider first, or authenticate a CLI on a connected runtime."
                icon={<Key size={19} />}
              />
            ) : null}
            {activeEntries.map((entry) => (
              <button
                key={entry.id}
                type="button"
                className="flex w-full items-center justify-between gap-3 rounded-md border border-border-subtle bg-card px-3 py-2 text-left hover:border-ember-border"
                onClick={() => setEntryId(entry.id)}
              >
                <div className="min-w-0">
                  <p className="truncate text-sm font-medium text-foreground">
                    {humanize(entry.provider)} · {entry.label}
                  </p>
                  <p className="mt-0.5 text-xs text-muted-foreground">
                    {entry.credential_method === 'oauth_bundle' ? 'OAuth login' : 'API key'} · used by{' '}
                    {entry.used_by.length}
                  </p>
                </div>
                <CaretRight size={15} className="shrink-0 text-muted-foreground" aria-hidden />
              </button>
            ))}
            {cliRuntimes
              .filter(
                (runtimeEntry, index, all) =>
                  all.findIndex((candidate) => candidate.kind === runtimeEntry.kind) === index,
              )
              .map((runtimeEntry) => (
                <button
                  key={runtimeEntry.kind}
                  type="button"
                  className="flex w-full items-center justify-between gap-3 rounded-md border border-border-subtle bg-card px-3 py-2 text-left hover:border-ember-border"
                  onClick={() => setCliKind(runtimeEntry.kind)}
                >
                  <div className="min-w-0">
                    <p className="truncate text-sm font-medium text-foreground">
                      {runtimeDisplayNames[runtimeEntry.kind] ?? humanize(runtimeEntry.kind)}
                    </p>
                    <p className="mt-0.5 text-xs text-muted-foreground">
                      Uses its own CLI login · {humanize(runtimeEntry.availability)}
                    </p>
                  </div>
                  <CaretRight size={15} className="shrink-0 text-muted-foreground" aria-hidden />
                </button>
              ))}
            <DialogFooter className="gap-2">
              <Button type="button" variant="outline" onClick={onAddProvider}>
                <Plus size={15} aria-hidden />
                Add a provider
              </Button>
            </DialogFooter>
          </div>
        ) : null}

        {step === 2 ? (
          <div className="mt-5 space-y-3">
            <p className="text-xs text-muted-foreground">
              Source:{' '}
              <strong className="text-foreground">
                {selectedEntry
                  ? `${humanize(selectedEntry.provider)} · ${selectedEntry.label}`
                  : (runtimeDisplayNames[cliKind ?? ''] ?? humanize(cliKind))}
              </strong>
            </p>
            {runtimeOptions.map((option) => {
              const unavailable = option.support_level === 'unavailable'
              return (
                <button
                  key={option.runtime}
                  type="button"
                  disabled={unavailable}
                  className={`flex w-full items-center justify-between gap-3 rounded-md border px-3 py-2 text-left ${unavailable ? 'cursor-not-allowed border-border-subtle bg-muted/40 opacity-70' : 'border-border-subtle bg-card hover:border-ember-border'}`}
                  onClick={() => setRuntime(option.runtime)}
                >
                  <div className="min-w-0">
                    <div className="flex flex-wrap items-center gap-2">
                      <p className="text-sm font-medium text-foreground">
                        {runtimeDisplayNames[option.runtime] ?? humanize(option.runtime)}
                      </p>
                      <StateBadge status={option.support_level} label={humanize(option.support_level)} />
                    </div>
                    {option.reason ? (
                      <p className="mt-0.5 text-xs text-muted-foreground">{option.reason}</p>
                    ) : null}
                  </div>
                  {!unavailable ? (
                    <CaretRight size={15} className="shrink-0 text-muted-foreground" aria-hidden />
                  ) : null}
                </button>
              )
            })}
            <DialogFooter>
              <Button
                type="button"
                variant="ghost"
                onClick={() => {
                  setEntryId(null)
                  setCliKind(null)
                }}
              >
                Back
              </Button>
            </DialogFooter>
          </div>
        ) : null}

        {step === 3 ? (
          <form onSubmit={submit} className="mt-5 space-y-4">
            <p className="text-xs text-muted-foreground">
              {selectedEntry
                ? `${humanize(selectedEntry.provider)} · ${selectedEntry.label}`
                : 'CLI-managed login'}{' '}
              → {runtimeDisplayNames[runtime ?? ''] ?? humanize(runtime)}
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
              {isHarness ? (
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
              {isHarness ? (
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
                </>
              ) : null}
              <div className="space-y-2 sm:col-span-2">
                <Label htmlFor="agent-description">Description</Label>
                <Input
                  id="agent-description"
                  value={description}
                  onChange={(event) => setDescription(event.target.value)}
                  placeholder="What this agent is for"
                />
              </div>
              {runtime === 'direct' ? (
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
              ) : null}
            </div>
            {error ? (
              <p role="alert" className="text-xs text-destructive">
                {error}
              </p>
            ) : null}
            <DialogFooter className="gap-2">
              <Button type="button" variant="ghost" onClick={() => setRuntime(null)}>
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
          {runtimeDisplayNames[runtime] ?? humanize(runtime)} · {agent.model ?? 'profile pending'}
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
          Choose an agent from the list to view its model, profiles, and bindings
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
  onChangeModel,
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
  onChangeModel: (agent: FederatedAgent) => void
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
                onChangeModel={onChangeModel}
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
