import { useEffect, useMemo, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { Robot, ShieldCheck } from '@phosphor-icons/react'
import { useUpdateAgent } from '@/api/hooks'
import { Button } from '@/components/ui/button'
import { CollapsibleSection } from '@/components/ui/collapsible-section'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select } from '@/components/ui/select'
import { Textarea } from '@/components/ui/textarea'
import { ModelSelector } from '@/components/execution-config/ModelSelector'
import { PolicySelector } from '@/components/execution-config/PolicySelector'
import { ReasoningSelector } from '@/components/execution-config/ReasoningSelector'
import { getReasoningOptionsForModel, useDiscoveredOptions } from '@/hooks/useDiscoveredOptions'
import type { AgentChatEntry } from '@/features/agent-chat/types'
import {
  federationQueryKeys,
  isVersionConflict,
  useAgentProviderCapabilitiesQuery,
  useConnectEmbeddedProfileMutation,
} from '@/features/federation/hooks'
import type { FederatedAgent } from '@/features/federation/types'
import type { ProviderEntryResponse } from '@/types/generated'
import { StateBadge, StatusDot } from '@/features/federation/components'
import {
  formatCost,
  formatDuration,
  formatRate,
  formatTokens,
} from '@/components/settings/project-settings-utils'
import { DEFAULT_CEILING, humanize, isDirectAgent, runtimeDisplayNames } from './format'

export function AgentDetailPanel({
  agent,
  entries,
  chatEntries,
}: {
  agent: FederatedAgent
  entries: ProviderEntryResponse[]
  chatEntries: AgentChatEntry[]
}) {
  const updateAvailability = useUpdateAgent()
  const [confirmingDisable, setConfirmingDisable] = useState(false)
  const [availabilityError, setAvailabilityError] = useState<string>()
  const credentialEntry = entries.find((entry) => entry.id === agent.credential_handle_id)
  const requiresRecovery =
    credentialEntry != null &&
    (credentialEntry.status === 'revoked' || credentialEntry.status === 'invalid')
  const connectionStatus = requiresRecovery
    ? 'recovery_required'
    : (agent.effective_status ?? agent.status)
  const runtime = agent.executor_type === 'embedded' ? 'direct' : agent.executor_type

  const boundChips = chatEntries
    .filter((entry) => entry.identity_id === agent.id)
    .map((entry) => (entry.kind === 'main' ? 'Main Agent' : (entry.project_name ?? 'Project Agent')))

  return (
    <div className="flex flex-1 flex-col overflow-y-auto">
      <header className="flex shrink-0 items-start gap-4 border-b border-border-subtle px-6 py-4">
        <div className="flex h-10 w-10 shrink-0 items-center justify-center rounded-lg bg-ember-surface text-primary">
          <Robot size={20} weight="duotone" aria-hidden />
        </div>
        <div className="min-w-0 flex-1">
          <div className="flex flex-wrap items-center gap-2">
            <h2 className="truncate text-lg font-semibold text-foreground">{agent.name}</h2>
            <StateBadge status={agent.effective_status ?? agent.status} label={humanize(agent.effective_status ?? agent.status)} />
          </div>
          <p className="mt-1 truncate font-mono text-xs text-muted-foreground">
            {runtimeDisplayNames[runtime] ?? humanize(runtime)}
            {credentialEntry ? ` · ${credentialEntry.label}` : ''}
          </p>
          {boundChips.length > 0 ? (
            <div className="mt-2 flex flex-wrap gap-1.5">
              {boundChips.map((chip) => (
                <span
                  key={chip}
                  className="rounded-full border border-ember-border bg-ember-surface px-2 py-0.5 font-mono text-micro font-semibold uppercase tracking-[0.6px] text-primary"
                >
                  {chip}
                </span>
              ))}
            </div>
          ) : null}
        </div>
      </header>

      <div className="flex-1 space-y-6 px-6 py-5">
        <section className="rounded-md border border-border-subtle bg-card p-3" aria-label="Agent availability">
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <p className="text-sm font-medium text-foreground">
                Agent is {agent.paused ? 'disabled' : 'enabled'}
              </p>
              <p className="mt-1 text-xs text-muted-foreground">
                {agent.paused
                  ? 'Configuration and bindings are preserved. Enable it to accept new work.'
                  : 'Disabling stops new work without removing this Agent or its bindings.'}
              </p>
            </div>
            <Button
              size="sm"
              variant="outline"
              disabled={updateAvailability.isPending}
              onClick={() => {
                setAvailabilityError(undefined)
                if (!agent.paused) {
                  setConfirmingDisable(true)
                  return
                }
                void updateAvailability
                  .mutateAsync({
                    agentId: agent.id,
                    body: { paused: false, version: agent.version },
                  })
                  .catch((cause: unknown) =>
                    setAvailabilityError(
                      cause instanceof Error ? cause.message : 'Agent could not be enabled.',
                    ),
                  )
              }}
            >
              {agent.paused ? 'Enable agent' : 'Disable agent'}
            </Button>
          </div>
          {confirmingDisable ? (
            <div className="mt-3 rounded-md border border-warning/30 bg-warning/10 p-3 text-xs text-warning">
              <p>
                Disable this Agent? Existing {boundChips.length > 0 ? boundChips.join(' and ') : 'settings'}
                {' '}stay in place, but the Agent will not accept new work until re-enabled.
              </p>
              <div className="mt-2 flex gap-2">
                <Button
                  size="sm"
                  variant="outline"
                  disabled={updateAvailability.isPending}
                  onClick={() =>
                    void updateAvailability
                      .mutateAsync({
                        agentId: agent.id,
                        body: { paused: true, version: agent.version },
                      })
                      .then(() => setConfirmingDisable(false))
                      .catch((cause: unknown) =>
                        setAvailabilityError(
                          cause instanceof Error ? cause.message : 'Agent could not be disabled.',
                        ),
                      )
                  }
                >
                  Disable agent
                </Button>
                <Button size="sm" variant="ghost" onClick={() => setConfirmingDisable(false)}>
                  Keep enabled
                </Button>
              </div>
            </div>
          ) : null}
          {availabilityError ? (
            <p className="mt-2 text-xs text-destructive" role="alert">{availabilityError}</p>
          ) : null}
        </section>
        {/* Stat grid */}
        <div className="grid grid-cols-2 gap-2.5 sm:grid-cols-4">
          {[
            { label: 'Model', value: agent.model ?? 'Not set' },
            { label: 'Provider', value: agent.provider ? humanize(agent.provider) : 'CLI-managed' },
            {
              label: agent.reasoning_effort ? 'Reasoning' : 'Permission',
              value: agent.reasoning_effort
                ? humanize(agent.reasoning_effort)
                : agent.permission_policy
                  ? humanize(agent.permission_policy)
                  : '—',
            },
            {
              label: 'Total runs',
              value: agent.total_runs,
              detail: agent.success_rate != null ? `${formatRate(agent.success_rate)} success` : undefined,
            },
            {
              label: 'Tokens used',
              value: formatTokens(agent.total_tokens ?? 0),
              detail: `${formatTokens(agent.total_input_tokens ?? 0)} in / ${formatTokens(agent.total_output_tokens ?? 0)} out`,
            },
            {
              label: 'Est. cost',
              value: formatCost(agent.total_cost_usd ?? null),
            },
            {
              label: 'Success rate',
              value: agent.success_rate != null ? formatRate(agent.success_rate) : '—',
            },
            {
              label: 'Avg duration',
              value: formatDuration(agent.avg_duration_ms ?? null),
            },
          ].map((stat) => (
            <div key={stat.label} className="rounded-lg border border-border-subtle bg-muted/40 px-3.5 py-3">
              <p className="mb-1.5 font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
                {stat.label}
              </p>
              <p className="truncate font-mono text-lg font-semibold tabular-nums text-foreground" title={String(stat.value)}>
                {stat.value}
              </p>
              {'detail' in stat && stat.detail ? (
                <p className="mt-1 truncate font-mono text-[11px] text-muted-foreground">
                  {stat.detail}
                </p>
              ) : null}
            </div>
          ))}
        </div>

        <AgentSettingsForm agent={agent} entries={entries} />

        {requiresRecovery ? (
          <p
            className="rounded-md border border-warning/30 bg-warning/10 px-3 py-2 text-xs text-warning"
            role="status"
          >
            This agent&apos;s provider entry is disconnected. Reconnect the entry or move the agent
            onto another one before relying on its Main or Project binding.
          </p>
        ) : null}

        {/* Collapsed identity facts */}
        <CollapsibleSection title="Identity details" className="border-t border-border-subtle pt-4">
          <dl className="mt-2 space-y-2 text-xs">
            <div className="flex justify-between gap-4">
              <dt className="text-muted-foreground">Stable ID</dt>
              <dd className="truncate font-mono text-foreground">{agent.id}</dd>
            </div>
            <div className="flex justify-between gap-4">
              <dt className="text-muted-foreground">Connection</dt>
              <dd className="inline-flex items-center gap-1.5 text-foreground">
                <StatusDot status={connectionStatus} />
                {humanize(connectionStatus)}
              </dd>
            </div>
            <div className="flex justify-between gap-4">
              <dt className="text-muted-foreground">Max concurrent tasks</dt>
              <dd className="font-mono text-foreground">{agent.max_concurrent_tasks}</dd>
            </div>
          </dl>
        </CollapsibleSection>
      </div>
    </div>
  )
}

/**
 * The agent's settings, edited directly in the panel: fields show the current
 * values and a Save/Discard bar appears once something changes. Bindings
 * follow the agent, so saving here is all it takes — no profile publishing or
 * rebinding steps.
 */
function AgentSettingsForm({
  agent,
  entries,
}: {
  agent: FederatedAgent
  entries: ProviderEntryResponse[]
}) {
  const queryClient = useQueryClient()
  const direct = isDirectAgent(agent.backend_kind)
  const runtime = agent.executor_type === 'embedded' ? 'direct' : agent.executor_type
  const credentialEntry = entries.find((entry) => entry.id === agent.credential_handle_id)
  const [name, setName] = useState(agent.name)
  const [description, setDescription] = useState(agent.description ?? '')
  const [entryId, setEntryId] = useState(agent.credential_handle_id ?? '')
  const [model, setModel] = useState(agent.model ?? '')
  const [reasoningEffort, setReasoningEffort] = useState(agent.reasoning_effort ?? '')
  const [permissionPolicy, setPermissionPolicy] = useState<string | null>(
    agent.permission_policy ?? null,
  )
  const [systemPrompt, setSystemPrompt] = useState(agent.prompt_template ?? '')
  const [error, setError] = useState<string>()

  const activeEntries = entries.filter((entry) => entry.status === 'configured' && entry.enabled)
  const capabilities = useAgentProviderCapabilitiesQuery()
  const discovered = useDiscoveredOptions(agent.id, direct ? null : agent.executor_type)

  const connectProfile = useConnectEmbeddedProfileMutation()
  const updateAgent = useUpdateAgent()

  function syncFromAgent() {
    setName(agent.name)
    setDescription(agent.description ?? '')
    setEntryId(agent.credential_handle_id ?? '')
    setModel(agent.model ?? '')
    setReasoningEffort(agent.reasoning_effort ?? '')
    setPermissionPolicy(agent.permission_policy ?? null)
    setSystemPrompt(agent.prompt_template ?? '')
    setError(undefined)
  }

  // Re-sync when another agent is selected or a save lands (version bump).
  useEffect(() => {
    syncFromAgent()
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [agent.id, agent.version])

  const dirty =
    name !== agent.name ||
    description !== (agent.description ?? '') ||
    model !== (agent.model ?? '') ||
    systemPrompt !== (agent.prompt_template ?? '') ||
    (direct
      ? entryId !== (agent.credential_handle_id ?? '')
      : reasoningEffort !== (agent.reasoning_effort ?? '') ||
        permissionPolicy !== (agent.permission_policy ?? null))

  const selectedEntryCapability = useMemo(() => {
    const entry = activeEntries.find((candidate) => candidate.id === entryId)
    if (!entry) return undefined
    return capabilities.data?.items.find((item) => item.provider === entry.provider)
  }, [activeEntries, capabilities.data?.items, entryId])

  const modelSuggestions = discovered.data?.models ?? []
  const reasoningOptionsForModel = useMemo(
    () => getReasoningOptionsForModel(discovered.data, model),
    [discovered.data, model],
  )
  const pending = connectProfile.isPending || updateAgent.isPending

  async function submit(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!dirty) return
    if (!name.trim()) {
      setError('A name is required.')
      return
    }
    if (!model.trim()) {
      setError('A model is required.')
      return
    }
    setError(undefined)
    try {
      if (direct) {
        if (!entryId) {
          setError('A provider entry is required for a direct agent.')
          return
        }
        // Name and description live on the identity; the runtime settings
        // publish internally as the agent's next settings revision.
        let version = agent.version
        if (name.trim() !== agent.name || description.trim() !== (agent.description ?? '')) {
          const updated = await updateAgent.mutateAsync({
            agentId: agent.id,
            body: {
              name: name.trim(),
              description: description.trim() ? description.trim() : null,
              version,
            },
          })
          version = updated.version
        }
        await connectProfile.mutateAsync({
          identityId: agent.id,
          input: {
            version,
            credential_id: entryId,
            model: model.trim(),
            system_prompt: systemPrompt.trim() ? systemPrompt.trim() : null,
            permission_policy: agent.permission_policy ?? 'scoped_proposals',
            tool_policy: DEFAULT_CEILING,
          },
        })
      } else {
        await updateAgent.mutateAsync({
          agentId: agent.id,
          body: {
            name: name.trim(),
            description: description.trim() ? description.trim() : null,
            model: model.trim(),
            reasoning_effort: reasoningEffort.trim() ? reasoningEffort.trim() : null,
            permission_policy: permissionPolicy,
            prompt_template: systemPrompt.trim() ? systemPrompt.trim() : null,
            version: agent.version,
          },
        })
      }
      void queryClient.invalidateQueries({ queryKey: federationQueryKeys.agents })
    } catch (cause) {
      setError(
        isVersionConflict(cause)
          ? 'This agent changed in another session. Refresh and try again.'
          : cause instanceof Error
            ? cause.message
            : 'The agent settings could not be saved.',
      )
    }
  }

  return (
    <section aria-labelledby="agent-detail-settings-heading">
      <h3
        id="agent-detail-settings-heading"
        className="mb-3 font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground"
      >
        Settings
      </h3>
      <form onSubmit={(event) => void submit(event)} className="space-y-4">
        <dl className="divide-y divide-border-subtle rounded-md border border-border-subtle bg-card">
          <div className="flex items-center justify-between gap-4 px-3 py-2">
            <dt className="text-xs text-muted-foreground">Harness</dt>
            <dd className="truncate text-xs font-medium text-foreground">
              {direct
                ? 'Direct · built-in runtime'
                : (runtimeDisplayNames[runtime] ?? humanize(runtime))}
            </dd>
          </div>
          {!direct ? (
            <div className="flex items-center justify-between gap-4 px-3 py-2">
              <dt className="text-xs text-muted-foreground">Credential</dt>
              <dd className="truncate text-xs font-medium text-foreground">
                {credentialEntry
                  ? `${humanize(credentialEntry.provider)} · ${credentialEntry.label}`
                  : 'CLI-managed login'}
              </dd>
            </div>
          ) : null}
        </dl>

        <div className="grid gap-4 sm:grid-cols-2">
          <div className="space-y-2">
            <Label htmlFor="agent-settings-name">Name</Label>
            <Input
              id="agent-settings-name"
              value={name}
              onChange={(event) => setName(event.target.value)}
              required
            />
          </div>
          <div className="space-y-2">
            <Label htmlFor="agent-settings-description">Description</Label>
            <Input
              id="agent-settings-description"
              value={description}
              onChange={(event) => setDescription(event.target.value)}
              placeholder="What this agent is for"
            />
          </div>

          {direct ? (
            <>
              <div className="space-y-2">
                <Label htmlFor="agent-settings-entry">Provider entry</Label>
                <Select
                  id="agent-settings-entry"
                  value={entryId}
                  placeholder={activeEntries.length === 0 ? 'No connected entries' : 'Select entry'}
                  onChange={setEntryId}
                  disabled={activeEntries.length === 0}
                  options={activeEntries.map((entry) => ({
                    value: entry.id,
                    label: `${humanize(entry.provider)} · ${entry.label}`,
                  }))}
                />
              </div>
              <div className="space-y-2">
                <Label htmlFor="agent-settings-model">Model</Label>
                <Input
                  id="agent-settings-model"
                  value={model}
                  onChange={(event) => setModel(event.target.value)}
                  placeholder={selectedEntryCapability?.default_model ?? 'e.g. claude-sonnet-5'}
                  required
                />
              </div>
            </>
          ) : (
            <>
              <ModelSelector
                id="agent-settings-model"
                models={modelSuggestions}
                recentModelIds={[]}
                value={model.trim() ? model : null}
                isLoading={discovered.isFetching}
                hasError={discovered.isError}
                onChange={(next) => {
                  setModel(next ?? '')
                  setReasoningEffort('')
                }}
              />
              <ReasoningSelector
                id="agent-settings-reasoning"
                options={reasoningOptionsForModel}
                value={reasoningEffort.trim() ? reasoningEffort : null}
                isLoading={discovered.isFetching}
                hasError={discovered.isError}
                onChange={(next) => setReasoningEffort(next ?? '')}
              />
              <PolicySelector
                id="agent-settings-policy"
                className="sm:col-span-2"
                value={permissionPolicy}
                onChange={setPermissionPolicy}
              />
            </>
          )}

          <div className="space-y-2 sm:col-span-2">
            <Label htmlFor="agent-settings-prompt">System prompt (optional)</Label>
            <Textarea
              id="agent-settings-prompt"
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

        <div className="flex flex-wrap items-center gap-3 border-t border-border-subtle pt-3">
          <p className="min-w-0 flex-1 text-micro leading-5 text-muted-foreground">
            {dirty
              ? 'Unsaved changes. Saving applies to every bound scope with the agent’s next turn.'
              : 'Every scope this agent is bound to follows these settings. Task launches can still override them per execution.'}
          </p>
          {dirty ? (
            <Button type="button" variant="ghost" size="sm" onClick={syncFromAgent} disabled={pending}>
              Discard
            </Button>
          ) : null}
          <Button type="submit" size="sm" disabled={pending || !dirty}>
            <ShieldCheck size={14} aria-hidden />
            {pending ? 'Saving…' : 'Save settings'}
          </Button>
        </div>
      </form>
    </section>
  )
}
