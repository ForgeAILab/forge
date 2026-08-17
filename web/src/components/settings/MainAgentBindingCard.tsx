import { useEffect, useMemo, useState } from 'react'
import { ShieldCheck } from '@phosphor-icons/react'
import { toast } from 'sonner'
import { Button } from '@/components/ui/button'
import { Select } from '@/components/ui/select'
import { Card } from '@/components/ui/card'
import { ErrorPanel, SectionKicker, StateBadge } from '@/features/federation/components'
import { humanize, isDirectAgent, numberValue, runtimeDisplayNames } from '@/features/federation/format'
import {
  isVersionConflict,
  useMainAgentBindingQuery,
  useSetMainAgentBindingMutation,
} from '@/features/federation/hooks'
import type { FederatedAgent, MainAgentBindingInput } from '@/features/federation/types'
import { ApiError } from '@/api/client'

/** One-line harness/model summary of an agent, as shown under binding pickers. */
export function agentSummary(agent: FederatedAgent): string {
  const runtime = isDirectAgent(agent.backend_kind)
    ? 'Direct'
    : (runtimeDisplayNames[agent.executor_type] ?? humanize(agent.executor_type))
  return `${runtime} · ${agent.model ?? 'model not set'}`
}

export function MainAgentBindingCard({
  agents,
  onConnect,
}: {
  agents: FederatedAgent[]
  onConnect: () => void
}) {
  const bindingQuery = useMainAgentBindingQuery()
  const setBinding = useSetMainAgentBindingMutation()
  const [identityId, setIdentityId] = useState('')
  const [formError, setFormError] = useState<string | null>(null)
  const bindingMissing = bindingQuery.error instanceof ApiError && bindingQuery.error.status === 404
  const selectedAgent = agents.find((agent) => agent.id === identityId)

  useEffect(() => {
    if (!bindingQuery.data) return
    setIdentityId(bindingQuery.data.identity_id)
  }, [bindingQuery.data])

  const identityOptions = useMemo(() => {
    const options = agents.map((agent) => ({
      value: agent.id,
      label: `${agent.name} · ${agent.provider ?? agent.executor_type}`,
    }))
    const currentIdentity = bindingQuery.data?.identity_id
    if (currentIdentity && !agents.some((agent) => agent.id === currentIdentity)) {
      options.unshift({ value: currentIdentity, label: 'Current binding · unavailable in roster' })
    }
    return options
  }, [agents, bindingQuery.data?.identity_id])

  async function save() {
    if (!identityId) {
      setFormError('Choose a Main Agent before saving.')
      return
    }
    setFormError(null)
    const input: MainAgentBindingInput = {
      identity_id: identityId,
      expected_version: numberValue(bindingQuery.data?.version, 0),
      autonomy_policy: bindingQuery.data?.autonomy_policy ?? {},
    }
    try {
      await setBinding.mutateAsync(input)
      toast.success('Main Agent binding saved')
    } catch (cause) {
      if (isVersionConflict(cause)) {
        setFormError('The Main Agent changed elsewhere. Refresh the binding and try again.')
        void bindingQuery.refetch()
        return
      }
      setFormError(
        cause instanceof Error ? cause.message : 'Main Agent binding could not be saved.',
      )
    }
  }

  if (bindingQuery.isLoading) {
    return (
      <Card className="border-border-subtle bg-card/70 p-5" role="status" aria-live="polite">
        Loading Main Agent binding…
      </Card>
    )
  }
  if (bindingQuery.isError && !bindingMissing) {
    return (
      <ErrorPanel
        title="Main Agent binding unavailable"
        description="Forge could not load the account's Main Agent binding. Retry before changing it."
        onRetry={() => void bindingQuery.refetch()}
      />
    )
  }

  return (
    <Card className="border-ember-border bg-ember-surface p-5">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div>
          <SectionKicker>Main Agent Chat</SectionKicker>
          <h2 className="mt-1 text-lg font-semibold tracking-tight text-foreground">
            One account-owned Main Agent
          </h2>
          <p className="mt-1 max-w-2xl text-sm leading-6 text-muted-foreground">
            Choose the agent that answers the global timeline. The binding follows the agent&apos;s
            settings — edit the agent to change its model or policy.
          </p>
        </div>
        <StateBadge
          status={bindingQuery.data?.state ?? 'setup_required'}
          label={(bindingQuery.data?.state ?? 'setup required').replaceAll('_', ' ')}
        />
      </div>

      {agents.length === 0 ? (
        <div className="mt-5 rounded-md border border-border-subtle bg-background/60 px-3 py-3 text-sm text-muted-foreground">
          Create an agent before selecting a Main Agent.
        </div>
      ) : (
        <div className="mt-5 max-w-md space-y-2">
          <label
            htmlFor="main-agent-identity"
            className="font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground"
          >
            Agent
          </label>
          <Select
            id="main-agent-identity"
            value={identityId}
            options={identityOptions}
            placeholder="Select agent"
            onChange={(next) => {
              setIdentityId(next)
              setFormError(null)
            }}
            aria-label="Main Agent"
          />
          {selectedAgent ? (
            <p className="text-xs leading-5 text-muted-foreground">{agentSummary(selectedAgent)}</p>
          ) : null}
        </div>
      )}

      <div className="mt-5 flex flex-wrap items-center gap-3 border-t border-ember-border pt-4">
        <div className="flex min-w-0 flex-1 items-start gap-2 text-xs text-muted-foreground">
          <ShieldCheck size={15} className="mt-0.5 shrink-0 text-primary" aria-hidden />
          <span>
            Server-enforced Main scope. Expected version{' '}
            {numberValue(bindingQuery.data?.version, 0)}.
          </span>
        </div>
        <Button
          onClick={() => void save()}
          disabled={setBinding.isPending || !identityId || agents.length === 0}
        >
          {setBinding.isPending ? 'Saving…' : bindingMissing ? 'Set Main Agent' : 'Save Main Agent'}
        </Button>
        <Button type="button" variant="outline" onClick={onConnect}>
          New agent
        </Button>
      </div>
      {formError ? (
        <p
          className="mt-3 rounded-md border border-destructive/30 bg-destructive/5 px-3 py-2 text-xs text-destructive"
          role="alert"
        >
          {formError}
        </p>
      ) : null}
    </Card>
  )
}
