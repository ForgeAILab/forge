import { useEffect, useState } from 'react'
import { Link } from '@tanstack/react-router'
import { ArrowUpRight, Robot, ShieldCheck } from '@phosphor-icons/react'
import { toast } from 'sonner'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { Select } from '@/components/ui/select'
import { cn } from '@/lib/cn'
import {
  EmptyPanel,
  ErrorPanel,
  LoadingPanel,
  SectionKicker,
  StateBadge,
} from '@/features/federation/components'
import { DEFAULT_PROJECT_PERMISSION_CEILING, numberValue } from '@/features/federation/format'
import {
  isVersionConflict,
  useFederatedAgentsQuery,
  useProjectAgentBindingQuery,
  useSetProjectAgentBindingMutation,
} from '@/features/federation/hooks'
import type { ProjectAgentBindingInput } from '@/features/federation/types'
import { ApiError } from '@/api/client'
import { agentSummary } from './MainAgentBindingCard'

function policyValues(policy: Record<string, unknown> | null | undefined): string[] {
  const allowed = policy?.allowed
  return Array.isArray(allowed)
    ? allowed.filter((value): value is string => typeof value === 'string')
    : []
}

export function ProjectAgentTab({
  projectId,
  projectName,
  highlighted = false,
}: {
  projectId: string
  projectName?: string
  highlighted?: boolean
}) {
  const bindingQuery = useProjectAgentBindingQuery(projectId)
  const agentsQuery = useFederatedAgentsQuery()
  const [identityId, setIdentityId] = useState('')
  const [wakeBudget, setWakeBudget] = useState('3')
  const [formError, setFormError] = useState<string | null>(null)
  const setBinding = useSetProjectAgentBindingMutation(projectId)
  const bindingMissing = bindingQuery.error instanceof ApiError && bindingQuery.error.status === 404
  const agents = agentsQuery.data?.items ?? []
  const selectedAgent = agents.find((agent) => agent.id === identityId)
  // A Project that has not been through Product Genesis carries a placeholder
  // binding whose ceiling is `{}`. That is present but grants nothing, so `??`
  // would save an empty ceiling and leave the Project Agent with no tools.
  const storedCeiling = bindingQuery.data?.permission_ceiling
  const permissionCeiling: Record<string, unknown> =
    storedCeiling && policyValues(storedCeiling).length > 0
      ? storedCeiling
      : DEFAULT_PROJECT_PERMISSION_CEILING

  useEffect(() => {
    const binding = bindingQuery.data
    if (!binding) return
    setIdentityId(binding.identity_id ?? '')
    setWakeBudget(String(numberValue(binding.wake_budget, 3)))
  }, [bindingQuery.data])

  async function save(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault()
    if (!identityId) {
      setFormError('Choose an agent before saving the Project Agent.')
      return
    }
    const parsedWakeBudget = Number(wakeBudget)
    if (!Number.isInteger(parsedWakeBudget) || parsedWakeBudget < 0) {
      setFormError('Wake budget must be a non-negative whole number.')
      return
    }
    setFormError(null)
    const input: ProjectAgentBindingInput = {
      identity_id: identityId,
      expected_version: numberValue(bindingQuery.data?.version, 0),
      permission_ceiling: permissionCeiling,
      autonomy_policy: bindingQuery.data?.autonomy_policy ?? {},
      subscriptions: bindingQuery.data?.subscriptions ?? [],
      wake_budget: parsedWakeBudget,
    }
    try {
      await setBinding.mutateAsync(input)
      toast.success('Project Agent binding saved')
    } catch (cause) {
      if (isVersionConflict(cause)) {
        setFormError('This Project Agent changed elsewhere. Refresh the binding and try again.')
        void bindingQuery.refetch()
        return
      }
      setFormError(
        cause instanceof Error ? cause.message : 'Project Agent binding could not be saved.',
      )
    }
  }

  const bindingError = bindingQuery.error && !bindingMissing ? bindingQuery.error : null
  if (bindingQuery.isLoading || agentsQuery.isLoading) {
    return <LoadingPanel label="Loading Project Agent setup" />
  }
  if (bindingError || agentsQuery.isError) {
    return (
      <ErrorPanel
        title="Project Agent setup unavailable"
        description="Forge could not load the authorized binding or agent roster. Retry before changing this Project's agent."
        onRetry={() => {
          void bindingQuery.refetch()
          void agentsQuery.refetch()
        }}
      />
    )
  }
  if (agents.length === 0) {
    return (
      <Card
        id={`project-agent-${projectId}`}
        className={cn('space-y-6 p-4 sm:p-5', highlighted && 'border-primary ring-2 ring-primary/30')}
      >
        <div>
          <SectionKicker>{projectName ?? 'Project Agent'}</SectionKicker>
          <h2 className="mt-1 text-page font-semibold tracking-tight">Connect one Project Agent</h2>
          <p className="mt-1 max-w-2xl text-sm leading-6 text-muted-foreground">
            A Project has one durable Agent Chat and one binding. Create an agent in Agent settings
            before selecting it here.
          </p>
        </div>
        <EmptyPanel
          title="No agents yet"
          description="Agents stay in Agent settings until you explicitly choose one for this Project."
          icon={<Robot size={19} />}
        />
        <Link
          to="/agents"
          className="inline-flex items-center gap-1.5 text-sm font-medium text-primary underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
        >
          Open Agent settings
          <ArrowUpRight size={14} aria-hidden />
        </Link>
      </Card>
    )
  }

  return (
    <Card
      id={`project-agent-${projectId}`}
      className={cn('space-y-6 p-4 sm:p-5', highlighted && 'border-primary ring-2 ring-primary/30')}
    >
      <div>
        <SectionKicker>{projectName ?? 'Project Agent'}</SectionKicker>
        <h2 className="mt-1 text-page font-semibold tracking-tight">
          {bindingMissing ? 'Not configured' : 'Project Agent binding'}
        </h2>
        <p className="mt-1 max-w-2xl text-sm leading-6 text-muted-foreground">
          Select the single agent admitted to this Project&apos;s durable Agent Chat. The binding
          follows the agent&apos;s settings; replacing it preserves the chat timeline.
        </p>
      </div>

      <form onSubmit={(event) => void save(event)} className="space-y-5">
        <div className="rounded-lg border border-border-subtle bg-muted/10 p-4 sm:p-5">
          <div className="grid gap-5 md:grid-cols-2">
            <div className="space-y-2">
              <label
                htmlFor="project-agent-identity"
                className="font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground"
              >
                Agent
              </label>
              <Select
                id="project-agent-identity"
                value={identityId}
                options={agents.map((agent) => ({
                  value: agent.id,
                  label: `${agent.name} · ${agent.provider ?? agent.executor_type}`,
                }))}
                placeholder="Select agent"
                onChange={(next) => {
                  setIdentityId(next)
                  setFormError(null)
                }}
                aria-label="Project Agent"
              />
              <p className="text-xs leading-5 text-muted-foreground">
                {selectedAgent
                  ? agentSummary(selectedAgent)
                  : 'Only the selected agent is bound to the Project. Creating an agent does not grant it Project access.'}
              </p>
            </div>

            <div className="space-y-2">
              <label
                htmlFor="project-agent-wake-budget"
                className="font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground"
              >
                Wake budget
              </label>
              <Input
                id="project-agent-wake-budget"
                type="number"
                min={0}
                step={1}
                value={wakeBudget}
                onChange={(event) => setWakeBudget(event.target.value)}
                aria-describedby="project-agent-wake-budget-help"
              />
              <p
                id="project-agent-wake-budget-help"
                className="text-xs leading-5 text-muted-foreground"
              >
                Maximum background wake attempts configured for this binding.
              </p>
            </div>

            <div className="space-y-2">
              <span className="font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
                Binding state
              </span>
              <div className="flex h-9 items-center gap-2 rounded-md border border-border-subtle bg-muted/20 px-3">
                <StateBadge
                  status={bindingQuery.data?.state ?? (bindingMissing ? 'setup_required' : 'unknown')}
                  label={bindingMissing ? 'Not configured' : undefined}
                />
                <span className="font-mono text-micro text-muted-foreground">
                  expected version {numberValue(bindingQuery.data?.version, 0)}
                </span>
              </div>
            </div>
          </div>

          <div
            className="mt-5 border-t border-border-subtle pt-4"
            role="note"
            aria-label="Project Agent permission ceiling"
          >
            <div className="flex items-center gap-2">
              <ShieldCheck size={15} className="text-primary" aria-hidden />
              <p className="text-sm font-medium text-foreground">
                Server-enforced permission ceiling
              </p>
            </div>
            <p className="mt-1 text-xs leading-5 text-muted-foreground">
              This setup screen does not grant arbitrary capabilities. The server intersects this
              ceiling with the Project scope before each turn.
            </p>
            <div className="mt-2 flex flex-wrap gap-1.5">
              {policyValues(permissionCeiling).map((capability) => (
                <span
                  key={capability}
                  className="rounded bg-muted px-2 py-1 font-mono text-micro text-foreground"
                >
                  {capability}
                </span>
              ))}
              {policyValues(permissionCeiling).length === 0 ? (
                <span className="text-xs text-muted-foreground">No capabilities reported.</span>
              ) : null}
            </div>
          </div>
        </div>

        {formError ? (
          <p
            className="rounded-md border border-destructive/30 bg-destructive/5 px-3 py-2 text-xs text-destructive"
            role="alert"
          >
            {formError}
          </p>
        ) : null}
        <div className="flex flex-wrap items-center gap-3">
          <Button type="submit" disabled={setBinding.isPending || !identityId}>
            {setBinding.isPending
              ? 'Saving…'
              : bindingMissing
                ? 'Set Project Agent'
                : 'Save Project Agent'}
          </Button>
          {bindingQuery.data?.chat_id ? (
            <span className="font-mono text-micro text-muted-foreground">
              Timeline {bindingQuery.data.chat_id.slice(0, 8)}…
            </span>
          ) : null}
        </div>
      </form>
    </Card>
  )
}
