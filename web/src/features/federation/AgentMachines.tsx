import { useQuery, useQueryClient } from '@tanstack/react-query'
import { apiFetch } from '@/api/client'
import { qk } from '@/api/query-keys'
import type { Daemon } from '@/types/generated'
import { useUpdateAgent } from '@/api/hooks'
import { useAuthStore } from '@/stores/auth'
import { Button } from '@/components/ui/button'
import { getApiErrorMessage } from '@/lib/api-error'
import type { FederatedAgent } from './types'

export function AgentMachines({ agent }: { agent: FederatedAgent }) {
  const admin = useAuthStore((state) => state.user?.is_admin === true)
  const update = useUpdateAgent()
  const queryClient = useQueryClient()
  const runnablePin = agent.runnable_on?.machines?.find(
    (machine) => machine.daemon_id === agent.daemon_id && agent.daemon_id !== null,
  )
  const pinQuery = useQuery({
    queryKey: [...qk.daemons, agent.daemon_id],
    queryFn: () => apiFetch<Daemon>(`/daemons/${agent.daemon_id}`),
    enabled: admin && Boolean(agent.daemon_id) && !runnablePin,
    retry: false,
  })
  const embeddedPin = pinQuery.data?.machine_id.startsWith('embedded:') === true
  const pinnedRunnable =
    runnablePin ??
    (embeddedPin
      ? agent.runnable_on?.machines?.find((machine) => machine.owner_kind === 'server')
      : undefined)
  const pinnedName =
    pinnedRunnable?.name ??
    (embeddedPin ? 'Server host' : pinQuery.data?.hostname) ??
    (pinQuery.isPending ? 'Loading machine…' : 'Unavailable machine')
  const pinUnrunnable =
    agent.daemon_id !== null && agent.runnable_on?.machines !== undefined && !pinnedRunnable
  const pinStatus =
    pinQuery.data?.status === 'offline'
      ? 'Offline'
      : agent.paused
        ? 'Agent disabled'
        : 'Not currently runnable'

  return (
    <section
      aria-label="Agent machines"
      className="min-w-0 space-y-2 rounded-md border border-border-subtle bg-card p-3 text-xs"
    >
      <p className="break-words">
        <span className="font-medium">Runs on: </span>
        {agent.runnable_on?.machines?.map((machine) => machine.name).join(', ') ||
          (agent.runnable_on
            ? `${agent.runnable_on.count} machines`
            : 'Loading machine availability…')}
      </p>
      {agent.runnable_on?.count === 0 ? (
        <p role="status" className="text-warning">
          This Agent cannot run on any machine. Enable and authenticate its executor.
        </p>
      ) : null}
      {admin ? (
        <div className="flex flex-wrap items-center justify-between gap-2">
          <p className="break-words">
            {agent.daemon_id ? (
              <>
                <span title={agent.daemon_id}>Pinned to {pinnedName}</span>
                {pinUnrunnable ? <span className="text-warning"> · {pinStatus}</span> : null}
              </>
            ) : (
              'No machine pin'
            )}
          </p>
          {agent.daemon_id ? (
            <Button
              size="sm"
              variant="outline"
              disabled={update.isPending}
              onClick={() =>
                update.mutate(
                  { agentId: agent.id, body: { version: agent.version, daemon_id: null } },
                  {
                    onSuccess: () => {
                      void queryClient.invalidateQueries({ queryKey: ['federated-agents'] })
                    },
                  },
                )
              }
            >
              {update.isPending ? 'Clearing…' : 'Clear pin'}
            </Button>
          ) : null}
        </div>
      ) : null}
      {admin && pinUnrunnable && !agent.paused && pinQuery.data?.status !== 'offline' ? (
        <p className="text-muted-foreground">
          This Agent’s executor is unavailable or disabled on the pinned machine.
        </p>
      ) : null}
      {update.isError ? (
        <p role="alert" className="text-destructive">
          {getApiErrorMessage(update.error, 'Could not clear pin')}
        </p>
      ) : null}
    </section>
  )
}
