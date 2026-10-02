import { useQueryClient } from '@tanstack/react-query'
import { useUpdateAgent } from '@/api/hooks'
import { useAuthStore } from '@/stores/auth'
import { Button } from '@/components/ui/button'
import { getApiErrorMessage } from '@/lib/api-error'
import type { FederatedAgent } from './types'

export function AgentMachines({ agent }: { agent: FederatedAgent }) {
  const admin = useAuthStore((state) => state.user?.is_admin === true)
  const update = useUpdateAgent()
  const queryClient = useQueryClient()
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
          <p className="break-all">Pin: {agent.daemon_id ?? 'None'}</p>
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
      {admin && agent.daemon_id && agent.runnable_on?.count === 0 ? (
        <p className="text-warning">The pinned machine cannot run this Agent’s executor.</p>
      ) : null}
      {update.isError ? (
        <p role="alert" className="text-destructive">
          {getApiErrorMessage(update.error, 'Could not clear pin')}
        </p>
      ) : null}
    </section>
  )
}
