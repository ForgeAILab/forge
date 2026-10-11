import { useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { qk } from '@/api/query-keys'
import { useNavigate } from '@tanstack/react-router'
import { toast } from 'sonner'
import { useLaunchExecution } from '@/api/hooks'
import type { ExecutionConfigValue } from '@/components/execution-config/ExecutionConfigBar'
import { Button } from '@/components/ui/button'
import { getApiErrorMessage } from '@/lib/api-error'
import { TaskLaunchDialog } from '@/pages/task-detail/TaskLaunchDialog'

/** Interactive sessions use their retained resource, independently of Task offers. */
export function TaskInteractiveLaunch({ taskId }: { taskId: string }) {
  const [open, setOpen] = useState(false)
  return (
    <>
      <Button size="sm" variant="outline" onClick={() => setOpen(true)}>
        Open interactive
      </Button>
      {open ? <InteractiveLaunchDialog taskId={taskId} onClose={() => setOpen(false)} /> : null}
    </>
  )
}
function InteractiveLaunchDialog({ taskId, onClose }: { taskId: string; onClose: () => void }) {
  const launch = useLaunchExecution()
  const queryClient = useQueryClient()
  const navigate = useNavigate()
  const submit = (config: ExecutionConfigValue, summary: string) => {
    if (!config.agentId) return
    launch.mutate(
      {
        taskId,
        body: {
          agent_id: config.agentId,
          summary: summary.trim() || null,
          overrides: config.overrides,
        },
      },
      {
        onSuccess: (response) => {
          void queryClient.invalidateQueries({ queryKey: qk.taskDetail(taskId) })
          onClose()
          void navigate({
            to: '/tasks/$taskId/executions/$executionId',
            params: { taskId, executionId: response.data.execution.id },
          })
        },
        onError: (error) => toast.error(getApiErrorMessage(error, 'Interactive launch failed')),
      },
    )
  }
  return (
    <TaskLaunchDialog
      open
      onOpenChange={(open) => {
        if (!open) onClose()
      }}
      isPending={launch.isPending}
      onSubmit={submit}
    />
  )
}
