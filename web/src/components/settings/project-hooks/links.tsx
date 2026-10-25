// Project hook Task and execution links.

import { Link } from '@tanstack/react-router'
import type { ProjectHookRunResponse } from '@/types/generated/bindings/ProjectHookRunResponse'
import { compactId } from './utils'

export function TaskLink({ taskId }: { taskId: string | null }) {
  if (!taskId) return <span className="text-muted-foreground">-</span>
  return (
    <Link
      to="/tasks/$taskId"
      params={{ taskId }}
      className="font-mono text-xs text-primary hover:underline"
      title={taskId}
    >
      {compactId(taskId)}
    </Link>
  )
}

export function ExecutionLink({ run }: { run: ProjectHookRunResponse }) {
  if (!run.execution_id) return <span className="text-muted-foreground">-</span>
  const taskId = run.automation_task_id ?? run.source_task_id
  if (!taskId) {
    return (
      <span className="font-mono text-xs text-muted-foreground" title={run.execution_id}>
        {compactId(run.execution_id)}
      </span>
    )
  }
  return (
    <Link
      to="/tasks/$taskId/executions/$executionId"
      params={{ taskId, executionId: run.execution_id }}
      className="font-mono text-xs text-primary hover:underline"
      title={run.execution_id}
    >
      {compactId(run.execution_id)}
    </Link>
  )
}
