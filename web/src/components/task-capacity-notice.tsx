import type { Task } from '@/types/generated'

export function TaskCapacityNotice({ task }: { task: Pick<Task, 'workflow_health'> }) {
  const health = task.workflow_health
  if (
    health?.stale_reason !== 'project_at_capacity' &&
    health?.stale_reason !== 'project_waiting_on_owner' &&
    health?.stale_reason !== 'machine_capacity' &&
    health?.stale_reason !== 'disk_pressure'
  ) {
    return null
  }
  return <p className="mt-2 break-words text-xs text-muted-foreground">{health.message}</p>
}
