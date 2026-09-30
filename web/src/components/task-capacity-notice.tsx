import type { Task } from '@/types/generated'

export function TaskCapacityNotice({ task }: { task: Task }) {
  const health = task.workflow_health
  if (
    health?.stale_reason !== 'project_at_capacity' &&
    health?.stale_reason !== 'project_waiting_on_owner'
  ) {
    return null
  }
  return <p className="mt-2 break-words text-xs text-muted-foreground">{health.message}</p>
}
