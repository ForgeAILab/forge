import { Badge } from '@/components/ui/badge'
import type { TaskType } from '@/types/generated'

const labels: Record<Exclude<TaskType, 'task'>, string> = {
  planning_task: 'Planning Task',
  discovery: 'Discovery',
  sub_task: 'Sub Task',
}

export function TaskTypeBadge({ taskType }: { taskType: TaskType }) {
  if (taskType === 'task') return null

  return (
    <Badge variant="outline" className="shrink-0">
      <span className="sr-only">Task type: </span>
      {labels[taskType]}
    </Badge>
  )
}
