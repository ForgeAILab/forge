import type { Task, WorkflowDefinition } from '@/types/generated'

export type HumanGateActions = { stateName: string; approveLabel: string; rejectLabel?: string }

// Presentation only: validity and labels come from the server's offers.
export function getHumanGateActions(task: Task | undefined, _workflow?: WorkflowDefinition): HumanGateActions | null {
  const approve = task?.available_actions?.find((offer) => offer.action.verb === 'approve')
  const reject = task?.available_actions?.find((offer) => offer.action.verb === 'send_back')
  if (!task || !approve) return null
  return { stateName: task.status, approveLabel: approve.label, rejectLabel: reject?.label }
}
