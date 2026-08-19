import type {
  StateDefinition,
  Task,
  WorkflowDefinition,
} from '@/types/generated'
import { getBlockingAnnotation, outgoingWorkflowEdges, type WorkflowEdge } from '@/lib/workflow-utils'

type TaskWithHumanGate = Task & { awaiting_human?: boolean }

export type HumanGateActions = {
  stateName: string
  approveLabel: string
  rejectLabel?: string
}

export function getHumanGateActions(
  task: TaskWithHumanGate | undefined,
  workflow: WorkflowDefinition | undefined,
): HumanGateActions | null {
  if (!task || !workflow) return null
  const blockingAnnotation = getBlockingAnnotation(task)
  if (
    task.status === 'blocked' &&
    blockingAnnotation?.recovery_actions?.includes('retry_hook')
  ) {
    return {
      stateName: task.status,
      approveLabel:
        blockingAnnotation.type === 'target_repo_dirty' ? 'Resume Merge' : 'Resume Task',
    }
  }
  // A workflow guard rejected the agent's completed turn (e.g. the plan
  // checklist is still open). The task stays in its active state but nothing
  // advances until someone acts — send the agent back to finish.
  if (
    blockingAnnotation?.type === 'workflow_guard_rejected' &&
    blockingAnnotation.recovery_actions?.includes('resume_session')
  ) {
    return {
      stateName: task.status,
      approveLabel: 'Send Back to Agent',
    }
  }
  if (task.blocked || task.failed) return null

  const state = workflow.states.find((candidate) => candidate.name === task.status)
  if (!state || state.kind !== 'gate') return null
  if (!isHumanHeldGate(task, state)) return null

  const approveTransition = gateApproveTransition(workflow, state.name)
  if (!approveTransition) return null

  const rejectTransition = gateRejectTransition(workflow, state.name, approveTransition)
  const stateLabel = state.display_name || formatStateName(state.name)
  return {
    stateName: state.name,
    approveLabel: actionLabel(state.gate_config?.approve_label, `Approve ${stateLabel}`),
    rejectLabel: rejectTransition
      ? actionLabel(state.gate_config?.reject_label, `Reject ${stateLabel}`)
      : undefined,
  }
}

function isHumanHeldGate(task: TaskWithHumanGate, state: StateDefinition): boolean {
  if (task.awaiting_human) return true
  if (!state.role) return false
  return task.role_assignments.some(
    (assignment) => assignment.role_name === state.role && assignment.assignee_type === 'user',
  )
}

function gateApproveTransition(
  workflow: WorkflowDefinition,
  stateName: string,
): WorkflowEdge | undefined {
  const outgoing = outgoingTransitions(workflow, stateName)
  const rejectTargets = explicitRejectTargets(workflow, stateName)
  return (
    outgoing.find((transition) => transition.trigger === 'accept') ??
    outgoing.find(
      (transition) =>
        stateKind(workflow, transition.to) === 'active' && !rejectTargets.has(transition.to),
    ) ??
    outgoing.find((transition) => !rejectTargets.has(transition.to))
  )
}

// Rejection semantics come only from explicit workflow data: a reject/fail
// trigger edge or gate_config.reject_target. State names carry no meaning.
function explicitRejectTargets(workflow: WorkflowDefinition, stateName: string): Set<string> {
  const targets = new Set<string>()
  const configured = workflow.states.find((candidate) => candidate.name === stateName)
    ?.gate_config?.reject_target
  if (configured) targets.add(configured)
  for (const transition of outgoingTransitions(workflow, stateName)) {
    if (transition.trigger === 'reject' || transition.trigger === 'fail') {
      targets.add(transition.to)
    }
  }
  return targets
}

function gateRejectTransition(
  workflow: WorkflowDefinition,
  stateName: string,
  approveTransition: WorkflowEdge,
): WorkflowEdge | undefined {
  const outgoing = outgoingTransitions(workflow, stateName)
  const rejectTransition = outgoing.find((transition) => transition.trigger === 'reject')
  if (rejectTransition) return rejectTransition

  const configuredRejectTarget = workflow.states.find(
    (candidate) => candidate.name === stateName,
  )?.gate_config?.reject_target
  if (configuredRejectTarget) {
    const configuredTransition = outgoing.find(
      (transition) => transition.to === configuredRejectTarget,
    )
    if (configuredTransition) return configuredTransition
  }

  const rejectTargets = explicitRejectTargets(workflow, stateName)
  return outgoing.find(
    (transition) => transition.to !== approveTransition.to && rejectTargets.has(transition.to),
  )
}

function outgoingTransitions(workflow: WorkflowDefinition, stateName: string): WorkflowEdge[] {
  return outgoingWorkflowEdges(workflow, stateName)
}

function stateKind(workflow: WorkflowDefinition, stateName: string): string | undefined {
  return workflow.states.find((state) => state.name === stateName)?.kind
}

function actionLabel(configured: string | null | undefined, fallback: string): string {
  const trimmed = configured?.trim()
  return trimmed || fallback
}

function formatStateName(stateName: string): string {
  return stateName.replace(/[-_]+/g, ' ').replace(/\b\w/g, (letter) => letter.toUpperCase())
}
