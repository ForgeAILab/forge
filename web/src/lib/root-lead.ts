import { getApiErrorCode } from '@/lib/api-error'
import type {
  Agent,
  RoleDefinition,
  Task,
  TaskRoleAssignmentResponse,
  WorkflowDefinition,
} from '@/types/generated'

/**
 * The lead of a coordination root, as the web reads it.
 *
 * A coordination root is a root Task with subtasks. Its lead is the Agent on
 * its planning role (`planner`); its `coder` row is the default worker of its
 * subtasks. A subtask passes through the step check `lead_check` after its
 * implementation: the root's lead checks the step, and a root without a lead
 * skips the gate in the same step that entered it.
 *
 * Lead-operation comments (`handoff`, `request_input`) are rendered in
 * `task-comments-panel`. The `lead_input` wait in the Task sidebar links to
 * the question's answer control in Mission Control.
 */

/** The inherited subtask status between implementation and done. */
export const STEP_CHECK_STATE = 'lead_check'
export const STEP_CHECK_LABEL = 'Lead Check'

/** The role a lead holds: the built-in planning role. */
export const LEAD_ROLE = 'planner'
export const LEAD_ROLE_LABEL = 'Lead'
export const DEFAULT_WORKER_LABEL = 'Default worker'

export const LEAD_HELD_BY_ROOT_REASON =
  "The lead is assigned on the parent task; this subtask uses the parent's lead."
export const LEAD_REQUIRES_NATIVE_REASON =
  'A lead must be a native Agent. CLI Agents cannot lead.'

/** A lead must be a native Agent: the server admits `backend_kind == "native"` only. */
export function isLeadCandidate(agent: Pick<Agent, 'backend_kind'>): boolean {
  return agent.backend_kind === 'native'
}

export function leadCandidateDisabledReason(
  agent: Pick<Agent, 'backend_kind'>,
): string | undefined {
  return isLeadCandidate(agent) ? undefined : LEAD_REQUIRES_NATIVE_REASON
}

export type LeadView = {
  assignment: TaskRoleAssignmentResponse
  /** The Task's own row (a root) or its root's row (a subtask). */
  inherited: boolean
}

export function effectiveLead(task: Pick<Task, 'effective_lead'>): LeadView | null {
  const lead = task.effective_lead
  if (!lead?.assignment.assignee_id) return null
  return { assignment: lead.assignment, inherited: lead.source === 'inherited_from_root' }
}

export type RoleSlot = {
  role: RoleDefinition
  /** What the chip is called for this Task. */
  label: string
  /** The lead of a coordination root: native Agents only, no person. */
  lead: boolean
}

/**
 * The role pickers a Task offers, in order.
 *
 * - A subtask never offers the step-check role: the root's lead holds it
 *   (`STEP_CHECK_ROLE_HELD_BY_ROOT`), and the subtask shows that lead read-only.
 * - A coordination root names its `coder` the default worker and its planning
 *   role the lead.
 * - Every other Task keeps its workflow's roles as they are.
 */
export function roleSlots({
  roles,
  coderRole,
  isSubtask,
  isCoordinationRoot,
}: {
  roles: RoleDefinition[]
  coderRole: string
  isSubtask: boolean
  isCoordinationRoot: boolean
}): RoleSlot[] {
  const ordered = [
    ...roles.filter((role) => role.name === coderRole),
    ...roles.filter((role) => role.name !== coderRole),
  ]
  return ordered
    .filter((role) => !(isSubtask && role.name === LEAD_ROLE))
    .map((role) => {
      if (isCoordinationRoot && role.name === LEAD_ROLE) {
        return { role, label: LEAD_ROLE_LABEL, lead: true }
      }
      if (isCoordinationRoot && role.name === coderRole) {
        return { role, label: DEFAULT_WORKER_LABEL, lead: false }
      }
      return { role, label: role.display_name || role.name, lead: false }
    })
}

/** The step check's name in this workflow, or the built-in one. */
export function stepCheckLabel(workflow?: Pick<WorkflowDefinition, 'states'>): string {
  return (
    workflow?.states.find((state) => state.name === STEP_CHECK_STATE)?.display_name ||
    STEP_CHECK_LABEL
  )
}

/**
 * How the step check reads on a subtask that is in it. `null` outside the
 * gate. Without a lead the gate is passed by the system in the same step, so
 * a subtask seen there is in transit: that is a note, never an error.
 */
export function stepCheckNotice(
  task: Pick<Task, 'status' | 'effective_lead'>,
  leadName: string | undefined,
): { message: string; waitingOnLead: boolean } | null {
  if (task.status !== STEP_CHECK_STATE) return null
  const lead = effectiveLead(task)
  if (!lead) {
    return { message: 'No lead on the parent task: this step passes automatically.', waitingOnLead: false }
  }
  return {
    message: `${leadName ?? 'The lead'} checks this step before it is done.`,
    waitingOnLead: true,
  }
}

const ROLE_ASSIGNMENT_ERRORS: Record<string, string> = {
  LEAD_REQUIRES_NATIVE_PROFILE:
    'The lead of a task with subtasks must be a native Agent. Pick a native Agent as the lead; CLI Agents and people cannot lead.',
  LEAD_ROLE_NOT_SUPPORTED:
    "This Project's workflow plans with a custom role, and only the built-in planner role can lead a task with subtasks. This task cannot have a lead.",
  STEP_CHECK_ROLE_HELD_BY_ROOT:
    "A subtask's step check is done by its parent task's lead. Assign the lead on the parent task.",
}

/**
 * The readable message for a refused role assignment, keyed on the server's
 * error code; `undefined` for any other error (the caller shows its own).
 */
export function roleAssignmentErrorMessage(error: unknown): string | undefined {
  const code = getApiErrorCode(error)
  return code ? ROLE_ASSIGNMENT_ERRORS[code] : undefined
}
