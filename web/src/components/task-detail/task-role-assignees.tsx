import { type AssigneeSelection, AgentAssigneeDropdown } from '@/components/task-controls'
import { Avatar } from '@/components/ui/avatar'
import { cn } from '@/lib/cn'
import {
  LEAD_HELD_BY_ROOT_REASON,
  LEAD_ROLE_LABEL,
  effectiveLead,
  leadCandidateDisabledReason,
  roleSlots,
  stepCheckLabel,
  stepCheckNotice,
} from '@/lib/root-lead'
import { getStateColors } from '@/lib/workflow-utils'
import type {
  Agent,
  RoleDefinition,
  Task,
  TaskRoleAssignmentResponse,
  WorkflowDefinition,
} from '@/types/generated'

type Member = { user_id: string; email: string; display_name?: string | null; role: string }

function selectionOf(assignment?: TaskRoleAssignmentResponse): AssigneeSelection {
  if (!assignment) return { type: 'unassigned' }
  if (assignment.assignee_type === 'agent' && assignment.assignee_id) {
    return { type: 'agent', agentId: assignment.assignee_id }
  }
  if (assignment.assignee_type === 'user') {
    return { type: 'user', userId: assignment.assignee_id ?? 'manual' }
  }
  return { type: 'unassigned' }
}

function agentNameOf(
  assignment: TaskRoleAssignmentResponse | undefined,
  agentName: (agentId?: string | null) => string | undefined | null,
): string | undefined {
  return assignment?.assignee_type === 'agent' && assignment.assignee_id
    ? (agentName(assignment.assignee_id) ?? assignment.assignee_id)
    : undefined
}

/**
 * The role pickers of one Task. A coordination root offers its default worker
 * and its lead (native Agents only); a subtask offers no lead picker and shows
 * its root's lead read-only.
 */
export function TaskRoleAssignees({
  task,
  roles,
  coderRole,
  currentRole,
  isCoordinationRoot,
  agents,
  members,
  disabled,
  agentName,
  onSelect,
}: {
  task: Task
  roles: RoleDefinition[]
  coderRole: string
  currentRole: string | null
  /** A root Task with subtasks: its planning role is the lead. */
  isCoordinationRoot: boolean
  agents: Agent[]
  members?: Member[]
  disabled: boolean
  agentName: (agentId?: string | null) => string | undefined | null
  onSelect: (
    roleName: string,
    current: TaskRoleAssignmentResponse | undefined,
    selection: AssigneeSelection,
  ) => void
}) {
  const isSubtask = task.parent_task_id != null
  const slots = roleSlots({ roles, coderRole, isSubtask, isCoordinationRoot })
  const inheritedLead = isSubtask ? effectiveLead(task) : null
  const inheritedLeadName = inheritedLead
    ? (agentNameOf(inheritedLead.assignment, agentName) ?? 'Assigned')
    : undefined

  return (
    <>
      {slots.map(({ role, label, lead }) => {
        const existing = task.role_assignments.find((row) => row.role_name === role.name)
        return (
          <span key={role.name}>
            <AgentAssigneeDropdown
              agents={agents}
              members={members}
              disabled={disabled}
              fallbackName={agentNameOf(existing, agentName)}
              value={selectionOf(existing)}
              variant="chip"
              roleLabel={label}
              requiredNow={currentRole === role.name}
              agentsOnly={lead}
              agentDisabledReason={lead ? leadCandidateDisabledReason : undefined}
              onChange={(selection) => onSelect(role.name, existing, selection)}
            />
          </span>
        )
      })}
      {inheritedLead && inheritedLeadName ? (
        <span
          data-testid="inherited-lead"
          title={LEAD_HELD_BY_ROOT_REASON}
          className="flex h-7 max-w-full items-center gap-1.5 rounded-full border border-dashed border-input bg-muted/40 pl-1 pr-2 text-xs"
        >
          <Avatar
            name={inheritedLeadName}
            seed={inheritedLead.assignment.assignee_id ?? inheritedLeadName}
            size="xs"
          />
          <span className="shrink-0 font-medium text-muted-foreground">{LEAD_ROLE_LABEL}:</span>
          <span className="min-w-0 max-w-[6.5rem] truncate">{inheritedLeadName}</span>
          <span className="shrink-0 text-muted-foreground">(parent)</span>
        </span>
      ) : null}
    </>
  )
}

/**
 * The step check of a subtask that is in `lead_check`: the gate's name and
 * who checks the step. Nothing is rendered outside the gate. A subtask with no
 * lead is only passing through, so this is a quiet note and never an alert.
 */
export function TaskStepCheck({
  task,
  workflow,
  agentName,
}: {
  task: Task
  workflow?: Pick<WorkflowDefinition, 'states'>
  agentName: (agentId?: string | null) => string | undefined | null
}) {
  const lead = effectiveLead(task)
  const leadName = lead ? (agentNameOf(lead.assignment, agentName) ?? undefined) : undefined
  const notice = stepCheckNotice(task, leadName)
  if (!notice) return null
  const colors = getStateColors(task.status)
  return (
    <div data-testid="step-check" className="space-y-1">
      <span
        className={cn(
          'inline-flex w-fit items-center gap-1.5 rounded-full px-2 py-[2px] text-micro font-semibold',
          colors.bg,
          colors.text,
        )}
      >
        <span className={cn('h-1.5 w-1.5 rounded-full', colors.dot)} />
        {stepCheckLabel(workflow)}
      </span>
      <p className="text-xs text-muted-foreground">{notice.message}</p>
    </div>
  )
}
