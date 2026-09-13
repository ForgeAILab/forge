import { Copy, Trash, Warning } from '@phosphor-icons/react'
import { toast } from 'sonner'
import {
  useCancelTask,
  useDuplicateTask,
  useMembersQuery,
  useProjectAgentsQuery,
  useUpdateTask,
} from '@/api/hooks'
import {
  type AssigneeSelection,
  AgentAssigneeDropdown,
  TaskStatusBadge,
  TaskStatusDropdown,
} from '@/components/task-controls'
import { TaskExecutionObservabilityPanel } from '@/components/task-execution-observability'
import { useProjectTasksForSubtasks } from '@/components/task-detail/task-subtasks-panel'
import { useRolePicker } from '@/components/task-detail/use-role-picker'
import { productTerm } from '@/lib/i18n'
import { Badge } from '@/components/ui/badge'
import { Button } from '@/components/ui/button'
import { Checkbox } from '@/components/ui/checkbox'
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Select } from '@/components/ui/select'
import { Skeleton } from '@/components/ui/skeleton'
import { Textarea } from '@/components/ui/textarea'
import { getApiErrorMessage } from '@/lib/api-error'
import { getHumanGateActions } from '@/lib/gate-actions'
import { useState, type ReactNode } from 'react'
import type {
  Agent,
  Execution,
  Task,
  TaskRoleAssignmentResponse,
  WorkflowDefinition,
} from '@/types/generated'

interface TaskDetailSidebarProps {
  task?: Task
  isLoading: boolean
  workflow?: WorkflowDefinition
  agents: Agent[]
  executions: Execution[]
  availableTransitions: string[]
  transitionPending: boolean
  cancelTask: ReturnType<typeof useCancelTask>
  priorityDraft: string
  setPriorityDraft: (value: string) => void
  savePriority: () => void
  agentName: (agentId?: string | null) => string | undefined | null
  formatDate: (value?: string | null) => string
  onStatusChange: (status: string, reason?: string) => void
  onApproveGate: (stateName: string) => void
  onRejectGate: (stateName: string, reason: string) => void
}

function hasAwaitingHuman(task: Task): task is Task & { awaiting_human: boolean } {
  return 'awaiting_human' in task && typeof task.awaiting_human === 'boolean'
}

function getTaskDetailApiErrorMessage(error: unknown, fallback = 'Request failed'): string {
  return getApiErrorMessage(error, fallback)
}

export function TaskDetailSidebar({
  task,
  isLoading,
  workflow,
  executions,
  availableTransitions,
  transitionPending,
  cancelTask,
  priorityDraft,
  setPriorityDraft,
  savePriority,
  agentName,
  formatDate,
  onStatusChange,
  onApproveGate,
  onRejectGate,
}: TaskDetailSidebarProps) {
  const rolePicker = useRolePicker()
  const duplicateTask = useDuplicateTask()
  const projectId = task?.project_id ?? ''
  const { data: projectAgentsData } = useProjectAgentsQuery(projectId)
  const { data: membersData } = useMembersQuery(projectId)
  const projectTasksQuery = useProjectTasksForSubtasks(projectId, Boolean(task))
  const [pendingSelection, setPendingSelection] = useState<{
    roleName: string
    selection: AssigneeSelection
  } | null>(null)
  const [resetWorkspace, setResetWorkspace] = useState(false)
  const [resetWorktree, setResetWorktree] = useState(false)
  const [rejectingStateName, setRejectingStateName] = useState<string | null>(null)
  const [rejectReasonDraft, setRejectReasonDraft] = useState('')
  const terminal = task?.status === 'done' || task?.status === 'cancelled'
  const effectiveWorkflow = workflow
  const currentRole =
    effectiveWorkflow?.states.find((state) => state.name === task?.status)?.role ?? null
  const coderRole =
    effectiveWorkflow?.states.find((state) => state.name === 'in_progress')?.role ??
    effectiveWorkflow?.roles.find((role) => role.name === 'coder')?.name ??
    'coder'
  const gateActions = getHumanGateActions(task, effectiveWorkflow)

  const openRejectDialog = (stateName: string) => {
    setRejectingStateName(stateName)
    setRejectReasonDraft('')
  }

  const closeRejectDialog = () => {
    setRejectingStateName(null)
    setRejectReasonDraft('')
  }

  const submitRejectDialog = () => {
    if (!rejectingStateName) return
    const reason = rejectReasonDraft.trim()
    if (!reason) {
      toast.error('Rejection reason is required')
      return
    }
    onRejectGate(rejectingStateName, reason)
    closeRejectDialog()
  }
  const effectiveAvailableTransitions = availableTransitions

  const closeReassignmentDialog = () => {
    setPendingSelection(null)
    setResetWorkspace(false)
    setResetWorktree(false)
  }

  const submitRoleSelection = (
    roleName: string,
    selection: AssigneeSelection,
    resetFlags?: { resetWorkspace?: boolean; resetWorktree?: boolean },
  ) => {
    if (!task) return
    rolePicker.submit({
      taskId: task.id,
      roleName,
      selection,
      resetWorkspace: resetFlags?.resetWorkspace,
      resetWorktree: resetFlags?.resetWorktree,
      onError: (error) => toast.error(getTaskDetailApiErrorMessage(error, 'Assignment failed')),
    })
  }

  const handleRoleSelection = (
    roleName: string,
    currentAssignment: TaskRoleAssignmentResponse | undefined,
    selection: AssigneeSelection,
  ) => {
    if (!task || terminal) return
    if (
      roleName === coderRole &&
      !sameSelection(currentAssignment, selection) &&
      hasRunningRoleExecution(task, roleName, currentRole, executions)
    ) {
      setPendingSelection({ roleName, selection })
      setResetWorkspace(false)
      setResetWorktree(false)
      return
    }

    submitRoleSelection(roleName, selection)
  }

  return (
    <>
      <aside className="w-full shrink-0 border-t bg-muted/30 lg:w-72 lg:overflow-y-auto lg:border-l lg:border-t-0">
        {isLoading ? (
          <div className="space-y-4 p-5">
            <Skeleton className="h-8 w-full" />
            <Skeleton className="h-8 w-full" />
            <Skeleton className="h-8 w-full" />
          </div>
        ) : task ? (
          <div className="p-5">
            <SidebarField label={productTerm('phase')}>
              {gateActions ? (
                <div className={gateActions.rejectLabel ? 'mb-2 grid grid-cols-2 gap-2' : 'mb-2'}>
                  <Button
                    className="w-full"
                    disabled={transitionPending}
                    size="sm"
                    onClick={() => onApproveGate(gateActions.stateName)}
                  >
                    {gateActions.approveLabel}
                  </Button>
                  {gateActions.rejectLabel ? (
                    <Button
                      className="w-full"
                      disabled={transitionPending}
                      size="sm"
                      variant="outline"
                      onClick={() => openRejectDialog(gateActions.stateName)}
                    >
                      {gateActions.rejectLabel}
                    </Button>
                  ) : null}
                </div>
              ) : null}
              <span className="block">
                {effectiveAvailableTransitions.length > 0 ? (
                  <TaskStatusDropdown
                    availableStatuses={effectiveAvailableTransitions}
                    className="h-8"
                    disabled={transitionPending}
                    status={task.status}
                    onChange={onStatusChange}
                  />
                ) : (
                  <TaskStatusBadge status={task.status} />
                )}
              </span>
            </SidebarField>

            {hasAwaitingHuman(task) && task.awaiting_human ? (
              <SidebarField label={task.status === 'planning' ? 'Plan' : 'Review'}>
                <Badge className="border-transparent bg-violet-100 text-violet-900 dark:bg-violet-950 dark:text-violet-300 text-[11px]">
                  {task.status === 'planning' ? 'Plan ready - awaiting review' : 'Awaiting human'}
                </Badge>
              </SidebarField>
            ) : null}

            {(() => {
              const roles = effectiveWorkflow?.roles ?? [
                { name: 'coder', display_name: 'Coder', description: '' },
              ]
              const orderedRoles = [
                ...roles.filter((r) => r.name === coderRole),
                ...roles.filter((r) => r.name !== coderRole),
              ]
              return (
                <SidebarField label="Assignees">
                  <div
                    className="flex flex-wrap gap-1.5"
                    title={terminal ? 'task is terminal; cannot reassign' : undefined}
                  >
                    {orderedRoles.map((role) => {
                      const existing = task.role_assignments.find(
                        (ra) => ra.role_name === role.name,
                      )
                      return (
                        <span key={role.name}>
                          <AgentAssigneeDropdown
                            agents={projectAgentsData ?? []}
                            members={membersData}
                            disabled={terminal || rolePicker.isPending}
                            fallbackName={assignmentAgentName(existing, agentName)}
                            value={assignmentSelection(existing)}
                            variant="chip"
                            roleLabel={role.display_name || role.name}
                            requiredNow={currentRole === role.name}
                            onChange={(selection) =>
                              handleRoleSelection(role.name, existing, selection)
                            }
                          />
                        </span>
                      )
                    })}
                  </div>
                </SidebarField>
              )
            })()}

            <SidebarField label="Priority">
              <Input
                type="number"
                className="h-8"
                value={priorityDraft}
                onBlur={savePriority}
                onChange={(e) => setPriorityDraft(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') savePriority()
                  if (e.key === 'Escape') setPriorityDraft(String(task.priority))
                }}
              />
            </SidebarField>

            {task.parent_task_id ? (
              <SidebarField label="Subtask">
                <SubtaskParentField
                  task={task}
                  allProjectTasks={projectTasksQuery.data ?? []}
                  disabled={terminal}
                />
              </SidebarField>
            ) : null}

            <SidebarField label="Created">
              <p className="text-sm text-muted-foreground">{formatDate(task.created_at)}</p>
            </SidebarField>
            <SidebarField label="Updated">
              <p className="text-sm text-muted-foreground">{formatDate(task.updated_at)}</p>
            </SidebarField>
            <SidebarField label="Observability">
              <TaskExecutionObservabilityPanel
                formatDate={formatDate}
                taskId={task.id}
                value={task.execution_observability}
              />
            </SidebarField>

            {task.workspace ? (
              <>
                <SidebarField label="Branch">
                  <p className="font-mono text-xs break-all text-muted-foreground">
                    {task.workspace.branch}
                  </p>
                </SidebarField>
                <SidebarField label="Path">
                  <p className="font-mono text-xs break-all text-muted-foreground">
                    {task.workspace.worktree_path}
                  </p>
                </SidebarField>
              </>
            ) : null}

            {task.status === 'done' ? (
              <p className="mt-4 text-xs text-muted-foreground">Workspace cleaned after merge.</p>
            ) : null}

            <div className="space-y-2 py-3">
              {task.status !== 'done' && task.status !== 'cancelled' ? (
                <Button
                  variant="outline"
                  size="sm"
                  className="w-full gap-1.5 text-destructive hover:bg-destructive/10 hover:text-destructive"
                  disabled={cancelTask.isPending}
                  onClick={() => {
                    cancelTask.mutate(task.id, {
                      onError: (error) => toast.error(getApiErrorMessage(error, 'Cancel failed')),
                    })
                  }}
                >
                  <Trash size={14} />
                  Cancel task
                </Button>
              ) : null}
              {(task.status === 'done' || task.status === 'cancelled') && (
                <Button
                  variant="outline"
                  size="sm"
                  className="w-full gap-1.5"
                  disabled={duplicateTask.isPending}
                  onClick={() => {
                    duplicateTask.mutate(task.id, {
                      onSuccess: () => toast.success('Task duplicated to Todo'),
                      onError: (error) =>
                        toast.error(getApiErrorMessage(error, 'Duplicate failed')),
                    })
                  }}
                >
                  <Copy size={14} />
                  {duplicateTask.isPending ? 'Duplicating...' : 'Duplicate to Todo'}
                </Button>
              )}
            </div>
          </div>
        ) : null}
      </aside>

      <Dialog
        open={rejectingStateName != null}
        onOpenChange={(open) => {
          if (!open) closeRejectDialog()
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Reject Gate</DialogTitle>
          </DialogHeader>
          <Textarea
            value={rejectReasonDraft}
            onChange={(event) => setRejectReasonDraft(event.target.value)}
            placeholder="Describe what needs to change"
            rows={4}
          />
          <DialogFooter>
            <Button variant="outline" onClick={closeRejectDialog}>
              Cancel
            </Button>
            <Button
              disabled={transitionPending || !rejectReasonDraft.trim()}
              onClick={submitRejectDialog}
            >
              Reject
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>

      <Dialog
        open={Boolean(pendingSelection)}
        onOpenChange={(open) => {
          if (!open) closeReassignmentDialog()
        }}
      >
        <DialogContent>
          <DialogHeader>
            <DialogTitle className="leading-6">
              The current executor run will be cancelled and the task will move back to Todo. You
              will need to claim it again to resume.
            </DialogTitle>
          </DialogHeader>

          <div className="mt-5 space-y-4">
            <label className="flex gap-3 rounded-md border p-3">
              <Checkbox
                checked={resetWorkspace}
                className="mt-1 h-4 w-4 accent-primary"
                onChange={(event) => {
                  const checked = event.target.checked
                  setResetWorkspace(checked)
                  if (checked) setResetWorktree(false)
                }}
              />
              <span className="min-w-0">
                <span className="block text-sm font-medium">Reset workspace</span>
                <span className="mt-1 block text-xs leading-5 text-muted-foreground">
                  Tear down the workspace and create a fresh one from the base branch on the next
                  claim. Commits on the task branch will be lost.
                </span>
              </span>
            </label>

            <label className="flex gap-3 rounded-md border p-3">
              <Checkbox
                checked={resetWorktree}
                className="mt-1 h-4 w-4 accent-primary disabled:opacity-50"
                disabled={resetWorkspace}
                onChange={(event) => setResetWorktree(event.target.checked)}
              />
              <span className="min-w-0">
                <span className="flex flex-wrap items-center gap-2 text-sm font-medium">
                  Reset worktree
                  {resetWorkspace ? (
                    <span className="text-xs font-normal text-muted-foreground">
                      Already implied by Reset workspace
                    </span>
                  ) : null}
                </span>
                <span className="mt-1 block text-xs leading-5 text-muted-foreground">
                  Discard uncommitted changes inside the worktree, keep commits on the task branch.
                </span>
              </span>
            </label>
          </div>

          {resetWorkspace ? (
            <div className="mt-5 flex gap-2 rounded-md border border-destructive/40 bg-destructive/10 p-3 text-sm text-destructive">
              <Warning size={18} weight="fill" className="mt-0.5 shrink-0" />
              <span>
                This will discard all commits on the task branch - the prior assignees work will be
                lost.
              </span>
            </div>
          ) : null}

          <DialogFooter className="mt-6">
            <Button variant="outline" onClick={closeReassignmentDialog}>
              Cancel
            </Button>
            <Button
              disabled={rolePicker.isPending}
              onClick={() => {
                if (!pendingSelection) return
                submitRoleSelection(pendingSelection.roleName, pendingSelection.selection, {
                  resetWorkspace,
                  resetWorktree,
                })
                closeReassignmentDialog()
              }}
            >
              Confirm reassignment
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </>
  )
}

function SidebarField({
  label,
  requiredNow,
  children,
}: {
  label: string
  requiredNow?: boolean
  children: ReactNode
}) {
  return (
    <div className="py-3 [&:not(:last-child)]:border-b">
      <p className="mb-1.5 flex items-center gap-1.5 text-xs font-medium uppercase tracking-wide text-muted-foreground">
        <span>{label}</span>
        {requiredNow ? (
          <span className="rounded border px-1 py-0.5 text-micro normal-case tracking-normal">
            required now
          </span>
        ) : null}
      </p>
      {children}
    </div>
  )
}

function assignmentSelection(assignment?: TaskRoleAssignmentResponse): AssigneeSelection {
  if (!assignment) return { type: 'unassigned' }
  if (assignment.assignee_type === 'agent' && assignment.assignee_id) {
    return { type: 'agent', agentId: assignment.assignee_id }
  }
  if (assignment.assignee_type === 'user') {
    return { type: 'user', userId: assignment.assignee_id ?? 'manual' }
  }
  return { type: 'unassigned' }
}

function assignmentAgentName(
  assignment: TaskRoleAssignmentResponse | undefined,
  agentName: (agentId?: string | null) => string | undefined | null,
): string | undefined {
  return assignment?.assignee_type === 'agent' && assignment.assignee_id
    ? (agentName(assignment.assignee_id) ?? assignment.assignee_id)
    : undefined
}

function sameSelection(
  assignment: TaskRoleAssignmentResponse | undefined,
  selection: AssigneeSelection,
): boolean {
  if (!assignment) return selection.type === 'unassigned'
  if (selection.type === 'agent') {
    return assignment.assignee_type === 'agent' && assignment.assignee_id === selection.agentId
  }
  if (selection.type === 'user') {
    return (
      assignment.assignee_type === 'user' &&
      (assignment.assignee_id ?? 'manual') === selection.userId
    )
  }
  return false
}

function SubtaskParentField({
  task,
  allProjectTasks,
  disabled,
}: {
  task: Task
  allProjectTasks: Task[]
  disabled: boolean
}) {
  const updateTask = useUpdateTask()
  const rootTasks = allProjectTasks.filter((t) => t.parent_task_id == null && t.id !== task.id)
  const currentParent = allProjectTasks.find((t) => t.id === task.parent_task_id)

  const handleParentChange = (newParentId: string) => {
    if (newParentId === task.parent_task_id) return
    updateTask.mutate(
      { taskId: task.id, body: { parent_task_id: newParentId, version: task.version } },
      { onError: (err) => toast.error(getApiErrorMessage(err, 'Failed to update parent')) },
    )
  }

  return (
    <div className="space-y-2 text-sm">
      <div className="flex items-center justify-between gap-3">
        <span className="text-muted-foreground">Order</span>
        <span>{task.subtask_order ?? '-'}</span>
      </div>
      <div>
        <p className="mb-1 text-xs text-muted-foreground">Parent task</p>
        <Select
          className="h-8 text-xs"
          disabled={disabled || updateTask.isPending || rootTasks.length === 0}
          value={task.parent_task_id ?? ''}
          options={[
            ...(currentParent && !rootTasks.find((t) => t.id === currentParent.id)
              ? [{ value: currentParent.id, label: currentParent.title }]
              : []),
            ...rootTasks.map((t) => ({ value: t.id, label: t.title })),
          ]}
          onChange={(v) => v && handleParentChange(v)}
        />
      </div>
    </div>
  )
}

function hasRunningRoleExecution(
  task: Task,
  roleName: string,
  currentRole: string | null,
  executions: Execution[],
): boolean {
  if (task.status === 'done' || task.status === 'cancelled' || currentRole !== roleName) {
    return false
  }
  return executions.some((execution) => {
    const status = execution.status as string
    return (
      (status === 'pending' || status === 'running') &&
      (execution.role === roleName || execution.role === 'executor')
    )
  })
}
