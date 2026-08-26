import { useEffect, useState } from 'react'
import { Link } from '@tanstack/react-router'
import { FastForwardIcon as FastForward } from '@phosphor-icons/react'
import { toast } from 'sonner'
import { useMembersQuery, useProjectAgentsQuery } from '@/api/hooks'
import { ErrorBanner } from '@/components/error-banner'
import { PlanChecklist } from '@/components/plan-checklist'
import {
  type AssigneeSelection,
  AgentAssigneeDropdown,
  TaskStatusDropdown,
} from '@/components/task-controls'
import { TaskExecutionObservabilityPanel } from '@/components/task-execution-observability'
import { TaskExecutionApprovalNotice } from '@/features/project-execution/TaskExecutionApprovalNotice'
import { TaskBlockingBanner } from '@/components/task-detail/task-blocking-banner'
import { WorkflowExceptionPanel } from '@/components/task-detail/workflow-exception-panel'
import { TaskExternalLinks } from '@/components/task-detail/task-external-links'
import { TaskPrSummaryCard } from '@/components/task-detail/task-pr-summary-card'
import { WorkflowHealthBadge } from '@/components/workflow-health-badge'
import { Button } from '@/components/ui/button'
import { CollapsibleSection } from '@/components/ui/collapsible-section'
import {
  Dialog,
  DialogContent,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Skeleton } from '@/components/ui/skeleton'
import { productTerm } from '@/lib/i18n'
import { Textarea } from '@/components/ui/textarea'
import { Tooltip } from '@/components/ui/tooltip'
import { MarkdownEditor, MarkdownView } from '@/components/ui/markdown-editor'
import { cn } from '@/lib/cn'
import type { HumanGateActions } from '@/lib/gate-actions'
import type {
  Agent,
  Execution,
  ExecutionAction,
  RecoveryAction,
  RoleDefinition,
  Task,
  WorkflowExceptionAction,
} from '@/types/generated'
import { getBlockingAnnotation } from '@/lib/workflow-utils'
import {
  assignmentAgentName,
  budgetValue,
  formatDate,
  readTaskStateConfig,
  retryBudgetFromConfig,
  stripRunSuffix,
} from './utils'

interface TaskOverviewPanelProps {
  task: Task | undefined
  isLoading: boolean
  isError: boolean
  error: Error | null
  onRetryLoad: () => void
  // pending states
  updatePending: boolean
  recoverPending: boolean
  transitionPending: boolean
  gateDecisionPending: boolean
  advancePending: boolean
  rolePickerPending: boolean
  cancelPending: boolean
  duplicatePending: boolean
  executionActionPending: boolean
  // computed
  errorInfo: { tone: 'timeout' | 'crash' | 'workspace'; message: string } | undefined
  gateActions: HumanGateActions | null
  gateDecisionDisabledReason: string | undefined
  availableTransitions: string[]
  managedStatusDisabledReason: string | undefined
  reviewDisabledReason: string | undefined
  manualAdvanceTarget: string | null
  manualAdvanceLabel: string | null
  terminal: boolean
  currentRole: string | null
  assignableRoles: RoleDefinition[]
  agents: Agent[]
  executions: Execution[]
  canLaunch: boolean
  hasAgents: boolean
  runSuffix: string
  workflowRetryBudgets: Record<string, unknown> | undefined
  agentName: (agentId?: string | null) => string | undefined
  // handlers
  onUpdateTitle: (title: string) => void
  onUpdateDescription: (description: string | null) => void
  onUpdatePriority: (priority: number) => void
  onRecover: (action: RecoveryAction, input?: { reason?: string; context?: string }) => void
  onOpenWorkflowExceptionAction: (action: WorkflowExceptionAction) => void
  onApproveGate: (stateName: string) => void
  onRejectGate: (stateName: string, reason: string) => void
  onStatusChange: (status: string) => void
  onManualAdvance: () => void
  onAssigneeChange: (roleName: string, selection: AssigneeSelection) => void
  onCancelTask: () => void
  onDuplicateTask: () => void
  onOpenLaunchDialog: () => void
  onContinueSession: (executionId: string) => void
  onStopExecution: (executionId: string) => void
  onReExecuteExecution: (executionId: string) => void
  onSaveRetryBudgets: (
    review: number | undefined,
    mergeFix: number | undefined,
    execution: number | undefined,
  ) => void
}

export function TaskOverviewPanel({
  task,
  isLoading,
  isError,
  error,
  onRetryLoad,
  updatePending,
  recoverPending,
  transitionPending,
  gateDecisionPending,
  advancePending,
  rolePickerPending,
  cancelPending,
  duplicatePending,
  executionActionPending,
  errorInfo,
  gateActions,
  gateDecisionDisabledReason,
  availableTransitions,
  managedStatusDisabledReason,
  reviewDisabledReason,
  manualAdvanceTarget,
  manualAdvanceLabel,
  terminal,
  currentRole,
  assignableRoles,
  executions,
  canLaunch,
  hasAgents,
  runSuffix,
  workflowRetryBudgets,
  agentName,
  onUpdateTitle,
  onUpdateDescription,
  onUpdatePriority,
  onRecover,
  onOpenWorkflowExceptionAction,
  onApproveGate,
  onRejectGate,
  onStatusChange,
  onManualAdvance,
  onAssigneeChange,
  onCancelTask,
  onDuplicateTask,
  onOpenLaunchDialog,
  onContinueSession,
  onStopExecution,
  onReExecuteExecution,
  onSaveRetryBudgets,
}: TaskOverviewPanelProps) {
  const [editingTitle, setEditingTitle] = useState(false)
  const [titleDraft, setTitleDraft] = useState('')
  const [editingDescription, setEditingDescription] = useState(false)
  const [descriptionDraft, setDescriptionDraft] = useState('')
  const [priorityDraft, setPriorityDraft] = useState('')
  const [reviewRetryOverride, setReviewRetryOverride] = useState('')
  const [mergeFixRetryOverride, setMergeFixRetryOverride] = useState('')
  const [executionRetryOverride, setExecutionRetryOverride] = useState('')
  const [rejectingStateName, setRejectingStateName] = useState<string | null>(null)
  const [rejectReasonDraft, setRejectReasonDraft] = useState('')

  const projectId = task?.project_id ?? ''
  const { data: projectAgentsData } = useProjectAgentsQuery(projectId)
  const { data: membersData } = useMembersQuery(projectId)
  const workflowExceptionActions = task?.workflow_exception?.actions ?? []
  const executionActions = Array.isArray(task?.execution_actions) ? task.execution_actions : null
  const runningExecutionId =
    executions.find((execution) => execution.status === 'running')?.id ?? null
  const blockingAnnotation = task ? getBlockingAnnotation(task) : null

  useEffect(() => {
    if (!task) return
    const timeout = window.setTimeout(() => {
      setTitleDraft(task.title)
      setDescriptionDraft(task.description ?? '')
      setPriorityDraft(String(task.priority))
      const taskStateConfig = readTaskStateConfig(task)
      setReviewRetryOverride(retryBudgetFromConfig(taskStateConfig, 'review'))
      setMergeFixRetryOverride(retryBudgetFromConfig(taskStateConfig, 'merge_fix'))
      setExecutionRetryOverride(retryBudgetFromConfig(taskStateConfig, 'execution'))
    }, 0)
    return () => window.clearTimeout(timeout)
  }, [task])

  const effectiveRetryBudget = (
    key: 'review' | 'merge_fix' | 'execution',
    draft: string,
    fallback: number,
  ) => {
    const taskOverride = budgetValue(Number(draft.trim()))
    if (draft.trim() && taskOverride !== undefined) {
      return `(effective: ${taskOverride} — task override)`
    }
    const workflowDefault = budgetValue(workflowRetryBudgets?.[key])
    if (workflowDefault !== undefined) return `(effective: ${workflowDefault} — workflow default)`
    return `(effective: ${fallback} — system default)`
  }

  const handleSaveTitle = () => {
    if (!task) return
    const title = titleDraft.trim()
    if (!title || title === task.title) {
      setTitleDraft(task.title)
      setEditingTitle(false)
      return
    }
    onUpdateTitle(title)
    setEditingTitle(false)
  }

  const handleCancelTitleEdit = () => {
    if (!task) return
    setTitleDraft(task.title)
    setEditingTitle(false)
  }

  const handleSaveDescription = () => {
    if (!task) return
    const description = descriptionDraft.trim()
    const normalizedCurrent = (task.description ?? '').trim()
    if (description === normalizedCurrent) {
      setDescriptionDraft(task.description ?? '')
      setEditingDescription(false)
      return
    }
    onUpdateDescription(description.length > 0 ? description : null)
    setEditingDescription(false)
  }

  const handleCancelDescriptionEdit = () => {
    if (!task) return
    setDescriptionDraft(task.description ?? '')
    setEditingDescription(false)
  }

  const handleSavePriority = () => {
    if (!task) return
    const priority = Number(priorityDraft)
    if (!Number.isFinite(priority) || priority === task.priority) {
      setPriorityDraft(String(task.priority))
      return
    }
    onUpdatePriority(priority)
  }

  const handleSaveRetryBudgets = () => {
    const parseDraft = (label: string, draft: string) => {
      if (!draft.trim()) return undefined
      const value = Number(draft.trim())
      if (!Number.isInteger(value) || value < 0) {
        toast.error(`${label} must be blank or a whole number 0 or greater`)
        return null
      }
      return value
    }
    const review = parseDraft('Review retries', reviewRetryOverride)
    const mergeFix = parseDraft('Merge-fix retries', mergeFixRetryOverride)
    const execution = parseDraft(`${productTerm('run')} retries`, executionRetryOverride)
    if (review === null || mergeFix === null || execution === null) {
      return
    }
    onSaveRetryBudgets(review, mergeFix, execution)
  }

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

  const runExecutionAction = (action: ExecutionAction) => {
    if (!task || !action.enabled || executionActionPending) return
    switch (action.action) {
      case 'manual_launch':
        onOpenLaunchDialog()
        return
      case 'session_follow_up':
        if (action.target_execution_id) onContinueSession(action.target_execution_id)
        return
      case 'workflow_resume':
        onRecover('resume_session')
        return
      case 're_execute':
        if (blockingAnnotation?.recovery_actions?.includes('reexecute')) {
          onRecover('reexecute')
          return
        }
        if (action.target_execution_id) onReExecuteExecution(action.target_execution_id)
        return
      case 'stop_execution': {
        const targetId = action.target_execution_id ?? runningExecutionId
        if (targetId) onStopExecution(targetId)
        return
      }
      case 'cancel_task':
        onCancelTask()
        return
    }
  }

  const executionActionDisabled = (action: ExecutionAction) => {
    if (executionActionPending || recoverPending || cancelPending) return true
    if (!action.enabled) return true
    if (action.action === 'manual_launch' && !hasAgents) return true
    if (action.action === 'session_follow_up' && !action.target_execution_id) return true
    if (action.action === 're_execute' && !action.target_execution_id) return true
    if (action.action === 'stop_execution' && !(action.target_execution_id ?? runningExecutionId)) {
      return true
    }
    return false
  }

  const executionActionDisabledReason = (action: ExecutionAction) => {
    if (action.action === 'manual_launch' && !hasAgents) return 'No agents available'
    if (action.action === 'stop_execution' && !(action.target_execution_id ?? runningExecutionId)) {
      return action.disabled_reason ?? `No running ${productTerm('run').toLowerCase()}`
    }
    return action.disabled_reason ?? ''
  }

  const visibleExecutionActions = terminal ? [] : (executionActions ?? [])
  const showFallbackActions = !terminal && executionActions == null
  const showActionsSection = visibleExecutionActions.length > 0 || showFallbackActions

  const onTitleKeyDown = (event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'Enter') {
      event.preventDefault()
      handleSaveTitle()
    }
    if (event.key === 'Escape') {
      handleCancelTitleEdit()
    }
  }

  return (
    <div className="px-8 py-6">
      <div className="max-w-[760px] space-y-6">
        {isLoading ? (
          <div className="space-y-3">
            <Skeleton className="h-8 w-3/4" />
            <Skeleton className="h-24 w-full" />
          </div>
        ) : isError ? (
          <ErrorBanner error={error} fallback="Task failed to load" onRetry={onRetryLoad} />
        ) : task ? (
          <>
            {editingTitle ? (
              <div className="space-y-2">
                <Input
                  autoFocus
                  value={titleDraft}
                  onChange={(e) => setTitleDraft(e.target.value)}
                  onKeyDown={onTitleKeyDown}
                />
                <div className="flex items-center gap-2">
                  <Button
                    size="sm"
                    disabled={updatePending || !titleDraft.trim()}
                    onClick={handleSaveTitle}
                  >
                    Save
                  </Button>
                  <Button size="sm" variant="outline" type="button" onClick={handleCancelTitleEdit}>
                    Cancel
                  </Button>
                </div>
              </div>
            ) : (
              <button
                className="-mx-1 block w-full rounded-md px-1 py-0.5 text-left text-xl font-semibold hover:bg-accent"
                type="button"
                onClick={() => setEditingTitle(true)}
              >
                {stripRunSuffix(task.title, runSuffix)}
              </button>
            )}

            {task.workflow_health ? (
              <div className="flex flex-wrap items-center gap-2">
                <WorkflowHealthBadge health={task.workflow_health} />
                {task.workflow_health.message ? (
                  <span className="text-sm text-muted-foreground">
                    {task.workflow_health.message}
                  </span>
                ) : null}
                {task.workflow_health.execution_id ? (
                  <Link
                    to="/tasks/$taskId/executions/$executionId"
                    params={{ taskId: task.id, executionId: task.workflow_health.execution_id }}
                    className="font-mono text-xs text-primary hover:underline"
                  >
                    {productTerm('run')} {task.workflow_health.execution_id.slice(0, 8)}
                  </Link>
                ) : null}
                {task.workflow_health.review_id ? (
                  <Link
                    to="/tasks/$taskId/$tab"
                    params={{ taskId: task.id, tab: 'review' }}
                    className="font-mono text-xs text-primary hover:underline"
                  >
                    Review {task.workflow_health.review_id.slice(0, 8)}
                  </Link>
                ) : null}
              </div>
            ) : null}

            <TaskExecutionApprovalNotice
              projectId={task.project_id}
              blocker={task.execution_blocker}
              evidence={task.execution_evidence}
            />

            {errorInfo && !task.workflow_exception ? (
              <div
                className={cn(
                  'rounded-lg border p-3 text-sm',
                  errorInfo.tone === 'workspace'
                    ? 'border-amber-300 bg-amber-50 text-amber-900 dark:border-amber-700 dark:bg-amber-950 dark:text-amber-300'
                    : errorInfo.tone === 'crash'
                      ? 'border-red-300 bg-red-50 text-red-900 dark:border-red-800 dark:bg-red-950 dark:text-red-300'
                      : 'border-orange-300 bg-orange-50 text-orange-900 dark:border-orange-700 dark:bg-orange-950 dark:text-orange-300',
                )}
              >
                {errorInfo.message}
              </div>
            ) : null}

            <WorkflowExceptionPanel
              task={task}
              actions={workflowExceptionActions}
              recoverPending={recoverPending}
              terminal={terminal}
              cancelPending={cancelPending}
              onRecover={onRecover}
              onOpenInteractive={onOpenWorkflowExceptionAction}
              onCancelTask={onCancelTask}
            />
            {!task.workflow_exception ? <TaskBlockingBanner task={task} /> : null}
            <TaskPrSummaryCard task={task} />

            {task.plan_progress || task.plan_artifact ? (
              <PlanChecklist progress={task.plan_progress} artifact={task.plan_artifact} />
            ) : null}

            <div>
              <p className="mb-2 text-xs font-medium uppercase tracking-wide text-muted-foreground">
                Description
              </p>
              {editingDescription ? (
                <div className="space-y-2">
                  <MarkdownEditor
                    autoFocus
                    minHeight="112px"
                    value={descriptionDraft}
                    onChange={setDescriptionDraft}
                    onKeyDown={(e) => {
                      if (e.key === 'Escape') handleCancelDescriptionEdit()
                    }}
                  />
                  <div className="flex items-center gap-2">
                    <Button size="sm" disabled={updatePending} onClick={handleSaveDescription}>
                      Save
                    </Button>
                    <Button
                      size="sm"
                      variant="outline"
                      type="button"
                      onClick={handleCancelDescriptionEdit}
                    >
                      Cancel
                    </Button>
                  </div>
                </div>
              ) : task.description?.trim() ? (
                <button
                  className="block w-full rounded-lg border border-dashed p-3 text-left transition-colors hover:border-border hover:bg-accent"
                  type="button"
                  onClick={() => setEditingDescription(true)}
                >
                  <MarkdownView content={task.description} />
                </button>
              ) : (
                <button
                  className="block w-full rounded-lg border border-dashed p-3 text-left text-sm text-muted-foreground transition-colors hover:border-border hover:bg-accent"
                  type="button"
                  onClick={() => setEditingDescription(true)}
                >
                  <span className="italic opacity-60">Click to add a description…</span>
                </button>
              )}
            </div>

            {showActionsSection ? (
              <div>
                <p className="mb-2 text-xs font-medium uppercase tracking-wide text-muted-foreground">
                  Actions
                </p>
                <div className="flex flex-wrap gap-2 rounded-md border bg-background p-3">
                  {visibleExecutionActions.map((action) => {
                    const disabled = executionActionDisabled(action)
                    const disabledReason = executionActionDisabledReason(action)
                    return (
                      <Tooltip
                        key={action.action}
                        content={disabledReason}
                        className={disabledReason ? undefined : 'hidden'}
                      >
                        <span>
                          <Button
                            variant="outline"
                            size="sm"
                            disabled={disabled}
                            className={cn(
                              action.action === 'cancel_task' &&
                                'text-destructive hover:bg-destructive/10 hover:text-destructive',
                            )}
                            onClick={() => runExecutionAction(action)}
                          >
                            {action.label}
                          </Button>
                        </span>
                      </Tooltip>
                    )
                  })}
                  {showFallbackActions ? (
                    <>
                      {/* An outstanding execution blocker (D16/D17) means
                          launching would just re-enter the same gate this
                          Task is already waiting on -- `TaskExecutionApprovalNotice`
                          above already states the one authorized recovery, so this
                          option is removed rather than offered only to fail (8.2.6). */}
                      {canLaunch && !task.execution_blocker ? (
                        <Button
                          variant="outline"
                          size="sm"
                          disabled={!hasAgents || executionActionPending}
                          onClick={onOpenLaunchDialog}
                        >
                          Launch {productTerm('run')}
                        </Button>
                      ) : null}
                      <Button
                        variant="outline"
                        size="sm"
                        disabled={cancelPending}
                        className="text-destructive hover:bg-destructive/10 hover:text-destructive"
                        onClick={onCancelTask}
                      >
                        Cancel Task
                      </Button>
                    </>
                  ) : null}
                </div>
              </div>
            ) : null}

            <div>
              <p className="mb-2 text-xs font-medium uppercase tracking-wide text-muted-foreground">
                Observability
              </p>
              <TaskExecutionObservabilityPanel
                formatDate={formatDate}
                taskId={task.id}
                value={task.execution_observability}
              />
            </div>

            <div>
              <p className="mb-2 text-xs font-medium uppercase tracking-wide text-muted-foreground">
                Status
              </p>
              {gateActions ? (
                <div className="mb-2 flex flex-wrap gap-2">
                  <Tooltip
                    content={gateDecisionDisabledReason ?? ''}
                    className={gateDecisionDisabledReason ? undefined : 'hidden'}
                  >
                    <span>
                      <Button
                        disabled={
                          transitionPending ||
                          gateDecisionPending ||
                          Boolean(gateDecisionDisabledReason)
                        }
                        size="sm"
                        onClick={() => onApproveGate(gateActions.stateName)}
                      >
                        {gateActions.approveLabel}
                      </Button>
                    </span>
                  </Tooltip>
                  {gateActions.rejectLabel ? (
                    <Button
                      disabled={
                        transitionPending ||
                        gateDecisionPending ||
                        Boolean(gateDecisionDisabledReason)
                      }
                      size="sm"
                      variant="outline"
                      onClick={() => openRejectDialog(gateActions.stateName)}
                    >
                      {gateActions.rejectLabel}
                    </Button>
                  ) : null}
                </div>
              ) : null}
              <div className="flex flex-wrap items-center gap-2">
                <span title={managedStatusDisabledReason}>
                  <TaskStatusDropdown
                    availableStatuses={availableTransitions}
                    disabled={
                      transitionPending ||
                      gateDecisionPending ||
                      Boolean(managedStatusDisabledReason)
                    }
                    disabledStatusReasons={{ review: reviewDisabledReason }}
                    status={task.status}
                    onChange={(status) => onStatusChange(status)}
                  />
                </span>
                {manualAdvanceTarget ? (
                  <Tooltip
                    content={`Stop any running ${productTerm('run').toLowerCase()} and advance to the next ${productTerm('phase').toLowerCase()}.`}
                  >
                    <Button
                      className="gap-1.5"
                      disabled={
                        transitionPending || gateDecisionPending || advancePending || terminal
                      }
                      size="sm"
                      variant="outline"
                      onClick={onManualAdvance}
                    >
                      <FastForward size={14} />
                      Advance to {manualAdvanceLabel}
                    </Button>
                  </Tooltip>
                ) : null}
              </div>
              {managedStatusDisabledReason ? (
                <p className="mt-1 text-xs text-muted-foreground">{managedStatusDisabledReason}</p>
              ) : null}
            </div>

            <div>
              <p className="mb-2 text-xs font-medium uppercase tracking-wide text-muted-foreground">
                Assignees
              </p>
              <div
                className="flex flex-wrap gap-1.5"
                title={terminal ? 'task is terminal; cannot reassign' : undefined}
              >
                {assignableRoles.map((role) => {
                  const assignment = task.role_assignments.find(
                    (item) => item.role_name === role.name,
                  )
                  const roleDisabledReason = undefined
                  return (
                    <span key={role.name}>
                      <AgentAssigneeDropdown
                        agents={projectAgentsData ?? []}
                        members={membersData}
                        disabled={terminal || Boolean(roleDisabledReason) || rolePickerPending}
                        fallbackName={assignmentAgentName(assignment, agentName)}
                        requiredNow={currentRole === role.name}
                        roleLabel={role.display_name || role.name}
                        value={overviewAssignmentSelection(assignment)}
                        variant="chip"
                        onChange={(selection) => onAssigneeChange(role.name, selection)}
                      />
                    </span>
                  )
                })}
              </div>
              {rolePickerPending ? (
                <p className="mt-1 text-xs text-muted-foreground">Updating assignee...</p>
              ) : null}
            </div>

            <div className="flex items-center gap-3">
              <Label
                htmlFor="task-priority"
                className="shrink-0 text-xs font-medium uppercase tracking-wide text-muted-foreground"
              >
                Priority
              </Label>
              <Input
                id="task-priority"
                type="number"
                className="h-7 w-20 text-sm"
                value={priorityDraft}
                onBlur={handleSavePriority}
                onChange={(e) => setPriorityDraft(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter') handleSavePriority()
                  if (e.key === 'Escape') setPriorityDraft(String(task.priority))
                }}
              />
            </div>

            {task.status === 'done' || task.status === 'cancelled' ? (
              <div className="flex flex-wrap gap-1">
                <Button
                  variant="ghost"
                  size="sm"
                  disabled={duplicatePending}
                  onClick={onDuplicateTask}
                >
                  {duplicatePending ? 'Duplicating...' : 'Duplicate to Todo'}
                </Button>
              </div>
            ) : null}

            {Object.keys(task.remaining_retries).length > 0 ? (
              <div className="flex flex-wrap items-center gap-x-4 gap-y-1 text-xs text-muted-foreground">
                <span className="font-medium uppercase tracking-wide">Remaining retries</span>
                {Object.entries(task.remaining_retries).map(([key, value]) => (
                  <span key={key}>
                    {key.replace(/_/g, ' ')}:{' '}
                    <span className="font-mono text-foreground">{value}</span>
                  </span>
                ))}
              </div>
            ) : null}

            <CollapsibleSection
              title="Overrides"
              className="rounded-md border p-3"
              contentClassName="space-y-3"
            >
              <div>
                <p className="text-sm font-medium">Retry budgets</p>
                <p className="text-xs text-muted-foreground">
                  Blank inherits the workflow setting.
                </p>
              </div>
              <div className="grid gap-3 sm:grid-cols-4">
                <div className="space-y-1">
                  <Label htmlFor="task-review-retry-budget">Review retries</Label>
                  <Input
                    id="task-review-retry-budget"
                    type="number"
                    min={0}
                    step={1}
                    value={reviewRetryOverride}
                    onChange={(e) => setReviewRetryOverride(e.target.value)}
                  />
                  <p className="text-xs text-muted-foreground">
                    {effectiveRetryBudget('review', reviewRetryOverride, 3)}
                  </p>
                </div>
                <div className="space-y-1">
                  <Label htmlFor="task-merge-fix-retry-budget">Merge-fix retries</Label>
                  <Input
                    id="task-merge-fix-retry-budget"
                    type="number"
                    min={0}
                    step={1}
                    value={mergeFixRetryOverride}
                    onChange={(e) => setMergeFixRetryOverride(e.target.value)}
                  />
                  <p className="text-xs text-muted-foreground">
                    {effectiveRetryBudget('merge_fix', mergeFixRetryOverride, 1)}
                  </p>
                </div>
                <div className="space-y-1">
                  <Label htmlFor="task-execution-retry-budget">{productTerm('run')} retries</Label>
                  <Input
                    id="task-execution-retry-budget"
                    type="number"
                    min={0}
                    step={1}
                    value={executionRetryOverride}
                    onChange={(e) => setExecutionRetryOverride(e.target.value)}
                  />
                  <p className="text-xs text-muted-foreground">
                    {effectiveRetryBudget('execution', executionRetryOverride, 3)}
                  </p>
                </div>
              </div>
              <Button size="sm" disabled={updatePending} onClick={handleSaveRetryBudgets}>
                Save retry budgets
              </Button>
            </CollapsibleSection>

            <div className="space-y-1.5 pb-6 text-xs text-muted-foreground">
              <div className="grid grid-cols-2 gap-x-3 gap-y-1">
                <div>
                  <span>Created </span>
                  <span className="text-foreground">{formatDate(task.created_at)}</span>
                </div>
                <div>
                  <span>Updated </span>
                  <span className="text-foreground">{formatDate(task.updated_at)}</span>
                </div>
              </div>
              <TaskExternalLinks taskId={task.id} />
              {task.workspace ? (
                <div className="space-y-0.5">
                  <div>
                    <span>Branch </span>
                    <span className="font-mono text-foreground">{task.workspace.branch}</span>
                  </div>
                  <p className="break-all font-mono">{task.workspace.worktree_path}</p>
                </div>
              ) : null}
              {task.status === 'done' ? <p>Workspace cleaned after merge.</p> : null}
            </div>
          </>
        ) : (
          <div className="rounded-md border border-dashed p-6 text-sm text-muted-foreground">
            Task not found
          </div>
        )}
      </div>
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
          <div className="space-y-2">
            <Label htmlFor="gate-reject-reason">Reason</Label>
            <Textarea
              id="gate-reject-reason"
              value={rejectReasonDraft}
              onChange={(event) => setRejectReasonDraft(event.target.value)}
              placeholder="Describe what needs to change"
              rows={4}
            />
          </div>
          <DialogFooter>
            <Button variant="outline" onClick={closeRejectDialog}>
              Cancel
            </Button>
            <Button
              disabled={gateDecisionPending || !rejectReasonDraft.trim()}
              onClick={submitRejectDialog}
            >
              Reject
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}

function overviewAssignmentSelection(
  assignment?: Task['role_assignments'][number],
): AssigneeSelection {
  if (!assignment) return { type: 'unassigned' }
  if (assignment.assignee_type === 'agent' && assignment.assignee_id) {
    return { type: 'agent', agentId: assignment.assignee_id }
  }
  if (assignment.assignee_type === 'user') {
    return { type: 'user', userId: assignment.assignee_id ?? 'manual' }
  }
  return { type: 'unassigned' }
}
