import { useEffect, useState } from 'react'
import { TaskActionButtons } from '@/components/task-detail/task-action-buttons'
import { Link } from '@tanstack/react-router'
import { toast } from 'sonner'
import { useMembersQuery, useProjectAgentsQuery } from '@/api/hooks'
import { ErrorBanner } from '@/components/error-banner'
import { isTransientApiError } from '@/lib/api-error'
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
import { WorkflowHealthBadge } from '@/components/workflow-health-badge'
import { Button } from '@/components/ui/button'
import { CollapsibleSection } from '@/components/ui/collapsible-section'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Skeleton } from '@/components/ui/skeleton'
import { productTerm } from '@/lib/i18n'
import { MarkdownEditor, MarkdownView } from '@/components/ui/markdown-editor'
import { cn } from '@/lib/cn'
import type {
  Agent,
  
  RoleDefinition,
  Task,
} from '@/types/generated'
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
  transitionPending: boolean
  rolePickerPending: boolean
  duplicatePending: boolean
  // computed
  errorInfo: { tone: 'timeout' | 'crash' | 'workspace'; message: string } | undefined
  availableTransitions: string[]
  managedStatusDisabledReason: string | undefined
  reviewDisabledReason: string | undefined
  terminal: boolean
  currentRole: string | null
  assignableRoles: RoleDefinition[]
  agents: Agent[]
  runSuffix: string
  workflowRetryBudgets: Record<string, unknown> | undefined
  agentName: (agentId?: string | null) => string | undefined
  // handlers
  onUpdateTitle: (title: string) => void
  onUpdateDescription: (description: string | null) => void
  onUpdatePriority: (priority: number) => void
  onStatusChange: (status: string) => void
  onAssigneeChange: (roleName: string, selection: AssigneeSelection) => void
  onDuplicateTask: () => void
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
  transitionPending,
  rolePickerPending,
  duplicatePending,
  errorInfo,
  availableTransitions,
  managedStatusDisabledReason,
  reviewDisabledReason,
  terminal,
  currentRole,
  assignableRoles,
  runSuffix,
  workflowRetryBudgets,
  agentName,
  onUpdateTitle,
  onUpdateDescription,
  onUpdatePriority,
  onStatusChange,
  onAssigneeChange,
  onDuplicateTask,
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

  const projectId = task?.project_id ?? ''
  const { data: projectAgentsData } = useProjectAgentsQuery(projectId)
  const { data: membersData } = useMembersQuery(projectId)
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
    <div className="px-4 py-4 sm:px-6 sm:py-6 lg:px-8">
      <div className="max-w-[760px] space-y-6">
        {isLoading ? (
          <div className="space-y-3">
            <Skeleton className="h-8 w-3/4" />
            <Skeleton className="h-24 w-full" />
          </div>
        ) : isError && !task ? (
          <ErrorBanner
            error={error}
            fallback="Task failed to load"
            onRetry={onRetryLoad}
            showRetry={isTransientApiError(error)}
          />
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

            <WorkflowExceptionPanel task={task} />
            {!task.workflow_exception ? <TaskBlockingBanner task={task} /> : null}

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

            {!task.workflow_exception ? <TaskActionButtons taskId={task.id} version={task.version} offers={task.available_actions ?? []} /> : null}

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
              <div className="flex flex-wrap items-center gap-2">
                <span title={managedStatusDisabledReason}>
                  <TaskStatusDropdown
                    availableStatuses={availableTransitions}
                    disabled={
                      transitionPending ||
                      
                      Boolean(managedStatusDisabledReason)
                    }
                    disabledStatusReasons={{ review: reviewDisabledReason }}
                    status={task.status}
                    onChange={(status) => onStatusChange(status)}
                  />
                </span>

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
              {task.status === 'done' && (task.placement ?? task.workspace?.placement)?.state === 'cleaned' ? (
                <p>Workspace cleaned after merge.</p>
              ) : null}
            </div>
          </>
        ) : (
          <div className="rounded-md border border-dashed p-6 text-sm text-muted-foreground">
            Task not found
          </div>
        )}
      </div>

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
