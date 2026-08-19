import { useMemo, useState } from 'react'
import { useMutation, useQueryClient } from '@tanstack/react-query'
import { useNavigate } from '@tanstack/react-router'
import { toast } from 'sonner'
import {
  useAgentsQuery,
  useAdvanceTask,
  useApproveGate,
  useCancelTask,
  useCommentsQuery,
  useCreateComment,
  useDeleteComment,
  useDuplicateTask,
  useExecutionsQuery,
  useLaunchExecution,
  useRecoverTask,
  useReviewsQuery,
  useTaskDiffQuery,
  useTaskQuery,
  useRejectGate,
  useTransitionTask,
  useTriggerReview,
  useUpdateTask,
  useWorkflowQuery,
} from '@/api/hooks'
import { apiFetch } from '@/api/client'
import { qk } from '@/api/query-keys'
import type { AssigneeSelection } from '@/components/task-controls'
import { TaskCommentsPanel } from '@/components/task-detail/task-comments-panel'
import { TaskHistoryPanel } from '@/components/task-detail/task-history-panel'
import { useRolePicker } from '@/components/task-detail/use-role-picker'
import { getApiErrorMessage } from '@/lib/api-error'
import { productTerm } from '@/lib/i18n'
import { outgoingWorkflowEdges, workflowTriggerTargets } from '@/lib/workflow-utils'
import { saveRecentExecutionSelection } from '@/lib/execution-config-storage'
import { getHumanGateActions } from '@/lib/gate-actions'
import { getBlockingAnnotation } from '@/lib/workflow-utils'
import { TaskExecutionsTab } from '@/pages/task-detail/TaskExecutionsTab'
import { TaskReviewTab } from '@/pages/task-detail/TaskReviewTab'
import {
  extractRunSuffix,
  formatDate,
  getErrorInfo,
  getLatestReview,
  getTaskDetailApiErrorMessage,
  isRecord,
  readTaskStateConfig,
  stripRunSuffix,
  type UpdateTaskRequestWithStateConfig,
} from '@/pages/task-detail/utils'
import { TaskDetailSidebar } from '@/pages/task-detail/TaskDetailSidebar'
import { TaskDiffPanel } from '@/pages/task-detail/TaskDiffPanel'
import { TaskLaunchDialog } from '@/pages/task-detail/TaskLaunchDialog'
import { TaskOverviewPanel } from '@/pages/task-detail/TaskOverviewPanel'
import { TaskTerminalPanel } from '@/components/task-detail/task-terminal-panel'
import type { ExecutionConfigValue } from '@/components/execution-config/ExecutionConfigBar'
import type {
  Execution,
  LaunchExecutionResponse,
  TaskStatus,
  WorkflowDefinition,
  WorkflowExceptionAction,
} from '@/types/generated'

export type TaskDetailTab =
  | 'overview'
  | 'executions'
  | 'review'
  | 'diff'
  | 'terminal'
  | 'comments'
  | 'history'

export const taskDetailTabs = [
  'overview',
  'executions',
  'review',
  'diff',
  'terminal',
  'comments',
  'history',
] as const

export function isTaskDetailTab(value: string | undefined): value is TaskDetailTab {
  return taskDetailTabs.some((tab) => tab === value)
}

function retryBudgetFromStateConfig(
  workflow: WorkflowDefinition | undefined,
  taskStatus?: string,
): Record<string, unknown> | undefined {
  if (!workflow) return undefined
  const review = workflow.states.find((state) => state.name === 'review')
  const mergeFailed = workflow.states.find((state) => state.name === 'merge_failed')
  const current = workflow.states.find((state) => state.name === taskStatus)
  const mergeBudgets = isRecord(mergeFailed?.config.retry_budgets)
    ? mergeFailed.config.retry_budgets
    : undefined
  const currentBudgets = isRecord(current?.config.retry_budgets)
    ? current.config.retry_budgets
    : undefined
  return {
    ...(review?.gate_config?.max_rejections == null
      ? {}
      : { review: review.gate_config.max_rejections }),
    ...(mergeBudgets?.merge_fix == null ? {} : { merge_fix: mergeBudgets.merge_fix }),
    ...(currentBudgets?.execution == null ? {} : { execution: currentBudgets.execution }),
  }
}

export function TaskDetailPage({
  taskId,
  initialTab = 'overview',
}: {
  taskId: string
  initialTab?: TaskDetailTab
}) {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const taskQuery = useTaskQuery(taskId)
  const executionsQuery = useExecutionsQuery(taskId)
  const reviewsQuery = useReviewsQuery(taskId)
  const diffQuery = useTaskDiffQuery(taskId)
  const agentsQuery = useAgentsQuery()
  const updateTask = useUpdateTask()
  const transitionTask = useTransitionTask()
  const advanceTask = useAdvanceTask()
  const approveGate = useApproveGate()
  const rejectGate = useRejectGate()
  const rolePicker = useRolePicker()
  const launchExecution = useLaunchExecution()
  const triggerReview = useTriggerReview()
  const cancelTask = useCancelTask()
  const duplicateTask = useDuplicateTask()
  const recoverTask = useRecoverTask()
  const commentsQuery = useCommentsQuery(taskId)
  const createComment = useCreateComment()
  const deleteComment = useDeleteComment()

  const [launchDialogOpen, setLaunchDialogOpen] = useState(false)
  const [commentDraft, setCommentDraft] = useState('')
  const [expandedHistoryAttempts, setExpandedHistoryAttempts] = useState<Set<number>>(new Set())

  const stopExecution = useMutation({
    mutationFn: (executionId: string) =>
      apiFetch<Execution>(`/executions/${executionId}/cancel`, { method: 'POST' }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.task(taskId) })
      void queryClient.invalidateQueries({ queryKey: qk.executions(taskId) })
      void queryClient.invalidateQueries({ queryKey: qk.agents })
      toast.success(`${productTerm('run')} stopped`)
    },
    onError: (error) => toast.error(getApiErrorMessage(error, 'Stop failed')),
  })

  const reExecuteExecution = useMutation({
    mutationFn: (executionId: string) =>
      apiFetch<LaunchExecutionResponse | Execution>(`/executions/${executionId}/re-execute`, {
        method: 'POST',
      }),
    onSuccess: (response) => {
      const nextExecution = 'data' in response ? response.data.execution : response
      const nextTask = 'data' in response ? response.data.task : task
      void queryClient.invalidateQueries({ queryKey: qk.task(taskId) })
      void queryClient.invalidateQueries({ queryKey: qk.executions(taskId) })
      void queryClient.invalidateQueries({ queryKey: qk.taskDiff(taskId) })
      void queryClient.invalidateQueries({ queryKey: qk.agents })
      if (nextTask) {
        void queryClient.invalidateQueries({ queryKey: qk.projectTasks(nextTask.project_id) })
      }
      void navigate({
        to: '/tasks/$taskId/executions/$executionId',
        params: { taskId, executionId: nextExecution.id },
      })
    },
    onError: (error) => toast.error(getApiErrorMessage(error, 'Re-execute failed')),
  })

  const task = taskQuery.data
  const coderAssignment = task?.role_assignments.find(
    (assignment) => assignment.role_name === 'coder',
  )
  const executions = useMemo(() => executionsQuery.data?.items ?? [], [executionsQuery.data])
  const reviewDisabledReason = undefined
  const runSuffix = task ? extractRunSuffix(task.title) : ''
  const agentNamesById = useMemo(
    () => new Map((agentsQuery.data?.items ?? []).map((agent) => [agent.id, agent.name])),
    [agentsQuery.data],
  )
  const agentName = (agentId?: string | null) => {
    const name = agentId ? (agentNamesById.get(agentId) ?? agentId) : undefined
    return name ? stripRunSuffix(name, runSuffix) : undefined
  }
  const reviews = useMemo(() => reviewsQuery.data ?? [], [reviewsQuery.data])
  const latestReview = useMemo(() => getLatestReview(reviews), [reviews])
  const comments = useMemo(() => commentsQuery.data ?? [], [commentsQuery.data])
  const workflowQuery = useWorkflowQuery(task?.project_id ?? '')
  const workflow = workflowQuery.data
  const effectiveWorkflow = workflow
  const workflowRetryBudgets = retryBudgetFromStateConfig(effectiveWorkflow, task?.status)

  const errorInfo = task ? getErrorInfo(task) : undefined
  const showReviewTab = true
  const launchableStatuses = new Set<TaskStatus>([
    'todo',
    'in_progress',
    'blocked',
    'merge_failed',
    'review',
  ])
  const canLaunch = Boolean(task && launchableStatuses.has(task.status))
  const hasAgents = (agentsQuery.data?.items ?? []).length > 0

  const hiddenTransitions = ['merging', effectiveWorkflow?.cancellation_state ?? 'cancelled']

  const transitions: Record<TaskStatus, TaskStatus[]> = {
    todo: ['in_progress', 'cancelled'],
    in_progress: ['review', 'cancelled'],
    review: ['merging', 'cancelled'],
    merging: [],
    merge_failed: ['cancelled'],
    done: [],
    cancelled: [],
  }

  const workflowTransitions =
    effectiveWorkflow && task ? workflowTriggerTargets(effectiveWorkflow, task.status) : undefined
  const availableTransitions = (
    workflowTransitions ??
    (task && task.status === 'todo' && !coderAssignment
      ? transitions.todo.filter((status) => status !== 'in_progress')
      : task
        ? (transitions[task.status] ?? [])
        : [])
  ).filter((status) => !hiddenTransitions.includes(status))

  const manualAdvanceTarget =
    effectiveWorkflow && task
      ? (() => {
          const currentIndex = effectiveWorkflow.states.findIndex(
            (state) => state.name === task.status,
          )
          if (currentIndex < 0) return null
          const cancellationState = effectiveWorkflow.cancellation_state ?? 'cancelled'
          const currentState = effectiveWorkflow.states[currentIndex]
          const rejectTarget =
            typeof currentState.gate_config === 'object' && currentState.gate_config
              ? currentState.gate_config.reject_target
              : null
          const candidates = outgoingWorkflowEdges(effectiveWorkflow, task.status).filter(
            (transition) =>
              transition.to !== task.status &&
              transition.to !== cancellationState &&
              transition.to !== rejectTarget,
          )
          const forwardTarget = candidates
            .map((transition) => ({
              transition,
              index: effectiveWorkflow.states.findIndex((state) => state.name === transition.to),
            }))
            .filter((candidate) => candidate.index > currentIndex)
            .sort((a, b) => a.index - b.index)[0]?.transition.to
          return forwardTarget ?? candidates[0]?.to ?? null
        })()
      : null
  const manualAdvanceLabel = manualAdvanceTarget
    ? (effectiveWorkflow?.states.find((state) => state.name === manualAdvanceTarget)
        ?.display_name ?? manualAdvanceTarget.replace(/_/g, ' '))
    : null
  const managedStatusDisabledReason = undefined
  const gateActions = getHumanGateActions(task, effectiveWorkflow)
  const gateRole =
    gateActions && effectiveWorkflow
      ? (effectiveWorkflow.states.find((state) => state.name === gateActions.stateName)?.role ??
        null)
      : null
  const runningGateExecution =
    gateRole == null
      ? undefined
      : executions.find(
          (execution) => execution.status === 'running' && execution.role === gateRole,
        )
  const gateDecisionDisabledReason = runningGateExecution
    ? `${gateRole} is still running. Wait for the ${productTerm('run').toLowerCase()} to finish before approving or rejecting.`
    : undefined
  const gateDecisionPending = approveGate.isPending || rejectGate.isPending
  const terminal =
    task?.status === 'done' ||
    task?.status === (effectiveWorkflow?.cancellation_state ?? 'cancelled')
  const currentRole =
    effectiveWorkflow?.states.find((state) => state.name === task?.status)?.role ?? null
  const coderRole =
    effectiveWorkflow?.states.find((state) => state.name === 'in_progress')?.role ??
    effectiveWorkflow?.roles.find((role) => role.name === 'coder')?.name ??
    'coder'
  const visibleRoles = effectiveWorkflow?.roles ?? [
    { name: 'coder', display_name: 'Coder', description: '' },
  ]
  const assignableRoles = [
    ...visibleRoles.filter((role) => role.name === coderRole),
    ...visibleRoles.filter((role) => role.name !== coderRole),
  ]

  // Handlers

  const onUpdateTitle = (title: string) => {
    if (!task) return
    updateTask.mutate({ taskId: task.id, body: { title, version: task.version } })
  }

  const onUpdateDescription = (description: string | null) => {
    if (!task) return
    updateTask.mutate({
      taskId: task.id,
      body: { description, version: task.version },
    })
  }

  const onUpdatePriority = (priority: number) => {
    if (!task) return
    updateTask.mutate({ taskId: task.id, body: { priority, version: task.version } })
  }

  const onSaveRetryBudgets = (
    review: number | undefined,
    mergeFix: number | undefined,
    execution: number | undefined,
  ) => {
    if (!task) return
    const nextConfig = { ...readTaskStateConfig(task) }
    if (review === undefined && mergeFix === undefined && execution === undefined) {
      delete nextConfig.retry_budgets
    } else {
      nextConfig.retry_budgets = {
        ...(review === undefined ? {} : { review }),
        ...(mergeFix === undefined ? {} : { merge_fix: mergeFix }),
        ...(execution === undefined ? {} : { execution }),
      }
    }
    const body: UpdateTaskRequestWithStateConfig = {
      version: task.version,
      task_state_config: nextConfig,
    }
    updateTask.mutate(
      { taskId: task.id, body },
      {
        onSuccess: () => toast.success('Retry budgets saved'),
        onError: (error) => toast.error(getApiErrorMessage(error, 'Retry budget update failed')),
      },
    )
  }

  const onStatusChange = (status: string, reason?: string) => {
    if (!task || status === task.status) return
    transitionTask.mutate(
      {
        taskId: task.id,
        body: { status, version: task.version, reason },
        currentStatus: task.status,
      },
      {
        onSuccess: (result) => {
          if (status !== 'review' || !result.review) return
          if (result.review.status === 'passed') {
            toast.success('Review passed')
            return
          }
          if (result.review.status === 'failed') {
            const failedStep = result.review.step_results.find((step) => step.exit_code !== 0)
            if (failedStep) {
              toast.error(`Review failed on step ${failedStep.index}: ${failedStep.command}`)
            } else {
              toast.error('Review failed')
            }
          }
        },
        onError: (error) => {
          toast.error(getTaskDetailApiErrorMessage(error, 'Transition failed'))
        },
      },
    )
  }

  const onRecoverTask = (
    action: Parameters<typeof recoverTask.mutate>[0]['action'],
    input?: { reason?: string; context?: string },
  ) => {
    if (!task) return
    recoverTask.mutate(
      { taskId: task.id, action, reason: input?.reason, context: input?.context },
      {
        onError: (error) => {
          toast.error(getTaskDetailApiErrorMessage(error, 'Task recovery failed'))
        },
      },
    )
  }

  const onOpenWorkflowExceptionAction = (action: WorkflowExceptionAction) => {
    if (!task) return
    if (action.target_execution_id) {
      void navigate({
        to: '/tasks/$taskId/executions/$executionId',
        params: { taskId: task.id, executionId: action.target_execution_id },
        search: { followUp: true },
      })
      return
    }
    setLaunchDialogOpen(true)
  }

  const onApproveGate = (stateName: string) => {
    if (!task) return
    const blockingAnnotation = getBlockingAnnotation(task)
    if (
      task.status === 'blocked' &&
      stateName === 'blocked' &&
      blockingAnnotation?.recovery_actions?.includes('retry_hook')
    ) {
      onRecoverTask('retry_hook')
      return
    }
    if (
      blockingAnnotation?.type === 'workflow_guard_rejected' &&
      blockingAnnotation.recovery_actions?.includes('resume_session')
    ) {
      onRecoverTask('resume_session')
      return
    }
    approveGate.mutate(
      { taskId: task.id, stateName, body: { version: task.version } },
      { onError: (error) => toast.error(getApiErrorMessage(error, 'Gate approval failed')) },
    )
  }

  const onRejectGate = (stateName: string, reason: string) => {
    if (!task) return
    rejectGate.mutate(
      { taskId: task.id, stateName, body: { version: task.version, reason } },
      { onError: (error) => toast.error(getApiErrorMessage(error, 'Gate rejection failed')) },
    )
  }

  const onManualAdvance = () => {
    if (!task || !manualAdvanceTarget) return
    advanceTask.mutate(task.id, {
      onSuccess: (advancedTask) => {
        toast.success(`Advanced to ${advancedTask.status.replace(/_/g, ' ')}`)
      },
      onError: (error) => {
        toast.error(getTaskDetailApiErrorMessage(error, 'Manual advance failed'))
      },
    })
  }

  const onAssigneeChange = (roleName: string, selection: AssigneeSelection) => {
    if (!task || terminal) return
    rolePicker.submit({
      taskId: task.id,
      roleName,
      selection,
      onError: (error) => toast.error(getTaskDetailApiErrorMessage(error, 'Assignment failed')),
    })
  }

  const onCancelTask = () => {
    if (!task) return
    cancelTask.mutate(task.id, {
      onError: (error) => toast.error(getApiErrorMessage(error, 'Cancel failed')),
    })
  }

  const onDuplicateTask = () => {
    if (!task) return
    duplicateTask.mutate(task.id, {
      onSuccess: () => toast.success('Task duplicated to Todo'),
      onError: (error) => toast.error(getApiErrorMessage(error, 'Duplicate failed')),
    })
  }

  const postComment = () => {
    if (!task) return
    const content = commentDraft.trim()
    if (!content) return
    createComment.mutate(
      { taskId: task.id, body: { content, author_name: 'You' } },
      {
        onSuccess: () => setCommentDraft(''),
        onError: (error) => toast.error(getApiErrorMessage(error, 'Comment failed')),
      },
    )
  }

  const rerunReview = () => {
    if (!task) return
    triggerReview.mutate(task.id, {
      onSuccess: (result) => {
        if (result.review?.status === 'passed') {
          toast.success('Review passed')
        } else if (result.review?.status === 'failed') {
          const failedStep = result.review.step_results.find((step) => step.exit_code !== 0)
          if (failedStep) {
            toast.error(`Review failed on step ${failedStep.index}: ${failedStep.command}`)
          } else {
            toast.error('Review failed')
          }
        } else {
          toast.success('Review started')
        }
      },
      onError: (error) => {
        toast.error(getApiErrorMessage(error, 'Review trigger failed'))
      },
    })
  }

  const onSubmitLaunch = (config: ExecutionConfigValue, summary: string) => {
    if (!task || !config.agentId) return
    launchExecution.mutate(
      {
        taskId: task.id,
        body: {
          agent_id: config.agentId,
          summary: summary.trim() ? summary.trim() : null,
          overrides: config.overrides,
        },
      },
      {
        onSuccess: () => {
          saveRecentExecutionSelection(
            config.agentId,
            config.selection ?? {
              modelId: null,
              reasoningEffort: null,
              permissionPolicy: null,
            },
          )
          toast.success(`${productTerm('run')} launched`)
          setLaunchDialogOpen(false)
          void navigate({
            to: '/tasks/$taskId/$tab',
            params: { taskId: task.id, tab: 'executions' },
          })
        },
        onError: (error) => {
          toast.error(getApiErrorMessage(error, 'Launch failed'))
        },
      },
    )
  }

  const toggleHistoryAttempt = (attemptNumber: number) => {
    setExpandedHistoryAttempts((current) => {
      const next = new Set(current)
      if (next.has(attemptNumber)) {
        next.delete(attemptNumber)
      } else {
        next.add(attemptNumber)
      }
      return next
    })
  }

  return (
    <>
      <div className="flex h-full gap-0 overflow-hidden rounded-xl border border-border-subtle bg-card shadow-card">
        <TaskDetailSidebar
          task={task}
          isLoading={taskQuery.isLoading}
          taskId={taskId}
          runSuffix={runSuffix}
          activeTab={initialTab}
          executionCount={executions.length}
          commentCount={comments.length}
          showReviewTab={showReviewTab}
        />

        <div className="flex-1 overflow-y-auto">
          {initialTab === 'overview' && (
            <TaskOverviewPanel
              task={task}
              isLoading={taskQuery.isLoading}
              isError={taskQuery.isError}
              error={taskQuery.error}
              onRetryLoad={() => void taskQuery.refetch()}
              updatePending={updateTask.isPending}
              recoverPending={recoverTask.isPending}
              transitionPending={transitionTask.isPending}
              gateDecisionPending={gateDecisionPending}
              advancePending={advanceTask.isPending}
              rolePickerPending={rolePicker.isPending}
              cancelPending={cancelTask.isPending}
              duplicatePending={duplicateTask.isPending}
              executionActionPending={stopExecution.isPending || reExecuteExecution.isPending}
              errorInfo={errorInfo}
              gateActions={gateActions}
              gateDecisionDisabledReason={gateDecisionDisabledReason}
              availableTransitions={availableTransitions}
              managedStatusDisabledReason={managedStatusDisabledReason}
              reviewDisabledReason={reviewDisabledReason}
              manualAdvanceTarget={manualAdvanceTarget}
              manualAdvanceLabel={manualAdvanceLabel}
              terminal={terminal}
              currentRole={currentRole}
              assignableRoles={assignableRoles}
              agents={agentsQuery.data?.items ?? []}
              executions={executions}
              canLaunch={canLaunch}
              hasAgents={hasAgents}
              runSuffix={runSuffix}
              workflowRetryBudgets={workflowRetryBudgets}
              agentName={agentName}
              onUpdateTitle={onUpdateTitle}
              onUpdateDescription={onUpdateDescription}
              onUpdatePriority={onUpdatePriority}
              onRecover={onRecoverTask}
              onOpenWorkflowExceptionAction={onOpenWorkflowExceptionAction}
              onApproveGate={onApproveGate}
              onRejectGate={onRejectGate}
              onStatusChange={onStatusChange}
              onManualAdvance={onManualAdvance}
              onAssigneeChange={onAssigneeChange}
              onCancelTask={onCancelTask}
              onDuplicateTask={onDuplicateTask}
              onOpenLaunchDialog={() => setLaunchDialogOpen(true)}
              onContinueSession={(executionId) => {
                void navigate({
                  to: '/tasks/$taskId/executions/$executionId',
                  params: { taskId, executionId },
                  search: { followUp: true },
                })
              }}
              onStopExecution={(executionId) => stopExecution.mutate(executionId)}
              onReExecuteExecution={(executionId) => reExecuteExecution.mutate(executionId)}
              onSaveRetryBudgets={onSaveRetryBudgets}
            />
          )}

          {initialTab === 'executions' && (
            <div className="p-6">
              <TaskExecutionsTab
                taskId={taskId}
                executions={executions}
                isLoading={executionsQuery.isLoading}
                agentName={agentName}
                formatDate={formatDate}
              />
            </div>
          )}

          {initialTab === 'review' && showReviewTab && task ? (
            <div className="p-6">
              <TaskReviewTab
                task={task}
                reviews={reviews}
                latestReview={latestReview}
                reviewsLoading={reviewsQuery.isLoading}
                transitionPending={transitionTask.isPending}
                triggerReviewPending={triggerReview.isPending}
                recoverPending={recoverTask.isPending}
                cancelPending={cancelTask.isPending}
                terminal={terminal}
                expandedHistoryAttempts={expandedHistoryAttempts}
                onRerunReview={rerunReview}
                onStatusChange={onStatusChange}
                onRecover={onRecoverTask}
                onOpenWorkflowExceptionAction={onOpenWorkflowExceptionAction}
                onCancelTask={onCancelTask}
                onToggleHistoryAttempt={toggleHistoryAttempt}
              />
            </div>
          ) : null}

          {initialTab === 'diff' && (
            <TaskDiffPanel
              diffQuery={diffQuery}
              canLaunch={canLaunch}
              hasAgents={hasAgents}
              onOpenLaunchDialog={() => {
                setLaunchDialogOpen(true)
              }}
            />
          )}

          {initialTab === 'terminal' && <TaskTerminalPanel taskId={taskId} className="h-full" />}

          {initialTab === 'comments' && (
            <div className="px-8 py-6">
              <div className="max-w-[760px]">
                {task ? (
                  <TaskCommentsPanel
                    task={task}
                    comments={comments}
                    commentDraft={commentDraft}
                    setCommentDraft={setCommentDraft}
                    createComment={createComment}
                    deleteComment={deleteComment}
                    formatDate={formatDate}
                    onPostComment={postComment}
                  />
                ) : null}
              </div>
            </div>
          )}

          {initialTab === 'history' && (
            <div className="p-6">
              <TaskHistoryPanel taskId={taskId} />
            </div>
          )}
        </div>
      </div>
      <TaskLaunchDialog
        open={launchDialogOpen}
        onOpenChange={(open) => {
          setLaunchDialogOpen(open)
        }}
        isPending={launchExecution.isPending}
        onSubmit={onSubmitLaunch}
      />
    </>
  )
}
