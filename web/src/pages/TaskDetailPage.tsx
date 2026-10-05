import { useMemo, useState } from 'react'
import { useNavigate } from '@tanstack/react-router'
import { toast } from 'sonner'
import {
  useAgentsQuery,
  useCommentsQuery,
  useCreateComment,
  useDeleteComment,
  useDuplicateTask,
  useLaunchExecution,
  useReviewsQuery,
  useTaskDiffQuery,
  useTaskDetailQuery,
  useTransitionTask,
  useUpdateTask,
} from '@/api/hooks'
import { ErrorBanner } from '@/components/error-banner'
import type { AssigneeSelection } from '@/components/task-controls'
import { TaskCommentsPanel } from '@/components/task-detail/task-comments-panel'
import { TaskHistoryPanel } from '@/components/task-detail/task-history-panel'
import { useRolePicker } from '@/components/task-detail/use-role-picker'
import { getApiErrorMessage, isTransientApiError, notifyTaskBusy } from '@/lib/api-error'
import { productTerm } from '@/lib/i18n'
import { workflowTriggerTargets } from '@/lib/workflow-utils'
import { saveRecentExecutionSelection } from '@/lib/execution-config-storage'
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
  
  
  TaskStatus,
  WorkflowDefinition,
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
  const taskDetailQuery = useTaskDetailQuery(taskId)
  const reviewsQuery = useReviewsQuery(taskId, { enabled: initialTab === 'review' })
  const diffQuery = useTaskDiffQuery(taskId, { enabled: initialTab === 'diff' })
  const agentsQuery = useAgentsQuery({
    enabled: initialTab === 'overview' || initialTab === 'executions' || initialTab === 'diff',
  })
  const updateTask = useUpdateTask()
  const transitionTask = useTransitionTask()
  const rolePicker = useRolePicker()
  const launchExecution = useLaunchExecution()
  const duplicateTask = useDuplicateTask()
  const commentsQuery = useCommentsQuery(taskId, { enabled: initialTab === 'comments' })
  const createComment = useCreateComment()
  const deleteComment = useDeleteComment()

  const [launchDialogOpen, setLaunchDialogOpen] = useState(false)
  const [commentDraft, setCommentDraft] = useState('')
  const [expandedHistoryAttempts, setExpandedHistoryAttempts] = useState<Set<number>>(new Set())

  const task = taskDetailQuery.data?.task
  const coderAssignment = task?.role_assignments.find(
    (assignment) => assignment.role_name === 'coder',
  )
  const executions = useMemo(
    () => taskDetailQuery.data?.executions.items ?? [],
    [taskDetailQuery.data?.executions.items],
  )
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
  const workflow = taskDetailQuery.data?.workflow
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



  const managedStatusDisabledReason = undefined





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
          if (notifyTaskBusy(error)) return
          toast.error(getTaskDetailApiErrorMessage(error, 'Transition failed'))
        },
      },
    )
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
          void taskDetailQuery.refetch()
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
      <div className="flex h-full flex-col gap-0 overflow-hidden rounded-xl border border-border-subtle bg-card shadow-card md:flex-row">
        <TaskDetailSidebar
          task={task}
          isLoading={taskDetailQuery.isLoading}
          taskId={taskId}
          runSuffix={runSuffix}
          activeTab={initialTab}
          executionCount={taskDetailQuery.data?.executions.total_count ?? undefined}
          commentCount={commentsQuery.data?.length}
          showReviewTab={showReviewTab}
        />

        <div className="min-w-0 flex-1 overflow-y-auto">
          {taskDetailQuery.isError && !taskDetailQuery.data && initialTab !== 'overview' ? (
            <div className="p-6">
              <ErrorBanner
                error={taskDetailQuery.error}
                fallback="Task details failed to load"
                onRetry={() => void taskDetailQuery.refetch()}
                showRetry={isTransientApiError(taskDetailQuery.error)}
              />
            </div>
          ) : null}

          {initialTab === 'overview' && (
            <TaskOverviewPanel
              task={task}
              isLoading={taskDetailQuery.isLoading}
              isError={taskDetailQuery.isError}
              error={taskDetailQuery.error instanceof Error ? taskDetailQuery.error : null}
              onRetryLoad={() => void taskDetailQuery.refetch()}
              updatePending={updateTask.isPending}
              transitionPending={transitionTask.isPending}
              rolePickerPending={rolePicker.isPending}
              duplicatePending={duplicateTask.isPending}
              errorInfo={errorInfo}
              availableTransitions={availableTransitions}
              managedStatusDisabledReason={managedStatusDisabledReason}
              reviewDisabledReason={reviewDisabledReason}
              terminal={terminal}
              currentRole={currentRole}
              assignableRoles={assignableRoles}
              agents={agentsQuery.data?.items ?? []}
              runSuffix={runSuffix}
              workflowRetryBudgets={workflowRetryBudgets}
              agentName={agentName}
              onUpdateTitle={onUpdateTitle}
              onUpdateDescription={onUpdateDescription}
              onUpdatePriority={onUpdatePriority}
              onStatusChange={onStatusChange}
              onAssigneeChange={onAssigneeChange}
              onDuplicateTask={onDuplicateTask}
              onOpenLaunchDialog={() => setLaunchDialogOpen(true)}

              onSaveRetryBudgets={onSaveRetryBudgets}
            />
          )}

          {initialTab === 'executions' && !(taskDetailQuery.isError && !taskDetailQuery.data) && (
            <div className="p-6">
              <TaskExecutionsTab
                version={task?.version ?? 0}
                offers={task?.available_actions ?? []}
                executions={executions}
                taskId={taskId}
                isLoading={taskDetailQuery.isLoading}
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
                reviewsIsError={reviewsQuery.isError}
                reviewsError={reviewsQuery.error}
                onRetryReviews={() => void reviewsQuery.refetch()}
                expandedHistoryAttempts={expandedHistoryAttempts}
                onToggleHistoryAttempt={toggleHistoryAttempt}
              />
            </div>
          ) : null}

          {initialTab === 'diff' && (
            <TaskDiffPanel
              canLaunch={canLaunch}
              hasAgents={hasAgents}
              diffQuery={diffQuery}
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
                    commentsLoading={commentsQuery.isLoading}
                    commentsIsError={commentsQuery.isError}
                    commentsError={commentsQuery.error}
                    onRetryComments={() => void commentsQuery.refetch()}
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
