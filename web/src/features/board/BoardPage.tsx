import { lazy, Suspense } from 'react'
import { Kanban, Plus } from '@phosphor-icons/react'
import { ErrorBanner } from '@/components/error-banner'
import { Button } from '@/components/ui/button'
import { ProjectFlowHeader } from '@/components/project-flow-status'
import { BoardToolbar } from './BoardToolbar'
import { BoardView } from './BoardView'
import { useBoardPageController } from './useBoardPageController'

const TaskCreateDialog = lazy(() =>
  import('@/components/task-create-dialog').then((module) => ({
    default: module.TaskCreateDialog,
  })),
)
const TaskDetailModal = lazy(() =>
  import('@/components/task-detail-modal').then((module) => ({
    default: module.TaskDetailModal,
  })),
)

export function BoardPage({ projectId }: { projectId: string }) {
  const board = useBoardPageController(projectId)

  return (
    <div className="flex h-full min-h-0 flex-col gap-3" data-board-page>
      <ProjectFlowHeader key={projectId} projectId={projectId} />
      <BoardToolbar
        agents={board.agentsQuery.data?.items ?? []}
        selectedAgentIds={board.filterAgentIds}
        q={board.filterQ}
        priorityMin={board.filterPriorityMin}
        priorityMax={board.filterPriorityMax}
        blockedOnly={board.filterBlockedOnly}
        includeCancelled={board.filterIncludeCancelled}
        includeArchived={board.filterIncludeArchived}
        showMobileFilters={board.showMobileFilters}
        searchInputRef={board.searchInputRef}
        orderingMessage={board.ordering.reason}
        onToggleMobileFilters={() => board.setShowMobileFilters((visible) => !visible)}
        onFilterChange={board.setUrlFilters}
        onNewTask={() => board.setCreateDialogOpen(true)}
      />

      {board.dragSession.state.announcement ? (
        <div
          className="flex shrink-0 items-center justify-between gap-3 rounded-lg border border-amber-300/60 bg-amber-50 px-3 py-2 text-xs text-amber-950 dark:border-amber-500/30 dark:bg-amber-500/10 dark:text-amber-100"
          role="status"
          data-board-announcement
        >
          <span>{board.dragSession.state.announcement}</span>
          <button
            type="button"
            className="font-medium underline underline-offset-2"
            onClick={board.dragSession.dismissAnnouncement}
          >
            Dismiss
          </button>
        </div>
      ) : null}

      {board.tasksQuery.isError ? (
        <ErrorBanner
          error={board.tasksQuery.error}
          fallback="Tasks failed to load"
          onRetry={() => void board.tasksQuery.refetch()}
        />
      ) : null}

      {board.tasksQuery.isLoading ? (
        <div className="flex min-h-0 flex-1 gap-2.5 overflow-hidden" aria-label="Loading board">
          {[0, 1, 2].map((index) => (
            <div
              key={index}
              className="h-full w-[280px] shrink-0 animate-pulse rounded-xl border bg-muted/40"
            />
          ))}
        </div>
      ) : null}

      {!board.tasksQuery.isLoading && !board.tasksQuery.isError && board.tasks.length === 0 ? (
        <div className="flex flex-1 items-center justify-center">
          <div className="rounded-xl border border-dashed p-12 text-center">
            <div className="mx-auto mb-4 flex h-12 w-12 items-center justify-center rounded-full bg-muted">
              <Kanban size={22} className="text-muted-foreground" />
            </div>
            <p className="text-sm font-semibold">No tasks yet</p>
            <p className="mt-1.5 text-xs text-muted-foreground">
              Create your first task to get started
            </p>
            <Button
              className="mt-5 rounded-lg"
              size="sm"
              onClick={() => board.setCreateDialogOpen(true)}
            >
              <Plus size={13} weight="bold" className="mr-1.5" />
              Create task
            </Button>
          </div>
        </div>
      ) : null}

      {!board.tasksQuery.isLoading && !board.tasksQuery.isError && board.tasks.length > 0 ? (
        <BoardView
          columns={board.boardColumns}
          groupedTasks={board.grouped}
          orderingEnabled={board.ordering.enabled}
          orderingReason={board.ordering.reason}
          movePending={board.dragSession.movePending}
          draggingTaskId={board.dragSession.state.draggableId}
          validDropStatuses={board.validDropStatuses}
          activeDropStatus={board.dragSession.state.activeDropStatus}
          quickCreateOpen={board.quickCreateOpen}
          quickCreateTitle={board.quickCreateTitle}
          quickCreateDescription={board.quickCreateDescription}
          quickCreateDescriptionRef={board.quickCreateDescriptionRef}
          createPending={board.createTask.isPending}
          agentPickerTaskId={board.agentPickerTaskId}
          agents={board.agentsQuery.data?.items ?? []}
          agentNamesById={board.agentNamesById}
          assignPending={board.assignRole.isPending}
          renderTaskMenuItems={board.renderTaskMenuItems}
          onToggleQuickCreate={() => board.setQuickCreateOpen((open) => !open)}
          onQuickCreateTitleChange={board.setQuickCreateTitle}
          onQuickCreateDescriptionChange={board.setQuickCreateDescription}
          onSubmitQuickCreate={board.submitQuickCreate}
          onCancelQuickCreate={board.cancelQuickCreate}
          onAssignAgent={board.assignAgent}
          onAgentClick={board.handleAgentClick}
          onTaskClick={(task) => board.openTaskDetail(task.id)}
          onTaskContextMenu={board.openContextMenu}
          hasMore={Boolean(board.tasksQuery.hasNextPage)}
          onLoadMore={board.handleLoadMore}
          onDragStart={board.dragSession.onDragStart}
          onDragUpdate={board.dragSession.onDragUpdate}
          onDragEnd={(result) => void board.dragSession.onDragEnd(result)}
        />
      ) : null}

      {board.contextMenu ? (
        <div
          className="fixed z-50 min-w-40 overflow-hidden rounded-lg border bg-popover p-1 text-popover-foreground shadow-float"
          style={{ left: board.contextMenu.x, top: board.contextMenu.y }}
          onClick={(event) => event.stopPropagation()}
        >
          <button
            type="button"
            className="flex w-full cursor-pointer items-center rounded-md px-2.5 py-1.5 text-sm hover:bg-accent"
            onClick={() => {
              board.transitionTaskFromMenu(board.contextMenu!.task, 'cancelled')
              board.setContextMenu(undefined)
            }}
          >
            Cancel
          </button>
          <button
            type="button"
            className="flex w-full cursor-pointer items-center rounded-md px-2.5 py-1.5 text-sm hover:bg-accent"
            onClick={() => {
              board.setAgentPickerTaskId(board.contextMenu!.task.id)
              board.setContextMenu(undefined)
            }}
          >
            Assign Agent
          </button>
          <button
            type="button"
            className="flex w-full cursor-pointer items-center rounded-md px-2.5 py-1.5 text-sm hover:bg-accent"
            onClick={() => {
              board.openTaskDetail(board.contextMenu!.task.id)
              board.setContextMenu(undefined)
            }}
          >
            View Detail
          </button>
        </div>
      ) : null}

      <Suspense fallback={null}>
        {board.createDialogOpen ? (
          <TaskCreateDialog
            open
            projectId={projectId}
            onCreated={(task) => board.openTaskDetail(task.id)}
            onOpenChange={board.setCreateDialogOpen}
          />
        ) : null}
        {board.selectedTaskId ? (
          <TaskDetailModal taskId={board.selectedTaskId} open onClose={board.closeTaskDetail} />
        ) : null}
      </Suspense>
    </div>
  )
}
