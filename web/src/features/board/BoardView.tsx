import type { MouseEvent, ReactNode, RefObject } from 'react'
import { DragDropContext, type DragStart, type DragUpdate, type DropResult } from '@hello-pangea/dnd'
import { KanbanColumn } from '@/components/kanban-column'
import type { ColumnGroup } from '@/lib/workflow-utils'
import type { Agent } from '@/types/generated'
import type { TaskListItem as Task } from '@/types/task-list'

export function BoardView({
  columns,
  groupedTasks,
  orderingEnabled,
  orderingReason,
  movePending,
  draggingTaskId,
  validDropStatuses,
  activeDropStatus,
  quickCreateOpen,
  quickCreateTitle,
  quickCreateDescription,
  quickCreateDescriptionRef,
  createPending,
  agentPickerTaskId,
  agents,
  agentNamesById,
  assignPending,
  renderTaskMenuItems,
  onToggleQuickCreate,
  onQuickCreateTitleChange,
  onQuickCreateDescriptionChange,
  onSubmitQuickCreate,
  onCancelQuickCreate,
  onAssignAgent,
  onAgentClick,
  onTaskClick,
  onTaskContextMenu,
  hasMore,
  onLoadMore,
  onDragStart,
  onDragUpdate,
  onDragEnd,
}: {
  columns: ColumnGroup[]
  groupedTasks: Record<string, Task[]>
  orderingEnabled: boolean
  orderingReason?: string
  movePending: boolean
  draggingTaskId?: string
  validDropStatuses: string[]
  activeDropStatus?: string
  quickCreateOpen: boolean
  quickCreateTitle: string
  quickCreateDescription: string
  quickCreateDescriptionRef: RefObject<HTMLTextAreaElement>
  createPending: boolean
  agentPickerTaskId?: string
  agents: Agent[]
  agentNamesById: Map<string, string>
  assignPending: boolean
  renderTaskMenuItems: (task: Task) => ReactNode
  onToggleQuickCreate: () => void
  onQuickCreateTitleChange: (title: string) => void
  onQuickCreateDescriptionChange: (description: string) => void
  onSubmitQuickCreate: () => void
  onCancelQuickCreate: () => void
  onAssignAgent: (task: Task, agentId: string) => void
  onAgentClick: (agentId: string) => void
  onTaskClick: (task: Task) => void
  onTaskContextMenu: (event: MouseEvent<HTMLElement>, task: Task) => void
  hasMore: boolean
  onLoadMore: () => void
  onDragStart: (start: DragStart) => void
  onDragUpdate: (update: DragUpdate) => void
  onDragEnd: (result: DropResult) => void
}) {
  return (
    <DragDropContext onDragEnd={onDragEnd} onDragStart={onDragStart} onDragUpdate={onDragUpdate}>
      <div
        className="min-h-0 flex-1 overflow-auto overscroll-contain rounded-xl [scrollbar-gutter:stable]"
        data-board-scroll-owner
        data-board-phase={movePending ? 'committing' : draggingTaskId ? 'dragging' : 'idle'}
      >
        <div className="flex min-h-full min-w-max items-stretch gap-2.5 pb-2 [--board-column-width:clamp(280px,82vw,320px)] sm:[--board-column-width:clamp(280px,42vw,340px)] lg:[--board-column-width:clamp(220px,19vw,280px)]">
          {columns.map((column) => (
            <KanbanColumn
              key={column.primaryState}
              column={column}
              tasks={groupedTasks[column.primaryState] ?? []}
              dragDisabled={!orderingEnabled}
              dragDisabledReason={orderingReason}
              movePending={movePending}
              validDropStatuses={draggingTaskId ? validDropStatuses : []}
              activeDropStatus={activeDropStatus}
              quickCreateOpen={quickCreateOpen}
              quickCreateTitle={quickCreateTitle}
              quickCreateDescription={quickCreateDescription}
              quickCreateDescriptionRef={quickCreateDescriptionRef}
              createPending={createPending}
              agentPickerTaskId={agentPickerTaskId}
              agents={agents}
              agentNamesById={agentNamesById}
              claimPending={assignPending}
              renderTaskMenuItems={renderTaskMenuItems}
              onToggleQuickCreate={onToggleQuickCreate}
              onQuickCreateTitleChange={onQuickCreateTitleChange}
              onQuickCreateDescriptionChange={onQuickCreateDescriptionChange}
              onSubmitQuickCreate={onSubmitQuickCreate}
              onCancelQuickCreate={onCancelQuickCreate}
              onAssignAgent={onAssignAgent}
              onAgentClick={onAgentClick}
              onTaskClick={onTaskClick}
              onTaskContextMenu={onTaskContextMenu}
              hasMore={hasMore}
              onLoadMore={onLoadMore}
            />
          ))}
        </div>
      </div>
    </DragDropContext>
  )
}
