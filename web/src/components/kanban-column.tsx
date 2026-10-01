import type { MouseEvent, ReactNode, RefObject } from 'react'
import { useEffect, useRef } from 'react'
import { Droppable } from '@hello-pangea/dnd'
import { Plus } from '@phosphor-icons/react'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Textarea } from '@/components/ui/textarea'
import { cn } from '@/lib/cn'
import { isInitialKind } from '@/lib/workflow-utils'
import type { Agent } from '@/types/generated'
import type { TaskListItem as Task } from '@/types/task-list'
import type { ColumnGroup } from '@/lib/workflow-utils'
import { KanbanTaskCard } from './kanban-task-card'

export function KanbanColumn({
  column,
  tasks,
  dragDisabled,
  dragDisabledReason,
  movePending,
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
  claimPending,
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
  onLoadMore,
  hasMore,
}: {
  column: ColumnGroup
  tasks: Task[]
  dragDisabled: boolean
  dragDisabledReason?: string
  movePending: boolean
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
  claimPending: boolean
  renderTaskMenuItems: (task: Task) => ReactNode
  onToggleQuickCreate: () => void
  onQuickCreateTitleChange: (title: string) => void
  onQuickCreateDescriptionChange: (description: string) => void
  onSubmitQuickCreate: () => void
  onCancelQuickCreate: () => void
  onAssignAgent: (task: Task, agentId: string) => void
  onAgentClick?: (agentId: string) => void
  onTaskClick: (task: Task) => void
  onTaskContextMenu: (event: MouseEvent<HTMLElement>, task: Task) => void
  onLoadMore?: () => void
  hasMore?: boolean
}) {
  const sentinelRef = useRef<HTMLDivElement>(null)

  useEffect(() => {
    if (!onLoadMore || !hasMore) return
    const sentinel = sentinelRef.current
    if (!sentinel) return
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) onLoadMore()
      },
      { threshold: 0 },
    )
    observer.observe(sentinel)
    return () => observer.disconnect()
  }, [onLoadMore, hasMore])

  return (
    <Droppable key={column.primaryState} droppableId={column.primaryState}>
      {(provided) => (
        <section
          ref={provided.innerRef}
          {...provided.droppableProps}
          aria-label={`${column.columnName} column`}
          className={cn(
            'flex w-[var(--board-column-width)] min-w-[var(--board-column-width)] flex-none flex-col rounded-xl border border-border-subtle bg-background p-2 transition-colors',
            validDropStatuses.includes(column.primaryState) &&
              (activeDropStatus === column.primaryState
                ? 'bg-primary/5 ring-2 ring-primary/30'
                : 'bg-primary/[0.03]'),
          )}
        >
          <header className="mb-2 flex items-center justify-between px-1.5 py-1.5">
            <div className="flex items-center gap-2">
              <span className={cn('h-2 w-2 rounded-full', column.dotColor)} />
              <span className="font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
                {column.columnName}
              </span>
              {isInitialKind(column.kind) && (
                <button
                  aria-label="Quick create task"
                  className="flex h-5 w-5 cursor-pointer items-center justify-center rounded text-muted-foreground transition-colors hover:bg-muted hover:text-primary"
                  type="button"
                  onClick={onToggleQuickCreate}
                >
                  <Plus size={13} weight="bold" />
                </button>
              )}
            </div>
            <span className="flex h-5 min-w-[20px] items-center justify-center rounded-md bg-muted px-1.5 font-mono text-micro font-medium text-muted-foreground">
              {tasks.length}
            </span>
          </header>

          {isInitialKind(column.kind) && quickCreateOpen && (
            <div
              className="mb-2 space-y-1.5 rounded-lg border bg-card p-2.5 shadow-soft animate-slide-in"
              onClick={(event) => event.stopPropagation()}
            >
              <Input
                autoFocus
                className="h-7 rounded-md border-0 bg-muted/50 px-2 text-xs focus-visible:ring-1"
                disabled={createPending}
                placeholder="Task title"
                value={quickCreateTitle}
                onChange={(event) => onQuickCreateTitleChange(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === 'Enter') {
                    event.preventDefault()
                    quickCreateDescriptionRef.current?.focus()
                  }
                  if (event.key === 'Escape') {
                    onCancelQuickCreate()
                  }
                }}
              />
              <Textarea
                ref={quickCreateDescriptionRef}
                className="min-h-[48px] resize-none rounded-md border-0 bg-muted/50 px-2 py-1.5 text-xs focus-visible:ring-1"
                disabled={createPending}
                placeholder="Task description"
                value={quickCreateDescription}
                onChange={(event) => onQuickCreateDescriptionChange(event.target.value)}
                onKeyDown={(event) => {
                  if (event.key === 'Escape') {
                    event.preventDefault()
                    onCancelQuickCreate()
                  }
                }}
              />
              <div className="flex items-center gap-1">
                <Button
                  className="h-6 rounded-md px-2 text-[11px]"
                  disabled={
                    createPending || !quickCreateTitle.trim() || !quickCreateDescription.trim()
                  }
                  size="sm"
                  onClick={onSubmitQuickCreate}
                >
                  Create
                </Button>
                <Button
                  className="h-6 rounded-md px-2 text-[11px]"
                  size="sm"
                  type="button"
                  variant="ghost"
                  onClick={onCancelQuickCreate}
                >
                  Cancel
                </Button>
              </div>
            </div>
          )}

          <div className="min-h-0 flex-1 space-y-1.5">
            {tasks.map((task, index) => (
              <KanbanTaskCard
                key={task.id}
                task={task}
                index={index}
                showSubStateBadge={column.states.length > 1}
                subStateLabel={column.stateLabels?.[task.status]}
                dragDisabled={dragDisabled}
                dragDisabledReason={dragDisabledReason}
                movePending={movePending}
                agentPickerTaskId={agentPickerTaskId}
                agents={agents}
                agentNamesById={agentNamesById}
                claimPending={claimPending}
                menuItems={renderTaskMenuItems(task)}
                onAssignAgent={onAssignAgent}
                onAgentClick={onAgentClick}
                onClick={onTaskClick}
                onContextMenu={onTaskContextMenu}
              />
            ))}
            {provided.placeholder}
            {hasMore && <div ref={sentinelRef} className="h-1 w-full shrink-0" />}
          </div>
        </section>
      )}
    </Droppable>
  )
}
