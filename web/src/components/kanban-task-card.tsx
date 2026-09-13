import type { MouseEvent, ReactNode } from 'react'
import { Draggable } from '@hello-pangea/dnd'
import { CircleNotch, DotsSixVertical, DotsThree, UserCircle, Warning } from '@phosphor-icons/react'
import { useMembersQuery, useProjectAgentsQuery } from '@/api/hooks'
import { AgentAssigneeDropdown } from '@/components/task-controls'
import { Avatar } from '@/components/ui/avatar'
import { WorkflowHealthBadge } from '@/components/workflow-health-badge'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { cn } from '@/lib/cn'
import { getBlockingAnnotation, getStateColors, taskHasError } from '@/lib/workflow-utils'
import type { Agent, Task } from '@/types/generated'

export type TaskCardMenuRenderer = (task: Task) => ReactNode

function formatBlockingReason(value: string) {
  const withSpaces = value.replace(/_/g, ' ')
  return withSpaces.charAt(0).toUpperCase() + withSpaces.slice(1)
}

export function KanbanTaskCard({
  task,
  index,
  showSubStateBadge,
  subStateLabel,
  dragDisabled,
  dragDisabledReason,
  movePending,
  agentPickerTaskId,
  agentNamesById,
  claimPending,
  menuItems,
  onAssignAgent,
  onAgentClick,
  onClick,
  onContextMenu,
}: {
  task: Task
  index: number
  showSubStateBadge: boolean
  subStateLabel?: string
  dragDisabled: boolean
  dragDisabledReason?: string
  movePending: boolean
  agentPickerTaskId?: string
  agents: Agent[]
  agentNamesById: Map<string, string>
  claimPending: boolean
  menuItems: ReactNode
  onAssignAgent: (task: Task, agentId: string) => void
  onAgentClick?: (agentId: string) => void
  onClick: (task: Task) => void
  onContextMenu: (event: MouseEvent<HTMLElement>, task: Task) => void
}) {
  const { data: projectAgentsData } = useProjectAgentsQuery(task.project_id)
  const { data: membersData } = useMembersQuery(task.project_id)
  const coderAssignment = task.role_assignments.find(
    (assignment) => assignment.role_name === 'coder',
  )
  const coderAgentId =
    coderAssignment?.assignee_type === 'agent' ? coderAssignment.assignee_id : null
  const coderAgentName = coderAgentId ? (agentNamesById.get(coderAgentId) ?? coderAgentId) : null
  const coderUserId =
    coderAssignment?.assignee_type === 'user' ? (coderAssignment.assignee_id ?? 'manual') : null
  const coderIsHuman = coderAssignment?.assignee_type === 'user'
  const pausedAnnotation = getBlockingAnnotation(task)
  const blockedReason = task.blocked?.reason ?? pausedAnnotation?.blocking_reason
  const isPaused = task.status !== 'cancelled' && Boolean(blockedReason)
  const hasActiveError = taskHasError(task)
  return (
    <Draggable
      draggableId={task.id}
      index={index}
      isDragDisabled={dragDisabled}
      disableInteractiveElementBlocking
    >
      {(drag) => (
        <article
          ref={drag.innerRef}
          {...drag.draggableProps}
          className={cn(
            'group relative cursor-pointer rounded-lg border border-t-border-subtle border-r-border-subtle border-b-border-subtle border-l-[3px] bg-card p-2.5 text-left shadow-soft transition-[border-color,box-shadow,opacity] hover:border-t-border hover:border-r-border hover:border-b-border hover:shadow-card-hover',
            getStateColors(task.status).accent,
            task.status === 'in_progress' &&
              'animate-pulse-ember border-l-4 hover:shadow-[var(--shadow-card-hover),inset_6px_0_8px_-6px_rgba(249,115,22,0.5)]',
            (task.status === 'done' || task.status === 'cancelled') &&
              'opacity-55 hover:opacity-75',
          )}
          onContextMenu={(e) => onContextMenu(e, task)}
        >
          <button
            type="button"
            className="absolute inset-0 z-0 rounded-lg focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary focus-visible:ring-offset-1"
            aria-label={`Open ${task.title}`}
            onClick={() => onClick(task)}
          />
          <div className="pointer-events-none relative z-10 flex items-start gap-2">
            <div className="min-w-0 flex-1">
              <div className="flex items-start gap-1">
                <p className="line-clamp-2 min-w-0 flex-1 text-ui font-medium leading-snug">
                  {task.title}
                </p>
                {hasActiveError && (
                  <span className="mt-0.5 shrink-0" title="Task needs attention">
                    <Warning className="text-destructive" size={14} weight="fill" />
                  </span>
                )}
              </div>
              <div className="mt-1.5 flex flex-wrap items-center gap-1.5">
                {task.priority !== undefined && (
                  <span className="rounded bg-muted px-1.5 py-0.5 font-mono text-micro font-medium text-muted-foreground">
                    P{task.priority}
                  </span>
                )}
                {showSubStateBadge && (
                  <span
                    className={cn(
                      'rounded-full px-2 py-[2px] text-micro font-semibold',
                      getStateColors(task.status).bg,
                      getStateColors(task.status).text,
                    )}
                  >
                    {subStateLabel ?? task.status.replace(/_/g, ' ')}
                  </span>
                )}
                {isPaused ? (
                  <span className="rounded bg-red-50 px-1.5 py-0.5 text-micro font-medium text-red-700 dark:bg-red-500/10 dark:text-red-300">
                    {blockedReason ? formatBlockingReason(blockedReason) : 'Blocked'}
                  </span>
                ) : null}
                {task.workflow_health && task.workflow_health.severity !== 'info' ? (
                  <WorkflowHealthBadge health={task.workflow_health} compact />
                ) : null}
              </div>
              <div className="mt-1.5 flex items-center gap-1.5 text-[11px] text-muted-foreground">
                {coderAgentId ? (
                  <button
                    type="button"
                    className={cn(
                      'pointer-events-auto flex min-w-0 items-center gap-1.5 rounded transition-colors',
                      onAgentClick && 'cursor-pointer hover:text-foreground',
                    )}
                    onClick={
                      onAgentClick
                        ? (e) => {
                            e.stopPropagation()
                            onAgentClick(coderAgentId)
                          }
                        : undefined
                    }
                    title={onAgentClick ? `${coderAgentName} · Click to filter` : undefined}
                  >
                    <Avatar
                      name={coderAgentName ?? 'A'}
                      seed={coderAgentId}
                      size="xs"
                      className="h-4 w-4 shrink-0 rounded text-[8px]"
                    />
                    <span className="truncate">{coderAgentName}</span>
                  </button>
                ) : coderIsHuman ? (
                  <>
                    <UserCircle size={16} className="shrink-0" />
                    <span className="truncate">Human</span>
                  </>
                ) : (
                  <span className="italic">Unassigned</span>
                )}
              </div>
            </div>
            <div
              className="pointer-events-auto flex shrink-0 items-center gap-0.5"
              onClick={(e) => e.stopPropagation()}
            >
              <button
                type="button"
                {...drag.dragHandleProps}
                aria-label={`Move ${task.title}`}
                aria-busy={movePending}
                disabled={dragDisabled}
                title={dragDisabled ? dragDisabledReason : `Move ${task.title}`}
                className="flex h-8 w-8 cursor-grab items-center justify-center rounded-md text-muted-foreground opacity-70 transition-[background,color,opacity] hover:bg-accent hover:text-foreground hover:opacity-100 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-primary focus-visible:ring-offset-1 active:cursor-grabbing disabled:cursor-not-allowed disabled:opacity-30"
                onClick={(event) => event.stopPropagation()}
              >
                {movePending ? (
                  <CircleNotch size={15} className="motion-safe:animate-spin" />
                ) : (
                  <DotsSixVertical size={16} weight="bold" />
                )}
              </button>
              <DropdownMenu>
                <DropdownMenuTrigger
                  aria-label={`Open actions for ${task.title}`}
                  className="flex h-8 w-8 cursor-pointer items-center justify-center rounded-md text-muted-foreground opacity-0 transition-[background,color,opacity] hover:bg-accent hover:text-foreground group-hover:opacity-100 focus:opacity-100"
                >
                  <DotsThree size={16} weight="bold" />
                </DropdownMenuTrigger>
                <DropdownMenuContent align="end">{menuItems}</DropdownMenuContent>
              </DropdownMenu>
            </div>
          </div>
          {agentPickerTaskId === task.id && (
            <div
              className="pointer-events-auto relative z-10 mt-2"
              onClick={(e) => e.stopPropagation()}
              onMouseDown={(e) => e.stopPropagation()}
            >
              <AgentAssigneeDropdown
                agents={projectAgentsData ?? []}
                members={membersData}
                className="h-7 text-xs"
                disabled={claimPending}
                placeholder="Select agent"
                value={
                  coderAgentId
                    ? { type: 'agent', agentId: coderAgentId }
                    : coderIsHuman
                      ? { type: 'user', userId: coderUserId ?? 'manual' }
                      : { type: 'unassigned' }
                }
                onChange={(selection) => {
                  if (selection.type === 'agent') onAssignAgent(task, selection.agentId)
                }}
              />
            </div>
          )}
        </article>
      )}
    </Draggable>
  )
}
