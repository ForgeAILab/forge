import { useCallback, useEffect, useRef, useState } from 'react'
import { createPortal } from 'react-dom'
import { CaretDown, Check, MagnifyingGlass, UserCircle } from '@phosphor-icons/react'
import { Avatar } from '@/components/ui/avatar'
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu'
import { cn } from '@/lib/cn'
import { isAgentUsable } from '@/lib/agent-availability'
import { productTerm } from '@/lib/i18n'
import { getStateColors } from '@/lib/workflow-utils'
import type { Agent } from '@/types/generated'

export type AssigneeSelection =
  | { type: 'agent'; agentId: string }
  | { type: 'user'; userId: string }
  | { type: 'unassigned' }

type MemberAssignee = {
  user_id: string
  email: string
  display_name?: string | null
  role: string
}

export const taskStatusTransitions: Record<string, string[]> = {
  todo: ['in_progress', 'cancelled'],
  in_progress: ['todo', 'review', 'cancelled'],
  review: ['merging', 'in_progress', 'cancelled'],
  merging: ['review', 'in_progress', 'done', 'merge_failed', 'cancelled'],
  merge_failed: ['in_progress', 'cancelled'],
  done: [],
  cancelled: [],
}

export const taskStatusColors: Record<string, { dot: string; bg: string; text: string }> = {
  todo: {
    dot: 'bg-stone-400',
    bg: 'bg-stone-400/15',
    text: 'text-stone-600 dark:text-stone-400',
  },
  in_progress: {
    dot: 'bg-orange-500',
    bg: 'bg-orange-500/12',
    text: 'text-orange-700 dark:text-orange-300',
  },
  review: {
    dot: 'bg-violet-400',
    bg: 'bg-violet-400/12',
    text: 'text-violet-700 dark:text-violet-300',
  },
  merging: {
    dot: 'bg-amber-400',
    bg: 'bg-amber-400/12',
    text: 'text-amber-600 dark:text-amber-200',
  },
  merge_failed: {
    dot: 'bg-red-500',
    bg: 'bg-red-500/12',
    text: 'text-red-700 dark:text-red-300',
  },
  done: {
    dot: 'bg-stone-500',
    bg: 'bg-stone-500/15',
    text: 'text-stone-600 dark:text-stone-500',
  },
  cancelled: {
    dot: 'bg-stone-400',
    bg: 'bg-stone-400/10',
    text: 'text-stone-500 dark:text-stone-500',
  },
}

function resolveStatusColors(status: string): { dot: string; bg: string; text: string } {
  return taskStatusColors[status] ?? getStateColors(status)
}

const agentStatusDotClasses: Record<string, string> = {
  idle: 'bg-emerald-500',
  busy: 'bg-sky-500',
  error: 'bg-red-500',
  offline: 'bg-zinc-400',
}

function formatTaskStatus(status: string): string {
  return status.replace(/_/g, ' ')
}

export function getAvailableTaskTransitions(status: string): string[] {
  return taskStatusTransitions[status] ?? []
}

export function TaskStatusBadge({ status, className }: { status: string; className?: string }) {
  const colors = resolveStatusColors(status)
  return (
    <span
      className={cn(
        'inline-flex w-fit items-center gap-1.5 rounded-full px-2 py-[2px] text-micro font-semibold',
        colors.bg,
        colors.text,
        className,
      )}
    >
      {formatTaskStatus(status)}
    </span>
  )
}

export function TaskStatusDropdown({
  status,
  availableStatuses,
  className,
  disabled,
  disabledStatusReasons,
  onChange,
}: {
  status: string
  availableStatuses?: string[]
  className?: string
  disabled?: boolean
  disabledStatusReasons?: Record<string, string | undefined>
  onChange: (status: string) => void
}) {
  const statuses = availableStatuses ?? getAvailableTaskTransitions(status)
  const colors = resolveStatusColors(status)

  if (statuses.length === 0) {
    return <TaskStatusBadge status={status} className={className} />
  }

  return (
    <DropdownMenu className="block w-full">
      <DropdownMenuTrigger
        aria-label={`Move ${productTerm('phase').toLowerCase()} from ${formatTaskStatus(status)}`}
        className={cn(
          'inline-flex w-full cursor-pointer items-center justify-between gap-2 rounded-md px-2 py-[3px] text-xs font-medium transition-colors hover:opacity-90 focus:outline-none focus:ring-2 focus:ring-ring disabled:cursor-not-allowed disabled:opacity-50',
          colors.bg,
          colors.text,
          className,
        )}
        disabled={disabled}
      >
        <span className="inline-flex min-w-0 items-center gap-1.5">
          <span className={cn('h-1.5 w-1.5 shrink-0 rounded-full', colors.dot)} />
          <span className="truncate">{formatTaskStatus(status)}</span>
        </span>
        <CaretDown size={11} className="shrink-0" />
      </DropdownMenuTrigger>
      <DropdownMenuContent className="w-44">
        {statuses.map((nextStatus) => {
          const nextColors = resolveStatusColors(nextStatus)
          const disabledReason = disabledStatusReasons?.[nextStatus]
          const item = (
            <DropdownMenuItem
              key={nextStatus}
              className="gap-2"
              disabled={Boolean(disabledReason)}
              onClick={() => onChange(nextStatus)}
            >
              <span className={cn('h-2 w-2 rounded-full', nextColors.dot)} />
              <span className="flex-1 text-left">{formatTaskStatus(nextStatus)}</span>
            </DropdownMenuItem>
          )
          return disabledReason ? (
            <div key={nextStatus} title={disabledReason}>
              {item}
            </div>
          ) : (
            item
          )
        })}
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

export function AgentAssigneeDropdown({
  agents,
  members,
  value,
  className,
  disabled,
  fallbackName,
  placeholder = 'Select agent',
  onChange,
  variant = 'full',
  roleLabel,
  requiredNow,
}: {
  agents: Agent[]
  members?: MemberAssignee[]
  value?: AssigneeSelection | null
  className?: string
  disabled?: boolean
  fallbackName?: string
  placeholder?: string
  onChange: (selection: AssigneeSelection) => void
  variant?: 'full' | 'chip'
  roleLabel?: string
  requiredNow?: boolean
}) {
  const [open, setOpen] = useState(false)
  const [query, setQuery] = useState('')
  const triggerRef = useRef<HTMLButtonElement>(null)
  const dropdownRef = useRef<HTMLDivElement>(null)
  const searchRef = useRef<HTMLInputElement>(null)
  const [style, setStyle] = useState<React.CSSProperties>({})

  const selectedAgentId = value?.type === 'agent' ? value.agentId : undefined
  const selectedAgent = selectedAgentId
    ? agents.find((agent) => agent.id === selectedAgentId)
    : undefined
  const selectedUserId = value?.type === 'user' ? value.userId : undefined
  const selectedMember = selectedUserId
    ? members?.find((member) => member.user_id === selectedUserId)
    : undefined
  const selectedName =
    value?.type === 'user'
      ? members
        ? selectedMember
          ? (selectedMember.display_name ?? selectedMember.email)
          : 'Unknown user'
        : 'Human'
      : (selectedAgent?.name ?? fallbackName ?? selectedAgentId)
  const isHuman = value?.type === 'user'
  const isChip = variant === 'chip'
  const avatarSize = isChip ? 'xs' : 'sm'
  const placeholderBox = isChip ? 'h-5 w-5 text-[11px]' : 'h-6 w-6'

  const updatePosition = useCallback(() => {
    if (!triggerRef.current) return
    const rect = triggerRef.current.getBoundingClientRect()
    setStyle({
      position: 'fixed',
      zIndex: 9999,
      top: rect.bottom + 4,
      left: rect.left,
      minWidth: Math.max(rect.width, 288),
    })
  }, [])

  useEffect(() => {
    if (!open) {
      setQuery('')
      return
    }
    updatePosition()
    requestAnimationFrame(() => searchRef.current?.focus())

    const onMouseDown = (e: globalThis.MouseEvent) => {
      if (
        dropdownRef.current &&
        !dropdownRef.current.contains(e.target as Node) &&
        triggerRef.current &&
        !triggerRef.current.contains(e.target as Node)
      )
        setOpen(false)
    }
    const onScroll = () => updatePosition()
    const onKey = (e: globalThis.KeyboardEvent) => {
      if (e.key === 'Escape') setOpen(false)
    }
    document.addEventListener('mousedown', onMouseDown)
    document.addEventListener('keydown', onKey)
    window.addEventListener('scroll', onScroll, true)
    return () => {
      document.removeEventListener('mousedown', onMouseDown)
      document.removeEventListener('keydown', onKey)
      window.removeEventListener('scroll', onScroll, true)
    }
  }, [open, updatePosition])

  const normalizedQuery = query.toLowerCase()

  const availableAgents = agents.filter(isAgentUsable)
  const filtered = normalizedQuery
    ? availableAgents.filter((a) => a.name.toLowerCase().includes(normalizedQuery))
    : availableAgents

  const filteredMembers = members
    ? normalizedQuery
      ? members.filter((member) => {
          const displayName = member.display_name ?? ''
          return (
            displayName.toLowerCase().includes(normalizedQuery) ||
            member.email.toLowerCase().includes(normalizedQuery)
          )
        })
      : members
    : undefined

  const showHuman =
    !members &&
    (!normalizedQuery || 'human'.includes(normalizedQuery) || 'manual'.includes(normalizedQuery))
  const manualSelected = value?.type === 'user' && (!selectedUserId || selectedUserId === 'manual')

  const select = (selection: AssigneeSelection) => {
    onChange(selection)
    setOpen(false)
  }

  return (
    <>
      <button
        ref={triggerRef}
        type="button"
        aria-label={roleLabel ? `Assign ${roleLabel}` : 'Assign agent'}
        aria-haspopup="listbox"
        aria-expanded={open}
        className={cn(
          isChip
            ? 'group flex h-7 max-w-full cursor-pointer items-center gap-1.5 rounded-full border border-input bg-background pl-1 pr-2 text-xs transition-colors hover:bg-accent focus:outline-none focus:ring-2 focus:ring-ring disabled:cursor-not-allowed disabled:opacity-50'
            : 'flex w-full cursor-pointer items-center justify-between gap-2 rounded-md border border-input bg-background px-2.5 py-[5px] text-xs transition-colors hover:bg-accent focus:outline-none focus:ring-2 focus:ring-ring disabled:cursor-not-allowed disabled:opacity-50',
          isChip && requiredNow && 'border-primary ring-1 ring-primary/40',
          className,
        )}
        disabled={disabled}
        onClick={() => {
          if (!disabled) {
            if (!open) updatePosition()
            setOpen((v) => !v)
          }
        }}
      >
        <span className="flex min-w-0 items-center gap-1.5">
          {selectedName && selectedAgentId ? (
            <Avatar name={selectedName} seed={selectedAgentId} size={avatarSize} />
          ) : (
            <span
              className={cn(
                'flex shrink-0 items-center justify-center rounded-full bg-muted text-muted-foreground',
                placeholderBox,
              )}
            >
              {isHuman ? <UserCircle size={isChip ? 12 : 16} weight="fill" /> : <span>—</span>}
            </span>
          )}
          {isChip && roleLabel ? (
            <span className="shrink-0 font-medium text-muted-foreground">{roleLabel}:</span>
          ) : null}
          <span
            className={cn(
              'min-w-0 flex-1 truncate',
              !selectedName && 'text-muted-foreground',
              isChip && 'max-w-[6.5rem]',
            )}
            title={selectedName ?? (isChip ? 'Unassigned' : placeholder)}
          >
            {selectedName ?? (isChip ? 'Unassigned' : placeholder)}
          </span>
        </span>
        <CaretDown size={isChip ? 11 : 13} className="shrink-0 text-muted-foreground" />
      </button>

      {open &&
        createPortal(
          <div
            ref={dropdownRef}
            role="listbox"
            style={style}
            className="flex flex-col overflow-hidden rounded-lg border border-border-subtle bg-popover text-popover-foreground shadow-float animate-slide-in"
          >
            {/* Search */}
            <div className="flex items-center gap-2 border-b border-border-subtle px-2 py-1.5">
              <MagnifyingGlass size={12} className="shrink-0 text-muted-foreground" />
              <input
                ref={searchRef}
                type="text"
                value={query}
                placeholder="Search agents…"
                className="min-w-0 flex-1 bg-transparent py-0.5 text-xs outline-none placeholder:text-muted-foreground"
                onChange={(e) => setQuery(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Escape') setOpen(false)
                }}
              />
            </div>

            {/* Scrollable list */}
            <div className="max-h-60 overflow-y-auto p-1">
              {filteredMembers?.map((member) => {
                const selected = selectedUserId === member.user_id
                return (
                  <button
                    key={member.user_id}
                    type="button"
                    role="option"
                    aria-selected={selected}
                    className={cn(
                      'flex w-full cursor-pointer items-center gap-2 rounded-sm p-2 text-left outline-none transition-colors hover:bg-accent',
                      selected && 'bg-accent/50',
                    )}
                    onClick={() => select({ type: 'user', userId: member.user_id })}
                  >
                    <span className="flex h-6 w-6 shrink-0 items-center justify-center rounded-md bg-muted text-muted-foreground">
                      <UserCircle size={16} weight="fill" />
                    </span>
                    <span className="min-w-0 flex-1">
                      <span className="block truncate text-sm font-medium">
                        {member.display_name ?? member.email}
                      </span>
                      <span className="mt-0.5 block text-xs text-muted-foreground">
                        {member.role}
                      </span>
                    </span>
                    {selected ? <Check size={14} className="shrink-0 text-primary" /> : null}
                  </button>
                )
              })}
              {showHuman && (
                <button
                  type="button"
                  role="option"
                  aria-selected={manualSelected}
                  className={cn(
                    'flex w-full cursor-pointer items-center gap-2 rounded-sm p-2 text-left outline-none transition-colors hover:bg-accent',
                    manualSelected && 'bg-accent/50',
                  )}
                  onClick={() => select({ type: 'user', userId: 'manual' })}
                >
                  <span className="flex h-6 w-6 shrink-0 items-center justify-center rounded-md bg-muted text-muted-foreground">
                    <UserCircle size={16} weight="fill" />
                  </span>
                  <span className="min-w-0 flex-1">
                    <span className="block truncate text-sm font-medium">Human (manual)</span>
                    <span className="mt-0.5 block text-xs text-muted-foreground">Manual work</span>
                  </span>
                  {manualSelected ? <Check size={14} className="shrink-0 text-primary" /> : null}
                </button>
              )}
              {filtered.length === 0 &&
              !showHuman &&
              (!filteredMembers || filteredMembers.length === 0) ? (
                <p className="px-2 py-3 text-center text-xs text-muted-foreground">No matches</p>
              ) : (
                filtered.map((agent) => {
                  const selected = agent.id === selectedAgentId
                  const activeTasks = agent.active_task_count ?? 0
                  return (
                    <button
                      key={agent.id}
                      type="button"
                      role="option"
                      aria-selected={selected}
                      className={cn(
                        'flex w-full cursor-pointer items-center gap-2 rounded-sm p-2 text-left outline-none transition-colors hover:bg-accent',
                        selected && 'bg-accent/50',
                      )}
                      onClick={() => select({ type: 'agent', agentId: agent.id })}
                    >
                      <Avatar name={agent.name} seed={agent.id} size="sm" />
                      <span className="min-w-0 flex-1">
                        <span className="block truncate text-sm font-medium">{agent.name}</span>
                        <span className="mt-0.5 flex items-center gap-1.5 text-xs text-muted-foreground">
                          <span
                            className={cn(
                              'h-1.5 w-1.5 rounded-full',
                              agentStatusDotClasses[agent.status] ?? agentStatusDotClasses.offline,
                            )}
                          />
                          {agent.status}
                          <span aria-hidden="true">/</span>
                          {activeTasks}/{agent.max_concurrent_tasks}
                        </span>
                      </span>
                      {selected ? <Check size={14} className="shrink-0 text-primary" /> : null}
                    </button>
                  )
                })
              )}
            </div>

            {/* Pinned unassigned */}
            <div className="border-t border-border-subtle p-1">
              <button
                type="button"
                role="option"
                aria-selected={value?.type === 'unassigned' || !value}
                className={cn(
                  'flex w-full cursor-pointer items-center gap-2 rounded-sm p-2 text-left text-muted-foreground outline-none transition-colors hover:bg-accent',
                  (value?.type === 'unassigned' || !value) && 'bg-accent/50',
                )}
                onClick={() => select({ type: 'unassigned' })}
              >
                <span className="flex h-6 w-6 shrink-0 items-center justify-center rounded-md bg-muted">
                  —
                </span>
                <span className="min-w-0 flex-1">
                  <span className="block truncate text-sm font-medium">Unassigned</span>
                  <span className="mt-0.5 block text-xs">Remove assignment</span>
                </span>
                {value?.type === 'unassigned' || !value ? (
                  <Check size={14} className="shrink-0 text-primary" />
                ) : null}
              </button>
            </div>
          </div>,
          document.body,
        )}
    </>
  )
}
