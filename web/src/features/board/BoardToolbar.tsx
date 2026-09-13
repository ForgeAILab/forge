import type { RefObject } from 'react'
import { Funnel, MagnifyingGlass, Plus, X } from '@phosphor-icons/react'
import { AgentFilterGroup } from '@/components/agent-filter-group'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/cn'
import type { Agent } from '@/types/generated'

export type BoardFilterPatch = {
  agentIds?: string[]
  priorityMax?: number
  priorityMin?: number
  q?: string
  blockedOnly?: boolean
  includeCancelled?: boolean
  includeArchived?: boolean
}

export function BoardToolbar({
  agents,
  selectedAgentIds,
  q,
  priorityMin,
  priorityMax,
  blockedOnly,
  includeCancelled,
  includeArchived,
  showMobileFilters,
  searchInputRef,
  orderingMessage,
  onToggleMobileFilters,
  onFilterChange,
  onNewTask,
}: {
  agents: Agent[]
  selectedAgentIds: string[]
  q: string
  priorityMin?: number
  priorityMax?: number
  blockedOnly: boolean
  includeCancelled: boolean
  includeArchived: boolean
  showMobileFilters: boolean
  searchInputRef: RefObject<HTMLInputElement>
  orderingMessage?: string
  onToggleMobileFilters: () => void
  onFilterChange: (patch: BoardFilterPatch) => void
  onNewTask: () => void
}) {
  const hasActiveFilters =
    selectedAgentIds.length > 0 ||
    priorityMin !== undefined ||
    priorityMax !== undefined ||
    blockedOnly ||
    includeCancelled ||
    includeArchived
  const activeFilterCount = [
    selectedAgentIds.length > 0,
    priorityMin !== undefined || priorityMax !== undefined,
    blockedOnly,
    includeCancelled,
    includeArchived,
  ].filter(Boolean).length

  return (
    <div className="shrink-0 space-y-2" data-board-toolbar>
      <div className="flex min-w-0 items-center gap-2">
        <div className="relative min-w-0 flex-1 sm:max-w-md">
          <MagnifyingGlass
            size={15}
            className="pointer-events-none absolute left-3 top-1/2 -translate-y-1/2 text-muted-foreground"
          />
          <input
            ref={searchInputRef}
            aria-label="Search board tasks"
            className="h-9 w-full rounded-lg border border-input bg-card pl-9 pr-3 text-sm shadow-xs placeholder:text-muted-foreground focus:outline-none focus:ring-2 focus:ring-ring focus:ring-offset-1 focus:ring-offset-background"
            placeholder="Search tasks"
            value={q}
            onChange={(event) => onFilterChange({ q: event.target.value })}
          />
        </div>

        <button
          type="button"
          aria-label="Filters"
          aria-controls="board-filters"
          aria-expanded={showMobileFilters}
          className={cn(
            'flex h-9 shrink-0 cursor-pointer items-center gap-1.5 rounded-lg border border-input bg-card px-3 text-xs font-medium shadow-xs transition-[background-color,color,transform] hover:bg-accent focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring active:scale-[0.98]',
            hasActiveFilters || showMobileFilters ? 'text-foreground' : 'text-muted-foreground',
          )}
          onClick={onToggleMobileFilters}
        >
          <Funnel size={14} />
          <span className="hidden sm:inline">Filters</span>
          {activeFilterCount > 0 ? (
            <span className="flex h-4 min-w-4 items-center justify-center rounded-full bg-primary px-1 font-mono text-micro text-primary-foreground">
              {activeFilterCount}
            </span>
          ) : null}
        </button>

        <Button
          size="sm"
          className="h-9 shrink-0 gap-1.5 rounded-lg text-xs"
          aria-label="New task"
          onClick={onNewTask}
        >
          <Plus size={14} weight="bold" />
          <span className="hidden sm:inline">New Task</span>
          <span className="sm:hidden">New</span>
        </Button>
      </div>

      {showMobileFilters ? (
        <div
          id="board-filters"
          className="flex flex-wrap items-center gap-2 rounded-lg border border-border-subtle bg-card p-3 shadow-xs animate-slide-in"
        >
          {agents.length > 0 ? (
            <div className="flex items-center gap-2">
              <span className="text-xs font-medium text-muted-foreground">Assignee</span>
              <AgentFilterGroup
                agents={agents}
                selectedAgentIds={selectedAgentIds}
                onSelect={(agentIds) => onFilterChange({ agentIds })}
              />
            </div>
          ) : null}
          {[
            { key: 'blockedOnly' as const, label: 'Blocked', active: blockedOnly },
            { key: 'includeCancelled' as const, label: 'Cancelled', active: includeCancelled },
            { key: 'includeArchived' as const, label: 'Archived', active: includeArchived },
          ].map(({ key, label, active }) => (
            <button
              key={key}
              type="button"
              className={cn(
                'flex h-8 cursor-pointer items-center rounded-lg border px-3 text-xs font-medium transition-[background-color,color,transform] focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring active:scale-[0.98]',
                active
                  ? 'border-ember-border bg-ember-surface text-foreground'
                  : 'border-border-subtle text-muted-foreground hover:bg-accent hover:text-foreground',
              )}
              onClick={() => onFilterChange({ [key]: !active })}
            >
              {label}
            </button>
          ))}
          <div className="flex items-center gap-2 sm:ml-1">
            <span className="text-xs font-medium text-muted-foreground">Priority</span>
            <input
              aria-label="Minimum priority"
              className="h-8 w-16 rounded-lg border border-input bg-background px-2 text-xs focus:outline-none focus:ring-2 focus:ring-ring"
              min={0}
              placeholder="Min"
              type="number"
              value={priorityMin ?? ''}
              onChange={(event) =>
                onFilterChange({
                  priorityMin: event.target.value === '' ? undefined : Number(event.target.value),
                })
              }
            />
            <span className="text-xs text-muted-foreground">–</span>
            <input
              aria-label="Maximum priority"
              className="h-8 w-16 rounded-lg border border-input bg-background px-2 text-xs focus:outline-none focus:ring-2 focus:ring-ring"
              min={0}
              placeholder="Max"
              type="number"
              value={priorityMax ?? ''}
              onChange={(event) =>
                onFilterChange({
                  priorityMax: event.target.value === '' ? undefined : Number(event.target.value),
                })
              }
            />
            {priorityMin !== undefined || priorityMax !== undefined ? (
              <button
                type="button"
                aria-label="Clear priority filter"
                className="flex h-8 w-8 cursor-pointer items-center justify-center rounded-lg text-muted-foreground transition-colors hover:bg-accent hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                onClick={() => onFilterChange({ priorityMin: undefined, priorityMax: undefined })}
              >
                <X size={13} weight="bold" />
              </button>
            ) : null}
          </div>
          {hasActiveFilters || q ? (
            <button
              type="button"
              className="ml-auto h-8 cursor-pointer rounded-lg px-2.5 text-xs font-medium text-muted-foreground transition-colors hover:bg-accent hover:text-foreground focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
              onClick={() =>
                onFilterChange({
                  agentIds: [],
                  priorityMin: undefined,
                  priorityMax: undefined,
                  blockedOnly: false,
                  includeCancelled: false,
                  includeArchived: false,
                  q: '',
                })
              }
            >
              Clear all
            </button>
          ) : null}
        </div>
      ) : null}
      {orderingMessage ? (
        <p className="text-xs text-muted-foreground" role="status" data-ordering-status>
          {orderingMessage}
        </p>
      ) : null}
    </div>
  )
}
