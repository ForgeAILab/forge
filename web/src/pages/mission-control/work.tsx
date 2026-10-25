// Mission Control work and outcome rows.

import { Link } from '@tanstack/react-router'
import { ArrowUpRight } from '@phosphor-icons/react'
import type { MissionControlWorkItem, OutcomeItem } from '@/features/federation/types'
import { StateBadge } from '@/features/federation/components'
import { formatDate } from './utils'

export function WorkRow({ item }: { item: MissionControlWorkItem }) {
  return (
    <Link
      to="/tasks/$taskId"
      params={{ taskId: item.task_id }}
      className="flex items-center justify-between gap-3 px-4 py-3 transition-colors hover:bg-muted/30 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-inset focus-visible:ring-ring"
    >
      <div className="min-w-0">
        <p className="truncate text-sm font-medium text-foreground">{item.title}</p>
        <p className="mt-1 text-xs text-muted-foreground">
          Project {item.project_id.slice(0, 8)} · {item.primary_action}
        </p>
      </div>
      <div className="flex shrink-0 items-center gap-2">
        <StateBadge status={item.status} />
        <ArrowUpRight size={15} className="text-muted-foreground" aria-hidden />
      </div>
    </Link>
  )
}

export function OutcomeRow({ item }: { item: OutcomeItem }) {
  return (
    <div className="flex flex-wrap items-center justify-between gap-3 border-b border-border-subtle px-4 py-3 last:border-b-0">
      <div className="min-w-0">
        <p className="truncate text-sm font-medium text-foreground">{item.title}</p>
        <p className="mt-1 text-xs text-muted-foreground">
          Project {item.project_id.slice(0, 8)} · {item.outcome}
        </p>
      </div>
      <span className="font-mono text-micro text-muted-foreground">
        {formatDate(item.occurred_at)}
      </span>
    </div>
  )
}
