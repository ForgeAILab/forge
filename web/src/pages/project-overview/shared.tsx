// Shared Project Overview status, section, and count components.

import type { ReactNode } from 'react'
import { CheckCircle, CircleNotch, XCircle } from '@phosphor-icons/react'
import { Card } from '@/components/ui/card'
import type { AcceptanceCheckSummary, TaskProgressCounts } from '@/types/generated'
import { humanize, statusClass, count } from './utils'

export function StatusLabel({ status }: { status: string }) {
  return (
    <span
      className={`inline-flex max-w-full items-center rounded-full border px-2 py-0.5 font-mono text-micro font-semibold uppercase tracking-[0.08em] ${statusClass(status)}`}
    >
      {humanize(status)}
    </span>
  )
}

export function SectionCard({
  title,
  eyebrow,
  children,
  className,
  action,
}: {
  title: string
  eyebrow?: string
  children: ReactNode
  className?: string
  action?: React.ReactNode
}) {
  return (
    <Card className={`min-w-0 border-border-subtle bg-card ${className ?? ''}`}>
      <div className="flex min-w-0 flex-wrap items-start justify-between gap-3 border-b border-border-subtle px-4 py-3 sm:px-5">
        <div className="min-w-0">
          {eyebrow ? (
            <p className="font-mono text-micro font-semibold uppercase tracking-[0.12em] text-muted-foreground">
              {eyebrow}
            </p>
          ) : null}
          <h2 className="mt-1 break-words text-sm font-semibold text-foreground">{title}</h2>
        </div>
        {action}
      </div>
      <div className="min-w-0 p-4 sm:p-5">{children}</div>
    </Card>
  )
}

export function MetricGrid({ counts }: { counts: TaskProgressCounts }) {
  const metrics = [
    ['Total', counts.total],
    ['Backlog', counts.backlog],
    ['Active', counts.active],
    ['Review', counts.review],
    ['Blocked', counts.blocked],
    ['Terminal', counts.terminal],
  ] as const

  return (
    <div className="grid grid-cols-2 gap-2 sm:grid-cols-3 xl:grid-cols-2">
      {metrics.map(([label, value]) => (
        <div
          key={label}
          className="min-w-0 rounded-md border border-border-subtle bg-muted/40 px-3 py-2"
        >
          <p className="text-xs text-muted-foreground">{label}</p>
          <p className="mt-1 font-mono text-lg font-semibold tabular-nums text-foreground">
            {count(value)}
          </p>
        </div>
      ))}
    </div>
  )
}

export function CheckSummary({ summary }: { summary: AcceptanceCheckSummary }) {
  const checks = [
    ['Passed', summary.passed, 'pass'],
    ['Failed', summary.failed, 'fail'],
    ['Missing', summary.missing, 'missing'],
    ['Stale', summary.stale, 'stale'],
    ['Waived', summary.waived, 'waived'],
    ['Unavailable', summary.unavailable, 'unavailable'],
  ] as const

  return (
    <div>
      <div className="mb-3 flex flex-wrap items-baseline justify-between gap-2">
        <p className="text-xs text-muted-foreground">Required acceptance checks</p>
        <p className="font-mono text-xs text-foreground">
          {count(summary.passed)} / {count(summary.required_total)} passed
        </p>
      </div>
      <div className="grid grid-cols-2 gap-2 sm:grid-cols-3">
        {checks.map(([label, value, state]) => (
          <div key={label} className="min-w-0 rounded-md border border-border-subtle px-3 py-2">
            <div className="flex items-center gap-1.5">
              {state === 'pass' ? (
                <CheckCircle size={14} className="shrink-0 text-success" aria-hidden />
              ) : state === 'fail' ? (
                <XCircle size={14} className="shrink-0 text-destructive" aria-hidden />
              ) : (
                <CircleNotch size={14} className="shrink-0 text-muted-foreground" aria-hidden />
              )}
              <span className="min-w-0 truncate text-xs text-muted-foreground">{label}</span>
            </div>
            <p className="mt-1 font-mono text-lg font-semibold tabular-nums text-foreground">
              {count(value)}
            </p>
          </div>
        ))}
      </div>
    </div>
  )
}

export function EmptyInline({ text }: { text: string }) {
  return <p className="mt-2 break-words text-xs leading-5 text-muted-foreground">{text}</p>
}
