// Execution setup state pills and readiness rows.

import { CheckCircle, CircleNotch, WarningCircle } from '@phosphor-icons/react'
import { stateLabel } from './utils'

function StateIcon({ value }: { value: string }) {
  if (value === 'ready' || value === 'active') {
    return <CheckCircle size={17} className="shrink-0 text-success" aria-hidden />
  }
  if (value === 'provisioning') {
    return (
      <CircleNotch
        size={17}
        className="shrink-0 animate-spin motion-reduce:animate-none text-primary"
        aria-hidden
      />
    )
  }
  if (value === 'failed' || value === 'unavailable' || value === 'reconciliation_required') {
    return <WarningCircle size={17} className="shrink-0 text-warning" aria-hidden />
  }
  return <CircleNotch size={17} className="shrink-0 text-muted-foreground" aria-hidden />
}

function StatePill({ value }: { value: string }) {
  return (
    <span className="inline-flex max-w-full items-center rounded-full border border-border-subtle bg-muted px-2 py-0.5 font-mono text-micro font-semibold uppercase tracking-[0.08em] text-muted-foreground">
      {stateLabel(value)}
    </span>
  )
}

export function ReadinessRow({
  label,
  value,
  description,
  compact = false,
}: {
  label: string
  value: string
  description: string
  compact?: boolean
}) {
  return (
    <li
      className={`flex min-w-0 items-start rounded-md border border-border-subtle bg-background px-3 ${compact ? 'gap-2 py-2.5' : 'gap-3 py-3'}`}
    >
      <StateIcon value={value} />
      <div className="min-w-0 flex-1">
        <div className="flex min-w-0 flex-wrap items-center gap-2">
          <p className="text-sm font-semibold text-foreground">{label}</p>
          <StatePill value={value} />
        </div>
        <p className="mt-1 break-words text-xs leading-5 text-muted-foreground">{description}</p>
      </div>
    </li>
  )
}
