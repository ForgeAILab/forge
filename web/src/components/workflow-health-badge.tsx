import { cn } from '@/lib/cn'
import type { WorkflowHealthSummary } from '@/types/generated'

const prominentKinds = new Set(['blocked', 'failed', 'stuck'])

export function workflowLabelFromKind(value: string): string {
  if (value === 'review_needs_owner') return 'Needs owner'
  if (value === 'defer_to_follow_up') return 'Defer to follow-up'
  return value
    .split('_')
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ')
}

export function WorkflowHealthBadge({
  health,
  compact = false,
  className,
}: {
  health: WorkflowHealthSummary
  compact?: boolean
  className?: string
}) {
  const prominent = prominentKinds.has(health.kind)
  const label = health.label || workflowLabelFromKind(health.kind)
  const title = [label, health.message].filter(Boolean).join(': ')

  return (
    <span
      title={title || undefined}
      className={cn(
        'inline-flex items-center gap-1 rounded px-2 py-[3px] text-micro font-semibold',
        compact && 'px-1.5 py-0.5',
        health.severity === 'error'
          ? prominent
            ? 'bg-red-500/15 text-red-700 ring-1 ring-inset ring-red-500/25 dark:text-red-300'
            : 'bg-red-500/10 text-red-700 dark:text-red-300'
          : health.severity === 'warning'
            ? prominent
              ? 'bg-amber-500/20 text-amber-800 ring-1 ring-inset ring-amber-500/30 dark:text-amber-300'
              : 'bg-amber-500/10 text-amber-700 dark:text-amber-300'
            : prominent
              ? 'bg-muted text-foreground ring-1 ring-inset ring-border'
              : 'bg-muted/70 text-muted-foreground',
        className,
      )}
    >
      <span
        className={cn(
          'h-1.5 w-1.5 rounded-full',
          health.severity === 'error'
            ? 'bg-red-500'
            : health.severity === 'warning'
              ? 'bg-amber-500'
              : 'bg-muted-foreground/60',
        )}
      />
      {label}
    </span>
  )
}
