import { useState } from 'react'
import {
  ArrowClockwise,
  CaretDown,
  CaretRight,
  CircleNotch,
  Warning,
  WarningCircle,
  WarningOctagon,
} from '@phosphor-icons/react'
import { Button } from '@/components/ui/button'
import { cn } from '@/lib/cn'

export type ErrorCardSeverity = 'error' | 'conflict' | 'warning'

export type ErrorCardAction = {
  label: string
  onClick: () => void
  isPending?: boolean
}

export type ErrorCardProps = {
  title: string
  description?: string
  severity?: ErrorCardSeverity
  action?: ErrorCardAction
  technicalDetails?: unknown
  className?: string
}

function severityStyles(severity: ErrorCardSeverity) {
  switch (severity) {
    case 'conflict':
      return {
        card: 'border-warning/30 bg-warning/5 text-foreground',
        icon: <Warning className="h-4 w-4 shrink-0 text-warning" aria-hidden />,
        badge: 'border-warning/30 bg-warning/10 text-warning',
        badgeText: 'Conflict',
      }
    case 'warning':
      return {
        card: 'border-warning/30 bg-warning/5 text-foreground',
        icon: <WarningCircle className="h-4 w-4 shrink-0 text-warning" aria-hidden />,
        badge: 'border-warning/30 bg-warning/10 text-warning',
        badgeText: 'Warning',
      }
    case 'error':
    default:
      return {
        card: 'border-destructive/30 bg-destructive/5 text-foreground',
        icon: <WarningOctagon className="h-4 w-4 shrink-0 text-destructive" aria-hidden />,
        badge: 'border-destructive/30 bg-destructive/10 text-destructive',
        badgeText: 'Error',
      }
  }
}

export function ErrorCard({
  title,
  description,
  severity = 'error',
  action,
  technicalDetails,
  className,
}: ErrorCardProps) {
  const [detailsOpen, setDetailsOpen] = useState(false)
  const styles = severityStyles(severity)

  const hasTechnicalDetails =
    technicalDetails !== undefined &&
    technicalDetails !== null &&
    (typeof technicalDetails === 'string'
      ? technicalDetails.trim().length > 0
      : Object.keys(technicalDetails).length > 0)

  const formattedDetails = hasTechnicalDetails
    ? typeof technicalDetails === 'string'
      ? technicalDetails
      : JSON.stringify(technicalDetails, null, 2)
    : null

  return (
    <div
      role="alert"
      className={cn(
        'animate-pop-in flex flex-col gap-3 rounded-lg border p-3.5 shadow-xs transition-colors',
        styles.card,
        className,
      )}
    >
      <div className="flex items-start justify-between gap-3">
        <div className="flex items-start gap-2.5 min-w-0">
          <div className="mt-0.5">{styles.icon}</div>
          <div className="min-w-0 flex-1">
            <div className="flex items-center gap-2">
              <h4 className="text-xs font-semibold text-foreground tracking-tight">{title}</h4>
              <span
                className={cn(
                  'inline-flex items-center rounded px-1.5 py-0.5 text-micro font-medium uppercase tracking-wider',
                  styles.badge,
                )}
              >
                {styles.badgeText}
              </span>
            </div>
            {description ? (
              <p className="mt-1 text-xs text-muted-foreground leading-relaxed">{description}</p>
            ) : null}
          </div>
        </div>
        {action ? (
          <Button
            type="button"
            variant="ghost"
            size="sm"
            onClick={action.onClick}
            disabled={action.isPending}
            className="shrink-0 h-7 text-xs font-medium gap-1.5"
          >
            {action.isPending ? (
              <CircleNotch size={13} className="animate-spin" aria-hidden />
            ) : (
              <ArrowClockwise size={13} aria-hidden />
            )}
            {action.label}
          </Button>
        ) : null}
      </div>

      {hasTechnicalDetails ? (
        <details
          className="mt-1 rounded-md border border-border-subtle bg-background/50 text-xs"
          open={detailsOpen}
          onToggle={(e) => setDetailsOpen(e.currentTarget.open)}
        >
          <summary className="flex cursor-pointer items-center gap-1.5 px-2.5 py-1.5 text-micro font-medium text-muted-foreground hover:text-foreground select-none">
            {detailsOpen ? <CaretDown size={12} /> : <CaretRight size={12} />}
            <span>Technical details</span>
          </summary>
          <div className="border-t border-border-subtle p-2.5">
            <pre className="max-h-48 overflow-auto font-mono text-micro text-foreground/90 whitespace-pre-wrap break-all">
              {formattedDetails}
            </pre>
          </div>
        </details>
      ) : null}
    </div>
  )
}
