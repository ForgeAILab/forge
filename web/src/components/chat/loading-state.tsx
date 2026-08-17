import { useEffect, useState } from 'react'
import { cn } from '@/lib/cn'

export type LoadingStateProps = {
  label?: string
  startedAt?: string | number | Date | null
  status?: string
  className?: string
  compact?: boolean
}

function formatElapsed(seconds: number): string {
  const mins = Math.floor(seconds / 60)
  const secs = seconds % 60
  return `${mins.toString().padStart(2, '0')}:${secs.toString().padStart(2, '0')}`
}

function PixelGridLoader() {
  // 3x3 grid of pixels with staggered animation delays
  const delays = [
    '0ms', '150ms', '300ms',
    '450ms', '600ms', '750ms',
    '300ms', '450ms', '600ms',
  ]

  return (
    <div
      className="grid grid-cols-3 gap-0.5 w-3.5 h-3.5 shrink-0 items-center justify-center"
      aria-hidden="true"
    >
      {delays.map((delay, index) => (
        <div
          key={index}
          className="w-1 h-1 rounded-[0.5px] bg-primary animate-pixel-on opacity-20"
          style={{ animationDelay: delay }}
        />
      ))}
    </div>
  )
}

export function LoadingState({
  label = 'Thinking…',
  startedAt,
  status,
  className,
  compact = false,
}: LoadingStateProps) {
  const [elapsed, setElapsed] = useState(() => {
    if (!startedAt) return 0
    const start = new Date(startedAt).getTime()
    return Math.max(0, Math.floor((Date.now() - start) / 1000))
  })

  useEffect(() => {
    const startTime = startedAt ? new Date(startedAt).getTime() : Date.now()
    const interval = setInterval(() => {
      setElapsed(Math.max(0, Math.floor((Date.now() - startTime) / 1000)))
    }, 1000)

    return () => clearInterval(interval)
  }, [startedAt])

  const displayLabel = status === 'awaiting_input'
    ? 'Awaiting input…'
    : label

  if (compact) {
    return (
      <div
        className={cn(
          'inline-flex items-center gap-2 text-xs text-muted-foreground',
          className,
        )}
        aria-busy="true"
        aria-live="polite"
      >
        <PixelGridLoader />
        <span className="animate-shimmer-text font-medium text-xs">
          {displayLabel}
        </span>
        <span className="font-mono text-micro tabular-nums text-muted-foreground/80">
          {formatElapsed(elapsed)}
        </span>
      </div>
    )
  }

  return (
    <div
      className={cn(
        'animate-fade-up flex items-center justify-between gap-3 rounded-lg border border-border-subtle bg-card/60 px-3.5 py-2.5 shadow-xs transition-colors',
        className,
      )}
      aria-busy="true"
      aria-live="polite"
    >
      <div className="flex items-center gap-2.5 min-w-0">
        <PixelGridLoader />
        <span className="animate-shimmer-text font-medium text-xs tracking-tight truncate">
          {displayLabel}
        </span>
      </div>
      <div className="flex items-center gap-2 shrink-0">
        <span className="rounded bg-muted/60 px-1.5 py-0.5 font-mono text-micro font-medium tabular-nums text-muted-foreground">
          {formatElapsed(elapsed)}
        </span>
      </div>
    </div>
  )
}
