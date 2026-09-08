import type { AnalyticsWindow } from '@/types/generated'
import { cn } from '@/lib/cn'
import { formatAnalyticsTimestamp } from './analytics-format'

export type AnalyticsRange = '7d' | '30d' | 'all'

export function analyticsRangeWindow(
  range: AnalyticsRange,
  now = new Date(),
): { from?: string; to?: string } {
  if (range === 'all') return {}
  const to = now.toISOString()
  const days = range === '7d' ? 7 : 30
  return {
    from: new Date(now.getTime() - days * 86400000).toISOString(),
    to,
  }
}

export function formatAnalyticsWindow(window: AnalyticsWindow): string {
  if (!window.from && !window.to) return 'All available activity'
  return `${window.from ? formatAnalyticsTimestamp(window.from) : 'Beginning'} to ${window.to ? formatAnalyticsTimestamp(window.to) : 'now'}`
}

export function AnalyticsRangeControls({
  value,
  onChange,
}: {
  value: AnalyticsRange
  onChange: (range: AnalyticsRange) => void
}) {
  const options: Array<{ value: AnalyticsRange; label: string }> = [
    { value: '7d', label: 'Last 7 days' },
    { value: '30d', label: 'Last 30 days' },
    { value: 'all', label: 'All time' },
  ]

  return (
    <div className="flex flex-wrap gap-2" role="group" aria-label="Analytics time range">
      {options.map((option) => (
        <button
          key={option.value}
          type="button"
          aria-pressed={value === option.value}
          onClick={() => onChange(option.value)}
          className={cn(
            'rounded-md border px-3 py-1.5 text-sm font-medium transition-colors focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 active:translate-y-px',
            value === option.value
              ? 'border-ember-border bg-ember-surface text-foreground shadow-ember'
              : 'border-border bg-secondary text-muted-foreground hover:bg-accent hover:text-foreground',
          )}
        >
          {option.label}
        </button>
      ))}
    </div>
  )
}
