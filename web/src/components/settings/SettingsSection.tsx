import type { ReactNode } from 'react'
import { cn } from '@/lib/cn'

export function SettingsSection({
  title,
  description,
  children,
  danger,
  layout = 'columns',
}: {
  title: ReactNode
  description?: string
  children: ReactNode
  danger?: boolean
  layout?: 'columns' | 'stacked'
}) {
  return (
    <section className="border-b py-6 last:border-b-0">
      <div
        className={cn(
          'grid min-w-0 grid-cols-1 items-start gap-4',
          layout === 'columns' && 'sm:grid-cols-[220px_minmax(0,1fr)] sm:gap-8',
        )}
      >
        <div>
          <h3
            className={cn(
              'text-[13px] font-semibold leading-snug',
              danger ? 'text-red-300' : 'text-foreground',
            )}
          >
            {title}
          </h3>
          {description && (
            <p className="mt-1 text-xs leading-relaxed text-muted-foreground">{description}</p>
          )}
        </div>
        <div className="min-w-0">{children}</div>
      </div>
    </section>
  )
}
