// Mission Control projection section wrapper.

import type { ReactNode } from 'react'

export function ProjectionSection({
  title,
  count,
  children,
  icon,
}: {
  title: string
  count: number
  children: ReactNode
  icon: ReactNode
}) {
  return (
    <section className="overflow-hidden rounded-xl border border-border-subtle bg-card shadow-card">
      <header className="flex items-center justify-between gap-3 border-b border-border-subtle px-4 py-3">
        <div className="flex items-center gap-2">
          <span className="text-primary">{icon}</span>
          <h2 className="font-mono text-micro font-semibold uppercase tracking-[0.9px] text-muted-foreground">
            {title}
          </h2>
        </div>
        <span className="rounded-full bg-muted px-2 py-0.5 font-mono text-micro text-muted-foreground">
          {count}
        </span>
      </header>
      {children}
    </section>
  )
}
