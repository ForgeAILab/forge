import { useEffect, useRef, useState } from 'react'
import { CaretDown, Check, Funnel } from '@phosphor-icons/react'

import { Button } from '@/components/ui/button'
import { cn } from '@/lib/cn'
import type { LogFilterKind } from '@/lib/log-filter'

export const visibleLogKinds: LogFilterKind[] = [
  'assistant',
  'thinking',
  'user',
  'tool_call',
  'tool_result',
  'stdout',
  'stderr',
  'system',
  'file_change',
  'shell_command',
  'approval_question',
  'session_info',
  'unknown',
]

export const logKindLabels: Record<LogFilterKind, string> = {
  assistant: 'Assistant',
  thinking: 'Thinking',
  user: 'User',
  tool_call: 'Tool Calls',
  tool_result: 'Tool Results',
  stdout: 'Stdout',
  stderr: 'Stderr',
  system: 'System',
  file_change: 'File Changes',
  shell_command: 'Shell',
  approval_question: 'Approvals',
  session_info: 'Session',
  unknown: 'Other',
}

export const defaultLogKinds = new Set<LogFilterKind>(visibleLogKinds)

export function ExecutionLogFilterDropdown({
  enabledKinds,
  onToggle,
}: {
  enabledKinds: Set<LogFilterKind>
  onToggle: (kind: LogFilterKind) => void
}) {
  const [open, setOpen] = useState(false)
  const ref = useRef<HTMLDivElement>(null)
  const activeCount = enabledKinds.size
  const allActive = activeCount === visibleLogKinds.length

  useEffect(() => {
    if (!open) return
    const handler = (e: MouseEvent) => {
      if (ref.current && !ref.current.contains(e.target as Node)) setOpen(false)
    }
    document.addEventListener('mousedown', handler)
    return () => document.removeEventListener('mousedown', handler)
  }, [open])

  return (
    <div className="relative" ref={ref}>
      <Button
        size="sm"
        variant="ghost"
        className={cn(
          'h-7 gap-1.5 text-xs font-medium',
          !allActive && 'text-primary',
        )}
        onClick={() => setOpen(!open)}
      >
        <Funnel className="h-3.5 w-3.5" />
        <span>Filters</span>
        {!allActive && (
          <span className="rounded-full bg-primary/10 px-1.5 text-micro font-semibold text-primary">
            {activeCount}
          </span>
        )}
        <CaretDown className={cn('h-3 w-3 transition-transform', open && 'rotate-180')} />
      </Button>
      {open && (
        <div className="absolute left-0 top-full z-50 mt-1 w-48 rounded-lg border bg-popover p-1 shadow-float animate-slide-in">
          {visibleLogKinds.map((kind) => {
            const active = enabledKinds.has(kind)
            return (
              <button
                key={kind}
                type="button"
                className={cn(
                  'flex w-full items-center gap-2 rounded-md px-2.5 py-1.5 text-xs transition-colors cursor-pointer',
                  active
                    ? 'text-foreground'
                    : 'text-muted-foreground',
                  'hover:bg-accent',
                )}
                onClick={() => onToggle(kind)}
              >
                <span className={cn(
                  'flex h-4 w-4 items-center justify-center rounded border text-micro transition-colors',
                  active
                    ? 'border-primary bg-primary text-primary-foreground'
                    : 'border-muted-foreground/30',
                )}>
                  {active && <Check weight="bold" className="h-2.5 w-2.5" />}
                </span>
                <span>{logKindLabels[kind] ?? kind}</span>
              </button>
            )
          })}
        </div>
      )}
    </div>
  )
}
