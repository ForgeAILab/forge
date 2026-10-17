import { Robot, WarningCircle } from '@phosphor-icons/react'
import { Badge } from '@/components/ui/badge'
import { Label } from '@/components/ui/label'
import { Select, type SelectOption } from '@/components/ui/select'
import { cn } from '@/lib/cn'
import type { Agent } from '@/types/generated'

const EXECUTOR_LABELS: Record<string, string> = {
  claude_code: 'Claude Code',
  codex: 'Codex',
  cursor: 'Cursor',
  gemini: 'Gemini',
  opencode: 'OpenCode',
  shell: 'Shell',
  smith: 'Smith',
}

const EFFECTIVE_STATUS_DOT: Record<string, string> = {
  active: 'bg-emerald-500',
  busy: 'bg-amber-500',
  daemon_unavailable: 'bg-zinc-400',
  daemon_upgrade_required: 'bg-amber-500',
  offline: 'bg-red-500',
  error: 'bg-red-500',
}

export function AgentSelector({
  id,
  agents,
  value,
  disabled,
  isLoading,
  hasWarning,
  className,
  onChange,
}: {
  id: string
  agents: Agent[]
  value: string | null
  disabled?: boolean
  isLoading?: boolean
  hasWarning?: boolean
  className?: string
  onChange: (agentId: string | null) => void
}) {
  const selectedAgent = agents.find((a) => a.id === value)

  return (
    <div className={cn('min-w-0 space-y-1', className)}>
      <div className="flex items-center justify-between gap-2">
        <Label htmlFor={id} className="flex items-center gap-1.5">
          <Robot size={12} />
          Agent
        </Label>
        {selectedAgent ? (
          <span className="flex items-center gap-1 text-[11px] text-muted-foreground">
            <span
              className={cn(
                'inline-block h-1.5 w-1.5 rounded-full',
                EFFECTIVE_STATUS_DOT[selectedAgent.effective_status ?? 'offline'] ?? 'bg-zinc-400',
              )}
            />
            {EXECUTOR_LABELS[selectedAgent.executor_type] ?? selectedAgent.executor_type}
          </span>
        ) : null}
        {hasWarning ? (
          <Badge
            variant="outline"
            className="gap-1 rounded-md border-amber-300 px-1.5 text-[11px] text-amber-700"
            title="Could not load available options"
          >
            <WarningCircle size={12} />
            Options
          </Badge>
        ) : null}
      </div>
      <Select
        id={id}
        value={value ?? ''}
        disabled={disabled || isLoading}
        className="h-9 text-xs"
        placeholder={isLoading ? 'Loading agents...' : 'Select agent'}
        options={agents.map<SelectOption>((agent) => ({
          value: agent.id,
          label: agent.name + (agent.model ? ` · ${agent.model}` : ''),
        }))}
        onChange={(v) => onChange(v || null)}
      />
    </div>
  )
}
