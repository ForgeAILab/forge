import { Brain } from '@phosphor-icons/react'
import { Select } from '@/components/ui/select'
import { Label } from '@/components/ui/label'
import { Skeleton } from '@/components/ui/skeleton'
import { cn } from '@/lib/cn'
import type { ReasoningOption } from '@/hooks/useDiscoveredOptions'

export function ReasoningSelector({
  id,
  options,
  value,
  disabled,
  isLoading,
  hasError,
  placeholder,
  className,
  onChange,
}: {
  id: string
  options: ReasoningOption[]
  value: string | null
  disabled?: boolean
  isLoading?: boolean
  hasError?: boolean
  placeholder?: string
  className?: string
  onChange: (reasoningEffort: string | null) => void
}) {
  return (
    <div className={cn('min-w-0 space-y-1', className)}>
      <Label htmlFor={id} className="flex items-center gap-1.5">
        <Brain size={12} />
        Reasoning
      </Label>
      {isLoading ? (
        <Skeleton className="h-9 w-full" />
      ) : (
        <Select
          id={id}
          value={value ?? ''}
          placeholder={
            hasError ? 'Could not load options' : (placeholder ?? 'Default (agent setting)')
          }
          disabled={disabled}
          options={options.map((o) => ({ value: o.id, label: o.label }))}
          onChange={(v) => onChange(v || null)}
        />
      )}
    </div>
  )
}
