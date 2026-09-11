import { FastForward, HandPalm, ListChecks, Warning } from '@phosphor-icons/react'
import { Label } from '@/components/ui/label'
import { Select } from '@/components/ui/select'
import { cn } from '@/lib/cn'

const policyOptions = [
  {
    id: 'auto',
    label: 'Auto',
    description: 'Run without pausing for routine decisions.',
    Icon: FastForward,
  },
  {
    id: 'supervised',
    label: 'Supervised',
    description: 'Ask before risky operations.',
    Icon: HandPalm,
  },
  {
    id: 'plan',
    label: 'Plan',
    description: 'Plan first, then wait for approval.',
    Icon: ListChecks,
  },
  {
    id: 'yolo',
    label: 'YOLO',
    description: 'Full host access with no approval prompts.',
    Icon: Warning,
  },
]

export function PolicySelector({
  id,
  value,
  disabled,
  className,
  onChange,
}: {
  id: string
  value: string | null
  disabled?: boolean
  className?: string
  onChange: (policy: string | null) => void
}) {
  const selectedPolicy = policyOptions.find((policy) => policy.id === value)
  const yoloSelected = value === 'yolo'

  return (
    <div className={cn('min-w-0 space-y-1', className)}>
      <Label
        htmlFor={id}
        className={cn('flex items-center gap-1.5', yoloSelected && 'text-warning')}
      >
        {selectedPolicy ? <selectedPolicy.Icon size={12} /> : <FastForward size={12} />}
        Policy
      </Label>
      <Select
        id={id}
        value={value ?? ''}
        disabled={disabled}
        className={cn('h-9 text-xs', yoloSelected && 'border-warning/70')}
        title={selectedPolicy?.description ?? 'Use profile default'}
        placeholder="Default"
        options={policyOptions.map((policy) => ({
          value: policy.id,
          label: `${policy.label} — ${policy.description}`,
        }))}
        onChange={(v) => onChange(v || null)}
      />
      {yoloSelected ? (
        <p className="text-micro leading-4 text-warning" role="status">
          Full host access. Forge scope and user-only approval boundaries still apply.
        </p>
      ) : null}
    </div>
  )
}
