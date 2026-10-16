import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Switch } from '@/components/ui/switch'
import type { ReviewVerifyConfig } from '@/types/generated/bindings/ReviewVerifyConfig'

export function ReviewVerifyEditor({
  value,
  onChange,
  disabled,
}: {
  value: ReviewVerifyConfig | null
  onChange: (value: ReviewVerifyConfig | null) => void
  disabled: boolean
}) {
  const update = (patch: Partial<ReviewVerifyConfig>) => value && onChange({ ...value, ...patch })
  return (
    <div className="space-y-4">
      <div className="flex items-center gap-2">
        <Switch
          id="verify-enabled"
          checked={value !== null}
          disabled={disabled}
          onChange={(event) =>
            onChange(event.target.checked ? { checks: [{ name: 'smoke', command: '' }] } : null)
          }
        />
        <Label htmlFor="verify-enabled">Verify the running app before review</Label>
      </div>
      <p className="text-xs text-muted-foreground">
        Runs after CI passes. Daemon workspaces record verify as not run.
      </p>
      {value && (
        <div className="space-y-4 rounded-md border p-3">
          <div className="space-y-2">
            <Label htmlFor="verify-start">Start command (optional)</Label>
            <Input
              id="verify-start"
              value={value.start ?? ''}
              disabled={disabled}
              placeholder={'pnpm dev --port "$FORGE_VERIFY_PORT"'}
              onChange={(e) => update({ start: e.target.value || undefined })}
            />
          </div>
          <div className="grid gap-4 sm:grid-cols-2">
            <div className="space-y-2">
              <Label htmlFor="verify-ready-path">Readiness URL path (optional)</Label>
              <Input
                id="verify-ready-path"
                value={value.ready_path ?? ''}
                disabled={disabled}
                placeholder="/"
                onChange={(e) =>
                  update({ ready_path: e.target.value || undefined, ready_command: undefined })
                }
              />
            </div>
            <div className="space-y-2">
              <Label htmlFor="verify-ready-command">Readiness command (alternative)</Label>
              <Input
                id="verify-ready-command"
                value={value.ready_command ?? ''}
                disabled={disabled}
                onChange={(e) =>
                  update({ ready_command: e.target.value || undefined, ready_path: undefined })
                }
              />
            </div>
            {(['ready_timeout_seconds', 'timeout_seconds'] as const).map((field) => (
              <div key={field} className="space-y-2">
                <Label htmlFor={`verify-${field}`}>
                  {field === 'ready_timeout_seconds'
                    ? 'Readiness timeout (seconds)'
                    : 'Verify run timeout (seconds)'}
                </Label>
                <Input
                  id={`verify-${field}`}
                  type="number"
                  min={1}
                  max={field === 'ready_timeout_seconds' ? 900 : 3600}
                  placeholder={field === 'ready_timeout_seconds' ? '120' : '1200'}
                  disabled={disabled}
                  value={value[field] ?? ''}
                  onChange={(e) =>
                    update({ [field]: e.target.value === '' ? undefined : Number(e.target.value) })
                  }
                />
              </div>
            ))}
          </div>
          {value.checks.map((check, index) => (
            <fieldset key={index} className="space-y-2 rounded-md border p-3" disabled={disabled}>
              <legend className="px-1 text-sm">Check {index + 1}</legend>
              <Label htmlFor={`verify-name-${index}`}>Check {index + 1} name</Label>
              <Input
                id={`verify-name-${index}`}
                value={check.name}
                maxLength={32}
                pattern="[a-z0-9-]{1,32}"
                onChange={(e) =>
                  update({
                    checks: value.checks.map((c, i) =>
                      i === index ? { ...c, name: e.target.value } : c,
                    ),
                  })
                }
              />
              <Label htmlFor={`verify-command-${index}`}>Check {index + 1} command</Label>
              <Input
                id={`verify-command-${index}`}
                value={check.command}
                onChange={(e) =>
                  update({
                    checks: value.checks.map((c, i) =>
                      i === index ? { ...c, command: e.target.value } : c,
                    ),
                  })
                }
              />
              <Label htmlFor={`verify-timeout-${index}`}>Check {index + 1} timeout (seconds)</Label>
              <Input
                id={`verify-timeout-${index}`}
                type="number"
                min={1}
                max={3600}
                placeholder="600"
                value={check.timeout_seconds ?? ''}
                onChange={(e) =>
                  update({
                    checks: value.checks.map((c, i) =>
                      i === index
                        ? {
                            ...c,
                            timeout_seconds:
                              e.target.value === '' ? undefined : Number(e.target.value),
                          }
                        : c,
                    ),
                  })
                }
              />
              <Button
                type="button"
                variant="ghost"
                size="sm"
                disabled={disabled || value.checks.length <= 1}
                onClick={() => update({ checks: value.checks.filter((_, i) => i !== index) })}
              >
                Remove check {index + 1}
              </Button>
            </fieldset>
          ))}
          <Button
            type="button"
            variant="outline"
            size="sm"
            disabled={disabled || value.checks.length >= 16}
            onClick={() => update({ checks: [...value.checks, { name: '', command: '' }] })}
          >
            Add verify check
          </Button>
        </div>
      )}
    </div>
  )
}
