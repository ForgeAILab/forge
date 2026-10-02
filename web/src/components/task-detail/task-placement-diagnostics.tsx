import type { TaskPlacementDiagnostic } from '@/types/generated/bindings/TaskPlacementDiagnostic'

const labels: Record<string, string> = {
  environment_not_ready: 'Environment not ready',
  environment_probe_pending: 'Checking machine',
  machine_capacity: 'Machine run capacity reached',
  executor_unavailable: 'Executor unavailable',
}

export function TaskPlacementDiagnostics({
  diagnostics,
}: {
  diagnostics: TaskPlacementDiagnostic[]
}) {
  if (!diagnostics.length) return null
  return (
    <section aria-label="Placement diagnostics" role="status" className="min-w-0 space-y-2">
      <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
        Placement diagnostics
      </p>
      {diagnostics.map((diagnostic, index) => (
        <div
          key={`${diagnostic.machine?.id ?? 'capacity'}-${index}`}
          className="rounded-md border border-warning/40 bg-warning/10 p-3 text-xs"
        >
          {diagnostic.filter_codes.map((code) => (
            <p key={code} className="break-words">
              {code === 'environment_probe_pending'
                ? `Checking machine ${diagnostic.machine?.name ?? 'unknown'}…`
                : `${diagnostic.machine ? `${diagnostic.machine.name}: ` : ''}${labels[code] ?? code.replaceAll('_', ' ')}`}
              {code === 'environment_not_ready' && diagnostic.failing_checks.length
                ? ` (${diagnostic.failing_checks.join(', ')})`
                : ''}
            </p>
          ))}
        </div>
      ))}
    </section>
  )
}
