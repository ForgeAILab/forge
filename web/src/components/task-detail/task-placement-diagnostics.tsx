import type { TaskPlacementDiagnostic } from '@/types/generated/bindings/TaskPlacementDiagnostic'

const labels: Record<string, string> = {
  environment_not_ready: 'Environment not ready',
  environment_probe_pending: 'Checking machine',
  environment_unverified: 'No machine checks to verify before cloning',
  provision_failed:
    'Repository provisioning failed; reconnect or update placement settings to retry',
  machine_capacity: 'Machine run capacity reached',
  disk_pressure: 'Workspace disk is under its free-space floor',
  executor_unavailable: 'Executor unavailable',
  native_backend_unsupported: 'Daemon workspaces run CLI Agents only',
}

/**
 * A coordination root on a daemon-owned placement accepts a lead, but the
 * lead is a native Agent and cannot run there: the root parks on this
 * refusal. The two ways out are the owner's.
 */
const NATIVE_LEAD_REFUSAL =
  'The lead is a native Agent and cannot plan on a daemon workspace, so this task is parked. Remove the lead (Assignees, Lead, Unassigned) to run without one, or change the Project placement so this task runs on the server.'

export function TaskPlacementDiagnostics({
  diagnostics,
  hasLead = false,
}: {
  diagnostics: TaskPlacementDiagnostic[]
  /** The Task is a coordination root with a lead (`effective_lead.source = own`). */
  hasLead?: boolean
}) {
  if (!diagnostics.length) return null
  const leadRefused =
    hasLead &&
    diagnostics.some((diagnostic) => diagnostic.filter_codes.includes('native_backend_unsupported'))
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
      {leadRefused ? (
        <p data-testid="lead-placement-refusal" className="break-words text-xs">
          {NATIVE_LEAD_REFUSAL}
        </p>
      ) : null}
    </section>
  )
}
