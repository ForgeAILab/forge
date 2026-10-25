// CLI runtime availability controls.

import { useState } from 'react'
import { TerminalWindow } from '@phosphor-icons/react'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { useSetCliRuntimeAvailabilityMutation } from '@/features/federation/hooks'
import type { CliRuntimeEntryResponse } from '@/types/generated'
import { StateBadge } from '@/features/federation/components'
import { PricingSettingsEditor } from '../PricingSettingsEditor'
import { humanize, runtimeDisplayNames } from '../format'

export function CliRuntimeCard({ runtime }: { runtime: CliRuntimeEntryResponse }) {
  const setAvailability = useSetCliRuntimeAvailabilityMutation()
  const [confirmingDisable, setConfirmingDisable] = useState(false)
  const [error, setError] = useState<string>()
  const authenticated = runtime.availability === 'authenticated'
  const badgeLabel = !runtime.enabled
    ? 'Disabled'
    : authenticated
      ? 'Authenticated'
      : runtime.availability === 'unauthenticated'
        ? 'Not Logged In'
        : 'Unavailable'
  return (
    <Card className="flex flex-col p-4">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <h3 className="truncate text-sm font-semibold text-foreground">
              {runtimeDisplayNames[runtime.kind] ?? humanize(runtime.kind)}
            </h3>
            {runtime.version ? (
              <span className="font-mono text-xs text-muted-foreground">v{runtime.version}</span>
            ) : null}
            <StateBadge
              status={!runtime.enabled ? 'paused' : authenticated ? 'healthy' : 'unavailable'}
              label={badgeLabel}
            />
          </div>
          <p className="mt-1 truncate text-xs text-muted-foreground">
            {runtime.daemon_hostname ?? runtime.daemon_id}
          </p>
        </div>
        <TerminalWindow size={17} className="shrink-0 text-primary" aria-hidden />
      </div>
      <p className="mt-3 text-xs text-muted-foreground">
        Used by {runtime.used_by.length} agent{runtime.used_by.length === 1 ? '' : 's'}
        {runtime.used_by.length > 0
          ? `: ${runtime.used_by.map((agent) => agent.agent_name).join(', ')}`
          : ''}
      </p>
      {!authenticated && runtime.login_hint ? (
        <p className="mt-2 rounded-md border border-warning/30 bg-warning/10 px-3 py-2 text-xs text-warning">
          {runtime.login_hint}. Forge never reads the CLI&apos;s credential files.
        </p>
      ) : null}
      <PricingSettingsEditor
        subject={{ kind: 'cli_runtime', daemonId: runtime.daemon_id, executorType: runtime.kind }}
      />
      {confirmingDisable ? (
        <div className="mt-3 rounded-md border border-warning/30 bg-warning/10 px-3 py-2 text-xs text-warning">
          <p>
            Disable this runtime on this host? {runtime.used_by.length} referencing agent
            {runtime.used_by.length === 1 ? '' : 's'} will stop accepting new work, but all
            configuration stays in place.
          </p>
          <div className="mt-2 flex gap-2">
            <Button
              size="sm"
              variant="outline"
              disabled={setAvailability.isPending}
              onClick={() =>
                void setAvailability
                  .mutateAsync({
                    daemonId: runtime.daemon_id,
                    executorType: runtime.kind,
                    input: { enabled: false, version: runtime.policy_version },
                  })
                  .then(() => setConfirmingDisable(false))
                  .catch((cause: unknown) =>
                    setError(
                      cause instanceof Error ? cause.message : 'Runtime could not be disabled.',
                    ),
                  )
              }
            >
              Disable runtime
            </Button>
            <Button size="sm" variant="ghost" onClick={() => setConfirmingDisable(false)}>
              Keep enabled
            </Button>
          </div>
        </div>
      ) : null}
      {error ? (
        <p className="mt-2 text-xs text-destructive" role="alert">
          {error}
        </p>
      ) : null}
      {!confirmingDisable ? (
        <div className="mt-4 border-t border-border-subtle pt-3">
          <Button
            size="sm"
            variant="outline"
            disabled={setAvailability.isPending}
            onClick={() => {
              setError(undefined)
              if (runtime.enabled) {
                setConfirmingDisable(true)
                return
              }
              void setAvailability
                .mutateAsync({
                  daemonId: runtime.daemon_id,
                  executorType: runtime.kind,
                  input: { enabled: true, version: runtime.policy_version },
                })
                .catch((cause: unknown) =>
                  setError(
                    cause instanceof Error ? cause.message : 'Runtime could not be enabled.',
                  ),
                )
            }}
          >
            {runtime.enabled ? 'Disable' : 'Enable'}
          </Button>
        </div>
      ) : null}
    </Card>
  )
}
