// Stored provider entry controls.

import { useEffect, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { ArrowClockwise, ArrowUpRight, Key } from '@phosphor-icons/react'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import {
  federationQueryKeys,
  useProviderUsageQuery,
  useRemoveProviderEntryMutation,
  useRenameProviderEntryMutation,
  useSetProviderEntryAvailabilityMutation,
  isVersionConflict,
} from '@/features/federation/hooks'
import { testProviderEntry } from '@/features/federation/api'
import type { ProviderEntryResponse, ProviderEntryTestResponse } from '@/types/generated'
import { SectionKicker, StateBadge } from '@/features/federation/components'
import { PricingSettingsEditor } from '../PricingSettingsEditor'
import { humanize, shortId } from '../format'
import { UsageSummary } from './connection'

export function ProviderEntryCard({
  entry,
  onShowAgents,
}: {
  entry: ProviderEntryResponse
  onShowAgents: () => void
}) {
  const rename = useRenameProviderEntryMutation()
  const remove = useRemoveProviderEntryMutation()
  const setAvailability = useSetProviderEntryAvailabilityMutation()
  const queryClient = useQueryClient()
  const usageQuery = useProviderUsageQuery(
    entry.status === 'configured' && entry.enabled ? entry.id : undefined,
  )
  const [renaming, setRenaming] = useState(false)
  const [confirmingRemoval, setConfirmingRemoval] = useState(false)
  const [confirmingDisable, setConfirmingDisable] = useState(false)
  const [label, setLabel] = useState(entry.label)
  const [error, setError] = useState<string>()
  const [notice, setNotice] = useState<string>()
  const [testingConnection, setTestingConnection] = useState(false)
  const [connectionTest, setConnectionTest] = useState<ProviderEntryTestResponse | null>(null)
  const [statusClock, setStatusClock] = useState(Date.now)

  const retryAt = entry.health?.backoff_until
    ? Date.parse(entry.health.backoff_until)
    : Number.NaN
  useEffect(() => {
    const delay = retryAt - Date.now()
    if (!Number.isFinite(delay) || delay <= 0 || delay > 2_147_483_647) return
    const timer = window.setTimeout(() => setStatusClock(Date.now()), delay + 10)
    return () => window.clearTimeout(timer)
  }, [retryAt])
  const isWaiting = Number.isFinite(retryAt) && retryAt > statusClock
  const authError = entry.health?.last_error_kind === 'auth' && entry.health.status === 'error'

  const statusBadge = !entry.enabled
    ? { status: 'paused', label: 'Disabled' }
    : entry.status === 'configured'
      ? entry.health?.status === 'error'
        ? { status: 'error', label: 'Error' }
        : isWaiting
          ? { status: 'degraded', label: 'Backing off' }
          : entry.health?.status !== 'healthy' && entry.health != null
            ? { status: 'degraded', label: 'Retry available' }
            : { status: 'active', label: 'Connected' }
      : entry.status === 'revoked'
        ? { status: 'error', label: 'Revoked' }
        : entry.status === 'invalid'
          ? { status: 'error', label: 'Error' }
          : { status: entry.status, label: humanize(entry.status) }

  async function refreshStatus() {
    setTestingConnection(true)
    setError(undefined)
    try {
      if (entry.status === 'configured' && entry.enabled) {
        setConnectionTest(await testProviderEntry(entry.id))
        await usageQuery.refetch()
      }
      await queryClient.invalidateQueries({ queryKey: federationQueryKeys.credentials })
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'The connection test could not run.')
    } finally {
      setTestingConnection(false)
    }
  }

  async function submitRename(event: React.FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setError(undefined)
    try {
      await rename.mutateAsync({
        id: entry.id,
        input: { label: label.trim(), version: entry.version },
      })
      setRenaming(false)
    } catch (cause) {
      setError(
        isVersionConflict(cause)
          ? 'This entry changed in another session. Refresh before renaming.'
          : cause instanceof Error
            ? cause.message
            : 'Rename failed.',
      )
    }
  }

  async function disconnect() {
    setError(undefined)
    setNotice(undefined)
    try {
      const result = await remove.mutateAsync({ handleId: entry.id, version: entry.version })
      setConfirmingRemoval(false)
      setNotice(
        result.provider_revocation === 'failed'
          ? 'Disconnected locally. Provider-side revocation could not be confirmed; revoke Forge in the provider account as a follow-up.'
          : 'Provider entry disconnected. Referencing agents are now marked unhealthy.',
      )
    } catch (cause) {
      setError(cause instanceof Error ? cause.message : 'The entry could not be disconnected.')
    }
  }

  async function updateAvailability(enabled: boolean) {
    setError(undefined)
    setNotice(undefined)
    try {
      await setAvailability.mutateAsync({
        id: entry.id,
        input: { enabled, version: entry.version },
      })
      setConfirmingDisable(false)
      setNotice(
        enabled
          ? 'Provider enabled. Referencing agents can accept new work again.'
          : 'Provider disabled. Credentials, agents, and bindings were preserved.',
      )
    } catch (cause) {
      setError(
        isVersionConflict(cause)
          ? 'This provider changed in another session. Refresh and try again.'
          : cause instanceof Error
            ? cause.message
            : 'Provider availability could not be changed.',
      )
    }
  }

  return (
    <Card className="flex flex-col p-4">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-2">
            <h3 className="truncate text-sm font-semibold text-foreground">{entry.label}</h3>
            <StateBadge status={statusBadge.status} label={statusBadge.label} />
          </div>
          <p className="mt-1 truncate text-xs text-muted-foreground">{humanize(entry.provider)}</p>
        </div>
        <div className="flex shrink-0 items-center gap-2">
          <Button
            variant="ghost"
            size="icon-sm"
            aria-label="Test provider connection"
            title="Test provider connection"
            disabled={testingConnection || entry.status !== 'configured' || !entry.enabled}
            onClick={() => void refreshStatus()}
          >
            <ArrowClockwise
              size={14}
              className={testingConnection || usageQuery.isFetching ? 'animate-spin' : ''}
              aria-hidden
            />
          </Button>
          <Key size={17} className="text-primary" aria-hidden />
        </div>
      </div>
      <dl className="mt-3 space-y-1.5 text-xs">
        <div className="flex justify-between gap-3">
          <dt className="text-muted-foreground">Method</dt>
          <dd className="text-foreground">
            {entry.credential_method === 'oauth_bundle' ? 'OAuth login' : 'API key'}
          </dd>
        </div>
        {entry.provider_account_id ? (
          <div className="flex justify-between gap-3">
            <dt className="text-muted-foreground">Account</dt>
            <dd className="truncate font-mono text-foreground">
              {shortId(entry.provider_account_id)}
            </dd>
          </div>
        ) : null}
        <div className="flex justify-between gap-3">
          <dt className="text-muted-foreground">Last used</dt>
          <dd className="text-foreground">
            {entry.last_used_at ? new Date(entry.last_used_at).toLocaleString() : 'Never'}
          </dd>
        </div>
      </dl>
      {entry.health && entry.health.status !== 'healthy' ? (
        <p className="mt-3 text-xs text-muted-foreground" role="status">
          {entry.health.last_error_message ?? 'Provider connection failed.'}
          {authError
            ? ' Check the credential, then test the connection to resume work.'
            : isWaiting
              ? ` New work waits until ${new Date(retryAt).toLocaleString()}.`
              : ' A new call or connection test can retry now.'}
        </p>
      ) : null}
      {connectionTest ? (
        <p className="mt-2 text-xs text-muted-foreground" role="status">
          {connectionTest.status === 'ok'
            ? `Connection test passed · ${connectionTest.latency_ms} ms`
            : connectionTest.message ?? 'Connection test failed.'}
        </p>
      ) : null}
      {entry.status === 'configured' && entry.enabled ? (
        <section
          className="mt-3 rounded-md border border-border-subtle bg-muted/20 px-3 py-2 text-xs"
          aria-labelledby={`quota-heading-${entry.id}`}
        >
          <SectionKicker>Quota usage</SectionKicker>
          <h4 id={`quota-heading-${entry.id}`} className="sr-only">
            Quota Usage and Rate limits
          </h4>
          {usageQuery.isLoading ? (
            <span className="text-muted-foreground">Checking usage…</span>
          ) : usageQuery.isError ? (
            <span className="text-muted-foreground">Usage unavailable</span>
          ) : usageQuery.data ? (
            <UsageSummary usage={usageQuery.data} />
          ) : null}
        </section>
      ) : null}
      <PricingSettingsEditor subject={{ kind: 'provider', id: entry.id }} />
      <button
        type="button"
        className="mt-3 inline-flex items-center gap-1.5 text-left text-xs font-medium text-primary hover:underline"
        onClick={onShowAgents}
      >
        Used by {entry.used_by.length} agent{entry.used_by.length === 1 ? '' : 's'}
        <ArrowUpRight size={13} aria-hidden />
      </button>
      {renaming ? (
        <form onSubmit={submitRename} className="mt-3 flex items-center gap-2">
          <Input
            aria-label="New entry name"
            value={label}
            onChange={(event) => setLabel(event.target.value)}
          />
          <Button type="submit" size="sm" disabled={rename.isPending}>
            Save
          </Button>
          <Button type="button" size="sm" variant="ghost" onClick={() => setRenaming(false)}>
            Cancel
          </Button>
        </form>
      ) : null}
      {confirmingRemoval ? (
        <div
          className="mt-3 rounded-md border border-warning/30 bg-warning/10 px-3 py-2 text-xs text-warning"
          role="alertdialog"
          aria-label={`Confirm disconnecting ${entry.label}`}
        >
          {entry.used_by.length > 0 ? (
            <p>
              {entry.used_by.length} agent{entry.used_by.length === 1 ? '' : 's'} reference this
              entry ({entry.used_by.map((agent) => agent.agent_name).join(', ')}). They will become
              unhealthy and are never silently rebound.
            </p>
          ) : (
            <p>No agents reference this entry.</p>
          )}
          <div className="mt-2 flex gap-2">
            <Button
              size="sm"
              variant="destructive"
              disabled={remove.isPending}
              onClick={() => void disconnect()}
            >
              Disconnect
            </Button>
            <Button size="sm" variant="ghost" onClick={() => setConfirmingRemoval(false)}>
              Keep
            </Button>
          </div>
        </div>
      ) : null}
      {confirmingDisable ? (
        <div
          className="mt-3 rounded-md border border-warning/30 bg-warning/10 px-3 py-2 text-xs text-warning"
          role="alertdialog"
          aria-label={`Confirm disabling ${entry.label}`}
        >
          <p>
            Disable this provider for {entry.used_by.length} referencing agent
            {entry.used_by.length === 1 ? '' : 's'}
            {entry.used_by.length > 0
              ? ` (${entry.used_by.map((agent) => agent.agent_name).join(', ')})`
              : ''}
            ? Their configuration and bindings stay in place, but new work stops until you enable it
            again.
          </p>
          <div className="mt-2 flex gap-2">
            <Button
              size="sm"
              variant="outline"
              disabled={setAvailability.isPending}
              onClick={() => void updateAvailability(false)}
            >
              Disable provider
            </Button>
            <Button size="sm" variant="ghost" onClick={() => setConfirmingDisable(false)}>
              Keep enabled
            </Button>
          </div>
        </div>
      ) : null}
      {error ? (
        <p role="alert" className="mt-2 text-xs text-destructive">
          {error}
        </p>
      ) : null}
      {notice ? (
        <p role="status" className="mt-2 text-xs text-muted-foreground">
          {notice}
        </p>
      ) : null}
      {entry.status !== 'revoked' && !confirmingRemoval && !confirmingDisable ? (
        <div className="mt-4 flex flex-wrap gap-2 border-t border-border-subtle pt-3">
          <Button
            size="sm"
            variant="outline"
            disabled={setAvailability.isPending}
            onClick={() =>
              entry.enabled ? setConfirmingDisable(true) : void updateAvailability(true)
            }
          >
            {entry.enabled ? 'Disable' : 'Enable'}
          </Button>
          {!renaming ? (
            <Button size="sm" variant="outline" onClick={() => setRenaming(true)}>
              Rename
            </Button>
          ) : null}
          <Button
            size="sm"
            variant="outline"
            className="text-destructive hover:bg-destructive/10 hover:border-destructive/30"
            onClick={() => setConfirmingRemoval(true)}
          >
            Disconnect
          </Button>
        </div>
      ) : null}
    </Card>
  )
}
