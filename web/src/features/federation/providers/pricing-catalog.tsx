// Pricing catalog status and refresh controls.

import { useState } from 'react'
import { ArrowClockwise } from '@phosphor-icons/react'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import {
  usePricingCatalogStatusQuery,
  useRefreshPricingCatalogMutation,
} from '@/features/federation/hooks'
import type { PricingCatalogStatus } from '@/types/generated'
import { LoadingPanel, SectionKicker, StateBadge } from '@/features/federation/components'
import { newPricingIdempotencyKey, boundedErrorMessage, timestampLabel } from './utils'

export function PricingCatalogStatusPanel() {
  const statusQuery = usePricingCatalogStatusQuery()
  const refresh = useRefreshPricingCatalogMutation()
  const [notice, setNotice] = useState<string>()
  const [error, setError] = useState<string>()

  async function refreshCatalog() {
    setNotice(undefined)
    setError(undefined)
    try {
      await refresh.mutateAsync({
        idempotency_key: newPricingIdempotencyKey('pricing-catalog-refresh'),
      })
      setNotice(
        'Pricing catalog refresh completed. Last-known-good rates remain available if the source was unchanged or failed validation.',
      )
    } catch (cause) {
      setError(boundedErrorMessage(cause, 'Pricing catalog refresh failed.'))
      void statusQuery.refetch()
    }
  }

  if (statusQuery.isLoading) return <LoadingPanel label="Loading pricing catalog status" />

  const status = statusQuery.data
  if (statusQuery.isError || !status) {
    return (
      <Card className="border-destructive/30 bg-destructive/5 p-4" role="alert">
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div>
            <SectionKicker>Pricing catalog</SectionKicker>
            <h2 className="mt-1 text-sm font-semibold text-foreground">
              Catalog status unavailable
            </h2>
            <p className="mt-1 text-xs leading-5 text-muted-foreground">
              Forge could not read the server-owned catalog status. Existing provider execution
              remains available.
            </p>
          </div>
          <Button size="sm" variant="outline" onClick={() => void statusQuery.refetch()}>
            <ArrowClockwise size={14} aria-hidden />
            Refresh
          </Button>
        </div>
      </Card>
    )
  }

  const stateCopy: Record<PricingCatalogStatus['state'], string> = {
    absent:
      'No pricing catalog has been loaded. Rates remain unknown until an explicit refresh succeeds.',
    fresh: 'Pricing catalog is fresh. Exact catalog rows are available for binding.',
    stale:
      'Pricing catalog is stale. The last-known-good rates remain active and eligible for estimates.',
    refresh_failed: 'Pricing catalog refresh failed. The last-known-good rates remain active.',
  }
  const stateLabel: Record<PricingCatalogStatus['state'], string> = {
    absent: 'Absent',
    fresh: 'Fresh',
    stale: 'Stale',
    refresh_failed: 'Refresh failed',
  }
  const stateRole = status.state === 'refresh_failed' ? 'alert' : 'status'

  return (
    <Card className="border-border-subtle bg-card/80 p-4">
      <div className="flex flex-wrap items-start justify-between gap-3">
        <div className="min-w-0">
          <SectionKicker>Pricing catalog</SectionKicker>
          <div className="mt-1 flex flex-wrap items-center gap-2">
            <h2 className="text-sm font-semibold text-foreground">models.dev estimate source</h2>
            <StateBadge status={status.state} label={stateLabel[status.state]} />
          </div>
          <p
            className="mt-1 max-w-3xl text-xs leading-5 text-muted-foreground"
            role={stateRole}
            aria-live="polite"
          >
            {stateCopy[status.state]}
          </p>
        </div>
        <Button
          size="sm"
          variant="outline"
          disabled={refresh.isPending}
          onClick={() => void refreshCatalog()}
        >
          <ArrowClockwise
            size={14}
            className={refresh.isPending ? 'animate-spin' : ''}
            aria-hidden
          />
          {refresh.isPending ? 'Refreshing…' : 'Refresh catalog'}
        </Button>
      </div>
      <div className="mt-3 grid min-w-0 gap-2 text-xs sm:grid-cols-2 lg:grid-cols-4">
        <div className="min-w-0">
          <p className="text-muted-foreground">Effective snapshot</p>
          <p className="mt-0.5 break-all font-mono text-foreground">
            {status.active_snapshot_id ?? 'None'}
          </p>
        </div>
        <div className="min-w-0">
          <p className="text-muted-foreground">Revision</p>
          <p className="mt-0.5 break-all font-mono text-foreground">{status.revision ?? 'None'}</p>
        </div>
        <div>
          <p className="text-muted-foreground">Fetched at</p>
          <time className="mt-0.5 block text-foreground" dateTime={status.fetched_at ?? undefined}>
            {timestampLabel(status.fetched_at)}
          </time>
        </div>
        <div>
          <p className="text-muted-foreground">Last checked</p>
          <time
            className="mt-0.5 block text-foreground"
            dateTime={status.last_checked_at ?? undefined}
          >
            {timestampLabel(status.last_checked_at)}
          </time>
        </div>
      </div>
      {status.stale_after ? (
        <p className="mt-2 text-micro text-muted-foreground">Fresh through {status.stale_after}.</p>
      ) : null}
      {status.last_error_code ? (
        <p className="mt-2 break-words text-xs text-warning">
          Last refresh issue: {status.last_error_code.slice(0, 120)}
        </p>
      ) : null}
      {notice ? (
        <p className="mt-2 text-xs text-success" role="status">
          {notice}
        </p>
      ) : null}
      {error ? (
        <p className="mt-2 break-words text-xs text-destructive" role="alert">
          {error}
        </p>
      ) : null}
    </Card>
  )
}
