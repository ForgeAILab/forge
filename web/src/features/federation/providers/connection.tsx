// Provider connectivity checks and usage summaries.

import { useCallback, useEffect, useRef, useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { CheckCircle, CircleNotch, WarningCircle } from '@phosphor-icons/react'
import { federationQueryKeys } from '@/features/federation/hooks'
import { testProviderEntry } from '@/features/federation/api'
import type { ProviderUsage } from '@/features/federation/types'
import type { ProviderEntryTestResponse } from '@/types/generated'
import { formatResetRelative, windowLabel } from '../format'

/**
 * One-shot live connectivity check for a stored provider entry, rendered as a
 * single quiet status line. The check runs itself when `autoRun` is set; there
 * is deliberately no manual "test again" clutter here — the provider card's
 * refresh button covers manual re-checks.
 */
export function ProviderConnectionTest({
  entryId,
  autoRun = false,
}: {
  entryId: string
  autoRun?: boolean
}) {
  const [pending, setPending] = useState(false)
  const [result, setResult] = useState<ProviderEntryTestResponse | null>(null)
  const [failure, setFailure] = useState<string | null>(null)
  const runSeq = useRef(0)
  const autoRanFor = useRef<string | null>(null)
  const queryClient = useQueryClient()

  const runTest = useCallback((id: string) => {
    const seq = (runSeq.current += 1)
    setPending(true)
    setFailure(null)
    testProviderEntry(id)
      .then((response) => {
        if (runSeq.current !== seq) return
        setResult(response)
        void queryClient.invalidateQueries({ queryKey: federationQueryKeys.credentials })
      })
      .catch((cause: unknown) => {
        if (runSeq.current !== seq) return
        setResult(null)
        setFailure(cause instanceof Error ? cause.message : 'The connection test could not run.')
      })
      .finally(() => {
        if (runSeq.current === seq) setPending(false)
      })
  }, [queryClient])

  useEffect(() => {
    if (!autoRun || autoRanFor.current === entryId) return
    autoRanFor.current = entryId
    runTest(entryId)
  }, [autoRun, entryId, runTest])

  return (
    <p className="flex flex-wrap items-center gap-1.5 text-xs" role="status" aria-live="polite">
      {pending ? (
        <>
          <CircleNotch size={14} className="animate-spin text-primary" aria-hidden />
          <span className="text-muted-foreground">Checking the provider connection…</span>
        </>
      ) : result?.status === 'ok' ? (
        <>
          <CheckCircle size={14} className="text-success" aria-hidden />
          <span className="font-medium text-success">
            Provider responding · {result.latency_ms} ms
          </span>
          {result.message ? (
            <span className="text-muted-foreground">· {result.message}</span>
          ) : null}
        </>
      ) : result != null || failure != null ? (
        <>
          <WarningCircle size={14} className="text-destructive" aria-hidden />
          <span className="font-medium text-destructive">
            {result?.message ?? failure ?? 'The connection test failed.'}
          </span>
        </>
      ) : (
        <span className="text-muted-foreground">Forge verifies the stored credential once.</span>
      )}
    </p>
  )
}

export function UsageSummary({ usage }: { usage: ProviderUsage }) {
  if (usage.source === 'unknown' || usage.windows.length === 0) {
    return <span className="text-muted-foreground">Usage unknown</span>
  }
  const mostConsumed = usage.windows.reduce(
    (max, window) => (window.used_percent > max.used_percent ? window : max),
    usage.windows[0],
  )
  return (
    <div>
      <p className="font-medium text-foreground">
        {Math.round(mostConsumed.used_percent)}% used · resets{' '}
        {formatResetRelative(mostConsumed.resets_at)}
      </p>
      {usage.windows.length > 1 ? (
        <p className="mt-0.5 text-micro text-muted-foreground">
          {usage.windows
            .map(
              (window) =>
                `${windowLabel(window.window_minutes)} ${Math.round(window.used_percent)}%`,
            )
            .join(' · ')}
        </p>
      ) : null}
    </div>
  )
}
