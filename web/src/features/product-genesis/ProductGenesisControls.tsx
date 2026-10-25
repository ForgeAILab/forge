import { useState } from 'react'
import { Link } from '@tanstack/react-router'
import { ArrowRight, CircleNotch, Flask, X } from '@phosphor-icons/react'
import { Button } from '@/components/ui/button'
import { LoadingState } from '@/components/chat/loading-state'
import { useCancelProductGenesisMutation, useProductGenesisActiveQuery } from './hooks'
import { productGenesisVersion } from './types'
import { getApiErrorMessage, lifecycleLabel } from './product-genesis-controls/utils'

/** Compact Genesis status chip for the chat header: lifecycle + cancel. */
export function ProductGenesisControls() {
  const activeQuery = useProductGenesisActiveQuery()
  const cancelMutation = useCancelProductGenesisMutation()
  const active = activeQuery.data?.session ?? null
  const [error, setError] = useState<string | null>(null)
  const projectId = active?.project_id ?? null

  async function cancel() {
    if (!active) return
    setError(null)
    try {
      await cancelMutation.mutateAsync({
        sessionId: active.id,
        input: {
          expected_version: productGenesisVersion(active),
          reason: 'cancelled_from_main_chat',
        },
      })
    } catch (cause) {
      setError(getApiErrorMessage(cause, 'Product Genesis could not be cancelled.'))
    }
  }

  if (activeQuery.isLoading) {
    return <LoadingState label="Loading Product Genesis…" compact />
  }
  if (activeQuery.isError || !active) {
    return null
  }

  return (
    <div className="flex min-w-0 items-center gap-2">
      <div className="flex min-w-0 flex-wrap items-center gap-2 rounded-lg border border-ember-border bg-ember-surface px-3 py-1.5">
        <Flask size={14} className="shrink-0 text-primary" aria-hidden />
        <span className="min-w-0 truncate text-xs text-foreground">
          Genesis · {lifecycleLabel(active.lifecycle)} · {active.maturity}
        </span>
        {projectId && active.lifecycle === 'handed_off' ? (
          <Link
            to="/projects/$projectId/chat"
            params={{ projectId }}
            className="inline-flex items-center gap-1 text-xs font-medium text-primary underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
          >
            Continue with Project Agent
            <ArrowRight size={13} aria-hidden />
          </Link>
        ) : null}
        <Button
          type="button"
          variant="ghost"
          size="sm"
          onClick={() => void cancel()}
          disabled={cancelMutation.isPending || active.lifecycle === 'handed_off'}
          aria-label="Cancel Product Genesis"
          className="h-6 px-1.5"
        >
          {cancelMutation.isPending ? (
            <CircleNotch size={13} className="animate-spin" aria-hidden />
          ) : (
            <X size={13} aria-hidden />
          )}
          Cancel
        </Button>
      </div>
      {error ? (
        <p className="text-xs text-destructive" role="alert">
          {error}
        </p>
      ) : null}
    </div>
  )
}

export type { ProductGenesisCharterCardHandle } from './product-genesis-controls/charter-card'
export { ProductGenesisCharterCard } from './product-genesis-controls/charter-card'
