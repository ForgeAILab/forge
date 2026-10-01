import { useEffect, useState } from 'react'
import { useDaemonsQuery } from '@/api/hooks'
import { Badge } from '@/components/ui/badge'
import { cn } from '@/lib/cn'
import type { WorkspacePlacementResponse } from '@/types/generated'

const stateStyles: Record<string, string> = {
  reserved: 'border-primary/30 bg-primary/10 text-foreground',
  preparing: 'border-primary/30 bg-primary/10 text-foreground',
  ready: 'border-success/30 bg-success/10 text-foreground',
  disconnected: 'border-warning/40 bg-warning/10 text-foreground',
  cleaning: 'border-primary/30 bg-primary/10 text-foreground',
  cleaned: 'border-border bg-muted text-muted-foreground',
  failed: 'border-destructive/40 bg-destructive/10 text-foreground',
}

function disconnectDuration(disconnectedAt: string, now: number): string | undefined {
  const started = Date.parse(disconnectedAt)
  if (!Number.isFinite(started)) return undefined
  const minutes = Math.max(0, Math.floor((now - started) / 60_000))
  if (minutes < 1) return 'less than a minute'
  if (minutes < 60) return `${minutes}m`
  const hours = Math.floor(minutes / 60)
  if (hours < 24) return `${hours}h ${minutes % 60}m`
  return `${Math.floor(hours / 24)}d ${hours % 24}h`
}

function DisconnectNotice({ disconnectedAt }: { disconnectedAt: string | null }) {
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const interval = setInterval(() => setNow(Date.now()), 30_000)
    return () => clearInterval(interval)
  }, [])
  const elapsed = disconnectedAt ? disconnectDuration(disconnectedAt, now) : undefined

  return (
    <div className="space-y-2 rounded-md border border-warning/40 bg-warning/10 p-3 text-xs">
      <p className="font-medium">
        {elapsed && disconnectedAt ? (
          <time dateTime={disconnectedAt} title={new Date(disconnectedAt).toLocaleString()}>
            Disconnected for {elapsed}
          </time>
        ) : (
          'Disconnect time unavailable'
        )}
      </p>
      <p>This Task waits for its owner to reconnect.</p>
      <p className="text-muted-foreground">
        Retry stays on the same owner. You can also cancel the Task.
      </p>
    </div>
  )
}

export function TaskWorkspacePlacement({
  placement,
}: {
  placement: WorkspacePlacementResponse | null | undefined
}) {
  const { data: daemons } = useDaemonsQuery(
    placement?.owner_kind === 'daemon' && !!placement.daemon_id,
  )
  if (!placement) return null
  const daemon = daemons?.items.find((candidate) => candidate.id === placement.daemon_id)
  const owner =
    placement.owner_kind === 'server'
      ? 'Server'
      : daemon?.hostname || `Daemon ${placement.daemon_id ?? '(unknown)'}`

  return (
    <section aria-label="Workspace placement" className="min-w-0 space-y-2" role="status">
      <p className="text-xs font-medium uppercase tracking-wide text-muted-foreground">
        Workspace placement
      </p>
      <p className="break-all text-sm">
        <span className="text-muted-foreground">Owner </span>
        {owner}
      </p>
      <div className="flex flex-wrap items-center gap-2">
        <span className="text-xs text-muted-foreground">State</span>
        <Badge variant="outline" className={cn('capitalize', stateStyles[placement.state])}>
          {placement.state}
        </Badge>
      </div>
      {placement.state === 'disconnected' ? (
        <DisconnectNotice disconnectedAt={placement.disconnected_at} />
      ) : null}
    </section>
  )
}
