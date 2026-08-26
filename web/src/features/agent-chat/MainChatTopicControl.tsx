import { useId, useState } from 'react'
import { ClockCounterClockwise, Plus } from '@phosphor-icons/react'

import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { cn } from '@/lib/cn'
import { isTopicResetDenied } from './topics-api'
import { useAgentChatTopicsQuery, useStartAgentChatTopicMutation } from './topics-hooks'

function formatTopicTimestamp(value: string): string {
  const date = new Date(value)
  return Number.isNaN(date.getTime())
    ? value
    : date.toLocaleString([], { dateStyle: 'medium', timeStyle: 'short' })
}

function startErrorMessage(cause: unknown): string {
  if (isTopicResetDenied(cause)) return cause.message
  return cause instanceof Error ? cause.message : 'Could not start a new topic.'
}

/**
 * Main Chat topic boundary control (design D21, live-acceptance finding
 * F18). Shows the current topic and lets the user start a fresh one -- a
 * context epoch *inside* the one account Main Chat, never a second chat.
 *
 * Earlier topics stay inspectable here even though a new Main turn's
 * episodic context is bounded to the current one only.
 */
export function MainChatTopicControl({
  chatId,
  disabled = false,
  disabledReason,
  className,
}: {
  chatId: string | undefined
  disabled?: boolean
  disabledReason?: string
  className?: string
}) {
  const topicsQuery = useAgentChatTopicsQuery(chatId)
  const startMutation = useStartAgentChatTopicMutation(chatId)
  const [open, setOpen] = useState(false)
  const [label, setLabel] = useState('')
  const [formError, setFormError] = useState<string | null>(null)
  const labelInputId = useId()
  const historyId = useId()

  const topics = topicsQuery.data?.items ?? []
  const sortedByRecency = [...topics].reverse()
  const current = topics.find((topic) => topic.is_current) ?? topics.at(-1) ?? null
  const earlier = sortedByRecency.filter((topic) => topic !== current)

  const isStale = topicsQuery.isStale && !topicsQuery.isFetching
  const controlDisabled = disabled || topicsQuery.isLoading || startMutation.isPending

  function openDialog() {
    setFormError(null)
    setLabel('')
    setOpen(true)
  }

  async function startTopic() {
    if (!chatId) return
    setFormError(null)
    try {
      await startMutation.mutateAsync({
        label: label.trim() ? label.trim() : null,
        summary: null,
      })
      setOpen(false)
      setLabel('')
    } catch (cause) {
      setFormError(startErrorMessage(cause))
    }
  }

  return (
    <div className={cn('flex min-w-0 items-center gap-2', className)}>
      {topicsQuery.isError ? (
        <span className="text-micro text-destructive" role="alert">
          Topic history unavailable
        </span>
      ) : current ? (
        <details className="group relative min-w-0">
          <summary
            className="flex min-w-0 cursor-pointer list-none items-center gap-1 rounded-md px-1.5 py-1 text-micro text-muted-foreground marker:content-none hover:bg-muted/40 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring [&::-webkit-details-marker]:hidden"
            aria-controls={earlier.length > 0 ? historyId : undefined}
          >
            <ClockCounterClockwise size={12} aria-hidden />
            <span className="max-w-[10rem] truncate" title={current.label}>
              {current.label}
            </span>
            {isStale ? (
              <span className="rounded-full border border-border px-1 text-[0.6rem] uppercase tracking-[0.06em]">
                Refreshing
              </span>
            ) : null}
          </summary>
          {earlier.length > 0 ? (
            <div
              id={historyId}
              className="absolute right-0 z-20 mt-1 max-h-64 w-64 overflow-y-auto rounded-lg border border-border-subtle bg-card p-2 shadow-md"
            >
              <p className="px-1 pb-1 text-micro font-medium uppercase tracking-[0.06em] text-muted-foreground">
                Earlier topics
              </p>
              <ul className="flex flex-col gap-0.5">
                {earlier.map((topic) => (
                  <li
                    key={topic.id}
                    className="rounded-md px-2 py-1.5 text-xs leading-5 text-foreground"
                  >
                    <span className="block truncate font-medium">{topic.label}</span>
                    <span className="block text-micro text-muted-foreground">
                      {formatTopicTimestamp(topic.created_at)}
                    </span>
                  </li>
                ))}
              </ul>
              <p className="mt-1 px-1 text-micro text-muted-foreground">
                Inspect earlier messages in the chat scrollback above their divider. They are not
                sent to the Main Agent for a new turn.
              </p>
            </div>
          ) : null}
        </details>
      ) : null}
      <Button
        type="button"
        variant="outline"
        size="sm"
        onClick={openDialog}
        disabled={controlDisabled}
        title={disabled ? disabledReason : 'Start a new topic in this chat'}
      >
        <Plus size={13} aria-hidden />
        New topic
      </Button>

      <Dialog open={open} onOpenChange={setOpen} ariaLabel="Start a new Main Chat topic">
        <DialogContent>
          <DialogHeader>
            <DialogTitle>Start a new topic</DialogTitle>
            <DialogDescription>
              This stays the same Main Chat. Forge rotates what the Main Agent sees on your next
              message to just this new topic plus your portfolio state, and adds a visible divider
              here. Earlier topics remain in the scrollback and are never deleted.
            </DialogDescription>
          </DialogHeader>
          <label htmlFor={labelInputId} className="text-xs font-medium text-foreground">
            Topic label (optional)
          </label>
          <Input
            id={labelInputId}
            value={label}
            onChange={(event) => setLabel(event.target.value)}
            placeholder="New topic"
            maxLength={200}
            disabled={startMutation.isPending}
            autoFocus
          />
          {formError ? (
            <p className="mt-2 text-xs leading-5 text-destructive" role="alert">
              {formError}
            </p>
          ) : null}
          <DialogFooter>
            <Button
              type="button"
              variant="outline"
              onClick={() => setOpen(false)}
              disabled={startMutation.isPending}
            >
              Cancel
            </Button>
            <Button type="button" onClick={() => void startTopic()} disabled={startMutation.isPending}>
              {startMutation.isPending ? 'Starting…' : 'Start topic'}
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </div>
  )
}
