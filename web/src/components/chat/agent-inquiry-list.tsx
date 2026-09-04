import { useMemo, useState } from 'react'
import { CaretDown, CheckCircle, CircleNotch, MagnifyingGlass, WarningCircle, XCircle } from '@phosphor-icons/react'
import { useAgentInquiriesQuery, useAgentInquiryLogsQuery, useCancelAgentInquiryMutation } from '@/api/hooks'
import { formatRuntimeSeconds, formatTokenCount } from '@/components/task-execution-observability'
import { Button } from '@/components/ui/button'
import { EmptyPanel, ErrorPanel, LoadingPanel } from '@/features/federation/components'
import { numberValue } from '@/features/federation/format'
import { getApiErrorMessage } from '@/lib/api-error'
import { cn } from '@/lib/cn'
import type { AgentInquiryResponse, AgentInquiryTokenUsage } from '@/types/generated'
import { ChatMarkdown } from './chat-markdown'
import { LoadingState } from './loading-state'
import { TurnActivityFeed, turnActivityEntries } from './turn-activity-feed'

function formatDuration(durationMs: bigint | number | null | undefined): string | null {
  const value = numberValue(durationMs, -1)
  if (value < 0) return null
  return formatRuntimeSeconds(value / 1000)
}

/**
 * The four counters are disjoint (context size = input + cache_read +
 * cache_write) — this reports each one honestly instead of folding them into
 * a single misleading "input" number.
 */
function formatInquiryTokenUsage(usage: AgentInquiryTokenUsage): string | null {
  const input = numberValue(usage.input_tokens, 0)
  const output = numberValue(usage.output_tokens, 0)
  const cacheRead = numberValue(usage.cache_read_tokens, 0)
  const cacheWrite = numberValue(usage.cache_write_tokens, 0)
  if (input === 0 && output === 0 && cacheRead === 0 && cacheWrite === 0) return null
  const parts = [`${formatTokenCount(input)} in`]
  if (cacheRead > 0) parts.push(`${formatTokenCount(cacheRead)} cache read`)
  if (cacheWrite > 0) parts.push(`${formatTokenCount(cacheWrite)} cache write`)
  parts.push(`${formatTokenCount(output)} out`)
  return parts.join(' · ')
}

function InquiryRow({ chatId, inquiry }: { chatId: string; inquiry: AgentInquiryResponse }) {
  const [open, setOpen] = useState(false)
  const [cancelError, setCancelError] = useState<string | null>(null)
  const cancelMutation = useCancelAgentInquiryMutation(chatId)
  const isRunning = inquiry.status === 'running'
  const durationLabel = formatDuration(inquiry.duration_ms)
  const tokenSummary = formatInquiryTokenUsage(inquiry.token_usage)
  // Only fetched once the row is open: a collapsed list of many inquiries
  // should not create background log traffic for rows nobody is watching.
  // Polls at the activity cadence while running, stops once terminal.
  const logsQuery = useAgentInquiryLogsQuery(inquiry.id, { live: isRunning, enabled: open })
  const activityEntries = useMemo(
    // Reply deltas only matter while running: a succeeded/failed inquiry's
    // outcome is already shown durably as findings/error above, so showing
    // reply text again in the activity feed would duplicate it.
    () => turnActivityEntries(logsQuery.data ?? [], { includeReply: isRunning }),
    [logsQuery.data, isRunning],
  )

  async function cancel() {
    setCancelError(null)
    try {
      await cancelMutation.mutateAsync({
        id: inquiry.id,
        input: { expected_version: numberValue(inquiry.version, 0) },
      })
    } catch (cause) {
      setCancelError(getApiErrorMessage(cause, 'The inquiry could not be cancelled.'))
    }
  }

  return (
    <li className="rounded-lg border border-border-subtle bg-background px-3 py-2.5">
      <div className="flex items-start gap-2">
        <button
          type="button"
          aria-expanded={open}
          aria-label="Toggle inquiry details"
          onClick={() => setOpen((current) => !current)}
          className="flex min-w-0 flex-1 items-start gap-1.5 rounded-md text-left"
        >
          <CaretDown
            size={11}
            className={cn(
              'mt-0.5 shrink-0 text-muted-foreground/60 transition-transform',
              open ? 'rotate-0' : '-rotate-90',
            )}
            aria-hidden
          />
          <span className="min-w-0 flex-1">
            <span className="block truncate text-xs font-semibold text-foreground" title={inquiry.title}>
              {inquiry.title}
            </span>
            <span className="mt-1 block">
              {isRunning ? (
                <LoadingState compact label="Running…" startedAt={inquiry.started_at} />
              ) : inquiry.status === 'succeeded' ? (
                <span className="inline-flex items-center gap-1.5 text-micro text-muted-foreground">
                  <CheckCircle size={12} className="text-success" aria-hidden />
                  Succeeded{durationLabel ? ` · ${durationLabel}` : ''}
                </span>
              ) : inquiry.status === 'failed' ? (
                <span className="inline-flex items-center gap-1.5 text-micro text-destructive">
                  <WarningCircle size={12} aria-hidden />
                  Failed{durationLabel ? ` · ${durationLabel}` : ''}
                </span>
              ) : (
                <span className="inline-flex items-center gap-1.5 text-micro text-muted-foreground">
                  <XCircle size={12} aria-hidden />
                  Cancelled{durationLabel ? ` · ${durationLabel}` : ''}
                </span>
              )}
            </span>
          </span>
        </button>
        {isRunning ? (
          <Button
            type="button"
            size="sm"
            variant="ghost"
            onClick={() => void cancel()}
            disabled={cancelMutation.isPending}
            aria-label="Cancel inquiry"
            className="h-6 shrink-0 px-2 text-micro text-muted-foreground hover:text-destructive"
          >
            {cancelMutation.isPending ? (
              <CircleNotch size={11} className="animate-spin" aria-hidden />
            ) : null}
            {cancelMutation.isPending ? 'Cancelling…' : 'Cancel'}
          </Button>
        ) : null}
      </div>
      {open ? (
        <div className="ml-[19px] mt-2 flex flex-col gap-2 border-l border-border-subtle pl-3">
          {inquiry.status === 'succeeded' && inquiry.findings ? (
            <div className="prose prose-sm max-w-none break-words text-xs leading-5 text-muted-foreground dark:prose-invert">
              <ChatMarkdown text={inquiry.findings} />
            </div>
          ) : inquiry.status === 'failed' && inquiry.error ? (
            <p className="break-words rounded-md border border-destructive/30 bg-destructive/5 px-2.5 py-1.5 text-xs leading-5 text-destructive">
              {inquiry.error}
            </p>
          ) : isRunning ? (
            <p className="break-words text-xs leading-5 text-muted-foreground">{inquiry.question}</p>
          ) : null}
          {tokenSummary ? (
            <p className="font-mono text-micro text-muted-foreground/80">{tokenSummary}</p>
          ) : null}
          <div className="min-w-0">
            {logsQuery.isLoading ? (
              <span className="text-xs text-muted-foreground">Loading activity…</span>
            ) : logsQuery.isError ? (
              <span className="text-xs text-destructive" role="alert">
                The activity for this inquiry could not be loaded.
              </span>
            ) : activityEntries.length === 0 ? (
              <span className="text-xs text-muted-foreground">
                No activity has been recorded for this inquiry yet.
              </span>
            ) : (
              <TurnActivityFeed entries={activityEntries} live={isRunning} />
            )}
          </div>
        </div>
      ) : null}
      {cancelError ? (
        <p className="mt-1.5 text-micro text-destructive" role="alert">
          {cancelError}
        </p>
      ) : null}
    </li>
  )
}

/**
 * The visible run log of ephemeral, read-only Main Agent inquiry sub-agents
 * (`inquiry.run`). Main Chat only — a Project Agent chat never dispatches
 * these. Deliberately not a task list: the only action is Cancel, and only
 * while an inquiry is running.
 */
export function AgentInquiryList({ chatId }: { chatId: string }) {
  const query = useAgentInquiriesQuery(chatId)
  const inquiries = query.data?.pages.flatMap((page) => page.items) ?? []

  return (
    <section aria-label="Main Agent inquiries" className="min-w-0">
      <div className="flex items-center gap-1.5 px-1 pb-2">
        <MagnifyingGlass size={13} className="text-muted-foreground" aria-hidden />
        <h2 className="font-mono text-micro font-semibold uppercase tracking-[0.8px] text-muted-foreground">
          Inquiries
        </h2>
      </div>
      {query.isLoading ? (
        <LoadingPanel label="Loading inquiries" />
      ) : query.isError ? (
        <ErrorPanel
          title="Inquiries unavailable"
          description="Forge could not load this chat's inquiry run log."
          onRetry={() => void query.refetch()}
        />
      ) : inquiries.length === 0 ? (
        <EmptyPanel
          title="No inquiries yet"
          description="When the Main Agent dispatches a read-only inquiry sub-agent, its run will show up here."
          icon={<MagnifyingGlass size={18} aria-hidden />}
        />
      ) : (
        <ul className="space-y-2">
          {inquiries.map((inquiry) => (
            <InquiryRow key={inquiry.id} chatId={chatId} inquiry={inquiry} />
          ))}
        </ul>
      )}
      {query.hasNextPage ? (
        <div className="mt-2 flex justify-center">
          <Button
            type="button"
            variant="outline"
            size="sm"
            disabled={query.isFetchingNextPage}
            onClick={() => void query.fetchNextPage()}
          >
            {query.isFetchingNextPage ? 'Loading…' : 'Load more'}
          </Button>
        </div>
      ) : null}
    </section>
  )
}
