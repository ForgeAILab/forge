import {
  memo,
  useCallback,
  useEffect,
  useLayoutEffect,
  useMemo,
  useRef,
  useState,
  type MouseEvent,
} from 'react'

import {
  ChatAggregatedToolCalls,
  ChatApprovalQuestion,
  ChatAssistantMessage,
  ChatEmptyState,
  ChatErrorMessage,
  ChatFileEdit,
  ChatSessionInfo,
  ChatShellOutput,
  ChatSkeleton,
  ChatSystemMessage,
  ChatThinking,
  ChatToolCall,
  ChatUserMessage,
  type ChatEntry,
} from '@/components/chat'
import { cn } from '@/lib/cn'
import { logsToChatEntries } from '@/lib/logs-to-chat'
import type { LogEntry } from '@/types/generated'
import { productTerm } from '@/lib/i18n'

type ChatLogViewerProps = {
  logs: LogEntry[]
  userPrompt?: string | null
  errorMessage?: string | null
  hiddenLogKinds?: Array<LogEntry['kind'] | string>
  autoScroll?: boolean
  className?: string
  isLoadingHistory?: boolean
  onNewLog?: (log: LogEntry) => void
  onLoadEarlier?: () => void
  onScrollBottomChange?: (isAtBottom: boolean) => void
}

type ChatEntryRendererProps = {
  entry: ChatEntry
}

function entryText(entry: ChatEntry): string | undefined {
  if (entry.kind === 'error') return entry.message ?? entry.title
  if (entry.kind === 'divider') return entry.label
  return 'text' in entry ? entry.text : undefined
}

const ChatEntryRenderer = memo(
  function ChatEntryRenderer({ entry }: ChatEntryRendererProps) {
    switch (entry.kind) {
      case 'assistant':
        return <ChatAssistantMessage entry={entry} />
      case 'thinking':
        return <ChatThinking entry={entry} />
      case 'user':
        return <ChatUserMessage entry={entry} />
      case 'system':
        return <ChatSystemMessage entry={entry} />
      case 'error':
        return <ChatErrorMessage entry={entry} />
      case 'tool_call':
        return <ChatToolCall entry={entry} />
      case 'aggregated_tool_calls':
        return <ChatAggregatedToolCalls entry={entry} />
      case 'shell_output':
        return <ChatShellOutput entry={entry} />
      case 'file_edit':
        return <ChatFileEdit entry={entry} />
      case 'approval':
        return <ChatApprovalQuestion entry={entry} />
      case 'session_info':
        return <ChatSessionInfo entry={entry} />
      case 'divider':
        return (
          <div className="flex items-center gap-3 py-2">
            <div className="h-px flex-1 bg-border" />
            <span className="text-micro font-semibold uppercase tracking-[0.12em] text-muted-foreground">
              {entry.label}
            </span>
            <div className="h-px flex-1 bg-border" />
          </div>
        )
    }
  },
  (previous, next) =>
    previous.entry.sequence === next.entry.sequence &&
    entryText(previous.entry) === entryText(next.entry),
)

export function ChatLogViewer({
  logs,
  userPrompt,
  errorMessage,
  hiddenLogKinds,
  autoScroll = true,
  className,
  isLoadingHistory = false,
  onNewLog,
  onLoadEarlier,
  onScrollBottomChange,
}: ChatLogViewerProps) {
  const scrollContainerRef = useRef<HTMLDivElement | null>(null)
  const pendingLogsRef = useRef<LogEntry[]>([])
  const notifiedLogsRef = useRef<LogEntry[]>([])
  const previousFirstLogSequenceRef = useRef<number | undefined>(undefined)
  const previousScrollHeightRef = useRef(0)
  const onNewLogRef = useRef(onNewLog)
  const onLoadEarlierRef = useRef(onLoadEarlier)
  const [processedLogs, setProcessedLogs] = useState<LogEntry[]>([])
  const [isAtBottom, setIsAtBottom] = useState(autoScroll)

  useEffect(() => {
    onNewLogRef.current = onNewLog
  }, [onNewLog])

  useEffect(() => {
    onLoadEarlierRef.current = onLoadEarlier
  }, [onLoadEarlier])

  useEffect(() => {
    pendingLogsRef.current = logs
    const frameId = requestAnimationFrame(() => {
      setProcessedLogs(pendingLogsRef.current)
    })
    return () => cancelAnimationFrame(frameId)
  }, [logs])

  const entries = useMemo(() => {
    const baseEntries = logsToChatEntries(processedLogs, {
      userPrompt,
      hiddenKinds: hiddenLogKinds,
    })
    const trimmedError = errorMessage?.trim()
    if (!trimmedError) return baseEntries
    if (baseEntries.some((entry) => entry.kind === 'error' && entry.message === trimmedError)) {
      return baseEntries
    }
    return [
      ...baseEntries,
      {
        sequence: Number.MAX_SAFE_INTEGER,
        timestamp: processedLogs[processedLogs.length - 1]?.timestamp ?? '',
        kind: 'error' as const,
        title: `${productTerm('run')} Error`,
        message: trimmedError,
        payload: { message: trimmedError },
      },
    ]
  }, [errorMessage, hiddenLogKinds, processedLogs, userPrompt])

  useEffect(() => {
    const notifiedLogs = notifiedLogsRef.current
    const canAppend =
      notifiedLogs.length <= processedLogs.length &&
      notifiedLogs.every(
        (log, index) => log.sequence === processedLogs[index]?.sequence,
      )
    const logsToNotify = canAppend
      ? processedLogs.slice(notifiedLogs.length)
      : processedLogs

    for (const log of logsToNotify) {
      onNewLogRef.current?.(log)
    }

    notifiedLogsRef.current = processedLogs
  }, [processedLogs])

  useEffect(() => {
    setIsAtBottom(autoScroll)
  }, [autoScroll])

  useEffect(() => {
    if (!autoScroll || !isAtBottom) return

    const scrollContainer = scrollContainerRef.current
    if (!scrollContainer) return

    scrollContainer.scrollTop = scrollContainer.scrollHeight
  }, [autoScroll, entries.length, isAtBottom])

  useLayoutEffect(() => {
    const scrollContainer = scrollContainerRef.current
    if (!scrollContainer) return

    const firstLogSequence = processedLogs[0]?.sequence
    const previousFirstLogSequence = previousFirstLogSequenceRef.current
    const previousScrollHeight = previousScrollHeightRef.current
    const prependedHistory =
      firstLogSequence !== undefined &&
      previousFirstLogSequence !== undefined &&
      firstLogSequence < previousFirstLogSequence

    if (prependedHistory && !autoScroll) {
      scrollContainer.scrollTop += scrollContainer.scrollHeight - previousScrollHeight
    }

    previousFirstLogSequenceRef.current = firstLogSequence
    previousScrollHeightRef.current = scrollContainer.scrollHeight
  }, [autoScroll, processedLogs])

  const handleScroll = useCallback(() => {
    const scrollContainer = scrollContainerRef.current
    if (!scrollContainer) return

    if (scrollContainer.scrollTop <= 120) {
      onLoadEarlierRef.current?.()
    }

    const nextIsAtBottom =
      scrollContainer.scrollTop + scrollContainer.clientHeight >=
      scrollContainer.scrollHeight - 20
    setIsAtBottom(nextIsAtBottom)
    onScrollBottomChange?.(nextIsAtBottom)
  }, [onScrollBottomChange])

  const handleAnchorClick = useCallback((event: MouseEvent<HTMLDivElement>) => {
    const target = (event.target as HTMLElement).closest(
      '[data-scroll-anchor-target]',
    )
    if (!target || !scrollContainerRef.current) return
    const topBefore = target.getBoundingClientRect().top
    requestAnimationFrame(() => {
      const topAfter = target.getBoundingClientRect().top
      const delta = topAfter - topBefore
      if (delta !== 0 && scrollContainerRef.current) {
        scrollContainerRef.current.scrollTop += delta
      }
    })
  }, [])

  return (
    <div
      ref={scrollContainerRef}
      role="log"
      aria-live="polite"
      aria-busy={isLoadingHistory}
      className={cn('h-full min-h-[320px] overflow-auto bg-background p-4', className)}
      onClick={handleAnchorClick}
      onScroll={handleScroll}
    >
      {isLoadingHistory && entries.length === 0 ? <ChatSkeleton /> : null}
      {!isLoadingHistory && entries.length === 0 ? <ChatEmptyState /> : null}
      {entries.length > 0 ? (
        <div className="space-y-3">
          {entries.map((entry, index) => (
            <ChatEntryRenderer key={`${entry.sequence}-${index}`} entry={entry} />
          ))}
        </div>
      ) : null}
    </div>
  )
}
