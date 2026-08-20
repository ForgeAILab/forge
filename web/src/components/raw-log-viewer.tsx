import { useEffect, useRef } from 'react'
import { FitAddon } from '@xterm/addon-fit'
import { WebLinksAddon } from '@xterm/addon-web-links'
import { Terminal } from '@xterm/xterm'
import '@xterm/xterm/css/xterm.css'
import { cn } from '@/lib/cn'
import type { LogEntry } from '@/types/generated'

type RawLogViewerProps = {
  logs: LogEntry[]
  autoScroll?: boolean
  className?: string
  onNewLog?: (log: LogEntry) => void
  onLoadEarlier?: () => void
  onScrollBottomChange?: (isAtBottom: boolean) => void
}

const labelByKind: Record<LogEntry['kind'], string> = {
  stdout: '',
  stderr: '',
  tool_call: 'tool call',
  tool_result: 'tool result',
  assistant: 'assistant',
  assistant_delta: 'assistant delta',
  thinking: 'thinking',
  user: 'user',
  system: 'system',
  file_change: 'file change',
  shell_command: 'shell command',
  approval_question: 'approval question',
  session_info: 'session info',
  unknown: 'unknown',
}

const colorByKind: Record<LogEntry['kind'], string> = {
  stdout: '\x1b[0m',
  stderr: '\x1b[31m',
  tool_call: '\x1b[36m',
  tool_result: '\x1b[32m',
  assistant: '\x1b[35m',
  assistant_delta: '\x1b[35m',
  thinking: '\x1b[90m',
  user: '\x1b[34m',
  system: '\x1b[33m',
  file_change: '\x1b[32m',
  shell_command: '\x1b[36m',
  approval_question: '\x1b[33m',
  session_info: '\x1b[90m',
  unknown: '\x1b[90m',
}

function normalizePayload(payload: string | undefined): string {
  return (payload ?? '').replace(/\r?\n/g, '\r\n')
}

function formatLog(log: LogEntry): string {
  const color = colorByKind[log.kind]
  const reset = '\x1b[0m'
  const payload = normalizePayload(
    typeof log.payload === 'string' ? log.payload : JSON.stringify(log.payload),
  )
  if (log.kind === 'stdout') {
    return `${payload.endsWith('\r\n') ? payload : `${payload}\r\n`}`
  }
  if (log.kind === 'stderr') {
    return `${color}${payload.endsWith('\r\n') ? payload : `${payload}\r\n`}${reset}`
  }
  const timestamp = new Date(log.timestamp)
  const timeLabel = Number.isNaN(timestamp.getTime())
    ? log.timestamp
    : timestamp.toLocaleTimeString()
  return `${color}[${timeLabel}] ${labelByKind[log.kind]}${reset}\r\n${payload.endsWith('\r\n') ? payload : `${payload}\r\n`}`
}

export function RawLogViewer({
  logs,
  autoScroll = true,
  className,
  onNewLog,
  onLoadEarlier,
  onScrollBottomChange,
}: RawLogViewerProps) {
  const containerRef = useRef<HTMLDivElement | null>(null)
  const terminalRef = useRef<Terminal | null>(null)
  const fitAddonRef = useRef<FitAddon | null>(null)
  const renderedLogsRef = useRef<LogEntry[]>([])
  const onNewLogRef = useRef(onNewLog)
  const onLoadEarlierRef = useRef(onLoadEarlier)
  const onScrollBottomChangeRef = useRef(onScrollBottomChange)
  const autoScrollRef = useRef(autoScroll)

  useEffect(() => {
    onNewLogRef.current = onNewLog
  }, [onNewLog])

  useEffect(() => {
    onLoadEarlierRef.current = onLoadEarlier
  }, [onLoadEarlier])

  useEffect(() => {
    onScrollBottomChangeRef.current = onScrollBottomChange
  }, [onScrollBottomChange])

  useEffect(() => {
    autoScrollRef.current = autoScroll
  }, [autoScroll])

  useEffect(() => {
    if (!containerRef.current) return undefined

    const terminal = new Terminal({
      convertEol: true,
      cursorBlink: false,
      fontFamily:
        'ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, "Liberation Mono", monospace',
      fontSize: 13,
      scrollback: 20_000,
      theme: {
        background: '#050505',
        foreground: '#d4d4d4',
        cursor: '#e5e7eb',
        selectionBackground: '#374151',
      },
    })
    const fitAddon = new FitAddon()
    const webLinksAddon = new WebLinksAddon()

    terminal.loadAddon(fitAddon)
    terminal.loadAddon(webLinksAddon)
    terminal.open(containerRef.current)
    fitAddon.fit()

    const scrollDisposable = terminal.onScroll(() => {
      if (terminal.buffer.active.viewportY <= 5) {
        onLoadEarlierRef.current?.()
      }
      const isAtBottom = terminal.buffer.active.viewportY >= terminal.buffer.active.baseY
      onScrollBottomChangeRef.current?.(isAtBottom)
    })

    const resizeObserver = new ResizeObserver(() => {
      fitAddon.fit()
      if (autoScrollRef.current) {
        terminal.scrollToBottom()
      }
    })
    resizeObserver.observe(containerRef.current)

    terminalRef.current = terminal
    fitAddonRef.current = fitAddon

    return () => {
      resizeObserver.disconnect()
      scrollDisposable.dispose()
      renderedLogsRef.current = []
      fitAddonRef.current = null
      terminalRef.current = null
      terminal.dispose()
    }
  }, [])

  useEffect(() => {
    const terminal = terminalRef.current
    if (!terminal) return

    const renderedLogs = renderedLogsRef.current
    const canAppend =
      renderedLogs.length <= logs.length &&
      renderedLogs.every((log, index) => log.sequence === logs[index]?.sequence)
    const logsToWrite = canAppend ? logs.slice(renderedLogs.length) : logs

    if (!canAppend) {
      terminal.clear()
      terminal.reset()
    }

    for (const log of logsToWrite) {
      terminal.write(formatLog(log))
      if (canAppend) {
        onNewLogRef.current?.(log)
      }
    }

    renderedLogsRef.current = logs
    if (autoScroll) {
      terminal.scrollToBottom()
    }
  }, [autoScroll, logs])

  return (
    <div
      ref={containerRef}
      className={cn('h-full min-h-[320px] overflow-hidden bg-black p-2', className)}
    />
  )
}
