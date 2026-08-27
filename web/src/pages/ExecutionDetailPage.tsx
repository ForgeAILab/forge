import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import { useNavigate } from '@tanstack/react-router'
import { useHotkeys } from 'react-hotkeys-hook'
import { Panel, PanelGroup, PanelResizeHandle } from 'react-resizable-panels'
import { toast } from 'sonner'
import {
  ArrowDown,
  ClockCounterClockwise,
  Sidebar,
  Spinner,
} from '@phosphor-icons/react'
import {
  useAgentsQuery,
  useExecutionHookLogs,
  useExecutionLogs,
  useExecutionQuery,
  useExecutionUsageQuery,
  useFollowUpExecution,
  useTaskQuery,
  type ExecutionLogsParams,
} from '@/api/hooks'
import { apiFetch } from '@/api/client'
import { ExecutionViewer, type ExecutionViewerMode } from '@/components/execution-viewer'
import { ExecutionDetailSidebar } from '@/components/execution-detail/ExecutionDetailSidebar'
import { ExecutionFollowUpComposer } from '@/components/execution-detail/ExecutionFollowUpComposer'
import {
  defaultLogKinds,
  ExecutionLogFilterDropdown,
} from '@/components/execution-detail/ExecutionLogFilterDropdown'
import { ExecutionStatusBadge } from '@/components/execution-detail/ExecutionStatusBadge'
import type { ExecutionConfigValue } from '@/components/execution-config/ExecutionConfigBar'
import { Button } from '@/components/ui/button'
import { Tooltip } from '@/components/ui/tooltip'
import { getApiErrorMessage } from '@/lib/api-error'
import { cn } from '@/lib/cn'
import { roleDisplayName } from '@/lib/execution-utils'
import { productTerm } from '@/lib/i18n'
import { saveRecentExecutionSelection } from '@/lib/execution-config-storage'
import { useAuthStore } from '@/stores/auth'
import {
  compareLogsChronologically,
  mergeLogs,
  parseExecutionLogEvent,
} from '@/lib/execution-log-utils'
import { effectiveLogFilterKind, type LogFilterKind } from '@/lib/log-filter'
import type {
  Execution,
  FollowUpRequest,
  LaunchExecutionResponse,
  LogEntry,
} from '@/types/generated'

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null
}

function snapshotString(
  snapshot: Record<string, unknown> | null | undefined,
  key: string,
): string | null {
  const value = snapshot?.[key]
  return typeof value === 'string' && value.trim() ? value : null
}

function snapshotConfigString(
  snapshot: Record<string, unknown> | null | undefined,
  key: string,
): string | null {
  const config = snapshot?.config
  if (!isRecord(config)) return null
  const value = config[key]
  return typeof value === 'string' && value.trim() ? value : null
}

function executionOverrideKeys(snapshot: Record<string, unknown> | null | undefined): Set<string> {
  const overridesApplied = snapshot?.overrides_applied
  if (!isRecord(overridesApplied) || !Array.isArray(overridesApplied.execution)) return new Set()
  return new Set(overridesApplied.execution.filter((key): key is string => typeof key === 'string'))
}

function initialOverridesFromSnapshot(
  snapshot: Record<string, unknown> | null | undefined,
): { modelId: string | null; reasoningEffort: string | null; permissionPolicy: string | null } {
  const executionKeys = executionOverrideKeys(snapshot)
  return {
    modelId: executionKeys.has('model') ? snapshotConfigString(snapshot, 'model') : null,
    reasoningEffort:
      executionKeys.has('model_reasoning_effort') || executionKeys.has('effort')
        ? (snapshotConfigString(snapshot, 'model_reasoning_effort') ??
          snapshotConfigString(snapshot, 'effort'))
        : null,
    permissionPolicy: executionKeys.has('permission_policy')
      ? snapshotConfigString(snapshot, 'permission_policy')
      : null,
  }
}

function extractRunSuffix(title: string): string {
  const parts = title.split(' ')
  if (parts.length < 2) return ''
  const last = parts[parts.length - 1]
  return /^[a-z]+-\d{10,}-[a-z0-9]+$/.test(last) ? last : ''
}

function stripRunSuffix(name: string, suffix: string): string {
  if (!suffix) return name
  const token = ' ' + suffix
  return name.endsWith(token) ? name.slice(0, -token.length) : name
}

const FOLLOW_UP_SEQUENCE_STRIDE = 1_000_000
const LOG_PAGE_LIMIT = 200

type ExecutionTurn = {
  execution: Execution
  logs: LogEntry[]
}

function nextSequenceForLogs(logs: LogEntry[]): number | undefined {
  if (logs.length === 0) return undefined
  return Math.max(...logs.map((log) => log.sequence)) + 1
}

function advanceLogParams(
  current: ExecutionLogsParams,
  nextSequence: number | undefined,
): ExecutionLogsParams {
  if (nextSequence === undefined) return current
  if (current.tail === undefined && (current.from_sequence ?? 0) >= nextSequence) return current
  return { from_sequence: nextSequence, limit: current.limit ?? LOG_PAGE_LIMIT }
}

function promptLogForExecution(execution: Execution, sequence: number): LogEntry | null {
  const prompt = execution.prompt?.trim()
  if (!prompt) return null
  return {
    schema_version: 1,
    sequence,
    timestamp: execution.created_at,
    execution_id: execution.id,
    kind: 'user',
    stream: 'main',
    payload: { text: prompt, source: 'forge_prompt' },
    truncated: false,
  }
}

function dividerLogForExecution(execution: Execution, sequence: number): LogEntry {
  return {
    schema_version: 1,
    sequence,
    timestamp: execution.created_at,
    execution_id: execution.id,
    kind: 'system',
    stream: 'main',
    payload: { type: 'turn_divider', label: 'Follow-up' },
    truncated: false,
  }
}

function logsForExecutionSection(
  execution: Execution,
  sectionLogs: LogEntry[],
  sequenceBase: number,
): LogEntry[] {
  const promptLog = promptLogForExecution(execution, sequenceBase)
  return [
    ...(promptLog ? [promptLog] : []),
    ...sectionLogs.map((log, index) => ({
      ...log,
      sequence: sequenceBase + index + 1,
    })),
  ]
}

async function fetchExecutionLogs(executionId: string): Promise<LogEntry[]> {
  const response = await apiFetch<{ items: LogEntry[]; has_more: boolean }>(
    `/executions/${executionId}/logs`,
    { search: { tail: LOG_PAGE_LIMIT } },
  )
  return response.items ?? []
}

async function fetchExecutionTurn(executionId: string): Promise<ExecutionTurn> {
  const execution: Execution = await apiFetch<Execution>(`/executions/${executionId}`)
  const logs = await fetchExecutionLogs(executionId)
  return { execution, logs }
}

function timelineLogsForTurns(turns: ExecutionTurn[]): LogEntry[] {
  const currentTurnIndex = turns.length - 1
  return turns.flatMap((turn, index) => {
    const sequenceBase = (index - currentTurnIndex) * FOLLOW_UP_SEQUENCE_STRIDE
    return [
      ...(index > 0
        ? [dividerLogForExecution(turn.execution, sequenceBase - 1)]
        : []),
      ...logsForExecutionSection(turn.execution, turn.logs, sequenceBase),
    ]
  })
}

export function ExecutionDetailPage({
  taskId,
  executionId,
  viewerMode,
}: {
  taskId: string
  executionId: string
  viewerMode: ExecutionViewerMode
}) {
  const navigate = useNavigate({ from: '/tasks/$taskId/executions/$executionId' })
  const executionQuery = useExecutionQuery(executionId)
  const executionUsageQuery = useExecutionUsageQuery(executionId)
  const taskQuery = useTaskQuery(taskId)
  const agentsQuery = useAgentsQuery()
  const followUpExecution = useFollowUpExecution(executionId)
  const execution = executionQuery.data
  const parentExecutionId = execution?.parent_execution_id ?? ''
  const parentTurnQuery = useQuery({
    queryKey: ['executions', parentExecutionId, 'timeline-turn'] as const,
    enabled: Boolean(parentExecutionId),
    queryFn: () => fetchExecutionTurn(parentExecutionId),
  })

  const [followUpMessage, setFollowUpMessage] = useState('')
  const [followUpConfig, setFollowUpConfig] = useState<ExecutionConfigValue | null>(null)
  const followUpTextareaRef = useRef<HTMLTextAreaElement>(null)

  const [logsParams, setLogsParams] = useState<ExecutionLogsParams>({ tail: LOG_PAGE_LIMIT })
  const logsQuery = useExecutionLogs(
    executionId,
    logsParams,
    executionQuery.data?.status === 'running',
  )
  const hookLogsQuery = useExecutionHookLogs(executionId)
  const accessToken = useAuthStore((state) => state.accessToken)
  const [logs, setLogs] = useState<LogEntry[]>([])
  const [hasMoreLogs, setHasMoreLogs] = useState(false)
  const [enabledKinds, setEnabledKinds] = useState<Set<LogFilterKind>>(() => new Set(defaultLogKinds))
  const [autoScroll, setAutoScroll] = useState(true)
  const [sidebarOpen, setSidebarOpen] = useState(true)
  const [showConfigBar, setShowConfigBar] = useState(false)
  const [ancestorTurns, setAncestorTurns] = useState<ExecutionTurn[]>([])
  const [isLoadingOlderTurn, setIsLoadingOlderTurn] = useState(false)
  const nextLoadedSequence = useMemo(() => nextSequenceForLogs(logs), [logs])

  const runSuffix = taskQuery.data ? extractRunSuffix(taskQuery.data.title) : ''
  const agentNamesById = useMemo(
    () => new Map((agentsQuery.data?.items ?? []).map((agent) => [agent.id, agent.name])),
    [agentsQuery.data],
  )
  const agentName = (agentId?: string | null) => {
    const name = (agentId ? agentNamesById.get(agentId) : undefined) ?? agentId
    return name ? stripRunSuffix(name, runSuffix) : name
  }

  useHotkeys('i', () => setSidebarOpen((open) => !open))

  useEffect(() => {
    const timeout = window.setTimeout(() => {
      setLogs([])
      setHasMoreLogs(false)
      setLogsParams({ tail: LOG_PAGE_LIMIT })
      setAncestorTurns([])
      setIsLoadingOlderTurn(false)
      setAutoScroll(true)
    }, 0)
    return () => window.clearTimeout(timeout)
  }, [executionId])

  useEffect(() => {
    if (!parentTurnQuery.data) return
    setAncestorTurns([parentTurnQuery.data])
  }, [parentTurnQuery.data])

  useEffect(() => {
    if (!logsQuery.data) return
    const timeout = window.setTimeout(() => {
      setLogs((current) => mergeLogs(current, logsQuery.data.items))
      if (logsQuery.data.items.length > 0) {
        setHasMoreLogs((logsQuery.data.items[0]?.sequence ?? 0) > 0)
      }
      if (executionQuery.data?.status === 'running') {
        const nextSequence = Math.max(
          logsQuery.data.next_sequence ?? 0,
          nextLoadedSequence ?? 0,
        )
        setLogsParams((current) => advanceLogParams(current, nextSequence || undefined))
      }
    }, 0)
    return () => window.clearTimeout(timeout)
  }, [executionQuery.data?.status, logsQuery.data, nextLoadedSequence])

  useEffect(() => {
    if (executionQuery.data?.status !== 'running' || !accessToken) return undefined

    const source = new EventSource(`/api/v1/events?token=${encodeURIComponent(accessToken)}`)
    const handleMessage = (event: MessageEvent<string>) => {
      const parsedLogs = parseExecutionLogEvent(event.data, executionId)
      if (parsedLogs.length === 0) return
      setLogs((current) => mergeLogs(current, parsedLogs))
      setLogsParams((current) => advanceLogParams(current, nextSequenceForLogs(parsedLogs)))
    }

    // Frames are unnamed (D20), so `onmessage` is the only delivery path.
    source.onmessage = handleMessage

    return () => {
      source.close()
    }
  }, [accessToken, executionId, executionQuery.data?.status])

  const executionUsage = execution?.usage ?? executionUsageQuery.data ?? []
  const executionAgentSessionId = execution?.agent_session_id ?? null
  const showRecoveryAction = execution?.status === 'cancelled' || execution?.status === 'failed'
  const recoveryActionLabel = executionAgentSessionId ? 'Continue Session' : 'Re-execute'

  const parentAgent = useMemo(
    () => (agentsQuery.data?.items ?? []).find((agent) => agent.id === execution?.agent_id),
    [agentsQuery.data, execution?.agent_id],
  )
  const parentExecutorType =
    snapshotString(execution?.executor_config_snapshot, 'executor_type') ??
    parentAgent?.executor_type ??
    null

  const followUpInitialAgentId = execution?.agent_id ?? null
  const followUpInitialOverrides = useMemo(
    () => initialOverridesFromSnapshot(execution?.executor_config_snapshot),
    [execution?.executor_config_snapshot],
  )

  const sortedLogs = useMemo(() => [...logs].sort(compareLogsChronologically), [logs])
  const earliestSequence = sortedLogs[0]?.sequence

  const syntheticUserPrompt =
    enabledKinds.has('user') && (earliestSequence === undefined || earliestSequence === 0)
      ? execution?.prompt
      : undefined

  const timelineLogs = useMemo(() => {
    if (!execution || !parentExecutionId) return logs
    return timelineLogsForTurns([
      ...ancestorTurns,
      { execution, logs },
    ])
  }, [ancestorTurns, execution, logs, parentExecutionId])

  const filteredLogs = useMemo(
    () => timelineLogs.filter((log) => enabledKinds.has(effectiveLogFilterKind(log))),
    [enabledKinds, timelineLogs],
  )

  const viewerUserPrompt = parentExecutionId ? undefined : syntheticUserPrompt

  const oldestLoadedParentExecutionId = ancestorTurns[0]?.execution.parent_execution_id ?? null

  const loadOlderFollowUpTurn = useCallback(async () => {
    if (!oldestLoadedParentExecutionId || isLoadingOlderTurn) return
    setAutoScroll(false)
    setIsLoadingOlderTurn(true)
    try {
      const olderTurn = await fetchExecutionTurn(oldestLoadedParentExecutionId)
      setAncestorTurns((current) => {
        if (current.some((turn) => turn.execution.id === olderTurn.execution.id)) return current
        return [olderTurn, ...current]
      })
    } catch (error) {
      toast.error(getApiErrorMessage(error, 'Older follow-up history failed to load'))
    } finally {
      setIsLoadingOlderTurn(false)
    }
  }, [isLoadingOlderTurn, oldestLoadedParentExecutionId])

  const recoveryExecution = useMutation({
    mutationFn: () => {
      if (!execution) {
        throw new Error(`${productTerm('run')} not found`)
      }
      if (executionAgentSessionId) {
        const body: FollowUpRequest = { message: 'Resume' }
        return apiFetch<LaunchExecutionResponse>(`/executions/${execution.id}/follow-up`, {
          method: 'POST',
          body: JSON.stringify(body),
        })
      }
      return apiFetch<LaunchExecutionResponse>(`/executions/${execution.id}/re-execute`, {
        method: 'POST',
      })
    },
    onSuccess: (response) => {
      void navigate({
        to: '/tasks/$taskId/executions/$executionId',
        params: {
          taskId: response.data.task.id,
          executionId: response.data.execution.id,
        },
      })
    },
    onError: (error) => toast.error(getApiErrorMessage(error, `${recoveryActionLabel} failed`)),
  })

  const cancelExecution = useMutation({
    mutationFn: () => {
      if (!execution) throw new Error(`${productTerm('run')} not found`)
      return apiFetch<Execution>(`/executions/${execution.id}/cancel`, { method: 'POST' })
    },
    onSuccess: () => {
      void executionQuery.refetch()
      toast.success(`${productTerm('run')} cancelled`)
    },
    onError: (error) => toast.error(getApiErrorMessage(error, 'Cancel failed')),
  })

  const toggleLogKind = (kind: LogFilterKind) => {
    setEnabledKinds((current) => {
      const next = new Set(current)
      if (next.has(kind)) {
        next.delete(kind)
      } else {
        next.add(kind)
      }
      return next
    })
  }

  const loadEarlierLogs = useCallback(() => {
    if (earliestSequence === undefined || earliestSequence === 0) return
    if (!hasMoreLogs || logsQuery.isFetching) return
    setAutoScroll(false)
    setLogsParams({
      from_sequence: Math.max(0, earliestSequence - LOG_PAGE_LIMIT),
      limit: LOG_PAGE_LIMIT,
    })
  }, [earliestSequence, hasMoreLogs, logsQuery.isFetching])

  const sendFollowUp = () => {
    const trimmed = followUpMessage.trim()
    if (!trimmed) return
    const body: FollowUpRequest = { message: trimmed }
    if (followUpConfig?.agentId) body.agent_id = followUpConfig.agentId
    if (followUpConfig?.overrides) body.overrides = followUpConfig.overrides
    followUpExecution.mutate(body, {
      onSuccess: (response) => {
        saveRecentExecutionSelection(
          followUpConfig?.agentId,
          followUpConfig?.selection ?? {
            modelId: null,
            reasoningEffort: null,
            permissionPolicy: null,
          },
        )
        toast.success('Follow-up sent')
        setFollowUpMessage('')
        setShowConfigBar(false)
        void navigate({
          to: '/tasks/$taskId/executions/$executionId',
          params: {
            taskId: response.data.task.id,
            executionId: response.data.execution.id,
          },
        })
      },
      onError: (error) => toast.error(getApiErrorMessage(error, 'Follow-up failed')),
    })
  }

  return (
    <div className="flex h-full flex-col gap-2">
      <nav className="flex shrink-0 items-center gap-1.5 text-sm text-muted-foreground">
        <button
          type="button"
          className="cursor-pointer hover:text-foreground transition-colors"
          onClick={() => navigate({ to: '/tasks/$taskId', params: { taskId } })}
        >
          Task
        </button>
        <span className="text-muted-foreground/50">/</span>
        <span className="text-foreground font-medium">{productTerm('run')}</span>
      </nav>

      <PanelGroup direction="horizontal" className="flex-1 min-h-0 rounded-xl border border-border-subtle bg-card shadow-soft overflow-hidden">
        <Panel minSize={40}>
          <div className="flex h-full flex-col">
            <div className="flex items-center justify-between gap-2 border-b px-3 py-2">
              <div className="flex items-center gap-2.5 min-w-0">
                {execution ? <ExecutionStatusBadge status={execution.status} /> : null}
                {execution ? (
                  <span className="text-sm font-medium truncate">
                    {roleDisplayName(execution.role)} Session
                  </span>
                ) : null}
              </div>
              <div className="flex items-center gap-1">
                <ExecutionLogFilterDropdown enabledKinds={enabledKinds} onToggle={toggleLogKind} />

                {hasMoreLogs && earliestSequence !== undefined && (
                  <Tooltip content="Load earlier logs">
                    <Button
                      size="sm"
                      variant="ghost"
                      className="h-7 w-7 p-0"
                      disabled={logsQuery.isFetching}
                      onClick={loadEarlierLogs}
                    >
                      {logsQuery.isFetching && logsParams.from_sequence !== undefined ? (
                        <Spinner className="h-3.5 w-3.5 animate-spin" />
                      ) : (
                        <ClockCounterClockwise className="h-3.5 w-3.5" />
                      )}
                    </Button>
                  </Tooltip>
                )}

                {!autoScroll && (
                  <Tooltip content="Scroll to bottom">
                    <Button
                      size="sm"
                      variant="ghost"
                      className="h-7 w-7 p-0"
                      onClick={() => setAutoScroll(true)}
                    >
                      <ArrowDown className="h-3.5 w-3.5" />
                    </Button>
                  </Tooltip>
                )}

                <Tooltip content={sidebarOpen ? 'Hide sidebar (I)' : 'Show sidebar (I)'}>
                  <Button
                    size="sm"
                    variant="ghost"
                    className={cn('h-7 w-7 p-0', sidebarOpen && 'text-primary')}
                    onClick={() => setSidebarOpen((open) => !open)}
                  >
                    <Sidebar className="h-3.5 w-3.5" />
                  </Button>
                </Tooltip>
              </div>
            </div>

            <div className="min-h-0 flex-1">
              <ExecutionViewer
                logs={filteredLogs}
                rawLogs={timelineLogs}
                mode={viewerMode}
                userPrompt={viewerUserPrompt}
                errorMessage={execution?.error}
                autoScroll={autoScroll}
                isLoadingHistory={
                  logsQuery.isLoading || parentTurnQuery.isLoading || isLoadingOlderTurn
                }
                onModeChange={(mode) => {
                  void navigate({
                    search: (prev) => ({
                      ...prev,
                      view: mode === 'chat' ? undefined : mode,
                    }),
                  })
                }}
                onLoadEarlier={
                  oldestLoadedParentExecutionId ? loadOlderFollowUpTurn : loadEarlierLogs
                }
                onScrollBottomChange={setAutoScroll}
                className="h-full"
              />
            </div>

            <ExecutionFollowUpComposer
              message={followUpMessage}
              onMessageChange={setFollowUpMessage}
              isPending={followUpExecution.isPending}
              showConfigBar={showConfigBar}
              onToggleConfigBar={() => setShowConfigBar(!showConfigBar)}
              onSend={sendFollowUp}
              onConfigChange={setFollowUpConfig}
              initialAgentId={followUpInitialAgentId}
              initialOverrides={followUpInitialOverrides}
              executorTypeConstraint={parentExecutorType}
              textareaRef={followUpTextareaRef}
            />
          </div>
        </Panel>

        {sidebarOpen ? (
          <>
            <PanelResizeHandle className="w-px bg-border hover:bg-primary/20 transition-colors" />
            <Panel defaultSize={28} minSize={20} maxSize={42} collapsible>
              <ExecutionDetailSidebar
                isLoading={executionQuery.isLoading}
                execution={execution ?? null}
                logs={logs}
                usage={executionUsage}
                hookLogs={hookLogsQuery.data ?? []}
                agentName={agentName}
                taskId={taskId}
                onNavigateParent={(nextTaskId, nextExecutionId) => {
                  void navigate({
                    to: '/tasks/$taskId/executions/$executionId',
                    params: { taskId: nextTaskId, executionId: nextExecutionId },
                  })
                }}
                actions={{
                  onStop: execution?.status === 'running' ? () => cancelExecution.mutate() : undefined,
                  stopPending: cancelExecution.isPending,
                  onRefresh: execution?.status === 'running' ? () => void executionQuery.refetch() : undefined,
                  refreshPending: executionQuery.isFetching,
                  onContinue: showRecoveryAction && executionAgentSessionId ? () => recoveryExecution.mutate() : undefined,
                  continuePending: recoveryExecution.isPending,
                  onRetry: showRecoveryAction && !executionAgentSessionId ? () => recoveryExecution.mutate() : undefined,
                  retryPending: recoveryExecution.isPending,
                }}
              />
            </Panel>
          </>
        ) : null}
      </PanelGroup>
    </div>
  )
}
