import { useEffect } from 'react'
import type { QueryClient } from '@tanstack/react-query'
import { invalidateAnalyticsQueries } from '@/api/analytics-query-invalidation'
import { qk } from '@/api/query-keys'
import { useAuthStore } from '@/stores/auth'
import { useChatSelection } from '@/stores/chat'
import type { AgentChatTurn } from '@/features/agent-chat/types'

/**
 * The backend sends SSE as one canonical envelope per frame (D20): every
 * frame is a default `message` event, and the JSON payload's `event_type`
 * field is the sole routing discriminator:
 *   id: <entity_id>
 *   data: JSON { event_type, entity_id, timestamp, ...context_fields }
 *
 * Context fields vary by event type and are flattened via serde(flatten).
 *
 * There used to be a duplicate identity: the server also set an SSE
 * `event: <event_type>` name, and this client hand-maintained a matching
 * catalog of every name to `addEventListener` on `EventSource`. A frame
 * whose name was missing from that catalog was delivered to no listener and
 * silently dropped before `onmessage` ever saw it (F16). The server no
 * longer sets an `event:` name, so every frame flows through `onmessage` and
 * is routed by `payload.event_type` here instead.
 *
 * `domain_event.committed` is a second, generic envelope (8.4.2): several
 * commands — Project creation from a Charter approval, Main Genesis control
 * transfer, every Agent Chat message/turn, milestone readiness/release —
 * append their durable event inside a larger composite transaction via the
 * `domain_event` outbox rather than publishing a bespoke `event_type`
 * directly. `DomainEventBroadcastConsumer` (services crate) drains that
 * outbox after commit and republishes each row as `domain_event.committed`,
 * carrying `sequence`/`entity_type`/`scope_type`/`scope_id` but not the
 * row's own `event_type`. `routeDomainEventCommitted` below routes those by
 * `scope_type`/`entity_type` to the exact query keys that scope affects,
 * falling back to a full resync only for a scope it does not recognize.
 */
type SsePayload = {
  event_type: string
  entity_id: string
  timestamp: string
  // Flattened context fields (varies by event)
  project_id?: string
  task_id?: string
  assignee_type?: string | null
  assignee_id?: string | null
  agent_id?: string
  name?: string
  old_status?: string
  new_status?: string
  title?: string
  body?: string
  notification_id?: string
  error?: string
  execution_id?: string
  kind?: string | null
  source?: string | null
  chat_id?: string
  handoff_id?: string
  message_id?: string
  media_id?: string
  role?: string
  status?: string
  delta?: string
  // `domain_event.committed` fields (see the module comment above).
  sequence?: number
  entity_type?: string
  scope_type?: string
  scope_id?: string
  [key: string]: unknown
}

type BrowserEvents = {
  dispatch: (name: string, detail: SsePayload) => void
}

function parseSseData(raw: string): SsePayload | undefined {
  try {
    return JSON.parse(raw) as SsePayload
  } catch {
    return undefined
  }
}

function invalidateAllActiveQueries(queryClient: QueryClient): void {
  void queryClient.invalidateQueries(
    {
      predicate: () => true,
      refetchType: 'active',
    },
    { cancelRefetch: false },
  )
}

const TASK_LIST_INVALIDATION_THROTTLE_MS = 500
type TaskListInvalidationState = {
  lastRunAt: number | null
  trailingTimer: ReturnType<typeof setTimeout> | null
  pendingAll: boolean
  pendingProjectIds: Set<string>
}
const taskListInvalidationStates = new WeakMap<QueryClient, TaskListInvalidationState>()

function runProjectTaskListInvalidation(queryClient: QueryClient, projectId?: string): void {
  if (projectId) {
    void queryClient.invalidateQueries(
      { queryKey: qk.projectTasks(projectId) },
      { cancelRefetch: false },
    )
    return
  }
  void queryClient.invalidateQueries(
    {
      predicate: (query) => query.queryKey[0] === 'projects' && query.queryKey[2] === 'tasks',
    },
    { cancelRefetch: false },
  )
}

function invalidateProjectTaskLists(queryClient: QueryClient, projectId?: string): void {
  let state = taskListInvalidationStates.get(queryClient)
  if (!state) {
    state = {
      lastRunAt: null,
      trailingTimer: null,
      pendingAll: false,
      pendingProjectIds: new Set(),
    }
    taskListInvalidationStates.set(queryClient, state)
  }
  if (projectId) {
    if (!state.pendingAll) state.pendingProjectIds.add(projectId)
  } else {
    state.pendingAll = true
    state.pendingProjectIds.clear()
  }

  const flush = () => {
    state.trailingTimer = null
    state.lastRunAt = Date.now()
    if (state.pendingAll) {
      state.pendingAll = false
      state.pendingProjectIds.clear()
      runProjectTaskListInvalidation(queryClient)
      return
    }
    const pendingProjectIds = [...state.pendingProjectIds]
    state.pendingProjectIds.clear()
    for (const pendingProjectId of pendingProjectIds) {
      runProjectTaskListInvalidation(queryClient, pendingProjectId)
    }
  }

  const now = Date.now()
  if (state.lastRunAt === null || now - state.lastRunAt >= TASK_LIST_INVALIDATION_THROTTLE_MS) {
    if (state.trailingTimer) clearTimeout(state.trailingTimer)
    flush()
    return
  }
  if (state.trailingTimer) return
  state.trailingTimer = setTimeout(
    flush,
    TASK_LIST_INVALIDATION_THROTTLE_MS - (now - state.lastRunAt),
  )
}

function invalidateMissionControl(queryClient: QueryClient): void {
  void queryClient.invalidateQueries({ queryKey: ['mission-control'] })
}

// Every `event_type` prefix/exact value this router has bespoke handling
// for below. Anything outside this set still degrades safely (see the
// fallback in `routeSsePayload`) instead of being silently dropped — the
// prior named-listener catalog offered no such fallback, which is exactly
// how F16 went unnoticed.
const KNOWN_EVENT_TYPE_PREFIXES = [
  'task.',
  'agent.',
  'daemon.',
  'workspace.',
  'execution.',
  'agent_chat.',
  'agent_handoff.',
  'project.',
  'project_hook.',
  'review.',
  'merge.',
]

const KNOWN_EVENT_TYPE_EXACT = new Set([
  'reconciliation.event',
  'operations.refreshed',
  'events.resync_required',
  'operations.status_changed',
  'follow_up.dispatched',
  'comment.created',
  'notification.created',
  'domain_event.committed',
])

function isKnownEventType(eventType: string): boolean {
  return (
    KNOWN_EVENT_TYPE_EXACT.has(eventType) ||
    KNOWN_EVENT_TYPE_PREFIXES.some((prefix) => eventType.startsWith(prefix))
  )
}

// `domain_event.committed` (see the module comment above) carries the scope
// of whatever command wrote it, not that command's own `event_type` — so
// routing here keys off `scope_type`/`entity_type` rather than a name. Every
// entry is a scope this app currently writes to the `domain_event` outbox;
// an unlisted scope still converges via the broad-invalidation fallback
// below rather than being silently dropped.
function routeDomainEventCommitted(payload: SsePayload, queryClient: QueryClient): void {
  const scopeId = payload.scope_id
  const scopeType = payload.scope_type
  const entityType = payload.entity_type

  if (scopeType === 'project' && scopeId) {
    void queryClient.invalidateQueries({ queryKey: qk.project(scopeId) })
    void queryClient.invalidateQueries({ queryKey: qk.projects })
    invalidateAnalyticsQueries(queryClient, scopeId)
    if (entityType === 'milestone') {
      void queryClient.invalidateQueries({ queryKey: qk.projectOverview(scopeId) })
    }
    if (entityType === 'task') {
      // Adaptive-boundary/reconciliation events scope to the Project, not
      // the Task, they arose from.
      void queryClient.invalidateQueries({ queryKey: qk.projectReconciliations(scopeId) })
    }
    return
  }

  if (scopeType === 'agent_chat' && scopeId) {
    void queryClient.invalidateQueries({ queryKey: ['agent-chats'] })
    void queryClient.invalidateQueries({ queryKey: ['agent-chats', scopeId] })
    void queryClient.invalidateQueries({ queryKey: ['agent-chats', scopeId, 'messages'] })
    void queryClient.invalidateQueries({ queryKey: ['agent-chats', scopeId, 'turns'] })
    invalidateAnalyticsQueries(queryClient)
    return
  }

  if (scopeType === 'task' && scopeId) {
    void queryClient.invalidateQueries({ queryKey: qk.task(scopeId) })
    void queryClient.invalidateQueries({ queryKey: qk.taskDetail(scopeId) })
    invalidateProjectTaskLists(queryClient)
    if (entityType === 'review') {
      void queryClient.invalidateQueries({ queryKey: qk.reviews(scopeId) })
    }
    invalidateMissionControl(queryClient)
    invalidateAnalyticsQueries(queryClient)
    return
  }

  // An unrecognized scope (or one with no scope_id) still needs to
  // converge the UI. Fall back to the same broad invalidation resync uses
  // rather than dropping the frame.
  invalidateAllActiveQueries(queryClient)
}

export function routeSsePayload(
  payload: SsePayload,
  queryClient: QueryClient,
  browserEvents: BrowserEvents,
): void {
  const eventType = payload.event_type

  // Live stream events are consumed by dedicated UI listeners.
  if (eventType === 'execution.log') return
  if (eventType === 'agent_chat.message_delta') return

  // An `event_type` this router has no bespoke handling for at all (a new
  // server event outside every known prefix/exact value) still needs to
  // converge the UI. Fall back to the same broad invalidation resync uses
  // rather than dropping the frame.
  if (!isKnownEventType(eventType)) {
    invalidateAllActiveQueries(queryClient)
    return
  }

  if (eventType === 'domain_event.committed') {
    routeDomainEventCommitted(payload, queryClient)
    return
  }

  // Resync/reconciliation events.
  if (
    eventType === 'reconciliation.event' ||
    eventType === 'operations.refreshed' ||
    eventType === 'events.resync_required'
  ) {
    invalidateAllActiveQueries(queryClient)
    return
  }

  if (eventType === 'operations.status_changed') {
    void queryClient.invalidateQueries({ queryKey: qk.operationsStatus })
  }

  if (eventType === 'project_hook.run_changed' && payload.project_id) {
    void queryClient.invalidateQueries({ queryKey: qk.projectHookRuns(payload.project_id) })
  }

  if (eventType.startsWith('task.')) {
    const taskId = payload.task_id ?? payload.entity_id
    void queryClient.invalidateQueries({ queryKey: qk.task(taskId) })
    void queryClient.invalidateQueries({ queryKey: qk.taskDetail(taskId) })
    invalidateProjectTaskLists(queryClient, payload.project_id)

    if (
      eventType === 'task.status_changed' ||
      eventType === 'task.moved' ||
      eventType === 'task.assigned' ||
      eventType === 'task.role_reassigned' ||
      eventType === 'task.cancelled' ||
      eventType === 'task.recovered'
    ) {
      void queryClient.invalidateQueries({ queryKey: qk.agents })
    }
    if (eventType === 'task.role_reassigned') {
      void queryClient.invalidateQueries({ queryKey: qk.taskRoles(taskId) })
    }
    if (eventType === 'task.media.uploaded' || eventType === 'task.media.deleted') {
      void queryClient.invalidateQueries({ queryKey: qk.taskMedia(taskId) })
    }
    if (payload.execution_id) {
      void queryClient.invalidateQueries({ queryKey: qk.executions(taskId) })
      void queryClient.invalidateQueries({ queryKey: qk.execution(payload.execution_id) })
    }
    if (eventType === 'task.recovery_applied') {
      void queryClient.invalidateQueries({ queryKey: qk.executions(taskId) })
      void queryClient.invalidateQueries({ queryKey: qk.reviews(taskId) })
      void queryClient.invalidateQueries({ queryKey: qk.transitions(taskId) })
    }
    invalidateMissionControl(queryClient)
    invalidateAnalyticsQueries(queryClient, payload.project_id)
  }

  if (eventType.startsWith('agent.')) {
    void queryClient.invalidateQueries({ queryKey: qk.agents })
    void queryClient.invalidateQueries({ queryKey: qk.agent(payload.entity_id) })
    invalidateMissionControl(queryClient)
  }

  if (eventType.startsWith('daemon.')) {
    void queryClient.invalidateQueries({ queryKey: qk.daemons })
  }

  if (eventType.startsWith('workspace.')) {
    if (payload.task_id) {
      void queryClient.invalidateQueries({ queryKey: qk.taskWorkspace(payload.task_id) })
      void queryClient.invalidateQueries({ queryKey: qk.taskDetail(payload.task_id) })
    } else {
      invalidateAllActiveQueries(queryClient)
    }
  }

  if (eventType.startsWith('execution.')) {
    if (eventType !== 'execution.log') {
      void queryClient.invalidateQueries({ queryKey: qk.agents })
    }
    if (payload.task_id) {
      void queryClient.invalidateQueries({ queryKey: qk.task(payload.task_id) })
      void queryClient.invalidateQueries({ queryKey: qk.taskDetail(payload.task_id) })
      void queryClient.invalidateQueries({ queryKey: qk.executions(payload.task_id) })
      void queryClient.invalidateQueries({ queryKey: qk.taskDiff(payload.task_id) })
      invalidateProjectTaskLists(queryClient)
    }
    void queryClient.invalidateQueries({ queryKey: qk.execution(payload.entity_id) })
    invalidateMissionControl(queryClient)
    invalidateAnalyticsQueries(queryClient, payload.project_id)
  }

  if (eventType.startsWith('agent_chat.')) {
    const chatId = payload.chat_id ?? payload.entity_id
    void queryClient.invalidateQueries({ queryKey: ['agent-chats'] })
    void queryClient.invalidateQueries({ queryKey: ['agent-chats', chatId] })
    void queryClient.invalidateQueries({ queryKey: ['agent-chats', chatId, 'messages'] })
    void queryClient.invalidateQueries({ queryKey: ['agent-chats', chatId, 'turns'] })
    if (payload.project_id) {
      void queryClient.invalidateQueries({ queryKey: ['agent-handoffs', payload.project_id] })
    }
    invalidateAnalyticsQueries(queryClient, payload.project_id)
  }

  if (eventType.startsWith('agent_handoff.')) {
    void queryClient.invalidateQueries({ queryKey: ['agent-chats'] })
    if (payload.project_id) {
      void queryClient.invalidateQueries({ queryKey: ['agent-handoffs', payload.project_id] })
    }
    if (payload.chat_id) {
      void queryClient.invalidateQueries({ queryKey: ['agent-chats', payload.chat_id, 'messages'] })
      void queryClient.invalidateQueries({ queryKey: ['agent-chats', payload.chat_id, 'turns'] })
    }
    invalidateAnalyticsQueries(queryClient, payload.project_id)
  }

  if (eventType.startsWith('project.')) {
    void queryClient.invalidateQueries({ queryKey: qk.project(payload.entity_id) })
    void queryClient.invalidateQueries({ queryKey: qk.projects })
    invalidateAnalyticsQueries(queryClient, payload.entity_id)
    if (eventType === 'project.deleted') {
      // 8.4.4 / F17: an external deletion while a deleted route is open
      // must converge the same way an explicit delete does. `app-shell.tsx`
      // owns the actual scope-clear/navigate — it is the one place that
      // already knows the currently viewed Project — this only carries the
      // notice there. The 404-on-next-fetch path (`DeletedProjectRedirect`)
      // is the fallback if this frame never arrives.
      browserEvents.dispatch('forge:project-deleted', payload)
    }
  }

  if ((eventType.startsWith('review.') || eventType.startsWith('merge.')) && payload.task_id) {
    void queryClient.invalidateQueries({ queryKey: qk.task(payload.task_id) })
    void queryClient.invalidateQueries({ queryKey: qk.taskDetail(payload.task_id) })
    if (eventType.startsWith('review.')) {
      void queryClient.invalidateQueries({ queryKey: qk.reviews(payload.task_id) })
    }
    invalidateMissionControl(queryClient)
  }
  if (eventType === 'follow_up.dispatched' && payload.task_id) {
    void queryClient.invalidateQueries({ queryKey: qk.task(payload.task_id) })
    void queryClient.invalidateQueries({ queryKey: qk.taskDetail(payload.task_id) })
    void queryClient.invalidateQueries({ queryKey: qk.executions(payload.task_id) })
    if (payload.execution_id) {
      void queryClient.invalidateQueries({ queryKey: qk.execution(payload.execution_id) })
    }
  }
  if (eventType === 'comment.created' && payload.task_id) {
    void queryClient.invalidateQueries({ queryKey: qk.comments(payload.task_id) })
  }

  if (eventType === 'notification.created') {
    void queryClient.invalidateQueries({
      predicate: (query) => String(query.queryKey[0]) === 'notifications',
    })
    browserEvents.dispatch('forge:notification-created', payload)
  }
}

// Turn statuses that are done changing. `succeeded` also covers a Main
// Genesis control transfer: `complete_agent_chat_control_transfer` marks the
// source turn `succeeded` with a null response, it just does not add a
// message of its own.
const TERMINAL_TURN_STATUSES = new Set(['succeeded', 'failed', 'cancelled'])

// While a chat turn is live, an SSE frame carrying its next state can be lost
// (dropped connection, a backgrounded tab throttling delivery, a broadcast
// channel at capacity). Correctness cannot depend on that frame arriving
// (D20), so this is the bounded fallback: once either an optimistic pending
// turn or an authoritative cached live turn is older than
// `PENDING_TURN_STALE_AFTER_MS`, re-read its chat's messages/turns directly.
// Watching the authoritative cache matters after the first server read has
// cleared the optimistic entry: a turn can still advance from `retry_wait` to
// `failed` while the tab is hidden. This must be an explicit refetch rather
// than an invalidation because TanStack pauses interval/background
// invalidation refetches for hidden tabs. The bounded refetch also gives the
// REST client a chance to refresh the access token. Polling stops when neither
// source contains an old live turn, so steady state costs nothing extra.
const PENDING_TURN_POLL_INTERVAL_MS = 3_000
const PENDING_TURN_STALE_AFTER_MS = 5_000

function isStaleLiveTurn(turn: AgentChatTurn, now: number): boolean {
  if (TERMINAL_TURN_STATUSES.has(turn.status)) return false
  const startedAt = new Date(turn.created_at).getTime()
  return Number.isNaN(startedAt) || now - startedAt >= PENDING_TURN_STALE_AFTER_MS
}

function pollStalePendingTurns(queryClient: QueryClient): void {
  const { pendingTurns } = useChatSelection.getState()
  const now = Date.now()
  const staleChatIds = new Set<string>()

  for (const [chatId, turns] of Object.entries(pendingTurns)) {
    if (turns.some((turn) => isStaleLiveTurn(turn, now))) staleChatIds.add(chatId)
  }

  for (const [queryKey, turns] of queryClient.getQueriesData<AgentChatTurn[]>({
    queryKey: ['agent-chats'],
  })) {
    if (
      queryKey.length === 3 &&
      queryKey[2] === 'turns' &&
      typeof queryKey[1] === 'string' &&
      turns?.some((turn) => isStaleLiveTurn(turn, now))
    ) {
      staleChatIds.add(queryKey[1])
    }
  }

  for (const chatId of staleChatIds) {
    const messagesKey = ['agent-chats', chatId, 'messages'] as const
    const turnsKey = ['agent-chats', chatId, 'turns'] as const
    if (
      queryClient.getQueryState(messagesKey)?.status === 'error' ||
      queryClient.getQueryState(turnsKey)?.status === 'error'
    ) {
      continue
    }
    void queryClient.refetchQueries({
      queryKey: messagesKey,
      type: 'active',
    })
    void queryClient.refetchQueries({
      queryKey: turnsKey,
      type: 'active',
    })
  }
}

const SSE_INITIAL_RECONNECT_MS = 1_000
const SSE_MAX_RECONNECT_MS = 30_000
const SSE_STABLE_CONNECTION_MS = 30_000
const SSE_RESYNC_AFTER_OPEN_MS = 1_000

export function useSSE(queryClient: QueryClient, accessToken: string | null): void {
  useEffect(() => {
    // Return a cleanup function on every branch so ownership of the stream,
    // reconnect timer, and watchdog is explicit to both React and static
    // effect-lifecycle analysis. This branch allocates nothing.
    if (!accessToken) return () => undefined

    let cancelled = false
    let source: EventSource | null = null
    let backoffMs = SSE_INITIAL_RECONNECT_MS
    let backoffTimer: ReturnType<typeof setTimeout> | null = null
    let stableConnectionTimer: ReturnType<typeof setTimeout> | null = null
    let resyncTimer: ReturnType<typeof setTimeout> | null = null
    // EventSource cannot send an Authorization header, so the access token rides
    // in the query string and is fixed for the life of a connection. Access
    // tokens expire in 15 minutes, well inside a single sitting, after which
    // reconnecting with the captured token 401s forever and the live stream
    // stays silent — a turn completes but its events never arrive. Re-read the
    // store on each reconnect so a token refreshed elsewhere is picked up.
    //
    // Deliberately does NOT refresh: refresh tokens are single-use, so every
    // extra caller is another chance to burn one and destroy the session. REST
    // traffic owns refreshing, and a refresh there re-runs this effect.
    let streamToken = accessToken

    const handleEvent = (event: MessageEvent<string>) => {
      const payload = parseSseData(event.data)
      if (!payload) return
      routeSsePayload(payload, queryClient, {
        dispatch: (name, detail) => {
          window.dispatchEvent(new CustomEvent(name, { detail }))
        },
      })
    }

    const connect = () => {
      if (backoffTimer) {
        clearTimeout(backoffTimer)
        backoffTimer = null
      }
      const nextSource = new EventSource(`/api/v1/events?token=${encodeURIComponent(streamToken)}`)
      source = nextSource

      // Every frame is a default `message` event (D20): the server no
      // longer sets an SSE `event:` name, so `onmessage` alone sees
      // everything and `routeSsePayload` routes by `payload.event_type`.
      nextSource.onmessage = handleEvent

      nextSource.onerror = () => {
        nextSource.close()
        // Ignore a late callback from a stream that has already been replaced.
        if (source !== nextSource) return
        if (cancelled) return
        if (stableConnectionTimer) {
          clearTimeout(stableConnectionTimer)
          stableConnectionTimer = null
        }
        if (resyncTimer) {
          clearTimeout(resyncTimer)
          resyncTimer = null
        }
        if (backoffTimer) return
        const reconnectDelay = backoffMs
        backoffMs = Math.min(backoffMs * 2, SSE_MAX_RECONNECT_MS)
        backoffTimer = setTimeout(reconnect, reconnectDelay)
      }

      nextSource.onopen = () => {
        const openedSource = nextSource
        if (stableConnectionTimer) clearTimeout(stableConnectionTimer)
        stableConnectionTimer = setTimeout(() => {
          if (!cancelled && source === openedSource) {
            backoffMs = SSE_INITIAL_RECONNECT_MS
          }
        }, SSE_STABLE_CONNECTION_MS)
        // A connection that opens and immediately fails must not trigger a
        // full-query refetch on every flap. Resync only after it remains open
        // long enough to be useful; an error clears this timer.
        if (resyncTimer) clearTimeout(resyncTimer)
        resyncTimer = setTimeout(() => {
          resyncTimer = null
          if (!cancelled && source === openedSource) {
            invalidateAllActiveQueries(queryClient)
          }
        }, SSE_RESYNC_AFTER_OPEN_MS)
      }
    }

    const reconnect = () => {
      if (cancelled) return
      const current = useAuthStore.getState().accessToken
      if (current) streamToken = current
      connect()
    }

    connect()
    const pendingTurnWatchdog = setInterval(
      () => pollStalePendingTurns(queryClient),
      PENDING_TURN_POLL_INTERVAL_MS,
    )
    return () => {
      cancelled = true
      if (backoffTimer) clearTimeout(backoffTimer)
      if (stableConnectionTimer) clearTimeout(stableConnectionTimer)
      if (resyncTimer) clearTimeout(resyncTimer)
      clearInterval(pendingTurnWatchdog)
      source?.close()
    }
  }, [queryClient, accessToken])
}
