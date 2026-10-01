import { useEffect } from 'react'
import type { QueryClient } from '@tanstack/react-query'
import { invalidateAnalyticsQueries } from '@/api/analytics-query-invalidation'
import { qk } from '@/api/query-keys'
import { invalidateProjectTaskDetails } from '@/api/task-detail-invalidation'
import { useAuthStore } from '@/stores/auth'
import { useChatSelection } from '@/stores/chat'
import type { TasksResponse } from '@/types/generated'
import type { InfiniteData } from '@tanstack/react-query'
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
 * carrying `sequence`/`entity_type`/`scope_type`/`scope_id` plus the row's
 * own `domain_event_type`/`domain_entity_id`. `routeDomainEventCommitted`
 * below routes those by `scope_type`/`entity_type` to the exact query keys
 * that scope affects, falling back to a full resync only for a scope it does
 * not recognize. Execution liveness rows only refresh their own Task.
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
  domain_event_type?: string
  domain_entity_id?: string
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

const TASK_RELATION_SUMMARY_EVENTS = new Set([
  'task.created',
  'task.updated',
  'task.status_changed',
  'task.transitioned',
  'task.moved',
  'task.done',
  'task.completed',
  'task.blocked',
  'task.failed',
  'task.cancelled',
  'task.recovered',
  'task.archived',
  'task.deleted',
])

function invalidateProjectTaskRelations(
  queryClient: QueryClient,
  projectId: string,
  changedTaskId: string,
): void {
  void queryClient.invalidateQueries(
    {
      predicate: (query) =>
        query.queryKey[0] === 'tasks' &&
        query.queryKey[2] === 'relations' &&
        query.queryKey[1] !== changedTaskId &&
        query.meta?.projectId === projectId,
      refetchType: 'active',
    },
    { cancelRefetch: false },
  )
}

const TASK_LIST_INVALIDATION_THROTTLE_MS = 1_500
type TaskListInvalidationState = {
  lastRunAt: number | null
  trailingTimer: ReturnType<typeof setTimeout> | null
  pendingAll: boolean
  pendingProjectIds: Set<string>
}
const taskListInvalidationStates = new WeakMap<QueryClient, TaskListInvalidationState>()

type ProjectSummaryInvalidationState = {
  lastRunAt: number | null
  trailingTimer: ReturnType<typeof setTimeout> | null
  pendingProjectIds: Set<string>
}
const projectSummaryInvalidationStates = new WeakMap<QueryClient, ProjectSummaryInvalidationState>()
const PROJECT_SUMMARY_INVALIDATION_THROTTLE_MS = 500

function runProjectSummaryInvalidation(
  queryClient: QueryClient,
  projectIds: Iterable<string>,
): void {
  // These keys are prefixes of every Project-owned query. Keep the summary
  // invalidation exact, then target paginated Project lists explicitly.
  for (const projectId of projectIds) {
    void queryClient.invalidateQueries(
      { queryKey: qk.project(projectId), exact: true },
      { cancelRefetch: false },
    )
  }
  void queryClient.invalidateQueries(
    { queryKey: qk.projects, exact: true },
    { cancelRefetch: false },
  )
  void queryClient.invalidateQueries({ queryKey: qk.projectPagesRoot }, { cancelRefetch: false })
}

function invalidateProjectSlotUsage(queryClient: QueryClient, projectId?: string): void {
  if (projectId) {
    invalidateProjectSummaries(queryClient, projectId)
    return
  }
  // Review and awaiting-human events carry only the Task id. Refresh summary
  // projections without invalidating every Project-owned detail query.
  void queryClient.invalidateQueries(
    {
      predicate: (query) =>
        query.queryKey[0] === 'projects' &&
        query.queryKey.length === 2 &&
        query.queryKey[1] !== 'pages',
    },
    { cancelRefetch: false },
  )
  runProjectSummaryInvalidation(queryClient, [])
}

function invalidateProjectSummaries(queryClient: QueryClient, projectId: string): void {
  let state = projectSummaryInvalidationStates.get(queryClient)
  if (!state) {
    state = {
      lastRunAt: null,
      trailingTimer: null,
      pendingProjectIds: new Set(),
    }
    projectSummaryInvalidationStates.set(queryClient, state)
  }
  state.pendingProjectIds.add(projectId)

  const flush = () => {
    state.trailingTimer = null
    state.lastRunAt = Date.now()
    const pendingProjectIds = [...state.pendingProjectIds]
    state.pendingProjectIds.clear()
    runProjectSummaryInvalidation(queryClient, pendingProjectIds)
  }

  const now = Date.now()
  if (
    state.lastRunAt === null ||
    now - state.lastRunAt >= PROJECT_SUMMARY_INVALIDATION_THROTTLE_MS
  ) {
    if (state.trailingTimer) clearTimeout(state.trailingTimer)
    flush()
    return
  }
  if (state.trailingTimer) return
  state.trailingTimer = setTimeout(
    flush,
    PROJECT_SUMMARY_INVALIDATION_THROTTLE_MS - (now - state.lastRunAt),
  )
}

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

function invalidateProjectTaskLists(
  queryClient: QueryClient,
  projectId?: string,
  defer = false,
): void {
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
  if (defer && !state.trailingTimer) state.lastRunAt = now
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

/** Paint delivered fields immediately; missing diagnostics and uncertain page
 * membership converge through a deferred, throttled authoritative read. */
function patchTaskList(payload: SsePayload, queryClient: QueryClient): boolean {
  if (
    !payload.project_id ||
    !payload.new_status ||
    !['task.status_changed', 'task.moved'].includes(payload.event_type)
  )
    return false
  // Test clients that only model invalidation have no cache to patch.
  if (!queryClient.getQueriesData) return false
  let patched = false
  let needsRefetch = queryClient.isFetching({ queryKey: qk.projectTasks(payload.project_id) }) !== 0
  for (const [key, cached] of queryClient.getQueriesData<InfiniteData<TasksResponse>>({
    queryKey: qk.projectTasks(payload.project_id),
  })) {
    if (!cached?.pages) continue
    // Reject partial lists and membership filters.
    let search: Record<string, unknown> = {}
    try {
      search = JSON.parse(String(key[3] ?? '{}')) as Record<string, unknown>
    } catch {
      needsRefetch = true
      continue
    }
    if (
      cached.pages.some((page) => page.has_more) ||
      [
        'status',
        'canonical_phase',
        'q',
        'agent_id',
        'assignee_id',
        'assignee_type',
        'priority',
      ].some((field) => search[field])
    ) {
      needsRefetch = true
    }
    const taskId = payload.task_id ?? payload.entity_id
    const item = cached.pages.flatMap((page) => page.items).find((task) => task.id === taskId)
    if (!item) {
      needsRefetch = true
      continue
    }
    if (payload.event_type === 'task.moved' && typeof payload.new_board_position !== 'number')
      return false
    // Revision gaps can include renormalized neighbors or missed mutations.
    if (cached.pages.some((page) => payload.board_revision !== page.board_revision + 1))
      needsRefetch = true
    if (item.blocked) needsRefetch = true
    // A status transition can also change diagnostics and retry budgets.
    // Paint the delivered status immediately, then use the throttled fallback
    // for fields the event does not carry. Same-status moves need no refetch
    // only at the next revision and without an older list request in flight.
    if (item.status !== payload.new_status) needsRefetch = true
    if (typeof payload.task_version === 'number' && payload.task_version < item.version) continue
    if (payload.new_status === 'cancelled' && !search.include_cancelled) {
      needsRefetch = true
    }
    // A status/position sort can move a row across pages. Only a complete
    // single page can be reordered locally without guessing page membership.
    if (
      cached.pages.length !== 1 ||
      (search.sort_by &&
        ![
          'board_position',
          'status',
          'id',
          'title',
          'created_at',
          'priority',
          'task_type',
        ].includes(String(search.sort_by)))
    ) {
      needsRefetch = true
    }
    queryClient.setQueryData<InfiniteData<TasksResponse>>(key, {
      ...cached,
      pages: cached.pages.map((page) => {
        const items = page.items.map((task) =>
          task.id === taskId
            ? {
                ...task,
                status: payload.new_status!,
                canonical_phase: task.canonical_phase,
                ...(typeof payload.new_board_position === 'number'
                  ? { board_position: payload.new_board_position }
                  : {}),
                ...(typeof payload.task_version === 'number'
                  ? { version: payload.task_version }
                  : {}),
                updated_at: payload.timestamp,
              }
            : task,
        )
        const sort = String(search.sort_by ?? 'board_position')
        const direction = search.sort_by && search.sort_order === 'desc' ? -1 : 1
        if (
          cached.pages.length === 1 &&
          !page.has_more &&
          (sort === 'board_position' || sort === 'status')
        ) {
          items.sort(
            (a, b) =>
              direction *
                (sort === 'board_position'
                  ? a.board_position - b.board_position
                  : a.status.localeCompare(b.status)) ||
              (sort === 'board_position'
                ? direction * a.created_at.localeCompare(b.created_at)
                : 0) ||
              direction * a.id.localeCompare(b.id),
          )
        }
        return {
          ...page,
          items,
          ...(payload.board_revision === page.board_revision + 1
            ? { board_revision: payload.board_revision }
            : {}),
        }
      }),
    })
    patched = true
  }
  if (patched && needsRefetch) invalidateProjectTaskLists(queryClient, payload.project_id, true)
  return patched
}

function invalidateChatQueries(queryClient: QueryClient, chatId: string): void {
  // The chat prefix includes messages, turns, topics and every turn's activity.
  void queryClient.invalidateQueries({ queryKey: ['agent-chats'], exact: true })
  void queryClient.invalidateQueries({ queryKey: ['agent-chats', chatId] })
  void queryClient.invalidateQueries({ queryKey: ['agent-handoffs'] })
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

  // Execution liveness (`execution.progressed` / `execution.progress_warning`)
  // concerns one running Task. Refresh only that Task's queries — they refetch
  // only while its page is open — plus the throttled Project summary that
  // carries attention counts. Board lists and analytics are unaffected.
  if (
    entityType === 'task' &&
    payload.domain_event_type?.startsWith('execution.progress') &&
    payload.domain_entity_id
  ) {
    void queryClient.invalidateQueries(
      { queryKey: qk.task(payload.domain_entity_id) },
      { cancelRefetch: false },
    )
    if (scopeType === 'project' && scopeId) invalidateProjectSummaries(queryClient, scopeId)
    return
  }

  if (scopeType === 'project' && scopeId) {
    invalidateProjectSummaries(queryClient, scopeId)
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
    invalidateChatQueries(queryClient, scopeId)
    if (entityType === 'agent_inquiry' && payload.domain_entity_id) {
      void queryClient.invalidateQueries({ queryKey: qk.agentInquiry(payload.domain_entity_id) })
    }
    invalidateAnalyticsQueries(queryClient)
    return
  }

  if (scopeType === 'task' && scopeId) {
    void queryClient.invalidateQueries({ queryKey: qk.task(scopeId) })
    invalidateProjectTaskLists(queryClient)
    if (entityType === 'task' || entityType === 'review') {
      invalidateProjectSlotUsage(queryClient, payload.project_id)
    }
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
    if (!patchTaskList(payload, queryClient))
      invalidateProjectTaskLists(queryClient, payload.project_id)
    if (
      TASK_RELATION_SUMMARY_EVENTS.has(eventType) ||
      eventType === 'task.awaiting_human' ||
      eventType === 'task.unblocked' ||
      eventType === 'task.recovery_applied'
    ) {
      invalidateProjectSlotUsage(queryClient, payload.project_id)
    }
    if (payload.project_id && TASK_RELATION_SUMMARY_EVENTS.has(eventType)) {
      invalidateProjectTaskRelations(queryClient, payload.project_id, taskId)
    }

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
    invalidateChatQueries(queryClient, chatId)
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
    invalidateProjectSummaries(queryClient, payload.entity_id)
    invalidateProjectTaskDetails(queryClient, payload.entity_id)
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
    invalidateProjectSlotUsage(queryClient, payload.project_id)
    void queryClient.invalidateQueries({ queryKey: qk.task(payload.task_id) })
    if (eventType.startsWith('review.')) {
      void queryClient.invalidateQueries({ queryKey: qk.reviews(payload.task_id) })
    }
    invalidateMissionControl(queryClient)
  }
  if (eventType === 'follow_up.dispatched' && payload.task_id) {
    void queryClient.invalidateQueries({ queryKey: qk.task(payload.task_id) })
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
// `failed` while the tab is hidden. The watchdog pauses while hidden and
// checks immediately when the document becomes visible, since global
// refetch-on-focus is disabled. These explicit reads also give the REST client
// a chance to refresh the access token. Polling stops when neither source
// contains an old live turn, so steady state costs nothing extra.
const PENDING_TURN_POLL_INTERVAL_MS = 15_000
const PENDING_TURN_STALE_AFTER_MS = 5_000

function isStaleLiveTurn(turn: AgentChatTurn, now: number): boolean {
  if (TERMINAL_TURN_STATUSES.has(turn.status)) return false
  const startedAt = new Date(turn.created_at).getTime()
  return Number.isNaN(startedAt) || now - startedAt >= PENDING_TURN_STALE_AFTER_MS
}

function pollStalePendingTurns(queryClient: QueryClient): void {
  if (document.visibilityState === 'hidden') return
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
    const onVisibilityChange = () => {
      if (document.visibilityState === 'visible') pollStalePendingTurns(queryClient)
    }
    document.addEventListener('visibilitychange', onVisibilityChange)
    return () => {
      cancelled = true
      if (backoffTimer) clearTimeout(backoffTimer)
      if (stableConnectionTimer) clearTimeout(stableConnectionTimer)
      if (resyncTimer) clearTimeout(resyncTimer)
      clearInterval(pendingTurnWatchdog)
      document.removeEventListener('visibilitychange', onVisibilityChange)
      source?.close()
    }
  }, [queryClient, accessToken])
}
