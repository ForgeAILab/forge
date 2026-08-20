import { useEffect } from 'react'
import type { QueryClient } from '@tanstack/react-query'
import { qk } from '@/api/query-keys'
import { refreshAccess, useAuthStore } from '@/stores/auth'

/** Margin that keeps a reconnect from racing an expiry it is about to cross. */
const TOKEN_FRESHNESS_MARGIN_MS = 30_000

/** True while the JWT's `exp` is still ahead. An unreadable token counts as stale. */
function accessTokenIsFresh(token: string): boolean {
  const payload = token.split('.')[1]
  if (!payload) return false
  try {
    const claims = JSON.parse(
      atob(payload.replace(/-/g, '+').replace(/_/g, '/')),
    ) as { exp?: number }
    if (typeof claims.exp !== 'number') return false
    return claims.exp * 1000 - Date.now() > TOKEN_FRESHNESS_MARGIN_MS
  } catch {
    return false
  }
}

/**
 * The backend sends SSE with:
 *   event: <event_type>        (e.g. "task.created", "agent.status_changed")
 *   id: <entity_id>
 *   data: JSON { event_type, entity_id, timestamp, ...context_fields }
 *
 * Context fields vary by event type and are flattened via serde(flatten).
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
  void queryClient.invalidateQueries({
    predicate: () => true,
    refetchType: 'active',
  })
}

function invalidateProjectTaskLists(queryClient: QueryClient, projectId?: string): void {
  if (projectId) {
    void queryClient.invalidateQueries({ queryKey: qk.projectTasks(projectId) })
    return
  }
  void queryClient.invalidateQueries({
    predicate: (query) => query.queryKey[0] === 'projects' && query.queryKey[2] === 'tasks',
  })
}

function invalidateMissionControl(queryClient: QueryClient): void {
  void queryClient.invalidateQueries({ queryKey: ['mission-control'] })
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
  }

  if (eventType.startsWith('project.')) {
    void queryClient.invalidateQueries({ queryKey: qk.project(payload.entity_id) })
    void queryClient.invalidateQueries({ queryKey: qk.projects })
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

export function useSSE(queryClient: QueryClient, accessToken: string | null): void {
  useEffect(() => {
    if (!accessToken) return

    let cancelled = false
    let source: EventSource | null = null
    let backoffMs = 1000
    let backoffTimer: ReturnType<typeof setTimeout> | null = null
    // EventSource cannot send an Authorization header, so the access token rides
    // in the query string and is fixed for the life of a connection. Access
    // tokens expire in 15 minutes, well inside a single sitting, after which
    // every reconnect 401s with the same dead token and the live stream stays
    // silent — a turn completes but its events never arrive. Re-read the store
    // on each reconnect and mint a fresh token when the current one is stale.
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
      source = new EventSource(`/api/v1/events?token=${encodeURIComponent(streamToken)}`)

      // Listen on generic message (unnamed events)
      source.onmessage = handleEvent

      // Also listen on known named event types so we catch both
      const namedEvents = [
        'task.created',
        'task.status_changed',
        'task.moved',
        'task.assigned',
        'task.role_reassigned',
        'task.blocked',
        'task.unblocked',
        'task.failed',
        'task.restarted',
        'task.cancelled',
        'task.recovered',
        'task.updated',
        'task.deleted',
        'task.archived',
        'task.execution_launched',
        'task.execution_cancelled',
        'task.execution_retry',
        'task.execution_resumed',
        'task.recovery_applied',
        'task.recovery_action',
        'task.auto_transitioned',
        'task.awaiting_human',
        'task.dependency_satisfied',
        'task.role_agent_dispatched',
        'task.role_notified',
        'task.subtask_sequence_started',
        'task.subtask_sequence_paused',
        'task.subtask_sequence_resumed',
        'task.subtask_commit_recorded',
        'task.media.uploaded',
        'task.media.deleted',
        'execution.started',
        'execution.log',
        'execution.completed',
        'execution.failed',
        'execution.cancelled',
        'execution.stalled',
        'execution.daemon_disconnected',
        'agent.status_changed',
        'agent.created',
        'agent.deleted',
        'agent.paused',
        'agent.resumed',
        'agent.timeout',
        'daemon.registered',
        'daemon.connected',
        'daemon.report_received',
        'daemon.offline',
        'workspace.created',
        'workspace.execution_waiting',
        'workspace.cleaned',
        'project.created',
        'project.updated',
        'project.deleted',
        'project.paused',
        'project.resumed',
        'project_hook.run_changed',
        'profile.created',
        'profile.updated',
        'profile.deleted',
        'review.started',
        'review.decided',
        'review.passed',
        'review.failed',
        'review.approved',
        'review.rejected',
        'merge.started',
        'merge.succeeded',
        'merge.failed',
        'transition.guard_rejected',
        'transition.effect_failed',
        'transition.cascade_depth_exceeded',
        'comment.created',
        'notification.created',
        'follow_up.dispatched',
        'agent_chat.created',
        'agent_chat.updated',
        'agent_chat.message_created',
        'agent_chat.message_delta',
        'agent_chat.message_completed',
        'agent_chat.turn_updated',
        'agent_handoff.created',
        'agent_handoff.delivered',
        'agent_handoff.failed',
        'external_sync.completed',
        'external_sync.failed',
        'reconciliation.event',
        'events.resync_required',
        'operations.refreshed',
        'operations.status_changed',
      ]
      for (const name of namedEvents) {
        source.addEventListener(name, handleEvent as EventListener)
      }

      source.onerror = () => {
        source?.close()
        if (cancelled) return
        backoffTimer = setTimeout(() => {
          void reconnect()
        }, backoffMs)
      }

      source.onopen = () => {
        backoffMs = 1000
      }
    }

    const reconnect = async () => {
      if (cancelled) return
      backoffMs = Math.min(backoffMs * 2, 30_000)
      const current = useAuthStore.getState().accessToken
      if (current && current !== streamToken) {
        // Another caller already refreshed; use what the store holds.
        streamToken = current
      } else if (!accessTokenIsFresh(streamToken)) {
        try {
          streamToken = await refreshAccess()
        } catch {
          // Leave streamToken alone: a refresh that could not reach a verdict is
          // retried on the next backoff tick, and a rejected refresh has already
          // cleared the session, which unmounts this effect.
        }
      }
      if (!cancelled) connect()
    }

    connect()
    return () => {
      cancelled = true
      if (backoffTimer) clearTimeout(backoffTimer)
      source?.close()
    }
  }, [queryClient, accessToken])
}
