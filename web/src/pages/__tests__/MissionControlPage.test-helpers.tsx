// Mission Control mocks, fixtures, and shared authentication setup.

import { afterEach, beforeEach, vi } from 'vitest'
import type { AgentChatEntry } from '@/features/agent-chat/types'
import type { MissionControlResponse } from '@/features/federation/types'
import { useAuthStore } from '@/stores/auth'

const retryTurn = vi.hoisted(() => vi.fn())

const resolveAttention = vi.hoisted(() => vi.fn())

const answerEscalation = vi.hoisted(() => vi.fn())

const turnState = vi.hoisted(() => ({ live: false }))

const taskAction = vi.hoisted(() => vi.fn())

const permissions = vi.hoisted(() => ({
  ownerId: 'user-1' as string | null,
  role: 'owner' as string | null,
  projectLoading: false,
  projectError: false,
  membersLoading: false,
  membersError: false,
}))

vi.mock('@/api/hooks', () => ({
  useTaskAction: () => ({ mutate: taskAction, isPending: false }),
  useAgentQuery: (id?: string) => ({
    data: id === 'lead-agent' ? { name: 'Ada Lead' } : undefined,
  }),
  useProjectQuery: (id: string) => ({
    data: permissions.projectLoading ? undefined : { id, owner_id: permissions.ownerId },
    isLoading: permissions.projectLoading,
    isError: permissions.projectError,
  }),
  useMembersQuery: () => ({
    data: permissions.membersLoading
      ? undefined
      : permissions.role === null
        ? []
        : [{ user_id: 'user-1', role: permissions.role }],
    isLoading: permissions.membersLoading,
    isError: permissions.membersError,
  }),
}))

vi.mock('@tanstack/react-router', () => ({
  Link: ({
    children,
    params,
    ...props
  }: { children: React.ReactNode } & Record<string, unknown>) => (
    <a href="#" {...props} data-params={JSON.stringify(params)}>
      {children}
    </a>
  ),
}))

vi.mock('@/features/federation/hooks', () => ({
  useMissionControlQuery: () => ({
    data,
    isLoading: false,
    isError: false,
    isFetching: false,
    dataUpdatedAt: Date.now(),
    refetch: vi.fn(),
  }),
  useResolveAttentionMutation: () => ({
    mutate: resolveAttention,
    isPending: false,
    isSuccess: false,
    error: null,
  }),
  useAnswerEscalationMutation: () => ({
    mutate: answerEscalation,
    isPending: false,
    isSuccess: false,
    error: null,
  }),
}))

vi.mock('@/features/agent-chat/hooks', () => ({
  useAgentChatTurnsQuery: () => ({
    data: turnState.live ? [{ status: 'leased' }] : [],
    isLoading: false,
    isError: false,
  }),
  useRetryAgentChatTurnMutation: () => ({
    mutate: retryTurn,
    isPending: false,
    isSuccess: false,
    error: null,
  }),
  useAgentChatsQuery: () => ({
    data: { items: chatEntries },
    isLoading: false,
    isError: false,
    refetch: vi.fn(),
  }),
}))

const chatEntries: AgentChatEntry[] = [
  {
    chat_id: 'main-chat',
    kind: 'main',
    project_id: null,
    project_name: null,
    identity_id: 'main-agent',
    identity_name: 'Main Agent',
    binding_state: 'active',
    chat_status: 'ready',
    unread_count: 0n,
    pending_turn_count: 0n,
    last_message_at: null,
  },
]

export const data: MissionControlResponse = {
  needs_attention: [
    {
      id: 'attention-1',
      category: 'review_risk',
      scope_type: 'task',
      scope_id: 'task-1',
      identity_id: null,
      source_event_id: 'event-1',
      priority: 80,
      lifecycle: 'open',
      summary: 'A human decision is required.',
      details: {},
      dedupe_key: 'review:task-1',
      occurred_at: '2026-08-12T12:00:00Z',
      updated_at: '2026-08-12T12:00:00Z',
      version: 1,
      acknowledged_at: null,
      snoozed_until: null,
      resolved_at: null,
      recommended_action: 'Open review',
    },
  ],
  review_ready: [
    {
      task_id: 'task-1',
      title: 'Ship the Project worker',
      project_id: 'project-1',
      status: 'awaiting_human',
      priority: 80,
      primary_action: 'Open review',
      updated_at: '2026-08-12T12:00:00Z',
    },
  ],
  active_work: [],
  agent_health: [
    {
      identity_id: 'main-agent',
      name: 'Main Agent identity',
      backend_kind: 'native',
      provider: 'forge',
      model: 'main-model',
      identity_status: 'ready',
      paused: false,
      connection_status: 'healthy',
      last_activity_at: '2026-08-12T12:00:00Z',
      active_session_count: 0,
      project_count: 0,
    },
    {
      identity_id: 'unbound-profile',
      name: 'Unbound profile',
      backend_kind: 'native',
      provider: 'forge',
      model: 'unused-model',
      identity_status: 'ready',
      paused: false,
      connection_status: 'healthy',
      last_activity_at: null,
      active_session_count: 0,
      project_count: 0,
    },
    {
      identity_id: 'task-worker',
      name: 'Task Worker',
      backend_kind: 'native',
      provider: 'forge',
      model: 'worker-model',
      identity_status: 'ready',
      paused: false,
      connection_status: 'healthy',
      last_activity_at: '2026-08-12T12:00:00Z',
      active_session_count: 1,
      project_count: 1,
    },
  ],
  recent_outcomes: [],
  coordination_activity: [
    {
      id: 'activity-direct',
      activity_kind: 'direct_command',
      actor_type: 'user',
      actor_id: 'user-1',
      scope_type: 'project',
      scope_id: 'project-1',
      operation: 'task.propose',
      input_digest: 'sha256:direct-input',
      policy_result: 'allowed',
      status: 'committed',
      correlation_id: 'correlation-direct',
      outcome: { task_id: 'task-1', secret_payload: 'must not render' },
      occurred_at: '2026-08-12T12:02:00Z',
    },
    {
      id: 'activity-approval',
      activity_kind: 'approval_action',
      actor_type: 'agent',
      actor_id: 'agent-1',
      scope_type: 'project',
      scope_id: 'project-1',
      operation: 'baseline.activate',
      input_digest: 'sha256:approval-input',
      policy_result: 'requires_approval',
      status: 'pending',
      correlation_id: 'correlation-approval',
      outcome: null,
      occurred_at: '2026-08-12T12:01:00Z',
    },
  ],
  capacity: { active_executions: 1, queued_tasks: 0, active_sessions: 4, healthy: true },
  consumer_health: null,
  computed_at: '2026-08-12T12:00:00Z',
}

export function reviewOwnerAttention(): MissionControlResponse['needs_attention'][number] {
  return {
    ...data.needs_attention[0],
    category: 'review_needs_owner',
    scope_type: 'project',
    scope_id: 'project-1',
    summary: 'Review needs the owner: repeated finding F1',
    recommended_action: 'inspect_review',
    details: {
      entity_type: 'task',
      entity_id: 'task-1',
      task: { task_version: 12 },
      interruption: { kind: 'review_needs_owner', reason: 'repeated finding F1' },
      recovery: {
        actions: [
          {
            action: { verb: 'retry' },
            label: 'Retry With Guidance',
            reason: 'review_owner_retry',
            authority: ['owner'],
            parameters: [],
            target_execution_id: null,
            propagates: false,
          },
          {
            action: { verb: 'approve', override: false },
            label: 'Defer Finding to Follow-up',
            reason: 'review_needs_owner',
            authority: ['owner'],
            parameters: [{ name: 'reason', required: true, boolean_values: null }],
            target_execution_id: null,
            propagates: false,
          },
          {
            action: { verb: 'approve', override: true },
            label: 'Override Failed Review',
            reason: 'failed_review_override',
            authority: ['owner'],
            parameters: [{ name: 'reason', required: true, boolean_values: null }],
            target_execution_id: null,
            propagates: false,
          },
        ],
      },
    },
  }
}

beforeEach(() => {
  vi.clearAllMocks()
  Object.assign(permissions, {
    ownerId: 'user-1',
    role: 'owner',
    projectLoading: false,
    projectError: false,
    membersLoading: false,
    membersError: false,
  })
  useAuthStore.setState({
    user: {
      id: 'user-1',
      email: 'owner@example.test',
      display_name: 'Owner',
      is_admin: false,
      created_at: '2026-08-12T12:00:00Z',
    },
  })
})

afterEach(() => useAuthStore.getState().clearAuth())

export { retryTurn, resolveAttention, answerEscalation, turnState, taskAction, permissions }
