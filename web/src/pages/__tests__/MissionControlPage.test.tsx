import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { MissionControlPage } from '@/pages/MissionControlPage'
import type { AgentChatEntry } from '@/features/agent-chat/types'
import type { MissionControlResponse } from '@/features/federation/types'

vi.mock('@tanstack/react-router', () => ({
  Link: ({ children, ...props }: { children: React.ReactNode } & Record<string, unknown>) => (
    <a {...props}>{children}</a>
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
}))
vi.mock('@/features/agent-chat/hooks', () => ({
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

const data: MissionControlResponse = {
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

describe('MissionControlPage', () => {
  it('prioritizes attention and review-ready work', () => {
    render(<MissionControlPage />)
    expect(screen.getByText('What needs your attention?')).toBeTruthy()
    expect(screen.getByText('A human decision is required.')).toBeTruthy()
    expect(screen.getByText('Ship the Project worker')).toBeTruthy()
    expect(screen.getByText('Main and Project Agent bindings')).toBeTruthy()
    expect(screen.getByText('Global · Main')).toBeTruthy()
    expect(screen.getByText('Main Agent identity')).toBeTruthy()
    expect(screen.getByText('Task Worker')).toBeTruthy()
    expect(screen.queryByText('Unbound profile')).toBeNull()
    expect(screen.getByText('Coordination activity')).toBeTruthy()
    expect(screen.getByText('Durable direct-command receipts')).toBeTruthy()
    expect(screen.getByText('Pending and approved approval actions')).toBeTruthy()
    expect(screen.getByText('Direct command receipt')).toBeTruthy()
    expect(screen.getByText('Approval action')).toBeTruthy()
    expect(screen.getByText('Committed')).toBeTruthy()
    expect(screen.getByText('Pending')).toBeTruthy()
    expect(screen.getByText('User · user-1')).toBeTruthy()
    expect(screen.getByText('Agent · agent-1')).toBeTruthy()
    expect(screen.getByText('sha256:direct-input')).toBeTruthy()
    expect(screen.getByText('correlation-approval')).toBeTruthy()
    expect(
      screen.getByText('Outcome recorded; payload details are withheld from Mission Control.'),
    ).toBeTruthy()
    expect(screen.queryByText('must not render')).toBeNull()
    const capacity =
      screen.getByText('Runtime capacity').parentElement?.parentElement?.parentElement?.textContent
    expect(capacity).toContain('1')
    expect(capacity).toContain('4')
  })

  it('accounts for an empty coordination activity projection', () => {
    const previous = {
      coordination_activity: data.coordination_activity,
      needs_attention: data.needs_attention,
      review_ready: data.review_ready,
      active_work: data.active_work,
      agent_health: data.agent_health,
    }
    data.coordination_activity = []
    data.needs_attention = []
    data.review_ready = []
    data.active_work = []
    data.agent_health = []

    try {
      render(<MissionControlPage />)

      expect(screen.getByText('No coordination activity recorded')).toBeTruthy()
      expect(screen.getByText(/All scopes are quiet\./)).toBeTruthy()
      expect(screen.queryByText('Durable direct-command receipts')).toBeNull()
    } finally {
      data.coordination_activity = previous.coordination_activity
      data.needs_attention = previous.needs_attention
      data.review_ready = previous.review_ready
      data.active_work = previous.active_work
      data.agent_health = previous.agent_health
    }
  })

  it('keeps semantic progress warnings distinct from owner failure', () => {
    const previous = data.needs_attention
    data.needs_attention = [
      {
        ...previous[0],
        id: 'attention-progress',
        category: 'progress_warning',
        summary: 'Execution is waiting for semantic progress.',
        details: { entity_type: 'task', entity_id: 'task-1' },
        recommended_action: 'Inspect run',
      },
      {
        ...previous[0],
        id: 'attention-failure',
        category: 'execution_failed',
        summary: 'Task execution failed',
        recommended_action: 'Inspect run',
      },
    ]

    try {
      render(<MissionControlPage />)

      const summary = screen.getByText('Execution is waiting for semantic progress.')
      const card = summary.closest('article')
      expect(card?.getAttribute('role')).toBe('status')
      expect(card?.textContent).toContain('owner health is reported separately')
      expect(card?.textContent).toContain('Next: Inspect run')
      expect(card?.className).toContain('bg-warning/5')
      expect(screen.getByLabelText('Inspect run').closest('a')).toBeTruthy()

      const failureCard = screen.getByText('Task execution failed').closest('article')
      expect(failureCard?.getAttribute('role')).toBeNull()
      expect(failureCard?.className).toContain('bg-destructive/5')
    } finally {
      data.needs_attention = previous
    }
  })
})
