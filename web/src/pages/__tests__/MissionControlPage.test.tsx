import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { MissionControlPage } from '@/pages/MissionControlPage'
import type { AgentChatEntry } from '@/features/agent-chat/types'
import type { MissionControlResponse } from '@/features/federation/types'
import { useAuthStore } from '@/stores/auth'
import type { Offer } from '@/types/generated'

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

function reviewOwnerAttention(): MissionControlResponse['needs_attention'][number] {
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

describe('MissionControlPage', () => {
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

  it.each([
    'viewer',
    'non-member',
    'instance administrator',
    'failed project',
    'failed memberships',
    'signed out',
    'owner id with viewer role',
  ])('keeps review parks visible without unauthorized controls for %s', (viewer) => {
    const previous = data.needs_attention
    permissions.ownerId = 'another-owner'
    permissions.role = 'viewer'
    if (viewer === 'non-member') permissions.role = null
    if (viewer === 'instance administrator') {
      useAuthStore.setState({ user: { ...useAuthStore.getState().user!, is_admin: true } })
    }
    if (viewer === 'signed out') useAuthStore.getState().clearAuth()
    if (viewer === 'failed project') {
      permissions.role = 'owner'
      permissions.projectError = true
    }
    if (viewer === 'failed memberships') {
      permissions.role = 'owner'
      permissions.membersError = true
    }
    if (viewer === 'owner id with viewer role') {
      permissions.ownerId = 'user-1'
    }
    data.needs_attention = [reviewOwnerAttention()]
    try {
      render(<MissionControlPage />)
      const card = screen
        .getByRole('heading', {
          name: 'Review needs the owner: repeated finding F1',
        })
        .closest('article')!
      expect(within(card).getByText('Owner action needed')).toBeTruthy()
      expect(within(card).getByRole('link', { name: 'Open review' })).toBeTruthy()
      expect(within(card).queryAllByRole('button')).toHaveLength(0)
      expect(taskAction).not.toHaveBeenCalled()
      expect(resolveAttention).not.toHaveBeenCalled()
    } finally {
      data.needs_attention = previous
    }
  })

  it.each([
    { viewer: 'membership owner', ownerId: 'another-owner', role: 'owner', isOwner: true },
    { viewer: 'admin member', ownerId: 'another-owner', role: 'admin', isOwner: false },
    { viewer: 'member', ownerId: 'another-owner', role: 'member', isOwner: false },
    { viewer: 'owner id with member role', ownerId: 'user-1', role: 'member', isOwner: false },
    { viewer: 'owner id without membership', ownerId: 'user-1', role: null, isOwner: true },
    { viewer: 'legacy ownerless Project viewer', ownerId: null, role: 'viewer', isOwner: true },
  ])('shows permitted review-park controls to $viewer', ({ ownerId, role, isOwner }) => {
    const previous = data.needs_attention
    Object.assign(permissions, { ownerId, role })
    data.needs_attention = [reviewOwnerAttention()]
    try {
      render(<MissionControlPage />)
      expect(screen.queryByText('Owner action needed')).toBeNull()
      fireEvent.click(screen.getByRole('button', { name: 'Resolve' }))
      expect(resolveAttention).toHaveBeenCalledWith({ id: 'attention-1', expectedVersion: 1 })
      expect(screen.getByRole('button', { name: 'Defer Finding to Follow-up' })).toBeTruthy()
      expect(Boolean(screen.queryByRole('button', { name: 'Override Failed Review' }))).toBe(
        isOwner,
      )
      fireEvent.click(screen.getByRole('button', { name: 'Retry With Guidance' }))
      expect(taskAction).toHaveBeenCalledWith(
        { taskId: 'task-1', version: 12, action: { verb: 'retry' } },
        expect.anything(),
      )
    } finally {
      data.needs_attention = previous
    }
  })

  it.each([
    { pending: 'Project', field: 'projectLoading', role: 'owner' },
    { pending: 'Project', field: 'projectLoading', role: 'member' },
    { pending: 'Project', field: 'projectLoading', role: 'viewer' },
    { pending: 'memberships', field: 'membersLoading', role: 'owner' },
    { pending: 'memberships', field: 'membersLoading', role: 'member' },
    { pending: 'memberships', field: 'membersLoading', role: 'viewer' },
  ] as const)(
    'withholds controls and denial text while $pending loads for $role',
    ({ field, role }) => {
      const previous = data.needs_attention
      Object.assign(permissions, { ownerId: 'another-owner', role, [field]: true })
      data.needs_attention = [reviewOwnerAttention()]
      try {
        const { rerender } = render(<MissionControlPage />)
        const card = screen
          .getByRole('heading', { name: 'Review needs the owner: repeated finding F1' })
          .closest('article')!
        expect(within(card).queryAllByRole('button')).toHaveLength(0)
        expect(within(card).queryByText('Owner action needed')).toBeNull()
        expect(within(card).getByRole('link', { name: 'Open review' })).toBeTruthy()

        permissions[field] = false
        rerender(<MissionControlPage />)
        if (role === 'viewer') {
          expect(within(card).queryAllByRole('button')).toHaveLength(0)
          expect(within(card).getByText('Owner action needed')).toBeTruthy()
        } else {
          expect(within(card).getByRole('button', { name: 'Retry With Guidance' })).toBeTruthy()
          expect(within(card).getByRole('button', { name: 'Resolve' })).toBeTruthy()
          expect(within(card).queryByText('Owner action needed')).toBeNull()
        }
      } finally {
        data.needs_attention = previous
      }
    },
  )

  it.each(['owner', 'admin', 'member'])(
    'handles fixed and optional approval overrides for %s',
    (role) => {
      const previous = data.needs_attention
      permissions.role = role
      permissions.ownerId = 'another-owner'
      const item = reviewOwnerAttention()
      const recovery = item.details.recovery as { actions: Offer[] }
      recovery.actions = [
        {
          ...recovery.actions[2],
          action: { verb: 'approve' },
          label: 'Fixed override',
          parameters: [{ name: 'override', required: true, boolean_values: [true] }],
        },
        {
          ...recovery.actions[1],
          label: 'Approve with optional override',
          parameters: [{ name: 'override', required: true, boolean_values: [false, true] }],
        },
      ]
      data.needs_attention = [item]
      try {
        render(<MissionControlPage />)
        expect(Boolean(screen.queryByRole('button', { name: 'Fixed override' }))).toBe(
          role === 'owner',
        )
        fireEvent.click(screen.getByRole('button', { name: 'Approve with optional override' }))
        if (role === 'owner') {
          expect(screen.getAllByRole('option').map((option) => option.textContent)).toEqual([
            'No',
            'Yes',
          ])
        } else {
          expect(screen.queryByRole('combobox')).toBeNull()
          expect(screen.getByText('Uses the current checks')).toBeTruthy()
          fireEvent.click(screen.getByRole('button', { name: 'Apply' }))
          expect(taskAction).toHaveBeenCalledWith(
            { taskId: 'task-1', version: 12, action: { verb: 'approve', override: false } },
            expect.anything(),
          )
        }
      } finally {
        data.needs_attention = previous
      }
    },
  )

  it.each(['member', 'viewer', 'failed memberships', 'signed out'])(
    'keeps Resolve on other Attention categories for %s',
    (viewer) => {
      permissions.ownerId = 'another-owner'
      permissions.role = viewer
      if (viewer === 'failed memberships') {
        permissions.role = 'member'
        permissions.membersError = true
      }
      if (viewer === 'signed out') useAuthStore.getState().clearAuth()
      render(<MissionControlPage />)
      const card = screen
        .getByRole('heading', { name: 'A human decision is required.' })
        .closest('article')!
      expect(within(card).queryByText('Owner action needed')).toBeNull()
      fireEvent.click(within(card).getByRole('button', { name: 'Resolve' }))
      expect(resolveAttention).toHaveBeenCalled()
    },
  )

  it.each(['owner', 'admin', 'member'])('filters refreshed review offers for %s', (role) => {
    const previous = data.needs_attention
    permissions.role = role
    permissions.ownerId = 'another-owner'
    const item = reviewOwnerAttention()
    const recovery = item.details.recovery as { actions: Offer[] }
    const ownerOffer = recovery.actions[0]
    const agentOffer: Offer = {
      ...ownerOffer,
      label: 'Project Agent retry',
      reason: 'agent_retry',
      authority: ['project_agent'],
    }
    recovery.actions.push(agentOffer)
    data.needs_attention = [item]
    taskAction.mockImplementationOnce((_input, callbacks) =>
      callbacks.onSuccess({
        version: 13,
        available_actions: [
          { ...agentOffer, label: 'Refreshed Project Agent retry' },
          { ...ownerOffer, label: 'Refreshed owner retry' },
          { ...recovery.actions[1], label: 'Refreshed deferral' },
          { ...recovery.actions[2], label: 'Refreshed override' },
        ],
      }),
    )
    try {
      render(<MissionControlPage />)
      expect(screen.queryByRole('button', { name: 'Project Agent retry' })).toBeNull()
      fireEvent.click(screen.getByRole('button', { name: 'Retry With Guidance' }))
      expect(screen.queryByRole('button', { name: 'Refreshed Project Agent retry' })).toBeNull()
      expect(screen.getByRole('button', { name: 'Refreshed deferral' })).toBeTruthy()
      expect(Boolean(screen.queryByRole('button', { name: 'Refreshed override' }))).toBe(
        role === 'owner',
      )
      fireEvent.click(screen.getByRole('button', { name: 'Refreshed owner retry' }))
      expect(taskAction).toHaveBeenLastCalledWith(
        { taskId: 'task-1', version: 13, action: { verb: 'retry' } },
        expect.anything(),
      )
    } finally {
      data.needs_attention = previous
    }
  })

  it('keeps a withdrawn lead draft at its card position until dismissed, and can copy it', async () => {
    const previous = data.needs_attention
    const writeText = vi.fn().mockResolvedValue(undefined)
    vi.stubGlobal('navigator', { ...navigator, clipboard: { writeText } })
    data.needs_attention = [
      {
        ...previous[0],
        id: 'lead-draft',
        category: 'human_input_required',
        recommended_action: 'answer_lead_input',
        details: { question: 'Which parser?' },
      },
      {
        ...previous[0],
        id: 'other-question',
        category: 'human_input_required',
        summary: 'Another question',
      },
    ]
    try {
      const view = render(<MissionControlPage />)
      fireEvent.change(screen.getByLabelText('Answer the lead'), {
        target: { value: '  Use parser A.\nKeep the tests.  ' },
      })
      data.needs_attention = [data.needs_attention[1]]
      view.rerender(<MissionControlPage />)
      const notice = screen
        .getByText(/This question was withdrawn; your draft:/)
        .closest('article')!
      expect(notice.id).toBe('attention-lead-draft')
      expect(notice.textContent).toContain('  Use parser A.\nKeep the tests.  ')
      expect(notice.nextElementSibling?.textContent).toContain('Another question')
      expect(screen.queryByLabelText('Answer the lead')).toBeNull()
      fireEvent.click(screen.getByRole('button', { name: 'Copy draft' }))
      await waitFor(() =>
        expect(writeText).toHaveBeenCalledExactlyOnceWith('  Use parser A.\nKeep the tests.  '),
      )
      expect(await screen.findByRole('button', { name: 'Copied' })).toBeTruthy()
      fireEvent.click(screen.getByRole('button', { name: 'Dismiss draft' }))
      expect(screen.queryByText(/This question was withdrawn/)).toBeNull()
      view.rerender(<MissionControlPage />)
      expect(screen.queryByText(/This question was withdrawn/)).toBeNull()
    } finally {
      data.needs_attention = previous
      vi.unstubAllGlobals()
    }
  })

  it('keys drafts by attention id across refetches and multiple withdrawn questions', () => {
    const previous = data.needs_attention
    data.needs_attention = ['first', 'second', 'empty'].map((id) => ({
      ...previous[0],
      id,
      category: 'human_input_required',
      recommended_action: 'answer_lead_input',
      details: { question: `Question ${id}?` },
    }))
    try {
      const view = render(<MissionControlPage />)
      const inputs = screen.getAllByLabelText('Answer the lead')
      fireEvent.change(inputs[0], { target: { value: 'First answer' } })
      fireEvent.change(inputs[1], { target: { value: 'Second answer' } })
      fireEvent.change(inputs[2], { target: { value: '  ' } })
      data.needs_attention = data.needs_attention.map((item) => ({ ...item, version: 9 }))
      view.rerender(<MissionControlPage />)
      expect((screen.getAllByLabelText('Answer the lead')[0] as HTMLTextAreaElement).value).toBe(
        'First answer',
      )
      data.needs_attention = []
      view.rerender(<MissionControlPage />)
      expect(screen.getAllByText(/This question was withdrawn/)).toHaveLength(2)
      expect(screen.getByText('First answer')).toBeTruthy()
      expect(screen.getByText('Second answer')).toBeTruthy()
      expect(document.getElementById('attention-empty')).toBeNull()
    } finally {
      data.needs_attention = previous
    }
  })

  it('does not call a successfully submitted answer a withdrawn draft', () => {
    const previous = data.needs_attention
    data.needs_attention = [
      {
        ...previous[0],
        id: 'answered',
        category: 'human_input_required',
        recommended_action: 'answer_lead_input',
        details: { question: 'Which parser?' },
      },
    ]
    try {
      const view = render(<MissionControlPage />)
      fireEvent.change(screen.getByLabelText('Answer the lead'), { target: { value: 'Parser A' } })
      fireEvent.click(screen.getByRole('button', { name: 'Resolve' }))
      const options = resolveAttention.mock.calls.at(-1)![1]
      act(() => options.onSuccess?.())
      data.needs_attention = []
      view.rerender(<MissionControlPage />)
      expect(screen.queryByText(/This question was withdrawn/)).toBeNull()
    } finally {
      data.needs_attention = previous
      resolveAttention.mockClear()
    }
  })
  it('renders CI incidents with their count, base failures and task links', () => {
    const previous = data.needs_attention
    data.needs_attention = [
      {
        ...previous[0],
        category: 'base_check_failing',
        summary: 'Base CI needs attention',
        scope_type: 'project',
        recommended_action: 'inspect_ci',
        details: {
          command: 'pnpm test',
          count: 2,
          task_ids: ['task-a', 'task-b'],
          base_commit: 'abc123',
          base_failures: ['suite > base'],
        },
      },
    ]
    try {
      render(<MissionControlPage />)
      const card = screen.getByText('Base CI needs attention').closest('article')
      expect(card?.className).toContain('bg-warning/5')
      expect(card?.textContent).toContain('2 occurrences · 2 tasks')
      expect(screen.getByText('pnpm test')).toBeTruthy()
      expect(screen.getByText('suite > base')).toBeTruthy()
      expect(screen.getByLabelText('Inspect CI for task task-a')).toBeTruthy()
      expect(screen.getByLabelText('Inspect CI for task task-b')).toBeTruthy()
    } finally {
      data.needs_attention = previous
    }
  })

  it('renders conflict hot spots with the existing attention warning treatment', () => {
    const previous = data.needs_attention
    data.needs_attention = [
      {
        ...previous[0],
        category: 'conflict_hotspot',
        scope_type: 'project',
        scope_id: 'project-1',
        summary: 'src/shared.rs conflicted in 3 Tasks this week',
        recommended_action: 'split_hotspot',
      },
    ]
    try {
      render(<MissionControlPage />)
      expect(screen.getByText('Conflict Hotspot')).toBeTruthy()
      expect(screen.getByText('src/shared.rs conflicted in 3 Tasks this week')).toBeTruthy()
      expect(screen.getByText('Split Hotspot')).toBeTruthy()
      expect(
        screen
          .getByText('Conflict Hotspot')
          .closest('article')
          ?.classList.contains('border-warning/30'),
      ).toBe(true)
    } finally {
      data.needs_attention = previous
    }
  })

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

  it.each([
    'repeated finding F1',
    'only non-blocking findings remain',
    'no change after repair round',
  ])('shows review parks with their reason and owner offers: %s', (reason) => {
    const previous = data.needs_attention
    data.needs_attention = [
      {
        ...previous[0],
        category: 'review_needs_owner',
        scope_type: 'project',
        scope_id: 'project-1',
        summary: `Review needs the owner: ${reason}`,
        recommended_action: 'inspect_review',
        details: {
          entity_type: 'task',
          entity_id: 'task-1',
          task: { task_version: 12 },
          interruption: { kind: 'review_needs_owner', reason },
          recovery: {
            actions: [
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
                action: { verb: 'retry' },
                label: 'Retry With Guidance',
                reason: 'review_owner_retry',
                authority: ['owner'],
                parameters: [{ name: 'guidance', required: false, boolean_values: null }],
                target_execution_id: null,
                propagates: false,
              },
            ],
          },
        },
      },
    ]
    try {
      render(<MissionControlPage />)
      const card = screen
        .getByRole('heading', { name: `Review needs the owner: ${reason}` })
        .closest('article')
      expect(card?.className).toContain('bg-warning/5')
      expect(card?.textContent).not.toContain('Task needs recovery')
      expect(
        screen.getByRole('link', { name: 'Open review' }).getAttribute('data-params'),
      ).toContain('review')
      expect(screen.getByRole('button', { name: 'Retry With Guidance' })).toBeTruthy()
      fireEvent.click(screen.getByRole('button', { name: 'Defer Finding to Follow-up' }))
      expect(screen.getByRole('button', { name: 'Apply' }).hasAttribute('disabled')).toBe(true)
      fireEvent.change(screen.getByLabelText('reason (required)'), {
        target: { value: 'Track in follow-up' },
      })
      fireEvent.click(screen.getByRole('button', { name: 'Apply' }))
      expect(taskAction).toHaveBeenCalledWith(
        {
          taskId: 'task-1',
          version: 12,
          action: { verb: 'approve', override: false, reason: 'Track in follow-up' },
        },
        expect.anything(),
      )
    } finally {
      data.needs_attention = previous
      taskAction.mockClear()
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

  it('resolves an attention item with its version', () => {
    const previous = data.needs_attention
    data.needs_attention = [{ ...previous[0], id: 'attention-hotspot', version: 7 }]
    try {
      render(<MissionControlPage />)
      fireEvent.click(screen.getByRole('button', { name: 'Resolve' }))
      expect(resolveAttention).toHaveBeenCalledWith({ id: 'attention-hotspot', expectedVersion: 7 })
    } finally {
      data.needs_attention = previous
      resolveAttention.mockClear()
    }
  })

  it('routes lead-question Resolve through the answer form instead of a version-only request', () => {
    const previous = data.needs_attention
    data.needs_attention = [
      {
        ...previous[0],
        id: 'lead-input-1',
        category: 'human_input_required',
        version: 8,
        recommended_action: 'answer_lead_input',
        details: { question: 'Which parser?' },
      },
    ]
    try {
      render(<MissionControlPage />)
      expect(screen.getAllByRole('button', { name: 'Resolve' })).toHaveLength(1)
      expect(screen.getByText('Which parser?').closest('article')?.id).toBe(
        'attention-lead-input-1',
      )
      fireEvent.change(screen.getByLabelText('Answer the lead'), { target: { value: 'Parser A' } })
      fireEvent.click(screen.getByRole('button', { name: 'Resolve' }))
      expect(resolveAttention).toHaveBeenCalledExactlyOnceWith(
        { id: 'lead-input-1', expectedVersion: 8, answer: 'Parser A' },
        expect.anything(),
      )
    } finally {
      data.needs_attention = previous
      resolveAttention.mockClear()
    }
  })

  it('names the lead Agent on a coordination activity receipt, with ID fallback', () => {
    const previous = data.coordination_activity
    data.coordination_activity = [
      { ...previous[0], actor_type: 'task_lead', actor_id: 'lead-agent' },
      {
        ...previous[0],
        id: 'other-lead-receipt',
        actor_type: 'task_lead',
        actor_id: 'unavailable-lead',
      },
    ]
    try {
      render(<MissionControlPage />)
      expect(screen.getByText('Lead · Ada Lead')).toBeTruthy()
      expect(screen.getByText('Lead · unavailable-lead')).toBeTruthy()
    } finally {
      data.coordination_activity = previous
    }
  })

  it('shows an escalation need and answers it from the card', () => {
    const previous = data.needs_attention
    data.needs_attention = [
      {
        ...previous[0],
        id: 'attention-escalation',
        category: 'human_input_required',
        scope_type: 'project',
        scope_id: 'project-1',
        summary: 'Blocker cannot-fix',
        recommended_action: 'answer_escalation',
        details: {
          escalation_id: 'escalation-1',
          need: '- Blocker cannot-fix (Task "Build login"; reason: disk full)',
          task_ids: [],
        },
      },
    ]
    try {
      render(<MissionControlPage />)
      expect(
        screen.getByText('- Blocker cannot-fix (Task "Build login"; reason: disk full)'),
      ).toBeTruthy()
      const answer = screen.getByRole('button', { name: 'Answer' })
      expect(answer.hasAttribute('disabled')).toBe(true)
      fireEvent.change(screen.getByLabelText('Answer'), {
        target: { value: 'Freed 20 GB on the host' },
      })
      fireEvent.click(answer)
      expect(answerEscalation).toHaveBeenCalledWith({
        projectId: 'project-1',
        escalationId: 'escalation-1',
        answer: 'Freed 20 GB on the host',
      })
    } finally {
      data.needs_attention = previous
      answerEscalation.mockClear()
    }
  })

  it('re-drives the typed turn action from Attention with its version', () => {
    const previous = data.needs_attention
    data.needs_attention = [
      {
        ...previous[0],
        category: 'retry_exhausted',
        summary: 'Agent Chat turn failed: configuration_invalid',
        recommended_action: 'retry_turn',
        details: {
          retry_action: {
            kind: 'retry_turn',
            chat_id: 'main-chat',
            turn_id: 'turn-1',
            expected_version: 4,
          },
        },
      },
    ]
    try {
      render(<MissionControlPage />)
      fireEvent.click(screen.getByRole('button', { name: 'Retry turn' }))
      expect(retryTurn).toHaveBeenCalledWith({
        turnId: 'turn-1',
        input: {
          expected_version: 4,
          idempotency_key: 'agent-chat-turn-retry:turn-1:4',
        },
      })
    } finally {
      data.needs_attention = previous
      retryTurn.mockClear()
    }
  })

  it('disables the typed retry action while another turn is live', () => {
    const previous = data.needs_attention
    data.needs_attention = [
      {
        ...previous[0],
        category: 'retry_exhausted',
        summary: 'Agent Chat turn failed: configuration_invalid',
        recommended_action: 'retry_turn',
        details: {
          retry_action: {
            kind: 'retry_turn',
            chat_id: 'main-chat',
            turn_id: 'turn-1',
            expected_version: 4,
          },
        },
      },
    ]
    try {
      turnState.live = true
      render(<MissionControlPage />)
      expect(screen.getByRole('button', { name: 'Retry turn' }).hasAttribute('disabled')).toBe(true)
    } finally {
      data.needs_attention = previous
      retryTurn.mockClear()
      turnState.live = false
    }
  })

  it('does not enable retry from an untyped Attention recommendation', () => {
    const previous = data.needs_attention
    data.needs_attention = [{ ...previous[0], recommended_action: 'retry_turn' }]
    try {
      render(<MissionControlPage />)
      expect(screen.queryByRole('button', { name: 'Retry turn' })).toBeNull()
    } finally {
      data.needs_attention = previous
    }
  })
  it('renders worker error kind and message without a processed-event counter', () => {
    data.consumer_health = {
      consumer_name: 'attention_projection',
      last_sequence: 12,
      last_success_at: null,
      last_error_code: 'terminal',
      last_error_message: 'Commitment transition rejected',
      stale: false,
      updated_at: '2026-10-02T00:00:00Z',
    }
    render(<MissionControlPage />)
    expect(screen.getByText('Commitment transition rejected')).toBeTruthy()
    expect(screen.queryByText('Processed events')).toBeNull()
    expect(screen.queryByText('Stale')).toBeNull()
    data.consumer_health = null
  })
})
