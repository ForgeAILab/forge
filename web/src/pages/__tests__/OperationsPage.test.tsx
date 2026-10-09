import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import type { ReactNode } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import {
  useDeadLetterActionMutation,
  useOperationsStatusQuery,
  useRefreshOperationsMutation,
} from '@/api/hooks'
import { ApiError } from '@/api/client'
import { OperationsPage } from '@/pages/OperationsPage'
import { emptyUsage } from '@/test-utils/usage'
import type {
  CostSummary,
  DeadLetterActionResponse,
  OperatorStatusResponse,
  UsageAggregate,
} from '@/types/generated'

type LinkProps = {
  to: string
  params?: Record<string, string>
  className?: string
  children: ReactNode
}

vi.mock('@tanstack/react-router', () => ({
  Link: ({ to, params, className, children }: LinkProps) => {
    const href = params
      ? Object.entries(params).reduce((path, [key, value]) => path.replace(`$${key}`, value), to)
      : to
    return (
      <a href={href} className={className}>
        {children}
      </a>
    )
  },
}))

vi.mock('@/api/hooks', () => ({
  useDeadLetterActionMutation: vi.fn(),
  useOperationsStatusQuery: vi.fn(),
  useRefreshOperationsMutation: vi.fn(),
}))

const pendingCost: CostSummary = {
  ...emptyUsage.cost,
  kind: 'none',
  coverage: 'pending',
  usage_coverage: {
    ...emptyUsage.cost.usage_coverage,
    total_runs_or_turns: 1,
    pending_runs_or_turns: 1,
    total_provider_attempts: 1,
    pending_provider_attempts: 1,
    unpriced_provider_attempts: 1,
    reasons: [
      {
        code: 'pending',
        run_or_turn_count: 1,
        provider_attempt_count: 1,
        tokens: emptyUsage.tokens,
      },
    ],
  },
}

const usageSummary: UsageAggregate = {
  counts: {
    task_execution_count: 1,
    chat_turn_count: 0,
    inquiry_count: 0,
    provider_attempt_count: 1,
  },
  tokens: {
    input_tokens: 1200,
    output_tokens: 450,
    cache_read_tokens: 0,
    cache_write_tokens: 0,
  },
  cost: pendingCost,
}

const degradedStatus: OperatorStatusResponse = {
  check_runs: {
    by_state: { queued: 0, running: 0, cancelling: 0, cleaning: 0, uncertain: 0, succeeded: 0, failed: 0, cancelled: 0 },
    reusable_results: 0,
    admitted_runs: 0,
    waiting_for_capacity: 0,
    borrowed_runs: 0,
  },
  integration_queues: {
    queues_by_state: {},
    current_attempts_by_state: {},
    quarantined_imports: 0,
  },
  pending_remote_cancels: 0,
  periodic_workers: [],
  task_steps: {
    worker_name: 'task_steps',
    pending: 0,
    claimed: 0,
    failed: 0,
    parked: 0,
    in_flight: 0,
    oldest_pending_age_seconds: null,
    last_error: null,
    last_error_at: null,
    restart_count: 0,
  },
  usage_index: {
    current_size_bytes: 10 * 1024 * 1024,
    budget_bytes: 128 * 1024 * 1024,
    fallback: false,
  },
  overall_severity: 'error',
  event_relay: {
    running: true,
    position: 12,
    head: 15,
    last_error: 'tail retry',
    last_error_at: '2026-04-29T11:59:00Z',
  },
  database: { incremental_vacuum: false, free_pages: 42 },
  event_consumers: [
    {
      consumer_name: 'attention_projection',
      last_sequence: 10,
      lag: 5,
      last_advanced_at: '2026-04-29T11:50:00Z',
      oldest_unprocessed_at: '2026-04-29T11:55:00Z',
      oldest_unprocessed_age_seconds: 300,
      stalled: true,
      dead_letter_count: 7,
      recent_dead_letters: [
        {
          id: 'dead-1',
          consumer_name: 'attention_projection',
          event_type: 'task.done',
          attempts: 8,
          item_key: '9',
          replayable: true,
          event_created_at: '2025-04-29T12:00:00Z',
          events_since: 4,
          event_sequence: 9,
          reason: 'blocked commitment',
          occurred_at: '2025-04-29T12:00:00Z',
        },
      ],
    },
  ],
  computed_at: '2026-04-29T12:00:00Z',
  active_executions: [
    {
      execution_id: 'exec-active-1',
      task_id: 'task-active-1',
      task_title: null,
      role: 'coder',
      agent_id: 'agent-1',
      agent_name: 'Agent One',
      daemon_id: 'daemon-1',
      workspace_id: 'workspace-active-1',
      workspace_path: '/workspaces/task-active-1',
      session_id: 'session-1',
      started_at: '2026-04-29T11:30:00Z',
      runtime_seconds: 1800,
      elapsed_seconds: 1800,
      latest_event: 'Waiting for policy approval',
      last_event: 'Waiting for policy approval',
      last_event_time: '2026-04-29T11:59:00Z',
      turn_count: 3,
      token_totals: {
        tokens: usageSummary.tokens,
        cost: pendingCost,
      },
      rate_limit_snapshot: { requests_remaining: 10 },
      effective_policy: {
        executor_kind: 'codex_cli',
        permission_policy: 'on_request',
        isolation_posture: 'workspace_write',
        is_high_risk: true,
        effective_cwd: '/workspaces/task-active-1',
        workspace_root: '/workspaces/task-active-1',
        environment_posture: 'network_enabled',
        scoped_tools: ['shell'],
        mcp_servers: [],
      },
      plan_progress: {
        total: 4,
        completed: 2,
        remaining: 2,
        available: true,
        warnings: [],
      },
    },
  ],
  blocked_tasks: [
    {
      task_id: 'task-blocked-1',
      title: 'Blocked migration task',
      blocked_reason: 'Waiting for reviewer handoff',
      blocked_since: '2026-04-29T10:00:00Z',
    },
  ],
  daemon_issues: [
    {
      daemon_id: 'daemon-1',
      hostname: 'worker-01',
      issue: 'Heartbeat stale for 90 seconds',
      severity: 'error',
      detected_at: '2026-04-29T11:55:00Z',
    },
  ],
  daemon_pressure: [
    {
      daemon_id: 'server_host',
      hostname: 'Server host',
      active_runs: 2,
      check_runs: 0,
      borrowed_check_runs: 0,
      max_concurrent_runs: 4,
      logical_cores: 8,
      build_jobs_per_run: 2,
      run_nice: 10,
      at_capacity: false,
    },
    {
      daemon_id: 'daemon-1',
      hostname: 'worker-01',
      active_runs: 2,
      check_runs: 0,
      borrowed_check_runs: 0,
      max_concurrent_runs: 4,
      logical_cores: null,
      build_jobs_per_run: null,
      run_nice: null,
      at_capacity: false,
    },
  ],
  agent_pressure: [
    {
      agent_id: 'agent-1',
      agent_name: 'Agent One',
      daemon_id: 'daemon-1',
      active_tasks: 1,
      max_concurrent_tasks: 2,
      at_capacity: false,
    },
  ],
  workspace_cleanup: [
    {
      workspace_id: 'workspace-1',
      task_id: 'task-cleanup-1',
      worktree_path: '/tmp/forge/workspace-1',
      cleanup_after: '2026-04-29T13:00:00Z',
    },
  ],
  retry_pressure: [],
  usage_summary: {
    ...usageSummary,
    active_execution_count: 1,
  },
  recent_errors: [
    {
      entity_type: 'task',
      entity_id: 'task-active-1',
      error: 'Policy escalation failed',
      occurred_at: '2026-04-29T11:58:00Z',
      severity: 'error',
    },
  ],
}

describe('OperationsPage', () => {
  beforeEach(() => {
    vi.mocked(useDeadLetterActionMutation).mockReturnValue({
      mutateAsync: vi.fn(),
    } as unknown as ReturnType<typeof useDeadLetterActionMutation>)
    vi.mocked(useOperationsStatusQuery).mockReturnValue({
      data: degradedStatus,
      isLoading: false,
      isError: false,
      error: null,
      refetch: vi.fn(),
    } as unknown as ReturnType<typeof useOperationsStatusQuery>)
    vi.mocked(useRefreshOperationsMutation).mockReturnValue({
      mutate: vi.fn(),
      isPending: false,
    } as unknown as ReturnType<typeof useRefreshOperationsMutation>)
  })

  function actionResult(outcome: DeadLetterActionResponse['outcome']) {
    return {
      outcome,
      dead_letter: {
        summary: {
          ...degradedStatus.event_consumers[0].recent_dead_letters[0],
          attempts: 9,
          reason: outcome === 'replay_failed' ? 'new error' : 'blocked commitment',
        },
        state: outcome === 'replay_failed' ? ('open' as const) : ('resolved' as const),
        error_kind: 'failure',
        first_failed_at: '',
        last_failed_at: '',
        resolved_at: outcome === 'replay_failed' ? null : '2026-10-03T00:00:00Z',
        resolved_by: 'admin',
        resolution: outcome,
        resolution_reason: null,
      },
    }
  }
  it('replays and shows success inline with resolved actions removed', async () => {
    const mutate = vi.fn().mockResolvedValue(actionResult('replayed'))
    vi.mocked(useDeadLetterActionMutation).mockReturnValue({
      mutateAsync: mutate,
    } as unknown as ReturnType<typeof useDeadLetterActionMutation>)
    render(<OperationsPage />)
    fireEvent.click(screen.getByRole('button', { name: 'Replay' }))
    await screen.findByText('Replayed successfully.')
    expect(mutate).toHaveBeenCalledWith({ id: 'dead-1', action: 'replay', reason: undefined })
    expect(screen.queryByRole('button', { name: 'Replay' })).toBeNull()
    expect(screen.queryByRole('button', { name: 'Dismiss' })).toBeNull()
  })
  it('dismisses with the optional reason and shows its result inline', async () => {
    const mutate = vi.fn().mockResolvedValue(actionResult('dismissed'))
    vi.mocked(useDeadLetterActionMutation).mockReturnValue({
      mutateAsync: mutate,
    } as unknown as ReturnType<typeof useDeadLetterActionMutation>)
    render(<OperationsPage />)
    fireEvent.change(screen.getByLabelText('Dismiss reason (optional)'), {
      target: { value: ' obsolete ' },
    })
    fireEvent.click(screen.getByRole('button', { name: 'Dismiss' }))
    await screen.findByText('Dismissed without delivery.')
    expect(mutate).toHaveBeenCalledWith({ id: 'dead-1', action: 'dismiss', reason: 'obsolete' })
  })
  it('shows new replay error and attempts while keeping the row actionable', async () => {
    vi.mocked(useDeadLetterActionMutation).mockReturnValue({
      mutateAsync: vi.fn().mockResolvedValue(actionResult('replay_failed')),
    } as unknown as ReturnType<typeof useDeadLetterActionMutation>)
    render(<OperationsPage />)
    fireEvent.click(screen.getByRole('button', { name: 'Replay' }))
    await screen.findByText('Replay failed; remains open. Retry after fixing the cause.')
    expect(screen.getByText('new error')).toBeTruthy()
    expect(screen.getByText(/9 attempts/)).toBeTruthy()
    expect(screen.getByRole('button', { name: 'Replay' }).hasAttribute('disabled')).toBe(false)
  })
  it('shows newer polled attempts and errors after a failed action', async () => {
    vi.mocked(useDeadLetterActionMutation).mockReturnValue({
      mutateAsync: vi.fn().mockResolvedValue(actionResult('replay_failed')),
    } as unknown as ReturnType<typeof useDeadLetterActionMutation>)
    const view = render(<OperationsPage />)
    fireEvent.click(screen.getByRole('button', { name: 'Replay' }))
    await screen.findByText('new error')
    vi.mocked(useOperationsStatusQuery).mockReturnValue({
      data: {
        ...degradedStatus,
        event_consumers: [
          {
            ...degradedStatus.event_consumers[0],
            recent_dead_letters: [
              {
                ...degradedStatus.event_consumers[0].recent_dead_letters[0],
                attempts: 10,
                reason: 'latest polled error',
              },
            ],
          },
        ],
      },
      isLoading: false,
    } as unknown as ReturnType<typeof useOperationsStatusQuery>)
    view.rerender(<OperationsPage />)
    expect(screen.getByText(/10 attempts/)).toBeTruthy()
    expect(screen.getByText('latest polled error')).toBeTruthy()
    expect(screen.queryByText('new error')).toBeNull()
  })
  it('hides Replay on item quarantines while keeping Dismiss', () => {
    vi.mocked(useOperationsStatusQuery).mockReturnValue({
      data: {
        ...degradedStatus,
        event_consumers: [
          {
            ...degradedStatus.event_consumers[0],
            recent_dead_letters: [
              {
                ...degradedStatus.event_consumers[0].recent_dead_letters[0],
                item_key: 'event:9:commitment:c-1',
                replayable: false,
              },
            ],
          },
        ],
      },
      isLoading: false,
    } as unknown as ReturnType<typeof useOperationsStatusQuery>)
    render(<OperationsPage />)
    expect(screen.queryByRole('button', { name: 'Replay' })).toBeNull()
    expect(screen.getByRole('button', { name: 'Dismiss' })).toBeTruthy()
  })
  it('shows event age and later-event context next to Replay', () => {
    render(<OperationsPage />)
    expect(screen.getByText(/Event .* old · 4 later events processed/)).toBeTruthy()
  })
  it('retains the action result when status refresh removes the resolved row', async () => {
    vi.mocked(useDeadLetterActionMutation).mockReturnValue({
      mutateAsync: vi.fn().mockResolvedValue(actionResult('replayed')),
    } as unknown as ReturnType<typeof useDeadLetterActionMutation>)
    const view = render(<OperationsPage />)
    fireEvent.click(screen.getByRole('button', { name: 'Replay' }))
    await screen.findByText('Replayed successfully.')
    vi.mocked(useOperationsStatusQuery).mockReturnValue({
      data: {
        ...degradedStatus,
        event_consumers: [
          { ...degradedStatus.event_consumers[0], dead_letter_count: 0, recent_dead_letters: [] },
        ],
      },
      isLoading: false,
    } as unknown as ReturnType<typeof useOperationsStatusQuery>)
    view.rerender(<OperationsPage />)
    expect(screen.getByText('Dead letters 0')).toBeTruthy()
    expect(screen.getByText('Replayed successfully.')).toBeTruthy()
    expect(screen.queryByRole('button', { name: 'Replay' })).toBeNull()
  })
  it('shows the resolved receipt when a poll removed the row during after-commit work', async () => {
    let complete: (value: ReturnType<typeof actionResult>) => void = () => {}
    const promise = new Promise<ReturnType<typeof actionResult>>((resolve) => {
      complete = resolve
    })
    vi.mocked(useDeadLetterActionMutation).mockReturnValue({
      mutateAsync: vi.fn().mockReturnValue(promise),
    } as unknown as ReturnType<typeof useDeadLetterActionMutation>)
    const view = render(<OperationsPage />)
    fireEvent.click(screen.getByRole('button', { name: 'Replay' }))
    vi.mocked(useOperationsStatusQuery).mockReturnValue({
      data: {
        ...degradedStatus,
        event_consumers: [
          { ...degradedStatus.event_consumers[0], dead_letter_count: 0, recent_dead_letters: [] },
        ],
      },
      isLoading: false,
    } as unknown as ReturnType<typeof useOperationsStatusQuery>)
    view.rerender(<OperationsPage />)
    complete(actionResult('replayed'))
    await screen.findByText('Replayed successfully.')
    expect(screen.queryByRole('button', { name: 'Replay' })).toBeNull()
    expect(screen.getByText('Dead letters 0')).toBeTruthy()
  })
  it('disables both actions during replay and handles conflict inline', async () => {
    let reject: (reason: unknown) => void = () => {}
    const promise = new Promise((_, fail) => {
      reject = fail
    })
    vi.mocked(useDeadLetterActionMutation).mockReturnValue({
      mutateAsync: vi.fn().mockReturnValue(promise),
    } as unknown as ReturnType<typeof useDeadLetterActionMutation>)
    render(<OperationsPage />)
    fireEvent.click(screen.getByRole('button', { name: 'Replay' }))
    expect(screen.getByRole('button', { name: 'Replaying…' }).hasAttribute('disabled')).toBe(true)
    expect(screen.getByRole('button', { name: 'Dismiss' }).hasAttribute('disabled')).toBe(true)
    reject(new ApiError(JSON.stringify({ code: 'version_conflict' }), 409))
    await waitFor(() =>
      expect(screen.getByRole('alert').textContent).toContain('already resolved or changed'),
    )
  })
  it('shows request failures inline and allows another action', async () => {
    vi.mocked(useDeadLetterActionMutation).mockReturnValue({
      mutateAsync: vi.fn().mockRejectedValue(new Error('network failed')),
    } as unknown as ReturnType<typeof useDeadLetterActionMutation>)
    render(<OperationsPage />)
    fireEvent.click(screen.getByRole('button', { name: 'Dismiss' }))
    await waitFor(() => expect(screen.getByRole('alert').textContent).toBe('network failed'))
    expect(screen.getByRole('button', { name: 'Replay' }).hasAttribute('disabled')).toBe(false)
  })
  it('lists server-host occupancy and links it to live settings', () => {
    render(<OperationsPage />)
    expect(screen.getByRole('link', { name: 'Server host' }).getAttribute('href')).toBe('/settings')
    expect(screen.getByRole('link', { name: 'Server host' }).parentElement?.textContent).toContain(
      '2/4 active runs',
    )
  })
  it('renders summary counters with correct counts', () => {
    render(<OperationsPage />)

    expect(
      within(screen.getByText('Active').parentElement as HTMLElement).getByText('1'),
    ).toBeTruthy()
    expect(
      within(screen.getByText('Blocked').parentElement as HTMLElement).getByText('1'),
    ).toBeTruthy()
    expect(
      within(screen.getByText('Runtimes').parentElement as HTMLElement).getByText('1'),
    ).toBeTruthy()
    expect(
      within(screen.getByText('Cleanup').parentElement as HTMLElement).getByText('1'),
    ).toBeTruthy()
    expect(
      within(screen.getByText('Retries').parentElement as HTMLElement).getByText('0'),
    ).toBeTruthy()
    expect(
      within(screen.getByText('Errors').parentElement as HTMLElement).getByText('1'),
    ).toBeTruthy()
  })

  it('renders active execution rows with task links', () => {
    render(<OperationsPage />)

    expect(screen.getByRole('link', { name: 'exec-active-1' }).getAttribute('href')).toBe(
      '/executions/exec-active-1',
    )
    expect(screen.getByRole('link', { name: 'task-active-1' }).getAttribute('href')).toBe(
      '/tasks/task-active-1',
    )
    expect(screen.getByText('2/4 completed')).toBeTruthy()
  })

  it('renders blocked task rows', () => {
    render(<OperationsPage />)

    expect(screen.getByRole('link', { name: 'Blocked migration task' }).getAttribute('href')).toBe(
      '/tasks/task-blocked-1',
    )
    expect(screen.getByText('Waiting for reviewer handoff')).toBeTruthy()
  })

  it('renders daemon issues', () => {
    render(<OperationsPage />)

    expect(screen.getAllByRole('link', { name: 'worker-01' })[0].getAttribute('href')).toBe(
      '/daemons/daemon-1',
    )
    expect(screen.getByText('Heartbeat stale for 90 seconds')).toBeTruthy()
  })

  it('renders pressure and active execution observability fields', () => {
    render(<OperationsPage />)

    expect(screen.getByText('Machine Pressure')).toBeTruthy()
    expect(screen.getByText('Agent Pressure')).toBeTruthy()
    expect(screen.getByText('3 turns')).toBeTruthy()
    expect(screen.getByText('Agent Agent One')).toBeTruthy()
    expect(screen.getByText('Rate requests_remaining: 10')).toBeTruthy()
  })

  it('high-risk policy badge is visible', () => {
    render(<OperationsPage />)

    expect(screen.getByText('High Risk')).toBeTruthy()
  })

  it('renders consumer lag, stalled status and database reclamation diagnostics', () => {
    render(<OperationsPage />)
    expect(screen.getAllByText(/attention_projection/).length).toBeGreaterThan(0)
    expect(screen.getByText('Pending events 5')).toBeTruthy()
    expect(screen.getByText('Stalled')).toBeTruthy()
    expect(screen.getByText('Conversion required')).toBeTruthy()
    expect(screen.getByText('42')).toBeTruthy()
    expect(screen.queryByText('All systems healthy')).toBeNull()
  })

  it('shows effective server build and priority facts with the run cap', () => {
    render(<OperationsPage />)
    expect(screen.getByText(/8 cores · 2 jobs\/run · nice \+10/)).toBeTruthy()
  })

  it('renders recent error rows as task drill-down links', () => {
    render(<OperationsPage />)

    expect(screen.getByRole('link', { name: 'task:task-active-1' }).getAttribute('href')).toBe(
      '/tasks/task-active-1',
    )
    expect(screen.getByText('Policy escalation failed')).toBeTruthy()
  })
  it('shows relay state and lasting worker quarantine history', () => {
    render(<OperationsPage />)
    expect(screen.getByText(/Event relay Running/).textContent).toContain(
      'Position 12 · Head 15 · tail retry',
    )
    expect(screen.getByText('Dead letters 7')).toBeTruthy()
    expect(screen.getByText(/9 · 8 attempts/)).toBeTruthy()
    expect(screen.getByText('blocked commitment')).toBeTruthy()
  })
})

it('shows usage index charge and budget and the fallback read path', () => {
  vi.mocked(useOperationsStatusQuery).mockReturnValue({
    data: degradedStatus,
    isLoading: false,
  } as ReturnType<typeof useOperationsStatusQuery>)
  const view = render(<OperationsPage />)
  expect(screen.getByText('Usage index 10.0 MiB / 128 MiB budget · Incremental reads')).toBeTruthy()
  vi.mocked(useOperationsStatusQuery).mockReturnValue({
    data: {
      ...degradedStatus,
      usage_index: { current_size_bytes: 0, budget_bytes: 0, fallback: true },
    },
    isLoading: false,
  } as ReturnType<typeof useOperationsStatusQuery>)
  view.rerender(<OperationsPage />)
  expect(screen.getByText('Usage index 0.0 MiB / 0 MiB budget · Memoized full reads')).toBeTruthy()
})
