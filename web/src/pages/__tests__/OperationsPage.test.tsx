import { render, screen, within } from '@testing-library/react'
import type { ReactNode } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { useOperationsStatusQuery, useRefreshOperationsMutation } from '@/api/hooks'
import { OperationsPage } from '@/pages/OperationsPage'
import { emptyUsage } from '@/test-utils/usage'
import type { CostSummary, OperatorStatusResponse, UsageAggregate } from '@/types/generated'

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
          item_key: 'event:9:commitment:c-1',
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
    expect(screen.getByText('attention_projection')).toBeTruthy()
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
    expect(screen.getByText(/event:9:commitment:c-1: blocked commitment/)).toBeTruthy()
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
