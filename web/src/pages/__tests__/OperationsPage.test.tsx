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
  overall_severity: 'error',
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
      daemon_id: 'daemon-1',
      hostname: 'worker-01',
      active_sessions: 2,
      max_sessions: 4,
      at_capacity: false,
    },
  ],
  agent_pressure: [
    {
      agent_id: 'agent-1',
      agent_name: 'Agent One',
      daemon_id: 'daemon-1',
      active_sessions: 1,
      max_sessions: 2,
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

    expect(screen.getByText('Runtime Pressure')).toBeTruthy()
    expect(screen.getByText('Agent Pressure')).toBeTruthy()
    expect(screen.getByText('3 turns')).toBeTruthy()
    expect(screen.getByText('Agent Agent One')).toBeTruthy()
    expect(screen.getByText('Rate requests_remaining: 10')).toBeTruthy()
  })

  it('high-risk policy badge is visible', () => {
    render(<OperationsPage />)

    expect(screen.getByText('High Risk')).toBeTruthy()
  })

  it('renders recent error rows as task drill-down links', () => {
    render(<OperationsPage />)

    expect(screen.getByRole('link', { name: 'task:task-active-1' }).getAttribute('href')).toBe(
      '/tasks/task-active-1',
    )
    expect(screen.getByText('Policy escalation failed')).toBeTruthy()
  })
})
