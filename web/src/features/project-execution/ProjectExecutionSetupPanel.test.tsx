import { render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { useReposQuery } from '@/api/hooks'
import { ApiError } from '@/api/client'
import type { ProjectExecutionSetupResponse } from '@/types/generated'

import { ProjectExecutionSetupPanel } from './ProjectExecutionSetupPanel'
import {
  useAttachPrimaryRepositoryMutation,
  useProjectExecutionSetupQuery,
  useRetryProvisioningMutation,
  useSelectIndependentReviewerMutation,
  useSelectWorkerMutation,
} from './hooks'

vi.mock('@tanstack/react-router', () => ({
  Link: ({
    to,
    params,
    children,
  }: {
    to: string
    params?: Record<string, string>
    children: React.ReactNode
  }) => {
    const href = params
      ? Object.entries(params).reduce((path, [key, value]) => path.replace(`$${key}`, value), to)
      : to
    return <a href={href}>{children}</a>
  },
}))

vi.mock('@/api/hooks', () => ({
  useReposQuery: vi.fn(() => ({
    data: { items: [] },
    isLoading: false,
    isError: false,
    refetch: vi.fn(),
  })),
}))

vi.mock('./hooks', () => ({
  useProjectExecutionSetupQuery: vi.fn(),
  useSelectWorkerMutation: vi.fn(),
  useSelectIndependentReviewerMutation: vi.fn(),
  useAttachPrimaryRepositoryMutation: vi.fn(),
  useRetryProvisioningMutation: vi.fn(),
}))

const worker = {
  identity_id: 'worker-1',
  name: 'Build Worker',
  profile_id: 'profile-worker',
  executor_type: 'native',
  provider: null,
  model: null,
  status: 'active',
  paused: false,
  version: 1n,
}

const reviewer = {
  identity_id: 'reviewer-1',
  name: 'Independent Reviewer',
  profile_id: 'profile-reviewer',
  executor_type: 'native',
  provider: null,
  model: null,
  status: 'active',
  paused: false,
  version: 1n,
}

const baseSetup = {
  project_id: 'project-1',
  project_version: 7n,
  coordination_state: 'ready' as const,
  execution_setup_state: 'setup_required' as const,
  execution_gate: 'pre_baseline_read_only' as const,
  primary_repo: null,
  worker: null,
  independent_reviewer: null,
  eligible_workers: [],
  eligible_reviewers: [],
  setup_requirements: [],
  next_action: null,
  provisioning: null,
} as unknown as ProjectExecutionSetupResponse

function mutationState() {
  return { mutate: vi.fn(), isPending: false, error: null }
}

function mockPanel(data: ProjectExecutionSetupResponse) {
  vi.mocked(useProjectExecutionSetupQuery).mockReturnValue({
    data,
    isLoading: false,
    isError: false,
    error: null,
    refetch: vi.fn(),
  } as unknown as ReturnType<typeof useProjectExecutionSetupQuery>)
  vi.mocked(useSelectWorkerMutation).mockReturnValue(
    mutationState() as unknown as ReturnType<typeof useSelectWorkerMutation>,
  )
  vi.mocked(useSelectIndependentReviewerMutation).mockReturnValue(
    mutationState() as unknown as ReturnType<typeof useSelectIndependentReviewerMutation>,
  )
  vi.mocked(useAttachPrimaryRepositoryMutation).mockReturnValue(
    mutationState() as unknown as ReturnType<typeof useAttachPrimaryRepositoryMutation>,
  )
  vi.mocked(useRetryProvisioningMutation).mockReturnValue(
    mutationState() as unknown as ReturnType<typeof useRetryProvisioningMutation>,
  )
}

describe('ProjectExecutionSetupPanel', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    mockPanel(baseSetup)
  })

  it('offers exactly one create-Worker action when no eligible Worker exists', () => {
    mockPanel({
      ...baseSetup,
      setup_requirements: [
        {
          requirement_type: 'role_assignment',
          resource_type: null,
          resource_id: null,
          role: 'worker',
          capability: 'repository_write',
          action: 'select_worker',
        },
      ],
      next_action: 'select_worker',
    } as unknown as ProjectExecutionSetupResponse)

    render(<ProjectExecutionSetupPanel projectId="project-1" />)

    expect(screen.getByRole('link', { name: /Create Worker/ })).toBeTruthy()
    expect(screen.queryAllByRole('button')).toHaveLength(0)
    expect(screen.getByText(/Select or create a Worker/)).toBeTruthy()
  })

  it('keeps Worker and independent reviewer choices distinct', () => {
    mockPanel({
      ...baseSetup,
      execution_setup_state: 'setup_required',
      worker,
      eligible_workers: [worker],
      eligible_reviewers: [worker, reviewer],
      setup_requirements: [
        {
          requirement_type: 'role_assignment',
          resource_type: null,
          resource_id: null,
          role: 'independent_reviewer',
          capability: 'repository_read',
          action: 'select_independent_reviewer',
        },
      ],
      next_action: 'select_independent_reviewer',
    } as unknown as ProjectExecutionSetupResponse)

    render(<ProjectExecutionSetupPanel projectId="project-1" />)

    const reviewerSelect = screen.getByRole('combobox', { name: 'Independent reviewer' })
    expect(reviewerSelect.querySelector('option[value="reviewer-1"]')).toBeTruthy()
    expect(reviewerSelect.querySelector('option[value="worker-1"]')).toBeNull()
    expect(screen.getByRole('button', { name: /Select reviewer/ }).className).toContain('w-full')
  })

  it('describes provisioning as in progress without claiming execution success', () => {
    mockPanel({
      ...baseSetup,
      execution_setup_state: 'provisioning',
      next_action: 'retry_provisioning',
      provisioning: {
        id: 'provisioning-1',
        status: 'provisioning',
        current_checkpoint: 'workspace',
        attempt_count: 1n,
        max_attempts: 3n,
        lease_owner: null,
        lease_expires_at: null,
        next_retry_at: null,
        retryable: true,
        last_error_code: null,
        last_error_message: null,
        version: 2n,
      },
    } as unknown as ProjectExecutionSetupResponse)

    render(<ProjectExecutionSetupPanel projectId="project-1" />)

    expect(screen.getByText('Provisioning in progress')).toBeTruthy()
    expect(screen.getByText(/not executable until the server reports ready/)).toBeTruthy()
    expect(screen.getByRole('button', { name: /Refresh provisioning status/ })).toBeTruthy()
    expect(screen.getByText(/No operational success is claimed/)).toBeTruthy()
  })

  it('shows a retry for a terminal provisioning action even when history is not retryable', () => {
    mockPanel({
      ...baseSetup,
      execution_setup_state: 'failed',
      next_action: 'retry_provisioning',
      provisioning: {
        id: 'provisioning-1',
        status: 'failed',
        current_checkpoint: 'workspace',
        attempt_count: 2n,
        max_attempts: 3n,
        lease_owner: null,
        lease_expires_at: null,
        next_retry_at: null,
        retryable: false,
        last_error_code: 'workspace_unavailable',
        last_error_message: 'Workspace is temporarily unavailable.',
        version: 4n,
      },
    } as unknown as ProjectExecutionSetupResponse)

    render(<ProjectExecutionSetupPanel projectId="project-1" />)

    expect(screen.getByRole('alert').textContent).toContain('Workspace is temporarily unavailable.')
    expect(screen.getByRole('button', { name: /Retry provisioning/ })).toBeTruthy()
    expect(screen.getAllByRole('button')).toHaveLength(1)
    expect(screen.queryByRole('link', { name: /Continue planning/ })).toBeNull()
  })

  it('keeps a retrying operation visibly pending until the server confirms a result', () => {
    mockPanel({
      ...baseSetup,
      execution_setup_state: 'failed',
      next_action: 'retry_provisioning',
      provisioning: {
        id: 'provisioning-1',
        status: 'failed',
        current_checkpoint: 'workspace',
        attempt_count: 1n,
        max_attempts: 3n,
        lease_owner: null,
        lease_expires_at: null,
        next_retry_at: null,
        retryable: true,
        last_error_code: 'workspace_unavailable',
        last_error_message: 'Workspace is temporarily unavailable.',
        version: 2n,
      },
    } as unknown as ProjectExecutionSetupResponse)
    vi.mocked(useRetryProvisioningMutation).mockReturnValue({
      ...mutationState(),
      isPending: true,
    } as unknown as ReturnType<typeof useRetryProvisioningMutation>)

    render(<ProjectExecutionSetupPanel projectId="project-1" />)

    expect(
      screen
        .getAllByRole('status')
        .some((node) => node.textContent?.includes('Retrying provisioning')),
    ).toBe(true)
    expect(screen.getByRole('button', { name: /Retrying/ }).hasAttribute('disabled')).toBe(true)
  })

  it('shows operation identity and next retry for a server-scheduled retry', () => {
    mockPanel({
      ...baseSetup,
      execution_setup_state: 'provisioning',
      next_action: 'refresh_and_retry',
      provisioning: {
        id: 'provisioning-retry-1',
        status: 'retry_wait',
        current_checkpoint: 'workspace',
        attempt_count: 2n,
        max_attempts: 3n,
        lease_owner: 'setup-worker',
        lease_expires_at: '2026-08-21T12:02:00Z',
        next_retry_at: '2026-08-21T12:03:00Z',
        retryable: true,
        last_error_code: 'workspace_unavailable',
        last_error_message: 'Workspace is temporarily unavailable.',
        version: 4n,
      },
    } as unknown as ProjectExecutionSetupResponse)

    render(<ProjectExecutionSetupPanel projectId="project-1" />)

    expect(screen.getByText('Retrying provisioning')).toBeTruthy()
    expect(screen.getByText('provisioning-retry-1')).toBeTruthy()
    expect(screen.getByText('Retry Wait')).toBeTruthy()
    expect(screen.getByText(/Next retry/)).toBeTruthy()
    expect(screen.getByText(/not executable until the server reports ready/)).toBeTruthy()
  })

  it('does not retry a stale setup mutation and exposes authorized conflict details', () => {
    mockPanel({
      ...baseSetup,
      eligible_workers: [worker],
      setup_requirements: [
        {
          requirement_type: 'role_assignment',
          resource_type: null,
          resource_id: null,
          role: 'worker',
          capability: 'repository_write',
          action: 'select_worker',
        },
      ],
      next_action: 'select_worker',
    } as unknown as ProjectExecutionSetupResponse)
    vi.mocked(useSelectWorkerMutation).mockReturnValue({
      ...mutationState(),
      error: new ApiError(
        JSON.stringify({
          code: 'version_conflict',
          message: 'Project setup changed.',
          details: {
            authority_domain: 'Project execution setup',
            expected_version: 7,
            current_version: 8,
          },
        }),
        409,
      ),
    } as unknown as ReturnType<typeof useSelectWorkerMutation>)

    render(<ProjectExecutionSetupPanel projectId="project-1" />)

    expect(screen.getByRole('button', { name: /Refresh readiness/ })).toBeTruthy()
    const alert = screen.getByRole('alert')
    expect(alert.textContent).toContain('current Project authority changed')
    expect(alert.textContent).toContain('Authority: Project execution setup')
    expect(alert.textContent).toContain('Expected revision')
    expect(alert.textContent).toContain('Current revision')
    expect(alert.textContent).toContain('7')
    expect(alert.textContent).toContain('8')
    expect(screen.queryByRole('button', { name: /Select Worker/ })).toBeNull()
  })

  it('offers an explicit repository attachment when repositories exist', () => {
    mockPanel({
      ...baseSetup,
      setup_requirements: [
        {
          requirement_type: 'repository',
          resource_type: null,
          resource_id: null,
          role: null,
          capability: 'repository_write',
          action: 'attach_repository',
        },
      ],
      next_action: 'attach_repository',
    } as unknown as ProjectExecutionSetupResponse)
    vi.mocked(useReposQuery).mockReturnValue({
      data: {
        items: [{ id: 'repo-1', name: 'Forge', default_branch: 'main' }],
      },
      isLoading: false,
      isError: false,
      refetch: vi.fn(),
    } as unknown as ReturnType<typeof useReposQuery>)

    render(<ProjectExecutionSetupPanel projectId="project-1" />)

    expect(screen.getByRole('combobox', { name: 'Primary repository' })).toBeTruthy()
    expect(screen.getByRole('button', { name: /Attach repository/ })).toBeTruthy()
  })

  it('keeps a ready setup visibly read-only before baseline activation', () => {
    mockPanel({
      ...baseSetup,
      execution_setup_state: 'ready',
      execution_gate: 'pre_baseline_read_only',
      worker,
      independent_reviewer: reviewer,
      primary_repo: { id: 'repo-1', name: 'forge', default_branch: 'main' },
      next_action: null,
    } as unknown as ProjectExecutionSetupResponse)

    render(<ProjectExecutionSetupPanel projectId="project-1" />)

    expect(screen.getByText(/Planning remains available, but execution is read-only/)).toBeTruthy()
    expect(screen.getByText('Pre Baseline Read Only')).toBeTruthy()
    expect(screen.getByRole('link', { name: /Plan execution baseline/ })).toBeTruthy()
    const region = screen.getByRole('region', { name: 'Execution readiness' })
    expect(region.className).toContain('min-w-0')
    expect(screen.getByRole('status').getAttribute('aria-live')).toBe('polite')
  })

  it('uses refresh_and_retry for unavailable projections instead of planning or setup links', () => {
    mockPanel({
      ...baseSetup,
      execution_setup_state: 'setup_required',
      availability: {
        coordination: {
          availability: 'current',
          retry: null,
          error_code: null,
        },
        execution_setup: {
          availability: 'unavailable',
          retry: 'refresh_and_retry',
          error_code: 'projection_source_unavailable',
        },
        execution_gate: {
          availability: 'current',
          retry: null,
          error_code: null,
        },
      },
      next_action: 'select_worker',
    } as unknown as ProjectExecutionSetupResponse)

    render(<ProjectExecutionSetupPanel projectId="project-1" />)

    expect(screen.getByRole('button', { name: /Refresh readiness/ })).toBeTruthy()
    expect(screen.getAllByRole('button')).toHaveLength(1)
    expect(screen.queryByRole('link', { name: /Restore Project Agent binding/ })).toBeNull()
    expect(screen.queryByRole('link', { name: /Continue planning/ })).toBeNull()
  })
})
