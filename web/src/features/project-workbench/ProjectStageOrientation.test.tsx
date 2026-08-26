import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import type { ExecutionBlockerProjection } from '@/types/generated/bindings/ExecutionBlockerProjection'
import type { ProjectExecutionSetupResponse } from '@/types/generated/bindings/ProjectExecutionSetupResponse'
import type { ProjectOverview } from '@/types/generated/bindings/ProjectOverview'

import { deriveProjectStageOrientation, ProjectStageOrientation } from './ProjectStageOrientation'

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
    const path = params
      ? Object.entries(params).reduce((path, [key, value]) => path.replace(`$${key}`, value), to)
      : to
    return <a href={path}>{children}</a>
  },
}))

function executionSetup(overrides: Partial<ProjectExecutionSetupResponse> = {}): ProjectExecutionSetupResponse {
  return {
    project_id: 'project-1',
    project_version: BigInt(1),
    coordination_state: 'ready',
    execution_setup_state: 'ready',
    execution_gate: 'active',
    availability: {
      coordination: { availability: 'current', retry: null, error_code: null },
      execution_setup: { availability: 'current', retry: null, error_code: null },
      execution_gate: { availability: 'current', retry: null, error_code: null },
    },
    primary_repo: null,
    worker: null,
    independent_reviewer: null,
    eligible_workers: [],
    eligible_reviewers: [],
    setup_requirements: [],
    next_action: null,
    provisioning: null,
    execution_blocker: null,
    ...overrides,
  }
}

function overview(overrides: Partial<ProjectOverview> = {}): ProjectOverview {
  return {
    project_id: 'project-1',
    project_name: 'Test Project',
    vision: 'A bounded Project.',
    charter_state: 'approved',
    current_charter: {
      id: 'charter-1',
      project_id: 'project-1',
      revision_number: BigInt(1),
      content_digest: 'digest-1',
      content: {},
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any,
    primary_milestone_id: null,
    active_milestones: [],
    task_counts: { total: BigInt(0), backlog: BigInt(0), active: BigInt(0), review: BigInt(0), terminal: BigInt(0), blocked: BigInt(0) },
    check_summary: {
      required_total: BigInt(0),
      passed: BigInt(0),
      failed: BigInt(0),
      missing: BigInt(0),
      stale: BigInt(0),
      waived: BigInt(0),
      unavailable: BigInt(0),
    },
    pending_decisions: [],
    decisions: [],
    risks: [],
    document_freshness: [],
    evidence: [],
    releases: [],
    next_action: null,
    projection_state: 'current',
    source_event_watermark: 'watermark-1',
    generated_at: '2026-08-25T00:00:00Z',
    execution_setup: executionSetup(),
    ...overrides,
  }
}

function blocker(overrides: Partial<ExecutionBlockerProjection> = {}): ExecutionBlockerProjection {
  return {
    code: 'reconciliation_required',
    stage: 'build',
    scope: 'project',
    affected_refs: [],
    governing_ref: null,
    headline: 'Reconciliation required',
    safe_explanation: 'A synthetic conflict needs your review before Worker execution can continue.',
    evidence: null,
    required_principal: 'user',
    next_action: 'resolve_reconciliation',
    blocker_digest: 'digest-1',
    observed_version: BigInt(1),
    ...overrides,
  }
}

describe('deriveProjectStageOrientation', () => {
  it('marks Define pending and every later stage not started before Charter approval', () => {
    const stages = deriveProjectStageOrientation(
      overview({ charter_state: 'charter_setup_required', execution_setup: null }),
    )
    expect(stages.find((stage) => stage.key === 'define')?.status).toBe('active')
    expect(stages.find((stage) => stage.key === 'plan')?.status).toBe('pending')
    expect(stages.find((stage) => stage.key === 'build')?.status).toBe('pending')
    expect(stages.find((stage) => stage.key === 'release')?.status).toBe('pending')
  })

  it('marks Plan active while the baseline awaits approval', () => {
    const stages = deriveProjectStageOrientation(
      overview({ execution_setup: executionSetup({ execution_gate: 'baseline_approval_required' }) }),
    )
    expect(stages.find((stage) => stage.key === 'define')?.status).toBe('complete')
    expect(stages.find((stage) => stage.key === 'plan')?.status).toBe('active')
    expect(stages.find((stage) => stage.key === 'build')?.status).toBe('pending')
  })

  it('marks Build active with in-flight Task counts once the baseline is active', () => {
    const stages = deriveProjectStageOrientation(
      overview({
        task_counts: { total: BigInt(4), backlog: BigInt(0), active: BigInt(2), review: BigInt(1), terminal: BigInt(1), blocked: BigInt(0) },
      }),
    )
    const build = stages.find((stage) => stage.key === 'build')
    expect(build?.status).toBe('active')
    expect(build?.detail).toContain('2 active')
    expect(build?.detail).toContain('1 in review')
    expect(build?.detail).toContain('1/4 done')
  })

  it('marks Build and Release complete once every Task is terminal, checks pass, and a release exists', () => {
    const stages = deriveProjectStageOrientation(
      overview({
        task_counts: { total: BigInt(2), backlog: BigInt(0), active: BigInt(0), review: BigInt(0), terminal: BigInt(2), blocked: BigInt(0) },
        releases: [
          {
            id: 'release-1',
            project_id: 'project-1',
            milestone_id: 'milestone-1',
            release_sequence: BigInt(1),
            release_identity: 'release-2026.08.25-1',
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            snapshot: {} as any,
            version: BigInt(1),
            created_at: '2026-08-25T00:00:00Z',
          },
        ],
      }),
    )
    expect(stages.find((stage) => stage.key === 'build')?.status).toBe('complete')
    const release = stages.find((stage) => stage.key === 'release')
    expect(release?.status).toBe('complete')
    expect(release?.detail).toContain('release-2026.08.25-1')
  })

  it('reuses the canonical ExecutionBlockerProjection stage and never invents its own vocabulary', () => {
    const projectBlocker = blocker({ stage: 'plan', headline: 'Approve the plan to start work' })
    const stages = deriveProjectStageOrientation(
      overview({
        execution_setup: executionSetup({
          execution_gate: 'baseline_approval_required',
          execution_blocker: projectBlocker,
        }),
      }),
    )
    const plan = stages.find((stage) => stage.key === 'plan')
    expect(plan?.status).toBe('blocked')
    expect(plan?.blocker).toBe(projectBlocker)
    // No other stage claims the blocker.
    expect(stages.filter((stage) => stage.blocker !== null)).toHaveLength(1)
  })

  it('folds a review-stage blocker into the Build stage rather than adding a fifth stop', () => {
    const stages = deriveProjectStageOrientation(
      overview({
        execution_setup: executionSetup({ execution_blocker: blocker({ stage: 'review' }) }),
      }),
    )
    expect(stages).toHaveLength(4)
    expect(stages.find((stage) => stage.key === 'build')?.status).toBe('blocked')
  })
})

describe('ProjectStageOrientation', () => {
  it('renders all four stage labels in order with accessible status text', () => {
    render(<ProjectStageOrientation overview={overview()} />)
    const nav = screen.getByRole('navigation', { name: 'Project stage' })
    expect(nav).toBeTruthy()
    ;['Define', 'Plan', 'Build', 'Release'].forEach((label) => {
      expect(screen.getByText(label)).toBeTruthy()
    })
  })

  it('shows the scoped blocker headline and a resolve link at the affected stage only', () => {
    render(
      <ProjectStageOrientation
        overview={overview({
          execution_setup: executionSetup({
            execution_gate: 'reconciliation_required',
            execution_blocker: blocker({ stage: 'build', headline: 'Reconciliation required' }),
          }),
        })}
      />,
    )
    expect(screen.getByText(/Reconciliation required/)).toBeTruthy()
    expect(screen.getByRole('link', { name: 'Review and resolve' })).toBeTruthy()
  })

  it('shows no blocker panel when the Project has no outstanding blocker', () => {
    render(<ProjectStageOrientation overview={overview()} />)
    expect(screen.queryByRole('link', { name: 'Review and resolve' })).toBeNull()
  })
})
