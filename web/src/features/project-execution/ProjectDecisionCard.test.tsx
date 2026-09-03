import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'

import type { ProjectOverview } from '@/types/generated'

import { ProjectDecisionCard } from './ProjectDecisionCard'

const mocks = vi.hoisted(() => ({
  overview: null as ProjectOverview | null,
  mutateAsync: vi.fn<(input: unknown) => Promise<void>>(async () => undefined),
  isPending: false,
  isSuccess: false,
}))

vi.mock('@/api/hooks', () => ({
  useProjectOverviewQuery: () => ({ data: mocks.overview }),
  useApproveMilestoneRevision: () => ({
    mutateAsync: mocks.mutateAsync,
    isPending: mocks.isPending,
    isSuccess: mocks.isSuccess,
    reset: vi.fn(),
  }),
}))

vi.mock('@/stores/auth', () => ({
  useAuthStore: {
    getState: () => ({ user: { id: 'user-1', display_name: 'Mai' } }),
  },
}))

function overviewWith(partial: Partial<ProjectOverview>): ProjectOverview {
  return {
    project_id: 'project-1',
    project_name: 'SproutCue',
    vision: 'Water the plants',
    charter_state: 'charter_backed',
    current_charter: null,
    primary_milestone_id: 'milestone-1',
    active_milestones: [],
    task_counts: { total: 0n, queued: 0n, in_progress: 0n, blocked: 0n, done: 0n, failed: 0n },
    check_summary: { total: 0n, passed: 0n, failed: 0n, missing: 0n, waived: 0n },
    pending_decisions: [],
    decisions: [],
    risks: [],
    document_freshness: [],
    evidence: [],
    releases: [],
    next_action: null,
    projection_state: 'current',
    source_event_watermark: '1',
    generated_at: '2026-09-02T12:00:00Z',
    ...partial,
  } as ProjectOverview
}

const milestone = {
  milestone: {
    id: 'milestone-1',
    project_id: 'project-1',
    milestone_sequence: 1n,
    canonical_id: 'M001',
    display_label: 'M001',
    definition_revision_id: 'revision-2',
    lifecycle: 'active',
    projection_reasons: [],
    version: 4n,
    created_at: '2026-09-02T12:00:00Z',
    updated_at: '2026-09-02T12:00:00Z',
  },
  definition: {
    id: 'revision-2',
    milestone_id: 'milestone-1',
    project_id: 'project-1',
    revision_number: 2n,
    base_revision_id: 'revision-1',
    lifecycle: 'proposed',
    schema_version: '1',
    content: {
      name: 'MVP',
      outcome: 'Plants get watered',
      included_scope: [],
      excluded_scope: [],
      charter_revision: null,
      document_revisions: [],
      task_ids: [],
      dependencies: [],
      risks: [],
      acceptance_checks: [
        { id: 'a', description: 'a', required: true, source_kind: 'task_validation', expected_result: 'ok', latest_result: null, latest_result_id: null, latest_result_digest: null },
        { id: 'b', description: 'b', required: true, source_kind: 'task_validation', expected_result: 'ok', latest_result: null, latest_result_id: null, latest_result_digest: null },
        { id: 'c', description: 'c', required: true, source_kind: 'manual', expected_result: 'ok', latest_result: null, latest_result_id: null, latest_result_digest: null },
      ],
      evidence_requirements: [],
      known_issues: [],
      target_date: null,
    },
    rendered_view: '# MVP',
    render_version: '1',
    content_digest: 'digest',
    render_digest: 'digest',
    provenance: { author: { kind: 'agent', id: 'agent-1', display_name: 'Sol' }, change_summary: null, source_refs: [] },
    created_at: '2026-09-02T12:00:00Z',
  },
  task_counts: { total: 0n, queued: 0n, in_progress: 0n, blocked: 0n, done: 0n, failed: 0n },
  check_summary: { total: 3n, passed: 0n, failed: 0n, missing: 3n, waived: 0n },
  current_checks: [],
  latest_readiness: null,
  readiness_freshness: null,
  evidence: [],
} as unknown as ProjectOverview['active_milestones'][number]

function renderCard() {
  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  return render(
    <QueryClientProvider client={queryClient}>
      <ProjectDecisionCard projectId="project-1" />
    </QueryClientProvider>,
  )
}

describe('ProjectDecisionCard', () => {
  afterEach(() => {
    vi.clearAllMocks()
    mocks.overview = null
  })

  it('renders nothing when the user has nothing to decide', () => {
    mocks.overview = overviewWith({})
    const { container } = renderCard()
    expect(container.innerHTML).toBe('')
  })

  it('offers the milestone definition approval with a one-click approve', async () => {
    mocks.overview = overviewWith({
      active_milestones: [milestone],
      next_action: {
        code: 'milestone_definition_approval',
        required_principal: 'user',
        target_type: 'milestone_revision',
        target_id: 'revision-2',
        title: 'Approve the milestone definition revision',
        explanation: 'The current definition revision is not approved.',
        action_kind: 'approval',
        route_or_operation: 'project.milestone.revision.transition',
        blocking: true,
        expected_version: 4n,
      },
    })
    renderCard()

    expect(screen.getByRole('region', { name: 'Needs your decision' })).toBeTruthy()
    expect(screen.getByText(/3 acceptance checks · 2 task validation, 1 manual/)).toBeTruthy()

    fireEvent.click(screen.getByRole('button', { name: 'Approve definition revision 2' }))

    await waitFor(() => expect(mocks.mutateAsync).toHaveBeenCalledTimes(1))
    const input = mocks.mutateAsync.mock.calls[0]?.[0] as Record<string, unknown>
    expect(input).toMatchObject({
      projectId: 'project-1',
      milestoneId: 'milestone-1',
      revisionId: 'revision-2',
      expectedMilestoneVersion: 4,
    })
    expect((input.authorization as Record<string, unknown>).authorization_basis).toBe(
      'interactive_user_approval',
    )
  })

  it('ignores a next action that is not the user\'s to take', () => {
    mocks.overview = overviewWith({
      active_milestones: [milestone],
      next_action: {
        code: 'milestone_definition_approval',
        required_principal: 'project_agent',
        target_type: 'milestone_revision',
        target_id: 'revision-2',
        title: 'Approve the milestone definition revision',
        explanation: 'x',
        action_kind: 'approval',
        route_or_operation: 'project.milestone.revision.transition',
        blocking: true,
        expected_version: 4n,
      },
    })
    const { container } = renderCard()
    expect(container.innerHTML).toBe('')
  })
})
