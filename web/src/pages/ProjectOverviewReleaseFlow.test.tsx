import { fireEvent, render as rtlRender, screen, waitFor, within } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import {
  useProjectOverviewQuery,
  useRecordManualMilestoneCheck,
  useReleaseProjectMilestone,
} from '@/api/hooks'
import { ProjectOverviewPage } from '@/pages/ProjectOverviewPage'
import { useAuthStore } from '@/stores/auth'
import type { ProjectOverview } from '@/types/generated'

type LinkProps = {
  to: string
  params?: Record<string, string>
  search?: Record<string, unknown>
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
  useProjectOverviewQuery: vi.fn(),
  useRecordManualMilestoneCheck: vi.fn(),
  useProjectQuery: vi.fn(() => ({ data: undefined, isLoading: false })),
  useReleaseProjectMilestone: vi.fn(),
}))

vi.mock('@/api/client', () => ({
  apiFetchBlob: vi.fn(),
  ApiError: class MockApiError extends Error {
    status: number

    constructor(message: string, status: number) {
      super(message)
      this.status = status
    }
  },
  // The adoption banner uses this projection. Keep it empty so these tests
  // exercise the Overview release/document surfaces in isolation.
  apiFetch: vi.fn(async () => ({
    charter: null,
    revisions: [],
    current_draft_revision: null,
    current_approved_revision: null,
    approval: null,
    selected_project_agent: null,
  })),
}))

vi.mock('@/features/project-execution/ProjectExecutionSetupPanel', () => ({
  ProjectExecutionSetupPanel: () => null,
}))

const counts = {
  total: 1n,
  backlog: 0n,
  active: 0n,
  review: 0n,
  terminal: 1n,
  blocked: 0n,
}

const checks = {
  required_total: 1n,
  passed: 1n,
  failed: 0n,
  missing: 0n,
  stale: 0n,
  waived: 0n,
  unavailable: 0n,
}

const readinessCandidate = {
  id: 'readiness-1',
  project_id: 'project-1',
  milestone_id: 'milestone-1',
  expected_milestone_version: 7n,
  milestone_definition_revision_id: 'milestone-revision-1',
  baseline_id: 'baseline-1',
  baseline_revision_id: 'baseline-revision-1',
  baseline_digest: 'baseline-digest',
  release_policy_revision: 'release-policy-1',
  release_policy_digest: 'release-policy-digest',
  input_manifest: [],
  source_event_watermark: 'event-700',
  result: 'ready',
  reasons: [],
  check_results: [],
  waiver_ids: [],
  evidence_attachment_ids: [],
  evidence_digests: [],
  evidence_availability: [],
  commit_build_check_context: ['commit:abc123', 'build:build-7', 'check:check-1'],
  computing_policy_revision: 'policy-computation-1',
  readiness_digest: 'readiness-digest-1',
  computed_at: '2026-08-21T10:00:00Z',
  requesting_principal: { kind: 'user', id: 'user-1', display_name: 'Test User' },
  authorization: {
    principal: { kind: 'user', id: 'user-1', display_name: 'Test User' },
    authorization_basis: 'interactive_user_approval',
    action: 'milestone.readiness',
    event_id: 'authorization-event-1',
    occurred_at: '2026-08-21T10:00:00Z',
  },
}

const readyMilestone = {
  milestone: {
    id: 'milestone-1',
    project_id: 'project-1',
    milestone_sequence: 1n,
    canonical_id: 'M001',
    display_label: 'First release',
    definition_revision_id: 'milestone-revision-1',
    lifecycle: 'ready_for_release',
    projection_reasons: [],
    version: 8n,
    created_at: '2026-08-21T09:00:00Z',
    updated_at: '2026-08-21T10:00:00Z',
  },
  definition: {
    id: 'milestone-revision-1',
    milestone_id: 'milestone-1',
    project_id: 'project-1',
    revision_number: 1n,
    base_revision_id: null,
    lifecycle: 'approved',
    schema_version: 'v1',
    content: {
      name: 'First release',
      outcome: 'A bounded project outcome with exact release proof.',
      included_scope: ['Overview release flow'],
      excluded_scope: ['Unapproved automation'],
      charter_revision: null,
      document_revisions: [],
      task_ids: ['task-1'],
      dependencies: [],
      risks: [],
      acceptance_checks: [],
      evidence_requirements: [],
      known_issues: [],
      target_date: null,
    },
    rendered_view: 'First release',
    render_version: 'v1',
    content_digest: 'milestone-definition-digest',
    render_digest: 'milestone-render-digest',
    provenance: {
      author: { kind: 'user', id: 'user-1', display_name: 'Test User' },
      profile_revision: null,
      operating_skill_revision: null,
      source_refs: [],
      change_summary: 'Initial milestone',
      material_diff: null,
    },
    created_at: '2026-08-21T09:00:00Z',
  },
  task_counts: counts,
  check_summary: checks,
  latest_readiness: readinessCandidate,
  readiness_freshness: {
    status: 'current',
    reason: null,
    snapshot_source_event_watermark: 'event-700',
    current_source_event_watermark: 'event-700',
  },
  evidence: [],
}

const release = {
  id: 'release-1',
  project_id: 'project-1',
  milestone_id: 'milestone-1',
  release_sequence: 1,
  release_identity: 'M001-r1',
  snapshot: {
    schema_version: 'forge.release/v1',
    project_id: 'project-1',
    milestone_id: 'milestone-1',
    milestone_canonical_id: 'M001',
    release_revision: 1,
    release_identity: 'M001-r1',
    milestone_definition_revision_id: 'milestone-revision-1',
    milestone_definition_digest: 'milestone-definition-digest',
    expected_milestone_version: 7,
    display_label: 'First release',
    summary: 'Frozen release truth.',
    changelog: [],
    known_issues: [],
    readiness_snapshot_id: 'readiness-1',
    readiness_digest: 'readiness-digest-1',
    source_event_watermark: 'event-700',
    baseline_id: 'baseline-1',
    baseline_revision_id: 'baseline-revision-1',
    baseline_digest: 'baseline-digest',
    charter_revision: {
      artifact_id: 'charter-1',
      revision_id: 'charter-revision-1',
      content_digest: 'charter-digest',
      render_digest: null,
    },
    document_revisions: [],
    included_decisions: [],
    included_tasks: [],
    validation_results: [],
    repository_references: [],
    evidence_pins: [],
    waived_check_ids: [],
    release_policy_revision: 'release-policy-1',
    released_by: { kind: 'user', id: 'user-1', display_name: 'Test User' },
    released_at: '2026-08-21T10:05:00Z',
    idempotency_key: 'release-key',
    snapshot_digest: 'snapshot-digest',
  },
  version: 1,
  created_at: '2026-08-21T10:05:00Z',
}

const baseDocument = {
  document_id: 'document-1',
  kind: 'delivery_brief',
  approved_revision_id: 'document-revision-1',
  approved_digest: 'document-approved-digest',
  working_revision_id: null,
  working_digest: null,
  status: 'current',
  reason: null,
}

const baseOverview = {
  project_id: 'project-1',
  project_name: 'Forge Project',
  vision: 'Make release truth inspectable.',
  charter_state: 'approved',
  current_charter: null,
  primary_milestone_id: 'milestone-1',
  active_milestones: [readyMilestone],
  task_counts: counts,
  check_summary: checks,
  unresolved_decision_ids: [],
  risks: [],
  document_freshness: [baseDocument],
  evidence: [
    {
      id: 'evidence-1',
      project_id: 'project-1',
      asset_id: 'asset-1',
      task_id: 'task-1',
      source_task_id: 'task-1',
      source_run_id: 'run-1',
      source_validation_id: null,
      source_task_version: null,
      source_context_digest: null,
      source_definition_revision_id: null,
      milestone_id: 'milestone-1',
      acceptance_check_ids: ['check-1'],
      caption: 'Release proof',
      kind: 'screenshot',
      checksum: 'asset-checksum',
      availability: 'quarantined',
      author: { kind: 'user', id: 'user-1', display_name: 'Test User' },
      captured_at: '2026-08-21T10:00:00Z',
      version: 1n,
      created_at: '2026-08-21T10:00:00Z',
      removed_at: null,
    },
  ],
  releases: [],
  decisions: [],
  next_action: {
    code: 'release_milestone',
    required_principal: 'user',
    target_type: 'milestone',
    target_id: 'milestone-1',
    title: 'Release First release',
    explanation:
      'Review the exact readiness candidate and release it if the snapshot is still current.',
    action_kind: 'release',
    route_or_operation: 'POST /projects/project-1/milestones/milestone-1/release',
    blocking: true,
    expected_version: 8,
  },
  projection_state: 'current',
  source_event_watermark: 'event-700',
  generated_at: '2026-08-21T10:00:00Z',
} as unknown as ProjectOverview

function renderPage(data: ProjectOverview = baseOverview) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  })
  vi.mocked(useProjectOverviewQuery).mockReturnValue({
    data,
    isLoading: false,
    isError: false,
    error: null,
    refetch: vi.fn(),
  } as unknown as ReturnType<typeof useProjectOverviewQuery>)
  return rtlRender(
    <QueryClientProvider client={queryClient}>
      <ProjectOverviewPage projectId="project-1" />
    </QueryClientProvider>,
  )
}

function releaseMutation(overrides: Record<string, unknown> = {}) {
  const mutate = vi.fn()
  const mutateAsync = vi.fn().mockResolvedValue(release)
  const state = {
    mutate,
    mutateAsync,
    isPending: false,
    isError: false,
    error: null,
    data: undefined,
    reset: vi.fn(),
    ...overrides,
  }
  vi.mocked(useReleaseProjectMilestone).mockReturnValue(state as never)
  return { mutate, mutateAsync }
}

function manualAttestationMutation(overrides: Record<string, unknown> = {}) {
  const mutateAsync = vi.fn().mockResolvedValue({ id: 'validation-result-1' })
  const state = {
    mutateAsync,
    isPending: false,
    error: null,
    reset: vi.fn(),
    ...overrides,
  }
  vi.mocked(useRecordManualMilestoneCheck).mockReturnValue(state as never)
  return { mutateAsync }
}

describe('Project Overview release flow', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    useAuthStore.setState({
      accessToken: 'access-token',
      refreshToken: 'refresh-token',
      user: { id: 'user-1', display_name: 'Test User' } as never,
    })
    releaseMutation()
    manualAttestationMutation()
  })

  it('submits the exact readiness candidate identity, digest, and milestone CAS version', async () => {
    const mutation = releaseMutation()

    renderPage()

    const releaseButton = screen.getByRole('button', {
      name: /review exact release snapshot for m001/i,
    }) as HTMLButtonElement
    expect(releaseButton.disabled).toBe(false)
    fireEvent.click(releaseButton)
    const dialog = screen.getByRole('dialog')
    expect(within(dialog).getByText('readiness-1')).toBeTruthy()
    expect(within(dialog).getByText('readiness-digest-1')).toBeTruthy()
    fireEvent.click(within(dialog).getByRole('button', { name: 'Confirm release' }))

    await waitFor(() => {
      expect(mutation.mutateAsync.mock.calls.length).toBe(1)
    })
    const call = mutation.mutateAsync.mock.calls[0]
    expect(call?.[0]).toEqual(
      expect.objectContaining({
        projectId: 'project-1',
        milestoneId: 'milestone-1',
        expectedMilestoneVersion: 8,
        readinessSnapshotId: 'readiness-1',
        readinessDigest: 'readiness-digest-1',
      }),
    )
    expect(call?.[0]?.authorization).toEqual(
      expect.objectContaining({
        action: 'project.milestone.release',
        authorization_basis: 'interactive_user_release',
        principal: expect.objectContaining({ kind: 'user' }),
      }),
    )
  })

  it('records a deliberate manual result without presenting it as evidence or release', async () => {
    const mutation = manualAttestationMutation()
    const manualMilestone = {
      ...readyMilestone,
      milestone: {
        ...readyMilestone.milestone,
        lifecycle: 'active',
        version: 4n,
      },
      definition: {
        ...readyMilestone.definition,
        content: {
          ...readyMilestone.definition.content,
          charter_revision: {
            artifact_id: 'charter-1',
            revision_id: 'charter-revision-1',
            content_digest: 'charter-digest',
            render_version: 'forge.project-charter/v1',
            render_digest: 'charter-render-digest',
          },
          acceptance_checks: [
            {
              id: 'check-manual-1',
              description: 'The user observes that the saved list survives a refresh.',
              required: true,
              source_kind: 'manual',
              expected_result: 'passed',
              latest_result: null,
              latest_result_id: null,
              latest_result_digest: null,
            },
          ],
          evidence_requirements: [
            {
              id: 'check-manual-1',
              description: 'Refresh proof',
              required: true,
              evidence_kind: 'report',
              check_definition_revision: 'milestone-revision-1',
            },
          ],
        },
      },
      check_summary: { ...checks, passed: 0n, missing: 1n },
      current_checks: [
        {
          id: 'check-manual-1',
          description: 'The user observes that the saved list survives a refresh.',
          required: true,
          source_kind: 'manual',
          expected_result: 'passed',
          version: 3n,
          latest_result: null,
          latest_result_id: null,
          latest_result_digest: null,
        },
      ],
      latest_readiness: null,
      readiness_freshness: null,
      evidence: [],
    }
    renderPage({
      ...baseOverview,
      active_milestones: [manualMilestone],
      check_summary: { ...checks, passed: 0n, missing: 1n },
    } as unknown as ProjectOverview)

    expect(screen.getByText('Required Report evidence is still missing.')).toBeTruthy()
    fireEvent.click(screen.getByRole('button', { name: 'Record attestation' }))
    const dialog = screen.getByRole('dialog', { name: 'Record manual acceptance' })
    const submit = within(dialog).getByRole('button', { name: 'Record result' })
    expect((submit as HTMLButtonElement).disabled).toBe(true)
    fireEvent.click(within(dialog).getByRole('button', { name: 'Pass' }))
    fireEvent.change(within(dialog).getByLabelText('Observation'), {
      target: { value: 'I refreshed the page and the exact list remained visible.' },
    })
    fireEvent.click(submit)

    await waitFor(() =>
      expect(mutation.mutateAsync).toHaveBeenCalledWith(
        expect.objectContaining({
          projectId: 'project-1',
          milestoneId: 'milestone-1',
          checkId: 'check-manual-1',
          definitionRevisionId: 'milestone-revision-1',
          charterRevisionId: 'charter-revision-1',
          expectedCheckVersion: 3,
          status: 'pass',
          result: 'I refreshed the page and the exact list remained visible.',
        }),
      ),
    )
    expect(screen.queryByText(/is now immutable release truth/i)).toBeNull()
  })

  it('keeps release disabled and announces the pending state while the user release is in flight', async () => {
    releaseMutation({ isPending: true })

    renderPage()

    const releaseButton = screen.getByRole('button', {
      name: /review exact release snapshot for m001/i,
    }) as HTMLButtonElement
    expect(releaseButton.disabled).toBe(true)
    expect(releaseButton.getAttribute('aria-busy')).toBe('true')
    expect(releaseButton.textContent).toMatch(/releas/i)
  })

  it('fails closed when the readiness freshness overlay is stale', () => {
    renderPage({
      ...baseOverview,
      active_milestones: [
        {
          ...readyMilestone,
          readiness_freshness: {
            status: 'stale',
            reason: 'A newer source event has not been incorporated yet.',
            snapshot_source_event_watermark: 'event-700',
            current_source_event_watermark: 'event-701',
          },
        },
      ],
    } as unknown as ProjectOverview)

    const releaseButton = screen.getByRole('button', {
      name: /review exact release snapshot for m001/i,
    }) as HTMLButtonElement
    expect(releaseButton.disabled).toBe(true)
    expect(screen.getByText(/newer source event has not been incorporated/i)).toBeTruthy()
  })

  it('fails closed when the readiness freshness overlay is absent', () => {
    renderPage({
      ...baseOverview,
      active_milestones: [{ ...readyMilestone, readiness_freshness: null }],
    } as unknown as ProjectOverview)

    const releaseButton = screen.getByRole('button', {
      name: /review exact release snapshot for m001/i,
    }) as HTMLButtonElement
    expect(releaseButton.disabled).toBe(true)
    expect(screen.getByText(/freshness is unavailable/i)).toBeTruthy()
  })

  it('does not expose an enabled release action without an authenticated user', () => {
    useAuthStore.setState({ user: null })

    renderPage()

    const releaseButton = screen.getByRole('button', {
      name: /review exact release snapshot for m001/i,
    }) as HTMLButtonElement
    expect(releaseButton.disabled).toBe(true)
    expect(screen.getByText(/sign in again before releasing/i)).toBeTruthy()
  })

  it('surfaces a version conflict as a retryable release state without losing the candidate metadata', async () => {
    releaseMutation({
      isError: true,
      error: new Error('The milestone changed while this release was open.'),
      mutateAsync: vi
        .fn()
        .mockRejectedValue(new Error('The milestone changed while this release was open.')),
    })

    renderPage()

    fireEvent.click(screen.getByRole('button', { name: /review exact release snapshot for m001/i }))
    const dialog = screen.getByRole('dialog')
    fireEvent.click(within(dialog).getByRole('button', { name: 'Confirm release' }))
    expect((await within(dialog).findByRole('alert')).textContent).toMatch(
      /changed|refresh|current|release/i,
    )
    expect(screen.getByTitle('readiness-1')).toBeTruthy()
    expect(screen.getByTitle('readiness-digest-1')).toBeTruthy()
    expect(
      (
        screen.getByRole('button', {
          name: /review exact release snapshot for m001/i,
        }) as HTMLButtonElement
      ).disabled,
    ).toBe(false)
  })

  it('renders a typed next action with its principal, target, and exact operation', () => {
    renderPage()

    expect(screen.getByText('Release First release')).toBeTruthy()
    expect(screen.getByText(/Review the exact readiness candidate/)).toBeTruthy()
    expect(screen.getByText(/code release_milestone · milestone milestone-1/i)).toBeTruthy()
    expect(screen.getAllByText(/milestone-1/).length).toBeGreaterThan(0)
    expect(screen.getByText(/blocking action/i)).toBeTruthy()
  })

  it('renders effective decisions and keeps pending proposal IDs separate', () => {
    renderPage({
      ...baseOverview,
      unresolved_decision_ids: ['candidate-1'],
      decisions: [
        {
          id: 'decision-1',
          project_id: 'project-1',
          state: 'active',
          question: 'Which release boundary is authoritative?',
          context: null,
          options: ['M001', 'M002'],
          selected_outcome: 'M001',
          rationale: 'The first milestone has the complete acceptance record.',
          decision_maker: { kind: 'user', id: 'user-1', display_name: 'Test User' },
          decision_class: 'user_scope',
          authority_basis: 'interactive_user_decision',
          affected_artifact_refs: [],
          affected_task_ids: [],
          affected_milestone_ids: ['milestone-1'],
          supersedes_id: null,
          provenance: [],
          created_at: '2026-08-21T10:00:00Z',
          effective_at: '2026-08-21T10:00:00Z',
        },
      ],
    } as unknown as ProjectOverview)

    expect(screen.getByText('Pending proposals')).toBeTruthy()
    expect(
      screen.getAllByText(
        (_content, element) =>
          element?.textContent?.replace(/\s+/g, ' ').trim() === 'Pending proposal candidate-1',
      ).length,
    ).toBeGreaterThan(0)
    expect(screen.getByText('Decision log')).toBeTruthy()
    expect(screen.getByText('Which release boundary is authoritative?')).toBeTruthy()
    expect(screen.getByText('The first milestone has the complete acceptance record.')).toBeTruthy()
    expect(screen.getByText(/User Scope/)).toBeTruthy()
    expect(screen.getByText('Charter risks')).toBeTruthy()
  })

  it('shows a draft-only Document as changes pending rather than pretending there is no Document', () => {
    renderPage({
      ...baseOverview,
      document_freshness: [
        {
          document_id: 'document-draft-only',
          kind: 'design',
          approved_revision_id: null,
          approved_digest: null,
          working_revision_id: 'document-revision-draft',
          working_digest: 'document-draft-digest',
          status: 'changes_pending',
          reason: 'A working draft is awaiting approval.',
        },
      ],
    } as unknown as ProjectOverview)

    expect(screen.getByText(/Design/i)).toBeTruthy()
    expect(screen.getAllByText(/changes pending|draft/i).length).toBeGreaterThan(0)
    expect(screen.getByText(/working .*document.*draft/)).toBeTruthy()
    expect(screen.getByText(/digest document.*digest/)).toBeTruthy()
    expect(screen.getByText(/awaiting approval/i)).toBeTruthy()
  })

  it('keeps immutable release history separate from the live readiness candidate', () => {
    renderPage({
      ...baseOverview,
      releases: [release as unknown as ProjectOverview['releases'][number]],
    })

    expect(screen.getAllByText('First release').length).toBeGreaterThan(0)
    expect(screen.getAllByText(/immutable|released/i).length).toBeGreaterThan(0)
    expect(screen.getByText(/snapshot-digest/)).toBeTruthy()
    expect(
      screen.getByRole('link', { name: /inspect immutable snapshot/i }).getAttribute('href'),
    ).toBe('/projects/project-1/releases/release-1')

    expect(screen.getAllByText(/ready for release/i).length).toBeGreaterThan(0)
  })

  it('keeps the release candidate and evidence controls keyboard-addressable', () => {
    renderPage()

    expect(screen.getByRole('button', { name: /review exact release snapshot/i })).toBeTruthy()
    expect(screen.getByRole('link', { name: /project agent chat/i }).getAttribute('href')).toBe(
      '/projects/project-1/chat',
    )
    expect(screen.getByRole('region', { name: /evidence gallery/i }).getAttribute('tabindex')).toBe(
      '0',
    )
  })
})
