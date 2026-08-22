import { fireEvent, render as rtlRender, screen, waitFor, within } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { ApiError } from '@/api/client'
import { useProjectOverviewQuery } from '@/api/hooks'
import { ProjectOverviewPage } from '@/pages/ProjectOverviewPage'
import type { ProjectOverview, ProjectRelease } from '@/types/generated'

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
  useProjectQuery: vi.fn(() => ({ data: undefined, isLoading: false })),
}))

vi.mock('@/features/project-execution/ProjectExecutionSetupPanel', () => ({
  ProjectExecutionSetupPanel: () => null,
}))

const mediaFetch = vi.hoisted(() => vi.fn())
vi.mock('@/api/client', () => {
  class MockApiError extends Error {
    status: number

    constructor(message: string, status: number) {
      super(message)
      this.status = status
    }
  }

  return {
    apiFetchBlob: mediaFetch,
    // The adoption banner reads the Project Charter; an empty projection keeps
    // it on the "nothing to approve yet" branch these Overview tests assert.
    apiFetch: vi.fn(async () => ({
      charter: null,
      revisions: [],
      current_draft_revision: null,
      current_approved_revision: null,
      approval: null,
      selected_project_agent: null,
    })),
    ApiError: MockApiError,
  }
})

/** Every page render needs a QueryClient: the adoption banner queries. */
function render(ui: ReactNode) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  })
  return rtlRender(<QueryClientProvider client={queryClient}>{ui}</QueryClientProvider>)
}

const counts = {
  total: 8n,
  backlog: 2n,
  active: 2n,
  review: 1n,
  terminal: 3n,
  blocked: 1n,
}

const checkSummary = {
  required_total: 3n,
  passed: 1n,
  failed: 1n,
  missing: 1n,
  stale: 0n,
  waived: 0n,
  unavailable: 0n,
}

const activeMilestone = {
  milestone: {
    id: 'milestone-1',
    project_id: 'project-1',
    milestone_sequence: 1n,
    canonical_id: 'M001',
    display_label: 'First release',
    definition_revision_id: 'milestone-revision-1',
    lifecycle: 'active' as const,
    projection_reasons: [],
    version: 1n,
    created_at: '2026-08-13T10:00:00Z',
    updated_at: '2026-08-13T10:00:00Z',
  },
  definition: {
    id: 'milestone-revision-1',
    milestone_id: 'milestone-1',
    project_id: 'project-1',
    revision_number: 1n,
    base_revision_id: null,
    lifecycle: 'approved' as const,
    schema_version: 'v1',
    content: {
      name: 'First release',
      outcome: 'A bounded project outcome with explicit evidence.',
      included_scope: ['Project Overview'],
      excluded_scope: ['Unapproved release automation'],
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
    content_digest: 'digest-milestone',
    render_digest: 'render-milestone',
    provenance: {
      author: { kind: 'user' as const, id: 'user-1', display_name: 'Test User' },
      profile_revision: null,
      operating_skill_revision: null,
      source_refs: [],
      change_summary: 'Initial milestone',
      material_diff: null,
    },
    created_at: '2026-08-13T10:00:00Z',
  },
  task_counts: counts,
  check_summary: checkSummary,
  latest_readiness: null,
  evidence: [],
}

const overview: ProjectOverview = {
  project_id: 'project-1',
  project_name: 'Forge Project',
  vision: 'Make project progress inspectable and honest.',
  charter_state: 'approved',
  current_charter: null,
  primary_milestone_id: 'milestone-1',
  active_milestones: [activeMilestone],
  task_counts: counts,
  check_summary: checkSummary,
  unresolved_decision_ids: ['decision-1'],
  risks: [
    {
      id: 'risk-1',
      description: 'Evidence may become stale.',
      impact: 'Release confidence drops.',
      treatment: 'Re-run validation.',
      revisit_trigger: null,
      owner: null,
    },
  ],
  document_freshness: [
    {
      document_id: 'document-1',
      kind: 'delivery_brief',
      current_revision_id: 'document-revision-1',
      current_digest: 'document-digest',
      stale: false,
      reason: null,
    },
  ],
  evidence: [],
  releases: [],
  next_action: 'Resolve the failed acceptance check.',
  projection_state: 'current',
  source_event_watermark: 'event-123',
  generated_at: '2026-08-13T10:00:00Z',
}

const videoEvidence = {
  id: 'evidence-1',
  project_id: 'project-1',
  asset_id: 'asset-1',
  task_id: 'task-1',
  source_task_id: 'task-1',
  source_run_id: 'run-1',
  source_validation_id: null,
  milestone_id: 'milestone-1',
  acceptance_check_ids: ['check-1'],
  caption: 'Project walkthrough',
  kind: 'walkthrough_video' as const,
  checksum: 'asset-checksum',
  availability: 'available' as const,
  author: { kind: 'user' as const, id: 'user-1', display_name: 'Test User' },
  captured_at: '2026-08-13T10:00:00Z',
  version: 1n,
  created_at: '2026-08-13T10:00:00Z',
  removed_at: null,
}

const release = {
  id: 'release-1',
  project_id: 'project-1',
  milestone_id: 'milestone-1',
  release_sequence: 1,
  release_identity: 'release-1',
  snapshot: {
    schema_version: 'forge.release/v1',
    project_id: 'project-1',
    milestone_id: 'milestone-1',
    milestone_canonical_id: 'M001',
    release_revision: 1,
    release_identity: 'release-1',
    milestone_definition_revision_id: 'milestone-revision-1',
    milestone_definition_digest: 'milestone-digest',
    display_label: 'First release',
    summary: 'The first bounded release.',
    changelog: [],
    known_issues: [],
    readiness_snapshot_id: 'readiness-1',
    readiness_digest: 'readiness-digest',
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
    release_policy_revision: 'policy-v1',
    released_by: { kind: 'user', id: 'user-1', display_name: 'Test User' },
    released_at: '2026-08-13T10:00:00Z',
    idempotency_key: 'release-key',
    snapshot_digest: 'snapshot-digest',
  },
  version: 1,
  created_at: '2026-08-13T10:00:00Z',
} as unknown as ProjectRelease

function mockQuery(value: Partial<ReturnType<typeof useProjectOverviewQuery>>) {
  vi.mocked(useProjectOverviewQuery).mockReturnValue({
    data: overview,
    isLoading: false,
    isError: false,
    error: null,
    refetch: vi.fn(),
    ...value,
  } as ReturnType<typeof useProjectOverviewQuery>)
}

describe('ProjectOverviewPage', () => {
  beforeEach(() => {
    mockQuery({})
    mediaFetch.mockReset()
    mediaFetch.mockResolvedValue(new Blob(['evidence'], { type: 'video/mp4' }))
    Object.defineProperty(URL, 'createObjectURL', {
      configurable: true,
      value: vi.fn(() => 'blob:evidence-preview'),
    })
    Object.defineProperty(URL, 'revokeObjectURL', {
      configurable: true,
      value: vi.fn(),
    })
  })

  it('shows live outcome, authoritative counts, risks, and separate release truth', () => {
    render(<ProjectOverviewPage projectId="project-1" />)

    expect(screen.getByRole('heading', { name: 'Forge Project' })).toBeTruthy()
    expect(screen.getByRole('heading', { name: 'Current outcome' })).toBeTruthy()
    expect(screen.getByText('Primary milestone ID milestone-1')).toBeTruthy()
    expect(screen.getByText('Evidence coverage')).toBeTruthy()
    expect(screen.getByText('0/0 available')).toBeTruthy()
    expect(screen.getByText('Coverage 0/0 available')).toBeTruthy()
    expect(screen.getByText('A bounded project outcome with explicit evidence.')).toBeTruthy()
    expect(screen.getAllByText('Resolve the failed acceptance check.').length).toBeGreaterThan(0)
    expect(screen.getByText('Evidence may become stale.')).toBeTruthy()
    expect(
      screen.getByText(
        'No immutable release snapshots exist yet. A readiness result is only a release candidate.',
      ),
    ).toBeTruthy()
    expect(screen.queryByText(/percent complete/i)).toBeNull()
    expect(screen.getByRole('link', { name: /Project Agent Chat/ }).getAttribute('href')).toBe(
      '/projects/project-1/chat',
    )
  })

  it('keeps Tasks and Project Agent Chat usable when Charter adoption is required', () => {
    mockQuery({
      data: {
        ...overview,
        charter_state: 'charter_setup_required',
        active_milestones: [],
        next_action: 'Ask the Project Agent to prepare an adoption Charter.',
      },
    })

    render(<ProjectOverviewPage projectId="project-1" />)

    expect(screen.getByText('Charter adoption is required before release')).toBeTruthy()
    expect(screen.getByText('No active milestone is defined yet.')).toBeTruthy()
    expect(screen.getByRole('link', { name: /View Tasks/ }).getAttribute('href')).toContain(
      '/projects/project-1/tasks',
    )
    expect(
      screen.getAllByRole('link', { name: /Continue with Project Agent/ }).length,
    ).toBeGreaterThan(0)
  })

  it('routes release history to the authenticated immutable snapshot view', () => {
    mockQuery({ data: { ...overview, releases: [release] } })

    render(<ProjectOverviewPage projectId="project-1" />)

    expect(
      screen.getByRole('link', { name: /Inspect immutable snapshot/ }).getAttribute('href'),
    ).toBe('/projects/project-1/releases/release-1')
  })

  it('marks stale projection data as not current release truth', () => {
    mockQuery({ data: { ...overview, projection_state: 'stale' } })

    render(<ProjectOverviewPage projectId="project-1" />)

    const status = screen.getByRole('status', { name: '' })
    expect(within(status).getByText('Overview is stale')).toBeTruthy()
    expect(within(status).getByText(/not current release truth/)).toBeTruthy()
  })

  it('renders authorized walkthroughs with bounded controls, poster, duration, and no autoplay', async () => {
    mockQuery({ data: { ...overview, evidence: [videoEvidence] } })

    const rendered = render(<ProjectOverviewPage projectId="project-1" />)

    const video = await screen.findByLabelText('Project walkthrough')
    expect(video.tagName).toBe('VIDEO')
    expect(video.hasAttribute('controls')).toBe(true)
    expect(video.hasAttribute('autoplay')).toBe(false)
    expect(video.hasAttribute('playsinline')).toBe(true)
    expect(video.getAttribute('poster')).toBe('/logo.png')
    expect(screen.getByText(/explicit play controls; video never autoplays/)).toBeTruthy()
    Object.defineProperty(video, 'duration', { configurable: true, value: 92 })
    fireEvent.loadedMetadata(video)
    expect(screen.getByText(/1:32 · explicit play controls/)).toBeTruthy()
    expect(screen.getByRole('region', { name: 'Evidence gallery' }).className).toContain(
      'overflow-x-auto',
    )
    expect(mediaFetch).toHaveBeenCalledWith('/projects/project-1/media/asset-1')
    expect(screen.getByRole('link', { name: /Open video/ }).getAttribute('href')).toBe(
      'blob:evidence-preview',
    )
    rendered.unmount()
    await waitFor(() => expect(URL.revokeObjectURL).toHaveBeenCalledWith('blob:evidence-preview'))
  })

  it('opens authorized non-media evidence with provenance metadata', async () => {
    const reportEvidence = {
      ...videoEvidence,
      id: 'evidence-report',
      asset_id: 'asset-report',
      source_run_id: null,
      source_validation_id: 'validation-1',
      caption: 'Validation report',
      kind: 'report' as const,
    }
    mockQuery({ data: { ...overview, evidence: [reportEvidence] } })

    render(<ProjectOverviewPage projectId="project-1" />)

    expect(await screen.findByRole('link', { name: /Open evidence file/ })).toBeTruthy()
    expect(mediaFetch).toHaveBeenCalledWith('/projects/project-1/media/asset-report')
    expect(
      screen.getByText(/Task task-1 · validation validation-1 · uploaded by Test User/),
    ).toBeTruthy()
    expect(screen.getByRole('link', { name: 'Download' }).getAttribute('href')).toBe(
      'blob:evidence-preview',
    )
  })

  it('shows server conflict authority and revision/digest comparison', () => {
    mockQuery({
      data: undefined,
      isError: true,
      error: new ApiError(
        JSON.stringify({
          code: 'overview_conflict',
          message: 'projection changed',
          details: {
            authority_domain: 'project_overview_projection',
            expected_revision: 'revision-3',
            current_revision: 'revision-4',
            expected_digest: 'digest-old',
            current_digest: 'digest-new',
          },
        }),
        409,
      ),
    })

    render(<ProjectOverviewPage projectId="project-1" />)

    expect(screen.getByText('Authority:')).toBeTruthy()
    expect(screen.getByText('project_overview_projection')).toBeTruthy()
    expect(screen.getByText('Expected revision')).toBeTruthy()
    expect(screen.getByText('revision-3')).toBeTruthy()
    expect(screen.getByText('Current revision')).toBeTruthy()
    expect(screen.getByText('revision-4')).toBeTruthy()
    expect(screen.getByText('digest-old')).toBeTruthy()
    expect(screen.getByText('digest-new')).toBeTruthy()
  })

  it('preserves an explicit loading state', () => {
    mockQuery({ data: undefined, isLoading: true })

    render(<ProjectOverviewPage projectId="project-1" />)

    expect(screen.getByRole('status').getAttribute('aria-busy')).toBe('true')
    expect(screen.getByText('Loading Project Overview…')).toBeTruthy()
  })
})
