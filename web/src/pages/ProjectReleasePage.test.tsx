import { render, screen } from '@testing-library/react'
import type { ReactNode } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { ApiError } from '@/api/client'
import { useProjectReleaseQuery } from '@/api/hooks'
import type { ProjectRelease } from '@/types/generated'
import { ProjectReleasePage } from './ProjectReleasePage'

type LinkProps = {
  to: string
  params?: Record<string, string>
  children: ReactNode
}

vi.mock('@tanstack/react-router', () => ({
  Link: ({ to, params, children }: LinkProps) => {
    const href = params
      ? Object.entries(params).reduce((path, [key, value]) => path.replace(`$${key}`, value), to)
      : to
    return <a href={href}>{children}</a>
  },
}))

vi.mock('@/api/hooks', () => ({
  useProjectReleaseQuery: vi.fn(),
}))

const release = {
  id: 'release-1',
  project_id: 'project-1',
  milestone_id: 'milestone-1',
  release_sequence: 2,
  release_identity: 'release-identity-2',
  snapshot: {
    schema_version: 'forge.release/v2',
    project_id: 'project-1',
    milestone_id: 'milestone-1',
    milestone_canonical_id: 'M001',
    release_revision: 2,
    release_identity: 'release-identity-2',
    milestone_definition_revision_id: 'milestone-revision-2',
    milestone_definition_digest: 'milestone-definition-digest',
    expected_milestone_version: 7,
    display_label: 'First bounded outcome',
    summary: 'A frozen outcome with inspectable proof.',
    changelog: ['Pinned the validation result.'],
    known_issues: ['One evidence item is quarantined.'],
    readiness_snapshot_id: 'readiness-2',
    readiness_digest: 'readiness-digest',
    source_event_watermark: 'event-900',
    charter_revision: {
      artifact_id: 'charter-1',
      revision_id: 'charter-revision-2',
      content_digest: 'charter-digest',
      render_version: 'v1',
      render_digest: 'charter-render-digest',
    },
    document_revisions: [],
    included_decisions: [],
    included_tasks: [],
    validation_results: [],
    repository_references: [],
    evidence_pins: [
      {
        id: 'pin-1',
        release_id: 'release-1',
        attachment_id: 'attachment-1',
        asset_id: 'asset-1',
        attachment_digest: 'attachment-digest',
        asset_checksum: 'asset-checksum',
        availability: 'available',
        availability_projection: 'available',
        task_media_id: null,
        stable_project_url: null,
        pinned_at: '2026-08-13T10:00:00Z',
      },
    ],
    waived_check_ids: [],
    released_by: { kind: 'user', id: 'user-1', display_name: 'Test User' },
    authorization: {
      principal: { kind: 'user', id: 'user-1', display_name: 'Test User' },
      authorization_basis: 'interactive_user_approval',
      action: 'project.release',
      event_id: 'authorization-event-1',
      occurred_at: '2026-08-13T10:00:00Z',
    },
    released_at: '2026-08-13T10:00:00Z',
    idempotency_key: 'release-key',
    snapshot_digest: 'snapshot-digest',
  },
  version: 1,
  created_at: '2026-08-13T10:00:00Z',
} as unknown as ProjectRelease

function mockQuery(value: Partial<ReturnType<typeof useProjectReleaseQuery>>) {
  vi.mocked(useProjectReleaseQuery).mockReturnValue({
    data: release,
    isLoading: false,
    isError: false,
    error: null,
    refetch: vi.fn(),
    ...value,
  } as ReturnType<typeof useProjectReleaseQuery>)
}

describe('ProjectReleasePage', () => {
  beforeEach(() => mockQuery({}))

  it('renders frozen provenance and release-time evidence projection', () => {
    render(<ProjectReleasePage projectId="project-1" releaseId="release-1" />)

    expect(screen.getByRole('heading', { name: 'release-identity-2' })).toBeTruthy()
    expect(screen.getByText('Source watermark')).toBeTruthy()
    expect(screen.getByText('event-900')).toBeTruthy()
    expect(screen.getByText('Milestone definition revision')).toBeTruthy()
    expect(screen.getByText('milestone-revision-2')).toBeTruthy()
    expect(screen.getByText('milestone-definition-digest')).toBeTruthy()
    expect(screen.getByText(/pin pin-1 · attachment attachment-1/)).toBeTruthy()
    expect(screen.getByText(/attachment digest attachment-digest/)).toBeTruthy()
    expect(screen.getByText(/release projection · Available/)).toBeTruthy()
    expect(screen.getByText('Authorization event')).toBeTruthy()
    expect(screen.getByText('authorization-event-1')).toBeTruthy()
  })

  it('withholds release details when the authenticated request is denied', () => {
    mockQuery({ data: undefined, isError: true, error: new ApiError('denied', 403) })

    render(<ProjectReleasePage projectId="project-1" releaseId="release-1" />)

    expect(screen.getByRole('heading', { name: 'Release snapshot access denied' })).toBeTruthy()
    expect(screen.queryByText('snapshot-digest')).toBeNull()
    expect(screen.getByRole('link', { name: /Project Overview/ })).toBeTruthy()
  })
})
