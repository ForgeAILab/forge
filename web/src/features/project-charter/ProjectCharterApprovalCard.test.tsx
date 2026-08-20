import { render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { apiFetch } from '@/api/client'
import { useProjectQuery } from '@/api/hooks'
import { useAuthStore } from '@/stores/auth'

import { ProjectCharterApprovalCard } from './ProjectCharterApprovalCard'

vi.mock('@/api/client', () => ({
  apiFetch: vi.fn(),
  ApiError: class extends Error {
    status: number
    constructor(message: string, status: number) {
      super(message)
      this.status = status
    }
  },
}))

vi.mock('@/api/hooks', () => ({
  useProjectQuery: vi.fn(),
}))

const draftRevision = {
  id: 'revision-1',
  charter_id: 'charter-1',
  revision_number: 1n,
  base_revision_id: null,
  lifecycle: 'draft' as const,
  project_mode: 'compact' as const,
  maturity: 'mvp' as const,
  schema_version: 'forge.project-charter/v1',
  content: { identity: { working_name: 'NoteJot', slug_proposal: 'notejot' } },
  rendered_view: '# NoteJot\nA durable adoption Charter.',
  render_version: 'charter-render-v1',
  content_digest: 'content-digest-0123456789abcdef',
  render_digest: 'render-digest',
  provenance: null,
  readiness: null,
  approved_at: null,
  superseded_by_revision_id: null,
  created_at: '2026-08-20T10:00:00Z',
}

const charterResponse = {
  charter: { id: 'charter-1', version: 2n },
  revisions: [draftRevision],
  current_draft_revision: draftRevision,
  current_approved_revision: null,
  approval: null,
  selected_project_agent: {
    identity_id: 'identity-1',
    profile_revision_id: 'profile-1',
    operating_skill_revision: 'skill-1',
    policy_digest: 'policy-digest',
  },
}

function renderCard(node: ReactNode) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  })
  return render(<QueryClientProvider client={queryClient}>{node}</QueryClientProvider>)
}

describe('ProjectCharterApprovalCard', () => {
  beforeEach(() => {
    vi.mocked(useProjectQuery).mockReturnValue({
      data: { id: 'project-1', version: 7 },
      isLoading: false,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any)
    useAuthStore.setState({
      accessToken: 'token',
      refreshToken: 'refresh',
      user: { id: 'user-1', display_name: 'Test User' },
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any)
    vi.mocked(apiFetch).mockReset()
  })

  it('renders nothing when the Project has no unapproved Charter revision', async () => {
    vi.mocked(apiFetch).mockResolvedValue({
      ...charterResponse,
      current_draft_revision: null,
    })

    const { container } = renderCard(<ProjectCharterApprovalCard projectId="project-1" />)

    await waitFor(() => expect(apiFetch).toHaveBeenCalled())
    expect(container.textContent).toBe('')
  })

  it('offers the draft revision for approval', async () => {
    vi.mocked(apiFetch).mockResolvedValue(charterResponse)

    renderCard(<ProjectCharterApprovalCard projectId="project-1" />)

    expect(await screen.findByText('Project Charter awaiting your approval')).toBeTruthy()
    expect(screen.getByText(/Revision 1/)).toBeTruthy()
    expect(screen.getByRole('button', { name: 'Approve Charter' })).toBeTruthy()
  })

  it('approves the exact revision against the observed Charter and Project versions', async () => {
    vi.mocked(apiFetch).mockImplementation(async (path: string) => {
      if (path.endsWith('/approve')) return { id: 'approval-1', state: 'active' }
      return charterResponse
    })

    renderCard(<ProjectCharterApprovalCard projectId="project-1" />)
    const button = await screen.findByRole('button', { name: 'Approve Charter' })
    button.click()

    await waitFor(() => {
      expect(apiFetch).toHaveBeenCalledWith(
        '/projects/project-1/charter/revisions/revision-1/approve',
        expect.objectContaining({ method: 'POST' }),
      )
    })
    const call = vi
      .mocked(apiFetch)
      .mock.calls.find(([path]) => String(path).endsWith('/approve'))
    const body = JSON.parse(String((call?.[1] as { body: string }).body))
    expect(body.revision_id).toBe('revision-1')
    expect(body.expected_charter_version).toBe(2)
    expect(body.expected_project_version).toBe(7)
    expect(body.content_digest).toBe(draftRevision.content_digest)
    expect(body.selected_project_agent_identity_id).toBe('identity-1')
  })

  it('signs the approval with the action string the server expects', async () => {
    vi.mocked(apiFetch).mockImplementation(async (path: string) => {
      if (path.endsWith('/approve')) return { id: 'approval-1', state: 'active' }
      return charterResponse
    })

    renderCard(<ProjectCharterApprovalCard projectId="project-1" />)
    const button = await screen.findByRole('button', { name: 'Approve Charter' })
    button.click()

    await waitFor(() => {
      const call = vi
        .mocked(apiFetch)
        .mock.calls.find(([path]) => String(path).endsWith('/approve'))
      expect(call).toBeTruthy()
      const body = JSON.parse(String((call?.[1] as { body: string }).body))
      // Mirrors APPROVAL_ACTION in crates/api/src/routes/project_charters.rs;
      // a mismatch is rejected as an invalid authorization event (403).
      expect(body.mutation.authorization.action).toBe('project_charter.approval')
      expect(body.mutation.authorization.principal).toMatchObject({ kind: 'user', id: 'user-1' })
    })
  })

  it('surfaces a version conflict as a retryable message', async () => {
    const { ApiError } = await import('@/api/client')
    vi.mocked(apiFetch).mockImplementation(async (path: string) => {
      if (path.endsWith('/approve')) {
        throw new ApiError(JSON.stringify({ code: 'version_conflict', message: 'stale' }), 409)
      }
      return charterResponse
    })

    renderCard(<ProjectCharterApprovalCard projectId="project-1" />)
    const button = await screen.findByRole('button', { name: 'Approve Charter' })
    button.click()

    expect(await screen.findByRole('alert')).toHaveProperty(
      'textContent',
      expect.stringContaining('changed while this approval was open'),
    )
  })

  it('reports the server reason for a rejection instead of a generic conflict', async () => {
    const { ApiError } = await import('@/api/client')
    vi.mocked(apiFetch).mockImplementation(async (path: string) => {
      if (path.endsWith('/approve')) {
        throw new ApiError(
          JSON.stringify({
            code: 'charter_approval_conflict',
            message: 'Charter revision is not ready: people_missing',
          }),
          409,
        )
      }
      return charterResponse
    })

    renderCard(<ProjectCharterApprovalCard projectId="project-1" />)
    const button = await screen.findByRole('button', { name: 'Approve Charter' })
    button.click()

    expect(await screen.findByRole('alert')).toHaveProperty(
      'textContent',
      expect.stringContaining('people_missing'),
    )
  })

  it('blocks approval up front when the revision readiness is blocked', async () => {
    vi.mocked(apiFetch).mockResolvedValue({
      ...charterResponse,
      current_draft_revision: {
        ...draftRevision,
        readiness: {
          status: 'blocked',
          project_mode: 'compact',
          maturity: 'mvp',
          gaps: [
            {
              kind: 'missing_content',
              code: 'people_missing',
              message: 'At least one target user or beneficiary is required.',
              blocking: true,
              section: 'problem_and_people',
              knowledge_item_id: null,
            },
          ],
        },
      },
    })

    renderCard(<ProjectCharterApprovalCard projectId="project-1" />)

    const button = await screen.findByRole('button', { name: 'Approve Charter' })
    expect(button.hasAttribute('disabled')).toBe(true)
    expect(screen.getByRole('status').textContent).toContain(
      'At least one target user or beneficiary is required.',
    )
    expect(vi.mocked(apiFetch).mock.calls.some(([p]) => String(p).endsWith('/approve'))).toBe(false)
  })
})
