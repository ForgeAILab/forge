import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { createRef, type ReactNode } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { ApiError } from '@/api/client'
import {
  ProductGenesisCharterCard,
  type ProductGenesisCharterCardHandle,
} from './ProductGenesisControls'
import type {
  ProductGenesisCharterResponse,
  ProductGenesisSession,
  ProjectCharterApproval,
  ProjectCharterRevision,
} from './types'

type ActiveQueryState = {
  data: { session: ProductGenesisSession | null } | undefined
  isLoading: boolean
  isError: boolean
  refetch: ReturnType<typeof vi.fn>
}

type CharterQueryState = {
  data: ProductGenesisCharterResponse | undefined
  isLoading: boolean
  isError: boolean
  refetch: ReturnType<typeof vi.fn>
}

const state = vi.hoisted(() => ({
  approval: vi.fn(),
  create: vi.fn(),
  navigate: vi.fn(),
  active: null as ActiveQueryState | null,
  charter: null as CharterQueryState | null,
}))

vi.mock('@tanstack/react-router', () => ({
  Link: ({ children, to, params }: { children: ReactNode; to: string; params?: unknown }) => (
    <a href={to} data-params={JSON.stringify(params)}>
      {children}
    </a>
  ),
  useNavigate: () => state.navigate,
}))

vi.mock('@/api/hooks', () => ({
  useAgentsQuery: () => ({ data: { items: [] }, isLoading: false }),
}))

vi.mock('@/stores/auth', () => ({
  useAuthStore: { getState: () => ({ user: { id: 'user-1', display_name: 'Test User' } }) },
}))

vi.mock('./hooks', () => ({
  useProductGenesisActiveQuery: () => state.active,
  useProductGenesisCharterQuery: () => state.charter,
  useStartProductGenesisMutation: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useCancelProductGenesisMutation: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useApproveProductGenesisCharterRevisionMutation: () => ({
    mutateAsync: state.approval,
    isPending: false,
  }),
  useCreateProjectFromCharterApprovalMutation: () => ({
    mutateAsync: state.create,
    isPending: false,
  }),
}))

const revision = {
  id: 'revision-1',
  charter_id: 'charter-1',
  revision_number: 1,
  base_revision_id: null,
  lifecycle: 'proposed',
  project_mode: 'compact',
  maturity: 'mvp',
  schema_version: 'forge.project-orchestration/v1',
  content: {
    identity: {
      working_name: 'Signal Garden',
      slug_proposal: 'signal-garden',
      one_line_vision: 'A calm workspace for turning signals into decisions.',
      maturity: 'mvp',
      lifecycle_intent: null,
      project_type: 'product',
      value_proposition: 'Make decisions legible.',
    },
    problem_and_people: {
      problem_or_opportunity: 'Important signals disappear in busy work.',
      target_users: ['Product teams'],
      beneficiaries: [],
      jobs_pains_opportunity: [],
      current_alternatives: [],
      stakeholders: [],
      excluded_audiences: [],
    },
    core_experience: {
      primary_outcome: 'A decision with a visible source.',
      core_loop: null,
      principal_journeys: [],
    },
    scope: {
      must_have_outcomes: ['Decision ledger'],
      required_deliverables: ['Working web app'],
      later_possibilities: [],
      explicit_non_goals: ['No autonomous release'],
    },
    success: {
      qualitative_outcome: 'Teams can explain why a decision exists.',
      success_signals: [],
      acceptance_statements: ['A decision preserves provenance.'],
      required_evidence: [],
      non_claims: [],
    },
    constraints_and_risks: {
      product: [],
      time_and_budget: [],
      technology: [],
      data: [],
      integrations: [],
      security_privacy_compliance: [],
      accessibility: [],
      operations: [],
      migration: [],
      launch: [],
      agent_authority: ['No chat agent receives a repository Workspace.'],
      risks: [],
    },
    knowledge_ledger: { items: [] },
    handoff_note: null,
  },
  rendered_view: '# Signal Garden\n\nA calm workspace for turning signals into decisions.',
  render_version: 'v1',
  content_digest: 'content-digest-1',
  render_digest: 'render-digest-1',
  provenance: {
    author: { kind: 'agent', id: 'main-agent', display_name: 'Main Agent' },
    profile_revision: 'profile-r1',
    operating_skill_revision: 'forge.main.project-discovery/v2',
    source_refs: [],
    change_summary: 'Initial typed Charter proposal.',
    material_diff: null,
  },
  readiness: {
    status: 'ready',
    project_mode: 'compact',
    maturity: 'mvp',
    gaps: [],
    policy_revision: 'charter-policy-v1',
    evaluated_at: '2026-08-13T00:00:00Z',
    readiness_digest: 'readiness-digest-1',
  },
  approved_at: null,
  superseded_by_revision_id: null,
  created_at: '2026-08-13T00:00:00Z',
} as unknown as ProjectCharterRevision

const activeSession = {
  id: 'genesis-1',
  account_id: 'account-1',
  main_chat_id: 'chat-1',
  prompt_revision: 'forge.main.project-discovery/v2',
  maturity: 'mvp',
  initial_idea: 'A provenance-aware decision workspace',
  lifecycle: 'ready_for_project',
  source_message_ids: [],
  preferred_project_agent_identity_id: null,
  project_id: null,
  handoff_id: null,
  failure_reason: null,
  version: 4,
  created_at: '2026-08-13T00:00:00Z',
  updated_at: '2026-08-13T00:00:00Z',
} as unknown as ProductGenesisSession

type CharterFixture = NonNullable<ProductGenesisCharterResponse['charter']>

const projectCharterFixture = {
  id: 'charter-1',
  genesis_session_id: 'genesis-1',
  project_id: null,
  state: 'legacy_unverified',
  project_mode: 'compact',
  maturity: 'mvp',
  current_draft_revision_id: revision.id,
  current_approved_revision_id: null,
  version: 4,
  created_at: '2026-08-13T00:00:00Z',
  updated_at: '2026-08-13T00:00:00Z',
} as unknown as CharterFixture

function charterResponse(
  charter: CharterFixture | null,
  approval: ProjectCharterApproval | null = null,
): ProductGenesisCharterResponse {
  return {
    charter,
    revisions: [revision],
    current_draft_revision: revision,
    current_approved_revision: null,
    approval,
    selected_project_agent: {
      identity_id: 'identity-1',
      display_name: 'Project Agent',
      profile_revision_id: 'profile-r2',
      operating_skill_revision: 'forge.project.orchestration/v1',
      policy_digest: 'policy-digest-1',
    },
  } as unknown as ProductGenesisCharterResponse
}

function renderExpandedCard() {
  render(<ProductGenesisCharterCard />)
  fireEvent.click(screen.getByRole('button', { name: /Project Charter/ }))
}

describe('ProductGenesisControls', () => {
  beforeEach(() => {
    state.active = {
      data: { session: activeSession },
      isLoading: false,
      isError: false,
      refetch: vi.fn(),
    }
    state.charter = {
      data: charterResponse(projectCharterFixture),
      isLoading: false,
      isError: false,
      refetch: vi.fn(),
    }
    state.approval.mockReset()
    state.create.mockReset()
    state.navigate.mockReset()
  })

  it('keeps approval unavailable until the backend publishes a real Charter', () => {
    state.charter = {
      data: charterResponse(null),
      isLoading: false,
      isError: false,
      refetch: vi.fn(),
    }

    renderExpandedCard()

    expect(screen.getByText('Approval unavailable · no exact revision')).toBeTruthy()
    expect(screen.queryByRole('button', { name: 'Approve exact Charter revision' })).toBeNull()
    expect(screen.queryByRole('button', { name: 'Approve this revision' })).toBeNull()
  })

  it('requires the explicit exact-revision approval action', () => {
    renderExpandedCard()

    const approvalButton = screen.getByRole('button', { name: 'Approve exact Charter revision' })
    expect((approvalButton as HTMLButtonElement).disabled).toBe(false)
    expect(screen.queryByText('Ready for Project')).toBeNull()
    expect(screen.queryByText(/No Project exists yet/)).toBeNull()
  })

  it('shows the atomic create action only after an active approval receipt exists', () => {
    const approval = {
      id: 'approval-1',
      state: 'active',
      charter_revision_id: revision.id,
      charter_content_digest: revision.content_digest,
      charter_render_digest: revision.render_digest,
    } as unknown as ProjectCharterApproval
    state.charter = {
      data: charterResponse(projectCharterFixture, approval),
      isLoading: false,
      isError: false,
      refetch: vi.fn(),
    }

    renderExpandedCard()

    expect(screen.getByText(/No Project exists yet/)).toBeTruthy()
    expect(
      (screen.getByRole('button', { name: 'Create Project and hand off' }) as HTMLButtonElement)
        .disabled,
    ).toBe(false)
  })

  it('sends the displayed revision and both digests when approval is clicked', async () => {
    state.approval.mockResolvedValueOnce({
      id: 'approval-1',
      state: 'active',
      charter_revision_id: revision.id,
      charter_content_digest: revision.content_digest,
      charter_render_digest: revision.render_digest,
    })
    renderExpandedCard()

    fireEvent.click(screen.getByRole('button', { name: 'Approve exact Charter revision' }))

    await waitFor(() => expect(state.approval).toHaveBeenCalledTimes(1))
    const call = state.approval.mock.calls[0][0]
    expect(call.revisionId).toBe(revision.id)
    expect(call.input.content_digest).toBe(revision.content_digest)
    expect(call.input.render_digest).toBe(revision.render_digest)
    expect(call.input.expected_charter_version).toBe(4)
    expect(call.input.selected_project_agent_operating_skill_revision).toBe(
      'forge.project.orchestration/v1',
    )
    expect(call.input.expected_project_version).toBeUndefined()
  })

  it('reuses the exact approval envelope when a request is retried', async () => {
    state.approval
      .mockRejectedValueOnce(new Error('temporary network failure'))
      .mockResolvedValueOnce({
        id: 'approval-1',
        state: 'active',
        charter_revision_id: revision.id,
        charter_content_digest: revision.content_digest,
        charter_render_digest: revision.render_digest,
      })

    renderExpandedCard()
    const approvalButton = screen.getByRole('button', { name: 'Approve exact Charter revision' })

    fireEvent.click(approvalButton)
    await waitFor(() => expect(state.approval).toHaveBeenCalledTimes(1))
    fireEvent.click(approvalButton)
    await waitFor(() => expect(state.approval).toHaveBeenCalledTimes(2))

    const first = state.approval.mock.calls[0][0]
    const second = state.approval.mock.calls[1][0]
    expect(second.input.mutation.idempotency_key).toBe(first.input.mutation.idempotency_key)
    expect(second.input.mutation.authorization.event_id).toBe(
      first.input.mutation.authorization.event_id,
    )
    expect(second.input.expected_project_version).toBeUndefined()
  })

  it('typed approval approves the current revision and then creates the Project', async () => {
    state.approval.mockResolvedValueOnce({
      id: 'approval-1',
      state: 'active',
      charter_revision_id: revision.id,
      charter_content_digest: revision.content_digest,
      charter_render_digest: revision.render_digest,
    })
    state.create.mockResolvedValueOnce({ project_id: 'project-1' })

    const handle = createRef<ProductGenesisCharterCardHandle>()
    render(<ProductGenesisCharterCard ref={handle} />)

    expect(handle.current?.canApproveTyped()).toBe(true)
    await act(async () => {
      await handle.current!.approveTyped()
    })

    expect(state.approval).toHaveBeenCalledTimes(1)
    expect(state.approval.mock.calls[0][0].revisionId).toBe(revision.id)
    expect(state.create).toHaveBeenCalledTimes(1)
    expect(state.create.mock.calls[0][0].approval_id).toBe('approval-1')
    // 8.4.4: the committed Project is selected and the user is taken to its
    // Project Agent handoff automatically, rather than leaving them to
    // notice and click a "Continue" link.
    expect(state.navigate).toHaveBeenCalledWith({
      to: '/projects/$projectId/chat',
      params: { projectId: 'project-1' },
    })
  })

  it('typed approval reports a clear error when nothing is approvable', async () => {
    state.charter = {
      data: charterResponse(null),
      isLoading: false,
      isError: false,
      refetch: vi.fn(),
    }

    const handle = createRef<ProductGenesisCharterCardHandle>()
    render(<ProductGenesisCharterCard ref={handle} />)

    expect(handle.current?.canApproveTyped()).toBe(false)
    await expect(handle.current!.approveTyped()).rejects.toThrow(
      /No proposed Charter revision is awaiting approval/,
    )
    expect(state.approval).not.toHaveBeenCalled()
    expect(state.create).not.toHaveBeenCalled()
  })

  it('shows conflict authority and expected/current revisions when approval races', async () => {
    state.approval.mockRejectedValueOnce(
      new ApiError(
        JSON.stringify({
          code: 'charter_conflict',
          message: 'Charter changed',
          details: {
            authority_domain: 'project_charter',
            expected_revision: 'charter-revision-1',
            current_revision: 'charter-revision-2',
            expected_digest: 'content-digest-1',
            current_digest: 'content-digest-2',
          },
        }),
        409,
      ),
    )

    renderExpandedCard()
    fireEvent.click(screen.getByRole('button', { name: 'Approve exact Charter revision' }))

    await waitFor(() => expect(screen.getByText('project_charter')).toBeTruthy())
    expect(screen.getByText('Expected revision')).toBeTruthy()
    expect(screen.getByText('charter-revision-2')).toBeTruthy()
    expect(screen.getAllByText('content-digest-1').length).toBeGreaterThan(0)
    expect(screen.getByText('content-digest-2')).toBeTruthy()
  })
})
