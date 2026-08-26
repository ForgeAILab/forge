import { render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { apiFetch } from '@/api/client'
import { useProjectQuery } from '@/api/hooks'
import { useProjectExecutionSetupQuery } from '@/features/project-execution/hooks'
import { useAuthStore } from '@/stores/auth'

import { BaselineApprovalCard } from './BaselineApprovalCard'

// F13: the web mapped any approval/activation 409/412 to a stale-baseline
// failure message even when the baseline path had already committed and
// dispatched the Task. These tests drive the component the same way a user
// does -- through its rendered button -- to prove the fix: one atomic
// approve-and-activate call, a stable idempotency key reused on retry, a
// conflict that resolves to success when the exact revision is already
// active, and a newer draft never presented as failure evidence.

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

vi.mock('@/features/project-execution/hooks', () => ({
  useProjectExecutionSetupQuery: vi.fn(),
}))

const proposedRevision = {
  id: 'revision-1',
  baseline_id: 'baseline-1',
  project_id: 'project-1',
  revision_number: 2,
  base_revision_id: 'revision-0',
  lifecycle: 'proposed' as const,
  schema_version: 'forge.execution-baseline/v1',
  content: {},
  rendered_view: '# Plan\nDo the thing.',
  render_version: 'forge.execution-baseline-render/v1',
  content_digest: 'content-digest-0123456789abcdef',
  render_digest: 'render-digest-0123456789abcdef',
  provenance: null,
  created_at: '2026-08-24T10:00:00Z',
  activated_at: null,
}

function baselineResponse(overrides: Record<string, unknown> = {}) {
  return {
    baseline: {
      id: 'baseline-1',
      project_id: 'project-1',
      current_revision_id: null,
      lifecycle: 'proposed',
      version: 3,
    },
    current_revision: null,
    proposed_revision: proposedRevision,
    approval: null,
    ...overrides,
  }
}

function renderCard(node: ReactNode) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false, gcTime: 0 } },
  })
  return render(<QueryClientProvider client={queryClient}>{node}</QueryClientProvider>)
}

function approveAndActivateCall() {
  return vi
    .mocked(apiFetch)
    .mock.calls.find(([path]) => String(path).endsWith('/approve-and-activate'))
}

describe('BaselineApprovalCard', () => {
  beforeEach(() => {
    vi.mocked(useProjectQuery).mockReturnValue({
      data: { id: 'project-1', version: 7 },
      isLoading: false,
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any)
    vi.mocked(useProjectExecutionSetupQuery).mockReturnValue({
      data: { execution_gate: 'ready' },
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

  it('offers the atomic "Approve plan & start work" gesture for a proposed revision', async () => {
    vi.mocked(apiFetch).mockResolvedValue(baselineResponse())

    renderCard(<BaselineApprovalCard projectId="project-1" />)

    expect(await screen.findByRole('button', { name: 'Approve plan & start work' })).toBeTruthy()
  })

  it('commits through the one atomic route and reuses the same idempotency key on retry', async () => {
    vi.mocked(apiFetch).mockImplementation(async (path: string) => {
      if (path.endsWith('/approve-and-activate')) {
        throw new Error('network down')
      }
      return baselineResponse()
    })

    renderCard(<BaselineApprovalCard projectId="project-1" />)
    const button = await screen.findByRole('button', { name: 'Approve plan & start work' })
    button.click()
    await screen.findByRole('alert')
    const firstCall = approveAndActivateCall()
    expect(firstCall).toBeTruthy()
    expect(firstCall?.[0]).toBe(
      '/projects/project-1/execution-baseline/baseline-1/revisions/revision-1/approve-and-activate',
    )
    const firstBody = JSON.parse(String((firstCall?.[1] as { body: string }).body))
    expect(firstBody.revision_id).toBe('revision-1')
    expect(firstBody.expected_baseline_version).toBe(3)
    expect(firstBody.mutation.expected_version).toBe(7)
    expect(firstBody.mutation.authorization.action).toBe(
      'project.execution_baseline.approve_and_activate',
    )
    const firstKey = firstBody.mutation.idempotency_key
    expect(typeof firstKey).toBe('string')
    expect(firstKey.length).toBeGreaterThan(0)

    // The client never saw a response and the button is retried. This must
    // reuse the exact same idempotency key so a lost response can only ever
    // replay the committed outcome (D18/8.3.1) -- not silently mint a second,
    // unrelated command.
    vi.mocked(apiFetch).mockImplementation(async (path: string) => {
      if (path.endsWith('/approve-and-activate')) {
        return {
          baseline_id: 'baseline-1',
          revision_id: 'revision-1',
          approval_id: 'approval-1',
          content_digest: proposedRevision.content_digest,
          render_digest: proposedRevision.render_digest,
          projection: baselineResponse({
            baseline: {
              id: 'baseline-1',
              project_id: 'project-1',
              current_revision_id: 'revision-1',
              lifecycle: 'active',
              version: 4,
            },
            current_revision: { ...proposedRevision, lifecycle: 'active' },
            proposed_revision: null,
          }),
          refresh_required: false,
        }
      }
      return baselineResponse()
    })
    button.click()

    await waitFor(() => {
      const calls = vi
        .mocked(apiFetch)
        .mock.calls.filter(([path]) => String(path).endsWith('/approve-and-activate'))
      expect(calls.length).toBe(2)
    })
    const secondCall = approveAndActivateCall()
    const bodies = vi
      .mocked(apiFetch)
      .mock.calls.filter(([path]) => String(path).endsWith('/approve-and-activate'))
      .map(([, init]) => JSON.parse(String((init as { body: string }).body)))
    expect(bodies).toHaveLength(2)
    expect(bodies[0].mutation.idempotency_key).toBe(firstKey)
    expect(bodies[1].mutation.idempotency_key).toBe(firstKey)
    void secondCall
  })

  it('renders success when a conflict resolves because the exact revision is already active', async () => {
    let baselineCallCount = 0
    vi.mocked(apiFetch).mockImplementation(async (path: string) => {
      if (path.endsWith('/approve-and-activate')) {
        const { ApiError } = await import('@/api/client')
        throw new ApiError('conflict', 409)
      }
      baselineCallCount += 1
      if (baselineCallCount === 1) {
        return baselineResponse()
      }
      // The server actually committed: a follow-up read shows the exact
      // requested revision is now the active baseline.
      return baselineResponse({
        baseline: {
          id: 'baseline-1',
          project_id: 'project-1',
          current_revision_id: 'revision-1',
          lifecycle: 'active',
          version: 4,
        },
        current_revision: { ...proposedRevision, lifecycle: 'active' },
        proposed_revision: null,
      })
    })

    renderCard(<BaselineApprovalCard projectId="project-1" />)
    const button = await screen.findByRole('button', { name: 'Approve plan & start work' })
    button.click()

    await waitFor(() => expect(screen.queryByRole('alert')).toBeNull())
    expect(approveAndActivateCall()).toBeTruthy()
  })

  it('still reports a genuine conflict when the exact revision never became active', async () => {
    vi.mocked(apiFetch).mockImplementation(async (path: string) => {
      if (path.endsWith('/approve-and-activate')) {
        const { ApiError } = await import('@/api/client')
        throw new ApiError('conflict', 409)
      }
      // The refreshed baseline still shows the old state -- this command
      // truly did not commit.
      return baselineResponse()
    })

    renderCard(<BaselineApprovalCard projectId="project-1" />)
    const button = await screen.findByRole('button', { name: 'Approve plan & start work' })
    button.click()

    expect(await screen.findByRole('alert')).toHaveProperty(
      'textContent',
      expect.stringContaining('changed while this approval was open'),
    )
  })

  it('labels a newer draft beside an active baseline as unapproved future work, not failure', async () => {
    vi.mocked(apiFetch).mockResolvedValue(
      baselineResponse({
        baseline: {
          id: 'baseline-1',
          project_id: 'project-1',
          current_revision_id: 'revision-1',
          lifecycle: 'active',
          version: 4,
        },
        current_revision: { ...proposedRevision, lifecycle: 'active' },
        proposed_revision: { ...proposedRevision, id: 'revision-2', lifecycle: 'draft' },
      }),
    )

    renderCard(<BaselineApprovalCard projectId="project-1" />)

    expect(await screen.findByText('Draft — not active')).toBeTruthy()
    expect(screen.queryByRole('alert')).toBeNull()
    expect(screen.queryByRole('button', { name: 'Approve plan & start work' })).toBeNull()
  })

  it('renders nothing while reconciliation is required, even with a newer draft present', async () => {
    vi.mocked(useProjectExecutionSetupQuery).mockReturnValue({
      data: { execution_gate: 'reconciliation_required' },
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any)
    vi.mocked(apiFetch).mockResolvedValue(
      baselineResponse({
        baseline: {
          id: 'baseline-1',
          project_id: 'project-1',
          current_revision_id: 'revision-1',
          lifecycle: 'active',
          version: 4,
        },
        current_revision: { ...proposedRevision, lifecycle: 'active' },
        proposed_revision: { ...proposedRevision, id: 'revision-2', lifecycle: 'draft' },
      }),
    )

    const { container } = renderCard(<BaselineApprovalCard projectId="project-1" />)

    await waitFor(() => expect(apiFetch).toHaveBeenCalled())
    expect(container.textContent).toBe('')
  })

  it('shows and approves a typed correction without activating the preserved invalid baseline', async () => {
    vi.mocked(useProjectExecutionSetupQuery).mockReturnValue({
      data: { execution_gate: 'reconciliation_required' },
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any)
    vi.mocked(apiFetch).mockImplementation(async (path: string) => {
      if (path.endsWith('/approve')) {
        return baselineResponse()
      }
      return baselineResponse({
        baseline: {
          id: 'baseline-1',
          project_id: 'project-1',
          current_revision_id: 'invalid-revision',
          lifecycle: 'active',
          version: 8,
        },
        current_revision: null,
        proposed_revision: { ...proposedRevision, id: 'corrected-revision' },
        integrity_issue: {
          revision_id: 'invalid-revision',
          baseline_id: 'baseline-1',
          field_path: 'adaptive_envelope.allowed_task_operations',
          invalid_values: ['task.propose', 'task.adaptive'],
          diagnostic: 'Unsupported historical adaptive operations.',
          successor_revision_id: 'audit-draft',
          conflict_id: 'conflict-1',
          reconciliation_id: 'reconciliation-1',
        },
      })
    })

    renderCard(<BaselineApprovalCard projectId="project-1" />)
    const button = await screen.findByRole('button', { name: 'Approve corrected plan' })
    button.click()

    await waitFor(() => {
      expect(
        vi
          .mocked(apiFetch)
          .mock.calls.some(([path]) => String(path).endsWith('/corrected-revision/approve')),
      ).toBe(true)
    })
    const approveCall = vi
      .mocked(apiFetch)
      .mock.calls.find(([path]) => String(path).endsWith('/corrected-revision/approve'))
    const body = JSON.parse(String((approveCall?.[1] as { body: string }).body))
    expect(body.mutation.expected_version).toBe(8)
    expect(body.expected_project_version).toBe(7)
    expect(body.mutation.authorization.action).toBe('project.execution_baseline.approve')
    expect(approveAndActivateCall()).toBeUndefined()
  })

  it('explains that an integrity-audit correction draft is not active authority', async () => {
    vi.mocked(useProjectExecutionSetupQuery).mockReturnValue({
      data: { execution_gate: 'reconciliation_required' },
      // eslint-disable-next-line @typescript-eslint/no-explicit-any
    } as any)
    vi.mocked(apiFetch).mockResolvedValue(
      baselineResponse({
        baseline: {
          id: 'baseline-1',
          project_id: 'project-1',
          current_revision_id: 'invalid-revision',
          lifecycle: 'active',
          version: 7,
        },
        current_revision: null,
        proposed_revision: { ...proposedRevision, id: 'audit-draft', lifecycle: 'draft' },
        integrity_issue: {
          revision_id: 'invalid-revision',
          baseline_id: 'baseline-1',
          field_path: 'adaptive_envelope.allowed_task_operations',
          invalid_values: ['task.propose', 'task.adaptive'],
          diagnostic: 'Unsupported historical adaptive operations.',
          successor_revision_id: 'audit-draft',
          conflict_id: 'conflict-1',
          reconciliation_id: 'reconciliation-1',
        },
      }),
    )

    renderCard(<BaselineApprovalCard projectId="project-1" />)

    expect(await screen.findByText('Active plan repair · draft only')).toBeTruthy()
    expect(screen.getByText(/task\.propose, task\.adaptive/)).toBeTruthy()
    expect(screen.queryByRole('button', { name: 'Approve corrected plan' })).toBeNull()
  })

  // F19: the Review dialog used to render only `revision.rendered_view` as a
  // raw preformatted JSON blob. It must now render semantic sections as the
  // primary view, with raw JSON reachable only through a collapsed
  // technical-details disclosure.
  it('renders the Review dialog as semantic sections, not a raw JSON blob', async () => {
    const richRevision = {
      ...proposedRevision,
      content: {
        charter_revision: {
          artifact_id: 'charter-1',
          revision_id: 'charter-revision-1',
          content_digest: 'digest-1',
          render_version: null,
          render_digest: null,
        },
        document_revisions: [],
        plan_item_ids: ['plan-1'],
        milestone_ids: ['milestone-1'],
        milestone_definition_revision_ids: ['milestone-def-1'],
        primary_milestone_id: 'milestone-1',
        release_policy_revision: 'policy-1',
        release_policy_digest: 'policy-digest-1',
        release_policy: {
          schema_version: 'forge.execution-baseline-release-policy/v1',
          revision: 'policy-1',
          required_check_definition_revisions: [],
          reviewer_independence_rules: [],
          manual_attestation_rules: [],
          waiver_rules: [],
          evidence_kinds: [],
          evidence_contexts: [],
          evidence_freshness_rules: [],
          dependency_rules: [],
          stale_input_rules: [],
          forbidden_side_effects: [],
          known_issue_rules: [],
          correction_rules: [],
          purge_rules: [],
        },
        acceptance_evidence_matrix: [
          {
            id: 'evidence-1',
            description: 'Filters clear on reload',
            required: true,
            evidence_kind: 'manual',
            check_definition_revision: null,
          },
        ],
        capability_classes: [],
        risk_classes: [],
        reviewer_independence_rules: [],
        elevated_operations: [],
        adaptive_envelope: {
          allowed_task_operations: ['split'],
          fixed_outcomes: ['Ship the filter UI'],
          fixed_acceptance: [],
          fixed_risk_classes: [],
          forbidden_side_effects: [],
          elevated_operations: [],
        },
        rollback_and_recovery: [],
        exclusions: [],
      },
      rendered_view: '{"plan_item_ids":["plan-1"]}',
    }
    vi.mocked(apiFetch).mockResolvedValue(baselineResponse({ proposed_revision: richRevision }))

    renderCard(<BaselineApprovalCard projectId="project-1" />)
    const reviewButton = await screen.findByRole('button', { name: 'Review' })
    reviewButton.click()

    expect(await screen.findByRole('heading', { name: 'Plan items' })).toBeTruthy()
    expect(screen.getByText('Ship the filter UI')).toBeTruthy()
    expect(screen.getByText('Filters clear on reload')).toBeTruthy()

    // The raw payload is reachable only inside the collapsed disclosure.
    const disclosure = screen.getByText('Technical details (raw JSON)').closest('details')
    expect(disclosure).toBeTruthy()
    expect((disclosure as HTMLDetailsElement).open).toBe(false)
    const rawJson = screen.getByText('{"plan_item_ids":["plan-1"]}')
    expect(rawJson.closest('details')).toBe(disclosure)
  })
})
