import { fireEvent, render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { useAuthStore } from '@/stores/auth'
import type { PendingDecisionSummary } from '@/types/generated'

import { DecisionCandidateCard } from './DecisionCandidateCard'
import {
  useApproveDecisionCandidateMutation,
  useRejectDecisionCandidateMutation,
} from './decision-hooks'

vi.mock('./decision-hooks', () => ({
  useApproveDecisionCandidateMutation: vi.fn(),
  useRejectDecisionCandidateMutation: vi.fn(),
}))

function baseCandidate(overrides: Partial<PendingDecisionSummary> = {}): PendingDecisionSummary {
  return {
    id: 'candidate-1',
    project_id: 'project-1',
    lifecycle: 'proposed',
    version: 1n,
    question: 'Which implementation choice should the Project use?',
    options: ['option-a', 'option-b'],
    recommendation: 'option-a',
    rationale: 'The bounded implementation choice fits the approved envelope.',
    decision_class: 'project_implementation',
    affected_records: {
      affected_artifact_refs: [],
      affected_task_ids: ['task-1'],
      affected_milestone_ids: [],
    },
    proposed_by: { kind: 'agent', id: 'agent-1', display_name: null },
    required_principal: 'user',
    validity: 'valid',
    invalid_reason: null,
    approve_target: {
      method: 'POST',
      path: '/api/v1/projects/project-1/decisions/candidates/candidate-1/approve',
    },
    reject_target: {
      method: 'POST',
      path: '/api/v1/projects/project-1/decisions/candidates/candidate-1/reject',
    },
    created_at: '2026-08-24T00:00:00Z',
    updated_at: '2026-08-24T00:00:00Z',
    ...overrides,
  } as unknown as PendingDecisionSummary
}

function mockMutations(
  approveOverrides: Partial<ReturnType<typeof useApproveDecisionCandidateMutation>> = {},
  rejectOverrides: Partial<ReturnType<typeof useRejectDecisionCandidateMutation>> = {},
) {
  const approveMutate = vi.fn()
  const rejectMutate = vi.fn()
  vi.mocked(useApproveDecisionCandidateMutation).mockReturnValue({
    mutate: approveMutate,
    isPending: false,
    isError: false,
    error: null,
    ...approveOverrides,
  } as unknown as ReturnType<typeof useApproveDecisionCandidateMutation>)
  vi.mocked(useRejectDecisionCandidateMutation).mockReturnValue({
    mutate: rejectMutate,
    isPending: false,
    isError: false,
    error: null,
    ...rejectOverrides,
  } as unknown as ReturnType<typeof useRejectDecisionCandidateMutation>)
  return { approveMutate, rejectMutate }
}

describe('DecisionCandidateCard', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    useAuthStore.setState({
      user: { id: 'user-1', display_name: 'Ada' } as unknown as ReturnType<
        typeof useAuthStore.getState
      >['user'],
    })
    mockMutations()
  })

  it('renders an empty-state message when there are no pending candidates', () => {
    render(<DecisionCandidateCard projectId="project-1" candidates={[]} />)
    expect(screen.getByText('No pending decision proposals are recorded.')).toBeTruthy()
  })

  it('renders question, alternatives, recommendation, rationale, affected records, and authority', () => {
    render(<DecisionCandidateCard projectId="project-1" candidates={[baseCandidate()]} />)

    expect(screen.getByText('Which implementation choice should the Project use?')).toBeTruthy()
    expect(screen.getByText('option-a · option-b')).toBeTruthy()
    expect(screen.getByText('option-a')).toBeTruthy()
    expect(
      screen.getByText('The bounded implementation choice fits the approved envelope.'),
    ).toBeTruthy()
    expect(screen.getByText(/task task-1/)).toBeTruthy()
    expect(screen.getByText(/required principal: user/)).toBeTruthy()
    expect(screen.getByRole('button', { name: 'Approve' })).toBeTruthy()
  })

  it('marks a malformed candidate non-approvable with its exact reason, but keeps Reject available', () => {
    render(
      <DecisionCandidateCard
        projectId="project-1"
        candidates={[
          baseCandidate({
            options: [],
            validity: 'malformed',
            invalid_reason: 'it does not have at least two distinct options',
            approve_target: null,
          }),
        ]}
      />,
    )

    expect(
      screen.getAllByText(/it does not have at least two distinct options/).length,
    ).toBeGreaterThan(0)
    const approveButton = screen.queryByRole('button', { name: 'Approve' })
    expect(approveButton).not.toBeNull()
    expect(approveButton?.hasAttribute('disabled')).toBe(true)
    expect(screen.getByRole('button', { name: 'Reject' })).toBeTruthy()
  })

  it('approves with the exact version, target path, and user authorization', () => {
    const { approveMutate } = mockMutations()
    render(<DecisionCandidateCard projectId="project-1" candidates={[baseCandidate()]} />)

    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))

    expect(approveMutate).toHaveBeenCalledTimes(1)
    const [call] = approveMutate.mock.calls[0] as [
      { approveTargetPath: string; input: Record<string, unknown> },
    ]
    expect(call.approveTargetPath).toBe(
      '/api/v1/projects/project-1/decisions/candidates/candidate-1/approve',
    )
    const mutation = call.input.mutation as {
      expected_version: number
      authorization: { principal: { kind: string; id: string }; action: string }
      idempotency_key: string
    }
    expect(mutation.expected_version).toBe(1)
    expect(mutation.authorization.principal).toEqual({
      kind: 'user',
      id: 'user-1',
      display_name: 'Ada',
    })
    expect(mutation.authorization.action).toBe('project.decision.candidate.approve')
    expect(mutation.idempotency_key.length).toBeGreaterThan(0)
  })

  it('disables Reject submission until a reason is entered, then rejects with it', () => {
    const { rejectMutate } = mockMutations()
    render(<DecisionCandidateCard projectId="project-1" candidates={[baseCandidate()]} />)

    fireEvent.click(screen.getByRole('button', { name: 'Reject' }))
    const submit = screen.getByRole('button', { name: 'Reject' })
    expect(submit.hasAttribute('disabled')).toBe(true)

    fireEvent.change(screen.getByLabelText('Rejection reason'), {
      target: { value: 'This option exceeds the approved implementation envelope.' },
    })
    expect(submit.hasAttribute('disabled')).toBe(false)
    fireEvent.click(submit)

    expect(rejectMutate).toHaveBeenCalledTimes(1)
    const [call] = rejectMutate.mock.calls[0] as [
      { rejectTargetPath: string; input: Record<string, unknown> },
    ]
    expect(call.rejectTargetPath).toBe(
      '/api/v1/projects/project-1/decisions/candidates/candidate-1/reject',
    )
    expect(call.input.reason).toBe('This option exceeds the approved implementation envelope.')
  })

  it('removes a candidate from the list once its mutation succeeds', () => {
    const approveMutate = vi.fn(
      (_input: unknown, options?: { onSuccess?: (result: unknown) => void }) => {
        options?.onSuccess?.({})
      },
    )
    mockMutations({ mutate: approveMutate } as unknown as Partial<
      ReturnType<typeof useApproveDecisionCandidateMutation>
    >)
    render(<DecisionCandidateCard projectId="project-1" candidates={[baseCandidate()]} />)

    fireEvent.click(screen.getByRole('button', { name: 'Approve' }))

    expect(screen.getByText('No pending decision proposals are recorded.')).toBeTruthy()
  })
})
