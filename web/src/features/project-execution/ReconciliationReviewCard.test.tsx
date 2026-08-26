import { fireEvent, render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { useAuthStore } from '@/stores/auth'
import type { ProjectReconciliation, ReconciliationReplacementRef } from '@/types/generated'

import { ReconciliationReviewCard } from './ReconciliationReviewCard'
import { useProjectReconciliationsQuery, useResolveProjectReconciliationMutation } from './reconciliation-hooks'

vi.mock('./reconciliation-hooks', () => ({
  useProjectReconciliationsQuery: vi.fn(),
  useResolveProjectReconciliationMutation: vi.fn(),
}))

function baseReconciliation(
  overrides: Partial<ProjectReconciliation> = {},
): ProjectReconciliation {
  return {
    id: 'reconciliation-1',
    project_id: 'project-1',
    conflict: {
      id: 'conflict-1',
      domain: 'execution',
      governing: {
        record_type: 'execution_baseline',
        record_id: 'baseline-1',
        record_revision: 'revision-3',
        record_digest: 'digest-governing',
      },
      conflicting: {
        record_type: 'task',
        record_id: 'task-1',
        record_revision: '4',
        record_digest: 'digest-conflicting',
      },
      affected_paths: ['/plan/items/0/outcome'],
      conflict_code: 'adaptive_task_boundary_crossed',
      description: 'The adaptive split changes an approved outcome boundary.',
      detected_by_type: 'system',
      detected_by_id: 'task-service',
      created_at: '2026-08-24T00:00:00Z',
    },
    affected: {
      record_type: 'task',
      record_id: 'task-1',
      record_revision: '4',
      record_digest: 'digest-conflicting',
    },
    governing: {
      record_type: 'execution_baseline',
      record_id: 'baseline-1',
      record_revision: 'revision-3',
      record_digest: 'digest-governing',
    },
    state: 'required',
    required_principal: 'user',
    allowed_actions: ['retained', 'revised', 'cancelled', 'superseded', 'invalidated'],
    suggested_replacement_ref: null,
    resolution: null,
    version: 1n,
    created_at: '2026-08-24T00:00:00Z',
    updated_at: '2026-08-24T00:00:00Z',
    ...overrides,
  } as unknown as ProjectReconciliation
}

function mockQuery(items: ProjectReconciliation[]) {
  vi.mocked(useProjectReconciliationsQuery).mockReturnValue({
    data: { items, next_cursor: null, has_more: false },
    isLoading: false,
    isError: false,
    error: null,
  } as unknown as ReturnType<typeof useProjectReconciliationsQuery>)
}

function mockMutation(overrides: Partial<ReturnType<typeof useResolveProjectReconciliationMutation>> = {}) {
  const mutate = vi.fn()
  vi.mocked(useResolveProjectReconciliationMutation).mockReturnValue({
    mutate,
    isPending: false,
    isError: false,
    error: null,
    ...overrides,
  } as unknown as ReturnType<typeof useResolveProjectReconciliationMutation>)
  return mutate
}

describe('ReconciliationReviewCard', () => {
  beforeEach(() => {
    vi.clearAllMocks()
    useAuthStore.setState({
      user: { id: 'user-1', display_name: 'Ada' } as unknown as ReturnType<
        typeof useAuthStore.getState
      >['user'],
    })
    mockMutation()
  })

  it('renders nothing when no reconciliation is pending', () => {
    mockQuery([])
    const { container } = render(<ReconciliationReviewCard projectId="project-1" />)
    expect(container.firstChild).toBeNull()
  })

  it('renders cause, governing/affected records, impact, and required principal', () => {
    mockQuery([baseReconciliation()])
    render(<ReconciliationReviewCard projectId="project-1" />)

    expect(
      screen.getByText('The adaptive split changes an approved outcome boundary.'),
    ).toBeTruthy()
    expect(screen.getByText(/adaptive_task_boundary_crossed/)).toBeTruthy()
    expect(screen.getByText(/Execution Baseline baseline-1 @ revision revision-3/)).toBeTruthy()
    expect(screen.getByText(/Task task-1 @ revision 4/)).toBeTruthy()
    expect(screen.getByText('/plan/items/0/outcome')).toBeTruthy()
    expect(screen.getByText(/required principal: user/)).toBeTruthy()
  })

  it('disables Resolve until a reason is entered', () => {
    mockQuery([baseReconciliation()])
    render(<ReconciliationReviewCard projectId="project-1" />)

    const button = screen.getByRole('button', { name: 'Resolve' })
    expect(button.hasAttribute('disabled')).toBe(true)

    fireEvent.change(screen.getByLabelText('Reason'), {
      target: { value: 'The Task boundary change is approved after review.' },
    })
    expect(button.hasAttribute('disabled')).toBe(false)
  })

  it('requires a replacement type and id before Resolve is enabled for a revised action', () => {
    mockQuery([baseReconciliation()])
    render(<ReconciliationReviewCard projectId="project-1" />)

    fireEvent.change(screen.getByRole('combobox'), { target: { value: 'revised' } })
    fireEvent.change(screen.getByLabelText('Reason'), {
      target: { value: 'A corrected baseline revision now governs this Task.' },
    })
    const button = screen.getByRole('button', { name: 'Resolve' })
    expect(button.hasAttribute('disabled')).toBe(true)

    fireEvent.change(screen.getByLabelText('Replacement type'), {
      target: { value: 'execution_baseline' },
    })
    fireEvent.change(screen.getByLabelText('Replacement id'), {
      target: { value: 'baseline-2' },
    })
    expect(button.hasAttribute('disabled')).toBe(false)
  })

  it('submits the resolve mutation with the expected version, action, reason, and user authorization', () => {
    mockQuery([baseReconciliation()])
    const mutate = mockMutation()
    render(<ReconciliationReviewCard projectId="project-1" />)

    fireEvent.change(screen.getByLabelText('Reason'), {
      target: { value: 'The Charter remains authoritative after review.' },
    })
    fireEvent.click(screen.getByRole('button', { name: 'Resolve' }))

    expect(mutate).toHaveBeenCalledTimes(1)
    const [call] = mutate.mock.calls[0] as [
      { reconciliationId: string; input: Record<string, unknown> },
    ]
    expect(call.reconciliationId).toBe('reconciliation-1')
    expect(call.input.action).toBe('retained')
    expect(call.input.reason).toBe('The Charter remains authoritative after review.')
    expect(call.input.replacement_ref).toBeNull()
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
    expect(mutation.authorization.action).toBe('project.reconciliation.resolve')
    expect(mutation.idempotency_key.length).toBeGreaterThan(0)
  })

  it('locks an invalid active baseline resolution to the server-verified approved successor', () => {
    mockQuery([
      baseReconciliation({
        conflict: {
          ...baseReconciliation().conflict,
          conflict_code: 'invalid_active_baseline',
          description: 'The active baseline contains task.propose and task.adaptive.',
        },
        affected: {
          record_type: 'execution_baseline_revision',
          record_id: 'invalid-revision',
          record_revision: '2',
          record_digest: 'invalid-digest',
        },
        allowed_actions: ['revised'],
        suggested_replacement_ref: {
          record_type: 'execution_baseline_revision',
          record_id: 'approved-successor',
          record_revision: '4',
        },
      }),
    ])
    const mutate = mockMutation()
    render(<ReconciliationReviewCard projectId="project-1" />)

    expect(screen.getByRole('combobox')).toHaveProperty('value', 'revised')
    expect(screen.getByLabelText('Replacement type')).toHaveProperty(
      'value',
      'execution_baseline_revision',
    )
    expect(screen.getByLabelText('Replacement id')).toHaveProperty(
      'value',
      'approved-successor',
    )
    expect(screen.getByLabelText('Replacement id').hasAttribute('readonly')).toBe(true)
    fireEvent.change(screen.getByLabelText('Reason'), {
      target: { value: 'Activate the exact approved correction.' },
    })
    fireEvent.click(screen.getByRole('button', { name: 'Resolve' }))

    const [call] = mutate.mock.calls[0] as [
      { input: { action: string; replacement_ref: ReconciliationReplacementRef } },
    ]
    expect(call.input.action).toBe('revised')
    expect(call.input.replacement_ref).toEqual({
      record_type: 'execution_baseline_revision',
      record_id: 'approved-successor',
      record_revision: '4',
    })
  })

  it('shows the exact resolution result after a successful resolve', () => {
    mockQuery([baseReconciliation()])
    const resolved = baseReconciliation({
      state: 'retained',
      allowed_actions: [],
      resolution: {
        id: 'resolution-1',
        action: 'retained',
        principal: { kind: 'user', id: 'user-1', display_name: 'Ada' },
        reason: 'The Charter remains authoritative after review.',
        replacement_ref: null,
        occurred_at: '2026-08-24T01:00:00Z',
      },
      version: 2n,
    } as unknown as Partial<ProjectReconciliation>)
    const mutate = vi.fn((_input: unknown, options?: { onSuccess?: (result: unknown) => void }) => {
      options?.onSuccess?.(resolved)
    })
    mockMutation({ mutate } as unknown as Partial<ReturnType<typeof useResolveProjectReconciliationMutation>>)
    render(<ReconciliationReviewCard projectId="project-1" />)

    fireEvent.change(screen.getByLabelText('Reason'), {
      target: { value: 'The Charter remains authoritative after review.' },
    })
    fireEvent.click(screen.getByRole('button', { name: 'Resolve' }))

    expect(screen.getByText('Recently resolved')).toBeTruthy()
    expect(screen.getByText(/By Ada at/)).toBeTruthy()
    expect(
      screen.getByText('Reason: The Charter remains authoritative after review.'),
    ).toBeTruthy()
  })
})
