import { fireEvent, render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'

import { useAuthStore } from '@/stores/auth'
import type { ProjectReconciliation, ReconciliationReplacementRef } from '@/types/generated'

import { ReconciliationReviewCard } from './ReconciliationReviewCard'
import {
  useProjectReconciliationsQuery,
  useResolveProjectReconciliationMutation,
} from './reconciliation-hooks'

vi.mock('./reconciliation-hooks', () => ({
  useProjectReconciliationsQuery: vi.fn(),
  useResolveProjectReconciliationMutation: vi.fn(),
}))

function baseReconciliation(overrides: Partial<ProjectReconciliation> = {}): ProjectReconciliation {
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
      conflict_code: 'task_definition_conflict',
      description: 'The proposed Task definition conflicts with the approved plan.',
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
    refetch: vi.fn(),
  } as unknown as ReturnType<typeof useProjectReconciliationsQuery>)
}

function mockMutation(
  overrides: Partial<ReturnType<typeof useResolveProjectReconciliationMutation>> = {},
) {
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

  it('leads with plain language and keeps canonical metadata in technical details', () => {
    mockQuery([baseReconciliation()])
    render(<ReconciliationReviewCard projectId="project-1" />)

    expect(screen.getByText('Review a requested change')).toBeTruthy()
    expect(screen.getByText('Apply the proposed change to this Task.')).toBeTruthy()
    expect(screen.getByText('Technical details')).toBeTruthy()
    expect(screen.getByText(/task_definition_conflict/)).toBeTruthy()
    expect(screen.getByText(/Execution Baseline baseline-1 @ revision revision-3/)).toBeTruthy()
    expect(screen.getByText(/Task task-1 @ revision 4/)).toBeTruthy()
    expect(screen.getByText('/plan/items/0/outcome')).toBeTruthy()
  })

  it('offers only accept and reject choices', () => {
    mockQuery([baseReconciliation()])
    render(<ReconciliationReviewCard projectId="project-1" />)

    expect(screen.queryByRole('combobox')).toBeNull()
    expect(screen.getByRole('button', { name: 'Accept' })).toBeTruthy()
    expect(screen.getByRole('button', { name: 'Reject' })).toBeTruthy()
    expect(screen.queryByLabelText('Reason')).toBeNull()
  })

  it('submits a plain-language decision with a server-auditable reason and user authorization', () => {
    mockQuery([baseReconciliation()])
    const mutate = mockMutation()
    render(<ReconciliationReviewCard projectId="project-1" />)

    fireEvent.click(screen.getByRole('button', { name: 'Reject' }))

    expect(mutate).toHaveBeenCalledTimes(1)
    const [call] = mutate.mock.calls[0] as [
      { reconciliationId: string; input: Record<string, unknown> },
    ]
    expect(call.reconciliationId).toBe('reconciliation-1')
    expect(call.input.action).toBe('retained')
    expect(call.input.reason).toContain('Keep the current work')
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

  it('accepts the recommended baseline correction in one click without technical fields', () => {
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
          record_id: 'recommended-successor',
          record_revision: '5',
        },
      }),
    ])
    const mutate = mockMutation()
    render(<ReconciliationReviewCard projectId="project-1" />)

    expect(screen.getByText('Update how the Project Agent manages work')).toBeTruthy()
    expect(screen.getByText(/split, reorder, and replace in-scope Tasks/)).toBeTruthy()
    expect(screen.queryByRole('combobox')).toBeNull()
    expect(screen.queryByLabelText('Replacement id')).toBeNull()
    expect(screen.queryByLabelText('Reason')).toBeNull()
    fireEvent.click(screen.getByRole('button', { name: 'Accept' }))

    const [call] = mutate.mock.calls[0] as [
      {
        input: {
          action: string
          reason: string
          replacement_ref: ReconciliationReplacementRef
        }
      },
    ]
    expect(call.input.action).toBe('revised')
    expect(call.input.reason).toContain('default Project Agent task authority')
    expect(call.input.replacement_ref).toEqual({
      record_type: 'execution_baseline_revision',
      record_id: 'recommended-successor',
      record_revision: '5',
    })
  })

  it('shows a historical Task replacement as a simple accept or reject decision', () => {
    mockQuery([
      baseReconciliation({
        conflict: {
          ...baseReconciliation().conflict,
          conflict_code: 'adaptive_task_boundary_crossed',
          description: "adaptive Task operation 'replace' is outside the approved envelope",
        },
        suggested_replacement_ref: {
          record_type: 'execution_baseline_revision',
          record_id: 'corrected-baseline-revision',
          record_revision: '5',
        },
      }),
    ])
    const mutate = mockMutation()
    render(<ReconciliationReviewCard projectId="project-1" />)

    expect(screen.getByText('Allow the Project Agent to retry this task replacement?')).toBeTruthy()
    expect(screen.getByText(/replaces the in-scope Task/)).toBeTruthy()
    expect(screen.getByRole('button', { name: 'Reject' })).toBeTruthy()
    fireEvent.click(screen.getByRole('button', { name: 'Accept' }))

    const [call] = mutate.mock.calls[0] as [
      {
        input: {
          action: string
          replacement_ref: ReconciliationReplacementRef
          reason: string
        }
      },
    ]
    expect(call.input.action).toBe('revised')
    expect(call.input.replacement_ref.record_id).toBe('corrected-baseline-revision')
    expect(call.input.reason).toContain('retry the blocked Task operation')
  })

  it('records rejection by retaining the historical record', () => {
    mockQuery([
      baseReconciliation({
        conflict: {
          ...baseReconciliation().conflict,
          conflict_code: 'invalid_active_baseline',
        },
        allowed_actions: ['revised'],
        suggested_replacement_ref: {
          record_type: 'execution_baseline_revision',
          record_id: 'recommended-successor',
          record_revision: '5',
        },
      }),
    ])
    const mutate = mockMutation()
    render(<ReconciliationReviewCard projectId="project-1" />)

    fireEvent.click(screen.getByRole('button', { name: 'Reject' }))

    expect(mutate).toHaveBeenCalledTimes(1)
    expect(mutate.mock.calls[0]?.[0]?.input.action).toBe('retained')
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
    mockMutation({ mutate } as unknown as Partial<
      ReturnType<typeof useResolveProjectReconciliationMutation>
    >)
    render(<ReconciliationReviewCard projectId="project-1" />)

    fireEvent.click(screen.getByRole('button', { name: 'Reject' }))

    expect(screen.getByText('Recently resolved')).toBeTruthy()
    expect(screen.getByText('Decision saved.')).toBeTruthy()
    expect(screen.getByText(/By Ada at/)).toBeTruthy()
    expect(screen.getByText('Reason: The Charter remains authoritative after review.')).toBeTruthy()
  })
})
