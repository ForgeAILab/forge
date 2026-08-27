import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import type { ExecutionBlockerProjection, ExecutionEvidenceSummary } from '@/types/generated'

import { TaskExecutionApprovalNotice } from './TaskExecutionApprovalNotice'

vi.mock('@tanstack/react-router', () => ({
  Link: ({
    to,
    params,
    hash,
    children,
    ...props
  }: {
    to: string
    params?: Record<string, string>
    hash?: string
    children: React.ReactNode
  }) => {
    const path = params
      ? Object.entries(params).reduce((value, [key, replacement]) => {
          return value.replace(`$${key}`, replacement)
        }, to)
      : to
    return (
      <a href={`${path}${hash ? `#${hash}` : ''}`} {...props}>
        {children}
      </a>
    )
  },
}))

function blocker(overrides: Partial<ExecutionBlockerProjection>): ExecutionBlockerProjection {
  return {
    code: 'baseline_approval_required',
    stage: 'plan',
    scope: 'project',
    affected_refs: [],
    governing_ref: null,
    headline: 'Legacy plan review',
    safe_explanation:
      'Implementation already follows the approved Charter; this optional plan is traceability only.',
    evidence: null,
    required_principal: 'user',
    next_action: 'reauthorize',
    blocker_digest: 'sha256:test',
    observed_version: 1,
    ...overrides,
  } as unknown as ExecutionBlockerProjection
}

function evidence(overrides: Partial<ExecutionEvidenceSummary>): ExecutionEvidenceSummary {
  return {
    attempt_count: 0n,
    execution_count: 0n,
    has_commit: false,
    latest_commit_sha: null,
    progress: 'not_started',
    progress_label: 'Not started',
    ...overrides,
  } as unknown as ExecutionEvidenceSummary
}

describe('TaskExecutionApprovalNotice', () => {
  it('does not claim a legacy plan review starts implementation', () => {
    render(
      <TaskExecutionApprovalNotice
        projectId="project-1"
        blocker={blocker({})}
        evidence={evidence({})}
      />,
    )

    expect(screen.getByText('Legacy plan review')).toBeTruthy()
    expect(screen.getByText(/approved Charter/)).toBeTruthy()
    const action = screen.getByRole('link', { name: /Review traceability plan/ })
    expect(action.getAttribute('href')).toBe('/projects/project-1/chat#execution-approval')
  })

  it('routes a reconciliation blocker to the Project overview review card instead of baseline-approval copy (F12b)', () => {
    render(
      <TaskExecutionApprovalNotice
        projectId="project-1"
        blocker={blocker({
          code: 'reconciliation_required',
          stage: 'build',
          headline: 'Waiting for plan reconciliation',
          safe_explanation:
            "This Task's governance changed and must be reconciled before it can safely resume. The rest of the Project's approved plan remains active.",
          next_action: 'resolve_reconciliation',
        })}
        evidence={evidence({
          attempt_count: 2n,
          execution_count: 2n,
          has_commit: true,
          latest_commit_sha: 'after-sha-committed',
          progress: 'implementation_committed',
          progress_label: 'Implementation committed',
        })}
      />,
    )

    expect(screen.getByText('Waiting for plan reconciliation')).toBeTruthy()
    expect(screen.queryByText(/permission to build/)).toBeNull()
    const action = screen.getByRole('link', { name: /Review current plan/ })
    expect(action.getAttribute('href')).toBe('/projects/project-1/overview')

    // F12: a Task with a commit is never shown as "not started," even while
    // blocked by a reconciliation.
    expect(screen.getByText('Implementation committed')).toBeTruthy()
    expect(screen.queryByText('Not started')).toBeNull()
    expect(screen.queryByText(/task not started/i)).toBeNull()
  })

  it('renders nothing once the Task has no outstanding blocker', () => {
    render(
      <TaskExecutionApprovalNotice projectId="project-1" blocker={null} evidence={undefined} />,
    )
    expect(screen.queryByRole('status')).toBeNull()
  })
})
