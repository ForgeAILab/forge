import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import type { ReviewConformance } from '@/types/generated/bindings/ReviewConformance'
import { ReviewConformancePanel } from './ReviewConformancePanel'

describe('ReviewConformancePanel', () => {
  it('does not turn a historic pass into a Charter assessment', () => {
    render(<ReviewConformancePanel />)
    expect(screen.getByText('Not assessed')).toBeTruthy()
    expect(screen.getByText(/review outcome is preserved/)).toBeTruthy()
    expect(screen.queryByText('Passed')).not.toBeTruthy()
  })

  it.each(['failed', 'blocked', 'unverified'] as const)('keeps the %s reason visible', (status) => {
    const result: ReviewConformance = {
      status,
      reason: 'Required Rust boundary missing',
      checks: [],
      assessment: null,
      contract: null,
    }
    render(<ReviewConformancePanel conformance={result} />)
    expect(screen.getByText('Required Rust boundary missing')).toBeTruthy()
  })

  it('marks a historical whole-Project contract as requiring a fresh review', () => {
    const result: ReviewConformance = {
      status: 'unverified',
      reason: 'Historical evidence could not be verified',
      checks: [],
      assessment: null,
      contract: {
        execution_id: 'execution',
        policy: 'forge.review-conformance/1',
        commit_sha: 'reviewed-sha',
        base_sha: 'base',
        candidate_changed_paths: [],
        check_results: [],
        digest: 'contract-digest',
        context: {
          project_id: 'project',
          task_id: 'task',
          repo_id: 'repo',
          charter_revision_id: 'charter-r1',
          charter_digest: 'charter-digest',
          charter: {},
          task_scope: {},
          linked_documents: [],
          setup_steps: [],
          source_digest: 'source',
          required_checks: [],
          deferred_requirement_count: 0,
          deferred_requirements_digest: null,
          requirements: Array.from({ length: 106 }, (_, index) => ({
            id: `requirement-${index}`,
            source: '/scope',
            text: `Requirement ${index}`,
            universal: false,
            allocated_task_id: null,
          })),
        },
      },
    }

    render(<ReviewConformancePanel conformance={result} />)
    expect(screen.getByText(/previous whole-Project scope \(106 requirements\)/)).toBeTruthy()
    expect(screen.getByText(/fresh review to use the current Task-scoped policy/)).toBeTruthy()
  })

  it('marks a v2 contract as obsolete and requiring a fresh review', () => {
    const result: ReviewConformance = {
      status: 'passed',
      reason: null,
      checks: [],
      assessment: null,
      contract: {
        execution_id: 'execution',
        policy: 'forge.review-conformance/2',
        commit_sha: 'reviewed-sha',
        base_sha: 'base',
        candidate_changed_paths: [],
        check_results: [],
        digest: 'contract-digest',
        context: {
          project_id: 'project',
          task_id: 'task',
          repo_id: 'repo',
          charter_revision_id: 'charter-r1',
          charter_digest: 'charter-digest',
          charter: {},
          task_scope: {},
          linked_documents: [],
          setup_steps: [],
          source_digest: 'source',
          required_checks: [],
          deferred_requirement_count: 0,
          deferred_requirements_digest: null,
          requirements: [],
        },
      },
    }

    render(<ReviewConformancePanel conformance={result} />)
    expect(screen.getByText(/obsolete conformance policy/)).toBeTruthy()
    expect(screen.getByText('forge.review-conformance/2')).toBeTruthy()
    expect(screen.getByText(/fresh review to use the current Task-scoped policy/)).toBeTruthy()
    expect(screen.queryByText(/This review covered/)).not.toBeTruthy()
  })

  it("shows the exact Charter and commit with the reviewer's Markdown review", () => {
    const result: ReviewConformance = {
      status: 'passed',
      reason: null,
      checks: [],
      contract: {
        execution_id: 'execution',
        policy: 'forge.review-conformance/3',
        commit_sha: 'reviewed-sha',
        base_sha: 'base',
        candidate_changed_paths: ['src/lib.rs'],
        check_results: [],
        digest: 'contract-digest',
        context: {
          project_id: 'project',
          task_id: 'task',
          repo_id: 'repo',
          charter_revision_id: 'charter-r1',
          charter_digest: 'charter-digest',
          charter: {},
          task_scope: {},
          linked_documents: [],
          setup_steps: [],
          source_digest: 'source',
          required_checks: [],
          deferred_requirement_count: 105,
          deferred_requirements_digest: 'deferred-digest',
          requirements: [
            {
              id: 'rust',
              source: '/scope',
              text: 'One Rust crate',
              universal: true,
              allocated_task_id: null,
            },
          ],
        },
      },
      assessment: {
        fixable_by: 'coder',
        repeat: false,
        result: 'pass',
        reason: 'The CLI and library compile',
        report: 'The crate is implemented in `src/lib.rs:1-3`.',
      },
    }
    render(<ReviewConformancePanel conformance={result} />)
    expect(screen.getByText('Passed')).toBeTruthy()
    expect(screen.getByText(/covered 1 Task-scoped requirement/)).toBeTruthy()
    expect(screen.getByText(/105 Project requirements remain for milestone readiness/)).toBeTruthy()
    expect(screen.getByText('charter-r1')).toBeTruthy()
    expect(screen.getByText(/src\/lib.rs:1-3/)).toBeTruthy()
  })
})

it('shows per-attempt findings, resolved ids and the full report without a contract', () => {
  const result: ReviewConformance = {
    status: 'failed',
    reason: 'Empty input crashes',
    checks: [],
    contract: null,
    assessment: {
      result: 'fail',
      reason: 'Empty input crashes',
      report: 'Detailed reviewer report',
      findings: [
        {
          id: 'F2',
          summary: 'Handle empty input',
          severity: 'blocking',
          where: 'parse.ts:42',
          expected: 'An error',
          observed: 'A panic',
        },
      ],
      resolved: ['F1'],
    },
  }
  render(<ReviewConformancePanel conformance={result} />)
  expect(screen.getByText(/Handle empty input/)).toBeTruthy()
  expect(screen.getByText('Resolved in this attempt: F1')).toBeTruthy()
  expect(screen.getByText('Detailed reviewer report')).toBeTruthy()
  expect(screen.queryByRole('region', { name: 'Verify results' })).toBeNull()
})

it('renders recorded verify checks and a skipped reason only when supplied', () => {
  const { rerender } = render(
    <ReviewConformancePanel
      verify={{
        status: 'fail',
        checks: [
          {
            name: 'smoke',
            result: 'fail',
            summary: 'Button does nothing',
            evidence: ['screenshot-1', 'screenshot-1'],
          },
        ],
      }}
    />,
  )
  expect(screen.getByText('Verify: fail')).toBeTruthy()
  expect(screen.getByText(/smoke · fail — Button does nothing/)).toBeTruthy()
  expect(screen.getByText('1 evidence')).toBeTruthy()
  rerender(
    <ReviewConformancePanel verify={{ status: 'not_run', reason: 'placement_unsupported' }} />,
  )
  expect(screen.getByText('placement_unsupported')).toBeTruthy()
  rerender(<ReviewConformancePanel />)
  expect(screen.queryByRole('region', { name: 'Verify results' })).toBeNull()
})
