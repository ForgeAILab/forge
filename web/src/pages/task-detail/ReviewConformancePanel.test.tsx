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

  it.each(['failed', 'unverified'] as const)('keeps the %s reason visible', (status) => {
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
    expect(screen.getByText(/fresh review to use Task-scoped v2/)).toBeTruthy()
  })

  it('shows the exact Charter and commit with requirement evidence', () => {
    const result: ReviewConformance = {
      status: 'passed',
      reason: null,
      checks: [],
      contract: {
        execution_id: 'execution',
        policy: 'forge.review-conformance/2',
        commit_sha: 'reviewed-sha',
        base_sha: 'base',
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
        contract_digest: 'contract-digest',
        verdict: 'pass',
        findings: [],
        requirements: [
          {
            requirement_id: 'rust',
            disposition: 'satisfied',
            rationale: 'The CLI and library compile',
            evidence: [
              {
                kind: 'file',
                path: 'src/lib.rs',
                commit_sha: 'reviewed-sha',
                start_line: 1,
                end_line: 3,
              },
            ],
          },
        ],
      },
    }
    render(<ReviewConformancePanel conformance={result} />)
    expect(screen.getByText('Passed')).toBeTruthy()
    expect(screen.getByText(/covered 1 Task-scoped requirement/)).toBeTruthy()
    expect(screen.getByText(/105 Project requirements remain for milestone readiness/)).toBeTruthy()
    expect(screen.getByText('One Rust crate')).toBeTruthy()
    expect(screen.getByText('charter-r1')).toBeTruthy()
    expect(screen.getByText(/src\/lib.rs:1–3/)).toBeTruthy()
  })
})
