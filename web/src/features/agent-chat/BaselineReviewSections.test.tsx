import { render, screen, within } from '@testing-library/react'
import { describe, expect, it } from 'vitest'

import type { ExecutionBaselineContent } from '@/types/generated/bindings/ExecutionBaselineContent'
import { BaselineReviewSections } from './BaselineReviewSections'

// F19: the baseline Review modal used to render only a raw preformatted JSON
// blob (`<pre>{revision.rendered_view}</pre>`), making plan items,
// acceptance, risk, adaptive authority, and rollback hard to evaluate. These
// tests prove the fix: semantic sections are the primary view, an exact
// revision diff highlights what changed, and the raw JSON survives only
// inside a collapsed technical-details disclosure.

function content(overrides: Partial<ExecutionBaselineContent> = {}): ExecutionBaselineContent {
  return {
    charter_revision: {
      artifact_id: 'charter-1',
      revision_id: 'charter-revision-1',
      content_digest: 'digest-1',
      render_version: null,
      render_digest: null,
    },
    document_revisions: [],
    plan_item_ids: ['plan-1', 'plan-2'],
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
      { id: 'evidence-1', description: 'Filters clear on reload', required: true, evidence_kind: 'manual', check_definition_revision: null },
    ],
    capability_classes: [],
    risk_classes: ['data_loss'],
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
    rollback_and_recovery: ['Revert the merge commit'],
    exclusions: ['No mobile layout in this revision'],
    ...overrides,
  }
}

describe('BaselineReviewSections', () => {
  it('renders semantic sections instead of a raw JSON blob as the primary view', () => {
    render(
      <BaselineReviewSections
        content={content()}
        renderedView='{"plan_item_ids":["plan-1","plan-2"]}'
        contentDigest="0123456789abcdef0123456789abcdef"
      />,
    )

    expect(screen.getByRole('heading', { name: 'Intended outcomes' })).toBeTruthy()
    expect(screen.getByText('Ship the filter UI')).toBeTruthy()
    expect(screen.getByRole('heading', { name: 'Plan items' })).toBeTruthy()
    expect(screen.getByText('plan-1')).toBeTruthy()
    expect(screen.getByText('plan-2')).toBeTruthy()
    expect(screen.getByRole('heading', { name: 'Milestones & acceptance checks' })).toBeTruthy()
    expect(screen.getByText('Filters clear on reload')).toBeTruthy()
    expect(screen.getByRole('heading', { name: 'Adaptive authority' })).toBeTruthy()
    expect(screen.getByText('Split a Task into smaller Tasks')).toBeTruthy()
    expect(screen.getByRole('heading', { name: 'Risks' })).toBeTruthy()
    expect(screen.getByText('data_loss')).toBeTruthy()
    expect(screen.getByRole('heading', { name: 'Exclusions' })).toBeTruthy()
    expect(screen.getByText('No mobile layout in this revision')).toBeTruthy()
    expect(screen.getByRole('heading', { name: /Rollback/ })).toBeTruthy()
    expect(screen.getByText('Revert the merge commit')).toBeTruthy()

    // The raw payload only ever appears inside the technical-details
    // disclosure, never as loose primary-view content next to it.
    const rawJson = screen.getByText('{"plan_item_ids":["plan-1","plan-2"]}')
    expect(rawJson.closest('details')).toBeTruthy()
  })

  it('keeps the raw JSON only inside a collapsed technical-details disclosure', () => {
    render(
      <BaselineReviewSections
        content={content()}
        renderedView='{"plan_item_ids":["plan-1","plan-2"]}'
        contentDigest="0123456789abcdef0123456789abcdef"
      />,
    )

    const disclosure = screen.getByText('Technical details (raw JSON)').closest('details')
    expect(disclosure).toBeTruthy()
    expect((disclosure as HTMLDetailsElement).open).toBe(false)
    expect(within(disclosure as HTMLElement).getByText(/plan_item_ids/)).toBeTruthy()
    expect(within(disclosure as HTMLElement).getByText(/content digest/)).toBeTruthy()
  })

  it('marks new plan items and evidence added since the previous revision', () => {
    const previous = content({ plan_item_ids: ['plan-1'] })
    const next = content({ plan_item_ids: ['plan-1', 'plan-2'] })
    render(
      <BaselineReviewSections
        content={next}
        previousContent={previous}
        renderedView="{}"
        contentDigest="digest"
      />,
    )
    const planSection = screen.getByRole('heading', { name: 'Plan items' }).closest('section')
    expect(planSection).toBeTruthy()
    const addedItem = within(planSection as HTMLElement).getByText('plan-2').closest('li')
    expect(addedItem?.textContent).toContain('Added')
  })

  it('marks a dropped exclusion as removed rather than silently omitting it', () => {
    const previous = content({ exclusions: ['No mobile layout in this revision', 'No offline mode'] })
    const next = content({ exclusions: ['No mobile layout in this revision'] })
    render(
      <BaselineReviewSections content={next} previousContent={previous} renderedView="{}" contentDigest="digest" />,
    )
    const exclusionsSection = screen.getByRole('heading', { name: 'Exclusions' }).closest('section')
    expect(exclusionsSection).toBeTruthy()
    const removedItem = within(exclusionsSection as HTMLElement).getByText('No offline mode').closest('li')
    expect(removedItem?.textContent).toContain('Removed')
  })

  it('renders no added/removed diff markers when there is no previous revision to compare', () => {
    render(<BaselineReviewSections content={content()} renderedView="{}" contentDigest="digest" />)
    expect(screen.queryByText('Added')).toBeNull()
    expect(screen.queryByText('Removed')).toBeNull()
  })
})
