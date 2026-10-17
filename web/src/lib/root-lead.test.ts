import { describe, expect, it } from 'vitest'
import { ApiError } from '@/api/client'
import type { StateDefinition } from '@/types/generated'
import {
  effectiveLead,
  isLeadCandidate,
  leadCandidateDisabledReason,
  roleAssignmentErrorMessage,
  roleSlots,
  rootHoldsRuns,
  stepCheckLabel,
  stepCheckNotice,
} from './root-lead'

const roles = [
  { name: 'planner', display_name: 'Planner', description: '' },
  { name: 'coder', display_name: 'Coder', description: '' },
  { name: 'reviewer', display_name: 'Reviewer', description: '' },
]

function assignment(assignee_id: string | null) {
  return {
    id: 'row',
    task_id: 'root-1',
    role_name: 'planner',
    assignee_type: 'agent',
    assignee_id,
    created_at: '',
    updated_at: '',
  }
}

function refusal(code: string): ApiError {
  return new ApiError('server text', 422, undefined, { code, message: 'server text' } as never)
}

describe('the lead of a coordination root', () => {
  it('admits a native Agent only, as the server does', () => {
    expect(isLeadCandidate({ backend_kind: 'native' })).toBe(true)
    // `embedded` is a direct Agent elsewhere in the web, but the server refuses it as a lead.
    for (const backend_kind of ['embedded', 'cli', 'codex', '']) {
      expect(isLeadCandidate({ backend_kind })).toBe(false)
      expect(leadCandidateDisabledReason({ backend_kind })).toMatch(/native Agent/)
    }
    expect(leadCandidateDisabledReason({ backend_kind: 'native' })).toBeUndefined()
  })

  it('names the planning role the lead and the coder the default worker on a root with subtasks', () => {
    const slots = roleSlots({ roles, coderRole: 'coder', isSubtask: false, isCoordinationRoot: true })
    expect(slots.map((slot) => [slot.role.name, slot.label, slot.lead])).toEqual([
      ['coder', 'Default worker', false],
      ['planner', 'Lead', true],
      ['reviewer', 'Reviewer', false],
    ])
  })

  it('leaves a Task without subtasks with its workflow roles: its planner may be any Agent', () => {
    const slots = roleSlots({ roles, coderRole: 'coder', isSubtask: false, isCoordinationRoot: false })
    expect(slots.map((slot) => [slot.label, slot.lead])).toEqual([
      ['Coder', false],
      ['Planner', false],
      ['Reviewer', false],
    ])
  })

  it('never offers the step-check role on a subtask: the root holds it', () => {
    const inherited = [
      { name: 'coder', display_name: 'Coder', description: '' },
      { name: 'planner', display_name: 'Lead', description: '' },
    ]
    const slots = roleSlots({
      roles: inherited,
      coderRole: 'coder',
      isSubtask: true,
      isCoordinationRoot: false,
    })
    expect(slots.map((slot) => slot.role.name)).toEqual(['coder'])
  })

  it('reads the lead from effective_lead and says whose row it is', () => {
    expect(effectiveLead({ effective_lead: null })).toBeNull()
    expect(effectiveLead({})).toBeNull()
    expect(
      effectiveLead({ effective_lead: { assignment: assignment(null), source: 'own' } }),
    ).toBeNull()
    expect(
      effectiveLead({
        effective_lead: { assignment: assignment('lead'), source: 'inherited_from_root' },
      })?.inherited,
    ).toBe(true)
    expect(
      effectiveLead({ effective_lead: { assignment: assignment('lead'), source: 'own' } })
        ?.inherited,
    ).toBe(false)
  })

  it('describes the step check only inside the gate, and a missing lead as a pass, not a fault', () => {
    const lead = { assignment: assignment('lead'), source: 'inherited_from_root' as const }
    expect(stepCheckNotice({ status: 'in_progress', effective_lead: lead }, 'Ada')).toBeNull()
    expect(stepCheckNotice({ status: 'lead_check', effective_lead: lead }, 'Ada')).toEqual({
      message: 'Ada checks this step before it is done.',
      waitingOnLead: true,
    })
    expect(stepCheckNotice({ status: 'lead_check', effective_lead: lead }, undefined)?.message).toBe(
      'The lead checks this step before it is done.',
    )
    const none = stepCheckNotice({ status: 'lead_check', effective_lead: null }, undefined)
    expect(none?.waitingOnLead).toBe(false)
    expect(none?.message).toMatch(/No lead .* passes automatically/)
  })

  it('takes the gate name from the workflow and falls back to the built-in one', () => {
    expect(stepCheckLabel(undefined)).toBe('Lead Check')
    expect(stepCheckLabel({ states: [] })).toBe('Lead Check')
    expect(
      stepCheckLabel({ states: [{ name: 'lead_check', display_name: 'Step check' }] } as never),
    ).toBe('Step check')
  })

  it('turns the three refusals into readable messages and leaves other errors alone', () => {
    expect(roleAssignmentErrorMessage(refusal('LEAD_REQUIRES_NATIVE_PROFILE'))).toMatch(
      /must be a native Agent/,
    )
    expect(roleAssignmentErrorMessage(refusal('LEAD_ROLE_NOT_SUPPORTED'))).toMatch(
      /only the built-in planner role can lead/,
    )
    expect(roleAssignmentErrorMessage(refusal('STEP_CHECK_ROLE_HELD_BY_ROOT'))).toMatch(
      /Assign the lead on the parent task/,
    )
    for (const message of [
      roleAssignmentErrorMessage(refusal('LEAD_REQUIRES_NATIVE_PROFILE')),
      roleAssignmentErrorMessage(refusal('LEAD_ROLE_NOT_SUPPORTED')),
      roleAssignmentErrorMessage(refusal('STEP_CHECK_ROLE_HELD_BY_ROOT')),
    ]) {
      // No server identifiers leak into the wording.
      expect(message).not.toMatch(/server text|root-1|_/)
    }
    expect(roleAssignmentErrorMessage(refusal('version_conflict'))).toBeUndefined()
    expect(roleAssignmentErrorMessage(new Error('offline'))).toBeUndefined()
  })
})

describe('launching a run on a coordination root', () => {
  const idle = { kind: 'idle' } as never
  const waitingOnSubtasks = {
    kind: 'parked',
    primary: { kind: 'children', root_id: 'root-1', remaining: ['a'] },
    additional: [],
  } as never
  const root = { parent_task_id: null, effective_lead: null, condition: idle }

  it('offers no run while the root coordinates its subtasks: the server refuses one', () => {
    // `ensure_ordered_execution_admission`: a root with subtasks runs its lead
    // in planning and its aggregate reviewer in review, and nothing else.
    for (const status of ['todo', 'in_progress', 'blocked', 'merge_failed', 'planning']) {
      expect(rootHoldsRuns({ ...root, status }, true), status).toBe(true)
    }
    expect(rootHoldsRuns({ ...root, status: 'review' }, true)).toBe(false)
  })

  it('knows a coordination root without the subtask list: its wait or its lead says so', () => {
    expect(
      rootHoldsRuns({ ...root, status: 'in_progress', condition: waitingOnSubtasks }, false),
    ).toBe(true)
    expect(
      rootHoldsRuns(
        {
          ...root,
          status: 'in_progress',
          effective_lead: { assignment: assignment('lead-1'), source: 'own' } as never,
        },
        false,
      ),
    ).toBe(true)
  })

  it('allows a renamed review gate by its kind, reviewer role and canonical phase', () => {
    const review = {
      name: 'qa_review',
      kind: 'gate',
      role: 'reviewer',
      canonical_phase: 'review',
    } as StateDefinition
    const task = { ...root, status: 'qa_review' }
    expect(rootHoldsRuns(task, true, { states: [review] })).toBe(false)
    expect(rootHoldsRuns(task, false, { states: [review] })).toBe(false)
    for (const state of [
      { ...review, kind: 'active' as const },
      { ...review, role: 'coder' },
      { ...review, canonical_phase: 'working' as const },
      { ...review, canonical_phase: null },
      { ...review, name: 'other_gate' },
    ]) {
      expect(rootHoldsRuns(task, true, { states: [state] })).toBe(true)
    }
    expect(rootHoldsRuns(task, true)).toBe(true)
  })

  it('leaves a plain Task and a subtask launchable', () => {
    expect(rootHoldsRuns({ ...root, status: 'in_progress' }, false)).toBe(false)
    expect(
      rootHoldsRuns({ ...root, parent_task_id: 'root-1', status: 'in_progress' }, false),
    ).toBe(false)
  })
})
