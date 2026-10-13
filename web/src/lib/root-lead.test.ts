import { describe, expect, it } from 'vitest'
import { ApiError } from '@/api/client'
import {
  effectiveLead,
  isLeadCandidate,
  leadCandidateDisabledReason,
  roleAssignmentErrorMessage,
  roleSlots,
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
