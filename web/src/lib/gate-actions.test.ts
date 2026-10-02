import { describe, expect, it } from 'vitest'
import { getHumanGateActions } from './gate-actions'
import type { Offer, Task } from '@/types/generated'
const offer = (verb: 'approve' | 'send_back', label: string): Offer => ({ action: verb === 'approve' ? { verb, override: false } : { verb, guidance: 'Revise' }, parameters: [], authority: ['owner'], reason: 'fixture', label, target_execution_id: null })
const task = (offers: Offer[], status = 'custom'): Task => ({ id: 'task', status, available_actions: offers } as Task)
describe('human gate offer presentation', () => {
  it('copies the labels supplied by the server for custom states', () => {
    expect(getHumanGateActions(task([offer('approve', 'Accept Candidate'), offer('send_back', 'Revise Candidate')]))).toEqual({ stateName: 'custom', approveLabel: 'Accept Candidate', rejectLabel: 'Revise Candidate' })
  })
  it('does not infer approvals from a review status without an offer', () => { expect(getHumanGateActions(task([], 'review'))).toBeNull() })
  it('does not invent a send-back button', () => { expect(getHumanGateActions(task([offer('approve', 'Approve')]))?.rejectLabel).toBeUndefined() })
  it('has no action without a Task', () => { expect(getHumanGateActions(undefined)).toBeNull() })
})
