// Mission Control work priority and coordination activity tests.

// Keep this import first: the helper registers the page's vi.mock calls.
import { data } from './MissionControlPage.test-helpers'
import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { MissionControlPage } from '@/pages/MissionControlPage'

describe('MissionControlPage', () => {
  it('prioritizes attention and review-ready work', () => {
    render(<MissionControlPage />)
    expect(screen.getByText('What needs your attention?')).toBeTruthy()
    expect(screen.getByText('A human decision is required.')).toBeTruthy()
    expect(screen.getByText('Ship the Project worker')).toBeTruthy()
    expect(screen.getByText('Main and Project Agent bindings')).toBeTruthy()
    expect(screen.getByText('Global · Main')).toBeTruthy()
    expect(screen.getByText('Main Agent identity')).toBeTruthy()
    expect(screen.getByText('Task Worker')).toBeTruthy()
    expect(screen.queryByText('Unbound profile')).toBeNull()
    expect(screen.getByText('Coordination activity')).toBeTruthy()
    expect(screen.getByText('Durable direct-command receipts')).toBeTruthy()
    expect(screen.getByText('Pending and approved approval actions')).toBeTruthy()
    expect(screen.getByText('Direct command receipt')).toBeTruthy()
    expect(screen.getByText('Approval action')).toBeTruthy()
    expect(screen.getByText('Committed')).toBeTruthy()
    expect(screen.getByText('Pending')).toBeTruthy()
    expect(screen.getByText('User · user-1')).toBeTruthy()
    expect(screen.getByText('Agent · agent-1')).toBeTruthy()
    expect(screen.getByText('sha256:direct-input')).toBeTruthy()
    expect(screen.getByText('correlation-approval')).toBeTruthy()
    expect(
      screen.getByText('Outcome recorded; payload details are withheld from Mission Control.'),
    ).toBeTruthy()
    expect(screen.queryByText('must not render')).toBeNull()
    const capacity =
      screen.getByText('Runtime capacity').parentElement?.parentElement?.parentElement?.textContent
    expect(capacity).toContain('1')
    expect(capacity).toContain('4')
  })

  it('accounts for an empty coordination activity projection', () => {
    const previous = {
      coordination_activity: data.coordination_activity,
      needs_attention: data.needs_attention,
      review_ready: data.review_ready,
      active_work: data.active_work,
      agent_health: data.agent_health,
    }
    data.coordination_activity = []
    data.needs_attention = []
    data.review_ready = []
    data.active_work = []
    data.agent_health = []

    try {
      render(<MissionControlPage />)

      expect(screen.getByText('No coordination activity recorded')).toBeTruthy()
      expect(screen.getByText(/All scopes are quiet\./)).toBeTruthy()
      expect(screen.queryByText('Durable direct-command receipts')).toBeNull()
    } finally {
      data.coordination_activity = previous.coordination_activity
      data.needs_attention = previous.needs_attention
      data.review_ready = previous.review_ready
      data.active_work = previous.active_work
      data.agent_health = previous.agent_health
    }
  })

  it('names the lead Agent on a coordination activity receipt, with ID fallback', () => {
    const previous = data.coordination_activity
    data.coordination_activity = [
      { ...previous[0], actor_type: 'task_lead', actor_id: 'lead-agent' },
      {
        ...previous[0],
        id: 'other-lead-receipt',
        actor_type: 'task_lead',
        actor_id: 'unavailable-lead',
      },
    ]
    try {
      render(<MissionControlPage />)
      expect(screen.getByText('Lead · Ada Lead')).toBeTruthy()
      expect(screen.getByText('Lead · unavailable-lead')).toBeTruthy()
    } finally {
      data.coordination_activity = previous
    }
  })
})
