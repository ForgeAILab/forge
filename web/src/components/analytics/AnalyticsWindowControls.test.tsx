import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { AnalyticsRangeControls, analyticsRangeWindow } from './AnalyticsWindowControls'

describe('analytics window controls', () => {
  it('creates a half-open UTC window without changing the selected end', () => {
    const now = new Date('2026-09-07T12:00:00.000Z')
    expect(analyticsRangeWindow('7d', now)).toEqual({
      from: '2026-08-31T12:00:00.000Z',
      to: '2026-09-07T12:00:00.000Z',
    })
    expect(analyticsRangeWindow('all', now)).toEqual({})
  })

  it('exposes pressed state and an accessible range group', () => {
    render(<AnalyticsRangeControls value="30d" onChange={() => {}} />)
    expect(screen.getByRole('group', { name: 'Analytics time range' })).toBeTruthy()
    expect(screen.getByRole('button', { name: 'Last 30 days' }).getAttribute('aria-pressed')).toBe(
      'true',
    )
  })
})
