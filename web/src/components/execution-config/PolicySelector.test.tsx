import { fireEvent, render, screen, within } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { PolicySelector } from './PolicySelector'

describe('PolicySelector', () => {
  it('offers an explicit YOLO policy', () => {
    const onChange = vi.fn()

    render(<PolicySelector id="policy" value={null} onChange={onChange} />)

    fireEvent.click(screen.getByLabelText('Policy'))
    fireEvent.click(
      within(screen.getByRole('listbox')).getByRole('option', {
        name: /YOLO — Full host access with no approval prompts/i,
      }),
    )

    expect(onChange).toHaveBeenCalledWith('yolo')
  })

  it('keeps the full-access warning visible while YOLO is selected', () => {
    render(<PolicySelector id="policy" value="yolo" onChange={vi.fn()} />)

    expect(screen.getByRole('status').textContent).toBe(
      'Full host access. Forge scope and user-only approval boundaries still apply.',
    )
    expect(screen.getByLabelText('Policy').getAttribute('title')).toBe(
      'Full host access with no approval prompts.',
    )
  })
})
