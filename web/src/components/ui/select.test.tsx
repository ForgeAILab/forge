import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { Select } from './select'

describe('Select', () => {
  it('moves focus through options and restores it to the trigger after selection', async () => {
    const onChange = vi.fn()
    render(
      <Select
        id="responder"
        value=""
        options={[
          { value: 'agent-1', label: 'Agent one' },
          { value: 'agent-2', label: 'Agent two' },
        ]}
        placeholder="Select responder"
        onChange={onChange}
      />,
    )

    const trigger = screen.getByRole('button', { name: 'Select responder' })
    fireEvent.keyDown(trigger, { key: 'ArrowDown' })

    const firstOption = await screen.findByRole('option', { name: 'Agent one' })
    await waitFor(() => expect(document.activeElement).toBe(firstOption))

    const secondOption = screen.getByRole('option', { name: 'Agent two' })
    fireEvent.keyDown(firstOption, { key: 'ArrowDown' })
    expect(document.activeElement).toBe(secondOption)

    fireEvent.keyDown(secondOption, { key: 'Enter' })
    expect(onChange).toHaveBeenCalledWith('agent-2')
    await waitFor(() => expect(document.activeElement).toBe(trigger))
  })

  it('opens upwards when there is no room below the trigger', async () => {
    const innerHeight = window.innerHeight
    Object.defineProperty(window, 'innerHeight', { configurable: true, value: 800 })
    try {
      render(
        <Select
          value="when_verified"
          aria-label="Provision"
          options={[
            { value: 'when_verified', label: 'When verified' },
            { value: 'never', label: 'Never' },
          ]}
          onChange={vi.fn()}
        />,
      )
      const trigger = screen.getByRole('button', { name: 'Provision' })
      trigger.getBoundingClientRect = () =>
        DOMRect.fromRect({ x: 0, y: 760, width: 200, height: 36 })
      fireEvent.click(trigger)

      const listbox = await screen.findByRole('listbox')
      expect(listbox.style.top).toBe('')
      expect(listbox.style.bottom).toBe('44px')
      expect(listbox.style.maxHeight).toBe('256px')
    } finally {
      Object.defineProperty(window, 'innerHeight', { configurable: true, value: innerHeight })
    }
  })
})
