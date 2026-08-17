import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { act, render, screen } from '@testing-library/react'
import { LoadingState } from './loading-state'

describe('LoadingState', () => {
  beforeEach(() => {
    vi.useFakeTimers()
  })

  afterEach(() => {
    vi.useRealTimers()
  })

  it('renders status label and increments elapsed timer', async () => {
    render(<LoadingState label="Thinking…" />)

    expect(screen.getByText('Thinking…')).toBeTruthy()
    expect(screen.getByText('00:00')).toBeTruthy()

    await act(async () => {
      await vi.advanceTimersByTimeAsync(5_000)
    })

    expect(screen.getByText('00:05')).toBeTruthy()
  })

  it('renders awaiting input label when status is awaiting_input', () => {
    render(<LoadingState status="awaiting_input" />)
    expect(screen.getByText('Awaiting input…')).toBeTruthy()
  })

  it('renders compact mode with custom startedAt timestamp', () => {
    const startedAt = new Date(Date.now() - 65_000).toISOString()
    render(<LoadingState label="Thinking…" startedAt={startedAt} compact />)

    expect(screen.getByText('Thinking…')).toBeTruthy()
    expect(screen.getByText('01:05')).toBeTruthy()
  })
})
