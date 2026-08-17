import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { ErrorCard } from './error-card'

describe('ErrorCard', () => {
  it('renders title, description, and severity badge', () => {
    render(
      <ErrorCard
        title="Operation failed"
        description="The network connection was lost."
        severity="error"
      />,
    )

    expect(screen.getByText('Operation failed')).toBeTruthy()
    expect(screen.getByText('The network connection was lost.')).toBeTruthy()
    expect(screen.getByText('Error')).toBeTruthy()
  })

  it('renders action button and triggers callback on click', () => {
    const onRetry = vi.fn()
    render(
      <ErrorCard
        title="Mutation conflict"
        description="Server version changed."
        severity="conflict"
        action={{ label: 'Refresh', onClick: onRetry }}
      />,
    )

    expect(screen.getByText('Conflict')).toBeTruthy()
    const button = screen.getByRole('button', { name: 'Refresh' })
    expect(button).toBeTruthy()
    fireEvent.click(button)
    expect(onRetry).toHaveBeenCalledTimes(1)
  })

  it('renders collapsed technical details by default', () => {
    render(
      <ErrorCard
        title="Turn error"
        technicalDetails={{ code: 'provider_timeout', attempt: 2 }}
      />,
    )

    expect(screen.getByText('Technical details')).toBeTruthy()
    expect(screen.getByText(/"provider_timeout"/)).toBeTruthy()
  })
})
