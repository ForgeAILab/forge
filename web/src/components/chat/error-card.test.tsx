import { describe, expect, it, vi } from 'vitest'
import { fireEvent, render, screen } from '@testing-library/react'
import { ChatErrorMessage } from './chat-error-message'
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
      <ErrorCard title="Turn error" technicalDetails={{ code: 'provider_timeout', attempt: 2 }} />,
    )

    expect(screen.getByText('Technical details')).toBeTruthy()
    expect(screen.getByText('provider_timeout')).toBeTruthy()
  })

  it('renders only bounded safe outcome fields and withholds protected payloads', () => {
    render(
      <ChatErrorMessage
        entry={{
          sequence: 1,
          timestamp: '2026-08-21T12:00:00Z',
          kind: 'error',
          title: 'Command failed',
          message: '{"code":"internal_failure","cause":"protected persistence detail"}',
          payload: {
            code: 'internal_failure',
            status: 'failed',
            operation: 'project.update',
            safe_message: 'The Project could not be updated.',
            correlation_id: 'corr-123',
            result: { secret_payload: 'must not render' },
            protected_cause: 'must not render',
          },
        }}
      />,
    )

    expect(screen.getByText('The Project could not be updated.')).toBeTruthy()
    expect(screen.getByText('internal_failure')).toBeTruthy()
    expect(screen.getByText('project.update')).toBeTruthy()
    expect(screen.getByText('corr-123')).toBeTruthy()
    expect(
      screen.queryByText(/secret_payload|protected persistence detail|protected_cause/),
    ).toBeNull()
  })

  it('does not promote nested outcome causes to the visible error message', () => {
    render(
      <ChatErrorMessage
        entry={{
          sequence: 2,
          timestamp: '2026-08-21T12:01:00Z',
          kind: 'error',
          title: 'Command failed',
          message: '{"error":{"message":"protected persistence detail"}}',
          payload: {
            status: 'failed',
            operation: 'project.update',
            details: { message: 'protected persistence detail' },
          },
        }}
      />,
    )

    expect(screen.getByText('The agent turn could not complete.')).toBeTruthy()
    expect(screen.queryByText('protected persistence detail')).toBeNull()
  })
})
