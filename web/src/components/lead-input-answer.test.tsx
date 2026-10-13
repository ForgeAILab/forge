import { cleanup, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { ApiError } from '@/api/client'
import type { AttentionItem } from '@/features/federation/types'
import { LeadInputAnswer } from './lead-input-answer'

const state = vi.hoisted(() => ({
  mutate: vi.fn(),
  isPending: false,
  isSuccess: false,
  error: null as unknown,
}))
vi.mock('@/features/federation/hooks', () => ({ useResolveAttentionMutation: () => state }))

const item: AttentionItem = {
  id: 'lead-input-1',
  category: 'human_input_required',
  scope_type: 'task',
  scope_id: 'root-1',
  identity_id: 'lead-1',
  source_event_id: 'event-1',
  priority: 80,
  lifecycle: 'open',
  summary: 'The lead needs your answer',
  details: { question: 'Which parser should the worker use?' },
  dedupe_key: 'lead-input:root-1:comment-1',
  occurred_at: '2026-10-13T00:00:00Z',
  updated_at: '2026-10-13T00:00:00Z',
  version: 7,
  acknowledged_at: null,
  snoozed_until: null,
  resolved_at: null,
  recommended_action: 'answer_lead_input',
}

beforeEach(() => {
  state.mutate.mockReset()
  // The request settles at once, as far as the form's own guard is concerned.
  state.mutate.mockImplementation((_input: unknown, options?: { onSettled?: () => void }) =>
    options?.onSettled?.(),
  )
  state.isPending = false
  state.isSuccess = false
  state.error = null
})
afterEach(cleanup)

describe('answering a lead question', () => {
  it('shows the question and sends the answer with the item version', () => {
    render(<LeadInputAnswer item={item} />)
    expect(screen.getByText('Which parser should the worker use?')).toBeTruthy()
    const resolve = screen.getByRole('button', { name: 'Resolve' }) as HTMLButtonElement
    expect(resolve.disabled).toBe(true)
    fireEvent.change(screen.getByLabelText('Answer the lead'), {
      target: { value: 'Use the existing parser.' },
    })
    fireEvent.click(resolve)
    expect(state.mutate).toHaveBeenCalledExactlyOnceWith(
      { id: item.id, expectedVersion: 7, answer: 'Use the existing parser.' },
      expect.anything(),
    )
  })

  it('rejects blank and overlong answers, allowing both length boundaries', () => {
    render(<LeadInputAnswer item={item} />)
    const input = screen.getByLabelText('Answer the lead')
    const resolve = screen.getByRole('button', { name: 'Resolve' }) as HTMLButtonElement
    for (const value of [' ', 'a'.repeat(4001)]) {
      fireEvent.change(input, { target: { value } })
      expect(resolve.disabled).toBe(true)
      expect(input.getAttribute('aria-invalid')).toBe('true')
      expect(screen.getByRole('alert')).toBeTruthy()
      fireEvent.submit(input.closest('form')!)
    }
    expect(state.mutate).not.toHaveBeenCalled()
    for (const value of ['a', '😀'.repeat(4000)]) {
      fireEvent.change(input, { target: { value } })
      expect(resolve.disabled).toBe(false)
      fireEvent.click(resolve)
    }
    expect(state.mutate).toHaveBeenCalledTimes(2)
  })

  it('keeps the draft and shows the server message on a failed resolve', () => {
    const view = render(<LeadInputAnswer item={item} />)
    fireEvent.change(screen.getByLabelText('Answer the lead'), {
      target: { value: 'Use parser A.' },
    })
    state.error = new ApiError('raw body', 400, undefined, {
      code: 'INVALID_OPERATION',
      message: 'Attention item has no question to answer',
      request_id: 'request-1',
    })
    view.rerender(<LeadInputAnswer item={item} />)
    expect(screen.getByRole('alert').textContent).toContain(
      'Attention item has no question to answer',
    )
    expect((screen.getByLabelText('Answer the lead') as HTMLTextAreaElement).value).toBe(
      'Use parser A.',
    )
  })

  it('disables repeated submissions while pending and after success', () => {
    const view = render(<LeadInputAnswer item={item} />)
    fireEvent.change(screen.getByLabelText('Answer the lead'), { target: { value: 'A' } })
    state.isPending = true
    view.rerender(<LeadInputAnswer item={item} />)
    expect((screen.getByRole('button', { name: 'Resolving…' }) as HTMLButtonElement).disabled).toBe(
      true,
    )
    expect((screen.getByLabelText('Answer the lead') as HTMLTextAreaElement).disabled).toBe(true)
    state.isPending = false
    state.isSuccess = true
    view.rerender(<LeadInputAnswer item={item} />)
    expect((screen.getByRole('button', { name: 'Answered' }) as HTMLButtonElement).disabled).toBe(
      true,
    )
  })

  it('retains resolved questions without offering another answer', () => {
    render(<LeadInputAnswer item={{ ...item, lifecycle: 'resolved' }} />)
    expect(screen.getByText('Which parser should the worker use?')).toBeTruthy()
    expect(screen.queryByRole('textbox')).toBeNull()
    expect(screen.queryByRole('button')).toBeNull()
  })

  it('does not submit when the question is missing from the projection', () => {
    render(<LeadInputAnswer item={{ ...item, details: {} }} />)
    expect(screen.getByText(/question is unavailable/)).toBeTruthy()
    expect((screen.getByRole('button', { name: 'Resolve' }) as HTMLButtonElement).disabled).toBe(
      true,
    )
  })
})
