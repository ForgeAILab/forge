import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { useState, type ReactElement } from 'react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { ApiError } from '@/api/client'
import type { AttentionItem } from '@/features/federation/types'
import { LeadInputAnswer as AnswerForm } from './lead-input-answer'

function LeadInputAnswer({ item }: { item: AttentionItem }) {
  const [answer, setAnswer] = useState('')
  return <AnswerForm item={item} answer={answer} onAnswerChange={setAnswer} onAnswered={() => {}} />
}

// The real mutation hook over a mocked transport: what the form does with the
// server's actual answers (`crates/services/src/attention_service.rs`,
// `resolve_with_answer`; `crates/api/src/errors.rs`).
const { apiFetch } = vi.hoisted(() => ({ apiFetch: vi.fn() }))
vi.mock('@/api/client', async (original) => ({
  ...(await original<typeof import('@/api/client')>()),
  apiFetch,
}))

// As `lead.request_input` writes it: the item is scoped to the Project and
// names the root in `details.task_id`.
const item: AttentionItem = {
  id: 'lead-input-1',
  category: 'human_input_required',
  scope_type: 'project',
  scope_id: 'project-1',
  identity_id: 'lead-1',
  source_event_id: 'event-1',
  priority: 95,
  lifecycle: 'open',
  summary: 'Which parser?',
  details: {
    task_id: 'root-1',
    comment_id: 'comment-1',
    question: 'Which parser should the worker use?',
    execution_id: 'run-1',
  },
  dedupe_key: 'lead-input:root-1:comment-1',
  occurred_at: '2026-10-13T00:00:00Z',
  updated_at: '2026-10-13T00:00:00Z',
  version: 7,
  acknowledged_at: null,
  snoozed_until: null,
  resolved_at: null,
  recommended_action: 'answer_lead_input',
}

function mount(element: ReactElement) {
  const client = new QueryClient({ defaultOptions: { mutations: { retry: false } } })
  const invalidate = vi.spyOn(client, 'invalidateQueries')
  const wrap = (child: ReactElement) => (
    <QueryClientProvider client={client}>{child}</QueryClientProvider>
  )
  const view = render(wrap(element))
  return { invalidate, rerender: (next: ReactElement) => view.rerender(wrap(next)) }
}

function type(answer: string) {
  fireEvent.change(screen.getByLabelText('Answer the lead'), { target: { value: answer } })
}

function posts() {
  return apiFetch.mock.calls.filter(([, init]) => init?.method === 'POST')
}

afterEach(() => {
  cleanup()
  apiFetch.mockReset()
})

describe('answering a lead question against the server', () => {
  it('keeps the answer on a stale version, says so, and sends it again at the new version', async () => {
    apiFetch.mockRejectedValueOnce(
      new ApiError('resource version conflict', 409, 'request-1', {
        code: 'version_conflict',
        message: 'resource version conflict',
      } as never),
    )
    const view = mount(<LeadInputAnswer item={item} />)
    type('Use parser A.')
    fireEvent.submit(screen.getByLabelText('Answer the lead').closest('form')!)

    const alert = await screen.findByRole('alert')
    // Not the server's "resource version conflict": what happened and what to do.
    expect(alert.textContent).toContain('Your answer is kept')
    expect(alert.textContent).not.toContain('resource version conflict')
    const input = screen.getByLabelText('Answer the lead') as HTMLTextAreaElement
    expect(input.value).toBe('Use parser A.')
    expect(input.disabled).toBe(false)
    expect(input.getAttribute('aria-describedby')).toContain(alert.id)

    // Mission Control was refetched; the item arrives at its new version.
    expect(view.invalidate).toHaveBeenCalledWith({ queryKey: ['mission-control'] })
    apiFetch.mockResolvedValueOnce({ ...item, lifecycle: 'resolved', version: 9 })
    view.rerender(<LeadInputAnswer item={{ ...item, version: 8 }} />)
    fireEvent.click(screen.getByRole('button', { name: 'Resolve' }))
    await screen.findByRole('button', { name: 'Answered' })
    expect(JSON.parse(posts()[1][1].body)).toEqual({
      expected_version: 8,
      answer: 'Use parser A.',
    })
    expect(screen.queryByRole('alert')).toBeNull()
  })

  it('sends one answer for two submits in the same tick', async () => {
    let settle: (value: unknown) => void = () => {}
    apiFetch.mockReturnValue(new Promise((resolve) => (settle = resolve)))
    mount(<LeadInputAnswer item={item} />)
    type('Use parser A.')
    const form = screen.getByLabelText('Answer the lead').closest('form')!
    fireEvent.submit(form)
    fireEvent.submit(form)
    await waitFor(() => expect(posts().length).toBeGreaterThan(0))
    settle({ ...item, lifecycle: 'resolved', version: 8 })
    await screen.findByRole('button', { name: 'Answered' })
    expect(posts()).toHaveLength(1)
  })

  it("refreshes Mission Control and the root's Task view after the answer", async () => {
    apiFetch.mockResolvedValueOnce({ ...item, lifecycle: 'resolved', version: 8 })
    const view = mount(<LeadInputAnswer item={item} />)
    type('Use parser A.')
    fireEvent.click(screen.getByRole('button', { name: 'Resolve' }))
    await screen.findByRole('button', { name: 'Answered' })
    expect(view.invalidate).toHaveBeenCalledWith({ queryKey: ['mission-control'] })
    // `['tasks', id]` is the prefix of the Task, its detail and its comments
    // (the answer is a comment on the root; the root's wait is cleared).
    expect(view.invalidate).toHaveBeenCalledWith({ queryKey: ['tasks', 'root-1'] })
  })
})
