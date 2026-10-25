// Mission Control attention drafts, incidents, escalation, and recovery tests.

// Keep this import first: the helper registers the page's vi.mock calls.
import {
  data,
  resolveAttention,
  answerEscalation,
  retryTurn,
  turnState,
} from './MissionControlPage.test-helpers'
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { MissionControlPage } from '@/pages/MissionControlPage'

describe('MissionControlPage', () => {
  it('keeps a withdrawn lead draft at its card position until dismissed, and can copy it', async () => {
    const previous = data.needs_attention
    const writeText = vi.fn().mockResolvedValue(undefined)
    vi.stubGlobal('navigator', { ...navigator, clipboard: { writeText } })
    data.needs_attention = [
      {
        ...previous[0],
        id: 'lead-draft',
        category: 'human_input_required',
        recommended_action: 'answer_lead_input',
        details: { question: 'Which parser?' },
      },
      {
        ...previous[0],
        id: 'other-question',
        category: 'human_input_required',
        summary: 'Another question',
      },
    ]
    try {
      const view = render(<MissionControlPage />)
      fireEvent.change(screen.getByLabelText('Answer the lead'), {
        target: { value: '  Use parser A.\nKeep the tests.  ' },
      })
      data.needs_attention = [data.needs_attention[1]]
      view.rerender(<MissionControlPage />)
      const notice = screen
        .getByText(/This question was withdrawn; your draft:/)
        .closest('article')!
      expect(notice.id).toBe('attention-lead-draft')
      expect(notice.textContent).toContain('  Use parser A.\nKeep the tests.  ')
      expect(notice.nextElementSibling?.textContent).toContain('Another question')
      expect(screen.queryByLabelText('Answer the lead')).toBeNull()
      fireEvent.click(screen.getByRole('button', { name: 'Copy draft' }))
      await waitFor(() =>
        expect(writeText).toHaveBeenCalledExactlyOnceWith('  Use parser A.\nKeep the tests.  '),
      )
      expect(await screen.findByRole('button', { name: 'Copied' })).toBeTruthy()
      fireEvent.click(screen.getByRole('button', { name: 'Dismiss draft' }))
      expect(screen.queryByText(/This question was withdrawn/)).toBeNull()
      view.rerender(<MissionControlPage />)
      expect(screen.queryByText(/This question was withdrawn/)).toBeNull()
    } finally {
      data.needs_attention = previous
      vi.unstubAllGlobals()
    }
  })

  it('keys drafts by attention id across refetches and multiple withdrawn questions', () => {
    const previous = data.needs_attention
    data.needs_attention = ['first', 'second', 'empty'].map((id) => ({
      ...previous[0],
      id,
      category: 'human_input_required',
      recommended_action: 'answer_lead_input',
      details: { question: `Question ${id}?` },
    }))
    try {
      const view = render(<MissionControlPage />)
      const inputs = screen.getAllByLabelText('Answer the lead')
      fireEvent.change(inputs[0], { target: { value: 'First answer' } })
      fireEvent.change(inputs[1], { target: { value: 'Second answer' } })
      fireEvent.change(inputs[2], { target: { value: '  ' } })
      data.needs_attention = data.needs_attention.map((item) => ({ ...item, version: 9 }))
      view.rerender(<MissionControlPage />)
      expect((screen.getAllByLabelText('Answer the lead')[0] as HTMLTextAreaElement).value).toBe(
        'First answer',
      )
      data.needs_attention = []
      view.rerender(<MissionControlPage />)
      expect(screen.getAllByText(/This question was withdrawn/)).toHaveLength(2)
      expect(screen.getByText('First answer')).toBeTruthy()
      expect(screen.getByText('Second answer')).toBeTruthy()
      expect(document.getElementById('attention-empty')).toBeNull()
    } finally {
      data.needs_attention = previous
    }
  })

  it('does not call a successfully submitted answer a withdrawn draft', () => {
    const previous = data.needs_attention
    data.needs_attention = [
      {
        ...previous[0],
        id: 'answered',
        category: 'human_input_required',
        recommended_action: 'answer_lead_input',
        details: { question: 'Which parser?' },
      },
    ]
    try {
      const view = render(<MissionControlPage />)
      fireEvent.change(screen.getByLabelText('Answer the lead'), { target: { value: 'Parser A' } })
      fireEvent.click(screen.getByRole('button', { name: 'Resolve' }))
      const options = resolveAttention.mock.calls.at(-1)![1]
      act(() => options.onSuccess?.())
      data.needs_attention = []
      view.rerender(<MissionControlPage />)
      expect(screen.queryByText(/This question was withdrawn/)).toBeNull()
    } finally {
      data.needs_attention = previous
      resolveAttention.mockClear()
    }
  })
  it('renders CI incidents with their count, base failures and task links', () => {
    const previous = data.needs_attention
    data.needs_attention = [
      {
        ...previous[0],
        category: 'base_check_failing',
        summary: 'Base CI needs attention',
        scope_type: 'project',
        recommended_action: 'inspect_ci',
        details: {
          command: 'pnpm test',
          count: 2,
          task_ids: ['task-a', 'task-b'],
          base_commit: 'abc123',
          base_failures: ['suite > base'],
        },
      },
    ]
    try {
      render(<MissionControlPage />)
      const card = screen.getByText('Base CI needs attention').closest('article')
      expect(card?.className).toContain('bg-warning/5')
      expect(card?.textContent).toContain('2 occurrences · 2 tasks')
      expect(screen.getByText('pnpm test')).toBeTruthy()
      expect(screen.getByText('suite > base')).toBeTruthy()
      expect(screen.getByLabelText('Inspect CI for task task-a')).toBeTruthy()
      expect(screen.getByLabelText('Inspect CI for task task-b')).toBeTruthy()
    } finally {
      data.needs_attention = previous
    }
  })

  it('renders conflict hot spots with the existing attention warning treatment', () => {
    const previous = data.needs_attention
    data.needs_attention = [
      {
        ...previous[0],
        category: 'conflict_hotspot',
        scope_type: 'project',
        scope_id: 'project-1',
        summary: 'src/shared.rs conflicted in 3 Tasks this week',
        recommended_action: 'split_hotspot',
      },
    ]
    try {
      render(<MissionControlPage />)
      expect(screen.getByText('Conflict Hotspot')).toBeTruthy()
      expect(screen.getByText('src/shared.rs conflicted in 3 Tasks this week')).toBeTruthy()
      expect(screen.getByText('Split Hotspot')).toBeTruthy()
      expect(
        screen
          .getByText('Conflict Hotspot')
          .closest('article')
          ?.classList.contains('border-warning/30'),
      ).toBe(true)
    } finally {
      data.needs_attention = previous
    }
  })

  it('keeps semantic progress warnings distinct from owner failure', () => {
    const previous = data.needs_attention
    data.needs_attention = [
      {
        ...previous[0],
        id: 'attention-progress',
        category: 'progress_warning',
        summary: 'Execution is waiting for semantic progress.',
        details: { entity_type: 'task', entity_id: 'task-1' },
        recommended_action: 'Inspect run',
      },
      {
        ...previous[0],
        id: 'attention-failure',
        category: 'execution_failed',
        summary: 'Task execution failed',
        recommended_action: 'Inspect run',
      },
    ]

    try {
      render(<MissionControlPage />)

      const summary = screen.getByText('Execution is waiting for semantic progress.')
      const card = summary.closest('article')
      expect(card?.getAttribute('role')).toBe('status')
      expect(card?.textContent).toContain('owner health is reported separately')
      expect(card?.textContent).toContain('Next: Inspect run')
      expect(card?.className).toContain('bg-warning/5')
      expect(screen.getByLabelText('Inspect run').closest('a')).toBeTruthy()

      const failureCard = screen.getByText('Task execution failed').closest('article')
      expect(failureCard?.getAttribute('role')).toBeNull()
      expect(failureCard?.className).toContain('bg-destructive/5')
    } finally {
      data.needs_attention = previous
    }
  })

  it('resolves an attention item with its version', () => {
    const previous = data.needs_attention
    data.needs_attention = [{ ...previous[0], id: 'attention-hotspot', version: 7 }]
    try {
      render(<MissionControlPage />)
      fireEvent.click(screen.getByRole('button', { name: 'Resolve' }))
      expect(resolveAttention).toHaveBeenCalledWith({ id: 'attention-hotspot', expectedVersion: 7 })
    } finally {
      data.needs_attention = previous
      resolveAttention.mockClear()
    }
  })

  it('routes lead-question Resolve through the answer form instead of a version-only request', () => {
    const previous = data.needs_attention
    data.needs_attention = [
      {
        ...previous[0],
        id: 'lead-input-1',
        category: 'human_input_required',
        version: 8,
        recommended_action: 'answer_lead_input',
        details: { question: 'Which parser?' },
      },
    ]
    try {
      render(<MissionControlPage />)
      expect(screen.getAllByRole('button', { name: 'Resolve' })).toHaveLength(1)
      expect(screen.getByText('Which parser?').closest('article')?.id).toBe(
        'attention-lead-input-1',
      )
      fireEvent.change(screen.getByLabelText('Answer the lead'), { target: { value: 'Parser A' } })
      fireEvent.click(screen.getByRole('button', { name: 'Resolve' }))
      expect(resolveAttention).toHaveBeenCalledExactlyOnceWith(
        { id: 'lead-input-1', expectedVersion: 8, answer: 'Parser A' },
        expect.anything(),
      )
    } finally {
      data.needs_attention = previous
      resolveAttention.mockClear()
    }
  })

  it('shows an escalation need and answers it from the card', () => {
    const previous = data.needs_attention
    data.needs_attention = [
      {
        ...previous[0],
        id: 'attention-escalation',
        category: 'human_input_required',
        scope_type: 'project',
        scope_id: 'project-1',
        summary: 'Blocker cannot-fix',
        recommended_action: 'answer_escalation',
        details: {
          escalation_id: 'escalation-1',
          need: '- Blocker cannot-fix (Task "Build login"; reason: disk full)',
          task_ids: [],
        },
      },
    ]
    try {
      render(<MissionControlPage />)
      expect(
        screen.getByText('- Blocker cannot-fix (Task "Build login"; reason: disk full)'),
      ).toBeTruthy()
      const answer = screen.getByRole('button', { name: 'Answer' })
      expect(answer.hasAttribute('disabled')).toBe(true)
      fireEvent.change(screen.getByLabelText('Answer'), {
        target: { value: 'Freed 20 GB on the host' },
      })
      fireEvent.click(answer)
      expect(answerEscalation).toHaveBeenCalledWith({
        projectId: 'project-1',
        escalationId: 'escalation-1',
        answer: 'Freed 20 GB on the host',
      })
    } finally {
      data.needs_attention = previous
      answerEscalation.mockClear()
    }
  })

  it('re-drives the typed turn action from Attention with its version', () => {
    const previous = data.needs_attention
    data.needs_attention = [
      {
        ...previous[0],
        category: 'retry_exhausted',
        summary: 'Agent Chat turn failed: configuration_invalid',
        recommended_action: 'retry_turn',
        details: {
          retry_action: {
            kind: 'retry_turn',
            chat_id: 'main-chat',
            turn_id: 'turn-1',
            expected_version: 4,
          },
        },
      },
    ]
    try {
      render(<MissionControlPage />)
      fireEvent.click(screen.getByRole('button', { name: 'Retry turn' }))
      expect(retryTurn).toHaveBeenCalledWith({
        turnId: 'turn-1',
        input: {
          expected_version: 4,
          idempotency_key: 'agent-chat-turn-retry:turn-1:4',
        },
      })
    } finally {
      data.needs_attention = previous
      retryTurn.mockClear()
    }
  })

  it('disables the typed retry action while another turn is live', () => {
    const previous = data.needs_attention
    data.needs_attention = [
      {
        ...previous[0],
        category: 'retry_exhausted',
        summary: 'Agent Chat turn failed: configuration_invalid',
        recommended_action: 'retry_turn',
        details: {
          retry_action: {
            kind: 'retry_turn',
            chat_id: 'main-chat',
            turn_id: 'turn-1',
            expected_version: 4,
          },
        },
      },
    ]
    try {
      turnState.live = true
      render(<MissionControlPage />)
      expect(screen.getByRole('button', { name: 'Retry turn' }).hasAttribute('disabled')).toBe(true)
    } finally {
      data.needs_attention = previous
      retryTurn.mockClear()
      turnState.live = false
    }
  })

  it('does not enable retry from an untyped Attention recommendation', () => {
    const previous = data.needs_attention
    data.needs_attention = [{ ...previous[0], recommended_action: 'retry_turn' }]
    try {
      render(<MissionControlPage />)
      expect(screen.queryByRole('button', { name: 'Retry turn' })).toBeNull()
    } finally {
      data.needs_attention = previous
    }
  })
  it('renders worker error kind and message without a processed-event counter', () => {
    data.consumer_health = {
      consumer_name: 'attention_projection',
      last_sequence: 12,
      last_success_at: null,
      last_error_code: 'terminal',
      last_error_message: 'Commitment transition rejected',
      stale: false,
      updated_at: '2026-10-02T00:00:00Z',
    }
    render(<MissionControlPage />)
    expect(screen.getByText('Commitment transition rejected')).toBeTruthy()
    expect(screen.queryByText('Processed events')).toBeNull()
    expect(screen.queryByText('Stale')).toBeNull()
    data.consumer_health = null
  })
})
