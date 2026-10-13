import { render, screen } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import type { Task } from '@/types/generated'
import { TaskBlockingBanner } from './task-blocking-banner'

const details = {
  failure_kind: null,
  diagnostic: null,
  interruption: null,
  failed: false,
  blocked: false,
  human_wait: false,
  entry_wait: false,
}

function parked(primary: Record<string, unknown>, overrides: Record<string, unknown> = {}): Task {
  return {
    id: 'task',
    status: 'todo',
    integration: null,
    execution_observability: { latest_execution_id: null },
    condition: {
      kind: 'parked',
      primary,
      additional: [],
      resume: { kind: 'reconcile' },
      since: null,
      details: { ...details, ...((overrides.details as object) ?? {}) },
    },
    ...Object.fromEntries(Object.entries(overrides).filter(([key]) => key !== 'details')),
  } as unknown as Task
}

describe('TaskBlockingBanner waits of a coordination root and its subtasks', () => {
  it("tells a subtask it waits for the lead's plan", () => {
    render(<TaskBlockingBanner task={parked({ kind: 'parent', parent_id: 'root', cause: 'planning' })} />)
    expect(screen.getByText("Waiting for the lead's plan")).toBeTruthy()
    expect(screen.getByText(/lead is planning/)).toBeTruthy()
    expect(screen.queryByText(/Task Blocked|Task Failed/)).toBeNull()
  })

  it('shows a root in in_progress as running its subtasks', () => {
    render(
      <TaskBlockingBanner
        task={parked({ kind: 'children', root_id: 'root', remaining: ['a', 'b', 'c'] }, { status: 'in_progress' })}
      />,
    )
    expect(screen.getByText('Running subtasks')).toBeTruthy()
    expect(screen.getByText(/3 subtasks are unfinished/)).toBeTruthy()
  })

  it('names the spent re-plan budget ahead of the raw blocked record', () => {
    render(
      <TaskBlockingBanner
        task={parked(
          { kind: 'budget_exhausted', budget_kind: 'replan' },
          {
            status: 'in_progress',
            details: {
              blocked: true,
              interruption: { kind: 'retry_exhausted', reason: 'replan budget exhausted', source: 'replan', created_at: '' },
            },
          },
        )}
      />,
    )
    expect(screen.getByText('Re-plan budget spent')).toBeTruthy()
    expect(screen.getByText(/Retry with a new budget/)).toBeTruthy()
  })

  it('renders nothing for a subtask passing through lead_check with no condition', () => {
    const { container } = render(
      <TaskBlockingBanner
        task={{ ...parked({ kind: 'held', actor: 'user' }), status: 'lead_check', condition: { kind: 'clear', details } } as unknown as Task}
      />,
    )
    expect(container.textContent).toBe('')
  })
})
