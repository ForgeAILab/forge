import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'

import type { Task, WorkflowExceptionAction } from '@/types/generated'
import { WorkflowExceptionPanel } from './workflow-exception-panel'

vi.mock('@tanstack/react-router', () => ({
  Link: ({ children }: { children: React.ReactNode }) => <a href="#evidence">{children}</a>,
}))

const now = '2026-09-27T12:00:00Z'

function action(
  kind: WorkflowExceptionAction['kind'],
  label: string,
  overrides: Partial<WorkflowExceptionAction> = {},
): WorkflowExceptionAction {
  return {
    kind,
    label,
    enabled: true,
    disabled_reason: null,
    requires_reason: false,
    requires_guidance: false,
    propagates: true,
    target_state: 'review',
    target_role: 'reviewer',
    target_execution_id: 'execution-1',
    ...overrides,
  }
}

const actions = [
  action('reexecute', 'Retry Review with Guidance', { requires_guidance: true }),
  action('mark_reviewed', 'Pass Review Manually', {
    requires_reason: true,
    target_state: 'merging',
    target_role: null,
    target_execution_id: null,
  }),
  action('open_interactive', 'Open Side Session', {
    propagates: false,
  }),
]

const task = {
  id: 'task-1',
  project_id: 'project-1',
  title: 'Blocked review',
  task_type: 'task',
  status: 'review',
  priority: 0,
  board_position: 0,
  role_assignments: [],
  remaining_retries: {},
  workflow_exception: {
    type: 'review_blocked',
    message: 'The reviewer could not access the provider.',
    review_id: 'review-1',
    execution_id: 'execution-1',
    state: 'review',
    role: 'reviewer',
    target_state: null,
    target_role: null,
    failing_step: null,
    related_evidence: [],
    actions,
  },
  version: 4,
  created_at: now,
  updated_at: now,
} as Task

describe('WorkflowExceptionPanel', () => {
  it('sends review guidance through authoritative recovery context', () => {
    const onRecover = vi.fn()
    render(
      <WorkflowExceptionPanel
        task={task}
        actions={actions}
        recoverPending={false}
        terminal={false}
        cancelPending={false}
        onRecover={onRecover}
        onOpenInteractive={vi.fn()}
        onCancelTask={vi.fn()}
      />,
    )

    fireEvent.click(screen.getByRole('button', { name: 'Retry Review with Guidance' }))
    const confirm = screen.getByRole('button', { name: 'Confirm' })
    expect((confirm as HTMLButtonElement).disabled).toBe(true)
    fireEvent.change(screen.getByLabelText('Guidance'), {
      target: { value: 'Use the configured test key and rerun the provider smoke test.' },
    })
    fireEvent.click(confirm)

    expect(onRecover).toHaveBeenCalledWith('reexecute', {
      reason: undefined,
      context: 'Use the configured test key and rerun the provider smoke test.',
    })
  })

  it('requires an audit reason before manually passing review', () => {
    const onRecover = vi.fn()
    render(
      <WorkflowExceptionPanel
        task={task}
        actions={actions}
        recoverPending={false}
        terminal={false}
        cancelPending={false}
        onRecover={onRecover}
        onOpenInteractive={vi.fn()}
        onCancelTask={vi.fn()}
      />,
    )

    fireEvent.click(screen.getByRole('button', { name: 'Pass Review Manually' }))
    expect(screen.getByText(/failed review remains in history/i)).toBeTruthy()
    const confirm = screen.getByRole('button', { name: 'Confirm' })
    expect((confirm as HTMLButtonElement).disabled).toBe(true)
    fireEvent.change(screen.getByLabelText('Reason'), {
      target: { value: 'Provider request and local build were verified by the owner.' },
    })
    fireEvent.click(confirm)

    expect(onRecover).toHaveBeenCalledWith('mark_reviewed', {
      reason: 'Provider request and local build were verified by the owner.',
      context: undefined,
    })
  })

  it('uses the server label for a non-propagating side session', () => {
    render(
      <WorkflowExceptionPanel
        task={task}
        actions={actions}
        recoverPending={false}
        terminal={false}
        cancelPending={false}
        onRecover={vi.fn()}
        onOpenInteractive={vi.fn()}
        onCancelTask={vi.fn()}
      />,
    )

    expect(screen.getByRole('button', { name: 'Open Side Session' })).toBeTruthy()
    expect(screen.queryByRole('button', { name: 'Open Interactive' })).not.toBeTruthy()
  })
})
