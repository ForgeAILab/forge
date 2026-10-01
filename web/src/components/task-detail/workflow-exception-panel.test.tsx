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
  effective_coder: null,
  effective_coder_source: null,
  remaining_retries: {},
  placement: null,
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
  function renderOwnerPanel(message: string, overrides: Partial<WorkflowExceptionAction> = {}) {
    const onRecover = vi.fn()
    const onOpenInteractive = vi.fn()
    const onCancelTask = vi.fn()
    const ownerActions = [
      action('reexecute', 'Retry Review with Guidance', { requires_guidance: true }),
      action('mark_reviewed', 'Pass Review Manually', { requires_reason: true }),
      action('defer_to_follow_up', 'Defer to Follow-up Task', {
        requires_reason: true,
        ...overrides,
      }),
      action('open_interactive', 'Open Side Session'),
      action('cancel_task', 'Cancel Task'),
    ]
    render(
      <WorkflowExceptionPanel
        task={{
          ...task,
          workflow_exception: {
            ...task.workflow_exception!,
            type: 'review_needs_owner',
            message,
            actions: ownerActions,
          },
        }}
        actions={ownerActions}
        recoverPending={false}
        terminal={false}
        cancelPending={false}
        onRecover={onRecover}
        onOpenInteractive={onOpenInteractive}
        onCancelTask={onCancelTask}
      />,
    )
    return { onRecover, onOpenInteractive, onCancelTask }
  }

  it.each(['fixable by owner', 'repeated finding'])(
    'shows Needs owner with a %s badge and every owner action',
    (badge) => {
      renderOwnerPanel(`${badge}: Forge linked_documents is empty`)
      expect(screen.getByText('Needs owner')).toBeTruthy()
      expect(screen.getByText(badge)).toBeTruthy()
      expect(screen.getByText(`${badge}: Forge linked_documents is empty`)).toBeTruthy()
      for (const name of [
        'Retry with guidance',
        'Mark reviewed',
        'Defer to follow-up',
        'Open interactive',
        'Cancel Task',
      ]) {
        expect(screen.getByRole('button', { name })).toBeTruthy()
      }
    },
  )

  it('requires a nonblank reason to defer the finding to a follow-up', () => {
    const { onRecover } = renderOwnerPanel('fixable by owner: macOS measurements missing', {
      requires_reason: false,
    })
    fireEvent.click(screen.getByRole('button', { name: 'Defer to follow-up' }))
    expect(screen.getByText(/creates a linked backlog task/)).toBeTruthy()
    const confirm = screen.getByRole('button', { name: 'Confirm' }) as HTMLButtonElement
    expect(confirm.disabled).toBe(true)
    fireEvent.change(screen.getByLabelText('Reason'), { target: { value: '   ' } })
    expect(confirm.disabled).toBe(true)
    expect(onRecover).not.toHaveBeenCalled()
    fireEvent.change(screen.getByLabelText('Reason'), {
      target: { value: ' macOS/Windows runs need a human ' },
    })
    fireEvent.click(confirm)
    expect(onRecover).toHaveBeenCalledWith('defer_to_follow_up', {
      reason: 'macOS/Windows runs need a human',
      context: undefined,
    })
  })

  it('routes owner retry guidance, interactive and cancel through the existing handlers', () => {
    const { onRecover, onOpenInteractive, onCancelTask } = renderOwnerPanel(
      'repeated finding: external endpoint missing',
    )
    fireEvent.click(screen.getByRole('button', { name: 'Retry with guidance' }))
    fireEvent.change(screen.getByLabelText('Guidance'), {
      target: { value: 'The endpoint is available now.' },
    })
    fireEvent.click(screen.getByRole('button', { name: 'Confirm' }))
    expect(onRecover).toHaveBeenCalledWith('reexecute', {
      reason: undefined,
      context: 'The endpoint is available now.',
    })
    fireEvent.click(screen.getByRole('button', { name: 'Open interactive' }))
    expect(onOpenInteractive).toHaveBeenCalledWith(
      expect.objectContaining({ kind: 'open_interactive' }),
    )
    fireEvent.click(screen.getByRole('button', { name: 'Cancel Task' }))
    expect(onCancelTask).toHaveBeenCalledOnce()
  })

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
