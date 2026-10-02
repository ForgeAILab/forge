import { fireEvent, render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { Offer, Task } from '@/types/generated'
import { WorkflowExceptionPanel } from './workflow-exception-panel'
const { mutate } = vi.hoisted(() => ({ mutate: vi.fn() }))
vi.mock('@/api/hooks', () => ({ useTaskAction: () => ({ mutate, isPending: false }) }))
vi.mock('@tanstack/react-router', () => ({ Link: ({ children }: { children: React.ReactNode }) => <a href="#evidence">{children}</a> }))
function task(offers: Offer[]): Task { return { id: 'task', version: 7, status: 'review', workflow_exception: { type: 'review_failed', message: 'Checks failed', actions: offers, review_id: null, execution_id: null, state: 'review', role: null, target_state: null, target_role: null, failing_step: null, related_evidence: [] } } as unknown as Task }
const retry: Offer = { action: { verb: 'retry' }, parameters: [], authority: ['owner'], reason: 'review_failed', label: 'Retry Review', target_execution_id: null }
beforeEach(() => mutate.mockReset())
describe('WorkflowExceptionPanel offers', () => {
  it('renders and applies the supplied offer at the Task version', () => { render(<WorkflowExceptionPanel task={task([retry])} />); fireEvent.click(screen.getByRole('button', { name: 'Retry Review' })); expect(mutate.mock.calls[0][0]).toEqual({ taskId: 'task', action: { verb: 'retry' }, version: 7 }) })
  it('does not invent recovery or cancellation controls for an empty set', () => { render(<WorkflowExceptionPanel task={task([])} />); expect(screen.queryByRole('button')).toBeNull() })
  it('collects guidance only when the offer declares that parameter', () => {
    render(<WorkflowExceptionPanel task={task([{ ...retry, parameters: [{ name: 'guidance', required: false, boolean_values: null }] }])} />)
    fireEvent.click(screen.getByRole('button', { name: 'Retry Review' })); fireEvent.change(screen.getByLabelText('Guidance'), { target: { value: 'Environment repaired' } }); fireEvent.click(screen.getByRole('button', { name: 'Apply' })); expect(mutate.mock.calls[0][0].action).toEqual({ verb: 'retry', guidance: 'Environment repaired' })
  })
  it('renders the server cancellation offer without checking Task state', () => { render(<WorkflowExceptionPanel task={task([{ ...retry, action: { verb: 'cancel' }, label: 'Cancel Task' }])} />); expect(screen.getByRole('button', { name: 'Cancel Task' })).toBeTruthy() })
})
