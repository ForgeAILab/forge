import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { ApiError } from '@/api/client'
import { BulkCancelTasks, TaskRowActions } from './task-list-actions'
const { apiFetch, mutateAsync } = vi.hoisted(() => ({ apiFetch: vi.fn(), mutateAsync: vi.fn() }))
vi.mock('@/api/client', async (original) => ({ ...(await original<object>()), apiFetch }))
vi.mock('@/api/hooks', () => ({
  useTaskAction: () => ({ mutateAsync, mutate: vi.fn(), isPending: false }),
}))
const cancel = {
  action: { verb: 'cancel' },
  parameters: [],
  label: 'Cancel Task',
  reason: 'task_cancel',
  authority: ['owner'],
  propagates: true,
  target_execution_id: null,
}
beforeEach(() => {
  apiFetch.mockReset()
  mutateAsync.mockReset()
  mutateAsync.mockResolvedValue({})
})
describe('list offers on demand', () => {
  it('loads row offers only when opened', async () => {
    apiFetch.mockResolvedValue({ version: 8, available_actions: [cancel] })
    render(<TaskRowActions taskId="t" />)
    expect(apiFetch).not.toHaveBeenCalled()
    fireEvent.click(screen.getByRole('button', { name: 'Actions' }))
    await screen.findByRole('button', { name: 'Cancel Task' })
    expect(apiFetch).toHaveBeenCalledWith('/tasks/t/actions')
  })
  it('bulk cancellation skips missing offers, confirms propagation and uses fetched versions', async () => {
    apiFetch.mockImplementation((path: string) =>
      Promise.resolve({ version: 9, available_actions: path.includes('/a/') ? [cancel] : [] }),
    )
    const completed = vi.fn()
    render(
      <BulkCancelTasks
        tasks={[
          { id: 'a', title: 'Waiting for machine' },
          { id: 'b', title: 'Custom state' },
        ]}
        onComplete={completed}
      />,
    )
    expect(apiFetch).not.toHaveBeenCalled()
    fireEvent.click(screen.getByRole('button', { name: 'Cancel selected' }))
    await screen.findByText('1 selected Tasks do not offer cancel.')
    expect(screen.getByText('This cancels this Task and its subtasks.')).toBeTruthy()
    fireEvent.click(screen.getByRole('button', { name: 'Cancel 1 Tasks' }))
    await waitFor(() => expect(completed).toHaveBeenCalled())
    expect(mutateAsync).toHaveBeenCalledExactlyOnceWith({
      taskId: 'a',
      version: 9,
      action: { verb: 'cancel' },
    })
  })
  it('refreshes and reports the available offers after bulk refusal', async () => {
    apiFetch
      .mockResolvedValueOnce({ version: 3, available_actions: [cancel] })
      .mockResolvedValueOnce({
        version: 4,
        available_actions: [{ ...cancel, action: { verb: 'hold' }, label: 'Hold' }],
      })
    mutateAsync.mockRejectedValue(new Error('action unavailable'))
    render(<BulkCancelTasks tasks={[{ id: 'a', title: 'Task A' }]} onComplete={vi.fn()} />)
    fireEvent.click(screen.getByRole('button', { name: 'Cancel selected' }))
    await screen.findByRole('button', { name: 'Cancel 1 Tasks' })
    fireEvent.click(screen.getByRole('button', { name: 'Cancel 1 Tasks' }))
    await screen.findByText('Available now: Task A: Hold')
    expect(apiFetch).toHaveBeenCalledTimes(2)
  })
  it('skips selected children covered by root cancellation', async () => {
    apiFetch.mockResolvedValue({ version: 1, available_actions: [cancel] })
    const completed = vi.fn()
    render(
      <BulkCancelTasks
        tasks={[
          { id: 'child', title: 'Child', parent_task_id: 'root' },
          { id: 'root', title: 'Root' },
        ]}
        onComplete={completed}
      />,
    )
    fireEvent.click(screen.getByRole('button', { name: 'Cancel selected' }))
    fireEvent.click(await screen.findByRole('button', { name: 'Cancel 1 Tasks' }))
    await waitFor(() => expect(completed).toHaveBeenCalled())
    expect(mutateAsync).toHaveBeenCalledExactlyOnceWith({
      taskId: 'root',
      version: 1,
      action: { verb: 'cancel' },
    })
  })
  it('accepts a cancellation conflict already settled by propagation', async () => {
    apiFetch
      .mockResolvedValueOnce({ version: 1, available_actions: [cancel] })
      .mockResolvedValueOnce({ version: 2, available_actions: [] })
    mutateAsync.mockRejectedValueOnce(new ApiError('changed', 409, ''))
    const completed = vi.fn()
    render(<BulkCancelTasks tasks={[{ id: 'child', title: 'Child' }]} onComplete={completed} />)
    fireEvent.click(screen.getByRole('button', { name: 'Cancel selected' }))
    fireEvent.click(await screen.findByRole('button', { name: 'Cancel 1 Tasks' }))
    await waitFor(() => expect(completed).toHaveBeenCalled())
    expect(apiFetch).toHaveBeenCalledTimes(2)
  })
})
