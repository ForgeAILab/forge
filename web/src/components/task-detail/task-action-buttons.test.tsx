import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { toast } from 'sonner'
import { ApiError } from '@/api/client'
import type { Offer } from '@/types/generated'
import { TaskActionButtons } from './task-action-buttons'
const { mutate, apiFetch } = vi.hoisted(() => ({ mutate: vi.fn(), apiFetch: vi.fn() }))
vi.mock('@/api/hooks', () => ({ useTaskAction: () => ({ mutate, isPending: false }) }))
vi.mock('@/api/client', async (original) => ({ ...(await original<object>()), apiFetch }))
const offer = (
  action: Offer['action'],
  parameters: Offer['parameters'] = [],
  extra: Partial<Offer> = {},
): Offer => ({
  action,
  parameters,
  authority: ['owner'],
  reason: 'review_failed',
  label: action.verb,
  propagates: false,
  target_execution_id: null,
  ...extra,
})
const mount = (value: Offer) =>
  render(<TaskActionButtons taskId="t" version={5} offers={[value]} />)
beforeEach(() => {
  mutate.mockReset()
  apiFetch.mockReset()
})
describe('descriptor action forms', () => {
  it('requires caller guidance and posts the action and version', () => {
    mount(
      offer({ verb: 'send_back', guidance: '' }, [
        { name: 'guidance', required: true, boolean_values: null },
      ]),
    )
    fireEvent.click(screen.getByRole('button', { name: 'send_back' }))
    expect((screen.getByRole('button', { name: 'Apply' }) as HTMLButtonElement).disabled).toBe(true)
    fireEvent.change(screen.getByLabelText(/guidance/i), {
      target: { value: 'Fix the missing tests' },
    })
    fireEvent.click(screen.getByRole('button', { name: 'Apply' }))
    expect(mutate.mock.calls[0][0]).toEqual({
      taskId: 't',
      version: 5,
      action: { verb: 'send_back', guidance: 'Fix the missing tests' },
    })
  })
  it('requires reason conditionally for approval override and allows only offered booleans', () => {
    mount(
      offer({ verb: 'approve', override: false }, [
        { name: 'override', required: true, boolean_values: [false, true] },
        {
          name: 'reason',
          required: false,
          boolean_values: null,
          required_when: { parameter: 'override', value: true },
        },
      ]),
    )
    fireEvent.click(screen.getByRole('button', { name: 'approve' }))
    const apply = screen.getByRole('button', { name: 'Apply' }) as HTMLButtonElement
    expect(apply.disabled).toBe(false)
    fireEvent.change(screen.getByLabelText(/override checks/i), { target: { value: 'true' } })
    expect(apply.disabled).toBe(true)
    fireEvent.change(screen.getByLabelText(/reason.*required/i), {
      target: { value: 'Verified independently' },
    })
    fireEvent.click(apply)
    expect(mutate.mock.calls[0][0].action).toEqual({
      verb: 'approve',
      override: true,
      reason: 'Verified independently',
    })
  })
  it('confirms subtask propagation even without parameters', () => {
    mount(offer({ verb: 'cancel' }, [], { propagates: true }))
    fireEvent.click(screen.getByRole('button', { name: 'cancel' }))
    expect(screen.getByText('This cancels this Task and its subtasks.')).toBeTruthy()
    expect(mutate).not.toHaveBeenCalled()
  })
  it('renders placement waits and placement retries from offers in any Task state', () => {
    render(
      <TaskActionButtons
        taskId="t"
        version={5}
        offers={[
          offer({ verb: 'hold' }, [], { reason: 'dispatch_wait' }),
          offer(
            { verb: 'retry', refresh_workspace: true },
            [{ name: 'refresh_workspace', required: false, boolean_values: [true] }],
            { reason: 'placement_retry' },
          ),
        ]}
      />,
    )
    fireEvent.click(screen.getByRole('button', { name: 'retry' }))
    expect(screen.queryByRole('combobox')).toBeNull()
    expect(screen.getByText('Refreshes the workspace before retrying')).toBeTruthy()
    fireEvent.click(screen.getByRole('button', { name: 'Apply' }))
    expect(mutate.mock.calls[0][0].action.refresh_workspace).toBe(true)
  })
  it.each(['action_unavailable', 'version_conflict'])(
    'refreshes %s and shows the current choices/version',
    async (code) => {
      apiFetch.mockResolvedValue({ version: 6, available_actions: [offer({ verb: 'hold' })] })
      mutate.mockImplementation((_request, callbacks) =>
        callbacks.onError(
          new ApiError('changed', 409, '', {
            code,
            message: 'changed',
            request_id: '',
          }),
        ),
      )
      mount(offer({ verb: 'start' }))
      fireEvent.click(screen.getByRole('button', { name: 'start' }))
      await waitFor(() =>
        expect(screen.getByRole('status').textContent).toContain('Available now: hold'),
      )
      expect(apiFetch).toHaveBeenCalledWith('/tasks/t/actions')
      fireEvent.click(screen.getByRole('button', { name: 'hold' }))
      expect(mutate.mock.calls[1][0].version).toBe(6)
    },
  )
  it('treats task_busy as accepted and queued, not as changed actions', async () => {
    const info = vi.spyOn(toast, 'info').mockImplementation(() => '')
    const error = vi.spyOn(toast, 'error').mockImplementation(() => '')
    mutate.mockImplementation((_request, callbacks) =>
      callbacks.onError(
        new ApiError('busy', 409, '', {
          code: 'task_busy',
          message: 'Task has pending steps; accepted work remains queued',
          request_id: '',
          details: { pending_steps: 1, retry_after_ms: 250, retry_hint: 'Refetch' },
        }),
      ),
    )
    mount(offer({ verb: 'start' }))
    fireEvent.click(screen.getByRole('button', { name: 'start' }))
    await waitFor(() =>
      expect(info).toHaveBeenCalledWith('Queued; it will apply after the current step'),
    )
    expect(error).not.toHaveBeenCalled()
    expect(apiFetch).not.toHaveBeenCalled()
    expect(screen.queryByRole('status')).toBeNull()
    info.mockRestore()
    error.mockRestore()
  })
  it('requires a reason for a one-shot retry when reset_budget is false', () => {
    mount(
      offer({ verb: 'retry', reset_budget: true }, [
        { name: 'reset_budget', required: false, boolean_values: [true, false] },
        {
          name: 'reason',
          required: false,
          boolean_values: null,
          required_when: { parameter: 'reset_budget', value: false },
        },
      ]),
    )
    fireEvent.click(screen.getByRole('button', { name: 'retry' }))
    fireEvent.change(screen.getByLabelText(/reset budget/i), { target: { value: 'false' } })
    expect((screen.getByRole('button', { name: 'Apply' }) as HTMLButtonElement).disabled).toBe(true)
    fireEvent.change(screen.getByLabelText(/reason.*required/i), {
      target: { value: 'Try the repaired environment once' },
    })
    fireEvent.click(screen.getByRole('button', { name: 'Apply' }))
    expect(mutate.mock.calls[0][0].action).toEqual({
      verb: 'retry',
      reset_budget: false,
      reason: 'Try the repaired environment once',
    })
  })
  it('enforces an unconditional Project Agent cancellation reason', () => {
    mount(
      offer({ verb: 'cancel' }, [{ name: 'reason', required: true, boolean_values: null }], {
        authority: ['project_agent'],
      }),
    )
    fireEvent.click(screen.getByRole('button', { name: 'cancel' }))
    const apply = screen.getByRole('button', { name: 'Apply' }) as HTMLButtonElement
    expect(apply.disabled).toBe(true)
    fireEvent.change(screen.getByLabelText(/reason.*required/i), { target: { value: '   ' } })
    expect(apply.disabled).toBe(true)
    fireEvent.change(screen.getByLabelText(/reason.*required/i), {
      target: { value: 'Replaced by corrected work' },
    })
    fireEvent.click(apply)
    expect(mutate.mock.calls[0][0].action.reason).toBe('Replaced by corrected work')
  })
  it('uses the returned Task offers and version after a successful row action', () => {
    mutate.mockImplementationOnce((_request, callbacks) =>
      callbacks.onSuccess({ version: 6, available_actions: [offer({ verb: 'hold' })] }),
    )
    mount(offer({ verb: 'start' }))
    fireEvent.click(screen.getByRole('button', { name: 'start' }))
    expect(screen.queryByRole('button', { name: 'start' })).toBeNull()
    fireEvent.click(screen.getByRole('button', { name: 'hold' }))
    expect(mutate.mock.calls[1][0]).toEqual({ taskId: 't', version: 6, action: { verb: 'hold' } })
  })
  it('drops a cleared optional reason before posting', () => {
    mount(offer({ verb: 'cancel' }, [{ name: 'reason', required: false, boolean_values: null }]))
    fireEvent.click(screen.getByRole('button', { name: 'cancel' }))
    fireEvent.change(screen.getByLabelText(/reason/i), { target: { value: '  ' } })
    fireEvent.click(screen.getByRole('button', { name: 'Apply' }))
    expect(mutate.mock.calls[0][0].action).toEqual({ verb: 'cancel' })
  })
  it.each(['success', 'conflict'])(
    'keeps the caller filter and fixed fresh session after %s',
    async (result) => {
      const retry = offer({ verb: 'retry', fresh_session: false }, [
        { name: 'fresh_session', required: false, boolean_values: [false, true] },
      ])
      const next = { version: 6, available_actions: [retry, offer({ verb: 'cancel' })] }
      mutate.mockImplementationOnce((_request, callbacks) =>
        result === 'success'
          ? callbacks.onSuccess(next)
          : callbacks.onError(new ApiError('changed', 409, '')),
      )
      apiFetch.mockResolvedValue(next)
      const fresh = (offers: Offer[]) =>
        offers
          .filter((item) => item.action.verb === 'retry')
          .map((item) => ({
            ...item,
            action: { ...item.action, fresh_session: true } as Offer['action'],
            parameters: item.parameters.map((spec) =>
              spec.name === 'fresh_session' ? { ...spec, boolean_values: [true] } : spec,
            ),
          }))
      render(<TaskActionButtons taskId="t" version={5} offers={[retry]} transformOffers={fresh} />)
      fireEvent.click(screen.getByRole('button', { name: 'retry' }))
      fireEvent.click(screen.getByRole('button', { name: 'Apply' }))
      await waitFor(() => expect(screen.queryByRole('button', { name: 'Apply' })).toBeNull())
      expect(screen.queryByRole('button', { name: 'cancel' })).toBeNull()
      fireEvent.click(screen.getByRole('button', { name: 'retry' }))
      expect(screen.queryByRole('combobox')).toBeNull()
      fireEvent.click(screen.getByRole('button', { name: 'Apply' }))
      expect(mutate.mock.calls[1][0]).toEqual({
        taskId: 't',
        version: 6,
        action: { verb: 'retry', fresh_session: true },
      })
    },
  )
})
