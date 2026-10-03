import type { ReactNode } from 'react'
import { fireEvent, render, screen } from '@testing-library/react'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { ForgeSettingsPage } from './ForgeSettingsPage'

const mutate = vi.hoisted(() => vi.fn())
const toastError = vi.hoisted(() => vi.fn())
vi.mock('@/api/hooks', () => {
  const query = {
    isLoading: false,
    data: {
      config_path: 'forge.yaml',
      restart_required: false,
      settings: [
        {
          key: 'server.bind',
          value: '127.0.0.1:8080',
          effective_value: '127.0.0.1:8080',
          restart_required: false,
        },
        { key: 'server.mcp_enabled', value: true, effective_value: true, restart_required: false },
        {
          key: 'server.max_concurrent_runs',
          value: null,
          effective_value: 4,
          restart_required: false,
        },
        { key: 'server.logical_cores', value: 8, effective_value: 8, restart_required: false },
        {
          key: 'server.build_jobs_per_run',
          value: null,
          effective_value: 2,
          restart_required: false,
        },
        { key: 'server.run_nice', value: 10, effective_value: 10, restart_required: false },
        {
          key: 'server.usage_index_budget_mb',
          value: null,
          effective_value: 128,
          restart_required: false,
        },
      ],
    },
  }
  return {
    useSettingsQuery: () => query,
    useUpdateSettings: () => ({ mutate, isPending: false }),
  }
})
vi.mock('@tanstack/react-router', () => ({
  Link: ({ children }: { children: ReactNode }) => <a href="#settings">{children}</a>,
}))
vi.mock('sonner', () => ({ toast: { error: toastError, success: vi.fn() } }))

describe('ForgeSettingsPage run resources', () => {
  beforeEach(() => vi.clearAllMocks())
  it('shows cores, effective run cap, automatic jobs and niceness', () => {
    render(<ForgeSettingsPage />)
    expect(screen.getByText('Automatic (4)')).toBeTruthy()
    expect(screen.getByText('8 logical cores · Automatic: 2 jobs per run')).toBeTruthy()
    expect(screen.getByText('Configured increment: 10')).toBeTruthy()
  })
  it('saves explicit and disabled budgets and priority', () => {
    render(<ForgeSettingsPage />)
    fireEvent.change(screen.getByLabelText('Build jobs per run'), { target: { value: '3' } })
    fireEvent.change(screen.getByLabelText('Run niceness'), { target: { value: '7' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))
    expect(mutate.mock.calls[0][0].server).toMatchObject({ build_jobs_per_run: 3, run_nice: 7 })
    fireEvent.change(screen.getByLabelText('Build jobs per run'), { target: { value: '0' } })
    fireEvent.change(screen.getByLabelText('Run niceness'), { target: { value: '0' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))
    expect(mutate.mock.calls[1][0].server).toMatchObject({ build_jobs_per_run: 0, run_nice: 0 })
  })
  it('shows and saves the usage budget alongside build jobs and niceness', () => {
    render(<ForgeSettingsPage />)
    expect(screen.getByText('In effect: 128 MiB')).toBeTruthy()
    fireEvent.change(screen.getByLabelText('Usage index memory budget'), {
      target: { value: '64' },
    })
    fireEvent.change(screen.getByLabelText('Build jobs per run'), { target: { value: '3' } })
    fireEvent.change(screen.getByLabelText('Run niceness'), { target: { value: '7' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))
    expect(mutate.mock.calls[0][0].server).toMatchObject({
      usage_index_budget_mb: 64,
      build_jobs_per_run: 3,
      run_nice: 7,
    })
    fireEvent.change(screen.getByLabelText('Usage index memory budget'), { target: { value: '0' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))
    expect(mutate.mock.calls[1][0].server).toMatchObject({
      usage_index_budget_mb: 0,
      build_jobs_per_run: 3,
      run_nice: 7,
    })
    fireEvent.change(screen.getByLabelText('Usage index memory budget'), { target: { value: '' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))
    expect(mutate.mock.calls[2][0].server).toMatchObject({
      usage_index_budget_mb: null,
      build_jobs_per_run: 3,
      run_nice: 7,
    })
    mutate.mockClear()
    fireEvent.change(screen.getByLabelText('Usage index memory budget'), {
      target: { value: '-1' },
    })
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))
    expect(mutate).not.toHaveBeenCalled()
    expect(toastError).toHaveBeenCalledWith(
      'Usage index budget must be a non-negative integer in MiB',
    )
  })
  it('saves blank jobs as automatic and rejects invalid values', () => {
    render(<ForgeSettingsPage />)
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))
    expect(mutate.mock.calls[0][0].server).toMatchObject({ build_jobs_per_run: null, run_nice: 10 })
    mutate.mockClear()
    fireEvent.change(screen.getByLabelText('Run niceness'), { target: { value: '20' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))
    expect(mutate).not.toHaveBeenCalled()
    expect(toastError).toHaveBeenCalledWith('Run niceness must be an integer from 0 to 19')
    fireEvent.change(screen.getByLabelText('Run niceness'), { target: { value: '10' } })
    fireEvent.change(screen.getByLabelText('Build jobs per run'), { target: { value: '-1' } })
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))
    expect(mutate).not.toHaveBeenCalled()
  })
})
