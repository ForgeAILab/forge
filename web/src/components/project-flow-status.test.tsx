import { act, fireEvent, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import type { Project } from '@/types/generated'
import type { ProjectEnvironmentCheckResult } from '@/types/generated/bindings/ProjectEnvironmentCheckResult'
import {
  ProjectEnvironmentPauseNotice,
  ProjectFlowHeader,
  ProjectSlotUsage,
} from './project-flow-status'

const mutation = vi.hoisted(() => ({
  mutate: vi.fn(),
  isPending: false,
  isError: false,
  error: null as Error | null,
  data: undefined as { checks: ProjectEnvironmentCheckResult[]; project: Project } | undefined,
}))
const query = vi.hoisted(() => ({ data: undefined as Project | undefined }))
vi.mock('@/api/hooks', () => ({
  useRecheckProjectEnvironment: () => mutation,
  useProjectQuery: () => query,
}))

const pausedProject = {
  id: 'project-1',
  name: 'NovelKit',
  paused: true,
  system_pause_reason: 'environment_not_ready',
  slots: { limit: 5, active: 4, parked: 3, queued: 7 },
  environment_pause: {
    checks: ['disk', 'browser'],
    role: 'coder',
    output: 'root free: 7G',
    paused_at: '2026-09-30T12:00:00Z',
    last_checked_at: '2026-09-30T12:00:00Z',
    next_check_at: '2026-09-30T12:10:00Z',
  },
} as Project

beforeEach(() => {
  vi.spyOn(Date, 'now').mockReturnValue(Date.parse('2026-09-30T12:02:00Z'))
  mutation.mutate.mockReset()
  mutation.isPending = false
  mutation.isError = false
  mutation.error = null
  mutation.data = undefined
  query.data = undefined
})
afterEach(() => {
  vi.restoreAllMocks()
  vi.useRealTimers()
})

describe('Project flow status', () => {
  it('shows the paused checks, output disclosure, next check and Check now in the header', () => {
    query.data = pausedProject
    render(<ProjectFlowHeader projectId="project-1" />)
    expect(screen.getByText('Environment paused')).toBeTruthy()
    expect(screen.getByText(/disk, browser/)).toBeTruthy()
    expect(screen.getByText(/next check in 8m/)).toBeTruthy()
    expect(screen.getByText('Active 4/5 · Parked 3 · Queued 7')).toBeTruthy()
    const output = screen.getByText('Output tail').closest('details')!
    expect(output.open).toBe(false)
    expect(output.querySelector('pre')?.textContent).toContain('root free: 7G')
    fireEvent.click(screen.getByRole('button', { name: 'Check now' }))
    expect(mutation.mutate).toHaveBeenCalledWith('project-1')
  })

  it('updates relative time while the project remains paused', () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-09-30T12:02:00Z'))
    vi.spyOn(Date, 'now').mockImplementation(() => new Date().getTime())
    render(<ProjectEnvironmentPauseNotice project={pausedProject} />)
    act(() => vi.advanceTimersByTime(60_000))
    expect(screen.getByText(/next check in 7m/)).toBeTruthy()
  })

  it('shows per-check failure results and prevents a second request while checking', () => {
    mutation.isPending = true
    mutation.data = {
      project: pausedProject,
      checks: [
        { name: 'disk', passed: false, exit_code: 1, output_tail: 'root free: 7G' },
        { name: 'browser', passed: true, exit_code: 0, output_tail: '' },
      ],
    }
    render(<ProjectEnvironmentPauseNotice project={pausedProject} />)
    expect(screen.getByText('Environment is still paused.')).toBeTruthy()
    expect(screen.getByText('disk: Failed (exit 1)')).toBeTruthy()
    expect(screen.getByText('browser: Passed (exit 0)')).toBeTruthy()
    expect((screen.getByRole('button', { name: 'Checking…' }) as HTMLButtonElement).disabled).toBe(
      true,
    )
  })

  it('retains the successful results after the header refreshes to resumed', () => {
    const resumed = {
      ...pausedProject,
      paused: false,
      system_pause_reason: null,
      environment_pause: null,
    }
    mutation.data = {
      project: resumed,
      checks: [{ name: 'disk', passed: true, exit_code: 0, output_tail: 'root free: 17G' }],
    }
    render(<ProjectEnvironmentPauseNotice project={resumed} />)
    expect(screen.queryByText('Environment paused')).toBeNull()
    expect(screen.getByText('Checks passed. Project is resumed.')).toBeTruthy()
    expect(screen.getByText('disk: Passed (exit 0)')).toBeTruthy()
  })

  it('shows a request error inline and leaves Check now available for retry', () => {
    mutation.isError = true
    mutation.error = new Error('Primary checkout unavailable')
    render(<ProjectEnvironmentPauseNotice project={pausedProject} />)
    expect(screen.getByRole('alert').textContent).toBe('Primary checkout unavailable')
    expect((screen.getByRole('button', { name: 'Check now' }) as HTMLButtonElement).disabled).toBe(
      false,
    )
  })

  it.each([null, 'missing_repository', 'repository_not_ready'])(
    'does not label a %s pause as environment-paused',
    (reason) => {
      render(
        <ProjectEnvironmentPauseNotice
          project={{ ...pausedProject, system_pause_reason: reason, environment_pause: null }}
        />,
      )
      expect(screen.queryByText('Environment paused')).toBeNull()
      expect(screen.queryByRole('button', { name: 'Check now' })).toBeNull()
    },
  )

  it('shows admitted recovery over the limit and the parked-owner guard', () => {
    render(<ProjectSlotUsage slots={{ limit: 5, active: 6, parked: 10, queued: 7 }} />)
    expect(screen.getByText('Active 6/5 · Parked 10 · Queued 7')).toBeTruthy()
    expect(screen.getByText('waiting on you: 10 parked')).toBeTruthy()
  })

  it('shows unlimited usage without a parked guard', () => {
    render(<ProjectSlotUsage slots={{ limit: 0, active: 4, parked: 100, queued: 7 }} />)
    expect(screen.getByText('Active 4 (no limit) · Parked 100 · Queued 7')).toBeTruthy()
    expect(screen.queryByText(/waiting on you/)).toBeNull()
  })
})
