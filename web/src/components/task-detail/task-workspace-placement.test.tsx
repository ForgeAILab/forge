import { act, cleanup, render, screen } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { serverPlacement } from '@/test-utils/placement'
import { TaskWorkspacePlacement } from './task-workspace-placement'

const { useDaemonsQuery } = vi.hoisted(() => ({ useDaemonsQuery: vi.fn() }))
vi.mock('@/api/hooks', () => ({ useDaemonsQuery }))

describe('Task workspace placement', () => {
  beforeEach(() => {
    useDaemonsQuery.mockReturnValue({ data: { items: [{ id: 'daemon-1', hostname: 'Mac Studio' }] } })
  })

  afterEach(() => {
    cleanup()
    vi.useRealTimers()
    vi.clearAllMocks()
  })

  it.each(['reserved', 'preparing', 'ready', 'disconnected', 'cleaning', 'cleaned', 'failed'])(
    'labels the owner and %s state', (state) => {
      render(<TaskWorkspacePlacement placement={serverPlacement({ state })} />)
      expect(screen.getByText('Server')).toBeTruthy()
      expect(screen.getByText(state)).toBeTruthy()
      expect(screen.getByRole('status').getAttribute('aria-label')).toBe('Workspace placement')
      expect(useDaemonsQuery).toHaveBeenCalledWith(false)
    },
  )

  it('uses the placement owner’s daemon name', () => {
    render(<TaskWorkspacePlacement placement={serverPlacement({
      owner_kind: 'daemon', daemon_id: 'daemon-1',
    })} />)
    expect(screen.getByText('Mac Studio')).toBeTruthy()
    expect(useDaemonsQuery).toHaveBeenCalledWith(true)
  })

  it('keeps the owner identifiable if daemon metadata cannot load', () => {
    useDaemonsQuery.mockReturnValue({ data: undefined })
    render(<TaskWorkspacePlacement placement={serverPlacement({
      owner_kind: 'daemon', daemon_id: 'daemon-1',
    })} />)
    expect(screen.getByText('Daemon daemon-1')).toBeTruthy()
  })

  it('updates disconnect duration and explains waiting and existing recovery choices', () => {
    vi.useFakeTimers()
    vi.setSystemTime(new Date('2026-09-30T12:10:00Z'))
    const placement = serverPlacement({
      owner_kind: 'daemon', daemon_id: 'daemon-1', state: 'disconnected',
      disconnected_at: '2026-09-30T12:00:00Z',
    })
    const { rerender } = render(<TaskWorkspacePlacement placement={placement} />)
    expect(screen.getByText('Disconnected for 10m')).toBeTruthy()
    expect(screen.getByText('This Task waits for its owner to reconnect.')).toBeTruthy()
    expect(screen.getByText('Retry stays on the same owner. You can also cancel the Task.')).toBeTruthy()
    expect(screen.queryByRole('button')).toBeNull()

    act(() => {
      vi.advanceTimersByTime(60_000)
    })
    expect(screen.getByText('Disconnected for 11m')).toBeTruthy()
    rerender(<TaskWorkspacePlacement placement={{ ...placement, state: 'ready', disconnected_at: null }} />)
    expect(screen.queryByText(/Disconnected for/)).toBeNull()
    expect(vi.getTimerCount()).toBe(0)
  })

  it.each([null, 'invalid date'])('handles a missing or invalid disconnect timestamp (%s)', (disconnected_at) => {
    render(<TaskWorkspacePlacement placement={serverPlacement({ state: 'disconnected', disconnected_at })} />)
    expect(screen.getByText('Disconnect time unavailable')).toBeTruthy()
    expect(screen.getByText('This Task waits for its owner to reconnect.')).toBeTruthy()
  })

  it('omits placement before workspace admission', () => {
    render(<TaskWorkspacePlacement placement={null} />)
    expect(screen.queryByRole('status')).toBeNull()
    expect(useDaemonsQuery).toHaveBeenCalledWith(false)
  })
})
