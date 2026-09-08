import type { QueryClient } from '@tanstack/react-query'
import { describe, expect, it, vi } from 'vitest'
import { invalidateAnalyticsQueries } from './analytics-query-invalidation'

describe('analytics query invalidation', () => {
  it('invalidates every finite project variant and account usage variant for a project event', () => {
    const invalidateQueries = vi.fn()
    invalidateAnalyticsQueries({ invalidateQueries } as unknown as QueryClient, 'project-1')

    expect(invalidateQueries).toHaveBeenCalledWith({
      queryKey: ['projects', 'project-1', 'analytics'],
    })
    expect(invalidateQueries).toHaveBeenCalledWith({ queryKey: ['analytics', 'usage'] })
  })

  it('invalidates all project analytics scopes when an event has no project id', () => {
    const invalidateQueries = vi.fn()
    invalidateAnalyticsQueries({ invalidateQueries } as unknown as QueryClient)

    const projectCall = invalidateQueries.mock.calls.find(
      ([options]) => typeof options?.predicate === 'function',
    )
    expect(projectCall).toBeDefined()
    const predicate = projectCall?.[0].predicate as (query: { queryKey: unknown[] }) => boolean
    expect(
      predicate({ queryKey: ['projects', 'project-1', 'analytics', '7d', '2026-09-07'] }),
    ).toBe(true)
    expect(predicate({ queryKey: ['projects', 'project-1', 'overview'] })).toBe(false)
    expect(invalidateQueries).toHaveBeenCalledWith({ queryKey: ['analytics', 'usage'] })
  })
})
