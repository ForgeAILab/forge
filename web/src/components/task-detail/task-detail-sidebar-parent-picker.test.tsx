import { createElement, type PropsWithChildren } from 'react'
import { act, cleanup, renderHook, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { filterParentTaskCandidates, useParentTaskCandidatesQuery } from './task-detail-sidebar'
import { useAuthStore } from '@/stores/auth'
import type { Task } from '@/types/generated'

describe('parent task candidate pagination', () => {
  afterEach(() => {
    cleanup()
    vi.restoreAllMocks()
    useAuthStore.getState().clearAuth()
    localStorage.clear()
  })

  it('uses debounced server search and fetches only the next bounded page on demand', async () => {
    useAuthStore.setState({ accessToken: 'access-token', refreshToken: 'refresh-token' })
    const fetchMock = vi.spyOn(window, 'fetch').mockImplementation(async (input) => {
      const url = new URL(input instanceof URL ? input.href : String(input))
      const cursor = url.searchParams.get('cursor')
      const body =
        cursor === 'cursor-2'
          ? { items: [], next_cursor: null, has_more: false, total_count: null }
          : { items: [], next_cursor: 'cursor-2', has_more: true, total_count: null }
      return new Response(JSON.stringify(body), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      })
    })
    const queryClient = new QueryClient({
      defaultOptions: {
        queries: { retry: false, gcTime: 0 },
        mutations: { retry: false },
      },
    })
    const wrapper = ({ children }: PropsWithChildren) =>
      createElement(QueryClientProvider, { client: queryClient }, children)
    const { result } = renderHook(
      () => useParentTaskCandidatesQuery('project-1', 'release notes', true),
      { wrapper },
    )

    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(1))
    const firstPageUrl = new URL(String(fetchMock.mock.calls[0][0]))
    expect(firstPageUrl.pathname).toBe('/api/v1/projects/project-1/tasks')
    expect(firstPageUrl.searchParams.get('q')).toBe('release notes')
    expect(firstPageUrl.searchParams.get('limit')).toBe('30')
    expect(firstPageUrl.searchParams.has('cursor')).toBe(false)

    await act(async () => {
      await result.current.fetchNextPage()
    })

    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2))
    const nextPageUrl = new URL(String(fetchMock.mock.calls[1][0]))
    expect(nextPageUrl.searchParams.get('cursor')).toBe('cursor-2')
    expect(nextPageUrl.searchParams.get('limit')).toBe('30')
    expect(nextPageUrl.searchParams.get('q')).toBe('release notes')
  })

  it('keeps server matches whose description, rather than title, matched the search', () => {
    const candidates = [
      {
        id: 'description-match',
        title: 'Prepare launch',
        description: 'Release notes',
        parent_task_id: null,
      },
      { id: 'child', title: 'Release notes child', parent_task_id: 'parent' },
      { id: 'current', title: 'Release notes task', parent_task_id: null },
    ] as Task[]

    expect(filterParentTaskCandidates(candidates, 'current').map((task) => task.id)).toEqual([
      'description-match',
    ])
  })
})
