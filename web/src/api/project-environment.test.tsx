import type { ReactNode } from 'react'
import { act, renderHook } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { recheckProjectEnvironment } from './client'
import { useRecheckProjectEnvironment } from './hooks'
import { qk } from './query-keys'

afterEach(() => vi.restoreAllMocks())

describe('Project environment re-check', () => {
  it('POSTs an empty request and returns each check result with the updated project', async () => {
    const response = {
      checks: [{ name: 'disk', passed: true, exit_code: 0, output_tail: 'root free: 17G' }],
      project: { id: 'project-1', paused: false, environment_pause: null },
    }
    const fetchMock = vi.spyOn(window, 'fetch').mockResolvedValue(
      new Response(JSON.stringify(response), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    )
    expect(await recheckProjectEnvironment('project-1')).toEqual(response)
    const [url, init] = fetchMock.mock.calls[0]
    expect((url as URL).pathname).toBe('/api/v1/projects/project-1/environment/recheck')
    expect(init?.method).toBe('POST')
    expect(init?.body).toBe('{}')
  })

  it('invalidates the project and project list after a successful re-check', async () => {
    vi.spyOn(window, 'fetch').mockResolvedValue(
      new Response(
        JSON.stringify({
          checks: [],
          project: { id: 'project-1' },
        }),
        { status: 200, headers: { 'content-type': 'application/json' } },
      ),
    )
    const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    client.setQueryData(qk.project('project-1'), {})
    client.setQueryData(qk.projects, {})
    client.setQueryData(qk.projectPages(20), {})
    const invalidate = vi.spyOn(client, 'invalidateQueries')
    const wrapper = ({ children }: { children: ReactNode }) => (
      <QueryClientProvider client={client}>{children}</QueryClientProvider>
    )
    const { result, unmount } = renderHook(() => useRecheckProjectEnvironment(), { wrapper })
    await act(async () => {
      await result.current.mutateAsync('project-1')
    })
    expect(invalidate).toHaveBeenCalledWith({ queryKey: qk.project('project-1') })
    expect(invalidate).toHaveBeenCalledWith({ queryKey: qk.projects })
    expect(client.getQueryState(qk.projectPages(20))?.isInvalidated).toBe(true)
    unmount()
    client.clear()
  })
})
