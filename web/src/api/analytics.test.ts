import { afterEach, describe, expect, it, vi } from 'vitest'
import { getAccountUsageAnalytics, getProjectAnalytics } from './client'

describe('analytics API client', () => {
  afterEach(() => {
    vi.restoreAllMocks()
  })

  it('percent-encodes RFC3339 plus offsets for the unchanged Project route', async () => {
    const fetchMock = vi.spyOn(window, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({}), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    )
    const from = '2026-09-01T00:00:00+05:30'
    const to = '2026-09-07T00:00:00+05:30'

    await getProjectAnalytics('project-1', from, to)

    const [input] = fetchMock.mock.calls[0]
    const url = input as URL
    expect(url.pathname).toBe('/api/v1/projects/project-1/analytics')
    expect(url.search).toContain('from=2026-09-01T00%3A00%3A00%2B05%3A30')
    expect(url.search).toContain('to=2026-09-07T00%3A00%3A00%2B05%3A30')
    expect(url.searchParams.get('from')).toBe(from)
    expect(url.searchParams.get('to')).toBe(to)
  })

  it('requests account usage at the dedicated endpoint with the same encoding', async () => {
    const fetchMock = vi.spyOn(window, 'fetch').mockResolvedValue(
      new Response(JSON.stringify({}), {
        status: 200,
        headers: { 'content-type': 'application/json' },
      }),
    )
    const from = '2026-09-01T00:00:00+05:30'

    await getAccountUsageAnalytics(from)

    const [input] = fetchMock.mock.calls[0]
    const url = input as URL
    expect(url.pathname).toBe('/api/v1/analytics/usage')
    expect(url.search).toContain('from=2026-09-01T00%3A00%3A00%2B05%3A30')
    expect(url.searchParams.get('from')).toBe(from)
  })
})
