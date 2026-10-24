import { QueryClient, QueryObserver } from '@tanstack/react-query'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { invalidateAfterFetch } from './invalidate-after-fetch'

let client: QueryClient
afterEach(() => {
  client?.clear()
  vi.restoreAllMocks()
})

async function flushMicrotasks() {
  for (let i = 0; i < 30; i++) await Promise.resolve()
}

function setup() {
  client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  let release!: (value: number) => void
  const held = new Promise<number>((resolve) => {
    release = resolve
  })
  const queryFn = vi.fn().mockReturnValueOnce(held).mockResolvedValue(2)
  const queryKey = ['projects', 'p', 'tasks', 'candidates']
  const observer = new QueryObserver(client, { queryKey, queryFn, initialData: 0 })
  const unsubscribe = observer.subscribe(() => {})
  const original = client.invalidateQueries.bind(client)
  const invalidations = vi.spyOn(client, 'invalidateQueries').mockImplementation((...args) => {
    // Bound the broken microtask chain so a regression cannot hang the runner.
    if (invalidations.mock.calls.length > 5) return new Promise<void>(() => {})
    return original(...args)
  })
  return {
    queryKey,
    queryFn,
    observer,
    unsubscribe,
    release,
    invalidations,
    held,
    invalidateOriginal: original,
  }
}

describe('invalidateAfterFetch', () => {
  it.each(['unmounted', 'disabled'] as const)(
    'terminates for an %s query mid-fetch',
    async (mode) => {
      const test = setup()
      if (mode === 'unmounted') test.unsubscribe()
      else
        test.observer.setOptions({ queryKey: test.queryKey, queryFn: test.queryFn, enabled: false })
      expect(client.getQueryState(test.queryKey)?.fetchStatus).toBe('fetching')
      invalidateAfterFetch(client, { queryKey: test.queryKey })
      await flushMicrotasks()
      expect(test.invalidations).toHaveBeenCalledTimes(1)
      expect(client.getQueryState(test.queryKey)?.isInvalidated).toBe(true)
      test.release(1)
      await test.held
      await flushMicrotasks()
      expect(test.queryFn).toHaveBeenCalledTimes(1)
      test.unsubscribe()
    },
  )

  it('waits for the active fetch and coalesces events into exactly one follow-up read', async () => {
    const test = setup()
    // Invalidation completion is not the fetch-settlement contract.
    test.invalidations.mockImplementation((...args) => {
      if (test.invalidations.mock.calls.length > 5) return new Promise<void>(() => {})
      void test.invalidateOriginal(...args)
      return Promise.resolve()
    })
    invalidateAfterFetch(client, { queryKey: test.queryKey })
    invalidateAfterFetch(client, { queryKey: test.queryKey })
    await flushMicrotasks()
    expect(test.invalidations).toHaveBeenCalledTimes(2)
    expect(test.queryFn).toHaveBeenCalledTimes(1)
    test.release(1)
    await test.held
    await flushMicrotasks()
    expect(test.queryFn).toHaveBeenCalledTimes(2)
    expect(client.getQueryData(test.queryKey)).toBe(2)
    test.unsubscribe()
  })

  it('does not schedule a follow-up after the active query unmounts mid-fetch', async () => {
    const test = setup()
    invalidateAfterFetch(client, { queryKey: test.queryKey })
    test.unsubscribe()
    await flushMicrotasks()
    test.release(1)
    await test.held
    await flushMicrotasks()
    expect(test.invalidations).toHaveBeenCalledTimes(1)
    expect(test.queryFn).toHaveBeenCalledTimes(1)
  })
})
