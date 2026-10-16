import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { apiFetch } from '@/api/client'
import { qk } from '@/api/query-keys'
import type { Review, Task } from '@/types/generated'
import type { ReviewFindingEntry } from '@/types/generated/bindings/ReviewFindingEntry'
import { TaskReviewTab } from './TaskReviewTab'

vi.mock('@/api/client', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/client')>()),
  apiFetch: vi.fn(),
}))
vi.mock('@/api/hooks', () => ({
  useTaskAction: () => ({ mutate: vi.fn(), isPending: false }),
  useTaskRelationsQuery: () => ({ data: { subtasks: [] } }),
}))
vi.mock('@tanstack/react-router', () => ({
  Link: ({ children }: { children: React.ReactNode }) => <a href="#review">{children}</a>,
}))

const review: Review = {
  id: 'review-3',
  task_id: 'task-1',
  execution_id: 'candidate',
  attempt_number: 3,
  status: 'failed',
  step_results: [],
  details: {
    ci_steps: [],
    conformance: {
      status: 'failed',
      reason: 'repeated finding: F2',
      checks: [],
      contract: null,
      assessment: {
        result: 'fail',
        reason: 'still crashes',
        findings: [
          {
            id: 'F2',
            summary: 'Empty input crashes',
            severity: 'blocking',
            where: 'src/parse.rs:42',
            expected: 'An error',
            observed: 'A panic',
          },
        ],
        resolved: ['F1'],
        report: 'The full report remains here.',
      },
    },
  },
  started_at: '2026-10-15T12:00:00Z',
  finished_at: '2026-10-15T12:01:00Z',
  created_at: '2026-10-15T12:00:00Z',
  updated_at: '2026-10-15T12:01:00Z',
}
function entry(id: string, status: ReviewFindingEntry['status'] = 'open'): ReviewFindingEntry {
  return {
    id,
    source: 'reviewer',
    severity: 'blocking',
    status,
    summary: `Issue ${id}`,
    where: 'src/parse.rs:42',
    expected: 'An error',
    observed: 'A panic',
    first_attempt: 1,
    last_checked_attempt: 3,
    streak: status === 'open' ? 3 : 0,
  }
}
function show(parked = false, attempts = [review]) {
  const task = {
    id: 'task-1',
    status: 'review',
    version: 1,
    available_actions: [],
    condition: {
      kind: 'clear',
      details: {
        failure_kind: null,
        diagnostic: null,
        interruption: null,
        failed: false,
        blocked: false,
        human_wait: false,
        entry_wait: false,
      },
    },
    workflow_exception: parked
      ? {
          type: 'review_needs_owner',
          message: 'repeated finding: F2',
          review_id: 'review-3',
          execution_id: null,
          state: 'review',
          role: 'reviewer',
          target_state: null,
          target_role: null,
          failing_step: null,
          related_evidence: [],
          actions: [
            {
              action: { verb: 'retry', guidance: null },
              label: 'Re-run review',
              reason: 'review_needs_owner',
              parameters: [],
              authority: ['owner'],
              target_execution_id: null,
              propagates: false,
            },
          ],
        }
      : null,
  } as unknown as Task
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  render(
    <QueryClientProvider client={client}>
      <TaskReviewTab
        task={task}
        reviews={attempts}
        latestReview={review}
        reviewsLoading={false}
        expandedHistoryAttempts={new Set([1])}
        onToggleHistoryAttempt={vi.fn()}
      />
    </QueryClientProvider>,
  )
  return client
}
beforeEach(() => {
  vi.mocked(apiFetch).mockReset()
})
afterEach(() => vi.restoreAllMocks())

describe('Review tab finding ledger', () => {
  it('reads /findings in the server order with expandable evidence text', async () => {
    // The API's natural order (byte order between digit runs) is the only
    // order: a locale collator would move V-alpha before V-Zeta here.
    vi.mocked(apiFetch).mockResolvedValue({
      items: [
        entry('F1', 'resolved'),
        entry('F2'),
        entry('F10'),
        entry('V-Zeta'),
        entry('V-alpha'),
      ],
      window_started_at: review.created_at,
      review_attempts: 3,
    })
    show()
    const table = await screen.findByRole('table')
    expect(vi.mocked(apiFetch).mock.calls[0][0]).toBe('/tasks/task-1/findings')
    const rows = within(table).getAllByRole('row').slice(1)
    expect(rows.map((row) => within(row).getByRole('rowheader').textContent)).toEqual([
      'F1',
      'F2',
      'F10',
      'V-Zeta',
      'V-alpha',
    ])
    expect(rows[1].textContent).toContain('reviewerblockingopen3')
    fireEvent.click(within(rows[1]).getByText('Finding details'))
    expect(within(rows[1]).getByText('A panic')).toBeTruthy()
    expect(screen.queryByRole('region', { name: 'Verify results' })).toBeNull()
  })

  it('keeps the park, finding streak and owner actions beside the attempts', async () => {
    vi.mocked(apiFetch).mockResolvedValue({
      items: [entry('F2')],
      window_started_at: review.created_at,
      review_attempts: 3,
    })
    show(true)
    expect(screen.getByText('Needs owner')).toBeTruthy()
    expect(screen.getByRole('button', { name: 'Re-run review' })).toBeTruthy()
    const row = within(await screen.findByRole('table')).getAllByRole('row')[1]
    expect(within(row).getByRole('rowheader').textContent).toBe('F2')
    expect(within(row).getByText('3')).toBeTruthy()
    expect(screen.getAllByText('Attempt 3').length).toBeGreaterThan(0)
    expect(
      within(screen.getByRole('region', { name: 'Attempt findings' })).getByText(
        /Empty input crashes/,
      ),
    ).toBeTruthy()
  })

  it('shows each historical attempt input without guessing Forge ids', async () => {
    vi.mocked(apiFetch).mockResolvedValue({
      items: [],
      window_started_at: review.created_at,
      review_attempts: 3,
    })
    const first: Review = {
      ...review,
      id: 'review-1',
      attempt_number: 1,
      details: {
        ...review.details,
        conformance: {
          ...review.details.conformance,
          assessment: {
            result: 'fail',
            reason: 'first',
            report: 'Earlier full report',
            findings: [{ summary: 'Original finding', severity: 'blocking' }],
          },
        },
      },
    }
    show(false, [first, review])
    expect(screen.getByText('Original finding', { exact: false })).toBeTruthy()
    expect(screen.getByText('New finding (Forge assigns ID)')).toBeTruthy()
    expect(screen.getByText('Earlier full report')).toBeTruthy()
    await screen.findByText('No findings in this review window.')
  })

  it('announces loading and an empty window', async () => {
    let resolve!: (value: unknown) => void
    vi.mocked(apiFetch).mockImplementation(
      () =>
        new Promise((done) => {
          resolve = done
        }),
    )
    show()
    expect(screen.getByRole('status', { name: 'Loading findings' })).toBeTruthy()
    resolve({ items: [], window_started_at: review.created_at, review_attempts: 0 })
    await screen.findByText('No findings in this review window.')
  })

  it('shows read errors and refetches findings on the existing review invalidation prefix', async () => {
    vi.mocked(apiFetch)
      .mockRejectedValueOnce(new Error('Read unavailable'))
      .mockResolvedValue({
        items: [entry('F2')],
        window_started_at: review.created_at,
        review_attempts: 3,
      })
    const client = show()
    await screen.findByText('Read unavailable')
    await client.invalidateQueries({ queryKey: qk.reviews('task-1') })
    await waitFor(() => expect(screen.getByRole('table')).toBeTruthy())
    expect(apiFetch).toHaveBeenCalledTimes(2)
  })
})
