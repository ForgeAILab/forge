import { render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import type { Review, Task, Offer } from '@/types/generated'
import { TaskReviewTab } from './TaskReviewTab'

vi.mock('@/api/hooks', () => ({ useTaskAction: () => ({ mutate: vi.fn(), isPending: false }) }))

vi.mock('@tanstack/react-router', () => ({
  Link: ({ children }: { children: React.ReactNode }) => <a href="#review">{children}</a>,
}))

describe('Needs owner review', () => {
  it.each([
    ['owner', false, 'fixable by owner'],
    ['coder', true, 'repeated finding'],
  ] as const)(
    'derives the %s badge from the latest assessment when the message has no routing prefix',
    (fixableBy, repeat, badge) => {
      const actions: Offer[] = []
      const task = {
        id: 'task-48',
        status: 'review',
        workflow_exception: {
          type: 'review_needs_owner',
          message: 'Forge linked_documents is empty',
          review_id: null,
          execution_id: null,
          state: 'review',
          role: 'reviewer',
          target_state: null,
          target_role: null,
          failing_step: null,
          related_evidence: [],
          actions,
        },
      } as unknown as Task
      const review: Review = {
        id: 'review-1',
        task_id: task.id,
        execution_id: 'execution-1',
        attempt_number: 2,
        status: 'failed',
        step_results: [],
        details: {
          ci_steps: [],
          conformance: {
            status: 'failed',
            reason: null,
            contract: null,
            checks: [],
            assessment: {
              result: 'fail',
              reason: 'Forge linked_documents is empty',
              fixable_by: fixableBy,
              repeat,
              report: 'Owner evidence is required.',
            },
          },
        },
        started_at: '2026-09-30T12:00:00Z',
        finished_at: '2026-09-30T12:01:00Z',
        created_at: '2026-09-30T12:00:00Z',
        updated_at: '2026-09-30T12:01:00Z',
      }
      render(
        <TaskReviewTab
          task={task}
          reviews={[review]}
          latestReview={review}
          reviewsLoading={false}
          expandedHistoryAttempts={new Set()}
          onToggleHistoryAttempt={vi.fn()}
        />,
      )
      expect(screen.getByText('Needs owner')).toBeTruthy()
      expect(screen.getByText(badge)).toBeTruthy()
      expect(screen.queryByRole('button', { name: 'Re-run review' })).toBeNull()
    },
  )
})
