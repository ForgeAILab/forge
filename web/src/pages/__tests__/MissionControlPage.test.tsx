// Keep this import first: the helper registers the page's vi.mock calls.
import {
  data,
  permissions,
  reviewOwnerAttention,
  taskAction,
  resolveAttention,
} from './MissionControlPage.test-helpers'
import { fireEvent, render, screen, within } from '@testing-library/react'
import { describe, expect, it } from 'vitest'
import { MissionControlPage } from '@/pages/MissionControlPage'
import { useAuthStore } from '@/stores/auth'
import type { Offer } from '@/types/generated'

describe('MissionControlPage', () => {
  it.each([
    'viewer',
    'non-member',
    'instance administrator',
    'failed project',
    'failed memberships',
    'signed out',
    'owner id with viewer role',
  ])('keeps review parks visible without unauthorized controls for %s', (viewer) => {
    const previous = data.needs_attention
    permissions.ownerId = 'another-owner'
    permissions.role = 'viewer'
    if (viewer === 'non-member') permissions.role = null
    if (viewer === 'instance administrator') {
      useAuthStore.setState({ user: { ...useAuthStore.getState().user!, is_admin: true } })
    }
    if (viewer === 'signed out') useAuthStore.getState().clearAuth()
    if (viewer === 'failed project') {
      permissions.role = 'owner'
      permissions.projectError = true
    }
    if (viewer === 'failed memberships') {
      permissions.role = 'owner'
      permissions.membersError = true
    }
    if (viewer === 'owner id with viewer role') {
      permissions.ownerId = 'user-1'
    }
    data.needs_attention = [reviewOwnerAttention()]
    try {
      render(<MissionControlPage />)
      const card = screen
        .getByRole('heading', {
          name: 'Review needs the owner: repeated finding F1',
        })
        .closest('article')!
      expect(within(card).getByText('Owner action needed')).toBeTruthy()
      expect(within(card).getByRole('link', { name: 'Open review' })).toBeTruthy()
      expect(within(card).queryAllByRole('button')).toHaveLength(0)
      expect(taskAction).not.toHaveBeenCalled()
      expect(resolveAttention).not.toHaveBeenCalled()
    } finally {
      data.needs_attention = previous
    }
  })

  it.each([
    { viewer: 'membership owner', ownerId: 'another-owner', role: 'owner', isOwner: true },
    { viewer: 'admin member', ownerId: 'another-owner', role: 'admin', isOwner: false },
    { viewer: 'member', ownerId: 'another-owner', role: 'member', isOwner: false },
    { viewer: 'owner id with member role', ownerId: 'user-1', role: 'member', isOwner: false },
    { viewer: 'owner id without membership', ownerId: 'user-1', role: null, isOwner: true },
    { viewer: 'legacy ownerless Project viewer', ownerId: null, role: 'viewer', isOwner: true },
  ])('shows permitted review-park controls to $viewer', ({ ownerId, role, isOwner }) => {
    const previous = data.needs_attention
    Object.assign(permissions, { ownerId, role })
    data.needs_attention = [reviewOwnerAttention()]
    try {
      render(<MissionControlPage />)
      expect(screen.queryByText('Owner action needed')).toBeNull()
      fireEvent.click(screen.getByRole('button', { name: 'Resolve' }))
      expect(resolveAttention).toHaveBeenCalledWith({ id: 'attention-1', expectedVersion: 1 })
      expect(screen.getByRole('button', { name: 'Defer Finding to Follow-up' })).toBeTruthy()
      expect(Boolean(screen.queryByRole('button', { name: 'Override Failed Review' }))).toBe(
        isOwner,
      )
      fireEvent.click(screen.getByRole('button', { name: 'Retry With Guidance' }))
      expect(taskAction).toHaveBeenCalledWith(
        { taskId: 'task-1', version: 12, action: { verb: 'retry' } },
        expect.anything(),
      )
    } finally {
      data.needs_attention = previous
    }
  })

  it.each([
    { pending: 'Project', field: 'projectLoading', role: 'owner' },
    { pending: 'Project', field: 'projectLoading', role: 'member' },
    { pending: 'Project', field: 'projectLoading', role: 'viewer' },
    { pending: 'memberships', field: 'membersLoading', role: 'owner' },
    { pending: 'memberships', field: 'membersLoading', role: 'member' },
    { pending: 'memberships', field: 'membersLoading', role: 'viewer' },
  ] as const)(
    'withholds controls and denial text while $pending loads for $role',
    ({ field, role }) => {
      const previous = data.needs_attention
      Object.assign(permissions, { ownerId: 'another-owner', role, [field]: true })
      data.needs_attention = [reviewOwnerAttention()]
      try {
        const { rerender } = render(<MissionControlPage />)
        const card = screen
          .getByRole('heading', { name: 'Review needs the owner: repeated finding F1' })
          .closest('article')!
        expect(within(card).queryAllByRole('button')).toHaveLength(0)
        expect(within(card).queryByText('Owner action needed')).toBeNull()
        expect(within(card).getByRole('link', { name: 'Open review' })).toBeTruthy()

        permissions[field] = false
        rerender(<MissionControlPage />)
        if (role === 'viewer') {
          expect(within(card).queryAllByRole('button')).toHaveLength(0)
          expect(within(card).getByText('Owner action needed')).toBeTruthy()
        } else {
          expect(within(card).getByRole('button', { name: 'Retry With Guidance' })).toBeTruthy()
          expect(within(card).getByRole('button', { name: 'Resolve' })).toBeTruthy()
          expect(within(card).queryByText('Owner action needed')).toBeNull()
        }
      } finally {
        data.needs_attention = previous
      }
    },
  )

  it.each(['owner', 'admin', 'member'])(
    'handles fixed and optional approval overrides for %s',
    (role) => {
      const previous = data.needs_attention
      permissions.role = role
      permissions.ownerId = 'another-owner'
      const item = reviewOwnerAttention()
      const recovery = item.details.recovery as { actions: Offer[] }
      recovery.actions = [
        {
          ...recovery.actions[2],
          action: { verb: 'approve' },
          label: 'Fixed override',
          parameters: [{ name: 'override', required: true, boolean_values: [true] }],
        },
        {
          ...recovery.actions[1],
          label: 'Approve with optional override',
          parameters: [{ name: 'override', required: true, boolean_values: [false, true] }],
        },
      ]
      data.needs_attention = [item]
      try {
        render(<MissionControlPage />)
        expect(Boolean(screen.queryByRole('button', { name: 'Fixed override' }))).toBe(
          role === 'owner',
        )
        fireEvent.click(screen.getByRole('button', { name: 'Approve with optional override' }))
        if (role === 'owner') {
          expect(screen.getAllByRole('option').map((option) => option.textContent)).toEqual([
            'No',
            'Yes',
          ])
        } else {
          expect(screen.queryByRole('combobox')).toBeNull()
          expect(screen.getByText('Uses the current checks')).toBeTruthy()
          fireEvent.click(screen.getByRole('button', { name: 'Apply' }))
          expect(taskAction).toHaveBeenCalledWith(
            { taskId: 'task-1', version: 12, action: { verb: 'approve', override: false } },
            expect.anything(),
          )
        }
      } finally {
        data.needs_attention = previous
      }
    },
  )

  it.each(['member', 'viewer', 'failed memberships', 'signed out'])(
    'keeps Resolve on other Attention categories for %s',
    (viewer) => {
      permissions.ownerId = 'another-owner'
      permissions.role = viewer
      if (viewer === 'failed memberships') {
        permissions.role = 'member'
        permissions.membersError = true
      }
      if (viewer === 'signed out') useAuthStore.getState().clearAuth()
      render(<MissionControlPage />)
      const card = screen
        .getByRole('heading', { name: 'A human decision is required.' })
        .closest('article')!
      expect(within(card).queryByText('Owner action needed')).toBeNull()
      fireEvent.click(within(card).getByRole('button', { name: 'Resolve' }))
      expect(resolveAttention).toHaveBeenCalled()
    },
  )

  it.each(['owner', 'admin', 'member'])('filters refreshed review offers for %s', (role) => {
    const previous = data.needs_attention
    permissions.role = role
    permissions.ownerId = 'another-owner'
    const item = reviewOwnerAttention()
    const recovery = item.details.recovery as { actions: Offer[] }
    const ownerOffer = recovery.actions[0]
    const agentOffer: Offer = {
      ...ownerOffer,
      label: 'Project Agent retry',
      reason: 'agent_retry',
      authority: ['project_agent'],
    }
    recovery.actions.push(agentOffer)
    data.needs_attention = [item]
    taskAction.mockImplementationOnce((_input, callbacks) =>
      callbacks.onSuccess({
        version: 13,
        available_actions: [
          { ...agentOffer, label: 'Refreshed Project Agent retry' },
          { ...ownerOffer, label: 'Refreshed owner retry' },
          { ...recovery.actions[1], label: 'Refreshed deferral' },
          { ...recovery.actions[2], label: 'Refreshed override' },
        ],
      }),
    )
    try {
      render(<MissionControlPage />)
      expect(screen.queryByRole('button', { name: 'Project Agent retry' })).toBeNull()
      fireEvent.click(screen.getByRole('button', { name: 'Retry With Guidance' }))
      expect(screen.queryByRole('button', { name: 'Refreshed Project Agent retry' })).toBeNull()
      expect(screen.getByRole('button', { name: 'Refreshed deferral' })).toBeTruthy()
      expect(Boolean(screen.queryByRole('button', { name: 'Refreshed override' }))).toBe(
        role === 'owner',
      )
      fireEvent.click(screen.getByRole('button', { name: 'Refreshed owner retry' }))
      expect(taskAction).toHaveBeenLastCalledWith(
        { taskId: 'task-1', version: 13, action: { verb: 'retry' } },
        expect.anything(),
      )
    } finally {
      data.needs_attention = previous
    }
  })

  it.each([
    'repeated finding F1',
    'only non-blocking findings remain',
    'no change after repair round',
  ])('shows review parks with their reason and owner offers: %s', (reason) => {
    const previous = data.needs_attention
    data.needs_attention = [
      {
        ...previous[0],
        category: 'review_needs_owner',
        scope_type: 'project',
        scope_id: 'project-1',
        summary: `Review needs the owner: ${reason}`,
        recommended_action: 'inspect_review',
        details: {
          entity_type: 'task',
          entity_id: 'task-1',
          task: { task_version: 12 },
          interruption: { kind: 'review_needs_owner', reason },
          recovery: {
            actions: [
              {
                action: { verb: 'approve', override: false },
                label: 'Defer Finding to Follow-up',
                reason: 'review_needs_owner',
                authority: ['owner'],
                parameters: [{ name: 'reason', required: true, boolean_values: null }],
                target_execution_id: null,
                propagates: false,
              },
              {
                action: { verb: 'retry' },
                label: 'Retry With Guidance',
                reason: 'review_owner_retry',
                authority: ['owner'],
                parameters: [{ name: 'guidance', required: false, boolean_values: null }],
                target_execution_id: null,
                propagates: false,
              },
            ],
          },
        },
      },
    ]
    try {
      render(<MissionControlPage />)
      const card = screen
        .getByRole('heading', { name: `Review needs the owner: ${reason}` })
        .closest('article')
      expect(card?.className).toContain('bg-warning/5')
      expect(card?.textContent).not.toContain('Task needs recovery')
      expect(
        screen.getByRole('link', { name: 'Open review' }).getAttribute('data-params'),
      ).toContain('review')
      expect(screen.getByRole('button', { name: 'Retry With Guidance' })).toBeTruthy()
      fireEvent.click(screen.getByRole('button', { name: 'Defer Finding to Follow-up' }))
      expect(screen.getByRole('button', { name: 'Apply' }).hasAttribute('disabled')).toBe(true)
      fireEvent.change(screen.getByLabelText('reason (required)'), {
        target: { value: 'Track in follow-up' },
      })
      fireEvent.click(screen.getByRole('button', { name: 'Apply' }))
      expect(taskAction).toHaveBeenCalledWith(
        {
          taskId: 'task-1',
          version: 12,
          action: { verb: 'approve', override: false, reason: 'Track in follow-up' },
        },
        expect.anything(),
      )
    } finally {
      data.needs_attention = previous
      taskAction.mockClear()
    }
  })
})
