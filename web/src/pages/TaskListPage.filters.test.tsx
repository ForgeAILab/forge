import { useState } from 'react'
import { fireEvent, render, screen } from '@testing-library/react'
import { describe, expect, it, vi } from 'vitest'
import { taskListItem } from '@/test-utils/task-list-item'
import { TaskListPage } from './TaskListPage'

const tasks = [
  taskListItem({ id: 'todo-low', title: 'Low todo', priority: 3 }),
  taskListItem({ id: 'todo-high', title: 'High todo', priority: 7 }),
  taskListItem({ id: 'review-high', title: 'High review', status: 'review', priority: 8 }),
  taskListItem({ id: 'done-high', title: 'High done', status: 'done', priority: 9 }),
]

vi.mock('@tanstack/react-router', () => ({ useNavigate: () => vi.fn() }))
vi.mock('@/api/hooks', () => ({
  useAgentsQuery: () => ({ data: { items: [] } }),
  useProjectAgentsQuery: () => ({ data: [] }),
  useMembersQuery: () => ({ data: [] }),
  useWorkflowQuery: () => ({
    data: { states: ['todo', 'review', 'done'].map((name) => ({ name, display_name: name })) },
  }),
  useArchiveTask: () => ({}),
  useAssignRole: () => ({}),
  useRemoveRole: () => ({}),
  useTransitionTask: () => ({}),
  useTasksQuery: (_projectId: string, search: { status?: string }) => ({
    data: {
      pages: [
        {
          items: tasks.filter(
            (task) => !search.status || search.status.split(',').includes(task.status),
          ),
        },
      ],
    },
  }),
}))
vi.mock('@/components/task-detail/task-list-actions', () => ({
  BulkCancelTasks: () => null,
  TaskRowActions: () => null,
}))

function Filters() {
  const [filters, setFilters] = useState({
    statuses: ['todo', 'review'],
    priorityMin: 6 as number | undefined,
    priorityMax: undefined as number | undefined,
  })
  return (
    <TaskListPage
      projectId="project-1"
      sortBy="updated_at"
      sortOrder="desc"
      agentIds={[]}
      blockedOnly={false}
      includeCancelled={false}
      includeArchived={false}
      {...filters}
      onSortChange={vi.fn()}
      onFilterChange={(patch) => setFilters((current) => ({ ...current, ...patch }))}
    />
  )
}

describe('Task table filters', () => {
  it('intersects selected statuses with minimum priority and clears every filter', () => {
    render(<Filters />)
    expect(screen.getByRole('button', { name: 'High todo' })).toBeTruthy()
    expect(screen.getByRole('button', { name: 'High review' })).toBeTruthy()
    expect(screen.queryByRole('button', { name: 'Low todo' })).toBeNull()
    expect(screen.queryByRole('button', { name: 'High done' })).toBeNull()
    fireEvent.click(screen.getByRole('button', { name: 'Clear all' }))
    for (const task of tasks) expect(screen.getByRole('button', { name: task.title })).toBeTruthy()
    expect((screen.getByPlaceholderText('Min') as HTMLInputElement).value).toBe('')
  })

  it('updates a status selection without dropping the minimum priority', () => {
    render(<Filters />)
    fireEvent.click(screen.getByRole('button', { name: 'Statuses' }))
    fireEvent.click(screen.getByRole('checkbox', { name: 'todo' }))
    expect(screen.queryByRole('button', { name: 'High todo' })).toBeNull()
    expect(screen.getByRole('button', { name: 'High review' })).toBeTruthy()
    expect((screen.getByPlaceholderText('Min') as HTMLInputElement).value).toBe('6')
  })
})
