import { expect, test, type Page } from './fixtures'
import { serverPlacement } from '../src/test-utils/placement'

const MOCK_PROJECT_ID = 'proj-test-ws'
const MOCK_TASK_ID = 'task-test-ws'

function mockTask(overrides: Record<string, unknown> = {}) {
  return {
    id: MOCK_TASK_ID,
    project_id: MOCK_PROJECT_ID,
    title: 'Test task with workspace issue',
    description: 'A task whose worktree was deleted',
    status: 'blocked',
    task_type: 'task',
    priority: 50,
    assignee: null,
    parent_task_id: null,
    role_assignments: [],
    remaining_retries: {},
    placement: null,
    error_annotation: null,
    task_state_config: null,
    review_passed_at: null,
    version: 1,
    created_at: '2026-04-22T00:00:00Z',
    updated_at: '2026-04-22T00:00:00Z',
    ...overrides,
  }
}

function mockProject() {
  return {
    id: MOCK_PROJECT_ID,
    name: 'Test Project',
    repo_id: 'repo-1',
    workflow_definition: '',
    created_at: '2026-04-22T00:00:00Z',
    updated_at: '2026-04-22T00:00:00Z',
  }
}

function emptyHooks() {
  return { before_exit: [], on_exit: [], on_enter: [], after_enter: [] }
}

function mockWorkflowResponse() {
  return {
    states: [
      { name: 'backlog', kind: 'backlog', column: 'Backlog', display_name: 'Backlog', role: null, hooks: emptyHooks(), gate_config: null, config: {} },
      { name: 'todo', kind: 'initial', column: 'Todo', display_name: 'Todo', role: null, hooks: emptyHooks(), gate_config: null, config: {} },
      { name: 'planning', kind: 'active', column: 'Planning', display_name: 'Planning', role: 'planner', hooks: emptyHooks(), gate_config: null, config: {} },
      { name: 'in_progress', kind: 'active', column: 'In Progress', display_name: 'In Progress', role: 'coder', hooks: emptyHooks(), gate_config: null, config: {} },
      { name: 'review', kind: 'gate', column: 'Review', display_name: 'Review', role: null, hooks: emptyHooks(), gate_config: null, config: {} },
      { name: 'merging', kind: 'active', column: 'Merging', display_name: 'Merging', role: null, hooks: emptyHooks(), gate_config: null, config: {} },
      { name: 'done', kind: 'terminal', column: 'Done', display_name: 'Done', role: null, hooks: emptyHooks(), gate_config: null, config: {} },
      { name: 'blocked', kind: 'custom', column: 'Blocked', display_name: 'Blocked', role: null, hooks: emptyHooks(), gate_config: null, config: {} },
      { name: 'cancelled', kind: 'terminal', column: 'Done', display_name: 'Cancelled', role: null, hooks: emptyHooks(), gate_config: null, config: {} },
      { name: 'merge_failed', kind: 'custom', column: 'Merging', display_name: 'Merge Failed', role: null, hooks: emptyHooks(), gate_config: null, config: {} },
    ],
    roles: [
      { name: 'planner', display_name: 'Planner', description: '' },
      { name: 'coder', display_name: 'Coder', description: '' },
      { name: 'reviewer', display_name: 'Reviewer', description: '' },
    ],
    cancellation_state: 'cancelled',
  }
}

async function setupMockRoutes(page: Page, task: ReturnType<typeof mockTask>) {
  await Promise.all([
    page.route('**/api/v1/projects', (route) =>
      route.fulfill({ json: { items: [mockProject()], has_more: false } }),
    ),
    page.route(`**/api/v1/projects/${MOCK_PROJECT_ID}`, (route) => {
      if (route.request().url().includes('/tasks') || route.request().url().includes('/workflow') || route.request().url().includes('/repos')) {
        return route.fallback()
      }
      return route.fulfill({ json: mockProject() })
    }),
    page.route(`**/api/v1/projects/${MOCK_PROJECT_ID}/repos*`, (route) =>
      route.fulfill({ json: { items: [], has_more: false } }),
    ),
    page.route(`**/api/v1/projects/${MOCK_PROJECT_ID}/tasks*`, (route) => {
      if (route.request().url().includes('/transitions')) {
        return route.fulfill({ json: { items: [], has_more: false } })
      }
      return route.fulfill({ json: { items: [task], has_more: false } })
    }),
    page.route(`**/api/v1/tasks/${MOCK_TASK_ID}`, (route) => {
      if (route.request().url().includes('/executions') || route.request().url().includes('/reviews') || route.request().url().includes('/comments') || route.request().url().includes('/transitions') || route.request().url().includes('/workspace')) {
        return route.fallback()
      }
      return route.fulfill({ json: { ...task, executions: [], reviews: [] } })
    }),
    page.route(`**/api/v1/tasks/${MOCK_TASK_ID}/executions*`, (route) =>
      route.fulfill({ json: { items: [], has_more: false } }),
    ),
    page.route(`**/api/v1/tasks/${MOCK_TASK_ID}/reviews*`, (route) =>
      route.fulfill({ json: [] }),
    ),
    page.route(`**/api/v1/tasks/${MOCK_TASK_ID}/comments*`, (route) =>
      route.fulfill({ json: { items: [], has_more: false } }),
    ),
    page.route(`**/api/v1/tasks/${MOCK_TASK_ID}/transitions*`, (route) =>
      route.fulfill({ json: { items: [], has_more: false } }),
    ),
    page.route(`**/api/v1/projects/${MOCK_PROJECT_ID}/workflow`, (route) =>
      route.fulfill({ json: mockWorkflowResponse() }),
    ),
    page.route('**/api/v1/agents*', (route) =>
      route.fulfill({ json: { items: [], has_more: false } }),
    ),
    page.route('**/api/v1/events*', (route) =>
      route.fulfill({ status: 200, body: '', contentType: 'text/event-stream' }),
    ),
  ])
}

test.describe('workspace recovery banner (mocked)', () => {
  test('shows workspace reset required banner with reset button', async ({ page }) => {
    const task = mockTask({
      error_annotation: {
        type: 'workspace_reset_required',
        message: 'workspace reset required: task branch no longer exists',
      },
    })
    await setupMockRoutes(page, task)

    await page.goto(`/projects/${MOCK_PROJECT_ID}/board?task=${MOCK_TASK_ID}`)
    await page.waitForLoadState('domcontentloaded')

    await expect(page.getByText('Workspace Reset Required', { exact: true })).toBeVisible({ timeout: 10000 })
    await expect(page.getByText('task branch no longer exists')).toBeVisible()
    await expect(page.getByRole('button', { name: 'Reset Workspace' })).toBeVisible()
  })

  test('shows confirmation step when Reset Workspace is clicked', async ({ page }) => {
    const task = mockTask({
      error_annotation: {
        type: 'workspace_reset_required',
        message: 'workspace reset required: task branch no longer exists',
      },
    })
    await setupMockRoutes(page, task)

    await page.goto(`/projects/${MOCK_PROJECT_ID}/board?task=${MOCK_TASK_ID}`)
    await page.waitForLoadState('domcontentloaded')

    const resetButton = page.getByRole('button', { name: 'Reset Workspace' })
    await expect(resetButton).toBeVisible({ timeout: 10000 })
    await resetButton.click()

    await expect(page.getByText('uncommitted work', { exact: false })).toBeVisible()
    await expect(page.getByRole('button', { name: 'Confirm Reset' })).toBeVisible()
    await expect(page.getByRole('button', { name: 'Cancel', exact: true })).toBeVisible()
    await expect(resetButton).not.toBeVisible()
  })

  test('cancel hides confirmation and restores reset button', async ({ page }) => {
    const task = mockTask({
      error_annotation: {
        type: 'workspace_reset_required',
        message: 'workspace reset required: branch lost',
      },
    })
    await setupMockRoutes(page, task)

    await page.goto(`/projects/${MOCK_PROJECT_ID}/board?task=${MOCK_TASK_ID}`)
    await page.waitForLoadState('domcontentloaded')

    const resetButton = page.getByRole('button', { name: 'Reset Workspace' })
    await expect(resetButton).toBeVisible({ timeout: 10000 })
    await resetButton.click()

    const cancelButton = page.getByRole('button', { name: 'Cancel', exact: true })
    await expect(cancelButton).toBeVisible()
    await cancelButton.click()

    await expect(page.getByRole('button', { name: 'Confirm Reset' })).not.toBeVisible()
    await expect(resetButton).toBeVisible()
  })

  test('confirm reset calls API and shows success toast', async ({ page }) => {
    let resetCalled = false
    const task = mockTask({
      error_annotation: {
        type: 'workspace_reset_required',
        message: 'workspace reset required: branch lost',
      },
    })
    const clearedTask = mockTask({ error_annotation: null, version: 2 })

    await setupMockRoutes(page, task)

    await page.route(`**/api/v1/tasks/${MOCK_TASK_ID}/workspace/reset`, (route) => {
      resetCalled = true
      return route.fulfill({
        json: {
          id: 'ws-new',
          task_id: MOCK_TASK_ID,
          worktree_path: '/tmp/worktrees/new',
          placement: serverPlacement({
            workspace_id: 'ws-new',
            task_id: MOCK_TASK_ID,
            workspace_handle: '/tmp/worktrees/new',
          }),
          branch: 'main',
          status: 'ready',
        },
      })
    })
    // Override task route to return cleared task after reset
    await page.route(`**/api/v1/tasks/${MOCK_TASK_ID}`, (route) => {
      if (route.request().url().includes('/executions') || route.request().url().includes('/reviews') || route.request().url().includes('/comments') || route.request().url().includes('/transitions') || route.request().url().includes('/workspace')) {
        return route.fallback()
      }
      return route.fulfill({
        json: { ...(resetCalled ? clearedTask : task), executions: [], reviews: [] },
      })
    })

    await page.goto(`/projects/${MOCK_PROJECT_ID}/board?task=${MOCK_TASK_ID}`)
    await page.waitForLoadState('domcontentloaded')

    const resetButton = page.getByRole('button', { name: 'Reset Workspace' })
    await expect(resetButton).toBeVisible({ timeout: 10000 })
    await resetButton.click()

    const confirmButton = page.getByRole('button', { name: 'Confirm Reset' })
    await expect(confirmButton).toBeVisible()
    await confirmButton.click()

    await expect(page.getByText('Workspace reset successfully')).toBeVisible({ timeout: 10000 })
    expect(resetCalled).toBe(true)
  })

  test('workspace_error banner shows without reset button', async ({ page }) => {
    const task = mockTask({
      error_annotation: {
        type: 'workspace_error',
        message: 'workspace error: repo directory not found',
      },
    })
    await setupMockRoutes(page, task)

    await page.goto(`/projects/${MOCK_PROJECT_ID}/board?task=${MOCK_TASK_ID}`)
    await page.waitForLoadState('domcontentloaded')

    await expect(page.getByText('Workspace Error', { exact: true })).toBeVisible({ timeout: 10000 })
    await expect(page.getByText('repo directory not found')).toBeVisible()
    await expect(page.getByRole('button', { name: 'Reset Workspace' })).not.toBeVisible()
  })
})

test.describe('workspace recovery with demo data', () => {
  test('blocked task with workspace_reset_required shows banner on board', async ({
    page,
    request,
  }) => {
    const projectsResp = await request.get('/api/v1/projects')
    expect(projectsResp.ok()).toBeTruthy()
    const projects = await projectsResp.json()
    const project = projects.items?.[0]
    test.skip(!project, 'No projects seeded; run `make dev-demo`')

    const tasksResp = await request.get(
      `/api/v1/projects/${project.id}/tasks?statuses=blocked`,
    )
    expect(tasksResp.ok()).toBeTruthy()
    const tasks = await tasksResp.json()
    const wsResetTask = tasks.items?.find(
      (t: { error_annotation?: { type?: string } }) =>
        t.error_annotation?.type === 'workspace_reset_required',
    )
    test.skip(!wsResetTask, 'No workspace_reset_required task in demo data')

    await page.goto(`/projects/${project.id}/board?task=${wsResetTask.id}`)
    await page.waitForLoadState('domcontentloaded')

    await expect(page.getByText('Workspace Reset Required', { exact: true })).toBeVisible({ timeout: 15000 })
    await expect(page.getByRole('button', { name: 'Reset Workspace' })).toBeVisible()
  })

  test('blocked task with workspace_error shows banner without reset button', async ({
    page,
    request,
  }) => {
    const projectsResp = await request.get('/api/v1/projects')
    expect(projectsResp.ok()).toBeTruthy()
    const projects = await projectsResp.json()
    const project = projects.items?.[0]
    test.skip(!project, 'No projects seeded; run `make dev-demo`')

    const tasksResp = await request.get(
      `/api/v1/projects/${project.id}/tasks?statuses=blocked`,
    )
    expect(tasksResp.ok()).toBeTruthy()
    const tasks = await tasksResp.json()
    const wsErrorTask = tasks.items?.find(
      (t: { error_annotation?: { type?: string } }) =>
        t.error_annotation?.type === 'workspace_error',
    )
    test.skip(!wsErrorTask, 'No workspace_error task in demo data')

    await page.goto(`/projects/${project.id}/board?task=${wsErrorTask.id}`)
    await page.waitForLoadState('domcontentloaded')

    await expect(page.getByText('Workspace Error', { exact: true })).toBeVisible({ timeout: 15000 })
    await expect(page.getByRole('button', { name: 'Reset Workspace' })).not.toBeVisible()
  })
})
