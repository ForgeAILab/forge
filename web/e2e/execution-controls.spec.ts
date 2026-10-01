import { expect, test, type Page } from './fixtures'

const PROJECT_ID = 'proj-exec-ctrl'
const TASK_ID = 'task-exec-ctrl'
const EXECUTION_ID = 'exec-001'

function mockProject() {
  return {
    id: PROJECT_ID,
    name: 'Exec Controls Project',
    settings: {},
    workflow_template_name: null,
    default_review_config: { ci_steps: [], review_prompt: null },
    created_at: '2026-04-25T00:00:00Z',
    updated_at: '2026-04-25T00:00:00Z',
  }
}

function emptyHooks() {
  return { before_exit: [], on_exit: [], on_enter: [], after_enter: [] }
}

function mockWorkflow() {
  return {
    states: [
      { name: 'todo', kind: 'initial', column: 'Todo', display_name: 'Todo', role: null, hooks: emptyHooks(), gate_config: null, config: {} },
      { name: 'in_progress', kind: 'active', column: 'In Progress', display_name: 'In Progress', role: 'coder', hooks: emptyHooks(), gate_config: null, config: {} },
      { name: 'review', kind: 'gate', column: 'Review', display_name: 'Review', role: null, hooks: emptyHooks(), gate_config: null, config: {} },
      { name: 'done', kind: 'terminal', column: 'Done', display_name: 'Done', role: null, hooks: emptyHooks(), gate_config: null, config: {} },
      { name: 'cancelled', kind: 'terminal', column: 'Done', display_name: 'Cancelled', role: null, hooks: emptyHooks(), gate_config: null, config: {} },
    ],
    roles: [{ name: 'coder', display_name: 'Coder', description: '' }],
    cancellation_state: 'cancelled',
  }
}

function mockTask(overrides: Record<string, unknown> = {}) {
  return {
    id: TASK_ID,
    project_id: PROJECT_ID,
    repo_id: 'repo-1',
    title: 'Test execution controls',
    description: 'A task for testing execution action buttons',
    status: 'in_progress',
    task_type: 'task',
    priority: 50,
    assignee: null,
    parent_task_id: null,
    role_assignments: [],
    remaining_retries: {},
    error_annotation: null,
    task_state_config: null,
    review_passed_at: null,
    workspace: null,
    placement: null,
    version: 1,
    created_at: '2026-04-25T00:00:00Z',
    updated_at: '2026-04-25T00:00:00Z',
    ...overrides,
  }
}

function mockExecution(overrides: Record<string, unknown> = {}) {
  return {
    id: EXECUTION_ID,
    task_id: TASK_ID,
    agent_id: 'agent-1',
    role: 'coder',
    status: 'completed',
    parent_execution_id: null,
    agent_session_id: 'session-abc',
    summary: 'Did some work',
    stop_reason: null,
    stopped_by: null,
    resume_policy: null,
    stopped_at: null,
    created_at: '2026-04-25T01:00:00Z',
    updated_at: '2026-04-25T01:30:00Z',
    ...overrides,
  }
}

function actionsForActiveTask() {
  return [
    { action: 'manual_launch', label: 'Start Manual Execution', enabled: true, propagates: false, requires_session: false, disabled_reason: null },
    { action: 'session_follow_up', label: 'Continue Session Manually', enabled: true, propagates: false, requires_session: true, disabled_reason: null, target_execution_id: EXECUTION_ID },
    { action: 'workflow_resume', label: 'Resume coder', enabled: false, propagates: true, requires_session: true, disabled_reason: 'No recovery session available' },
    { action: 're_execute', label: 'Re-execute coder', enabled: true, propagates: true, requires_session: false, disabled_reason: null, target_execution_id: EXECUTION_ID },
    { action: 'stop_execution', label: 'Stop Execution', enabled: false, propagates: false, requires_session: false, disabled_reason: 'No running execution' },
    { action: 'cancel_task', label: 'Cancel Task', enabled: true, propagates: false, requires_session: false, disabled_reason: null },
  ]
}

function actionsForTerminalTask() {
  return [
    { action: 'manual_launch', label: 'Start Manual Execution', enabled: false, propagates: false, requires_session: false, disabled_reason: 'Task is in terminal state' },
    { action: 'session_follow_up', label: 'Continue Session Manually', enabled: false, propagates: false, requires_session: true, disabled_reason: 'Task is in terminal state', target_execution_id: null },
    { action: 'workflow_resume', label: 'Resume Execution', enabled: false, propagates: true, requires_session: true, disabled_reason: 'Task is in terminal state' },
    { action: 're_execute', label: 'Re-execute Execution', enabled: false, propagates: true, requires_session: false, disabled_reason: 'Task is in terminal state', target_execution_id: null },
    { action: 'stop_execution', label: 'Stop Execution', enabled: false, propagates: false, requires_session: false, disabled_reason: 'No running execution' },
    { action: 'cancel_task', label: 'Cancel Task', enabled: false, propagates: false, requires_session: false, disabled_reason: 'Task is already in terminal state' },
  ]
}

async function setupRoutes(
  page: Page,
  task: ReturnType<typeof mockTask>,
  executions: ReturnType<typeof mockExecution>[] = [],
) {
  await Promise.all([
    page.route('**/api/v1/projects', (route) =>
      route.fulfill({ json: { items: [mockProject()], has_more: false } }),
    ),
    page.route(`**/api/v1/projects/${PROJECT_ID}`, (route) => {
      if (route.request().url().includes('/tasks') || route.request().url().includes('/workflow') || route.request().url().includes('/repos')) {
        return route.fallback()
      }
      return route.fulfill({ json: mockProject() })
    }),
    page.route(`**/api/v1/projects/${PROJECT_ID}/repos*`, (route) =>
      route.fulfill({ json: { items: [], has_more: false } }),
    ),
    page.route(`**/api/v1/projects/${PROJECT_ID}/tasks*`, (route) =>
      route.fulfill({ json: { items: [task], has_more: false } }),
    ),
    page.route(`**/api/v1/projects/${PROJECT_ID}/workflow`, (route) =>
      route.fulfill({ json: mockWorkflow() }),
    ),
    page.route(`**/api/v1/tasks/${TASK_ID}`, (route) => {
      if (
        route.request().url().includes('/executions') ||
        route.request().url().includes('/reviews') ||
        route.request().url().includes('/comments') ||
        route.request().url().includes('/transitions') ||
        route.request().url().includes('/workspace') ||
        route.request().url().includes('/diff') ||
        route.request().url().includes('/recover')
      ) {
        return route.fallback()
      }
      return route.fulfill({ json: task })
    }),
    page.route(`**/api/v1/tasks/${TASK_ID}/executions*`, (route) =>
      route.fulfill({ json: { items: executions, has_more: false } }),
    ),
    page.route(`**/api/v1/tasks/${TASK_ID}/reviews*`, (route) =>
      route.fulfill({ json: [] }),
    ),
    page.route(`**/api/v1/tasks/${TASK_ID}/comments*`, (route) =>
      route.fulfill({ json: { items: [], has_more: false } }),
    ),
    page.route(`**/api/v1/tasks/${TASK_ID}/transitions*`, (route) =>
      route.fulfill({ json: { items: [], has_more: false } }),
    ),
    page.route(`**/api/v1/tasks/${TASK_ID}/diff`, (route) =>
      route.fulfill({ status: 400, body: 'workspace.not_found' }),
    ),
    page.route('**/api/v1/agents*', (route) =>
      route.fulfill({
        json: {
          items: [
            {
              id: 'agent-1',
              name: 'Test Agent',
              executor_type: 'codex',
              status: 'idle',
              effective_status: 'active',
              model: null,
              reasoning_effort: null,
              permission_policy: null,
              version: 1,
              created_at: '2026-04-25T00:00:00Z',
              updated_at: '2026-04-25T00:00:00Z',
            },
          ],
          has_more: false,
        },
      }),
    ),
    page.route('**/api/v1/events*', (route) =>
      route.fulfill({ status: 200, body: '', contentType: 'text/event-stream' }),
    ),
  ])
}

test.describe('execution controls (mocked)', () => {
  test('renders all execution action buttons from server metadata', async ({ page }) => {
    const task = mockTask({ execution_actions: actionsForActiveTask() })
    await setupRoutes(page, task, [mockExecution()])

    await page.goto(`/tasks/${TASK_ID}`)
    await page.waitForLoadState('domcontentloaded')

    await expect(page.getByRole('button', { name: 'Start Manual Execution' })).toBeVisible({ timeout: 10000 })
    await expect(page.getByRole('button', { name: 'Continue Session Manually' })).toBeVisible()
    await expect(page.getByRole('button', { name: 'Re-execute coder' })).toBeVisible()
    await expect(page.getByRole('button', { name: 'Cancel Task' })).toBeVisible()
    await expect(page.getByRole('button', { name: 'Stop Execution' })).toBeVisible()
    await expect(page.getByRole('button', { name: 'Resume coder' })).toBeVisible()
  })

  test('disabled actions are not clickable', async ({ page }) => {
    const task = mockTask({ execution_actions: actionsForActiveTask() })
    await setupRoutes(page, task, [mockExecution()])

    await page.goto(`/tasks/${TASK_ID}`)
    await page.waitForLoadState('domcontentloaded')

    const stopButton = page.getByRole('button', { name: 'Stop Execution' })
    await expect(stopButton).toBeVisible({ timeout: 10000 })
    await expect(stopButton).toBeDisabled()

    const resumeButton = page.getByRole('button', { name: 'Resume coder' })
    await expect(resumeButton).toBeDisabled()
  })

  test('terminal task hides execution actions', async ({ page }) => {
    const task = mockTask({
      status: 'done',
      execution_actions: actionsForTerminalTask(),
    })
    await setupRoutes(page, task, [mockExecution()])

    await page.goto(`/tasks/${TASK_ID}`)
    await page.waitForLoadState('domcontentloaded')

    for (const label of [
      'Start Manual Execution',
      'Continue Session Manually',
      'Resume Execution',
      'Re-execute Execution',
      'Stop Execution',
      'Cancel Task',
    ]) {
      await expect(page.getByRole('button', { name: label })).toHaveCount(0)
    }
  })

  test('manual launch opens dialog', async ({ page }) => {
    const task = mockTask({ execution_actions: actionsForActiveTask() })
    await setupRoutes(page, task, [mockExecution()])

    await page.goto(`/tasks/${TASK_ID}`)
    await page.waitForLoadState('domcontentloaded')

    await page.getByRole('button', { name: 'Start Manual Execution' }).click()
    await expect(page.getByRole('heading', { name: 'Launch Execution' })).toBeVisible({ timeout: 5000 })
    await expect(page.getByLabel('Summary')).toBeVisible()
  })

  test('model-specific reasoning options work across supported viewports', async ({ page }) => {
    const task = mockTask({ execution_actions: actionsForActiveTask() })
    await setupRoutes(page, task, [mockExecution()])
    await page.route('**/api/v1/agents/agent-1/discovered-options*', (route) =>
      route.fulfill({
        json: {
          models: ['gpt-5.6-sol', 'gpt-5.6-luna'],
          permission_policies: ['auto', 'supervised', 'plan'],
          cli_specific: {
            reasoning_efforts: ['low', 'medium', 'high', 'xhigh', 'max', 'ultra'],
            model_reasoning_efforts: {
              'gpt-5.6-sol': ['low', 'medium', 'high', 'xhigh', 'max', 'ultra'],
              'gpt-5.6-luna': ['low', 'medium', 'high', 'xhigh', 'max'],
            },
          },
          available_daemons: [],
          warning: null,
        },
      }),
    )

    for (const viewport of [
      { width: 375, height: 812 },
      { width: 768, height: 1024 },
      { width: 1280, height: 900 },
    ]) {
      await page.setViewportSize(viewport)
      await page.goto(`/tasks/${TASK_ID}`)
      await page.getByRole('button', { name: 'Start Manual Execution' }).click()

      await page.getByRole('button', { name: 'Agent', exact: true }).click()
      await page.getByRole('option', { name: 'Test Agent' }).click()

      await page.getByLabel('Model').click()
      await page.getByRole('option', { name: /gpt-5\.6-luna/ }).click()
      await page.getByLabel('Reasoning').click()
      await expect(page.getByRole('option', { name: 'Max' })).toBeVisible()
      await expect(page.getByRole('option', { name: 'Ultra' })).toHaveCount(0)
      await page.getByRole('option', { name: 'Max' }).click()

      await page.getByLabel('Model').click()
      await page.getByRole('option', { name: /gpt-5\.6-sol/ }).click()
      await page.getByLabel('Reasoning').click()
      await expect(page.getByRole('option', { name: 'Ultra' })).toBeVisible()
      await page.getByRole('option', { name: 'Ultra' }).click()

      const hasHorizontalOverflow = await page.evaluate(
        () => document.documentElement.scrollWidth > document.documentElement.clientWidth,
      )
      expect(hasHorizontalOverflow).toBe(false)
      await page.getByRole('button', { name: 'Cancel', exact: true }).click()
    }
  })

  test('cancel task calls API', async ({ page }) => {
    let cancelCalled = false
    const task = mockTask({ execution_actions: actionsForActiveTask() })
    await setupRoutes(page, task, [mockExecution()])

    await page.route(`**/api/v1/tasks/${TASK_ID}/cancel`, (route) => {
      cancelCalled = true
      return route.fulfill({
        json: mockTask({ status: 'cancelled', version: 2 }),
      })
    })

    await page.goto(`/tasks/${TASK_ID}`)
    await page.waitForLoadState('domcontentloaded')

    await page.getByRole('button', { name: 'Cancel Task' }).click()
    await expect.poll(() => cancelCalled, { timeout: 5000 }).toBe(true)
  })

  test('stop execution calls cancel endpoint for running execution', async ({ page }) => {
    let stopCalled = false
    const runningExec = mockExecution({ status: 'running' })
    const actions = actionsForActiveTask().map((a) =>
      a.action === 'stop_execution'
        ? { ...a, enabled: true, disabled_reason: null }
        : a,
    )
    const task = mockTask({ execution_actions: actions })
    await setupRoutes(page, task, [runningExec])

    await page.route(`**/api/v1/executions/${EXECUTION_ID}/cancel`, (route) => {
      stopCalled = true
      return route.fulfill({
        json: mockExecution({ status: 'cancelled' }),
      })
    })

    await page.goto(`/tasks/${TASK_ID}`)
    await page.waitForLoadState('domcontentloaded')

    const stopButton = page.getByRole('button', { name: 'Stop Execution' })
    await expect(stopButton).toBeEnabled({ timeout: 10000 })
    await stopButton.click()

    await expect.poll(() => stopCalled, { timeout: 5000 }).toBe(true)
  })

  test('re-execute calls re-execute endpoint with target execution id', async ({ page }) => {
    let reExecuteCalled = false
    const task = mockTask({ execution_actions: actionsForActiveTask() })
    await setupRoutes(page, task, [mockExecution()])

    await page.route(`**/api/v1/executions/${EXECUTION_ID}/re-execute`, (route) => {
      reExecuteCalled = true
      return route.fulfill({
        json: mockExecution({ id: 'exec-002', status: 'running' }),
      })
    })

    await page.goto(`/tasks/${TASK_ID}`)
    await page.waitForLoadState('domcontentloaded')

    await page.getByRole('button', { name: 'Re-execute coder' }).click()
    await expect.poll(() => reExecuteCalled, { timeout: 5000 }).toBe(true)
  })

  test('session follow-up navigates to execution detail', async ({ page }) => {
    const task = mockTask({ execution_actions: actionsForActiveTask() })
    await setupRoutes(page, task, [mockExecution()])

    await page.route(`**/api/v1/executions/${EXECUTION_ID}`, (route) =>
      route.fulfill({ json: mockExecution() }),
    )
    await page.route(`**/api/v1/executions/${EXECUTION_ID}/logs*`, (route) =>
      route.fulfill({ json: { items: [], has_more: false } }),
    )

    await page.goto(`/tasks/${TASK_ID}`)
    await page.waitForLoadState('domcontentloaded')

    await page.getByRole('button', { name: 'Continue Session Manually' }).click()

    await expect(page).toHaveURL(new RegExp(`/tasks/${TASK_ID}/executions/${EXECUTION_ID}.*followUp=true`), { timeout: 10000 })
  })

  test('workflow resume calls recover endpoint', async ({ page }) => {
    let recoverCalled = false
    let recoverBody: Record<string, unknown> | null = null

    const actions = actionsForActiveTask().map((a) =>
      a.action === 'workflow_resume'
        ? { ...a, enabled: true, disabled_reason: null }
        : a,
    )
    const task = mockTask({
      execution_actions: actions,
      error_annotation: {
        type: 'execution_stopped',
        blocking_reason: 'execution_stopped',
        blocked_by: 'system',
        blocked_at: '2026-04-25T02:00:00Z',
        blocked_execution_id: EXECUTION_ID,
        artifact: null,
        message: 'Execution stopped',
        recovery_actions: ['resume_session', 'reexecute', 'cancel_task'],
      },
    })
    await setupRoutes(page, task, [mockExecution()])

    await page.route(`**/api/v1/tasks/${TASK_ID}/recover`, async (route) => {
      recoverCalled = true
      recoverBody = (await route.request().postDataJSON()) as Record<string, unknown>
      return route.fulfill({ json: mockTask({ version: 2 }) })
    })

    await page.goto(`/tasks/${TASK_ID}`)
    await page.waitForLoadState('domcontentloaded')

    const resumeButton = page.getByRole('button', { name: 'Resume coder' })
    await expect(resumeButton).toBeEnabled({ timeout: 10000 })
    await resumeButton.click()

    await expect.poll(() => recoverCalled, { timeout: 5000 }).toBe(true)
    expect(recoverBody).toMatchObject({ action: 'resume_session' })
  })

  test('task without execution_actions falls back to basic launch button', async ({ page }) => {
    const task = mockTask({ status: 'in_progress' })
    await setupRoutes(page, task)

    await page.goto(`/tasks/${TASK_ID}`)
    await page.waitForLoadState('domcontentloaded')

    await expect(page.getByRole('button', { name: 'Launch Execution' })).toBeVisible({ timeout: 10000 })
    await expect(page.getByRole('button', { name: 'Start Manual Execution' })).not.toBeVisible()
  })

  test('re-execute via recovery when blocking annotation has reexecute action', async ({ page }) => {
    let recoverCalled = false
    let recoverBody: Record<string, unknown> | null = null

    const reExecAction = actionsForActiveTask().find((a) => a.action === 're_execute')!
    const actions = actionsForActiveTask().map((a) =>
      a.action === 're_execute' ? { ...reExecAction } : a,
    )
    const task = mockTask({
      execution_actions: actions,
      error_annotation: {
        type: 'execution_stopped',
        blocking_reason: 'execution_stopped',
        blocked_by: 'system',
        blocked_at: '2026-04-25T02:00:00Z',
        blocked_execution_id: EXECUTION_ID,
        artifact: null,
        message: 'Execution stopped',
        recovery_actions: ['reexecute', 'cancel_task'],
      },
    })
    await setupRoutes(page, task, [mockExecution()])

    await page.route(`**/api/v1/tasks/${TASK_ID}/recover`, async (route) => {
      recoverCalled = true
      recoverBody = (await route.request().postDataJSON()) as Record<string, unknown>
      return route.fulfill({ json: mockTask({ version: 2 }) })
    })

    await page.goto(`/tasks/${TASK_ID}`)
    await page.waitForLoadState('domcontentloaded')

    await page.getByRole('button', { name: 'Re-execute coder' }).click()
    await expect.poll(() => recoverCalled, { timeout: 5000 }).toBe(true)
    expect(recoverBody).toMatchObject({ action: 'reexecute' })
  })
})
