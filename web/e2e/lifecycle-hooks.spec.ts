import { execFile } from 'node:child_process'
import { mkdtemp, mkdir, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { promisify } from 'node:util'
import { expect, test, type APIRequestContext, type APIResponse, type Page } from './fixtures'

const PROJECT_ID = 'proj-lifecycle-hooks'
const TASK_ID = 'task-lifecycle-hook-blocked'
const execFileAsync = promisify(execFile)
const runId = `hook-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
const blockingLifecycleHooks = {
  before_work: [
    {
      type: 'script',
      command: "printf 'flow-blocked-out\\n'; printf 'flow-blocked-err\\n' >&2; exit 9",
      timeout_seconds: 5,
      blocking: true,
    },
  ],
}
const FAILED_HOOK_RECOVERY_ACTIONS = [
  'retry_hook',
  'update_workspace_and_retry_hook',
  'skip_hook_once',
  'cancel_task',
]

type PaginatedResponse<T> = {
  items: T[]
  has_more: boolean
}

type ProjectResponse = {
  id: string
  name: string
  settings: Record<string, unknown>
}

type RepoResponse = {
  id: string
  project_id: string
}

type AgentResponse = {
  id: string
  name: string
}

type TaskResponse = {
  id: string
  project_id: string
  repo_id: string
  title: string
  status: string
  version: number
  error_annotation?: Record<string, unknown> | null
  role_assignments: Array<{
    role_name: string
    assignee_type: string | null
    assignee_id: string | null
  }>
}

type ExecutionResponse = {
  id: string
  status: string
  agent_id: string | null
  role: string | null
}

async function api<T>(
  request: APIRequestContext,
  method: 'GET' | 'POST' | 'PATCH' | 'PUT' | 'DELETE',
  path: string,
  data?: unknown,
): Promise<T> {
  const response = await request.fetch(path, {
    method,
    data,
    failOnStatusCode: false,
  })
  await expectOk(response, `${method} ${path}`)
  if (response.status() === 204) return undefined as T
  return (await response.json()) as T
}

async function expectOk(response: APIResponse, label: string) {
  if (response.ok()) return
  throw new Error(`${label} failed with ${response.status()}: ${await response.text()}`)
}

async function poll<T>(
  label: string,
  fn: () => Promise<T | null | undefined | false>,
  options: { timeoutMs: number; intervalMs?: number },
): Promise<T> {
  const startedAt = Date.now()
  const intervalMs = options.intervalMs ?? 5000
  let lastError: unknown

  while (Date.now() - startedAt < options.timeoutMs) {
    try {
      const value = await fn()
      if (value) return value
    } catch (error) {
      lastError = error
    }
    await new Promise((resolve) => setTimeout(resolve, intervalMs))
  }

  const suffix = lastError instanceof Error ? ` Last error: ${lastError.message}` : ''
  throw new Error(`Timed out waiting for ${label}.${suffix}`)
}

async function getTask(request: APIRequestContext, taskId: string): Promise<TaskResponse> {
  return api<TaskResponse>(request, 'GET', `/api/v1/tasks/${taskId}`)
}

async function waitForTaskStatus(
  request: APIRequestContext,
  taskId: string,
  statuses: string[],
  timeoutMs: number,
): Promise<TaskResponse> {
  return poll(
    `task ${taskId} to reach ${statuses.join(' or ')}`,
    async () => {
      const task = await getTask(request, taskId)
      return statuses.includes(task.status) ? task : null
    },
    { timeoutMs },
  )
}

async function waitForTaskAnnotation(
  request: APIRequestContext,
  taskId: string,
  type: string,
  timeoutMs: number,
): Promise<TaskResponse> {
  return poll(
    `task ${taskId} to record ${type}`,
    async () => {
      const task = await getTask(request, taskId)
      return task.error_annotation?.type === type ? task : null
    },
    { timeoutMs, intervalMs: 1000 },
  )
}

async function getExecutions(
  request: APIRequestContext,
  taskId: string,
): Promise<ExecutionResponse[]> {
  const response = await api<PaginatedResponse<ExecutionResponse>>(
    request,
    'GET',
    `/api/v1/tasks/${taskId}/executions`,
  )
  return response.items
}

async function waitForExecutionCreated(
  request: APIRequestContext,
  taskId: string,
  predicate: (execution: ExecutionResponse) => boolean,
  timeoutMs: number,
): Promise<ExecutionResponse> {
  return poll(
    `execution for task ${taskId}`,
    async () => {
      const executions = await getExecutions(request, taskId)
      return executions.find(predicate) ?? null
    },
    { timeoutMs, intervalMs: 1000 },
  )
}

async function waitForNoRunningExecutions(
  request: APIRequestContext,
  taskId: string,
  timeoutMs: number,
): Promise<boolean> {
  return poll(
    `executions for task ${taskId} to stop`,
    async () => {
      const executions = await getExecutions(request, taskId)
      return executions.every((execution) => execution.status !== 'running') ? true : null
    },
    { timeoutMs, intervalMs: 2000 },
  )
}

async function cleanupTask(request: APIRequestContext, taskId: string): Promise<boolean> {
  const taskResponse = await request.get(`/api/v1/tasks/${taskId}`, { failOnStatusCode: false })
  if (!taskResponse.ok()) return true

  const task = (await taskResponse.json()) as TaskResponse
  if (!['done', 'cancelled'].includes(task.status)) {
    await request.post(`/api/v1/tasks/${taskId}/cancel`, { failOnStatusCode: false })
    const cancelled = await waitForTaskStatus(request, taskId, ['done', 'cancelled'], 120000)
      .then(() => true)
      .catch(() => false)
    if (!cancelled) return false
  }

  const stopped = await waitForNoRunningExecutions(request, taskId, 120000)
    .then(() => true)
    .catch(() => false)
  if (!stopped) return false

  await request.delete(`/api/v1/tasks/${taskId}`, { failOnStatusCode: false })
  return true
}

async function createReactFixture(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), `forge-blocking-hook-${runId}-`))
  await mkdir(join(root, 'src'), { recursive: true })
  await writeFile(
    join(root, 'package.json'),
    JSON.stringify(
      {
        scripts: {
          build: 'vite build',
        },
        dependencies: {
          '@vitejs/plugin-react': 'latest',
          vite: 'latest',
          react: 'latest',
          'react-dom': 'latest',
        },
        devDependencies: {},
      },
      null,
      2,
    ),
  )
  await writeFile(
    join(root, 'index.html'),
    '<div id="root"></div>\n<script type="module" src="/src/App.tsx"></script>\n',
  )
  await writeFile(join(root, '.gitignore'), 'node_modules\n')
  await writeFile(
    join(root, 'src/App.tsx'),
    'export function App() {\n  return <main>Lifecycle hook fixture</main>\n}\n',
  )
  await execFileAsync('git', ['init'], { cwd: root })
  await execFileAsync('git', ['checkout', '-B', 'main'], { cwd: root })
  await execFileAsync('git', ['config', 'user.email', 'forge-integration@example.test'], {
    cwd: root,
  })
  await execFileAsync('git', ['config', 'user.name', 'Forge Integration'], { cwd: root })
  await execFileAsync('git', ['add', '.'], { cwd: root })
  await execFileAsync('git', ['commit', '-m', 'Initial lifecycle hook fixture'], { cwd: root })
  return root
}

function project(settings: Record<string, unknown> = {}) {
  return {
    id: PROJECT_ID,
    name: 'Lifecycle Hook Project',
    settings,
    workflow_template_name: null,
    default_review_config: { ci_steps: [], review_prompt: null },
    created_at: '2026-04-25T00:00:00Z',
    updated_at: '2026-04-25T00:00:00Z',
  }
}

function emptyHooks() {
  return { before_exit: [], on_exit: [], before_enter: [], on_enter: [], after_enter: [] }
}

function workflow() {
  return {
    states: [
      {
        name: 'todo',
        kind: 'initial',
        column: 'Todo',
        display_name: 'Todo',
        role: null,
        hooks: emptyHooks(),
        cleanup: null,
        gate_config: null,
        dispatch: null,
        triggers: {
          accept: { to: 'in_progress', dispatch: null },
        },
        config: {},
      },
      {
        name: 'in_progress',
        kind: 'active',
        column: 'In Progress',
        display_name: 'In Progress',
        role: 'coder',
        hooks: emptyHooks(),
        cleanup: null,
        gate_config: null,
        dispatch: null,
        triggers: {},
        config: {},
      },
      {
        name: 'done',
        kind: 'terminal',
        column: 'Done',
        display_name: 'Done',
        role: null,
        hooks: emptyHooks(),
        cleanup: null,
        gate_config: null,
        dispatch: null,
        triggers: {},
        config: {},
      },
    ],
    roles: [{ name: 'coder', display_name: 'Coder', description: '' }],
    configuration: [],
    cancellation_state: null,
  }
}

async function mockSettingsRoutes(page: Page) {
  let patchBody: Record<string, unknown> | null = null

  await Promise.all([
    page.route('**/api/v1/projects', (route) =>
      route.fulfill({ json: { items: [project()], has_more: false } }),
    ),
    page.route(`**/api/v1/projects/${PROJECT_ID}`, async (route) => {
      if (route.request().method() === 'PATCH') {
        patchBody = (await route.request().postDataJSON()) as Record<string, unknown>
        const settings = (patchBody.settings ?? {}) as Record<string, unknown>
        return route.fulfill({ json: project(settings) })
      }
      return route.fulfill({ json: project() })
    }),
    page.route(`**/api/v1/projects/${PROJECT_ID}/repos*`, (route) =>
      route.fulfill({ json: { items: [], has_more: false } }),
    ),
    page.route(`**/api/v1/projects/${PROJECT_ID}/tasks*`, (route) =>
      route.fulfill({ json: { items: [], has_more: false } }),
    ),
    page.route(`**/api/v1/projects/${PROJECT_ID}/workflow`, (route) =>
      route.fulfill({ json: workflow() }),
    ),
    page.route('**/api/v1/workflow-templates', (route) => route.fulfill({ json: [] })),
    page.route('**/api/v1/workflow/prompt-builders', (route) => route.fulfill({ json: [] })),
    page.route('**/api/v1/agents*', (route) =>
      route.fulfill({ json: { items: [], has_more: false } }),
    ),
    page.route('**/api/v1/notifications/unread-count*', (route) =>
      route.fulfill({ json: { count: 0 } }),
    ),
    page.route('**/api/v1/notifications*', (route) =>
      route.fulfill({ json: { items: [], has_more: false } }),
    ),
    page.route('**/api/v1/events*', (route) =>
      route.fulfill({ status: 200, body: '', contentType: 'text/event-stream' }),
    ),
  ])

  return () => patchBody
}

function blockedTask() {
  return {
    id: TASK_ID,
    project_id: PROJECT_ID,
    repo_id: 'repo-1',
    title: 'Blocked by required hook',
    description: 'Task did not start because the hook failed',
    status: 'in_progress',
    task_type: 'task',
    priority: 50,
    assignee: null,
    parent_task_id: null,
    role_assignments: [],
    remaining_retries: {},
    task_state_config: null,
    review_passed_at: null,
    workspace: null,
    placement: null,
    version: 1,
    created_at: '2026-04-25T00:00:00Z',
    updated_at: '2026-04-25T00:00:00Z',
    error_annotation: {
      type: 'before_work_hook_failed',
      blocking_reason: 'before_work_hook_failed',
      blocked_by: 'system:lifecycle_hook',
      blocked_at: '2026-04-25T00:00:00Z',
      blocked_execution_id: null,
      artifact: {
        kind: 'hook',
        id: 'before_work:0',
        log_path: '/tmp/forge/logs/task/hooks/hook-before_work-0.jsonl',
      },
      message: 'exit code 9',
      recovery_actions: FAILED_HOOK_RECOVERY_ACTIONS,
      hook: {
        command: 'echo preflight-out; echo preflight-err >&2; exit 9',
        exit_code: 9,
        timeout: false,
        duration_ms: 17,
        working_dir: '/tmp/hook-worktree',
        stdout: 'preflight-out\n',
        stderr: 'preflight-err\n',
      },
    },
  }
}

async function mockBlockedTaskRoutes(page: Page) {
  const task = blockedTask()
  const recoverBodies: Record<string, unknown>[] = []

  await Promise.all([
    page.route('**/api/v1/projects', (route) =>
      route.fulfill({ json: { items: [project()], has_more: false } }),
    ),
    page.route(`**/api/v1/projects/${PROJECT_ID}`, (route) =>
      route.fulfill({ json: project() }),
    ),
    page.route(`**/api/v1/projects/${PROJECT_ID}/workflow`, (route) =>
      route.fulfill({ json: workflow() }),
    ),
    page.route(`**/api/v1/projects/${PROJECT_ID}/tasks*`, (route) =>
      route.fulfill({ json: { items: [task], has_more: false } }),
    ),
    page.route(`**/api/v1/tasks/${TASK_ID}`, (route) => route.fulfill({ json: task })),
    page.route(`**/api/v1/tasks/${TASK_ID}/executions*`, (route) =>
      route.fulfill({ json: { items: [], has_more: false } }),
    ),
    page.route(`**/api/v1/tasks/${TASK_ID}/reviews*`, (route) => route.fulfill({ json: [] })),
    page.route(`**/api/v1/tasks/${TASK_ID}/comments*`, (route) =>
      route.fulfill({ json: { items: [], has_more: false } }),
    ),
    page.route(`**/api/v1/tasks/${TASK_ID}/recover`, async (route) => {
      recoverBodies.push((await route.request().postDataJSON()) as Record<string, unknown>)
      return route.fulfill({ json: task })
    }),
    page.route(`**/api/v1/tasks/${TASK_ID}/diff`, (route) =>
      route.fulfill({ status: 400, body: 'workspace.not_found' }),
    ),
    page.route('**/api/v1/agents*', (route) =>
      route.fulfill({ json: { items: [], has_more: false } }),
    ),
    page.route('**/api/v1/notifications/unread-count*', (route) =>
      route.fulfill({ json: { count: 0 } }),
    ),
    page.route('**/api/v1/notifications*', (route) =>
      route.fulfill({ json: { items: [], has_more: false } }),
    ),
    page.route('**/api/v1/events*', (route) =>
      route.fulfill({ status: 200, body: '', contentType: 'text/event-stream' }),
    ),
  ])

  return () => recoverBodies
}

test.describe('lifecycle hook settings', () => {
  test('adds, edits, and saves a required before_work script hook', async ({ page }) => {
    const getPatchBody = await mockSettingsRoutes(page)

    await page.goto(`/projects/${PROJECT_ID}/settings`)
    await page.getByRole('link', { name: 'Hooks' }).click()
    await expect(page.getByRole('heading', { name: 'Lifecycle Hooks' })).toBeVisible()

    await page.getByRole('button', { name: 'Add Script' }).click()
    await expect(page.getByRole('heading', { name: 'Add Script Hook' })).toBeVisible()
    await page.locator('[aria-label="Shell command editor"] .cm-content').last().click()
    await page.keyboard.insertText('echo initial')
    await page.locator('#lifecycle-dialog-timeout').fill('12')
    await page.getByText('Require this script before agent dispatch').click()
    await page.getByRole('button', { name: 'Add Script' }).last().click()

    await expect(page.locator('.cm-content')).toBeVisible()
    await page.locator('[aria-label="Shell command editor"] .cm-content').click()
    await page.keyboard.press('ControlOrMeta+A')
    await page.keyboard.insertText('echo edited')
    await page.locator('#script-before_work-0-timeout').fill('15')

    await page.getByRole('button', { name: 'Save' }).click()
    await expect.poll(() => getPatchBody()).not.toBeNull()

    const body = getPatchBody()
    const settings = body?.settings as Record<string, unknown>
    const lifecycleHooks = settings.lifecycle_hooks as Record<string, unknown>
    const beforeWork = lifecycleHooks.before_work as Array<Record<string, unknown>>
    expect(beforeWork).toHaveLength(1)
    expect(beforeWork[0]).toMatchObject({
      type: 'script',
      command: 'echo edited',
      timeout_seconds: 15,
      blocking: true,
    })
  })

  test('shows failed required hook output on blocked task detail', async ({ page }) => {
    await mockBlockedTaskRoutes(page)

    await page.goto(`/tasks/${TASK_ID}`)

    await expect(page.getByText('Before work hook failed', { exact: true })).toBeVisible()
    await expect(page.getByText('exit code 9', { exact: true }).first()).toBeVisible()
    await expect(page.getByText('echo preflight-out; echo preflight-err >&2; exit 9')).toBeVisible()
    await expect(page.getByText('exit 9', { exact: true })).toBeVisible()
    await expect(page.getByText('/tmp/hook-worktree')).toBeVisible()
    await expect(page.getByText('preflight-err', { exact: true })).toBeVisible()
    await expect(page.getByText('preflight-out', { exact: true })).toBeVisible()
    await expect(page.getByRole('button', { name: 'Retry Hook' })).toBeVisible()
    await page.getByLabel('More recovery actions').click()
    await expect(page.getByRole('button', { name: 'Update Workspace + Retry' })).toBeVisible()
    await expect(page.getByRole('button', { name: 'Skip Hook Once' })).toBeVisible()
    await expect(page.getByRole('button', { name: 'Cancel Task', exact: true }).first()).toBeVisible()
  })

  test('submits secondary failed-hook recovery actions', async ({ page }) => {
    const getRecoverBodies = await mockBlockedTaskRoutes(page)

    await page.goto(`/tasks/${TASK_ID}`)
    await expect(page.getByRole('button', { name: 'Retry Hook' })).toBeVisible()

    await page.getByLabel('More recovery actions').click()
    await page.getByRole('button', { name: 'Update Workspace + Retry' }).click()
    await expect.poll(() => getRecoverBodies()).toHaveLength(1)
    expect(getRecoverBodies()[0]).toMatchObject({
      action: 'update_workspace_and_retry_hook',
      reason: null,
      context: null,
    })

    await page.getByLabel('More recovery actions').click()
    await page.getByRole('button', { name: 'Skip Hook Once' }).click()
    await expect.poll(() => getRecoverBodies()).toHaveLength(2)
    expect(getRecoverBodies()[1]).toMatchObject({
      action: 'skip_hook_once',
      reason: null,
      context: null,
    })
  })
})

test.describe('lifecycle hook blocking flow (integration)', () => {
  test('records a failed required before_work hook before dispatching an agent', async ({
    request,
  }) => {
    test.setTimeout(5 * 60 * 1000)
    const createdAgentIds: string[] = []
    const createdTaskIds: string[] = []
    const fixturePath = await createReactFixture()

    try {
      const projectsResponse = await request.get('/api/v1/projects', { failOnStatusCode: false })
      await expectOk(
        projectsResponse,
        'GET /api/v1/projects. Start the Forge API server on localhost:8080 before running this integration test',
      )

      const project = await api<ProjectResponse>(request, 'POST', '/api/v1/projects', {
        name: `Blocked Hook Project ${runId}`,
        settings: { lifecycle_hooks: blockingLifecycleHooks },
        default_review_config: { ci_steps: [], review_prompt: null },
      })
      await api<unknown>(request, 'PUT', `/api/v1/projects/${project.id}/workflow`, {
        template_name: 'no-user-approval',
      })

      await api<RepoResponse>(
        request,
        'POST',
        `/api/v1/projects/${project.id}/repos`,
        {
          name: `blocked-hook-app-${runId}`,
          remote_url: fixturePath,
          local_path: fixturePath,
          default_branch: 'main',
        },
      )

      const coder = await api<AgentResponse>(request, 'POST', '/api/v1/agents', {
        name: `Blocking Hook Coder ${runId}`,
        executor_type: 'codex',
        model: 'gpt-5.3-codex',
        permission_policy: 'supervised',
        max_concurrent_tasks: 1,
      })
      createdAgentIds.push(coder.id)

      await api<ProjectResponse>(request, 'PATCH', `/api/v1/projects/${project.id}`, {
        name: project.name,
        settings: {
          default_role_assignments: [
            { role_name: 'coder', assignee_type: 'agent', assignee_id: coder.id },
          ],
          lifecycle_hooks: blockingLifecycleHooks,
        },
        default_review_config: { ci_steps: [], review_prompt: null },
      })

      const task = await api<TaskResponse>(
        request,
        'POST',
        `/api/v1/projects/${project.id}/tasks`,
        {
          title: `Block before work hook ${runId}`,
          description:
            'This task should never reach the agent because the required before_work hook exits 9.',
          task_type: 'task',
          priority: 0,
        },
      )
      createdTaskIds.push(task.id)

      expect(task.role_assignments).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            role_name: 'coder',
            assignee_type: 'agent',
            assignee_id: coder.id,
          }),
        ]),
      )

      const blockedTask = await waitForTaskAnnotation(
        request,
        task.id,
        'before_work_hook_failed',
        120000,
      )
      expect(blockedTask.error_annotation).toMatchObject({
        type: 'before_work_hook_failed',
        blocking_reason: 'before_work_hook_failed',
        blocked_by: 'system:lifecycle_hook',
        artifact: { kind: 'hook', id: 'before_work:0' },
        hook: expect.objectContaining({
          command: blockingLifecycleHooks.before_work[0].command,
          exit_code: 9,
          timeout: false,
          stdout: 'flow-blocked-out\n',
          stderr: 'flow-blocked-err\n',
        }),
        recovery_actions: FAILED_HOOK_RECOVERY_ACTIONS,
      })

      const executions = await getExecutions(request, task.id)
      expect(executions).toEqual([])
    } finally {
      let canRemoveBackingResources = true
      for (const taskId of createdTaskIds.reverse()) {
        canRemoveBackingResources =
          (await cleanupTask(request, taskId)) && canRemoveBackingResources
      }
      if (canRemoveBackingResources) {
        for (const agentId of createdAgentIds.reverse()) {
          await request.delete(`/api/v1/agents/${agentId}`, { failOnStatusCode: false })
        }
        await rm(fixturePath, { recursive: true, force: true })
      }
    }
  })

  test('skip_hook_once dispatches the assigned live agent after a failed before_work hook', async ({
    request,
  }) => {
    test.setTimeout(5 * 60 * 1000)
    const createdAgentIds: string[] = []
    const createdTaskIds: string[] = []
    const fixturePath = await createReactFixture()

    try {
      const projectsResponse = await request.get('/api/v1/projects', { failOnStatusCode: false })
      await expectOk(
        projectsResponse,
        'GET /api/v1/projects. Start the Forge API server on localhost:8080 before running this integration test',
      )

      const project = await api<ProjectResponse>(request, 'POST', '/api/v1/projects', {
        name: `Skip Hook Live Agent Project ${runId}`,
        settings: { lifecycle_hooks: blockingLifecycleHooks },
        default_review_config: { ci_steps: [], review_prompt: null },
      })
      await api<unknown>(request, 'PUT', `/api/v1/projects/${project.id}/workflow`, {
        template_name: 'no-user-approval',
      })

      await api<RepoResponse>(
        request,
        'POST',
        `/api/v1/projects/${project.id}/repos`,
        {
          name: `skip-hook-live-agent-app-${runId}`,
          remote_url: fixturePath,
          local_path: fixturePath,
          default_branch: 'main',
        },
      )

      const planner = await api<AgentResponse>(request, 'POST', '/api/v1/agents', {
        name: `Skip Hook Shell Planner ${runId}`,
        executor_type: 'shell',
        max_concurrent_tasks: 1,
      })
      createdAgentIds.push(planner.id)

      await api<ProjectResponse>(request, 'PATCH', `/api/v1/projects/${project.id}`, {
        name: project.name,
        settings: {
          default_role_assignments: [
            { role_name: 'planner', assignee_type: 'agent', assignee_id: planner.id },
          ],
          lifecycle_hooks: blockingLifecycleHooks,
        },
        default_review_config: { ci_steps: [], review_prompt: null },
      })

      const task = await api<TaskResponse>(
        request,
        'POST',
        `/api/v1/projects/${project.id}/tasks`,
        {
          title: `Skip before work hook once ${runId}`,
          description: "printf 'skip-hook-live-agent-dispatched\\n'",
          task_type: 'task',
          priority: 0,
        },
      )
      createdTaskIds.push(task.id)

      const blockedTask = await waitForTaskAnnotation(
        request,
        task.id,
        'before_work_hook_failed',
        120000,
      )
      expect(blockedTask.error_annotation).toMatchObject({
        type: 'before_work_hook_failed',
        recovery_actions: FAILED_HOOK_RECOVERY_ACTIONS,
      })
      expect(await getExecutions(request, task.id)).toEqual([])

      const recovered = await api<TaskResponse>(
        request,
        'POST',
        `/api/v1/tasks/${task.id}/recover`,
        {
          action: 'skip_hook_once',
          reason: 'e2e: verify live agent dispatch after skipping hook once',
        },
      )
      expect(recovered.error_annotation).toBeNull()

      const execution = await waitForExecutionCreated(
        request,
        task.id,
        (item) => item.agent_id === planner.id && item.role === 'planner',
        120000,
      )
      expect(['running', 'completed', 'failed', 'cancelled']).toContain(execution.status)
    } finally {
      let canRemoveBackingResources = true
      for (const taskId of createdTaskIds.reverse()) {
        canRemoveBackingResources =
          (await cleanupTask(request, taskId)) && canRemoveBackingResources
      }
      if (canRemoveBackingResources) {
        for (const agentId of createdAgentIds.reverse()) {
          await request.delete(`/api/v1/agents/${agentId}`, { failOnStatusCode: false })
        }
        await rm(fixturePath, { recursive: true, force: true })
      }
    }
  })
})
