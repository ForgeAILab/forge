import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { expect, test, type APIRequestContext, type APIResponse } from './fixtures'

const execFileAsync = promisify(execFile)

type PaginatedResponse<T> = {
  items: T[]
  has_more: boolean
}

type ProjectResponse = {
  id: string
  name: string
  settings: Record<string, unknown>
  default_review_config?: { ci_steps: string[]; review_prompt?: string | null } | null
}

type RepoResponse = {
  id: string
  project_id: string
  name: string
  local_path: string | null
  remote_url: string
  work_mode: 'direct_merge' | 'pull_request'
}

type AgentResponse = {
  id: string
  name: string
  executor_type: string
}

type TaskResponse = {
  id: string
  project_id: string
  repo_id: string
  parent_task_id: string | null
  title: string
  description: string | null
  status: string
  subtask_order: number | null
  version: number
  role_assignments: Array<{
    role_name: string
    assignee_type: string | null
    assignee_id: string | null
  }>
}

type WorkspaceResponse = {
  id: string
  task_id: string
  repo_id: string
  worktree_path: string
  branch: string
  status: string
}

type ExecutionResponse = {
  id: string
  task_id: string
  agent_id: string | null
  role: string
  status: 'running' | 'completed' | 'failed' | 'cancelled'
  parent_execution_id: string | null
  summary: string | null
  error: string | null
  workspace_id: string | null
}

type TransitionLogEntry = {
  from_state: string
  to_state: string
  created_at: string
}

const runId = `subtask-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
const fixtureReviewConfig = { ci_steps: ['npm run ci'], review_prompt: null }
const fixtureLifecycleHooks = {
  before_work: [
    {
      type: 'script',
      command: 'npm run install',
      timeout_seconds: 120,
      blocking: true,
    },
  ],
  on_task_done: [
    {
      type: 'script',
      command: 'rm -rf node_modules',
      timeout_seconds: 60,
      blocking: false,
    },
  ],
}

async function expectOk(response: APIResponse, label: string) {
  if (response.ok()) return
  throw new Error(`${label} failed with ${response.status()}: ${await response.text()}`)
}

async function api<T>(
  request: APIRequestContext,
  method: 'GET' | 'POST' | 'PATCH' | 'PUT' | 'DELETE',
  path: string,
  data?: unknown,
): Promise<T> {
  const response = await request.fetch(path, { method, data, failOnStatusCode: false })
  await expectOk(response, `${method} ${path}`)
  if (response.status() === 204) return undefined as T
  return (await response.json()) as T
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

async function getTransitions(
  request: APIRequestContext,
  taskId: string,
): Promise<TransitionLogEntry[]> {
  const response = await api<PaginatedResponse<TransitionLogEntry>>(
    request,
    'GET',
    `/api/v1/tasks/${taskId}/transitions`,
  )
  return response.items
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

async function waitForWorkspace(
  request: APIRequestContext,
  taskId: string,
  timeoutMs: number,
): Promise<WorkspaceResponse> {
  return poll(
    `workspace for task ${taskId}`,
    async () => {
      const response = await request.get(`/api/v1/tasks/${taskId}/workspace`, {
        failOnStatusCode: false,
      })
      if (response.status() === 404) return null
      await expectOk(response, `GET /api/v1/tasks/${taskId}/workspace`)
      return (await response.json()) as WorkspaceResponse
    },
    { timeoutMs, intervalMs: 1000 },
  )
}

async function waitForCompletedExecution(
  request: APIRequestContext,
  taskId: string,
  role: 'coder' | 'reviewer',
  agentId: string,
  timeoutMs: number,
): Promise<ExecutionResponse> {
  return poll(
    `${role} execution for task ${taskId} to complete`,
    async () => {
      const executions = await getExecutions(request, taskId)
      const stopped = executions.find(
        (execution) =>
          execution.role === role &&
          execution.agent_id === agentId &&
          (execution.status === 'failed' || execution.status === 'cancelled'),
      )
      if (stopped) {
        throw new Error(
          `${role} execution ${stopped.id} ended with ${stopped.status}: ${stopped.error ?? 'no error'}`,
        )
      }
      return (
        executions.find(
          (execution) =>
            execution.role === role &&
            execution.agent_id === agentId &&
            execution.status === 'completed',
        ) ?? null
      )
    },
    { timeoutMs },
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

async function createViteReactFixture(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), `forge-subtask-${runId}-`))
  await mkdir(join(root, 'src'), { recursive: true })
  await writeFile(
    join(root, 'package.json'),
    JSON.stringify(
      {
        scripts: {
          install: 'npm install --ignore-scripts --no-package-lock',
          dev: 'vite',
          build: 'vite build',
          ci: 'npm run build',
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
    'export function App() {\n  return <main>Vite React fixture</main>\n}\n',
  )
  await execFileAsync('git', ['init'], { cwd: root })
  await execFileAsync('git', ['checkout', '-B', 'main'], { cwd: root })
  await execFileAsync('git', ['config', 'user.email', 'forge-integration@example.test'], {
    cwd: root,
  })
  await execFileAsync('git', ['config', 'user.name', 'Forge Integration'], { cwd: root })
  await execFileAsync('git', ['add', '.'], { cwd: root })
  await execFileAsync('git', ['commit', '-m', 'Initial Vite React fixture'], { cwd: root })
  return root
}

test.describe('subtask flow (integration)', () => {
  test('creates a coordination root with independently executed ordered subtasks, reviews, and merges', async ({
    page,
    request,
  }) => {
    test.setTimeout(30 * 60 * 1000)
    const createdAgentIds: string[] = []
    const createdTaskIds: string[] = []
    const fixturePath = await createViteReactFixture()

    try {
      const projectsResponse = await request.get('/api/v1/projects', { failOnStatusCode: false })
      await expectOk(
        projectsResponse,
        'GET /api/v1/projects. Start the Forge API server on localhost:8080 before running this integration test',
      )

      const project = await api<ProjectResponse>(request, 'POST', '/api/v1/projects', {
        name: `Ordered Subtasks ${runId}`,
        settings: { lifecycle_hooks: fixtureLifecycleHooks },
        default_review_config: fixtureReviewConfig,
      })
      await api<ProjectResponse>(request, 'POST', `/api/v1/projects/${project.id}/pause`)
      await api<unknown>(request, 'PUT', `/api/v1/projects/${project.id}/workflow`, {
        template_name: 'no-user-approval',
      })

      const repo = await api<RepoResponse>(
        request,
        'POST',
        `/api/v1/projects/${project.id}/repos`,
        {
          name: `subtask-app-${runId}`,
          remote_url: fixturePath,
          local_path: fixturePath,
          work_mode: 'direct_merge',
          default_branch: 'main',
        },
      )

      const coder = await api<AgentResponse>(request, 'POST', '/api/v1/agents', {
        name: `Subtask Haiku Coder ${runId}`,
        executor_type: 'claude_code',
        model: 'claude-haiku-4-5',
        permission_policy: 'supervised',
        max_concurrent_tasks: 1,
      })
      createdAgentIds.push(coder.id)

      const reviewer = await api<AgentResponse>(request, 'POST', '/api/v1/agents', {
        name: `Subtask Haiku Reviewer ${runId}`,
        executor_type: 'claude_code',
        model: 'claude-haiku-4-5',
        permission_policy: 'supervised',
        max_concurrent_tasks: 1,
      })
      createdAgentIds.push(reviewer.id)

      await api<ProjectResponse>(request, 'PATCH', `/api/v1/projects/${project.id}`, {
        name: project.name,
        settings: {
          default_role_assignments: [
            { role_name: 'coder', assignee_type: 'agent', assignee_id: coder.id },
            { role_name: 'reviewer', assignee_type: 'agent', assignee_id: reviewer.id },
          ],
          lifecycle_hooks: fixtureLifecycleHooks,
        },
        default_review_config: fixtureReviewConfig,
      })

      // --- Create parent task ---
      const parentTask = await api<TaskResponse>(
        request,
        'POST',
        `/api/v1/projects/${project.id}/tasks`,
        {
          title: `Parent task with ordered subtasks ${runId}`,
          description:
            'This root coordinates the ordered subtasks below. Implementation runs only on the child tasks.',
          task_type: 'task',
          priority: 0,
        },
      )
      createdTaskIds.push(parentTask.id)
      expect(parentTask.parent_task_id).toBeNull()

      // --- Create subtask 1 ---
      const subtask1 = await api<TaskResponse>(
        request,
        'POST',
        `/api/v1/projects/${project.id}/tasks`,
        {
          title: `Subtask 1: add header component ${runId}`,
          description: [
            'Edit src/App.tsx.',
            `Replace "Vite React fixture" with "Subtask one done ${runId}".`,
            'Keep the change minimal and do not run tests.',
          ].join('\n'),
          parent_task_id: parentTask.id,
          task_type: 'task',
          priority: 0,
        },
      )
      createdTaskIds.push(subtask1.id)
      expect(subtask1.parent_task_id).toBe(parentTask.id)
      expect(subtask1.subtask_order).toBe(0)

      // --- Create subtask 2 ---
      const subtask2 = await api<TaskResponse>(
        request,
        'POST',
        `/api/v1/projects/${project.id}/tasks`,
        {
          title: `Subtask 2: add footer component ${runId}`,
          description: [
            'Edit src/App.tsx.',
            `Replace the current text (whatever it is) in the <main> tag with "Subtask two done ${runId}".`,
            'Keep the change minimal and do not run tests.',
          ].join('\n'),
          parent_task_id: parentTask.id,
          task_type: 'task',
          priority: 0,
        },
      )
      createdTaskIds.push(subtask2.id)
      expect(subtask2.parent_task_id).toBe(parentTask.id)
      expect(subtask2.subtask_order).toBe(1)

      // --- Verify subtask ordering ---
      expect(subtask1.subtask_order).toBeLessThan(subtask2.subtask_order!)

      await api<ProjectResponse>(request, 'POST', `/api/v1/projects/${project.id}/resume`)

      // --- The first child, not the coordination root, starts implementation ---
      await waitForTaskStatus(
        request,
        subtask1.id,
        ['in_progress', 'done'],
        120000,
      )

      // --- Verify workspace is created for the parent task ---
      const parentWorkspace = await waitForWorkspace(request, parentTask.id, 120000)
      expect(parentWorkspace).toMatchObject({
        task_id: parentTask.id,
        repo_id: repo.id,
        branch: expect.stringMatching(/^task\//),
      })

      // --- Wait for both subtasks to reach done ---
      const [doneSubtask1, doneSubtask2] = await Promise.all([
        waitForTaskStatus(request, subtask1.id, ['done'], 15 * 60 * 1000),
        waitForTaskStatus(request, subtask2.id, ['done'], 15 * 60 * 1000),
      ])
      expect(doneSubtask1.status).toBe('done')
      expect(doneSubtask2.status).toBe('done')

      // --- Each child has its own execution while sharing the root workspace ---
      const [subtask1Execution, subtask2Execution] = await Promise.all([
        waitForCompletedExecution(request, subtask1.id, 'coder', coder.id, 120000),
        waitForCompletedExecution(request, subtask2.id, 'coder', coder.id, 120000),
      ])
      expect(subtask1Execution.id).not.toBe(subtask2Execution.id)
      expect(subtask1Execution.workspace_id).toBe(parentWorkspace.id)
      expect(subtask2Execution.workspace_id).toBe(parentWorkspace.id)

      const [subtask1Transitions, subtask2Transitions] = await Promise.all([
        getTransitions(request, subtask1.id),
        getTransitions(request, subtask2.id),
      ])
      expect(subtask1Transitions.some((entry) => entry.to_state === 'in_progress')).toBeTruthy()
      expect(subtask1Transitions.some((entry) => entry.to_state === 'done')).toBeTruthy()
      expect(subtask2Transitions.some((entry) => entry.to_state === 'in_progress')).toBeTruthy()
      expect(subtask2Transitions.some((entry) => entry.to_state === 'done')).toBeTruthy()

      // --- Wait for parent task to reach review/merging/done ---
      await waitForTaskStatus(request, parentTask.id, ['review', 'merging', 'done'], 120000)

      // --- Wait for reviewer to complete on the parent ---
      const reviewerExecution = await waitForCompletedExecution(
        request,
        parentTask.id,
        'reviewer',
        reviewer.id,
        15 * 60 * 1000,
      )
      expect(reviewerExecution.workspace_id).toBe(parentWorkspace.id)

      const rootExecutions = await getExecutions(request, parentTask.id)
      expect(rootExecutions.some((execution) => execution.role === 'coder')).toBeFalsy()

      // --- Wait for parent to reach done ---
      const doneParent = await waitForTaskStatus(request, parentTask.id, ['done'], 5 * 60 * 1000)
      expect(doneParent.status).toBe('done')

      // --- Verify the final file content reflects subtask 2 (the last sequential edit) ---
      const appSource = await readFile(join(fixturePath, 'src/App.tsx'), 'utf8')
      expect(appSource).toContain(`Subtask two done ${runId}`)

      // --- Verify on the board ---
      await page.goto(`/projects/${project.id}/board`)
      await page.waitForLoadState('domcontentloaded')
      await expect(page.getByText(parentTask.title)).toBeVisible({ timeout: 15000 })
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
