import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from 'node:fs/promises'
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
  title: string
  status: string
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
  parent_execution_id?: string | null
  agent_session_id?: string | null
  summary: string | null
  error: string | null
  executor_config_snapshot?: Record<string, unknown> | null
  workspace_id: string | null
}

type LogEntry = {
  sequence: number
  kind: string
  payload: unknown
}

const ACTIVE_OR_COMPLETED_TASK_STATUSES = [
  'in_progress',
  'review',
  'merging',
  'merge_failed',
  'done',
]

const runId = `int-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
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

async function getExecutionLogs(
  request: APIRequestContext,
  executionId: string,
): Promise<LogEntry[]> {
  const response = await api<{ items: LogEntry[]; has_more: boolean }>(
    request,
    'GET',
    `/api/v1/executions/${executionId}/logs?tail=500`,
  )
  return response.items
}

function asRecord(value: unknown): Record<string, unknown> | null {
  return value && typeof value === 'object' && !Array.isArray(value)
    ? (value as Record<string, unknown>)
    : null
}

function executionConfig(execution: ExecutionResponse): Record<string, unknown> | null {
  return asRecord(asRecord(execution.executor_config_snapshot)?.config)
}

function contentText(content: unknown): string {
  if (!Array.isArray(content)) return ''
  return content
    .map((item) => {
      const record = asRecord(item)
      return typeof record?.text === 'string' ? record.text : ''
    })
    .filter(Boolean)
    .join('')
}

function codexUserMessages(logs: LogEntry[]): string[] {
  const messages: string[] = []
  for (const log of logs) {
    const payload = asRecord(log.payload)
    if (payload?.method !== 'item/completed') continue
    const params = asRecord(payload.params)
    const item = asRecord(params?.item)
    if (item?.type !== 'userMessage') continue
    const text = contentText(item.content).trim()
    if (text) messages.push(text)
  }
  return messages
}

async function waitForMergeFollowUpExecution(
  request: APIRequestContext,
  taskIds: string[],
  coderId: string,
  timeoutMs: number,
): Promise<{ taskId: string; execution: ExecutionResponse }> {
  return poll(
    'merge-conflict follow-up coder execution',
    async () => {
      for (const taskId of taskIds) {
        const executions = await getExecutions(request, taskId)
        const execution = executions.find((candidate) => {
          const config = executionConfig(candidate)
          return (
            candidate.role === 'coder' &&
            candidate.agent_id === coderId &&
            typeof config?.resume_thread_id === 'string' &&
            candidate.summary?.includes('merge-conflict re-review')
          )
        })
        if (execution) return { taskId, execution }
      }
      return null
    },
    { timeoutMs, intervalMs: 5000 },
  )
}

async function waitForCodexUserMessage(
  request: APIRequestContext,
  executionId: string,
  predicate: (message: string) => boolean,
  timeoutMs: number,
): Promise<string> {
  return poll(
    `Codex user message in execution ${executionId}`,
    async () => {
      const logs = await getExecutionLogs(request, executionId)
      const messages = codexUserMessages(logs)
      return messages.find(predicate) ?? null
    },
    { timeoutMs, intervalMs: 2000 },
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

async function waitForExecutionCreated(
  request: APIRequestContext,
  taskId: string,
  role: 'coder' | 'reviewer',
  agentId: string,
  timeoutMs: number,
): Promise<ExecutionResponse> {
  return poll(
    `${role} execution for task ${taskId} to be created`,
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
          (execution) => execution.role === role && execution.agent_id === agentId,
        ) ?? null
      )
    },
    { timeoutMs, intervalMs: 1000 },
  )
}

async function expectExactlyOneExecution(
  request: APIRequestContext,
  taskId: string,
  role: 'coder' | 'reviewer',
  agentId: string,
): Promise<ExecutionResponse> {
  const executions = await getExecutions(request, taskId)
  const matchingExecutions = executions.filter(
    (execution) => execution.role === role && execution.agent_id === agentId,
  )

  expect(
    matchingExecutions,
    `${role} should dispatch exactly once for task ${taskId}; saw ${matchingExecutions
      .map((execution) => `${execution.id}:${execution.status}`)
      .join(', ')}`,
  ).toHaveLength(1)

  return matchingExecutions[0]
}

async function waitForCiFollowUpExecution(
  request: APIRequestContext,
  taskId: string,
  coderId: string,
  originalExecutionId: string,
  timeoutMs: number,
): Promise<ExecutionResponse> {
  return poll(
    `CI follow-up coder execution for task ${taskId}`,
    async () => {
      const executions = await getExecutions(request, taskId)
      const execution = executions.find((candidate) => {
        const config = executionConfig(candidate)
        return (
          candidate.role === 'coder' &&
          candidate.agent_id === coderId &&
          candidate.parent_execution_id === originalExecutionId &&
          typeof config?.resume_thread_id === 'string' &&
          config?.resume_thread_in_place === true
        )
      })
      return execution ?? null
    },
    { timeoutMs, intervalMs: 2000 },
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
    await request.get(`/api/v1/tasks/${taskId}/actions`).then(async (response) => {
      const current = await response.json()
      const offer = current.available_actions?.find((item: { action: { verb: string } }) => item.action.verb === 'cancel')
      if (offer) await request.post(`/api/v1/tasks/${taskId}/actions`, { data: { action: offer.action, version: current.version }, failOnStatusCode: false })
    })
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
  const root = await mkdtemp(join(tmpdir(), `forge-vite-react-${runId}-`))
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

test.describe('project, agents, and task movement (integration)', () => {
  test('creates a Vite React project, assigns Codex and Claude defaults, and moves two tasks', async ({
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
        name: `Vite React Project ${runId}`,
        settings: { lifecycle_hooks: fixtureLifecycleHooks },
        default_review_config: fixtureReviewConfig,
      })
      await api<unknown>(request, 'PUT', `/api/v1/projects/${project.id}/workflow`, {
        template_name: 'no-user-approval',
      })

      const repo = await api<RepoResponse>(
        request,
        'POST',
        `/api/v1/projects/${project.id}/repos`,
        {
          name: `vite-react-app-${runId}`,
          remote_url: fixturePath,
          local_path: fixturePath,
          default_branch: 'main',
        },
      )

      const coder = await api<AgentResponse>(request, 'POST', '/api/v1/agents', {
        name: `Codex Coder ${runId}`,
        executor_type: 'codex',
        model: 'gpt-5.3-codex',
        permission_policy: 'supervised',
        max_concurrent_tasks: 2,
      })
      createdAgentIds.push(coder.id)

      const reviewer = await api<AgentResponse>(request, 'POST', '/api/v1/agents', {
        name: `Claude Reviewer ${runId}`,
        executor_type: 'claude_code',
        // model: 'claude-sonnet-4-6',
        model: 'claude-haiku-4-5',
        permission_policy: 'supervised',
        max_concurrent_tasks: 2,
      })
      createdAgentIds.push(reviewer.id)

      const updatedProject = await api<ProjectResponse>(
        request,
        'PATCH',
        `/api/v1/projects/${project.id}`,
        {
          name: project.name,
          settings: {
            default_role_assignments: [
              { role_name: 'coder', assignee_type: 'agent', assignee_id: coder.id },
              { role_name: 'reviewer', assignee_type: 'agent', assignee_id: reviewer.id },
            ],
            lifecycle_hooks: fixtureLifecycleHooks,
          },
          default_review_config: fixtureReviewConfig,
        },
      )

      expect(updatedProject.settings.default_role_assignments).toEqual([
        { role_name: 'coder', assignee_type: 'agent', assignee_id: coder.id },
        { role_name: 'reviewer', assignee_type: 'agent', assignee_id: reviewer.id },
      ])

      const firstTask = await api<TaskResponse>(
        request,
        'POST',
        `/api/v1/projects/${project.id}/tasks`,
        {
          title: `Create Vite React app shell ${runId}`,
          description:
            'Edit src/App.tsx. Replace "Vite React fixture" with "Forge integration task one". Keep the change minimal and do not run tests.',
          task_type: 'task',
          priority: 0,
        },
      )
      createdTaskIds.push(firstTask.id)
      const secondTask = await api<TaskResponse>(
        request,
        'POST',
        `/api/v1/projects/${project.id}/tasks`,
        {
          title: `Review Vite React app shell ${runId}`,
          description:
            'Edit src/App.tsx. Replace "Vite React fixture" with "Forge integration task two". Keep the change minimal and do not run tests.',
          task_type: 'task',
          priority: 0,
        },
      )
      createdTaskIds.push(secondTask.id)

      for (const task of [firstTask, secondTask]) {
        expect(task.role_assignments).toEqual(
          expect.arrayContaining([
            expect.objectContaining({
              role_name: 'coder',
              assignee_type: 'agent',
              assignee_id: coder.id,
            }),
            expect.objectContaining({
              role_name: 'reviewer',
              assignee_type: 'agent',
              assignee_id: reviewer.id,
            }),
          ]),
        )
      }

      await Promise.all([
        waitForTaskStatus(
          request,
          firstTask.id,
          ['in_progress', 'review', 'merging', 'done'],
          120000,
        ),
        waitForTaskStatus(
          request,
          secondTask.id,
          ['in_progress', 'review', 'merging', 'done'],
          120000,
        ),
      ])

      const [firstWorkspace, secondWorkspace] = await Promise.all([
        waitForWorkspace(request, firstTask.id, 120000),
        waitForWorkspace(request, secondTask.id, 120000),
      ])
      expect(firstWorkspace).toMatchObject({
        task_id: firstTask.id,
        repo_id: repo.id,
        branch: expect.stringMatching(/^task\//),
      })
      expect(secondWorkspace).toMatchObject({
        task_id: secondTask.id,
        repo_id: repo.id,
        branch: expect.stringMatching(/^task\//),
      })

      const [firstCoderExecution, secondCoderExecution] = await Promise.all([
        waitForCompletedExecution(request, firstTask.id, 'coder', coder.id, 15 * 60 * 1000),
        waitForCompletedExecution(request, secondTask.id, 'coder', coder.id, 15 * 60 * 1000),
      ])
      expect(firstCoderExecution.workspace_id).toBe(firstWorkspace.id)
      expect(secondCoderExecution.workspace_id).toBe(secondWorkspace.id)

      await Promise.all([
        waitForTaskStatus(request, firstTask.id, ['review', 'merging', 'done'], 120000),
        waitForTaskStatus(request, secondTask.id, ['review', 'merging', 'done'], 120000),
      ])

      const [firstReviewerExecution, secondReviewerExecution] = await Promise.all([
        waitForCompletedExecution(request, firstTask.id, 'reviewer', reviewer.id, 15 * 60 * 1000),
        waitForCompletedExecution(request, secondTask.id, 'reviewer', reviewer.id, 15 * 60 * 1000),
      ])
      expect(firstReviewerExecution.workspace_id).toBe(firstWorkspace.id)
      expect(secondReviewerExecution.workspace_id).toBe(secondWorkspace.id)

      await Promise.all([
        expectExactlyOneExecution(request, firstTask.id, 'reviewer', reviewer.id),
        expectExactlyOneExecution(request, secondTask.id, 'reviewer', reviewer.id),
      ])

      const mergeFollowUp = await waitForMergeFollowUpExecution(
        request,
        [firstTask.id, secondTask.id],
        coder.id,
        15 * 60 * 1000,
      )
      const followUpConfig = executionConfig(mergeFollowUp.execution)
      expect(followUpConfig?.resume_thread_in_place).toBe(true)
      expect(followUpConfig?.resume_fallback_prompt).toBeUndefined()
      expect(typeof followUpConfig?.resume_thread_id).toBe('string')
      expect(mergeFollowUp.execution.parent_execution_id).not.toBeNull()

      const followUpUserMessage = await waitForCodexUserMessage(
        request,
        mergeFollowUp.execution.id,
        (message) =>
          message.includes('merge-conflict re-review') ||
          message.includes('Merge conflict encountered on prior attempt'),
        120000,
      )
      expect(followUpUserMessage).toContain('merge-conflict re-review')
      expect(followUpUserMessage).not.toContain('Implementation objective:')
      expect(followUpUserMessage).not.toContain(firstTask.title)
      expect(followUpUserMessage).not.toContain(secondTask.title)
      expect(followUpUserMessage).not.toContain('Forge integration task one')
      expect(followUpUserMessage).not.toContain('Forge integration task two')

      const followUpLogs = await getExecutionLogs(request, mergeFollowUp.execution.id)
      expect(JSON.stringify(followUpLogs)).not.toContain('thread not found')

      const tasks = await api<PaginatedResponse<TaskResponse>>(
        request,
        'GET',
        `/api/v1/projects/${project.id}/tasks`,
      )
      const taskStatuses = new Map(tasks.items.map((task) => [task.id, task.status]))
      expect(ACTIVE_OR_COMPLETED_TASK_STATUSES).toContain(taskStatuses.get(firstTask.id))
      expect(ACTIVE_OR_COMPLETED_TASK_STATUSES).toContain(taskStatuses.get(secondTask.id))

      await page.goto(`/projects/${project.id}/board`)
      await page.waitForLoadState('domcontentloaded')

      await expect(page.getByText(firstTask.title)).toBeVisible({ timeout: 15000 })
      await expect(page.getByText(secondTask.title)).toBeVisible()
      await expect(page.getByText(coder.name).first()).toBeVisible()
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

  test('sends CI failure back to Codex and passes after the follow-up creates the missing file', async ({
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
        name: `CI Follow-up Project ${runId}`,
        default_review_config: { ci_steps: ['test -f ci-marker.txt'], review_prompt: null },
      })
      await api<unknown>(request, 'PUT', `/api/v1/projects/${project.id}/workflow`, {
        template_name: 'no-user-approval',
      })

      const repo = await api<RepoResponse>(
        request,
        'POST',
        `/api/v1/projects/${project.id}/repos`,
        {
          name: `ci-follow-up-app-${runId}`,
          remote_url: fixturePath,
          local_path: fixturePath,
          default_branch: 'main',
        },
      )
      expect(repo.local_path).not.toBeNull()
      await expect(realpath(repo.local_path ?? '')).resolves.toBe(await realpath(fixturePath))

      const coder = await api<AgentResponse>(request, 'POST', '/api/v1/agents', {
        name: `CI Follow-up Codex ${runId}`,
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
        },
        default_review_config: { ci_steps: ['test -f ci-marker.txt'], review_prompt: null },
      })

      const task = await api<TaskResponse>(
        request,
        'POST',
        `/api/v1/projects/${project.id}/tasks`,
        {
          title: `Fix CI marker after follow-up ${runId}`,
          description:
            'Edit src/App.tsx to say "Forge CI follow-up initial pass". On the initial implementation, do not create ci-marker.txt. If Forge later reports that CI failed because ci-marker.txt is missing, create ci-marker.txt with the text "fixed by follow-up" and commit it.',
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

      await waitForTaskStatus(request, task.id, ['in_progress', 'review'], 120000)
      const initialCoderExecution = await waitForExecutionCreated(
        request,
        task.id,
        'coder',
        coder.id,
        120000,
      )

      const ciFollowUpExecution = await waitForCiFollowUpExecution(
        request,
        task.id,
        coder.id,
        initialCoderExecution.id,
        15 * 60 * 1000,
      )
      const followUpConfig = executionConfig(ciFollowUpExecution)
      expect(followUpConfig?.resume_thread_in_place).toBe(true)
      expect(followUpConfig?.resume_fallback_prompt).toBeUndefined()
      expect(typeof followUpConfig?.resume_thread_id).toBe('string')
      expect(ciFollowUpExecution.parent_execution_id).toBe(initialCoderExecution.id)

      const ciFollowUpMessage = await waitForCodexUserMessage(
        request,
        ciFollowUpExecution.id,
        (message) =>
          message.includes('CI failed during review') &&
          message.includes('test -f ci-marker.txt'),
        120000,
      )
      expect(ciFollowUpMessage).toContain('Fix only the failing check below')
      expect(ciFollowUpMessage).toContain(`Previous coder execution:\n${initialCoderExecution.id}`)
      expect(ciFollowUpMessage).not.toContain('Implementation objective:')

      const doneTask = await waitForTaskStatus(request, task.id, ['done'], 15 * 60 * 1000)
      expect(doneTask.status).toBe('done')

      await expect
        .poll(
          async () => {
            const marker = await readFile(join(fixturePath, 'ci-marker.txt'), 'utf8').catch(
              () => '',
            )
            return marker.trim()
          },
          { timeout: 120000, intervals: [2000] },
        )
        .toBe('fixed by follow-up')

      await page.goto(`/tasks/${task.id}/executions/${ciFollowUpExecution.id}`)
      await page.waitForLoadState('domcontentloaded')
      await expect(page.getByText('CI failed during review')).toBeVisible({
        timeout: 15000,
      })
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
