import { mkdtemp, readFile, rm, writeFile, mkdir } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { expect, test, type APIRequestContext, type APIResponse } from './fixtures'

const execFileAsync = promisify(execFile)

type PaginatedResponse<T> = {
  items: T[]
  has_more: boolean
  next_cursor?: string | null
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
  description: string | null
  status: string
  awaiting_human: boolean
  blocked?: unknown | null
  version: number
  role_assignments: Array<{
    role_name: string
    assignee_type: string | null
    assignee_id: string | null
  }>
  plan_progress?: { total: number; completed: number; remaining: number; available: boolean } | null
  plan_artifact?: {
    source_path: string | null
    items: Array<{ label: string; checked: boolean }>
  } | null
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
  summary: string | null
  error: string | null
  workspace_id: string | null
}

type LaunchExecutionResponse = {
  data: {
    task: TaskResponse
    execution: ExecutionResponse
    workspace: WorkspaceResponse
  }
}

type WorkflowDefinition = {
  roles: Array<{ name: string; display_name: string; description: string }>
  states: Array<{
    name: string
    kind: string
    column: string
    display_name: string
    role: string | null
    hooks: Record<string, unknown[]>
    gate_config: {
      reject_target: string | null
      max_rejections: number | null
      approve_label: string | null
      reject_label: string | null
      requires_user_approval: boolean
    } | null
    config: Record<string, unknown>
  }>
  cancellation_state: string | null
}

const runId = `planner-flow-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
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
  role: 'planner' | 'coder' | 'reviewer' | 'interactive',
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

async function waitForPlanArtifactContaining(
  request: APIRequestContext,
  taskId: string,
  expectedLabel: string,
  timeoutMs: number,
): Promise<TaskResponse> {
  return poll(
    `plan artifact for task ${taskId} to include ${expectedLabel}`,
    async () => {
      const task = await getTask(request, taskId)
      const items = task.plan_artifact?.items ?? []
      const matchingItem = items.find((item) => item.label.includes(expectedLabel))
      return task.status === 'planning' &&
        task.awaiting_human &&
        task.blocked == null &&
        matchingItem?.checked === true &&
        task.plan_progress?.remaining === 0
        ? task
        : null
    },
    { timeoutMs, intervalMs: 2000 },
  )
}

async function waitForPlanArtifact(
  request: APIRequestContext,
  taskId: string,
  timeoutMs: number,
): Promise<TaskResponse> {
  return poll(
    `plan artifact for task ${taskId}`,
    async () => {
      const task = await getTask(request, taskId)
      const progress = task.plan_progress
      const items = task.plan_artifact?.items ?? []
      return task.status === 'planning' &&
        task.blocked == null &&
        items.length > 0 &&
        progress?.remaining === 0 &&
        task.awaiting_human
        ? task
        : null
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

function requirePlanningApproval(workflow: WorkflowDefinition): WorkflowDefinition {
  return {
    ...workflow,
    states: workflow.states.map((state) => {
      if (state.name !== 'planning') return state
      return {
        ...state,
        gate_config: {
          reject_target: 'planning',
          max_rejections: 2,
          approve_label: 'Approve plan',
          reject_label: null,
          requires_user_approval: true,
          ...state.gate_config,
          requires_user_approval: true,
          approve_label: state.gate_config?.approve_label ?? 'Approve plan',
        },
      }
    }),
  }
}

test.describe('planner-approved Vite React task flow (integration)', () => {
  test('plans, waits for user approval, codes, reviews, and merges', async ({ page, request }) => {
    test.setTimeout(45 * 60 * 1000)
    const createdAgentIds: string[] = []
    const createdTaskIds: string[] = []
    const fixturePath = await createViteReactFixture()
    const expectedText = `Forge planner approved flow ${runId}`

    try {
      const projectsResponse = await request.get('/api/v1/projects', { failOnStatusCode: false })
      await expectOk(
        projectsResponse,
        'GET /api/v1/projects. Start the Forge API server on localhost:8080 before running this integration test',
      )

      const planner = await api<AgentResponse>(request, 'POST', '/api/v1/agents', {
        name: `Codex Planner ${runId}`,
        executor_type: 'codex',
        model: 'gpt-5.3-codex',
        permission_policy: 'supervised',
        max_concurrent_tasks: 1,
      })
      createdAgentIds.push(planner.id)

      const coder = await api<AgentResponse>(request, 'POST', '/api/v1/agents', {
        name: `Codex Coder ${runId}`,
        executor_type: 'codex',
        model: 'gpt-5.3-codex',
        permission_policy: 'supervised',
        max_concurrent_tasks: 1,
      })
      createdAgentIds.push(coder.id)

      const reviewer = await api<AgentResponse>(request, 'POST', '/api/v1/agents', {
        name: `Claude Haiku Auditor ${runId}`,
        executor_type: 'claude_code',
        model: 'claude-haiku-4-5',
        permission_policy: 'supervised',
        max_concurrent_tasks: 1,
      })
      createdAgentIds.push(reviewer.id)

      const defaultRoleAssignments = [
        { role_name: 'planner', assignee_type: 'agent', assignee_id: planner.id },
        { role_name: 'coder', assignee_type: 'agent', assignee_id: coder.id },
        { role_name: 'reviewer', assignee_type: 'agent', assignee_id: reviewer.id },
      ]

      const project = await api<ProjectResponse>(request, 'POST', '/api/v1/projects', {
        name: `Planner Approved Vite React ${runId}`,
        paused: true,
        settings: {
          default_role_assignments: defaultRoleAssignments,
          lifecycle_hooks: fixtureLifecycleHooks,
          retry_budgets: { review: 3, merge_fix: 1, plan_checklist_reminders: 3 },
        },
        default_review_config: fixtureReviewConfig,
      })

      const workflow = await api<WorkflowDefinition>(
        request,
        'GET',
        `/api/v1/projects/${project.id}/workflow`,
      )
      await api<WorkflowDefinition>(request, 'PUT', `/api/v1/projects/${project.id}/workflow`, {
        definition: requirePlanningApproval(workflow),
      })

      const repo = await api<RepoResponse>(request, 'POST', `/api/v1/projects/${project.id}/repos`, {
        name: `vite-react-planner-app-${runId}`,
        remote_url: fixturePath,
        local_path: fixturePath,
        default_branch: 'main',
      })

      const task = await api<TaskResponse>(request, 'POST', `/api/v1/projects/${project.id}/tasks`, {
        title: `Plan and update Vite React shell ${runId}`,
        description: [
          'Planner instructions:',
          '- Inspect the Vite React fixture and create `../plan.md` as a Markdown checklist outside the git repository.',
          '- The plan must include steps to update `src/App.tsx` and verify with `npm run build`.',
          '- Do not edit application source during planning.',
          '- Check every item in `../plan.md` before finishing planning.',
          '',
          'Coder instructions after the plan is approved (implementation phase only):',
          '- Do not create or update `../plan.md` in the coder phase.',
          '- Do not answer with another plan.',
          `- Edit src/App.tsx so the page renders exactly: ${expectedText}`,
          '- Keep the change minimal.',
          '- Run `npm run build` before finishing.',
        ].join('\n'),
        task_type: 'task',
        priority: 0,
      })
      createdTaskIds.push(task.id)

      expect(task.role_assignments).toEqual(
        expect.arrayContaining([
          expect.objectContaining({
            role_name: 'planner',
            assignee_type: 'agent',
            assignee_id: planner.id,
          }),
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

      await api<ProjectResponse>(request, 'PATCH', `/api/v1/projects/${project.id}`, {
        name: project.name,
        settings: {
          default_role_assignments: defaultRoleAssignments,
          lifecycle_hooks: fixtureLifecycleHooks,
          retry_budgets: { review: 3, merge_fix: 1, plan_checklist_reminders: 3 },
        },
        default_review_config: fixtureReviewConfig,
        paused: false,
      })

      const planningTask = await waitForPlanArtifact(request, task.id, 20 * 60 * 1000)
      expect(planningTask.status).toBe('planning')
      expect(planningTask.awaiting_human).toBe(true)
      expect(planningTask.plan_progress?.total ?? 0).toBeGreaterThan(0)
      expect(planningTask.plan_progress?.remaining).toBe(0)
      expect(planningTask.plan_artifact?.items.length ?? 0).toBeGreaterThan(0)
      expect(planningTask.plan_artifact?.source_path).toMatch(/\/plan\.md$/)
      expect(planningTask.plan_artifact?.source_path).not.toContain('/.forge/')

      const workspace = await waitForWorkspace(request, task.id, 120000)
      expect(workspace).toMatchObject({
        task_id: task.id,
        repo_id: repo.id,
        branch: expect.stringMatching(/^task\//),
      })

      const plannerExecution = await waitForCompletedExecution(
        request,
        task.id,
        'planner',
        planner.id,
        20 * 60 * 1000,
      )
      expect(plannerExecution.workspace_id).toBe(workspace.id)
      expect(planningTask.blocked ?? null).toBeNull()

      const followUpPlanItem = 'Confirm reviewer handoff stays unblocked before approval'
      const plannerFollowUp = await api<LaunchExecutionResponse>(
        request,
        'POST',
        `/api/v1/executions/${plannerExecution.id}/follow-up`,
        {
          message: [
            'Before the plan is approved, update only `../plan.md`.',
            `Add this exact checked checklist item: - [x] ${followUpPlanItem}`,
            'Do not edit application source, package files, or tests.',
            'Finish after the plan file contains the new checked item.',
          ].join('\n'),
          agent_id: planner.id,
        },
      )
      expect(plannerFollowUp.data.task.id).toBe(task.id)
      expect(plannerFollowUp.data.execution.role).toBe('interactive')
      expect(plannerFollowUp.data.workspace.id).toBe(workspace.id)

      const plannerFollowUpExecution = await waitForCompletedExecution(
        request,
        task.id,
        'interactive',
        planner.id,
        15 * 60 * 1000,
      )
      expect(plannerFollowUpExecution.workspace_id).toBe(workspace.id)

      const revisedPlanningTask = await waitForPlanArtifactContaining(
        request,
        task.id,
        followUpPlanItem,
        5 * 60 * 1000,
      )
      expect(revisedPlanningTask.status).toBe('planning')
      expect(revisedPlanningTask.awaiting_human).toBe(true)
      expect(revisedPlanningTask.blocked ?? null).toBeNull()

      await page.goto(`/projects/${project.id}/board?task=${task.id}`)
      await page.waitForLoadState('domcontentloaded')
      const detailDialog = page.getByRole('dialog', { name: task.title })
      await expect(detailDialog.getByText(/Plan/i).first()).toBeVisible({ timeout: 15000 })
      await expect(detailDialog.getByRole('button', { name: 'Approve plan' })).toBeVisible({
        timeout: 15000,
      })
      await detailDialog.getByRole('button', { name: 'Approve plan' }).click()

      await waitForTaskStatus(request, task.id, ['in_progress', 'review', 'merging', 'done'], 120000)
      const coderExecution = await waitForCompletedExecution(
        request,
        task.id,
        'coder',
        coder.id,
        20 * 60 * 1000,
      )
      expect(coderExecution.workspace_id).toBe(workspace.id)

      await waitForTaskStatus(request, task.id, ['review', 'merging', 'done'], 120000)
      const reviewerExecution = await waitForCompletedExecution(
        request,
        task.id,
        'reviewer',
        reviewer.id,
        20 * 60 * 1000,
      )
      expect(reviewerExecution.workspace_id).toBe(workspace.id)

      const doneTask = await waitForTaskStatus(request, task.id, ['done'], 5 * 60 * 1000)
      expect(doneTask.status).toBe('done')

      const appSource = await readFile(join(fixturePath, 'src/App.tsx'), 'utf8')
      expect(appSource).toContain(expectedText)

      await page.goto(`/projects/${project.id}/board`)
      await page.waitForLoadState('domcontentloaded')
      await expect(page.getByText(task.title)).toBeVisible({ timeout: 15000 })
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
})
