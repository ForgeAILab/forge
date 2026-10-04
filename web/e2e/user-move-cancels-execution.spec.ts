import { mkdtemp, rm } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { expect, test, type APIRequestContext, type APIResponse, type Page } from './fixtures'

const execFileAsync = promisify(execFile)

type Id = string

type TaskResponse = {
  id: Id
  project_id: Id
  parent_task_id: Id | null
  title: string
  status: string
  version: number
}

type ExecutionResponse = {
  id: Id
  status: 'running' | 'completed' | 'failed' | 'cancelled'
  role: string
}

type PaginatedResponse<T> = {
  items: T[]
  has_more: boolean
}

async function git(repoPath: string, ...args: string[]) {
  await execFileAsync('git', ['-C', repoPath, ...args])
}

async function expectOk(response: APIResponse, label: string) {
  if (response.ok()) return
  throw new Error(`${label} failed with ${response.status()}: ${await response.text()}`)
}

async function expectBackend(request: APIRequestContext) {
  const projectsResponse = await request.get('/api/v1/projects', { failOnStatusCode: false })
  expect(
    projectsResponse.ok(),
    'GET /api/v1/projects. Start the Forge API server on localhost:8080 before running this integration test',
  ).toBeTruthy()
}

async function getTask(request: APIRequestContext, taskId: Id): Promise<TaskResponse> {
  const response = await request.get(`/api/v1/tasks/${taskId}`)
  await expectOk(response, `GET /api/v1/tasks/${taskId}`)
  return (await response.json()) as TaskResponse
}

async function getExecutions(
  request: APIRequestContext,
  taskId: Id,
): Promise<ExecutionResponse[]> {
  const response = await request.get(`/api/v1/tasks/${taskId}/executions`)
  await expectOk(response, `GET /api/v1/tasks/${taskId}/executions`)
  const body = (await response.json()) as PaginatedResponse<ExecutionResponse>
  return body.items
}

async function waitForNoRunningExecutions(
  request: APIRequestContext,
  taskId: Id,
  timeoutMs: number,
): Promise<void> {
  const startedAt = Date.now()
  while (Date.now() - startedAt < timeoutMs) {
    const running = (await getExecutions(request, taskId)).some(
      (execution) => execution.status === 'running',
    )
    if (!running) return
    await new Promise((resolve) => setTimeout(resolve, 1000))
  }
  throw new Error(`Timed out waiting for executions on task ${taskId} to stop running`)
}

async function createRepo(request: APIRequestContext, projectId: Id): Promise<string> {
  const repoDir = await mkdtemp(join(tmpdir(), 'forge-e2e-user-move-'))
  await git(repoDir, 'init', '-b', 'main')
  await git(repoDir, 'config', 'user.email', 'e2e@forge.test')
  await git(repoDir, 'config', 'user.name', 'e2e')
  await execFileAsync('sh', ['-c', `echo "# e2e" > "${repoDir}/README.md"`])
  await git(repoDir, 'add', '.')
  await git(repoDir, 'commit', '-m', 'init')

  const repo = await request.post(`/api/v1/projects/${projectId}/repos`, {
    data: {
      name: 'e2e-repo',
      local_path: repoDir,
      remote_url: repoDir,
      default_branch: 'main',
    },
  })
  await expectOk(repo, `POST /api/v1/projects/${projectId}/repos`)
  return repoDir
}

async function setupUserAssignedSubtaskInReview(
  request: APIRequestContext,
  userId: string,
): Promise<{ projectId: Id; subtaskId: Id; subtaskTitle: string }> {
  const project = await request.post('/api/v1/projects', {
    data: { name: `User Move E2E ${Date.now()}` },
  })
  await expectOk(project, 'POST /api/v1/projects')
  const projectId = (await project.json()).id as Id
  await createRepo(request, projectId)

  const root = await request.post(`/api/v1/projects/${projectId}/tasks`, {
    data: { title: 'Root feature' },
  })
  await expectOk(root, 'POST root task')
  const rootId = (await root.json()).id as Id

  const assignCoder = await request.put(`/api/v1/tasks/${rootId}/roles/coder`, {
    data: { assignee_type: 'user', assignee_id: userId },
  })
  await expectOk(assignCoder, `PUT /api/v1/tasks/${rootId}/roles/coder`)

  const subtaskTitle = `Subtask user move ${Date.now()}`
  const subtask = await request.post(`/api/v1/projects/${projectId}/tasks`, {
    data: { title: subtaskTitle, parent_task_id: rootId },
  })
  await expectOk(subtask, 'POST subtask')
  const subtaskJson = (await subtask.json()) as TaskResponse

  const toInProgress = await request.post(`/api/v1/tasks/${subtaskJson.id}/transition`, {
    data: { status: 'in_progress', version: subtaskJson.version },
  })
  await expectOk(toInProgress, 'todo->in_progress')
  const inProgress = (await toInProgress.json()).task as TaskResponse

  const toReview = await request.post(`/api/v1/tasks/${subtaskJson.id}/transition`, {
    data: { status: 'review', version: inProgress.version },
  })
  await expectOk(toReview, 'in_progress->review')

  return { projectId, subtaskId: subtaskJson.id, subtaskTitle }
}

async function dragTaskToColumn(
  page: Page,
  taskId: Id,
  taskTitle: string,
  targetColumnLabel: string,
  sourceColumnLabel = 'In Progress',
) {
  const taskCard = page.locator(`[data-rfd-draggable-id="${taskId}"]`)
  const column = page.getByRole('region', { name: `${targetColumnLabel} column` })
  await expect(
    page.getByRole('region', { name: `${sourceColumnLabel} column` }).getByText(taskTitle),
  ).toBeVisible({ timeout: 15000 })
  await expect(taskCard).toBeVisible({ timeout: 15000 })
  await taskCard.scrollIntoViewIfNeeded()
  await column.scrollIntoViewIfNeeded()

  const dragHandle = page.getByRole('button', { name: `Move ${taskTitle}`, exact: true })
  const taskBox = await dragHandle.boundingBox()
  const columnBox = await column.boundingBox()
  if (!taskBox || !columnBox) {
    throw new Error(`Missing drag bounds for ${taskTitle} -> ${targetColumnLabel}`)
  }

  await page.mouse.move(taskBox.x + taskBox.width / 2, taskBox.y + taskBox.height / 2)
  await page.mouse.down()
  await page.mouse.move(columnBox.x + columnBox.width / 2, columnBox.y + 100, { steps: 30 })
  await page.waitForTimeout(150)
  await page.mouse.up()
}

async function assertNoErrorToast(page: Page) {
  await expect(page.locator('[data-sonner-toast][data-type="error"]')).toHaveCount(0, {
    timeout: 5000,
  })
}

test('dragging a task with a running execution cancels it without error toast', async ({
  page,
  request,
}) => {
  test.setTimeout(90_000)
  await expectBackend(request)

  const runId = Date.now()
  const taskTitle = `Running execution drag ${runId}`
  const createdAgentIds: string[] = []
  const createdTaskIds: string[] = []
  let repoDir: string | undefined

  try {
    const project = await request.post('/api/v1/projects', {
      data: {
        name: `User move cancel exec ${runId}`,
        default_review_config: { ci_steps: [], review_prompt: null },
      },
    })
    await expectOk(project, 'POST /api/v1/projects')
    const projectId = (await project.json()).id as Id
    repoDir = await createRepo(request, projectId)

    await request.put(`/api/v1/projects/${projectId}/workflow`, {
      data: { template_name: 'no-user-approval' },
    })

    const coder = await request.post('/api/v1/agents', {
      data: {
        name: `Shell Coder ${runId}`,
        executor_type: 'shell',
        max_concurrent_tasks: 1,
      },
    })
    await expectOk(coder, 'POST /api/v1/agents')
    const coderId = (await coder.json()).id as Id
    createdAgentIds.push(coderId)

    await request.patch(`/api/v1/projects/${projectId}`, {
      data: {
        name: `User move cancel exec ${runId}`,
        settings: {
          default_role_assignments: [
            { role_name: 'coder', assignee_type: 'agent', assignee_id: coderId },
          ],
        },
        default_review_config: { ci_steps: [], review_prompt: null },
      },
    })

    const task = await request.post(`/api/v1/projects/${projectId}/tasks`, {
      data: {
        title: taskTitle,
        description: 'sleep 30',
        task_type: 'task',
        priority: 0,
      },
    })
    await expectOk(task, 'POST task')
    const taskJson = (await task.json()) as TaskResponse
    createdTaskIds.push(taskJson.id)

    await page.goto(`/projects/${projectId}/board`)
    await expect(page.getByText(taskTitle).first()).toBeVisible({ timeout: 15000 })

    const freshTask = await getTask(request, taskJson.id)
    const toInProgress = await request.post(`/api/v1/tasks/${taskJson.id}/transition`, {
      data: { status: 'in_progress', version: freshTask.version },
    })
    await expectOk(toInProgress, 'todo->in_progress')

    let runningId = ''
    await expect
      .poll(
        async () => {
          const running = (await getExecutions(request, taskJson.id)).find(
            (execution) => execution.status === 'running' && execution.role === 'coder',
          )
          if (running) {
            runningId = running.id
            return true
          }
          return false
        },
        { timeout: 10000, intervals: [200, 500, 1000] },
      )
      .toBe(true)

    await dragTaskToColumn(page, taskJson.id, taskTitle, 'Review')

    await expect
      .poll(async () => (await getTask(request, taskJson.id)).status, {
        timeout: 10000,
        intervals: [200, 500, 1000],
      })
      .toBe('review')

    await expect
      .poll(
        async () => {
          const executions = await getExecutions(request, taskJson.id)
          if (executions.some((execution) => execution.status === 'running')) return null
          return executions.find((execution) => execution.id === runningId)?.status ?? null
        },
        { timeout: 20000, intervals: [200, 500, 1000] },
      )
      .toBe('cancelled')

    await page.goto(`/projects/${projectId}/board?task=${taskJson.id}`)
    await page.getByRole('button', { name: 'Executions' }).click()
    await expect(page.getByText('running', { exact: true })).toHaveCount(0, { timeout: 15000 })

    await assertNoErrorToast(page)
  } finally {
    for (const taskId of createdTaskIds.reverse()) {
      await request.get(`/api/v1/tasks/${taskId}/actions`).then(async (response) => {
      const current = await response.json()
      const offer = current.available_actions?.find((item: { action: { verb: string } }) => item.action.verb === 'cancel')
      if (offer) await request.post(`/api/v1/tasks/${taskId}/actions`, { data: { action: offer.action, version: current.version }, failOnStatusCode: false })
    })
      await waitForNoRunningExecutions(request, taskId, 30000).catch(() => undefined)
      await request.delete(`/api/v1/tasks/${taskId}`, { failOnStatusCode: false })
    }
    for (const agentId of createdAgentIds.reverse()) {
      await request.delete(`/api/v1/agents/${agentId}`, { failOnStatusCode: false })
    }
    if (repoDir) {
      await rm(repoDir, { recursive: true, force: true })
    }
  }
})

test('user-assigned task drag across a missing workflow edge succeeds via override', async ({
  page,
  request,
  e2eAuth,
}) => {
  test.setTimeout(60_000)
  await expectBackend(request)

  const { projectId, subtaskId, subtaskTitle } = await setupUserAssignedSubtaskInReview(
    request,
    e2eAuth.user.id,
  )

  await page.goto(`/projects/${projectId}/board`)
  await expect(
    page.getByRole('region', { name: 'Review column' }).getByText(subtaskTitle),
  ).toBeVisible({ timeout: 15000 })

  await dragTaskToColumn(page, subtaskId, subtaskTitle, 'Done', 'Review')

  await expect
    .poll(async () => (await getTask(request, subtaskId)).status, { timeout: 15000 })
    .toBe('done')

  await expect(
    page.getByRole('region', { name: 'Done column' }).getByText(subtaskTitle),
  ).toBeVisible({ timeout: 15000 })

  await assertNoErrorToast(page)
})
