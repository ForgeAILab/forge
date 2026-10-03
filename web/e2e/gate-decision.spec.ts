import { mkdtemp } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { expect, test, type APIRequestContext, type Page } from './fixtures'

const execFileAsync = promisify(execFile)

type Id = string

type TaskResponse = {
  id: Id
  version: number
  status: string
  title: string
}

async function git(repoPath: string, ...args: string[]) {
  await execFileAsync('git', ['-C', repoPath, ...args])
}

async function expectBackend(request: APIRequestContext) {
  const projectsResponse = await request.get('/api/v1/projects', { failOnStatusCode: false })
  expect(
    projectsResponse.ok(),
    'GET /api/v1/projects. Start the Forge API server on localhost:8080 before running this integration test',
  ).toBeTruthy()
}

async function setupReviewGateTask(
  request: APIRequestContext,
  userId: string,
  title: string,
): Promise<{ projectId: Id; taskId: Id }> {
  const repoDir = await mkdtemp(join(tmpdir(), 'forge-e2e-gate-'))
  await git(repoDir, 'init', '-b', 'main')
  await git(repoDir, 'config', 'user.email', 'e2e@forge.test')
  await git(repoDir, 'config', 'user.name', 'e2e')
  await execFileAsync('sh', ['-c', `echo "# e2e" > "${repoDir}/README.md"`])
  await git(repoDir, 'add', '.')
  await git(repoDir, 'commit', '-m', 'init')

  const project = await request.post('/api/v1/projects', {
    data: {
      name: `Gate Decision E2E ${Date.now()}`,
      default_review_config: { ci_steps: [], review_prompt: null },
    },
  })
  expect(project.ok(), `create project: ${project.status()}`).toBeTruthy()
  const projectId = (await project.json()).id as Id

  await request.put(`/api/v1/projects/${projectId}/workflow`, {
    data: { template_name: 'user-approval-review' },
  })

  await request.post(`/api/v1/projects/${projectId}/repos`, {
    data: {
      name: 'e2e-repo',
      local_path: repoDir,
      remote_url: repoDir,
      default_branch: 'main',
    },
  })

  const task = await request.post(`/api/v1/projects/${projectId}/tasks`, {
    data: { title, description: 'Gate decision e2e task' },
  })
  expect(task.ok(), `create task: ${task.status()}`).toBeTruthy()
  let taskJson = (await task.json()) as TaskResponse

  const assignReviewer = await request.put(`/api/v1/tasks/${taskJson.id}/roles/reviewer`, {
    data: { assignee_type: 'user', assignee_id: userId },
  })
  expect(assignReviewer.ok(), `assign reviewer: ${assignReviewer.status()}`).toBeTruthy()

  const toInProgress = await request.post(`/api/v1/tasks/${taskJson.id}/transition`, {
    data: { status: 'in_progress', version: taskJson.version },
  })
  expect(
    toInProgress.ok(),
    `todo->in_progress should succeed: ${toInProgress.status()}`,
  ).toBeTruthy()
  taskJson = (await toInProgress.json()).task as TaskResponse

  const toReview = await request.post(`/api/v1/tasks/${taskJson.id}/transition`, {
    data: { status: 'review', version: taskJson.version },
  })
  expect(toReview.ok(), `in_progress->review should succeed: ${toReview.status()}`).toBeTruthy()
  const afterReview = (await toReview.json()).task as TaskResponse
  expect(afterReview.status).toBe('review')

  return { projectId, taskId: taskJson.id }
}

async function openBoardTask(page: Page, projectId: Id, taskId: Id, title: string) {
  await page.goto(`/projects/${projectId}/board?task=${taskId}`)
  await expect(page.getByText(title).first()).toBeVisible({ timeout: 15000 })
}

test('approve gate from UI advances task to accept target and updates the board', async ({
  page,
  request,
  e2eAuth,
}) => {
  await expectBackend(request)

  const title = `Gate approve ${Date.now()}`
  const { projectId, taskId } = await setupReviewGateTask(request, e2eAuth.user.id, title)

  await openBoardTask(page, projectId, taskId, title)

  await expect(page.getByRole('button', { name: 'Approve review' })).toBeVisible({
    timeout: 15000,
  })
  await page.getByRole('button', { name: 'Approve review' }).click()
  await page.getByRole('button', { name: 'Apply', exact: true }).click()

  await expect
    .poll(
      async () => {
        const res = await request.get(`/api/v1/tasks/${taskId}`)
        return res.ok() ? ((await res.json()) as TaskResponse).status : null
      },
      { timeout: 15000 },
    )
    .toBe('merging')

  await expect(page.getByRole('region', { name: 'Review column' }).getByText(title)).toBeVisible({
    timeout: 15000,
  })
})

test('reject gate from UI with reason records rejection in history', async ({
  page,
  request,
  e2eAuth,
}) => {
  test.setTimeout(60_000)
  await expectBackend(request)

  const title = `Gate reject ${Date.now()}`
  const rejectReason = 'e2e gate reject reason'
  const { projectId, taskId } = await setupReviewGateTask(request, e2eAuth.user.id, title)

  await openBoardTask(page, projectId, taskId, title)

  await page.getByRole('button', { name: 'Request changes' }).click()
  await expect(page.getByRole('heading', { name: 'Request changes' })).toBeVisible()
  await page.getByLabel('guidance (required)', { exact: true }).fill(rejectReason)
  await page.getByRole('button', { name: 'Apply', exact: true }).click()

  await expect
    .poll(
      async () => {
        const res = await request.get(`/api/v1/tasks/${taskId}`)
        return res.ok() ? ((await res.json()) as TaskResponse).status : null
      },
      { timeout: 15000 },
    )
    .toBe('in_progress')

  await page.getByRole('button', { name: 'History' }).click()
  await expect(page.getByText(rejectReason)).toBeVisible({ timeout: 15000 })
  await expect(page.getByText('rejection')).toBeVisible()
})
