import { writeFile } from 'node:fs/promises'
import type { Response as BrowserResponse } from '@playwright/test'
import { expect, test, type APIRequestContext, type APIResponse, type Page } from './fixtures'

type Project = { id: string; name: string }
type Task = {
  id: string
  project_id: string
  title: string
  description: string | null
  priority: number
  status: string
  version: number
}

type TasksPage = {
  items: Task[]
  board_revision: number
}

type PatchEvidence = {
  request: Record<string, unknown>
  status: number
  response: unknown
}

async function expectOk(response: APIResponse, label: string) {
  if (!response.ok()) throw new Error(`${label}: ${response.status()} ${await response.text()}`)
}

async function createTaskFixture(request: APIRequestContext) {
  const run = Date.now()
  const projectResponse = await request.post('/api/v1/projects', {
    data: { name: `Task detail edits ${run}` },
  })
  await expectOk(projectResponse, 'create project')
  const project = (await projectResponse.json()) as Project
  const title = `Set up CI pipeline ${run}`
  const description = 'Configure GitHub Actions for lint, test, and build.'
  const taskResponse = await request.post(`/api/v1/projects/${project.id}/tasks`, {
    data: { title, description, priority: 3 },
  })
  await expectOk(taskResponse, 'create task')
  return { project, task: (await taskResponse.json()) as Task, title, description }
}

async function readTask(request: APIRequestContext, taskId: string): Promise<Task> {
  const response = await request.get(`/api/v1/tasks/${taskId}`)
  await expectOk(response, 'read task')
  return (await response.json()) as Task
}

async function capturePatchResponses(
  page: Page,
  taskId: string,
  expectedCount: number,
  action: () => Promise<void>,
): Promise<PatchEvidence[]> {
  const captured: BrowserResponse[] = []
  const listener = (response: BrowserResponse) => {
    if (
      response.request().method() === 'PATCH' &&
      response.url().endsWith(`/api/v1/tasks/${taskId}`)
    ) {
      captured.push(response)
    }
  }
  page.on('response', listener)
  try {
    await action()
    await expect.poll(() => captured.length).toBe(expectedCount)
    return await Promise.all(
      captured.map(async (response) => ({
        request: response.request().postDataJSON() as Record<string, unknown>,
        status: response.status(),
        response: await response.json(),
      })),
    )
  } finally {
    page.off('response', listener)
  }
}

test('task detail saves title and empty description, then reconciles concurrent edits', async ({
  page,
  request,
}) => {
  test.setTimeout(120_000)
  await page.route('**/api/v1/events*', (route) => route.abort())
  const fixture = await createTaskFixture(request)
  const evidence: Record<string, unknown> = {}

  await page.goto(`/projects/${fixture.project.id}/board`)
  await page.getByRole('button', { name: `Open ${fixture.title}`, exact: true }).click()
  let dialog = page.getByRole('dialog')
  await expect(dialog).toBeVisible()

  const savedTitle = `${fixture.title} — edited`
  await dialog.getByRole('button', { name: fixture.title, exact: true }).click()
  await dialog.locator('header input').fill(savedTitle)
  const titleSave = await capturePatchResponses(page, fixture.task.id, 1, async () => {
    await dialog.getByRole('button', { name: 'Save', exact: true }).click()
  })
  expect(titleSave[0]).toMatchObject({
    request: { title: savedTitle, version: fixture.task.version },
    status: 200,
  })
  evidence.title_save = titleSave
  await expect(dialog.getByRole('button', { name: savedTitle, exact: true })).toBeVisible()
  await page.keyboard.press('Escape')
  await expect(page.getByRole('button', { name: `Open ${savedTitle}`, exact: true })).toBeVisible()

  await page.getByRole('button', { name: `Open ${savedTitle}`, exact: true }).click()
  dialog = page.getByRole('dialog')
  await dialog.getByRole('button', { name: fixture.description, exact: true }).click()
  await dialog.getByRole('textbox').fill('')
  const clearSave = await capturePatchResponses(page, fixture.task.id, 1, async () => {
    await dialog.getByRole('button', { name: 'Save', exact: true }).click()
  })
  expect(clearSave[0]).toMatchObject({ request: { description: '' }, status: 200 })
  expect((clearSave[0].response as Task).description).toBe('')
  evidence.description_clear = clearSave
  await expect(
    dialog.getByRole('button', { name: 'Click to add a description…', exact: true }),
  ).toBeVisible()

  await page.keyboard.press('Escape')
  await page.goto(`/projects/${fixture.project.id}/board`)
  await page.getByRole('button', { name: `Open ${savedTitle}`, exact: true }).click()
  dialog = page.getByRole('dialog')
  await dialog.getByRole('button', { name: savedTitle, exact: true }).click()
  const reconciledTitle = `${fixture.title} — reconciled`
  await dialog.locator('header input').fill(reconciledTitle)

  const beforeConcurrent = await readTask(request, fixture.task.id)
  const tasksResponse = await request.get(`/api/v1/projects/${fixture.project.id}/tasks?limit=200`)
  await expectOk(tasksResponse, 'read board before concurrent move')
  const taskPage = (await tasksResponse.json()) as TasksPage
  const boardTask = taskPage.items.find((candidate) => candidate.id === fixture.task.id)
  if (!boardTask) throw new Error('created task missing from board')
  const moveResponse = await request.post(`/api/v1/tasks/${fixture.task.id}/move`, {
    data: {
      operation_id: crypto.randomUUID(),
      task_version: boardTask.version,
      board_revision: taskPage.board_revision,
      target_status: boardTask.status === 'backlog' ? 'todo' : 'backlog',
      before_id: null,
      after_id: null,
    },
  })
  await expectOk(moveResponse, 'concurrent move')
  const movedTask = ((await moveResponse.json()) as { task: Task }).task
  expect(movedTask.version).toBeGreaterThan(beforeConcurrent.version)

  const concurrentDescription = 'Description written by the second client.'
  const concurrentUpdateResponse = await request.patch(`/api/v1/tasks/${fixture.task.id}`, {
    data: {
      description: concurrentDescription,
      priority: movedTask.priority + 1,
      version: movedTask.version,
    },
  })
  await expectOk(concurrentUpdateResponse, 'concurrent description and priority update')
  const concurrentTask = (await concurrentUpdateResponse.json()) as Task
  const concurrentCommentResponse = await request.post(
    `/api/v1/tasks/${fixture.task.id}/comments`,
    { data: { author_name: 'Second client', content: 'Concurrent comment.' } },
  )
  await expectOk(concurrentCommentResponse, 'concurrent comment')

  const reconciledSave = await capturePatchResponses(page, fixture.task.id, 2, async () => {
    await dialog.getByRole('button', { name: 'Save', exact: true }).click()
  })
  expect(reconciledSave.map((entry) => entry.status)).toEqual([409, 200])
  expect(reconciledSave[0].request).toMatchObject({ title: reconciledTitle })
  expect(reconciledSave[1].request).toMatchObject({
    title: reconciledTitle,
    version: concurrentTask.version,
  })
  evidence.non_overlapping_conflict = reconciledSave
  await expect(dialog.getByRole('button', { name: reconciledTitle, exact: true })).toBeVisible()

  await dialog.getByRole('button', { name: concurrentDescription, exact: true }).click()
  const descriptionEditor = dialog.getByRole('textbox')
  await descriptionEditor.fill('')
  const beforeDescriptionConflict = await readTask(request, fixture.task.id)
  const currentDescription = 'Description changed again by the second client.'
  const descriptionConflictResponse = await request.patch(`/api/v1/tasks/${fixture.task.id}`, {
    data: { description: currentDescription, version: beforeDescriptionConflict.version },
  })
  await expectOk(descriptionConflictResponse, 'concurrent same-field description update')

  const conflictingClear = await capturePatchResponses(page, fixture.task.id, 1, async () => {
    await dialog.getByRole('button', { name: 'Save', exact: true }).click()
  })
  expect(conflictingClear.map((entry) => entry.status)).toEqual([409])
  evidence.same_field_conflict = conflictingClear
  const conflictNotice = dialog.locator('[data-task-edit-conflict="description"]')
  await expect(conflictNotice).toContainText(currentDescription)
  await expect(descriptionEditor).toHaveText('')
  await expect(
    dialog.getByRole('button', { name: 'Click to add a description…', exact: true }),
  ).toHaveCount(0)

  const screenshotPrefix = process.env.FORGE_TASK_EDIT_SCREENSHOT_PREFIX
  if (screenshotPrefix) {
    for (const [name, width, height] of [
      ['mobile', 375, 900],
      ['tablet', 768, 1024],
      ['desktop', 1280, 900],
    ] as const) {
      await page.setViewportSize({ width, height })
      await expect(conflictNotice).toBeVisible()
      await page.screenshot({ path: `${screenshotPrefix}-${name}.png`, fullPage: true })
    }
  }

  const confirmedClear = await capturePatchResponses(page, fixture.task.id, 1, async () => {
    await dialog.getByRole('button', { name: 'Save', exact: true }).click()
  })
  expect(confirmedClear[0]).toMatchObject({ request: { description: '' }, status: 200 })
  evidence.confirmed_clear = confirmedClear
  await expect(
    dialog.getByRole('button', { name: 'Click to add a description…', exact: true }),
  ).toBeVisible()
  expect((await readTask(request, fixture.task.id)).description).toBe('')

  await page.keyboard.press('Escape')
  await expect(
    page.getByRole('button', { name: `Open ${reconciledTitle}`, exact: true }),
  ).toBeVisible()

  const evidencePath = process.env.FORGE_TASK_EDIT_EVIDENCE_PATH
  if (evidencePath) await writeFile(evidencePath, `${JSON.stringify(evidence, null, 2)}\n`)
})
