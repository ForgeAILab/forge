import { execFile } from 'node:child_process'
import { mkdir, mkdtemp, rm, writeFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { promisify } from 'node:util'
import type { BrowserContext, Page } from '@playwright/test'
import { expect, test, type APIRequestContext } from './fixtures'

const execFileAsync = promisify(execFile)

type Task = {
  id: string
  title: string
  status: string
  board_position: number
  version: number
}

type TasksResponse = {
  items: Task[]
  next_cursor: string | null
  has_more: boolean
  board_revision: number
}

async function expectOk(
  response: { ok(): boolean; status(): number; text(): Promise<string> },
  label: string,
) {
  if (!response.ok()) {
    throw new Error(`${label}: ${response.status()} ${await response.text()}`)
  }
}

async function setupBoard(request: APIRequestContext, titles: string[]) {
  const repoDir = await mkdtemp(join(tmpdir(), 'forge-board-e2e-'))
  await execFileAsync('git', ['-C', repoDir, 'init', '-b', 'main'])
  await execFileAsync('git', ['-C', repoDir, 'config', 'user.email', 'board@forge.test'])
  await execFileAsync('git', ['-C', repoDir, 'config', 'user.name', 'Board E2E'])
  await writeFile(join(repoDir, 'README.md'), '# board e2e\n')
  await execFileAsync('git', ['-C', repoDir, 'add', '.'])
  await execFileAsync('git', ['-C', repoDir, 'commit', '-m', 'initial'])

  const projectResponse = await request.post('/api/v1/projects', {
    data: { name: `Board interactions ${Date.now()}` },
  })
  await expectOk(projectResponse, 'create project')
  const projectId = ((await projectResponse.json()) as { id: string }).id
  const repoResponse = await request.post(`/api/v1/projects/${projectId}/repos`, {
    data: {
      name: 'board-e2e',
      local_path: repoDir,
      remote_url: repoDir,
      default_branch: 'main',
    },
  })
  await expectOk(repoResponse, 'create repo')

  const tasks: Task[] = []
  for (const title of titles) {
    const response = await request.post(`/api/v1/projects/${projectId}/tasks`, {
      data: { title, description: `${title} description` },
    })
    await expectOk(response, `create ${title}`)
    tasks.push((await response.json()) as Task)
  }
  return { projectId, repoDir, tasks }
}

async function taskPage(request: APIRequestContext, projectId: string): Promise<TasksResponse> {
  const response = await request.get(`/api/v1/projects/${projectId}/tasks?limit=200`)
  await expectOk(response, 'list project tasks')
  return (await response.json()) as TasksResponse
}

async function beginHandleDrag(page: Page, title: string) {
  const handle = page.getByRole('button', { name: `Move ${title}`, exact: true })
  await expect(handle).toBeEnabled({ timeout: 15_000 })
  await handle.scrollIntoViewIfNeeded()
  const box = await handle.boundingBox()
  if (!box) throw new Error(`missing drag handle bounds for ${title}`)
  const x = box.x + box.width / 2
  const y = box.y + box.height / 2
  await page.mouse.move(x, y)
  await page.mouse.down()
  await page.mouse.move(x + 12, y + 4, { steps: 6 })
  await expect(page.locator('[data-board-phase="dragging"]')).toBeVisible()
}

async function dropIntoColumn(page: Page, label: string, yOffset = 110) {
  const column = page.getByRole('region', { name: `${label} column` })
  await column.scrollIntoViewIfNeeded()
  const box = await column.boundingBox()
  if (!box) throw new Error(`missing column bounds for ${label}`)
  await page.mouse.move(box.x + box.width / 2, box.y + yOffset, { steps: 24 })
  await page.mouse.up()
}

async function disposeBoard(context: BrowserContext | undefined, repoDir: string) {
  await context?.close()
  await rm(repoDir, { recursive: true, force: true })
}

test('two clients reconcile a stale held drag without moving any card', async ({
  page,
  request,
  browser,
  baseURL,
  e2eAuth,
}) => {
  test.setTimeout(90_000)
  const run = Date.now()
  const setup = await setupBoard(request, [
    `Stable first ${run}`,
    `Held drag ${run}`,
    `Stable last ${run}`,
  ])
  let clientB: BrowserContext | undefined
  try {
    const before = await taskPage(request, setup.projectId)
    const positions = new Map(before.items.map((task) => [task.id, task.board_position]))
    await page.goto(`/projects/${setup.projectId}/board`)
    await expect(page.getByText(setup.tasks[1].title).first()).toBeVisible({ timeout: 15_000 })

    await beginHandleDrag(page, setup.tasks[1].title)
    clientB = await browser.newContext({
      baseURL,
      extraHTTPHeaders: { authorization: `Bearer ${e2eAuth.apiToken}` },
    })
    const insertedTitle = `Inserted by client B ${run}`
    const insertedResponse = await clientB.request.post(
      `/api/v1/projects/${setup.projectId}/tasks`,
      { data: { title: insertedTitle, description: 'concurrent board insertion' } },
    )
    await expectOk(insertedResponse, 'client B inserts task')
    const inserted = (await insertedResponse.json()) as Task

    const moveResponsePromise = page.waitForResponse(
      (response) =>
        response.request().method() === 'POST' &&
        response.url().endsWith(`/tasks/${setup.tasks[1].id}/move`),
    )
    await dropIntoColumn(page, 'In Progress')
    const moveResponse = await moveResponsePromise
    expect(moveResponse.status()).toBe(409)
    await expect(page.locator('[data-board-announcement]')).toContainText(
      'Board changed while you were dragging',
    )
    await expect(page.getByText(insertedTitle).first()).toBeVisible({ timeout: 15_000 })

    const after = await taskPage(request, setup.projectId)
    for (const task of setup.tasks) {
      const current = after.items.find((candidate) => candidate.id === task.id)
      expect(current?.status).toBe('todo')
      expect(current?.board_position).toBe(positions.get(task.id))
    }
    expect(after.items.some((task) => task.id === inserted.id)).toBe(true)
  } finally {
    await disposeBoard(clientB, setup.repoDir)
  }
})

test('atomic moves allow one pending gesture, preserve card navigation, and replay', async ({
  page,
  request,
}) => {
  test.setTimeout(90_000)
  const run = Date.now()
  const setup = await setupBoard(request, [
    `Order first ${run}`,
    `Order second ${run}`,
    `Order third ${run}`,
  ])
  try {
    let releaseResponse: (() => void) | undefined
    const responseGate = new Promise<void>((resolve) => {
      releaseResponse = resolve
    })
    await page.route(`**/api/v1/tasks/${setup.tasks[2].id}/move`, async (route) => {
      const response = await route.fetch()
      await responseGate
      await route.fulfill({ response })
    })
    await page.goto(`/projects/${setup.projectId}/board`)
    await expect(page.getByText(setup.tasks[2].title).first()).toBeVisible({ timeout: 15_000 })

    const moveRequestPromise = page.waitForRequest(
      (request) =>
        request.method() === 'POST' && request.url().endsWith(`/tasks/${setup.tasks[2].id}/move`),
    )
    await beginHandleDrag(page, setup.tasks[2].title)
    const firstCard = page.locator(`[data-rfd-draggable-id="${setup.tasks[0].id}"]`)
    const firstBox = await firstCard.boundingBox()
    if (!firstBox) throw new Error('missing first card bounds')
    await page.mouse.move(firstBox.x + firstBox.width / 2, firstBox.y + 8, { steps: 24 })
    await page.mouse.up()
    const moveRequest = await moveRequestPromise
    await expect(page.locator('[data-board-phase="committing"]')).toBeVisible()
    const handles = page.getByRole('button', { name: /^Move / })
    await expect(handles.first()).toBeDisabled()
    await expect(handles.nth(1)).toBeDisabled()
    releaseResponse?.()
    await expect(page.locator('[data-board-phase="idle"]')).toBeVisible({ timeout: 15_000 })

    const requestBody = moveRequest.postDataJSON() as Record<string, unknown>
    const afterReorder = await taskPage(request, setup.projectId)
    expect(afterReorder.items.slice(0, 3).map((task) => task.id)).toEqual([
      setup.tasks[2].id,
      setup.tasks[0].id,
      setup.tasks[1].id,
    ])
    const moved = afterReorder.items.find((task) => task.id === setup.tasks[2].id)
    const replay = await request.post(`/api/v1/tasks/${setup.tasks[2].id}/move`, {
      data: requestBody,
    })
    await expectOk(replay, 'replay move operation')
    const replayed = (await replay.json()) as { task: Task }
    expect(replayed.task.version).toBe(moved?.version)

    await page.getByRole('button', { name: `Open ${setup.tasks[0].title}` }).click()
    await expect(page).toHaveURL(new RegExp(`task=${setup.tasks[0].id}`))
    await page.keyboard.press('Escape')

    const crossMoveResponse = page.waitForResponse(
      (response) =>
        response.request().method() === 'POST' &&
        response.url().endsWith(`/tasks/${setup.tasks[1].id}/move`),
    )
    await beginHandleDrag(page, setup.tasks[1].title)
    await dropIntoColumn(page, 'In Progress')
    expect((await crossMoveResponse).status()).toBe(200)
    await expect
      .poll(async () => {
        const response = await request.get(`/api/v1/tasks/${setup.tasks[1].id}`)
        return ((await response.json()) as Task).status
      })
      .toBe('in_progress')
  } finally {
    await rm(setup.repoDir, { recursive: true, force: true })
  }
})

test('search/filter and incomplete pagination disable only ordering', async ({ page, request }) => {
  const run = Date.now()
  const setup = await setupBoard(request, [`Disabled ordering ${run}`])
  try {
    await page.goto(`/projects/${setup.projectId}/board?q=Disabled`)
    const filteredHandle = page.getByRole('button', { name: `Move ${setup.tasks[0].title}` })
    await expect(filteredHandle).toBeDisabled({ timeout: 15_000 })
    await expect(page.getByText(/complete unfiltered board/i)).toBeVisible()
    await page.getByRole('button', { name: `Open ${setup.tasks[0].title}` }).click()
    await expect(page).toHaveURL(new RegExp(`task=${setup.tasks[0].id}`))
    await page.keyboard.press('Escape')

    await page.route(`**/api/v1/projects/${setup.projectId}/tasks**`, async (route) => {
      const response = await route.fetch()
      const body = (await response.json()) as TasksResponse
      await route.fulfill({
        response,
        json: { ...body, has_more: true, next_cursor: null },
      })
    })
    await page.goto(`/projects/${setup.projectId}/board`)
    const incompleteHandle = page.getByRole('button', { name: `Move ${setup.tasks[0].title}` })
    await expect(incompleteHandle).toBeDisabled({ timeout: 15_000 })
    await expect(page.getByText(/Load every task page/i)).toBeVisible()
  } finally {
    await rm(setup.repoDir, { recursive: true, force: true })
  }
})

test('responsive board keeps one scroll owner and reachable navigation at 375, 768, and 1280', async ({
  page,
  request,
}) => {
  const run = Date.now()
  const setup = await setupBoard(request, [
    `Responsive first ${run}`,
    `Responsive second ${run}`,
    `Responsive third ${run}`,
  ])
  const proofDir = join(process.cwd(), '..', 'test', 'proof-media')
  const runtimeWarnings: string[] = []
  page.on('console', (message) => {
    if (message.type() === 'warning' || message.type() === 'error') {
      runtimeWarnings.push(message.text())
    }
  })

  try {
    await mkdir(proofDir, { recursive: true })
    for (const viewport of [
      { width: 1280, height: 800, name: 'board-1280.png' },
      { width: 768, height: 900, name: 'board-768.png' },
      { width: 375, height: 812, name: 'board-375.png' },
    ]) {
      await page.setViewportSize({ width: viewport.width, height: viewport.height })
      await page.goto(`/projects/${setup.projectId}/board`)
      await expect(page.getByText(setup.tasks[0].title).first()).toBeVisible({ timeout: 15_000 })
      await expect(page.locator('[data-shell-mode]')).toHaveAttribute('data-shell-mode', 'sidebar')
      if (viewport.width >= 1024) {
        const navigation = page.getByRole('navigation', { name: 'Main navigation' })
        await expect(navigation).toBeVisible()
        await expect(navigation.getByRole('link', { name: 'Kanban' })).toBeVisible()
        await expect(navigation.getByRole('link', { name: /Main Agent/ })).toBeVisible()
        await expect(navigation.getByRole('link', { name: /Project Agent/ })).toBeVisible()
      } else {
        const menu = page.getByRole('button', { name: 'Open navigation' })
        await expect(menu).toBeVisible()
        await menu.click()
        const navigation = page.getByRole('dialog', { name: 'Navigation' })
        await expect(navigation.getByRole('link', { name: /Main Agent/ })).toBeVisible()
        await expect(navigation.getByRole('link', { name: /Project Agent/ })).toBeVisible()
        await navigation.getByRole('button', { name: 'Close navigation' }).click()
      }
      await expect(page.locator('[data-board-scroll-owner]')).toHaveCount(1)

      const documentOverflows = await page.evaluate(
        () => document.documentElement.scrollWidth > document.documentElement.clientWidth,
      )
      expect(documentOverflows).toBe(false)

      const firstColumn = page.getByRole('region', { name: 'Todo column' })
      const firstColumnBox = await firstColumn.boundingBox()
      if (!firstColumnBox) throw new Error(`missing first column at ${viewport.width}px`)
      expect(firstColumnBox.width).toBeGreaterThanOrEqual(viewport.width === 1280 ? 220 : 280)

      if (viewport.width === 768) {
        const secondColumnBox = await page.getByRole('region').nth(1).boundingBox()
        if (!secondColumnBox) throw new Error('missing second tablet column')
        expect(secondColumnBox.x + secondColumnBox.width).toBeLessThanOrEqual(viewport.width)
      }

      if (viewport.width === 375) {
        await page.getByRole('region', { name: 'Todo column' }).evaluate((column) => {
          column.scrollIntoView({ block: 'nearest', inline: 'start' })
        })
      }

      await page.screenshot({ path: join(proofDir, viewport.name), animations: 'disabled' })
    }

    expect(
      runtimeWarnings.filter((message) => /nested scroll|drag.*warning/i.test(message)),
    ).toEqual([])
  } finally {
    await rm(setup.repoDir, { recursive: true, force: true })
  }
})

test('the sidebar distinguishes Main Agent and Project Agent navigation', async ({
  page,
  request,
}) => {
  const run = Date.now()
  const setup = await setupBoard(request, [`Primary navigation ${run}`])
  const proofDir = join(process.cwd(), '..', 'test', 'proof-media')

  try {
    await mkdir(proofDir, { recursive: true })
    for (const viewport of [
      { width: 1280, height: 800, name: 'main-chat-1280.png' },
      { width: 768, height: 900, name: 'main-chat-768.png' },
      { width: 375, height: 812, name: 'main-chat-375.png' },
    ]) {
      await page.setViewportSize({ width: viewport.width, height: viewport.height })
      await page.goto(`/projects/${setup.projectId}/board`)

      if (viewport.width < 1024) {
        await page.getByRole('button', { name: 'Open navigation' }).click()
      }
      let navigation = page.getByRole('navigation', { name: 'Main navigation' })
      await navigation.getByRole('link', { name: /Main Agent/ }).click()
      await expect(page).toHaveURL(/\/chat$/)
      await expect(page.getByRole('heading', { name: 'Main Agent' })).toBeVisible({
        timeout: 15_000,
      })
      if (viewport.width < 1024) {
        await page.getByRole('button', { name: 'Open navigation' }).click()
      }
      navigation = page.getByRole('navigation', { name: 'Main navigation' })
      await expect(navigation.getByRole('link', { name: /Main Agent/ })).toHaveAttribute(
        'data-status',
        'active',
      )
      if (viewport.width < 1024) {
        await page.getByRole('button', { name: 'Close navigation' }).click()
      }
      await page.screenshot({ path: join(proofDir, viewport.name), animations: 'disabled' })
      if (viewport.width < 1024) {
        await page.getByRole('button', { name: 'Open navigation' }).click()
        navigation = page.getByRole('navigation', { name: 'Main navigation' })
      }

      await navigation.getByRole('link', { name: /Project Agent/ }).click()
      await expect(page).toHaveURL(new RegExp(`/projects/${setup.projectId}/chat`))
      if (viewport.width < 1024) {
        await page.getByRole('button', { name: 'Open navigation' }).click()
      }
      navigation = page.getByRole('navigation', { name: 'Main navigation' })
      await navigation.getByRole('link', { name: 'Kanban' }).click()
      await expect(page).toHaveURL(new RegExp(`/projects/${setup.projectId}/board`))
    }

    await page.getByRole('button', { name: 'Open navigation' }).click()
    const navigation = page.getByRole('dialog', { name: 'Navigation' })
    await expect(navigation.getByRole('link', { name: /Project Agent/ })).toBeVisible()
    await expect(navigation.getByRole('link', { name: 'Agent Settings' })).toBeVisible()
    await page.screenshot({
      path: join(proofDir, 'secondary-navigation-375.png'),
      animations: 'disabled',
    })
    await navigation.getByRole('button', { name: 'Close navigation' }).click()

    const filters = page.getByRole('button', { name: 'Filters' })
    await filters.focus()
    await expect(filters).toBeFocused()
    await filters.click()
    await expect(page.getByLabel('Minimum priority')).toBeVisible()
    await page.screenshot({ path: join(proofDir, 'board-filters-375.png'), animations: 'disabled' })
    await filters.click()

    await page.getByRole('button', { name: `Open ${setup.tasks[0].title}` }).click()
    await expect(page.getByRole('dialog')).toBeVisible()
    await page.screenshot({ path: join(proofDir, 'task-detail-375.png'), animations: 'disabled' })
    await page.keyboard.press('Escape')
    await expect(page.getByRole('dialog')).toBeHidden()
  } finally {
    await rm(setup.repoDir, { recursive: true, force: true })
  }
})

test('board state harness covers hover, focus, keyboard drag, loading, empty, and error', async ({
  page,
  request,
}) => {
  const run = Date.now()
  const setup = await setupBoard(request, [`State harness ${run}`])
  try {
    await page.route(
      `**/api/v1/projects/${setup.projectId}/tasks?limit=200*`,
      async (route) => {
        await new Promise((resolve) => setTimeout(resolve, 800))
        try {
          await route.continue()
        } catch {
          // The URL-backed filter hydration can supersede this first request.
        }
      },
      { times: 1 },
    )
    await page.goto(`/projects/${setup.projectId}/board`)
    await expect(page.getByLabel('Loading board')).toBeVisible()
    await expect(page.getByText(setup.tasks[0].title).first()).toBeVisible({ timeout: 15_000 })

    const openCard = page.getByRole('button', { name: `Open ${setup.tasks[0].title}` })
    await openCard.hover()
    const actions = page.getByRole('button', { name: `Open actions for ${setup.tasks[0].title}` })
    await expect(actions).toHaveCSS('opacity', '1')
    await openCard.focus()
    await expect(openCard).toBeFocused()

    const dragHandle = page.getByRole('button', { name: `Move ${setup.tasks[0].title}` })
    await dragHandle.focus()
    await expect(dragHandle).toBeFocused()
    await dragHandle.press(' ')
    await expect(page.locator('[data-board-phase="dragging"]')).toBeVisible()
    await dragHandle.press('Escape')
    await expect(page.locator('[data-board-phase="idle"]')).toBeVisible()

    const emptySetup = await setupBoard(request, [])
    try {
      await page.unroute(`**/api/v1/projects/${setup.projectId}/tasks**`)
      await page.goto(`/projects/${emptySetup.projectId}/board`)
      await expect(page.getByText('No tasks yet')).toBeVisible({ timeout: 15_000 })

      await page.route(`**/api/v1/projects/${emptySetup.projectId}/tasks**`, async (route) => {
        await route.fulfill({
          status: 500,
          contentType: 'application/json',
          body: '{"message":"Board state harness error"}',
        })
      })
      await page.reload()
      await expect(page.getByText('Board state harness error')).toBeVisible({ timeout: 15_000 })
    } finally {
      await rm(emptySetup.repoDir, { recursive: true, force: true })
    }
  } finally {
    await rm(setup.repoDir, { recursive: true, force: true })
  }
})
