import { authStorageValue } from './auth-utils'
import { expect, test, type APIRequestContext, type Page } from './fixtures'
import type { Response as BrowserResponse, Route } from '@playwright/test'

type Task = {
  id: string
  project_id: string
  title: string
  description: string | null
  priority: number
  status: string
  version: number
}
type TaskPage = { items: Task[]; has_more: boolean }
type Overview = { task_counts: { total: number; backlog: number } }

const seedTitle = 'Set up CI pipeline'
let network: Promise<unknown>[] = []

function recordNetwork(page: Page) {
  page.on('response', (response: BrowserResponse) => {
    const url = new URL(response.url())
    if (
      !/\/api\/v1\/(tasks\/[^/]+(?:\/detail)?|projects\/[^/]+\/(tasks|overview))$/.test(
        url.pathname,
      )
    )
      return
    network.push(
      (async () => ({
        url: `${url.pathname}${url.search}`,
        method: response.request().method(),
        request: response.request().postDataJSON(),
        status: response.status(),
        body: await Promise.race([
          response.json().catch(() => null),
          new Promise((resolve) => setTimeout(() => resolve('Response body unavailable'), 1000)),
        ]),
      }))(),
    )
  })
}

test.beforeEach(async ({ page }) => {
  network = []
  await page.context().route('**/*', async (route) => {
    const url = new URL(route.request().url())
    if (['127.0.0.1', 'localhost', '[::1]'].includes(url.hostname)) await route.continue()
    else await route.abort()
  })
  recordNetwork(page)
})

test.afterEach(async ({ page }, info) => {
  await info.attach('network', {
    body: JSON.stringify(await Promise.all(network), null, 2),
    contentType: 'application/json',
  })
  await info.attach('rendered-dom', {
    body: await page
      .locator('body')
      .innerText()
      .catch(() => 'Page closed'),
    contentType: 'text/plain',
  })
})

async function json<T>(request: APIRequestContext, path: string): Promise<T> {
  const response = await request.get(`/api/v1${path}`)
  expect(response.ok(), `${path}: ${response.status()}`).toBe(true)
  return response.json() as Promise<T>
}

async function demo(request: APIRequestContext) {
  const projects = await json<{ items: { id: string; name: string }[] }>(request, '/projects')
  const project = projects.items.find((item) => item.name === 'Demo')
  expect(project, 'Run against a --demo server with the entrypoint accounts').toBeTruthy()
  return project!.id
}

async function patch(
  request: APIRequestContext,
  task: Task,
  changes: Partial<Task>,
): Promise<Task> {
  const response = await request.patch(`/api/v1/tasks/${task.id}`, {
    data: { ...changes, version: task.version },
  })
  expect(response.ok(), `PATCH: ${response.status()}`).toBe(true)
  return response.json() as Promise<Task>
}

async function seed(request: APIRequestContext) {
  const projectId = await demo(request)
  const tasks = await json<TaskPage>(request, `/projects/${projectId}/tasks?limit=200`)
  const task = tasks.items.find((item) => item.title === seedTitle)
  expect(task).toBeTruthy()
  return patch(request, await json<Task>(request, `/tasks/${task!.id}`), { priority: 3 })
}

async function openSeed(page: Page, task: Task) {
  await page.goto(`/projects/${task.project_id}/board`)
  await page.getByRole('button', { name: `Open ${task.title}`, exact: true }).click()
  const dialog = page.getByRole('dialog')
  await expect(dialog.getByRole('button', { name: task.title, exact: true })).toBeVisible()
  return dialog
}

async function loadAllTaskPages(page: Page) {
  const more = page.getByRole('button', { name: 'Load More', exact: true })
  while (await more.count()) {
    const loaded = page.waitForResponse((response) => {
      const url = new URL(response.url())
      return (
        response.request().method() === 'GET' &&
        url.pathname.endsWith('/tasks') &&
        url.searchParams.has('cursor')
      )
    })
    await more.click()
    const response = await loaded
    const data = (await response.json()) as TaskPage
    if (!data.has_more) {
      await expect(more).toHaveCount(0)
      return
    }
    await expect(page.getByRole('button', { name: 'Loading...', exact: true })).toHaveCount(0)
  }
}

for (const concurrent of [false, true]) {
  test(`KBD-001 clearing board search restores every card (${concurrent ? 'concurrent writer' : 'alone'})`, async ({
    page,
    request,
  }) => {
    const projectId = await demo(request)
    await page.addInitScript(() => {
      const events: { event_type: string; entity_id: string }[] = []
      Object.assign(window, { kbdTaskEvents: events })
      const OriginalEventSource = window.EventSource
      window.EventSource = class extends OriginalEventSource {
        constructor(url: string | URL, options?: EventSourceInit) {
          super(url, options)
          this.addEventListener('message', (event) => {
            const payload = JSON.parse(event.data)
            if (payload.event_type?.startsWith('task.')) events.push(payload)
          })
        }
      }
    })
    await page.goto(`/projects/${projectId}/board`)
    await expect(page.getByRole('button', { name: `Open ${seedTitle}`, exact: true })).toBeVisible()
    await page.waitForTimeout(1200)
    const cards = page.locator('article > button[aria-label^="Open "]')
    const cardNames = () =>
      cards.evaluateAll((buttons) =>
        buttons.map((button) => button.getAttribute('aria-label')).sort(),
      )
    const expected = await cardNames()
    expect(expected.length).toBeGreaterThan(1)
    const field = page.getByRole('textbox', { name: 'Search board tasks' })
    for (const method of ['Delete', 'Backspace', 'fill', 'Clear all'] as const) {
      await field.pressSequentially('WebSocket', { delay: 10 })
      await expect(cards).toHaveCount(1)
      await expect(cards).toHaveAttribute(
        'aria-label',
        'Open Add WebSocket support for live updates',
      )
      let release = () => {}
      let heldSearch = false
      let heldFull = false
      const gate = new Promise<void>((resolve) => {
        release = resolve
      })
      const pattern = `**/api/v1/projects/${projectId}/tasks?*`
      const holdReads = async (route: Route) => {
        if (route.request().method() !== 'GET') return route.continue()
        const response = await route.fetch()
        const q = new URL(route.request().url()).searchParams.get('q')
        if (q === 'WebSocket' && !heldSearch) {
          heldSearch = true
          await gate
        } else if (!q && !heldFull) {
          heldFull = true
          await gate
        }
        await route.fulfill({ response }).catch(() => {})
      }
      let created: Task | undefined
      const create = async (title: string) => {
        const response = await request.post(`/api/v1/projects/${projectId}/tasks`, {
          data: { title },
        })
        expect(response.ok(), `writer POST: ${response.status()}`).toBe(true)
        return response.json() as Promise<Task>
      }
      try {
        if (concurrent) {
          await page.route(pattern, holdReads)
          created = await create(`KBD before clear ${crypto.randomUUID()}`)
          await expect.poll(() => heldSearch).toBe(true)
          created = await patch(request, created, { description: 'Edited while search was active' })
        }
        if (method === 'fill') await field.fill('')
        else if (method === 'Clear all') {
          await page.getByRole('button', { name: 'Filters', exact: true }).click()
          await page.getByRole('button', { name: 'Clear all', exact: true }).click()
          await page.getByRole('button', { name: 'Filters', exact: true }).click()
        } else {
          await field.focus()
          await field.press('ControlOrMeta+A')
          await field.press(method)
        }
        await expect(field).toHaveValue('')
        await expect(page).not.toHaveURL(/[?&]q=/)
        if (concurrent && created) {
          await expect.poll(() => heldFull).toBe(true)
          const second = await create(`KBD during clear ${crypto.randomUUID()}`)
          created = await patch(request, created, { title: `KBD edited ${crypto.randomUUID()}` })
          expected.push(`Open ${created.title}`, `Open ${second.title}`)
          await expect
            .poll(() =>
              page.evaluate(
                (id) =>
                  (
                    window as unknown as {
                      kbdTaskEvents: { event_type: string; entity_id: string }[]
                    }
                  ).kbdTaskEvents.some(
                    (event) => event.event_type === 'task.updated' && event.entity_id === id,
                  ),
                created!.id,
              ),
            )
            .toBe(true)
          // Keep both reads in flight across the task-list invalidation throttle.
          await page.waitForTimeout(1800)
        }
        release()
        await expect.poll(cardNames).toEqual([...expected].sort())
        await test.info().attach(`kbd-${method}`, {
          body: JSON.stringify(
            {
              url: page.url(),
              input: await field.inputValue(),
              cards: await cardNames(),
              dom: await page.locator('body').innerText(),
            },
            null,
            2,
          ),
          contentType: 'application/json',
        })
      } finally {
        release()
        if (concurrent) await page.unroute(pattern, holdReads)
      }
    }
    await test.info().attach('kbd-task-events', {
      body: JSON.stringify(
        await page.evaluate(
          () => (window as unknown as { kbdTaskEvents: unknown[] }).kbdTaskEvents,
        ),
      ),
      contentType: 'application/json',
    })
  })

  test(`TDT-007 Escape discards a title draft (${concurrent ? 'concurrent, disconnected SSE' : 'alone'})`, async ({
    page,
    request,
  }) => {
    if (concurrent) await page.route('**/api/v1/events*', (route) => route.abort())
    const task = await seed(request)
    try {
      const dialog = await openSeed(page, task)
      await dialog.getByRole('button', { name: task.title, exact: true }).click()
      await dialog.locator('header input').fill('Unsaved title must be discarded')
      const stored = concurrent
        ? await patch(request, task, { title: `${seedTitle} from second client` })
        : task
      await page.keyboard.press('Escape')
      await expect(dialog.getByRole('button', { name: stored.title, exact: true })).toBeVisible()
      expect((await json<Task>(request, `/tasks/${task.id}`)).title).toBe(stored.title)
      await page.keyboard.press('Escape')
      await expect(
        page.getByRole('button', { name: `Open ${stored.title}`, exact: true }),
      ).toBeVisible()
    } finally {
      await patch(request, await json<Task>(request, `/tasks/${task.id}`), { title: seedTitle })
    }
  })

  test(`TDT-006 priority saves to the board (${concurrent ? 'concurrent version conflict' : 'alone'})`, async ({
    page,
    request,
  }) => {
    // A disconnected event stream makes the conflict deterministic: the form
    // retains the version the user actually read before the other client's write.
    await page.route('**/api/v1/events*', (route) => route.abort())
    const task = await seed(request)
    const dialog = await openSeed(page, task)
    const priority = dialog.locator('input[type="number"]').first()
    await expect(priority).toHaveValue('3')
    await priority.fill('4')
    if (concurrent) await patch(request, task, { description: 'Written by another client' })
    await dialog.locator('header').click({ position: { x: 3, y: 3 } })
    await expect.poll(async () => (await json<Task>(request, `/tasks/${task.id}`)).priority).toBe(4)
    await page.keyboard.press('Escape')
    const card = page
      .locator('article')
      .filter({ has: page.getByRole('button', { name: `Open ${task.title}`, exact: true }) })
    await expect(card).toContainText('P4')
  })

  test(`TKS-004 status and minimum priority intersect (${concurrent ? 'concurrent' : 'alone'})`, async ({
    page,
    request,
  }) => {
    const projectId = await demo(request)
    const before = await json<TaskPage>(request, `/projects/${projectId}/tasks?limit=200`)
    const edited = before.items.find((item) => item.title === 'Implement user settings page')!
    if (concurrent)
      await patch(request, await json<Task>(request, `/tasks/${edited.id}`), { priority: 5 })
    try {
      await page.goto(
        `/projects/${projectId}/tasks?statuses=todo,review&priorityMin=6&sort_by=priority&sort_order=desc`,
      )
      await expect(page.getByRole('table')).toBeVisible()
      await loadAllTaskPages(page)
      if (concurrent) {
        await patch(request, await json<Task>(request, `/tasks/${edited.id}`), { priority: 7 })
      }
      const matches = (
        await json<TaskPage>(request, `/projects/${projectId}/tasks?limit=200`)
      ).items.filter((task) => ['todo', 'review'].includes(task.status) && task.priority >= 6)
      await expect(page.locator('tbody tr')).toHaveCount(matches.length)
      for (const task of matches)
        await expect(page.getByRole('button', { name: task.title, exact: true })).toBeVisible()
      await page.getByRole('button', { name: 'Clear all', exact: true }).click()
      await expect(page.getByRole('table')).toBeVisible()
      await loadAllTaskPages(page)
      const all = await json<TaskPage>(request, `/projects/${projectId}/tasks?limit=200`)
      await expect(page.locator('tbody tr')).toHaveCount(all.items.length)
      await page.getByRole('button', { name: 'Statuses', exact: true }).click()
      await page.getByRole('checkbox', { name: 'todo', exact: true }).check()
      await page.getByRole('checkbox', { name: 'review', exact: true }).check()
      await page.keyboard.press('Escape')
      await page.getByPlaceholder('Min', { exact: true }).fill('6')
      await loadAllTaskPages(page)
      await expect(page.locator('tbody tr')).toHaveCount(matches.length)
    } finally {
      if (concurrent)
        await patch(request, await json<Task>(request, `/tasks/${edited.id}`), { priority: 5 })
    }
  })
}

function columnCount(page: Page, name: string) {
  return page.getByRole('region', { name: `${name} column`, exact: true }).locator('header > span')
}

function metric(page: Page, name: string) {
  return page.getByText(name, { exact: true }).locator('..').locator('p').last()
}

for (const concurrent of [false, true]) {
  test(`TSK-001 and OVW-003 create refreshes counts (${concurrent ? 'concurrent with an older board response' : 'alone'})`, async ({
    page,
    request,
    e2eAuth,
  }) => {
    const projectId = await demo(request)
    const before = await json<TaskPage>(request, `/projects/${projectId}/tasks?limit=200`)
    const beforeCounts = (await json<Overview>(request, `/projects/${projectId}/overview`))
      .task_counts
    const beforeTodo = before.items.filter((task) => task.status === 'todo').length
    const beforeBacklog = before.items.filter((task) => task.status === 'backlog').length
    const overviewPage = concurrent ? await page.context().newPage() : page
    if (concurrent) {
      await overviewPage.addInitScript(
        (value) => localStorage.setItem('forge-auth', value),
        authStorageValue(e2eAuth),
      )
      recordNetwork(overviewPage)
    }
    await overviewPage.goto(`/projects/${projectId}/overview`)
    await expect(metric(overviewPage, 'Total')).toHaveText(String(beforeCounts.total))
    // Let useSSE's one-second connection-open resync finish before arranging
    // a later read/event race; that resync would otherwise repair it by chance.
    if (concurrent) await overviewPage.waitForTimeout(1200)
    await page.goto(`/projects/${projectId}/board`)
    await expect(columnCount(page, 'Todo')).toHaveText(String(beforeTodo))
    if (concurrent) await page.waitForTimeout(1200)
    let release = () => {}
    let captured = Promise.resolve()
    let startedReads = 0
    let capturedReads = 0
    if (concurrent) {
      let capture = () => {}
      captured = new Promise<void>((resolve) => {
        capture = resolve
      })
      const released = new Promise<void>((resolve) => {
        release = resolve
      })
      await page.route(`**/api/v1/projects/${projectId}/tasks?*`, async (route: Route) => {
        if (route.request().method() !== 'GET') return route.continue()
        startedReads += 1
        const response = await route.fetch()
        capturedReads += 1
        capture()
        await released
        await route.fulfill({ response }).catch(() => {})
      })
    }
    try {
      const title = `TSK-001 created ${crypto.randomUUID()}`
      await page.getByRole('button', { name: 'New task', exact: true }).click()
      const dialog = page.getByRole('dialog', { name: 'Create Task', exact: true })
      await dialog.getByRole('textbox', { name: 'Title', exact: true }).fill(title)
      const created = page.waitForResponse(
        (response) =>
          response.request().method() === 'POST' &&
          response.url().endsWith(`/projects/${projectId}/tasks`),
      )
      await dialog.getByRole('button', { name: 'Create Task', exact: true }).click()
      expect((await created).ok()).toBe(true)
      await expect(dialog).not.toBeVisible()
      let secondTitle: string | undefined
      if (concurrent) {
        await captured
        await expect(page.getByRole('dialog', { name: title, exact: true })).toBeVisible()
        await expect.poll(() => capturedReads === startedReads).toBe(true)
        secondTitle = `Concurrent create ${crypto.randomUUID()}`
        const response = await request.post(`/api/v1/projects/${projectId}/tasks`, {
          data: { title: secondTitle },
        })
        expect(response.ok()).toBe(true)
        // Deliver both events while the pre-second-create read is in flight.
        // This spans the production list invalidation throttle, not a guess at
        // when the network or DOM will settle.
        await page.waitForTimeout(1800)
        release()
      }
      await expect(page.getByRole('button', { name: `Open ${title}`, exact: true })).toBeVisible()
      await expect(columnCount(page, 'Todo')).toHaveText(String(beforeTodo + (concurrent ? 2 : 1)))
      if (secondTitle)
        await expect(
          page.getByRole('button', { name: `Open ${secondTitle}`, exact: true }),
        ).toBeVisible()
      await expect(columnCount(page, 'Backlog')).toHaveText(String(beforeBacklog))
      if (!concurrent) await overviewPage.goto(`/projects/${projectId}/overview`)
      await expect(metric(overviewPage, 'Total')).toHaveText(
        String(Number(beforeCounts.total) + (concurrent ? 2 : 1)),
      )
      await expect(metric(overviewPage, 'Backlog')).toHaveText(
        String(Number(beforeCounts.backlog) + (concurrent ? 2 : 1)),
      )
    } finally {
      release()
      if (concurrent) {
        await test.info().attach('overview-dom', {
          body: await overviewPage.locator('body').innerText(),
          contentType: 'text/plain',
        })
        await overviewPage.close()
      }
    }
  })
}
