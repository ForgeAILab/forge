import { expect, test, type Page } from '@playwright/test'
import type { Offer, TaskAction } from '../src/types/generated'
import { join } from 'node:path'

const taskId = 'action-contract-task'
const projectId = 'action-contract-project'
const text = (
  name: string,
  required: boolean,
  required_when?: { parameter: string; value: boolean },
) => ({ name, required, boolean_values: null, ...(required_when ? { required_when } : {}) })
const offer = (
  action: TaskAction,
  label: string,
  parameters: Offer['parameters'] = [],
  propagates = false,
): Offer => ({
  action,
  label,
  parameters,
  reason: 'review_failed',
  authority: ['owner'],
  target_execution_id: null,
  propagates,
})
const offers = [
  offer({ verb: 'retry', fresh_session: true }, 'Re-run review', [
    { name: 'fresh_session', required: false, boolean_values: [true] },
    text('guidance', false),
  ]),
  offer({ verb: 'send_back', guidance: '' }, 'Send back', [text('guidance', true)]),
  offer({ verb: 'approve', override: false }, 'Approve', [
    { name: 'override', required: true, boolean_values: [false, true] },
    text('reason', false, { parameter: 'override', value: true }),
  ]),
  offer({ verb: 'cancel' }, 'Cancel Task', [text('reason', false)], true),
]
const hooks = { before_exit: [], on_exit: [], on_enter: [], after_enter: [] }
const workflow = {
  states: ['todo', 'in_progress', 'review', 'done', 'cancelled'].map((name) => ({
    name,
    kind:
      name === 'todo' ? 'initial' : ['done', 'cancelled'].includes(name) ? 'terminal' : 'active',
    column: name,
    display_name: name,
    role: name === 'in_progress' ? 'coder' : null,
    hooks,
    gate_config: null,
    config: {},
  })),
  roles: [{ name: 'coder', display_name: 'Worker', description: '' }],
  cancellation_state: 'cancelled',
}
async function setup(page: Page, running = false) {
  await page.addInitScript(() =>
    localStorage.setItem(
      'forge-auth',
      JSON.stringify({
        version: 0,
        state: {
          accessToken: 'mock-token',
          refreshToken: 'mock-refresh',
          user: { id: 'owner', email: 'owner@example.test', display_name: 'Owner', is_admin: true },
        },
      }),
    ),
  )
  const task = {
    id: taskId,
    project_id: projectId,
    title: 'Review the task action contract',
    description:
      'The review found missing coverage. Choose the next action and provide the required input.',
    status: running ? 'in_progress' : 'review',
    canonical_phase: running ? 'active' : 'review',
    task_type: 'task',
    priority: 50,
    board_position: 0,
    version: 7,
    parent_task_id: null,
    subtask_order: null,
    assignee_type: null,
    assignee_id: null,
    role_assignments: [],
    effective_coder: null,
    effective_coder_source: null,
    remaining_retries: {},
    available_actions: running ? [] : offers,
    error_annotation: null,
    blocked: null,
    failed: running
      ? null
      : {
          kind: 'review_failed',
          reason: 'Missing contract coverage',
          execution_id: null,
          metadata: {},
        },
    workflow_health: null,
    workflow_exception: running
      ? null
      : {
          type: 'review_failed',
          message: 'Review failed: add the missing coverage before continuing.',
          state: 'review',
          role: 'reviewer',
          target_state: null,
          target_role: null,
          review_id: null,
          execution_id: null,
          failing_step: null,
          related_evidence: [],
          actions: offers,
        },
    placement_diagnostics: [],
    awaiting_human: !running,
    execution_observability: null,
    execution_evidence: {
      attempt_count: 1,
      execution_count: 1,
      has_commit: true,
      latest_commit_sha: 'abc123',
      progress: 'committed',
      progress_phrase: 'Committed work',
    },
    execution_blocker: null,
    workspace: null,
    placement: null,
    plan: null,
    plan_progress: null,
    plan_artifact: null,
    task_state_config: null,
    review_passed_at: null,
    created_at: '2026-10-02T00:00:00Z',
    updated_at: '2026-10-02T00:00:00Z',
  }
  const project = {
    id: projectId,
    name: 'Task actions',
    settings: {},
    paused: false,
    version: 1,
    default_review_config: { ci_steps: [] },
    project_hooks: [],
    created_at: task.created_at,
    updated_at: task.updated_at,
  }
  const executions = running
    ? ['role-run', 'side-session'].map((id, index) => ({
        id,
        task_id: taskId,
        agent_id: 'worker',
        role: index ? 'interactive' : 'coder',
        status: 'running',
        parent_execution_id: null,
        agent_session_id: id,
        summary: index ? 'Interactive side session' : 'Workflow role run',
        created_at: task.created_at,
        updated_at: task.updated_at,
        is_resume: false,
      }))
    : []
  const posts: { path: string; body: unknown }[] = []
  await page.route('**/api/v1/**', async (route) => {
    const path = new URL(route.request().url()).pathname.slice('/api/v1'.length)
    if (route.request().method() === 'POST') {
      posts.push({ path, body: route.request().postDataJSON() })
      if (path.endsWith('/stop')) {
        const execution = executions.find((item) => item.id === path.split('/')[2])
        if (!execution) return route.fulfill({ status: 404, json: {} })
        execution.status = 'cancelled'
        return route.fulfill({ json: execution })
      }
      return route.fulfill({ json: task })
    }
    const empty = { items: [], has_more: false, next_cursor: null, total_count: 0 }
    let json: unknown = empty
    if (path === `/tasks/${taskId}/detail`)
      json = { task, workflow, executions: { ...empty, items: executions } }
    else if (path === `/tasks/${taskId}`) json = task
    else if (path === `/tasks/${taskId}/actions`)
      json = { available_actions: task.available_actions, version: task.version }
    else if (path === `/tasks/${taskId}/relations`)
      json = { dependencies: [], subtasks: [], parent: null }
    else if (path === `/projects/${projectId}`) json = project
    else if (path === '/projects') json = { ...empty, items: [project] }
    else if (path.endsWith('/workflow')) json = workflow
    else if (path === '/auth/me')
      json = { id: 'owner', email: 'owner@example.test', is_admin: true }
    else if (path.includes('/reviews') || path.endsWith('/transitions')) json = []
    else if (path.includes('/unread-count')) json = { count: 0 }
    else if (path === '/settings') json = {}
    else if (path === `/projects/${projectId}/agents` || path === `/projects/${projectId}/members`)
      json = []
    else if (path === '/agents')
      json = {
        ...empty,
        items: [
          {
            id: 'worker',
            name: 'Worker',
            status: 'busy',
            capabilities: [],
            config_json: {},
            paused: false,
          },
        ],
      }
    return route.fulfill({ json })
  })
  await page.goto(`/tasks/${taskId}${running ? '/executions' : ''}`, {
    waitUntil: 'domcontentloaded',
  })
  await page.waitForTimeout(500)
  return posts
}
async function shot(page: Page, name: string) {
  if (process.env.FORGE_ACTION_SHOTS_DIR)
    await page.screenshot({
      path: join(process.env.FORGE_ACTION_SHOTS_DIR, name + '.png'),
      fullPage: true,
    })
}

test('failed review exposes offered actions and retained launch controls', async ({ page }) => {
  await setup(page)
  await expect(page.getByRole('button', { name: 'Send back', exact: true })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Re-run review', exact: true })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Launch run', exact: true })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Open interactive', exact: true })).toBeVisible()
  await shot(page, 'failed-review-offers')
})
test('send-back requires typed guidance', async ({ page }) => {
  const posts = await setup(page)
  await page.getByRole('button', { name: 'Send back', exact: true }).click()
  await expect(page.getByRole('button', { name: 'Apply', exact: true })).toBeDisabled()
  await shot(page, 'send-back-guidance')
  await page
    .getByLabel('guidance (required)', { exact: true })
    .fill('Add coverage for the action contract')
  await page.getByRole('button', { name: 'Apply', exact: true }).click()
  await expect
    .poll(() => posts)
    .toContainEqual({
      path: `/tasks/${taskId}/actions`,
      body: {
        action: { verb: 'send_back', guidance: 'Add coverage for the action contract' },
        version: 7,
      },
    })
})
test('approve override requires its conditional reason', async ({ page }) => {
  await setup(page)
  await page.getByRole('button', { name: 'Approve', exact: true }).click()
  await page.getByLabel('Override checks (required)', { exact: true }).selectOption('true')
  await expect(page.getByLabel('reason (required)', { exact: true })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Apply', exact: true })).toBeDisabled()
  await shot(page, 'approve-override-reason')
})
test('cancel confirms subtask propagation', async ({ page }) => {
  await setup(page)
  await page.getByRole('button', { name: 'Cancel Task', exact: true }).click()
  await expect(page.getByText('This cancels this Task and its subtasks.')).toBeVisible()
  await shot(page, 'cancel-subtasks')
})
test('role run and side session stop independently', async ({ page }) => {
  const posts = await setup(page, true)
  const stops = page.getByRole('button', { name: 'Stop', exact: true })
  await expect(stops).toHaveCount(2)
  await shot(page, 'executions-two-running')
  await stops.nth(0).click()
  await expect.poll(() => posts.length).toBe(1)
  await expect(stops).toHaveCount(1)
  await stops.first().click()
  await expect.poll(() => posts.length).toBe(2)
  await expect(stops).toHaveCount(0)
  expect(new Set(posts.map((post) => post.path))).toEqual(
    new Set(['/executions/role-run/stop', '/executions/side-session/stop']),
  )
})
