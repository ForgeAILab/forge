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
  reason:
    action.verb === 'retry'
      ? action.reset_budget
        ? 'retry_budget_exhausted'
        : 'role_retry'
      : action.verb === 'approve'
        ? 'failed_review_override'
        : action.verb === 'cancel'
          ? 'cancellable'
          : 'gate_can_reject',
  authority: ['owner'],
  target_execution_id: null,
  propagates,
})
const offers = [
  offer({ verb: 'retry', fresh_session: false }, 'Re-run review', [
    { name: 'fresh_session', required: false, boolean_values: [false, true] },
    text('guidance', false),
    text('reason', false),
  ]),
  offer({ verb: 'send_back', guidance: '' }, 'Send back', [text('guidance', true)]),
  offer({ verb: 'approve', override: true }, 'Override failed review', [
    { name: 'override', required: false, boolean_values: [true] },
    text('reason', true),
  ]),
  offer({ verb: 'retry', reset_budget: true }, 'Reset Budget and Retry', [
    { name: 'reset_budget', required: false, boolean_values: [true] },
    text('reason', false),
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
async function setup(page: Page, running = false, board = false) {
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
    condition: running
      ? {
          kind: 'running',
          execution_id: 'role-run',
          role: 'coder',
          epoch: 0,
          since: '2026-10-02T00:00:00Z',
          details: {
            failure_kind: null,
            diagnostic: null,
            interruption: null,
            failed: false,
            blocked: false,
            human_wait: false,
            entry_wait: false,
          },
        }
      : {
          kind: 'failed',
          failure: { kind: 'failure', failure_kind: 'review_gate_failed' },
          additional: [],
          resume: { kind: 'reconcile' },
          since: '2026-10-02T00:00:00Z',
          details: {
            failure_kind: 'review_gate_failed',
            diagnostic: null,
            interruption: {
              reason: 'Missing contract coverage',
              created_at: '2026-10-02T00:00:00Z',
              kind: 'review_gate_failed',
              execution_id: null,
              details: {},
            },
            failed: true,
            blocked: false,
            human_wait: true,
            entry_wait: false,
          },
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
    slots: { active: 0, parked: 1, queued: 0, limit: 4 },
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
    else if (path === `/projects/${projectId}/overview`)
      json = {
        project_id: projectId,
        project_name: project.name,
        vision: '',
        charter_state: 'approved',
        current_charter: null,
        primary_milestone_id: null,
        active_milestones: [],
        task_counts: { total: 1, backlog: 0, active: 0, review: 1, terminal: 0, blocked: 0 },
        check_summary: {
          required_total: 0,
          passed: 0,
          failed: 0,
          missing: 0,
          stale: 0,
          waived: 0,
          unavailable: 0,
        },
        pending_decisions: [],
        decisions: [],
        risks: [],
        document_freshness: [],
        evidence: [],
        releases: [],
        next_action: null,
        projection_state: 'current',
        source_event_watermark: 'mock',
        generated_at: task.created_at,
        execution_setup: null,
      }
    else if (path === `/projects/${projectId}/tasks`)
      json = { ...empty, items: [task], board_revision: 1 }
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
  await page.goto(
    board
      ? `/projects/${projectId}/board?task=${taskId}`
      : `/tasks/${taskId}${running ? '/executions' : ''}`,
    {
      waitUntil: 'domcontentloaded',
    },
  )
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
  await page.getByRole('button', { name: 'Override failed review', exact: true }).click()
  await expect(page.getByText('Overrides the failed checks')).toBeVisible()
  await expect(page.getByRole('combobox')).toHaveCount(0)
  await expect(page.getByLabel('reason (required)', { exact: true })).toBeVisible()
  await expect(page.getByRole('button', { name: 'Apply', exact: true })).toBeDisabled()
  await shot(page, 'S4-fixed-override')
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

test('board exception offers appear once and sidebar hides duplicates', async ({ page }) => {
  await setup(page, false, true)
  await expect(page.getByRole('button', { name: 'Send back', exact: true })).toHaveCount(1)
  await expect(page.getByRole('button', { name: 'Cancel Task', exact: true })).toHaveCount(1)
  await shot(page, 'S6-board-exception')
})
test('Executions tab preserves fresh retries after refreshed Task offers', async ({ page }) => {
  const posts = await setup(page)
  await page.goto(`/tasks/${taskId}/executions`)
  await expect(page.getByRole('button', { name: 'Re-run review', exact: true })).toBeVisible()
  await page.getByRole('button', { name: 'Re-run review', exact: true }).click()
  await page.getByRole('button', { name: 'Apply', exact: true }).click()
  await expect.poll(() => posts.length).toBe(1)
  await expect(page.getByRole('button', { name: 'Cancel Task', exact: true })).toHaveCount(0)
  await expect(page.getByRole('button', { name: 'Send back', exact: true })).toHaveCount(0)
  await page.getByRole('button', { name: 'Re-run review', exact: true }).click()
  await expect(page.getByText('Starts a fresh session')).toBeVisible()
  await shot(page, 'S3-executions-refreshed')
})
