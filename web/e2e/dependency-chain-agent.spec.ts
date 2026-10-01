import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { expect, test, type APIRequestContext, type APIResponse } from './fixtures'

const execFileAsync = promisify(execFile)

type PaginatedResponse<T> = { items: T[]; has_more: boolean }
type ProjectResponse = { id: string; name: string }
type RepoResponse = { id: string; project_id: string }
type AgentResponse = { id: string; name: string }
type TaskResponse = { id: string; project_id: string; title: string; status: string; version: number }
type TransitionLogEntry = { from_state: string; to_state: string; created_at: string }

const runId = `dep-agent-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`

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
  const body = await response.text()
  if (!body) return undefined as T
  return JSON.parse(body) as T
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

async function waitForStatus(
  request: APIRequestContext,
  taskId: string,
  statuses: string[],
  timeoutMs: number,
): Promise<TaskResponse> {
  return poll(
    `task ${taskId} to reach ${statuses.join(' or ')}`,
    async () => {
      const task = await api<TaskResponse>(request, 'GET', `/api/v1/tasks/${taskId}`)
      return statuses.includes(task.status) ? task : null
    },
    { timeoutMs },
  )
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

async function firstEntryIntoState(
  request: APIRequestContext,
  taskId: string,
  toState: string,
): Promise<string | null> {
  const log = await getTransitions(request, taskId)
  const entry = log
    .filter((e) => e.to_state === toState)
    .sort((a, b) => a.created_at.localeCompare(b.created_at))[0]
  return entry?.created_at ?? null
}

async function cleanupTask(request: APIRequestContext, taskId: string): Promise<void> {
  const res = await request.get(`/api/v1/tasks/${taskId}`, { failOnStatusCode: false })
  if (!res.ok()) return
  const task = (await res.json()) as TaskResponse
  if (!['done', 'cancelled'].includes(task.status)) {
    await request.post(`/api/v1/tasks/${taskId}/cancel`, { failOnStatusCode: false })
    await waitForStatus(request, taskId, ['done', 'cancelled'], 120_000).catch(() => {})
  }
  await request.delete(`/api/v1/tasks/${taskId}`, { failOnStatusCode: false })
}

async function createFixture(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), `forge-dep-agent-${runId}-`))
  await mkdir(join(root, 'src'), { recursive: true })
  await Promise.all([
    writeFile(
      join(root, 'src/main.ts'),
      '// Dependency chain agent test fixture\nexport const phases: string[] = []\n',
    ),
    writeFile(join(root, '.gitignore'), 'node_modules\n'),
  ])
  await execFileAsync('git', ['init'], { cwd: root })
  await execFileAsync('git', ['checkout', '-B', 'main'], { cwd: root })
  await execFileAsync('git', ['config', 'user.email', 'forge-dep-agent@example.test'], { cwd: root })
  await execFileAsync('git', ['config', 'user.name', 'Forge Dep Agent'], { cwd: root })
  await execFileAsync('git', ['add', '.'], { cwd: root })
  await execFileAsync('git', ['commit', '-m', 'Initial fixture'], { cwd: root })
  return root
}

test.describe('dependency chain agent flow (integration)', () => {
  test(
    'A→B→C: agents execute in dependency order — B waits for A, C waits for B',
    async ({ request }) => {
      test.setTimeout(30 * 60 * 1000)

      const createdAgentIds: string[] = []
      const createdTaskIds: string[] = []
      const fixturePath = await createFixture()

      try {
        const serverCheck = await request.get('/api/v1/projects', { failOnStatusCode: false })
        await expectOk(
          serverCheck,
          'GET /api/v1/projects. Start the Forge API server on localhost:8080 before running this integration test',
        )

        // --- Project + repo ---
        const project = await api<ProjectResponse>(request, 'POST', '/api/v1/projects', {
          name: `Dep Agent Chain ${runId}`,
          settings: {},
          default_review_config: { ci_steps: [], review_prompt: null },
        })
        await api<unknown>(request, 'PUT', `/api/v1/projects/${project.id}/workflow`, {
          template_name: 'no-user-approval',
        })
        await api<RepoResponse>(request, 'POST', `/api/v1/projects/${project.id}/repos`, {
          name: `dep-agent-repo-${runId}`,
          remote_url: fixturePath,
          local_path: fixturePath,
          default_branch: 'main',
        })

        // --- Agents ---
        const coder = await api<AgentResponse>(request, 'POST', '/api/v1/agents', {
          name: `Dep Coder ${runId}`,
          executor_type: 'claude_code',
          model: 'claude-haiku-4-5',
          permission_policy: 'supervised',
          max_concurrent_tasks: 1,
        })
        createdAgentIds.push(coder.id)

        const reviewer = await api<AgentResponse>(request, 'POST', '/api/v1/agents', {
          name: `Dep Reviewer ${runId}`,
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
          },
          default_review_config: { ci_steps: [], review_prompt: null },
        })

        // --- Create tasks A, B, C ---
        const taskA = await api<TaskResponse>(
          request,
          'POST',
          `/api/v1/projects/${project.id}/tasks`,
          {
            title: `Phase A ${runId}`,
            description: [
              `Create a new file src/phase-a-${runId}.txt containing exactly one line: "phase-a-${runId}".`,
              'Do not modify any other files.',
            ].join('\n'),
          },
        )
        createdTaskIds.push(taskA.id)

        const taskB = await api<TaskResponse>(
          request,
          'POST',
          `/api/v1/projects/${project.id}/tasks`,
          {
            title: `Phase B ${runId}`,
            description: [
              `Create a new file src/phase-b-${runId}.txt containing exactly one line: "phase-b-${runId}".`,
              'Do not modify any other files.',
            ].join('\n'),
          },
        )
        createdTaskIds.push(taskB.id)

        const taskC = await api<TaskResponse>(
          request,
          'POST',
          `/api/v1/projects/${project.id}/tasks`,
          {
            title: `Phase C ${runId}`,
            description: [
              `Create a new file src/phase-c-${runId}.txt containing exactly one line: "phase-c-${runId}".`,
              'Do not modify any other files.',
            ].join('\n'),
          },
        )
        createdTaskIds.push(taskC.id)

        // --- Wire dependency chain: B→A, C→B ---
        await api(request, 'POST', `/api/v1/tasks/${taskB.id}/dependencies`, {
          depends_on_id: taskA.id,
        })
        await api(request, 'POST', `/api/v1/tasks/${taskC.id}/dependencies`, {
          depends_on_id: taskB.id,
        })

        // --- Wait for A to complete first ---
        const doneA = await waitForStatus(request, taskA.id, ['done'], 10 * 60 * 1000)
        expect(doneA.status).toBe('done')

        // --- While A was running, B must have stayed in todo ---
        const bAfterADone = await api<TaskResponse>(request, 'GET', `/api/v1/tasks/${taskB.id}`)
        // B may have just started moving (dispatcher poll may have fired) but must not be done yet
        expect(['todo', 'planning', 'in_progress']).toContain(bAfterADone.status)

        // --- Wait for B to complete ---
        const doneB = await waitForStatus(request, taskB.id, ['done'], 10 * 60 * 1000)
        expect(doneB.status).toBe('done')

        // --- Wait for C to complete ---
        const doneC = await waitForStatus(request, taskC.id, ['done'], 10 * 60 * 1000)
        expect(doneC.status).toBe('done')

        // --- Verify ordering via transition logs ---
        // B must not have entered an active state before A reached done
        const aDoneAt = await firstEntryIntoState(request, taskA.id, 'done')
        const bActiveAt = await firstEntryIntoState(request, taskB.id, 'in_progress')
        const bDoneAt = await firstEntryIntoState(request, taskB.id, 'done')
        const cActiveAt = await firstEntryIntoState(request, taskC.id, 'in_progress')

        expect(aDoneAt).toBeTruthy()
        expect(bActiveAt).toBeTruthy()
        expect(bDoneAt).toBeTruthy()
        expect(cActiveAt).toBeTruthy()

        // B went active only after (or at the same instant as) A went done
        expect(bActiveAt! >= aDoneAt!).toBe(true)
        // C went active only after (or at the same instant as) B went done
        expect(cActiveAt! >= bDoneAt!).toBe(true)

        // --- Verify all three output files exist on disk ---
        const [fileA, fileB, fileC] = await Promise.all([
          readFile(join(fixturePath, `src/phase-a-${runId}.txt`), 'utf8').catch(() => null),
          readFile(join(fixturePath, `src/phase-b-${runId}.txt`), 'utf8').catch(() => null),
          readFile(join(fixturePath, `src/phase-c-${runId}.txt`), 'utf8').catch(() => null),
        ])
        expect(fileA?.trim()).toBe(`phase-a-${runId}`)
        expect(fileB?.trim()).toBe(`phase-b-${runId}`)
        expect(fileC?.trim()).toBe(`phase-c-${runId}`)
      } finally {
        for (const taskId of [...createdTaskIds].reverse()) {
          await cleanupTask(request, taskId)
        }
        for (const agentId of [...createdAgentIds].reverse()) {
          await request.delete(`/api/v1/agents/${agentId}`, { failOnStatusCode: false })
        }
        await rm(fixturePath, { recursive: true, force: true })
      }
    },
  )
})
