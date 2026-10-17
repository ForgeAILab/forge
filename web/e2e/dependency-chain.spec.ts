import { mkdtemp, writeFile } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { execFile } from 'node:child_process'
import { promisify } from 'node:util'
import { expect, test, type APIRequestContext, type APIResponse } from './fixtures'

const execFileAsync = promisify(execFile)

type ProjectResponse = {
  id: string
  name: string
}

type RepoResponse = {
  id: string
  project_id: string
}

type TaskResponse = {
  id: string
  project_id: string
  title: string
  status: string
  version: number
}

type DependencyResponse = {
  task_id: string
  depends_on_id: string
  created_at: string
}

const runId = `dep-chain-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`

async function expectOk(response: APIResponse, label: string) {
  if (response.ok()) return
  throw new Error(`${label} failed with ${response.status()}: ${await response.text()}`)
}

async function api<T>(
  request: APIRequestContext,
  method: 'GET' | 'POST' | 'PATCH' | 'DELETE',
  path: string,
  data?: unknown,
): Promise<T> {
  const response = await request.fetch(path, { method, data, failOnStatusCode: false })
  await expectOk(response, `${method} ${path}`)
  const body = await response.text()
  if (!body) return undefined as T
  return JSON.parse(body) as T
}

async function tryTransition(
  request: APIRequestContext,
  taskId: string,
  status: string,
  version: number,
): Promise<number> {
  const response = await request.post(`/api/v1/tasks/${taskId}/transition`, {
    data: { status, version },
    failOnStatusCode: false,
  })
  return response.status()
}

async function createBareGitRepo(): Promise<string> {
  const root = await mkdtemp(join(tmpdir(), `forge-dep-${runId}-`))
  await writeFile(join(root, 'README.md'), '# Dependency chain fixture\n')
  await execFileAsync('git', ['init'], { cwd: root })
  await execFileAsync('git', ['checkout', '-B', 'main'], { cwd: root })
  await execFileAsync('git', ['config', 'user.email', 'forge-dep-test@example.test'], { cwd: root })
  await execFileAsync('git', ['config', 'user.name', 'Forge Dep Test'], { cwd: root })
  await execFileAsync('git', ['add', '.'], { cwd: root })
  await execFileAsync('git', ['commit', '-m', 'Initial commit'], { cwd: root })
  return root
}

test.describe('task dependency chain (integration)', () => {
  test(
    'A→B→C chain: dependency gate blocks transitions until prerequisites are done',
    async ({ request }) => {
      test.setTimeout(2 * 60 * 1000)

      const createdTaskIds: string[] = []
      let projectId: string | null = null
      const repoPath = await createBareGitRepo()

      try {
        const serverCheck = await request.get('/api/v1/projects', { failOnStatusCode: false })
        await expectOk(
          serverCheck,
          'GET /api/v1/projects. Start the Forge API server on localhost:8080 before running this integration test',
        )

        const project = await api<ProjectResponse>(request, 'POST', '/api/v1/projects', {
          name: `Dependency Chain ${runId}`,
          settings: {},
        })
        projectId = project.id

        await api<RepoResponse>(request, 'POST', `/api/v1/projects/${project.id}/repos`, {
          name: `dep-chain-repo-${runId}`,
          remote_url: repoPath,
          local_path: repoPath,
          default_branch: 'main',
        })

        // --- Create tasks A, B, C (all start in todo) ---
        const taskA = await api<TaskResponse>(
          request,
          'POST',
          `/api/v1/projects/${project.id}/tasks`,
          { title: `Task A – no deps ${runId}` },
        )
        createdTaskIds.push(taskA.id)

        const taskB = await api<TaskResponse>(
          request,
          'POST',
          `/api/v1/projects/${project.id}/tasks`,
          { title: `Task B – depends on A ${runId}` },
        )
        createdTaskIds.push(taskB.id)

        const taskC = await api<TaskResponse>(
          request,
          'POST',
          `/api/v1/projects/${project.id}/tasks`,
          { title: `Task C – depends on B ${runId}` },
        )
        createdTaskIds.push(taskC.id)

        // --- Wire up dependency chain: B→A, C→B ---
        await api(request, 'POST', `/api/v1/tasks/${taskB.id}/dependencies`, {
          depends_on_id: taskA.id,
        })
        await api(request, 'POST', `/api/v1/tasks/${taskC.id}/dependencies`, {
          depends_on_id: taskB.id,
        })

        // --- Verify dependency list for B and C ---
        const bDeps = await api<DependencyResponse[]>(
          request,
          'GET',
          `/api/v1/tasks/${taskB.id}/dependencies`,
        )
        expect(bDeps).toHaveLength(1)
        expect(bDeps[0].depends_on_id).toBe(taskA.id)

        const cDeps = await api<DependencyResponse[]>(
          request,
          'GET',
          `/api/v1/tasks/${taskC.id}/dependencies`,
        )
        expect(cDeps).toHaveLength(1)
        expect(cDeps[0].depends_on_id).toBe(taskB.id)

        // --- Verify dependents list for A and B ---
        const aDependents = await api<DependencyResponse[]>(
          request,
          'GET',
          `/api/v1/tasks/${taskA.id}/dependents`,
        )
        expect(aDependents.map((d) => d.task_id)).toContain(taskB.id)

        const bDependents = await api<DependencyResponse[]>(
          request,
          'GET',
          `/api/v1/tasks/${taskB.id}/dependents`,
        )
        expect(bDependents.map((d) => d.task_id)).toContain(taskC.id)

        // --- Cycle detection: adding A→C would close the loop A→B→C→A ---
        const cyclicResponse = await request.post(`/api/v1/tasks/${taskA.id}/dependencies`, {
          data: { depends_on_id: taskC.id },
          failOnStatusCode: false,
        })
        expect(cyclicResponse.status()).toBe(422)

        // --- Self-dependency is also rejected ---
        const selfDepResponse = await request.post(`/api/v1/tasks/${taskA.id}/dependencies`, {
          data: { depends_on_id: taskA.id },
          failOnStatusCode: false,
        })
        expect(selfDepResponse.status()).toBe(422)

        // --- Gate: B is blocked because A is not done ---
        expect(await tryTransition(request, taskB.id, 'planning', taskB.version)).toBe(412)

        // --- Gate: C is blocked because B is not done ---
        expect(await tryTransition(request, taskC.id, 'planning', taskC.version)).toBe(412)

        // --- A has no dependencies so it can leave todo freely ---
        const aTransitionStatus = await tryTransition(
          request,
          taskA.id,
          'planning',
          taskA.version,
        )
        expect([200, 201]).toContain(aTransitionStatus)

        // --- Remove B's dependency on A (simulates A reaching done for gate purposes) ---
        await api(request, 'DELETE', `/api/v1/tasks/${taskB.id}/dependencies/${taskA.id}`)

        const freshB = await api<TaskResponse>(request, 'GET', `/api/v1/tasks/${taskB.id}`)
        expect(await tryTransition(request, taskB.id, 'planning', freshB.version)).toBe(200)

        // --- C still has its dependency on B (B is planning, not done) ---
        const freshC = await api<TaskResponse>(request, 'GET', `/api/v1/tasks/${taskC.id}`)
        expect(await tryTransition(request, taskC.id, 'planning', freshC.version)).toBe(412)

        // --- Remove C's dependency on B ---
        await api(request, 'DELETE', `/api/v1/tasks/${taskC.id}/dependencies/${taskB.id}`)

        const freshC2 = await api<TaskResponse>(request, 'GET', `/api/v1/tasks/${taskC.id}`)
        expect(await tryTransition(request, taskC.id, 'planning', freshC2.version)).toBe(200)
      } finally {
        for (const taskId of createdTaskIds.reverse()) {
          await request.delete(`/api/v1/tasks/${taskId}`, { failOnStatusCode: false })
        }
        if (projectId) {
          await request.delete(`/api/v1/projects/${projectId}`, { failOnStatusCode: false })
        }
      }
    },
  )
})
