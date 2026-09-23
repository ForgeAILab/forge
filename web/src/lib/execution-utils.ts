import type { Execution } from '@/types/generated'

export function isResumeExecution(execution: Execution): boolean {
  const snapshot = execution.executor_config_snapshot
  if (!snapshot) return false
  const dispatch = snapshot.dispatch as Record<string, unknown> | undefined
  if (dispatch?.execution_policy === 'resume_latest_target_role_thread') return true

  // Check dispatch metadata set by executor_snapshot_with_resume_thread.
  const meta = snapshot.dispatch_metadata as Record<string, unknown> | undefined
  if (meta?.execution_policy === 'resume_latest_target_role_thread') return true

  // Fallback: check executor-specific resume config fields
  const config = snapshot.config as Record<string, unknown> | undefined
  return config?.resume_thread_in_place === true || typeof config?.resume_session_id === 'string'
}

export function roleDisplayName(role: string): string {
  const names: Record<string, string> = {
    executor: 'Executor',
    coder: 'Coder',
    planner: 'Planner',
    reviewer: 'Reviewer',
    auditor: 'Auditor',
    merge_fixer: 'Merge Fixer',
    interactive: 'Interactive',
  }
  return names[role] ?? role.charAt(0).toUpperCase() + role.slice(1).replace(/_/g, ' ')
}

export function turnLabel(index: number, execution: Execution): string {
  if (index === 0) return 'Initial run'
  if (isResumeExecution(execution)) return 'Follow-up turn'
  return 'Re-execution'
}

export type ExecutionChain = { root: Execution; turns: Execution[] }

/**
 * Whether `execution` is a later turn of `parent`'s session. A parent link
 * alone does not say so: a reviewer run points at the candidate run it
 * reviewed, which is a different role and agent with its own session.
 */
export function continuesParentSession(
  execution: Pick<Execution, 'role' | 'agent_id'>,
  parent: Pick<Execution, 'role' | 'agent_id'>,
): boolean {
  return execution.role === parent.role && execution.agent_id === parent.agent_id
}

export function buildExecutionChains(executions: Execution[]): ExecutionChain[] {
  const byId = new Map(executions.map((e) => [e.id, e]))
  const childrenOf = new Map<string, Execution[]>()

  const sessionParentOf = (e: Execution): Execution | undefined => {
    const parent = e.parent_execution_id ? byId.get(e.parent_execution_id) : undefined
    return parent && continuesParentSession(e, parent) ? parent : undefined
  }

  for (const e of executions) {
    const parent = sessionParentOf(e)
    if (parent) {
      const arr = childrenOf.get(parent.id) ?? []
      arr.push(e)
      childrenOf.set(parent.id, arr)
    }
  }

  // Roots: no parent in this list, or a parent from another session
  const roots = executions.filter((e) => !sessionParentOf(e))
  roots.sort((a, b) => new Date(b.created_at).getTime() - new Date(a.created_at).getTime())

  const chains: ExecutionChain[] = []
  for (const root of roots) {
    const turns: Execution[] = []
    const queue: Execution[] = [root]
    const seen = new Set<string>()
    while (queue.length > 0) {
      const curr = queue.shift()!
      if (seen.has(curr.id)) continue
      seen.add(curr.id)
      turns.push(curr)
      const children = (childrenOf.get(curr.id) ?? []).sort(
        (a, b) => new Date(a.created_at).getTime() - new Date(b.created_at).getTime(),
      )
      queue.push(...children)
    }
    chains.push({ root, turns })
  }

  return chains.sort((a, b) => {
    const aLatest = a.turns[a.turns.length - 1]
    const bLatest = b.turns[b.turns.length - 1]
    if (aLatest.status === 'running' && bLatest.status !== 'running') return -1
    if (bLatest.status === 'running' && aLatest.status !== 'running') return 1
    return new Date(bLatest.created_at).getTime() - new Date(aLatest.created_at).getTime()
  })
}
