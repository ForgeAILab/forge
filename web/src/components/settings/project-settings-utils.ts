import { ApiError } from '@/api/client'
import { getApiErrorMessage } from '@/lib/api-error'
import { productTerm } from '@/lib/i18n'
import type { AssigneeSelection } from '@/components/task-controls'
import type { LifecycleEvent, LifecycleHookDef, LifecycleHooks, Repo } from '@/types/generated'
import type { RepoFormState } from '@/components/settings/RepoForm'

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

export function ciStepsFromReviewConfig(reviewConfig: unknown): string[] {
  if (!isRecord(reviewConfig) || !Array.isArray(reviewConfig.ci_steps)) return []
  return reviewConfig.ci_steps
    .filter((step): step is string => typeof step === 'string')
    .map((step) => step.trim())
    .filter((step) => step.length > 0)
}

export const LIFECYCLE_EVENTS: Array<{
  key: LifecycleEvent
  label: string
  description: string
}> = [
  {
    key: 'before_work',
    label: 'Before Work',
    description: `Fires when a task is about to enter an active ${productTerm('phase').toLowerCase()}`,
  },
  {
    key: 'on_work_start',
    label: 'On Work Start',
    description: 'Fires after an agent has been dispatched',
  },
  {
    key: 'on_work_stop',
    label: 'On Work Stop',
    description: 'Fires when an agent stops without completing',
  },
  {
    key: 'on_task_done',
    label: 'On Task Done',
    description: 'Fires when a task completes successfully',
  },
  {
    key: 'on_task_cancel',
    label: 'On Task Cancel',
    description: 'Fires when a task is cancelled',
  },
]

export const BUILTIN_PLUGINS: Array<{
  name: string
  label: string
  description: string
  supportedEvents: LifecycleEvent[]
}> = [
  {
    name: 'knowledge-inject',
    label: 'Knowledge Inject',
    description: 'Injects relevant knowledge base entries into agent context',
    supportedEvents: ['before_work'],
  },
  {
    name: 'knowledge-capture',
    label: 'Knowledge Capture',
    description: 'Captures knowledge from completed tasks',
    supportedEvents: ['on_task_done'],
  },
]

export function lifecycleHooksFromSettings(
  settings: Record<string, unknown> | null | undefined,
): LifecycleHooks {
  const raw = settings?.lifecycle_hooks
  if (!isRecord(raw)) return {}
  const result: LifecycleHooks = {}
  for (const { key } of LIFECYCLE_EVENTS) {
    const rawHooks = raw[key]
    if (!Array.isArray(rawHooks)) continue
    const hooks: LifecycleHookDef[] = []
    for (const hook of rawHooks) {
      if (!isRecord(hook) || typeof hook.type !== 'string') continue
      if (hook.type === 'script' && typeof hook.command === 'string') {
        const timeout =
          typeof hook.timeout_seconds === 'number' && Number.isFinite(hook.timeout_seconds)
            ? Math.trunc(hook.timeout_seconds)
            : 30
        hooks.push({
          type: 'script',
          command: hook.command,
          timeout_seconds: timeout,
          blocking: hook.blocking === true,
        })
        continue
      }
      if (
        hook.type === 'plugin' &&
        typeof hook.name === 'string' &&
        typeof hook.enabled === 'boolean'
      ) {
        hooks.push({
          type: 'plugin',
          name: hook.name,
          enabled: hook.enabled,
          config: isRecord(hook.config) ? hook.config : null,
        })
      }
    }
    if (hooks.length > 0) result[key] = hooks
  }
  return result
}

export function assigneeSelectionFromValue(value: string | undefined): AssigneeSelection {
  if (!value || value === 'unassigned') return { type: 'unassigned' }
  if (value === 'manual') return { type: 'user', userId: 'manual' }
  if (value.startsWith('user:')) return { type: 'user', userId: value.slice('user:'.length) }
  if (value.startsWith('agent:')) return { type: 'agent', agentId: value.slice('agent:'.length) }
  return { type: 'unassigned' }
}

export function assigneeValueFromSelection(selection: AssigneeSelection): string {
  if (selection.type === 'agent') return `agent:${selection.agentId}`
  if (selection.type === 'user') return selection.userId === 'manual' ? 'manual' : `user:${selection.userId}`
  return 'unassigned'
}

export function settingsErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof ApiError && error.status === 409) return 'Reload and retry'
  return getApiErrorMessage(error, fallback)
}

export function repoFormFromRepo(repo: Repo): RepoFormState {
  return {
    source_mode: repo.local_path ? 'local' : 'remote',
    name: repo.name,
    local_path: repo.local_path ?? '',
    remote_url: repo.remote_url ?? '',
    default_branch: repo.default_branch || 'main',
    work_mode: repo.work_mode,
    pr_provider: repo.pr_provider_status?.provider_type ?? repo.pr_provider ?? 'github',
    pr_base_url: '',
    pr_token: '',
    pr_polling_interval_seconds: String(repo.pr_provider_status?.polling_interval_seconds ?? 60),
  }
}

export function repoSource(repo: Repo): string {
  return repo.remote_url ?? repo.local_path ?? ''
}

export function formatDuration(ms: number | null): string {
  if (ms === null) return '—'
  if (ms < 1000) return `${ms}ms`
  if (ms < 60000) return `${(ms / 1000).toFixed(1)}s`
  return `${Math.floor(ms / 60000)}m ${Math.round((ms % 60000) / 1000)}s`
}

export function formatTokens(n: number): string {
  return n.toLocaleString()
}

export function formatCost(usd: number | null): string {
  if (usd === null) return '—'
  return `$${usd.toFixed(2)}`
}

export function formatRate(rate: number): string {
  return `${(rate * 100).toFixed(1)}%`
}
