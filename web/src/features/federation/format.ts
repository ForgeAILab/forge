import type { AgentProviderCapability, ProviderEntryResponse, ProviderRuntimeCapability } from '@/types/generated'

/**
 * Tool ceiling applied to embedded agents/profiles created from this page.
 *
 * This is the outermost bound on what an agent may ever be granted, not an
 * effective grant: the server intersects it with the scope catalog and the
 * per-Project binding ceiling before every turn. Omitting an entry here
 * therefore removes the capability everywhere — an agent without
 * `propose_project` can never adopt a Charter or touch Project orchestration,
 * no matter how its bindings are configured.
 */
export const DEFAULT_CEILING = {
  allowed: [
    'read_account',
    'read_project',
    'read_agent_chat',
    'read_task',
    'read_memory',
    'propose_task',
    'propose_discovery',
    'propose_project',
    'propose_handoff',
    'propose_message',
    'propose_review',
    'propose_commitment',
    'propose_memory',
    'propose_decision',
    'propose_session',
    'task_read',
    'task_write',
  ],
}

export const runtimeDisplayNames: Record<string, string> = {
  direct: 'Direct · built-in runtime',
  codex: 'Codex CLI harness',
  claude_code: 'Claude Code harness',
  cursor: 'Cursor harness',
  gemini: 'Gemini CLI harness',
  opencode: 'OpenCode harness',
}

export function humanize(value: string | null | undefined): string {
  if (!value) return 'Unknown'
  return value.replaceAll('_', ' ').replace(/\b\w/g, (letter) => letter.toUpperCase())
}

export function shortId(value: string | null | undefined): string {
  if (!value) return 'Not recorded'
  return value.length > 18 ? `${value.slice(0, 9)}…${value.slice(-7)}` : value
}

export function allowedPolicyValues(policy: Record<string, unknown> | null | undefined): string[] {
  const allowed = policy?.allowed
  return Array.isArray(allowed)
    ? allowed.filter((value): value is string => typeof value === 'string')
    : []
}

/** The capability-catalog method entry matching a stored provider entry. */
export function catalogMethodForEntry(
  capabilities: AgentProviderCapability[] | undefined,
  entry: ProviderEntryResponse,
) {
  const capability = capabilities?.find((item) => item.provider === entry.provider)
  if (!capability) return undefined
  return entry.credential_method === 'api_key'
    ? capability.credential_methods.find((method) => method.method === 'api_key')
    : capability.credential_methods.find((method) => method.method !== 'api_key')
}

export function runtimeOptionsForEntry(
  capabilities: AgentProviderCapability[] | undefined,
  entry: ProviderEntryResponse,
): ProviderRuntimeCapability[] {
  return (
    catalogMethodForEntry(capabilities, entry)?.runtimes ?? [
      { runtime: 'direct', support_level: 'stable', reason: null },
    ]
  )
}

/**
 * A direct agent runs on the built-in runtime against a provider entry;
 * anything else is a CLI harness (Codex/Claude Code/Cursor/... executors).
 */
export function isDirectAgent(backendKind: string): boolean {
  return backendKind === 'native' || backendKind === 'embedded'
}

/** Coarse relative time to a future instant: "in 42m", "in 1h 30m", "in 3d", "now". */
export function formatResetRelative(resetsAt: string | null | undefined): string {
  if (!resetsAt) return 'unknown'
  const target = new Date(resetsAt).getTime()
  if (Number.isNaN(target)) return 'unknown'
  const diffMs = target - Date.now()
  if (diffMs <= 30_000) return 'now'
  const minutes = Math.round(diffMs / 60_000)
  if (minutes < 60) return `in ${minutes}m`
  const hours = Math.floor(minutes / 60)
  const remainingMinutes = minutes % 60
  if (hours < 24) return remainingMinutes > 0 ? `in ${hours}h ${remainingMinutes}m` : `in ${hours}h`
  const days = Math.floor(hours / 24)
  return `in ${days}d`
}

/** Short label for a usage window, e.g. 300 minutes -> "5h window", else "weekly". */
export function windowLabel(windowMinutes: number | null | undefined): string {
  if (windowMinutes == null) return 'window'
  if (windowMinutes <= 600) {
    const hours = Math.max(1, Math.round(windowMinutes / 60))
    return `${hours}h window`
  }
  return 'weekly'
}

/** The generated bindings model u64 fields as bigint; the UI stays numeric. */
export function numberValue(value: number | bigint | null | undefined, fallback: number): number {
  if (value === null || value === undefined) return fallback
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : fallback
}

/** Tool/permission ceiling applied to a fresh Project Agent binding. */
export const DEFAULT_PROJECT_PERMISSION_CEILING = {
  allowed: [
    'read_project',
    'read_agent_chat',
    'read_task',
    'read_memory',
    'propose_task',
    // Charter adoption on a Project that predates an approved Charter runs
    // through the typed Project orchestration surface, which this gates.
    'propose_project',
    'propose_message',
    'propose_review',
    'propose_commitment',
    'propose_memory',
    'propose_decision',
    'propose_session',
  ],
}
