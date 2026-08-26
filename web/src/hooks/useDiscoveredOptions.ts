import { useQuery } from '@tanstack/react-query'
import { apiFetch } from '@/api/client'
import { qk } from '@/api/query-keys'

export type DiscoveredModelOption = {
  id: string
  displayName: string
  provider: string | null
  reasoningOptions: string[]
}

export type ReasoningOption = {
  id: string
  label: string
}

export type DiscoveredExecutionOptions = {
  models: DiscoveredModelOption[]
  reasoningOptions: ReasoningOption[]
  permissionPolicies: string[]
  modelConfigs: Record<string, Record<string, string>>
}

type AgentDiscoveredOptionsResponse = {
  models?: Array<{ id: string; name?: string; reasoning_options?: string[] } | string>
  permission_policies?: string[]
  cli_specific?: unknown
}

function titleCase(value: string): string {
  if (value === 'xhigh') return 'XHigh'
  return value
    .split(/[-_\s]+/)
    .filter(Boolean)
    .map((part) => part.charAt(0).toUpperCase() + part.slice(1))
    .join(' ')
}

function providerForModel(modelId: string): string | null {
  const lower = modelId.toLowerCase()
  if (lower.includes('claude')) return 'Anthropic'
  if (lower.includes('gpt') || lower.includes('codex') || lower.includes('o3')) return 'OpenAI'
  return null
}

function uniqueStrings(values: unknown): string[] {
  if (!Array.isArray(values)) return []
  return Array.from(
    new Set(values.filter((value): value is string => typeof value === 'string' && Boolean(value))),
  )
}

function reasoningFromCliSpecific(cliSpecific: unknown, hasModels: boolean): string[] {
  if (cliSpecific && typeof cliSpecific === 'object') {
    const record = cliSpecific as Record<string, unknown>
    for (const key of ['reasoning_efforts', 'reasoning_options', 'model_reasoning_efforts']) {
      const values = uniqueStrings(record[key])
      if (values.length > 0) return values
    }
  }
  return hasModels ? ['low', 'medium', 'high'] : []
}

function reasoningByModelFromCliSpecific(cliSpecific: unknown): Map<string, string[]> {
  const result = new Map<string, string[]>()
  if (!cliSpecific || typeof cliSpecific !== 'object') return result

  const raw = (cliSpecific as Record<string, unknown>).model_reasoning_efforts
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return result

  for (const [modelId, efforts] of Object.entries(raw as Record<string, unknown>)) {
    if (Array.isArray(efforts)) result.set(modelId, uniqueStrings(efforts))
  }
  return result
}

function modelConfigsFromCliSpecific(
  cliSpecific: unknown,
): Record<string, Record<string, string>> {
  if (!cliSpecific || typeof cliSpecific !== 'object') return {}

  const raw = (cliSpecific as Record<string, unknown>).model_providers
  if (!raw || typeof raw !== 'object' || Array.isArray(raw)) return {}

  return Object.fromEntries(
    Object.entries(raw as Record<string, unknown>).flatMap(([model, provider]) =>
      typeof provider === 'string' && provider ? [[model, { provider }]] : [],
    ),
  )
}

export function normalizeDiscoveredOptions(
  response: AgentDiscoveredOptionsResponse,
): DiscoveredExecutionOptions {
  const allReasoningIds = new Set<string>()
  const models: DiscoveredModelOption[] = []
  const hasModels = Boolean(response.models?.length)
  const sharedReasoningIds = reasoningFromCliSpecific(response.cli_specific, hasModels)
  const reasoningByModel = reasoningByModelFromCliSpecific(response.cli_specific)

  sharedReasoningIds.forEach((id) => allReasoningIds.add(id))

  if (Array.isArray(response.models)) {
    for (const entry of response.models) {
      if (typeof entry === 'string') {
        const reasoningOptions = reasoningByModel.get(entry) ?? sharedReasoningIds
        reasoningOptions.forEach((id) => allReasoningIds.add(id))
        models.push({
          id: entry,
          displayName: entry,
          provider: providerForModel(entry),
          reasoningOptions,
        })
      } else if (entry && typeof entry === 'object') {
        const id = entry.id ?? ''
        const reasoningOptions = Array.isArray(entry.reasoning_options)
          ? uniqueStrings(entry.reasoning_options)
          : (reasoningByModel.get(id) ?? sharedReasoningIds)
        reasoningOptions.forEach((r) => allReasoningIds.add(r))
        models.push({
          id,
          displayName: entry.name ?? id,
          provider: providerForModel(id),
          reasoningOptions,
        })
      }
    }
  }

  const reasoningIds = allReasoningIds.size > 0 ? Array.from(allReasoningIds) : sharedReasoningIds

  const policies = uniqueStrings(response.permission_policies)

  return {
    models,
    reasoningOptions: reasoningIds.map((id) => ({ id, label: titleCase(id) })),
    permissionPolicies: policies.length > 0 ? policies : ['auto', 'supervised', 'plan'],
    modelConfigs: modelConfigsFromCliSpecific(response.cli_specific),
  }
}

export function getReasoningOptionsForModel(
  options: DiscoveredExecutionOptions | undefined,
  modelId: string | null | undefined,
): ReasoningOption[] {
  if (!options || !modelId) return options?.reasoningOptions ?? []
  const model = options.models.find((entry) => entry.id === modelId)
  if (!model) return options.reasoningOptions
  return model.reasoningOptions.map((id) => ({ id, label: titleCase(id) }))
}

export function useDiscoveredOptions(
  agentId: string | null | undefined,
  executorType?: string | null,
) {
  const hasAgentId = Boolean(agentId)
  const hasExecutorType = Boolean(executorType)

  return useQuery({
    queryKey: hasAgentId
      ? qk.agentDiscoveredOptions(agentId ?? '')
      : qk.executorDiscoveredOptions(executorType ?? ''),
    queryFn: async () => {
      const path = hasAgentId
        ? `/agents/${agentId}/discovered-options`
        : `/executor-types/${executorType}/discovered-options`
      const response = await apiFetch<AgentDiscoveredOptionsResponse>(path)
      return normalizeDiscoveredOptions(response)
    },
    enabled: hasAgentId || hasExecutorType,
    staleTime: 5 * 60 * 1000,
  })
}
