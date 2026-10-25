// Forge settings and provider usage types.

export interface SettingsResponse {
  config_path: string
  restart_required: boolean
  settings: ForgeSettingResponse[]
}

export interface ForgeSettingResponse {
  key: string
  value: unknown | null
  effective_value: unknown
  restart_required: boolean
}

export interface UpdateSettingsRequest {
  forge?: UpdateForgePathsRequest
  server?: UpdateServerSettingsRequest
  workspace?: UpdateWorkspaceSettingsRequest
  agent?: UpdateAgentSettingsRequest
  project?: Record<string, string>
}

export interface UpdateForgePathsRequest {
  data_dir?: string | null
}

export interface UpdateServerSettingsRequest {
  build_jobs_per_run?: number | null
  run_nice?: number | null
  usage_index_budget_mb?: number | null
  max_concurrent_runs?: number | null
  bind?: string | null
  mcp_enabled?: boolean | null
}

export interface UpdateWorkspaceSettingsRequest {
  root?: string | null
  cleanup_delay_seconds?: number | null
}

export interface UpdateAgentSettingsRequest {
  max_concurrent_tasks?: number | null
  heartbeat_interval_seconds?: number | null
  max_missed_heartbeats?: number | null
}

export interface ProviderUsageWindow {
  id: string
  used_percent: number
  window_minutes: number | null
  resets_at: string | null
}

export interface ProviderUsageResponse {
  id: string
  provider: string
  source: 'probe' | 'unknown'
  plan_type?: string | null
  windows: ProviderUsageWindow[]
  fetched_at: string
  detail: string | null
}
