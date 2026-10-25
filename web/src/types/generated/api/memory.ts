// Memory search, publication, and context manifest types.

export interface PromptPreviewResponse {
  system: string
  user: string
  tools: string[] | null
}

export interface MemorySearchQuery {
  query: string
  layer?: number | null
  token_budget?: number | null
  limit?: number | null
  cursor?: string | null
}

export interface MemoryGetQuery {
  layer?: number | null
  project_id?: string | null
}

export interface MemorySearchResultDto {
  id: string
  layer: number
  content: string
  score: number
  source_type: string
  source_id: string
  project_id: string
  task_id: string | null
  created_at: string
  creator: string | null
}

export interface MemorySearchResponse {
  items: MemorySearchResultDto[]
  has_more: boolean
  next_cursor: string | null
}

export interface MemoryPublicationRequest {
  source_scope_type: string
  source_scope_id: string
  target_scope_type: string
  target_scope_id: string
  target_project_id: string | null
  target_task_id: string | null
  target_chat_id: string | null
  target_visibility: string
  target_authority: string
  actor_identity_id: string
  reason: string
  evidence_json: string
}

export interface MemoryLifecycleRequest {
  scope_type: string
  scope_id: string
  assertion_type: string
  related_memory_id: string | null
  reason: string | null
  evidence_json: string
  actor_identity_id: string
}

export interface MemoryLifecycleResponse {
  id: string
  memory_item_id: string
  assertion_type: string
  related_memory_id: string | null
  reason: string | null
  evidence_present: boolean
  asserted_by_type: string
  asserted_by_id: string | null
  source_event_id: string | null
  created_at: string
}

export interface MemoryProvenanceResponse {
  id: string
  scope_type: string
  scope_id: string
  visibility: string
  owner_identity_id: string | null
  authority: string
  sensitivity: string
  retention_priority: number
  source_type: string
  source_ref: string | null
  source_event_id: string | null
  source_scope_type: string | null
  source_scope_id: string | null
  source_revision: string | null
  source_chat_sequence: number | null
  publication_source_id: string | null
  supersedes_id: string | null
  valid_from: string | null
  valid_until: string | null
  created_by_type: string | null
  created_by_id: string | null
  created_at: string
  lifecycle: MemoryLifecycleResponse[]
}

export interface ContextManifestQuery {
  identity_id: string
  context_scope_id: string
}

export interface ContextManifestListQuery {
  context_scope_id: string | null
  limit: number | null
}

export interface MemoryProvenanceQuery {
  scope_type: string
  scope_id: string
  identity_id: string
}

export interface ContextManifestSourceResponse {
  ordinal: number
  source_id: string
  source_type: string
  source_revision: string
  selection_reason: string
  disposition: string
  is_stale: boolean
  current_revision: string | null
  retention_priority: number
  fragment_fingerprint: string
}

export interface ContextManifestResponse {
  id: string
  identity_id: string
  agent_session_id: string | null
  context_scope_id: string
  scope_type: string
  scope_id: string
  policy_revision: string
  domain_revision: string
  lcm_binding_revision: string | null
  runtime_manifest_id: string | null
  runtime_manifest_fingerprint: string | null
  combined_fingerprint: string
  request_fingerprint: string
  created_at: string
  sources: ContextManifestSourceResponse[]
}

export interface ContextManifestListResponse {
  items: ContextManifestResponse[]
  has_more: boolean
}
