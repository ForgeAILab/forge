// Repository configuration and filesystem types.

// --- Repo (matches api_types::RepoResponse) ---

export interface Repo {
  id: string
  project_id: string
  name: string
  local_path: string | null
  remote_url: string | null
  default_branch: string
  created_at: string
  updated_at: string
}

export interface RepoSyncResponse {
  pull_output: string
  push_output: string
}

// --- Filesystem (matches api_types::{FsEntry, FsListResponse, BranchListResponse}) ---

export interface FsEntry {
  name: string
  path: string
  is_dir: boolean
  is_git_repo: boolean
}

export interface FsListResponse {
  path: string
  entries: FsEntry[]
}

export interface BranchListResponse {
  branches: string[]
  default_branch: string | null
  origin_url: string | null
}

export interface CreateRepoRequest {
  remote_url?: string | null
  local_path?: string | null
  name?: string | null
  default_branch?: string | null
}

export interface UpdateRepoRequest {
  name?: string | null
  local_path?: string | null
  remote_url?: string | null
  default_branch?: string | null
}
