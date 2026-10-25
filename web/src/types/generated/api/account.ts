// Authentication, account, membership, and notification types.

export interface NotificationResponse {
  id: string
  project_id: string
  task_id: string | null
  event_type: string
  title: string
  body: string | null
  read: boolean
  created_at: string
}

export interface UnreadCountResponse {
  count: number
}

export interface RegisterRequest {
  email: string
  password: string
  display_name: string | null
}

export interface LoginRequest {
  email: string
  password: string
}

export interface RefreshRequest {
  refresh_token?: string
}

export interface LogoutRequest {
  refresh_token?: string
}

export interface AuthResponse {
  access_token: string
  refresh_token: string
  token_type: string
  expires_in: number
}

export interface UserResponse {
  id: string
  email: string
  display_name: string | null
  is_admin: boolean
  created_at: string
}

export interface UpdateProfileRequest {
  email?: string | null
  display_name?: string | null
}

export type MemberRole = 'owner' | 'admin' | 'member' | 'viewer'

export interface TokenResponse {
  id: string
  name: string
  token?: string
  prefix: string
  scopes: string
  expires_at: string | null
  last_used_at: string | null
  created_at: string
}

export interface ProjectMemberResponse {
  id: string
  user_id: string
  email: string
  display_name: string | null
  role: MemberRole
  created_at: string
}

export interface UserSearchResult {
  id: string
  email: string
  display_name: string | null
}
