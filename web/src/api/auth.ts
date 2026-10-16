import type {
  AuthResponse,
  LoginRequest,
  LogoutRequest,
  RegisterRequest,
  UserResponse,
} from '@/types/generated'

async function authFetch<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await fetch(`/api/v1${path}`, {
    ...init,
    headers: {
      'content-type': 'application/json',
      ...(init?.headers as Record<string, string> | undefined),
    },
  })
  if (!response.ok) {
    const text = await response.text()
    let message = response.statusText
    try {
      const json = JSON.parse(text) as { message?: string }
      if (json.message) message = json.message
    } catch {
      if (text) message = text
    }
    throw new Error(message)
  }
  const text = await response.text()
  if (response.status === 204 || text.length === 0) return undefined as T
  return JSON.parse(text) as T
}

export function register(body: RegisterRequest): Promise<AuthResponse> {
  return authFetch<AuthResponse>('/auth/register', {
    method: 'POST',
    body: JSON.stringify(body),
  })
}

export function login(body: LoginRequest): Promise<AuthResponse> {
  return authFetch<AuthResponse>('/auth/login', {
    method: 'POST',
    body: JSON.stringify(body),
  })
}

export function logoutApi(body: LogoutRequest): Promise<void> {
  return authFetch<void>('/auth/logout', {
    method: 'POST',
    body: JSON.stringify(body),
  })
}

export function getMe(token?: string): Promise<UserResponse> {
  return authFetch<UserResponse>('/auth/me', {
    headers: token ? { authorization: `Bearer ${token}` } : undefined,
  })
}
