// Account, authorization, analytics, notification, and membership hooks.

import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  getAccountUsageAnalytics,
  apiFetch,
  addMember,
  searchUsers,
  createPat,
  deleteNotification,
  deletePat,
  getUnreadNotificationCount,
  listMembers,
  listNotifications,
  listPats,
  listProjectAgents,
  markAllNotificationsRead,
  markNotificationRead,
  removeMember,
  updateMe,
  updateMemberRole,
} from '@/api/client'
import { qk } from '@/api/query-keys'
import { useAuthStore } from '@/stores/auth'
import type {
  OAuthApproveRequest,
  OAuthApproveResponse,
  OAuthAuthorizeContext,
} from '@/types/generated'

function searchParamsString(query: Partial<Record<string, string>>): string {
  const params = new URLSearchParams()
  for (const [key, value] of Object.entries(query)) {
    if (value !== undefined && value !== '') {
      params.set(key, value)
    }
  }
  return params.toString()
}

// --- OAuth ---

export function useOAuthAuthorizeContext(query: Partial<Record<string, string>>) {
  const queryString = searchParamsString(query)
  return useQuery({
    queryKey: ['oauth-authorize-context', JSON.stringify(query)],
    queryFn: () =>
      apiFetch<OAuthAuthorizeContext>(
        `/oauth/authorize/context${queryString ? `?${queryString}` : ''}`,
      ),
    enabled: Boolean(query.client_id && query.redirect_uri && query.response_type),
  })
}

export function useOAuthApprove() {
  return useMutation({
    mutationFn: (input: OAuthApproveRequest) =>
      apiFetch<OAuthApproveResponse>('/oauth/authorize/approve', {
        method: 'POST',
        body: JSON.stringify(input),
      }),
  })
}

export function useAccountUsageAnalytics(from?: string, to?: string) {
  return useQuery({
    queryKey: qk.accountUsageAnalytics(from, to),
    queryFn: () => getAccountUsageAnalytics(from, to),
  })
}

// --- Notifications ---

export function useNotificationsQuery(projectId?: string, read?: boolean) {
  return useQuery({
    queryKey: qk.notifications(projectId, read),
    queryFn: () => listNotifications({ project_id: projectId, read, limit: 20 }),
  })
}

export function useUnreadNotificationsCountQuery(projectId?: string) {
  return useQuery({
    queryKey: qk.notificationUnreadCount(projectId),
    queryFn: () => getUnreadNotificationCount(projectId),
  })
}

export function useMarkNotificationRead() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (notificationId: string) => markNotificationRead(notificationId),
    onSuccess: (notification) => {
      void queryClient.invalidateQueries({
        predicate: (query) =>
          String(query.queryKey[0]) === 'notifications' &&
          String(query.queryKey[1] ?? '') === notification.project_id,
      })
      void queryClient.invalidateQueries({
        queryKey: qk.notificationUnreadCount(notification.project_id),
      })
      void queryClient.invalidateQueries({ queryKey: qk.notificationUnreadCount() })
    },
  })
}

export function useMarkAllNotificationsRead() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (projectId?: string) => markAllNotificationsRead(projectId),
    onSuccess: (_, projectId) => {
      void queryClient.invalidateQueries({
        predicate: (query) =>
          String(query.queryKey[0]) === 'notifications' &&
          String(query.queryKey[1] ?? '') === (projectId ?? 'all'),
      })
      void queryClient.invalidateQueries({ queryKey: qk.notificationUnreadCount(projectId) })
      void queryClient.invalidateQueries({ queryKey: qk.notificationUnreadCount() })
    },
  })
}

export function useDeleteNotification() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ notificationId }: { notificationId: string }) =>
      deleteNotification(notificationId),
    onSuccess: () => {
      void queryClient.invalidateQueries({
        predicate: (query) => String(query.queryKey[0]) === 'notifications',
      })
    },
  })
}

// --- User Profile ---

export function useUpdateMe() {
  return useMutation({
    mutationFn: (body: { email?: string | null; display_name?: string | null }) => updateMe(body),
    onSuccess: (user) => useAuthStore.getState().updateUser(user),
  })
}

// --- Personal Access Tokens ---

export function usePatsQuery() {
  return useQuery({
    queryKey: qk.pats(),
    queryFn: () => listPats(),
  })
}

export function useCreatePat() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: { name: string; expires_at?: string | null }) => createPat(body),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.pats() })
    },
  })
}

export function useDeletePat() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (id: string) => deletePat(id),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.pats() })
    },
  })
}

// --- User Search ---

export function useUserSearch(query: string) {
  return useQuery({
    queryKey: ['users', 'search', query],
    queryFn: () => searchUsers(query),
    enabled: query.trim().length >= 2,
    staleTime: 30_000,
  })
}

// --- Project Members ---

export function useMembersQuery(projectId: string) {
  return useQuery({
    queryKey: qk.projectMembers(projectId),
    queryFn: ({ signal }) => listMembers(projectId, { signal }),
    enabled: Boolean(projectId),
  })
}

export function useProjectAgentsQuery(projectId: string) {
  return useQuery({
    queryKey: qk.projectAgents(projectId),
    queryFn: ({ signal }) => listProjectAgents(projectId, { signal }),
    enabled: Boolean(projectId),
  })
}

export function useAddMember(projectId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (body: { user_id: string; role: string }) => addMember(projectId, body),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.projectMembers(projectId) })
    },
  })
}

export function useUpdateMemberRole(projectId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ userId, body }: { userId: string; body: { role: string } }) =>
      updateMemberRole(projectId, userId, body),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.projectMembers(projectId) })
    },
  })
}

export function useRemoveMember(projectId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (userId: string) => removeMember(projectId, userId),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.projectMembers(projectId) })
    },
  })
}
