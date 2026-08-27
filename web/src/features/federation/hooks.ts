import { useEffect } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { ApiError } from '@/api/client'
import {
  cancelAgentSessionTurn,
  cancelProviderAuthorization,
  connectEmbeddedProfile,
  createAgentSession,
  createEmbeddedAgent,
  createProviderEntry,
  getEffectivePermissions,
  getContextManifest,
  getMainAgentBinding,
  getMissionControl,
  getProviderAuthorization,
  listContextManifests,
  listAgentSessions,
  listFederatedAgents,
  listAgentProviderCapabilities,
  listProviders,
  getProjectAgentBinding,
  getProviderUsage,
  registerHarnessAgent,
  removeProviderEntry,
  renameProviderEntry,
  rotateAgentSession,
  setMainAgentBinding,
  setCliRuntimeAvailability,
  setProviderEntryAvailability,
  setProjectAgentBinding,
  setAgentSessionStatus,
  startProviderAuthorization,
  steerAgentSessionTurn,
} from './api'
import type {
  ConnectEmbeddedProfileInput,
  ContextManifestLookup,
  CreateAgentSessionInput,
  CreateEmbeddedAgentInput,
  ProjectAgentBindingInput,
} from './types'
import type {
  CancelProviderAuthorizationRequest,
  CreateProviderEntryRequest,
  RenameProviderEntryRequest,
  SetCliRuntimeAvailabilityRequest,
  SetProviderEntryAvailabilityRequest,
  StartProviderAuthorizationRequest,
} from '@/types/generated'

export const federationQueryKeys = {
  agents: ['federated-agents'] as const,
  profiles: (identityId: string) => ['federated-agents', identityId, 'profiles'] as const,
  sessions: (identityId: string) => ['federated-agents', identityId, 'sessions'] as const,
  credentials: ['federated-agents', 'credentials'] as const,
  mainAgent: ['federated-agents', 'main-agent'] as const,
  missionControl: ['mission-control'] as const,
  contextManifest: (manifestId: string, identityId: string, contextScopeId: string) =>
    ['context-manifests', manifestId, identityId, contextScopeId] as const,
  contextManifestDiscovery: (identityId: string, contextScopeId: string) =>
    ['context-manifests', 'discovery', identityId, contextScopeId] as const,
  projectAgent: (projectId: string) => ['projects', projectId, 'project-agent'] as const,
  providers: ['agent-providers'] as const,
  providerUsage: (id: string) => ['agent-providers', id, 'usage'] as const,
  providerAuthorization: (id: string) => ['provider-authorizations', id] as const,
} as const

export function useFederatedAgentsQuery() {
  return useQuery({
    queryKey: federationQueryKeys.agents,
    queryFn: () => listFederatedAgents(),
    staleTime: 10_000,
  })
}

export function useAgentSessionsQuery(identityId: string | undefined) {
  return useQuery({
    queryKey: federationQueryKeys.sessions(identityId ?? 'none'),
    queryFn: () => listAgentSessions(identityId!),
    enabled: Boolean(identityId),
    staleTime: 5_000,
  })
}

export function useCreateEmbeddedAgentMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (input: CreateEmbeddedAgentInput) => createEmbeddedAgent(input),
    onSuccess: (connected) => {
      queryClient.setQueryData(
        federationQueryKeys.agents,
        (current: { items: unknown[] } | undefined) => {
          if (!current) return current
          return {
            ...current,
            items: [
              connected.agent,
              ...current.items.filter(
                (item) => (item as { id?: string }).id !== connected.agent.id,
              ),
            ],
          }
        },
      )
      queryClient.setQueryData(federationQueryKeys.profiles(connected.agent.id), [
        connected.profile,
      ])
      queryClient.setQueryData(federationQueryKeys.sessions(connected.agent.id), [
        connected.session,
      ])
      void queryClient.invalidateQueries({ queryKey: federationQueryKeys.agents })
    },
  })
}

export function useConnectEmbeddedProfileMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({
      identityId,
      input,
    }: {
      identityId: string
      input: ConnectEmbeddedProfileInput
    }) => connectEmbeddedProfile(identityId, input),
    onSuccess: (connected) => {
      void queryClient.invalidateQueries({ queryKey: federationQueryKeys.agents })
      void queryClient.invalidateQueries({
        queryKey: federationQueryKeys.profiles(connected.agent.id),
      })
      void queryClient.invalidateQueries({ queryKey: federationQueryKeys.credentials })
    },
  })
}

export function useCreateAgentSessionMutation(identityId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (input: CreateAgentSessionInput) => createAgentSession(identityId, input),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: federationQueryKeys.sessions(identityId) })
      void queryClient.invalidateQueries({ queryKey: federationQueryKeys.agents })
    },
  })
}

export function useRotateAgentSessionMutation(identityId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ sessionId, version }: { sessionId: string; version: number }) =>
      rotateAgentSession(sessionId, version),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: federationQueryKeys.sessions(identityId) })
    },
  })
}

export function useSetAgentSessionStatusMutation(identityId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({
      sessionId,
      status,
      version,
    }: {
      sessionId: string
      status: 'suspend' | 'resume'
      version: number
    }) => setAgentSessionStatus(sessionId, status, version),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: federationQueryKeys.sessions(identityId) })
    },
  })
}

export function useCancelAgentSessionMutation(identityId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (sessionId: string) => cancelAgentSessionTurn(sessionId),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: federationQueryKeys.sessions(identityId) })
    },
  })
}

export function useSteerAgentSessionMutation(identityId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ sessionId, content }: { sessionId: string; content: string }) =>
      steerAgentSessionTurn(sessionId, content),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: federationQueryKeys.sessions(identityId) })
    },
  })
}

export function useRegisterHarnessAgentMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (input: Parameters<typeof registerHarnessAgent>[0]) => registerHarnessAgent(input),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: federationQueryKeys.agents })
      void queryClient.invalidateQueries({ queryKey: federationQueryKeys.credentials })
    },
  })
}

export function useProvidersQuery() {
  return useQuery({
    queryKey: federationQueryKeys.credentials,
    queryFn: listProviders,
    staleTime: 15_000,
  })
}

export function useCreateProviderEntryMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (input: CreateProviderEntryRequest) => createProviderEntry(input),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: federationQueryKeys.credentials })
    },
  })
}

export function useRenameProviderEntryMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, input }: { id: string; input: RenameProviderEntryRequest }) =>
      renameProviderEntry(id, input),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: federationQueryKeys.credentials })
    },
  })
}

export function useSetProviderEntryAvailabilityMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ id, input }: { id: string; input: SetProviderEntryAvailabilityRequest }) =>
      setProviderEntryAvailability(id, input),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: federationQueryKeys.credentials })
      void queryClient.invalidateQueries({ queryKey: federationQueryKeys.agents })
      void queryClient.invalidateQueries({ queryKey: ['agent-chats'] })
    },
  })
}

export function useSetCliRuntimeAvailabilityMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({
      daemonId,
      executorType,
      input,
    }: {
      daemonId: string
      executorType: string
      input: SetCliRuntimeAvailabilityRequest
    }) => setCliRuntimeAvailability(daemonId, executorType, input),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: federationQueryKeys.credentials })
      void queryClient.invalidateQueries({ queryKey: federationQueryKeys.agents })
      void queryClient.invalidateQueries({ queryKey: ['agent-chats'] })
    },
  })
}

export function useAgentProviderCapabilitiesQuery() {
  return useQuery({
    queryKey: federationQueryKeys.providers,
    queryFn: listAgentProviderCapabilities,
    staleTime: 60_000,
  })
}

/**
 * Usage is a best-effort projection: some providers cannot be probed, and
 * the endpoint itself may not exist on an older server. Never retry hard —
 * a stale "usage unavailable" is preferable to hammering the provider.
 */
export function useProviderUsageQuery(entryId: string | undefined) {
  return useQuery({
    queryKey: federationQueryKeys.providerUsage(entryId ?? 'none'),
    queryFn: () => getProviderUsage(entryId!),
    enabled: Boolean(entryId),
    staleTime: 3 * 60_000,
    retry: false,
  })
}

export function useProviderAuthorizationQuery(id: string | undefined) {
  const queryClient = useQueryClient()
  const query = useQuery({
    queryKey: federationQueryKeys.providerAuthorization(id ?? 'none'),
    queryFn: () => getProviderAuthorization(id!),
    enabled: Boolean(id),
    refetchInterval: (query) => {
      const state = query.state.data?.state
      return state && ['succeeded', 'denied', 'expired', 'cancelled', 'failed'].includes(state)
        ? false
        : 1500
    },
  })
  // A succeeded authorization created a provider entry server-side; refresh
  // the entries list so the new credential shows up without a reload.
  const succeeded = query.data?.state === 'succeeded'
  useEffect(() => {
    if (!succeeded) return
    void queryClient.invalidateQueries({ queryKey: federationQueryKeys.credentials })
    void queryClient.invalidateQueries({ queryKey: federationQueryKeys.agents })
  }, [succeeded, queryClient])
  return query
}

export function useStartProviderAuthorizationMutation() {
  return useMutation({
    mutationFn: (input: StartProviderAuthorizationRequest) => startProviderAuthorization(input),
  })
}

export function useCancelProviderAuthorizationMutation() {
  return useMutation({
    mutationFn: ({ id, input }: { id: string; input: CancelProviderAuthorizationRequest }) =>
      cancelProviderAuthorization(id, input),
  })
}

/** A 404 means "no binding yet" — an expected state, never worth retrying. */
function retryUnlessMissing(failureCount: number, error: unknown): boolean {
  return !(error instanceof ApiError && error.status === 404) && failureCount < 1
}

export function useMainAgentBindingQuery() {
  return useQuery({
    queryKey: federationQueryKeys.mainAgent,
    queryFn: getMainAgentBinding,
    staleTime: 10_000,
    retry: retryUnlessMissing,
  })
}

export function useSetMainAgentBindingMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: setMainAgentBinding,
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: federationQueryKeys.mainAgent })
      void queryClient.invalidateQueries({ queryKey: ['agent-chats'] })
      void queryClient.invalidateQueries({ queryKey: federationQueryKeys.agents })
    },
  })
}

export function useRemoveProviderEntryMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ handleId, version }: { handleId: string; version: number }) =>
      removeProviderEntry(handleId, version),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: federationQueryKeys.credentials })
      void queryClient.invalidateQueries({ queryKey: federationQueryKeys.agents })
      void queryClient.invalidateQueries({ queryKey: ['agent-chats'] })
    },
  })
}

export function useEffectivePermissionsQuery(
  identityId: string | undefined,
  scope: CreateAgentSessionInput['scope'] | undefined,
) {
  return useQuery({
    queryKey: ['federated-agents', identityId ?? 'none', 'permissions', scope],
    queryFn: () => getEffectivePermissions(identityId!, scope!),
    enabled: Boolean(identityId && scope),
    staleTime: 15_000,
  })
}

export function useMissionControlQuery() {
  return useQuery({
    queryKey: federationQueryKeys.missionControl,
    queryFn: getMissionControl,
    staleTime: 15_000,
    refetchInterval: 30_000,
  })
}

export function useContextManifestQuery(
  lookup: (ContextManifestLookup & { manifest_id: string }) | undefined,
) {
  return useQuery({
    queryKey: federationQueryKeys.contextManifest(
      lookup?.manifest_id ?? 'none',
      lookup?.identity_id ?? 'none',
      lookup?.context_scope_id ?? 'none',
    ),
    queryFn: () => {
      const { manifest_id, ...query } = lookup!
      return getContextManifest(manifest_id, query)
    },
    enabled: Boolean(lookup?.manifest_id && lookup.identity_id && lookup.context_scope_id),
    staleTime: 30_000,
  })
}

export function useContextManifestDiscoveryQuery(lookup: ContextManifestLookup | undefined) {
  return useQuery({
    queryKey: federationQueryKeys.contextManifestDiscovery(
      lookup?.identity_id ?? 'none',
      lookup?.context_scope_id ?? 'none',
    ),
    queryFn: () => listContextManifests(lookup!),
    enabled: Boolean(lookup?.identity_id && lookup.context_scope_id),
    staleTime: 30_000,
  })
}

export function useProjectAgentBindingQuery(projectId: string | undefined) {
  return useQuery({
    queryKey: federationQueryKeys.projectAgent(projectId ?? 'none'),
    queryFn: () => getProjectAgentBinding(projectId!),
    enabled: Boolean(projectId),
    staleTime: 10_000,
    retry: retryUnlessMissing,
  })
}

export function useSetProjectAgentBindingMutation(projectId: string) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (input: ProjectAgentBindingInput) => setProjectAgentBinding(projectId, input),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: federationQueryKeys.projectAgent(projectId) })
      void queryClient.invalidateQueries({ queryKey: ['agent-chats'] })
      void queryClient.invalidateQueries({ queryKey: federationQueryKeys.agents })
    },
  })
}

export function isVersionConflict(error: unknown): boolean {
  return error instanceof ApiError && error.status === 409
}
