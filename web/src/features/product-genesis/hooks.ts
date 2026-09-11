import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  approveProductGenesisCharterRevision,
  cancelProductGenesis,
  createProjectFromCharterApproval,
  getProductGenesisCharter,
  getActiveProductGenesis,
  saveProductGenesisCharterRevision,
  startProductGenesis,
} from './api'
import type {
  ApproveProductGenesisCharterRevisionInput,
  CreateProjectFromCharterApprovalInput,
  ProductGenesisCancelInput,
  ProductGenesisCharterResponse,
  ProductGenesisStartInput,
  SaveProductGenesisCharterRevisionInput,
} from './types'

export const productGenesisQueryKeys = {
  active: ['product-genesis', 'active'] as const,
  charter: (sessionId: string) => ['product-genesis', sessionId, 'charter'] as const,
} as const

export function useProductGenesisActiveQuery() {
  return useQuery({
    queryKey: productGenesisQueryKeys.active,
    queryFn: getActiveProductGenesis,
    staleTime: 3_000,
    // Genesis lifecycle transitions are server-side events. Keep the status
    // chip current even when the backend has no SSE event for the transition.
    refetchInterval: (query) => (query.state.status === 'error' ? false : 2_000),
  })
}

export function useProductGenesisCharterQuery(sessionId: string | undefined) {
  return useQuery<ProductGenesisCharterResponse>({
    queryKey: productGenesisQueryKeys.charter(sessionId ?? 'none'),
    queryFn: () => getProductGenesisCharter(sessionId!),
    enabled: Boolean(sessionId),
    staleTime: 1_000,
    refetchInterval: (query) => (query.state.status === 'error' ? false : 2_000),
  })
}

function invalidateProductGenesis(queryClient: ReturnType<typeof useQueryClient>) {
  void queryClient.invalidateQueries({ queryKey: productGenesisQueryKeys.active })
  // Genesis admits its first turn into the existing Main Chat.  Invalidate
  // the chat prefix so the timeline and switcher never require a second chat.
  void queryClient.invalidateQueries({ queryKey: ['agent-chats'] })
}

function invalidateCharter(queryClient: ReturnType<typeof useQueryClient>, sessionId: string) {
  void queryClient.invalidateQueries({ queryKey: productGenesisQueryKeys.active })
  void queryClient.invalidateQueries({ queryKey: productGenesisQueryKeys.charter(sessionId) })
  void queryClient.invalidateQueries({ queryKey: ['agent-chats'] })
}

export function useStartProductGenesisMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (input: ProductGenesisStartInput) => startProductGenesis(input),
    onSuccess: () => invalidateProductGenesis(queryClient),
  })
}

export function useCancelProductGenesisMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({ sessionId, input }: { sessionId: string; input: ProductGenesisCancelInput }) =>
      cancelProductGenesis(sessionId, input),
    onSuccess: () => invalidateProductGenesis(queryClient),
  })
}

export function useSaveProductGenesisCharterRevisionMutation(sessionId: string | undefined) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (input: SaveProductGenesisCharterRevisionInput) =>
      saveProductGenesisCharterRevision(sessionId!, input),
    onSuccess: () => {
      if (sessionId) invalidateCharter(queryClient, sessionId)
    },
  })
}

export function useApproveProductGenesisCharterRevisionMutation(sessionId: string | undefined) {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: ({
      revisionId,
      input,
    }: {
      revisionId: string
      input: ApproveProductGenesisCharterRevisionInput
    }) => approveProductGenesisCharterRevision(sessionId!, revisionId, input),
    onSuccess: () => {
      if (sessionId) invalidateCharter(queryClient, sessionId)
    },
  })
}

export function useCreateProjectFromCharterApprovalMutation() {
  const queryClient = useQueryClient()
  return useMutation({
    mutationFn: (input: CreateProjectFromCharterApprovalInput) =>
      createProjectFromCharterApproval(input),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: productGenesisQueryKeys.active })
      void queryClient.invalidateQueries({ queryKey: ['projects'] })
      void queryClient.invalidateQueries({ queryKey: ['agent-chats'] })
    },
  })
}
