import { create } from 'zustand'
import type { AgentChat, AgentChatTurn } from '@/features/agent-chat/types'

type ChatSelection = {
  globalChatId?: string
  projectChatIds: Record<string, string | undefined>
  pendingTurns: Record<string, AgentChatTurn[]>
  setGlobalChat: (chat: AgentChat | undefined) => void
  setProjectChat: (projectId: string, chat: AgentChat | undefined) => void
  setPendingTurns: (chatId: string, turns: AgentChatTurn[]) => void
  clearPendingTurn: (chatId: string, turnId: string) => void
  /**
   * Drop a deleted Project's chat selection and any pending-turn state for
   * its chat (8.4.4 / F17). `pendingTurns` is keyed by chat id, not project
   * id, so this resolves the Project's chat id first rather than leaving it
   * orphaned in the store after the Project itself is gone.
   */
  clearProjectChat: (projectId: string) => void
}

export const useChatSelection = create<ChatSelection>((set) => ({
  projectChatIds: {},
  pendingTurns: {},
  setGlobalChat: (chat) => set((current) => ({ ...current, globalChatId: chat?.id })),
  setProjectChat: (projectId, chat) =>
    set((current) => ({
      ...current,
      projectChatIds: { ...current.projectChatIds, [projectId]: chat?.id },
    })),
  setPendingTurns: (chatId, turns) =>
    set((current) => ({
      ...current,
      pendingTurns: { ...current.pendingTurns, [chatId]: turns },
    })),
  clearPendingTurn: (chatId, turnId) =>
    set((current) => {
      const turns = current.pendingTurns[chatId] ?? []
      const remaining = turns.filter((turn) => turn.id !== turnId)
      if (remaining.length === turns.length) return current
      const pendingTurns = { ...current.pendingTurns }
      if (remaining.length === 0) delete pendingTurns[chatId]
      else pendingTurns[chatId] = remaining
      return { ...current, pendingTurns }
    }),
  clearProjectChat: (projectId) =>
    set((current) => {
      const chatId = current.projectChatIds[projectId]
      if (chatId === undefined && !(projectId in current.projectChatIds)) return current
      const projectChatIds = { ...current.projectChatIds }
      delete projectChatIds[projectId]
      const pendingTurns = { ...current.pendingTurns }
      if (chatId) delete pendingTurns[chatId]
      return { ...current, projectChatIds, pendingTurns }
    }),
}))
