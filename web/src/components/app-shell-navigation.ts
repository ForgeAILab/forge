import type { ComponentType } from 'react'
import type { IconWeight } from '@phosphor-icons/react'
import {
  ChatCircleDots,
  ChatsCircle,
  ChartLineUp,
  Desktop,
  Gear,
  Kanban,
  List,
  Pulse,
  Robot,
  Sliders,
} from '@phosphor-icons/react'

type NavItemBase = {
  key:
    | 'overview'
    | 'board'
    | 'tasks'
    | 'mainChat'
    | 'agentWorkspace'
    | 'agentSettings'
    | 'missionControl'
    | 'daemons'
    | 'operations'
    | 'settings'
    | 'forgeSettings'
  icon: ComponentType<{ size?: string | number; weight?: IconWeight; className?: string }>
}

export type AppShellNavItem = NavItemBase &
  (
    | {
        section: 'project'
        to:
          | '/projects/$projectId/board'
          | '/projects/$projectId/chat'
          | '/projects/$projectId/tasks'
          | '/projects/$projectId/overview'
          | '/projects/$projectId/settings'
      }
    | {
        section: 'account' | 'workspace'
        to: '/chat' | '/agents' | '/mission-control' | '/daemons' | '/operations' | '/settings'
      }
  )

const navItems: AppShellNavItem[] = [
  { to: '/chat', key: 'mainChat', icon: ChatCircleDots, section: 'account' },
  { to: '/projects/$projectId/board', key: 'board', icon: Kanban, section: 'project' },
  {
    to: '/projects/$projectId/chat',
    key: 'agentWorkspace',
    icon: ChatsCircle,
    section: 'project',
  },
  { to: '/projects/$projectId/tasks', key: 'tasks', icon: List, section: 'project' },
  { to: '/projects/$projectId/overview', key: 'overview', icon: ChartLineUp, section: 'project' },
  { to: '/projects/$projectId/settings', key: 'settings', icon: Gear, section: 'project' },
  { to: '/agents', key: 'agentSettings', icon: Robot, section: 'workspace' },
  { to: '/mission-control', key: 'missionControl', icon: Pulse, section: 'workspace' },
  { to: '/daemons', key: 'daemons', icon: Desktop, section: 'workspace' },
  { to: '/operations', key: 'operations', icon: Pulse, section: 'workspace' },
  { to: '/settings', key: 'forgeSettings', icon: Sliders, section: 'workspace' },
]

export function navigationItemsForSection(section: AppShellNavItem['section']) {
  return navItems.filter((item) => item.section === section)
}
