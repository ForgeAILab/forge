import type { ComponentType } from 'react'
import type { IconWeight } from '@phosphor-icons/react'
import {
  ChatCircleDots,
  ChartLineUp,
  Desktop,
  Gear,
  Kanban,
  List,
  Pulse,
  Robot,
  Sliders,
} from '@phosphor-icons/react'

export type AppShellNavItem = {
  to: string
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
  section: 'primary' | 'project' | 'workspace'
}

const navItems: AppShellNavItem[] = [
  { to: '/projects/$projectId/board', key: 'board', icon: Kanban, section: 'primary' },
  { to: '/chat', key: 'mainChat', icon: ChatCircleDots, section: 'primary' },
  { to: '/projects/$projectId/overview', key: 'overview', icon: ChartLineUp, section: 'project' },
  { to: '/projects/$projectId/tasks', key: 'tasks', icon: List, section: 'project' },
  {
    to: '/projects/$projectId/chat',
    key: 'agentWorkspace',
    icon: ChatCircleDots,
    section: 'project',
  },
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
