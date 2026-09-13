import { describe, expect, it } from 'vitest'
import { navigationItemsForSection } from './app-shell-navigation'

describe('application shell navigation contract', () => {
  it('keeps exactly Kanban and Main Chat in primary navigation', () => {
    expect(navigationItemsForSection('primary').map(({ key, to }) => [key, to])).toEqual([
      ['board', '/projects/$projectId/board'],
      ['mainChat', '/chat'],
    ])
    expect(navigationItemsForSection('project').map(({ key, to }) => [key, to])).toEqual([
      ['overview', '/projects/$projectId/overview'],
      ['tasks', '/projects/$projectId/tasks'],
      ['agentWorkspace', '/projects/$projectId/chat'],
      ['settings', '/projects/$projectId/settings'],
    ])
  })

  it('keeps Agent Settings and Forge Settings distinct', () => {
    const workspace = navigationItemsForSection('workspace').map(({ key, to }) => [key, to])
    expect(workspace).toContainEqual(['agentSettings', '/agents'])
    expect(workspace).toContainEqual(['forgeSettings', '/settings'])
    expect(workspace.flat()).not.toContain('/agents/federated')
  })
})
