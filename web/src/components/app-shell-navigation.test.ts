import { describe, expect, it } from 'vitest'
import { navigationItemsForSection } from './app-shell-navigation'

describe('application shell navigation contract', () => {
  it('separates the account Main Agent from the selected Project Agent', () => {
    expect(navigationItemsForSection('account').map(({ key, to }) => [key, to])).toEqual([
      ['mainChat', '/chat'],
    ])
    expect(navigationItemsForSection('project').map(({ key, to }) => [key, to])).toEqual([
      ['board', '/projects/$projectId/board'],
      ['agentWorkspace', '/projects/$projectId/chat'],
      ['tasks', '/projects/$projectId/tasks'],
      ['overview', '/projects/$projectId/overview'],
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
