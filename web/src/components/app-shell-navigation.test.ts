import { describe, expect, it } from 'vitest'
import { getDrawerFocusableElements, navigationItemsForSection, trapDrawerFocus } from './app-shell'

describe('application shell navigation contract', () => {
  it('places the canonical Main Chat before Project navigation', () => {
    expect(navigationItemsForSection('main').map(({ key, to }) => [key, to])).toEqual([
      ['mainChat', '/chat'],
    ])
    expect(navigationItemsForSection('project').map(({ key, to }) => [key, to])).toEqual([
      ['overview', '/projects/$projectId/overview'],
      ['board', '/projects/$projectId/board'],
      ['tasks', '/projects/$projectId/tasks'],
      ['agentWorkspace', '/projects/$projectId/chat'],
      ['settings', '/projects/$projectId/settings'],
    ])
  })

  it('keeps Agent Settings and Forge Settings distinct', () => {
    const global = navigationItemsForSection('global').map(({ key, to }) => [key, to])
    expect(global).toContainEqual(['agentSettings', '/agents'])
    expect(global).toContainEqual(['forgeSettings', '/settings'])
    expect(global.flat()).not.toContain('/agents/federated')
  })

  it('cycles keyboard focus inside the overlay drawer', () => {
    const drawer = document.createElement('aside')
    const first = document.createElement('button')
    const last = document.createElement('button')
    const outside = document.createElement('button')
    drawer.append(first, last)
    document.body.append(drawer, outside)

    expect(getDrawerFocusableElements(drawer)).toEqual([first, last])

    last.focus()
    const forward = new KeyboardEvent('keydown', { key: 'Tab', cancelable: true })
    trapDrawerFocus(forward, drawer)
    expect(forward.defaultPrevented).toBe(true)
    expect(document.activeElement).toBe(first)

    first.focus()
    const backward = new KeyboardEvent('keydown', {
      key: 'Tab',
      shiftKey: true,
      cancelable: true,
    })
    trapDrawerFocus(backward, drawer)
    expect(backward.defaultPrevented).toBe(true)
    expect(document.activeElement).toBe(last)

    outside.focus()
    const outsideTab = new KeyboardEvent('keydown', { key: 'Tab', cancelable: true })
    trapDrawerFocus(outsideTab, drawer)
    expect(outsideTab.defaultPrevented).toBe(true)
    expect(document.activeElement).toBe(first)

    drawer.remove()
    outside.remove()
  })
})
