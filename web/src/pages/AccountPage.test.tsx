import { describe, expect, it } from 'vitest'
import { isAccountTab } from './AccountPage'

describe('account routes', () => {
  it('accepts the account usage analytics tab and rejects unknown tabs', () => {
    expect(isAccountTab('analytics')).toBe(true)
    expect(isAccountTab('unknown')).toBe(false)
    expect(isAccountTab(undefined)).toBe(false)
  })
})
