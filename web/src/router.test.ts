import { describe, expect, it } from 'vitest'
import { firstAuthorizedProjectId } from './router'

// F17 / 8.4.4: the root route redirected to `/projects/default/board` when
// the account had no Project — a fabricated ID that 404s on every
// Project-scoped fetch behind it. `firstAuthorizedProjectId` is the exact
// selection `indexRoute`'s `beforeLoad` redirects on; with no items it must
// return `undefined` so the caller falls back to `/chat`, never a literal
// `'default'`.
describe('firstAuthorizedProjectId', () => {
  it('returns the first authorized Project id when at least one exists', () => {
    expect(
      firstAuthorizedProjectId({
        items: [{ id: 'project-1' } as never, { id: 'project-2' } as never],
      }),
    ).toBe('project-1')
  })

  it('returns undefined rather than a fabricated default id when no Project remains', () => {
    expect(firstAuthorizedProjectId({ items: [] })).toBeUndefined()
  })
})
