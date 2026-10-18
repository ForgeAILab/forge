import type { ComponentProps, ReactNode } from 'react'
import { render, screen } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { useLayoutStore } from '@/stores/layout'
import { AppShell } from './app-shell'

const route = vi.hoisted(() => ({ projectId: undefined as string | undefined }))
vi.mock('@tanstack/react-router', () => ({
  Link: ({
    to,
    params,
    children,
    className,
    'aria-label': ariaLabel,
    onClick,
  }: ComponentProps<'a'> & {
    to: string
    params?: Record<string, string>
    activeOptions?: unknown
    children: ReactNode
  }) => {
    const href = Object.entries(params ?? {}).reduce(
      (path, [key, value]) => path.replace(`$${key}`, value),
      to,
    )
    return (
      <a href={href} className={className} aria-label={ariaLabel} onClick={onClick}>
        {children}
      </a>
    )
  },
  useNavigate: () => vi.fn(),
  useRouterState: ({ select }: { select: (state: unknown) => unknown }) =>
    select({
      matches: [{ params: { projectId: route.projectId } }],
      location: { pathname: route.projectId ? `/projects/${route.projectId}/settings` : '/chat' },
    }),
}))
vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string) =>
      ({
        'appShell.navigation.board': 'Kanban',
        'appShell.navigation.mainChat': 'Main Agent',
        'appShell.navigation.mainAgentDescription': 'Account assistant',
      })[key] ?? key,
  }),
}))
vi.mock('@/api/hooks', () => ({
  useAgentsQuery: () => ({ data: { items: [] } }),
  useCreateProject: () => ({ isPending: false }),
  useProjectsInfiniteQuery: () => ({ data: { pages: [{ items: [] }] } }),
}))
vi.mock('@/components/notification-center', () => ({ NotificationCenter: () => null }))
vi.mock('@/components/project-flow-status', () => ({ ProjectEnvironmentPauseNotice: () => null }))

beforeEach(() => useLayoutStore.setState({ selectedProjectId: undefined }))
afterEach(() => vi.restoreAllMocks())

describe('header brand navigation', () => {
  it.each([undefined, 'project-1'])(
    'links Forge home to / with selected Project %s',
    (projectId) => {
      route.projectId = projectId
      const client = new QueryClient()
      render(
        <QueryClientProvider client={client}>
          <AppShell>
            <p>Page content</p>
          </AppShell>
        </QueryClientProvider>,
      )

      const home = screen.getByRole('link', { name: 'Forge home' })
      expect(home.getAttribute('href')).toBe('/')
      expect(home.textContent).toContain('Forge')
      expect(screen.queryByRole('link', { name: 'Open Kanban' })).toBeNull()
      const chat = screen.getByRole('link', { name: /Main Agent.*Account assistant/ })
      expect(chat.getAttribute('href')).toBe('/chat')
      if (projectId) {
        expect(screen.getByRole('link', { name: 'Kanban' }).getAttribute('href')).toBe(
          `/projects/${projectId}/board`,
        )
      }
      client.clear()
    },
  )
})
