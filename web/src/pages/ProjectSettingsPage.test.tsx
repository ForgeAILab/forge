import type { ReactNode } from 'react'
import { act, fireEvent, render, screen, waitFor } from '@testing-library/react'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import type { Project } from '@/types/generated'
import { ProjectSettingsPage } from './ProjectSettingsPage'

const mutate = vi.hoisted(() => vi.fn())
const query = vi.hoisted(() => ({ data: undefined as Project | undefined }))
const project: Project = {
  id: 'project-1',
  name: 'NovelKit',
  primary_repo_id: 'repo-1',
  project_hooks: [],
  settings: {
    max_active_tasks: 5,
    retry_budgets: { review: 3 },
    default_role_assignments: [],
    lifecycle_hooks: {},
    automatic_recovery: { enabled: false, agent_id: null, max_attempts: 1 },
    environment: {
      env: { TOOL_PATH: '/opt/tool' },
      assets: [],
      checks: [
        { name: 'disk', command: 'df -h /', scope: 'workspace', roles: [], timeout_seconds: 120 },
      ],
      recheck_interval_seconds: 600,
    },
    placement: { provision: 'when_verified' },
    command_allowlist: { allow: ['cargo'] },
  },
  default_review_config: { ci_steps: ['cargo test -p db one_case'] },
  paused_at: null,
  system_pause_reason: null,
  environment_pause: null,
  slots: { limit: 5, active: 4, parked: 3, queued: 7 },
  paused: false,
  charter_status: 'approved',
  charter_setup_required: false,
  current_charter_id: 'charter-1',
  current_charter_revision_id: 'revision-1',
  current_charter_version: 1,
  primary_milestone_id: null,
  version: 7,
  execution_setup: null,
  created_at: '2026-09-30T12:00:00Z',
  updated_at: '2026-09-30T12:00:00Z',
}

vi.mock('@/api/hooks', () => ({
  useProjectQuery: () => ({ data: query.data, isLoading: false, isError: false }),
  useWorkflowQuery: () => ({ data: { roles: [] }, isLoading: false }),
  useAgentsQuery: () => ({ data: { items: [] }, isLoading: false, isError: false }),
  useProjectAgentsQuery: () => ({ data: [], isLoading: false }),
  useMembersQuery: () => ({ data: [] }),
  useUpdateProject: () => ({ mutate, isPending: false }),
  useDeleteProject: () => ({ mutate: vi.fn(), isPending: false }),
  usePauseProject: () => ({ mutate: vi.fn(), isPending: false }),
  useResumeProject: () => ({ mutate: vi.fn(), isPending: false }),
}))
vi.mock('@tanstack/react-router', () => ({
  Link: ({ children }: { children: ReactNode }) => <a href="#settings">{children}</a>,
  useNavigate: () => vi.fn(),
  useRouterState: () => ({ projectId: 'project-1' }),
}))

function renderSettings(initialTab: 'general' | 'environment') {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false } } })
  const wrapper = ({ children }: { children: ReactNode }) => (
    <QueryClientProvider client={client}>{children}</QueryClientProvider>
  )
  return render(<ProjectSettingsPage projectId="project-1" initialTab={initialTab} />, { wrapper })
}

beforeEach(() => {
  mutate.mockReset()
  query.data = project
})

describe('Project flow control settings', () => {
  it('preserves the provision timeout when saving environment controls', async () => {
    query.data = {
      ...project,
      settings: {
        ...project.settings,
        placement: { provision: 'when_verified', provision_timeout_seconds: 3600 },
      },
    }
    renderSettings('environment')
    await waitFor(() =>
      expect(
        (screen.getByLabelText('Environment re-check interval (minutes)') as HTMLInputElement)
          .value,
      ).toBe('10'),
    )
    fireEvent.click(screen.getByRole('button', { name: 'Save environment' }))
    expect(mutate).toHaveBeenCalledWith(
      expect.objectContaining({
        body: expect.objectContaining({
          settings: expect.objectContaining({
            placement: { provision: 'when_verified', provision_timeout_seconds: 3600 },
          }),
        }),
      }),
      expect.any(Object),
    )
  })

  it('preserves the active-limit draft when only slot counts refresh', async () => {
    const { rerender } = renderSettings('general')
    await waitFor(() =>
      expect((document.getElementById('project-name') as HTMLInputElement).value).toBe('NovelKit'),
    )
    fireEvent.change(screen.getByLabelText('Active task limit'), { target: { value: '8' } })
    query.data = { ...project, slots: { ...project.slots, active: 3 } }
    rerender(<ProjectSettingsPage projectId="project-1" initialTab="general" />)
    expect((screen.getByLabelText('Active task limit') as HTMLInputElement).value).toBe('8')
  })

  it.each(['0', '1000'])(
    'saves active task limit %s with the version and unrelated settings intact',
    async (value) => {
      renderSettings('general')
      const input = screen.getByLabelText('Active task limit') as HTMLInputElement
      await waitFor(() =>
        expect((document.getElementById('project-name') as HTMLInputElement).value).toBe(
          'NovelKit',
        ),
      )
      fireEvent.change(input, { target: { value } })
      fireEvent.click(screen.getByRole('button', { name: 'Save' }))
      expect(mutate).toHaveBeenCalledWith(
        expect.objectContaining({
          projectId: 'project-1',
          body: expect.objectContaining({
            version: 7,
            settings: expect.objectContaining({
              max_active_tasks: Number(value),
              retry_budgets: project.settings.retry_budgets,
              environment: project.settings.environment,
              command_allowlist: project.settings.command_allowlist,
            }),
          }),
        }),
        expect.any(Object),
      )
    },
  )

  it.each(['', '-1', '1.5', '1001'])(
    'refuses invalid active task limit %s inline',
    async (value) => {
      renderSettings('general')
      await waitFor(() =>
        expect((document.getElementById('project-name') as HTMLInputElement).value).toBe(
          'NovelKit',
        ),
      )
      fireEvent.change(screen.getByLabelText('Active task limit'), { target: { value } })
      expect(screen.getByRole('alert').textContent).toContain('integer from 0 to 1000')
      expect(screen.getByLabelText('Active task limit').getAttribute('aria-invalid')).toBe('true')
      expect((screen.getByRole('button', { name: 'Save' }) as HTMLButtonElement).disabled).toBe(
        true,
      )
      expect(mutate).not.toHaveBeenCalled()
    },
  )

  it.each([
    ['1', 60],
    ['1440', 86400],
    ['1.5', 90],
  ] as const)(
    'saves %s minutes in seconds without replacing other settings',
    async (value, seconds) => {
      renderSettings('environment')
      const input = screen.getByLabelText(
        'Environment re-check interval (minutes)',
      ) as HTMLInputElement
      await waitFor(() => expect(input.value).toBe('10'))
      await waitFor(() =>
        expect(
          (screen.getByLabelText('Project environment JSON') as HTMLTextAreaElement).value,
        ).toContain('TOOL_PATH'),
      )
      expect(
        (screen.getByLabelText('Project environment JSON') as HTMLTextAreaElement).value,
      ).not.toContain('recheck_interval_seconds')
      fireEvent.change(input, { target: { value } })
      fireEvent.click(screen.getByRole('button', { name: 'Save environment' }))
      expect(mutate).toHaveBeenCalledWith(
        {
          projectId: 'project-1',
          body: {
            version: 7,
            settings: {
              ...project.settings,
              environment: {
                ...(project.settings.environment as object),
                recheck_interval_seconds: seconds,
              },
            },
          },
        },
        expect.any(Object),
      )
    },
  )

  it.each(['', '0', '1441'])('refuses invalid re-check interval %s inline', async (value) => {
    renderSettings('environment')
    const input = screen.getByLabelText(
      'Environment re-check interval (minutes)',
    ) as HTMLInputElement
    await waitFor(() =>
      expect(
        (screen.getByLabelText('Project environment JSON') as HTMLTextAreaElement).value,
      ).toContain('TOOL_PATH'),
    )
    fireEvent.change(input, { target: { value } })
    expect(screen.getByRole('alert').textContent).toContain('1 to 1440 minutes')
    expect(
      (screen.getByRole('button', { name: 'Save environment' }) as HTMLButtonElement).disabled,
    ).toBe(true)
    expect(mutate).not.toHaveBeenCalled()
  })

  it('clears environment declarations while retaining the selected re-check interval', async () => {
    renderSettings('environment')
    const editor = screen.getByLabelText('Project environment JSON') as HTMLTextAreaElement
    await waitFor(() => expect(editor.value).toContain('TOOL_PATH'))
    fireEvent.change(editor, { target: { value: '' } })
    fireEvent.change(screen.getByLabelText('Environment re-check interval (minutes)'), {
      target: { value: '15' },
    })
    fireEvent.click(screen.getByRole('button', { name: 'Save environment' }))
    expect(mutate.mock.calls[0][0].body.settings.environment).toEqual({
      env: {},
      assets: [],
      checks: [],
      recheck_interval_seconds: 900,
    })
  })

  it('shows server validation errors inline for a settings save', async () => {
    renderSettings('general')
    await waitFor(() =>
      expect((document.getElementById('project-name') as HTMLInputElement).value).toBe('NovelKit'),
    )
    fireEvent.click(screen.getByRole('button', { name: 'Save' }))
    act(() =>
      mutate.mock.calls[0][1].onError(
        new Error('Project settings were changed; refresh and retry.'),
      ),
    )
    expect(screen.getByRole('alert').textContent).toContain('Project settings were changed')
  })
})
