import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { useQuery } from '@tanstack/react-query'
import { ApiError, listBranches, listFsEntries } from '@/api/client'
import { qk } from '@/api/query-keys'
import { Button } from '@/components/ui/button'
import { ComboSelect } from '@/components/ui/combo-select'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select } from '@/components/ui/select'
import { cn } from '@/lib/cn'
import { productTerm } from '@/lib/i18n'
import type { Daemon } from '@/types/generated/api'
import {
  ArrowCounterClockwise,
  CaretRight,
  CircleNotch,
  Folder,
  FolderOpen,
  GitBranch,
  House,
} from '@phosphor-icons/react'

export type RepoSourceMode = 'local' | 'remote'

export type RepoFormState = {
  source_mode: RepoSourceMode
  name: string
  local_path: string
  remote_url: string
  default_branch: string
}

export const emptyRepoForm: RepoFormState = {
  source_mode: 'local',
  name: '',
  local_path: '',
  remote_url: '',
  default_branch: 'main',
}

type RepoFormProps = {
  form: RepoFormState
  open: boolean
  pending: boolean
  daemons: Daemon[]
  daemonId: string | undefined
  onCancel: () => void
  onDaemonChange: (daemonId: string | undefined) => void
  onSubmit: (form: RepoFormState) => void
  onUpdate: (form: RepoFormState) => void
}

function breadcrumbs(path: string): Array<{ label: string; path: string }> {
  if (!path || path === '/') return [{ label: '/', path: '/' }]
  const parts = path.split('/').filter(Boolean)
  const result = [{ label: '/', path: '/' }]
  let current = ''
  for (const part of parts) {
    current += `/${part}`
    result.push({ label: part, path: current })
  }
  return result
}

function daemonDisplayName(daemon: Daemon): string {
  return daemon.hostname || daemon.machine_id
}

function daemonKind(daemon: Daemon): string | undefined {
  const candidate = daemon as Daemon & { kind?: unknown }
  return typeof candidate.kind === 'string' ? candidate.kind : undefined
}

function isEmbeddedDaemon(daemon: Daemon | undefined): boolean {
  return Boolean(
    daemon && (daemon.machine_id.startsWith('embedded:') || daemonKind(daemon) === 'embedded'),
  )
}

function apiErrorCode(error: unknown): string | undefined {
  if (!(error instanceof ApiError)) return undefined
  try {
    const parsed = JSON.parse(error.message) as { code?: unknown }
    return typeof parsed.code === 'string' ? parsed.code : undefined
  } catch {
    return undefined
  }
}

function DaemonStatusChip({ daemon, unavailable }: { daemon: Daemon; unavailable: boolean }) {
  const connected = daemon.status === 'online'
  const label = unavailable ? 'Unavailable' : connected ? 'Connected' : 'Offline'
  return (
    <span
      className={cn(
        'inline-flex shrink-0 items-center rounded-full px-2 py-0.5 text-xs font-medium',
        unavailable
          ? 'bg-red-500/10 text-red-700 dark:text-red-400'
          : connected
            ? 'bg-green-500/10 text-green-700 dark:text-green-400'
            : 'bg-amber-500/15 text-amber-700 dark:text-amber-300',
      )}
    >
      {label}
    </span>
  )
}

function DaemonUnavailableBanner() {
  return (
    <div className="rounded-md border border-red-500/30 bg-red-500/10 px-3 py-2 text-sm text-red-700 dark:text-red-300">
      This {productTerm('runtime').toLowerCase()} is currently unavailable. Restart it or pick a
      different {productTerm('runtime').toLowerCase()}.
    </div>
  )
}

export function RepoForm({
  form,
  open,
  pending,
  daemons,
  daemonId,
  onCancel,
  onDaemonChange,
  onSubmit,
  onUpdate,
}: RepoFormProps) {
  const [browsePath, setBrowsePath] = useState(() => form.local_path || '.')
  const [pathInput, setPathInput] = useState('')
  const [filter, setFilter] = useState('')
  const pathInputRef = useRef<HTMLInputElement>(null)
  const autoOriginRef = useRef<{ path: string; url: string } | null>(null)
  const selectedDaemon = daemonId ? daemons.find((daemon) => daemon.id === daemonId) : undefined
  const selectedDaemonOffline = selectedDaemon?.status === 'offline'

  useEffect(() => {
    setFilter('')
  }, [browsePath])

  const updateForm = useCallback(
    (patch: Partial<RepoFormState>) => onUpdate({ ...form, ...patch }),
    [form, onUpdate],
  )

  const fsQuery = useQuery({
    queryKey: qk.fsEntries(browsePath, daemonId ?? ''),
    queryFn: () => listFsEntries(browsePath, daemonId!),
    enabled: open && form.source_mode === 'local' && Boolean(daemonId) && !selectedDaemonOffline,
  })
  const displayPath = fsQuery.data?.path ?? browsePath

  useEffect(() => {
    setPathInput(displayPath === '.' ? '' : displayPath)
  }, [displayPath])

  const branchQuery = useQuery({
    queryKey: qk.branches(form.local_path, daemonId ?? ''),
    queryFn: () => listBranches(form.local_path, daemonId!),
    enabled:
      open &&
      form.source_mode === 'local' &&
      Boolean(form.local_path) &&
      Boolean(daemonId) &&
      !selectedDaemonOffline,
  })
  const daemonUnavailableCode = 'daemon_unavailable'
  const fsDaemonUnavailable =
    fsQuery.isError && apiErrorCode(fsQuery.error) === daemonUnavailableCode
  const branchDaemonUnavailable =
    branchQuery.isError && apiErrorCode(branchQuery.error) === daemonUnavailableCode
  const selectedDaemonUnavailable = fsDaemonUnavailable || branchDaemonUnavailable
  const showDirectoryUnavailable =
    form.source_mode === 'local' &&
    Boolean(selectedDaemon) &&
    (selectedDaemonOffline || fsDaemonUnavailable)
  const showBranchUnavailable =
    form.source_mode === 'local' &&
    Boolean(selectedDaemon) &&
    (selectedDaemonOffline || branchDaemonUnavailable)
  const daemonSelectionRequired = form.source_mode === 'local' && daemons.length > 1
  const needsDaemonSelection = daemonSelectionRequired && !daemonId
  const canNavigate =
    form.source_mode === 'local' &&
    Boolean(daemonId) &&
    !selectedDaemonOffline &&
    !fsDaemonUnavailable
  const showDaemonSelector = form.source_mode === 'local' && daemons.length > 1
  const showImplicitDaemonDisplay =
    form.source_mode === 'local' &&
    daemons.length === 1 &&
    Boolean(selectedDaemon) &&
    (!isEmbeddedDaemon(selectedDaemon) || selectedDaemonOffline || selectedDaemonUnavailable)
  const showRemotePathAnnotation =
    form.source_mode === 'local' && Boolean(selectedDaemon) && !isEmbeddedDaemon(selectedDaemon)

  const rawEntries = showDirectoryUnavailable
    ? []
    : (fsQuery.data?.entries ?? []).filter((entry) => entry.is_dir)
  const entries = useMemo(() => {
    const sorted = rawEntries.toSorted((a, b) =>
      a.name.localeCompare(b.name, undefined, { sensitivity: 'base' }),
    )
    const normalizedFilter = filter.trim().toLowerCase()
    if (!normalizedFilter) return sorted
    return sorted.filter((entry) => entry.name.toLowerCase().includes(normalizedFilter))
  }, [filter, rawEntries])

  const branchData = showBranchUnavailable ? undefined : branchQuery.data
  const branchOptions = branchData?.branches ?? []
  const originUrl = branchData?.origin_url?.trim()
  const effectiveDefaultBranch =
    form.default_branch || branchData?.default_branch || branchOptions[0] || 'main'
  const hasSelectedBranch =
    !effectiveDefaultBranch ||
    branchOptions.length === 0 ||
    branchOptions.includes(effectiveDefaultBranch)
  const canSave =
    !pending &&
    !branchQuery.isFetching &&
    (form.source_mode === 'local'
      ? Boolean(form.local_path.trim())
      : Boolean(form.remote_url.trim()))

  const navigateTo = (path: string) => {
    if (!canNavigate) return
    setBrowsePath(path)
  }

  const handlePathInputKeyDown = (event: React.KeyboardEvent<HTMLInputElement>) => {
    if (event.key === 'Enter' && canNavigate && pathInput.trim()) {
      navigateTo(pathInput.trim())
      pathInputRef.current?.blur()
    }
  }

  const selectLocalPath = (path: string) => {
    onUpdate({ ...form, source_mode: 'local', local_path: path, default_branch: '' })
  }

  useEffect(() => {
    if (form.source_mode !== 'local' || !form.local_path || !originUrl) return
    const lastAutoOrigin = autoOriginRef.current
    const currentRemoteUrl = form.remote_url.trim()
    const canAutofill =
      !currentRemoteUrl ||
      (lastAutoOrigin !== null &&
        currentRemoteUrl === lastAutoOrigin.url &&
        form.local_path !== lastAutoOrigin.path)
    if (!canAutofill || lastAutoOrigin?.path === form.local_path) return
    autoOriginRef.current = { path: form.local_path, url: originUrl }
    updateForm({ remote_url: originUrl })
  }, [form.local_path, form.remote_url, form.source_mode, originUrl, updateForm])

  const handleFolderClick = (path: string, isGitRepo: boolean) => {
    if (!canNavigate) return
    navigateTo(path)
    if (isGitRepo) selectLocalPath(path)
  }

  const submit = () => {
    onSubmit({
      ...form,
      remote_url: form.remote_url.trim(),
      default_branch: effectiveDefaultBranch || form.default_branch,
    })
  }

  const crumbs = breadcrumbs(displayPath)
  return (
    <>
      <div className="mt-4 grid gap-4">
        {form.source_mode === 'remote' ? (
          <div className="space-y-1.5">
            <Label htmlFor="repo-remote-url">Remote URL</Label>
            <Input
              id="repo-remote-url"
              required
              placeholder="https://github.com/org/repo.git"
              value={form.remote_url}
              onChange={(event) => updateForm({ remote_url: event.target.value })}
            />
            <p className="text-xs text-muted-foreground">
              Forge will clone this repository into its managed workspace cache.
            </p>
          </div>
        ) : (
          <div className="space-y-4 rounded-md border border-border-subtle bg-muted/20 p-3">
            {showDaemonSelector ? (
              <div className="space-y-1.5">
                <Label htmlFor="repo-daemon">{productTerm('runtime')}</Label>
                <div className="flex items-center gap-2">
                  <Select
                    id="repo-daemon"
                    value={daemonId ?? ''}
                    placeholder={`Select ${productTerm('runtime').toLowerCase()}...`}
                    className="min-w-0 flex-1"
                    options={daemons.map((daemon) => ({
                      value: daemon.id,
                      label: daemonDisplayName(daemon),
                    }))}
                    onChange={(value) => onDaemonChange(value || undefined)}
                  />
                  {selectedDaemon ? (
                    <DaemonStatusChip
                      daemon={selectedDaemon}
                      unavailable={selectedDaemonUnavailable}
                    />
                  ) : null}
                </div>
              </div>
            ) : null}

            {showImplicitDaemonDisplay && selectedDaemon ? (
              <div className="flex items-center justify-between gap-2 rounded-md border border-border-subtle bg-background px-3 py-2">
                <span className="truncate text-sm font-medium">
                  {daemonDisplayName(selectedDaemon)}
                </span>
                <DaemonStatusChip daemon={selectedDaemon} unavailable={selectedDaemonUnavailable} />
              </div>
            ) : null}

            {showDirectoryUnavailable ? (
              <DaemonUnavailableBanner />
            ) : (
              <>
                <div className="space-y-1.5">
                  <div className="flex items-center justify-between gap-2">
                    <Label>Local repo path</Label>
                    {showRemotePathAnnotation && selectedDaemon ? (
                      <span className="truncate text-xs text-muted-foreground">
                        Paths on {daemonDisplayName(selectedDaemon)}
                      </span>
                    ) : null}
                  </div>
                  <div className="flex gap-2">
                    <Input
                      ref={pathInputRef}
                      className="font-mono text-sm"
                      placeholder="Type a path and press Enter..."
                      value={pathInput}
                      disabled={!canNavigate}
                      onChange={(event) => setPathInput(event.target.value)}
                      onKeyDown={handlePathInputKeyDown}
                    />
                    <Button
                      size="icon"
                      variant="outline"
                      title="Home"
                      disabled={!canNavigate}
                      onClick={() => navigateTo('~')}
                    >
                      <House className="h-4 w-4" />
                    </Button>
                    <Button
                      size="icon"
                      variant="outline"
                      title="Launch directory"
                      disabled={!canNavigate}
                      onClick={() => navigateTo('.')}
                    >
                      <ArrowCounterClockwise className="h-4 w-4" />
                    </Button>
                  </div>
                </div>

                <div className="overflow-hidden rounded-md border bg-card">
                  <div className="flex flex-wrap items-center gap-0.5 border-b bg-muted/40 px-3 py-2">
                    {crumbs.map((crumb, index) => (
                      <span key={crumb.path} className="flex items-center gap-0.5">
                        {index > 0 && <CaretRight className="h-3 w-3 text-muted-foreground/60" />}
                        <button
                          type="button"
                          disabled={!canNavigate}
                          className="cursor-pointer rounded px-1 py-0.5 text-xs font-medium text-muted-foreground transition-colors hover:bg-accent hover:text-foreground disabled:cursor-not-allowed disabled:opacity-50"
                          onClick={() => navigateTo(crumb.path)}
                        >
                          {crumb.label}
                        </button>
                      </span>
                    ))}
                    {fsQuery.isFetching && (
                      <CircleNotch className="ml-1 h-3 w-3 animate-spin text-muted-foreground" />
                    )}
                  </div>

                  <div className="border-b px-3 py-2">
                    <Input
                      className="h-7 text-sm"
                      placeholder="Filter folders..."
                      value={filter}
                      disabled={!canNavigate}
                      onChange={(event) => setFilter(event.target.value)}
                    />
                  </div>

                  <div className="max-h-52 overflow-y-auto">
                    {needsDaemonSelection ? (
                      <p className="p-3 text-sm text-muted-foreground">
                        Select a {productTerm('runtime').toLowerCase()} to browse folders.
                      </p>
                    ) : !daemonId ? (
                      <p className="p-3 text-sm text-muted-foreground">
                        No {productTerm('runtime').toLowerCase()} is available to browse folders.
                      </p>
                    ) : fsQuery.isLoading ? (
                      <div className="flex items-center gap-2 p-3 text-sm text-muted-foreground">
                        <CircleNotch className="h-4 w-4 animate-spin" />
                        Loading...
                      </div>
                    ) : fsQuery.isError ? (
                      <p className="p-3 text-sm text-destructive">Unable to load this folder</p>
                    ) : rawEntries.length === 0 ? (
                      <p className="p-3 text-sm text-muted-foreground">No folders here</p>
                    ) : entries.length === 0 ? (
                      <p className="p-3 text-sm text-muted-foreground">
                        No matches for &quot;{filter.trim()}&quot;
                      </p>
                    ) : (
                      entries.map((entry) => {
                        const isSelected = form.local_path === entry.path
                        return (
                          <button
                            key={entry.path}
                            type="button"
                            className={cn(
                              'flex w-full cursor-pointer items-center gap-2.5 px-3 py-2 text-left text-sm transition-colors',
                              'border-b border-border/50 last:border-0',
                              isSelected ? 'bg-primary/10 text-foreground' : 'hover:bg-accent/60',
                            )}
                            onClick={() => handleFolderClick(entry.path, entry.is_git_repo)}
                          >
                            {isSelected ? (
                              <FolderOpen className="h-4 w-4 shrink-0 text-primary" />
                            ) : (
                              <Folder
                                className={cn(
                                  'h-4 w-4 shrink-0',
                                  entry.is_git_repo ? 'text-green-500' : 'text-muted-foreground',
                                )}
                              />
                            )}
                            <span className="min-w-0 flex-1 truncate font-medium">
                              {entry.name}
                            </span>
                            {entry.is_git_repo && (
                              <span className="flex shrink-0 items-center gap-1 rounded-full bg-green-500/10 px-2 py-0.5 text-xs font-medium text-green-600 dark:text-green-400">
                                <GitBranch className="h-3 w-3" />
                                git
                              </span>
                            )}
                            <CaretRight className="h-3.5 w-3.5 shrink-0 text-muted-foreground/50" />
                          </button>
                        )
                      })
                    )}
                  </div>
                </div>
              </>
            )}

            <div className="space-y-1.5">
              <Label htmlFor="repo-remote-url">
                Remote URL{' '}
                <span className="text-xs font-normal text-muted-foreground">(optional)</span>
              </Label>
              <Input
                id="repo-remote-url"
                placeholder="https://github.com/org/repo.git"
                value={form.remote_url}
                onChange={(event) => updateForm({ remote_url: event.target.value })}
              />
              <p className="text-xs text-muted-foreground">
                When a local path is selected, Forge keeps using it even if a remote URL is saved.
              </p>
            </div>
          </div>
        )}

        <div className="grid gap-3 sm:grid-cols-2">
          <div className="space-y-1.5">
            <Label htmlFor="repo-name">
              Name <span className="text-xs font-normal text-muted-foreground">(optional)</span>
            </Label>
            <Input
              id="repo-name"
              value={form.name}
              onChange={(event) => updateForm({ name: event.target.value })}
            />
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="repo-default-branch">Default branch</Label>
            {showBranchUnavailable ? (
              <DaemonUnavailableBanner />
            ) : branchOptions.length > 0 ? (
              <ComboSelect
                id="repo-default-branch"
                value={effectiveDefaultBranch || null}
                placeholder="Select branch..."
                options={[
                  ...(!hasSelectedBranch && effectiveDefaultBranch
                    ? [{ value: effectiveDefaultBranch, label: effectiveDefaultBranch }]
                    : []),
                  ...branchOptions.map((branch) => ({ value: branch, label: branch })),
                ]}
                onChange={(value) => updateForm({ default_branch: value ?? '' })}
              />
            ) : (
              <Input
                id="repo-default-branch"
                value={form.default_branch}
                onChange={(event) => updateForm({ default_branch: event.target.value })}
              />
            )}
          </div>
        </div>
      </div>

      <div className="mt-6 flex justify-end gap-2">
        <Button variant="outline" onClick={onCancel}>
          Cancel
        </Button>
        <Button disabled={!canSave} onClick={submit}>
          Save
        </Button>
      </div>
    </>
  )
}
