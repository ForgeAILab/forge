import { useMemo, useState, useEffect } from 'react'
import { ArrowRight, GitDiff } from '@phosphor-icons/react'
import { DiffModeEnum, DiffView } from '@git-diff-view/react'
import '@git-diff-view/react/styles/diff-view.css'
import type { UseQueryResult } from '@tanstack/react-query'
import { ApiError } from '@/api/client'
import { ErrorBanner } from '@/components/error-banner'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { cn } from '@/lib/cn'
import { isTransientApiError } from '@/lib/api-error'
import { productTerm } from '@/lib/i18n'
import { useLayoutStore } from '@/stores/layout'
import type { DiffEnvelope } from '@/types/generated'
import { diffStatusStyles, splitDiffIntoFiles } from './utils'

interface TaskDiffPanelProps {
  diffQuery: UseQueryResult<DiffEnvelope, Error>
  canLaunch: boolean
  hasAgents: boolean
  onOpenLaunchDialog: () => void
}

export function TaskDiffPanel({
  diffQuery,
  canLaunch,
  hasAgents,
  onOpenLaunchDialog,
}: TaskDiffPanelProps) {
  const theme = useLayoutStore((s) => s.theme)
  const [diffViewMode, setDiffViewMode] = useState<'unified' | 'split'>(() => {
    const saved = window.localStorage.getItem('task-diff-view-mode')
    return saved === 'split' ? 'split' : 'unified'
  })
  const [selectedDiffPath, setSelectedDiffPath] = useState<string | null>(null)

  useEffect(() => {
    window.localStorage.setItem('task-diff-view-mode', diffViewMode)
  }, [diffViewMode])

  const diffData = diffQuery.data?.data

  const parsedDiffFiles = useMemo(() => splitDiffIntoFiles(diffData?.diff ?? ''), [diffData?.diff])

  const selectedDiffFile = useMemo(
    () =>
      parsedDiffFiles.find((file) => file.path === selectedDiffPath) ?? parsedDiffFiles[0] ?? null,
    [parsedDiffFiles, selectedDiffPath],
  )

  useEffect(() => {
    const timeout = window.setTimeout(() => {
      setSelectedDiffPath(selectedDiffFile?.path ?? null)
    }, 0)
    return () => window.clearTimeout(timeout)
  }, [selectedDiffFile])

  const selectedDiffSummary = useMemo(
    () =>
      selectedDiffFile
        ? (diffData?.files.find((file) => file.path === selectedDiffFile.path) ?? null)
        : null,
    [diffData?.files, selectedDiffFile],
  )

  const selectedDiffViewData = useMemo(() => {
    if (!selectedDiffFile) return undefined
    return {
      oldFile: {
        fileName: selectedDiffFile.oldPath ?? selectedDiffFile.path,
        content: '',
      },
      newFile: {
        fileName: selectedDiffFile.newPath ?? selectedDiffFile.path,
        content: '',
      },
      hunks: selectedDiffFile.hunks,
    }
  }, [selectedDiffFile])

  const diffIsWorkspaceMissing =
    diffQuery.error instanceof ApiError &&
    diffQuery.error.status === 404 &&
    diffQuery.error.message.includes('workspace.not_found')

  const diffIsWorkspaceCleaned =
    diffQuery.error instanceof ApiError && diffQuery.error.message.includes('status=cleaned')

  const fileCount = diffData?.stats.files_changed ?? 0

  return (
    <div className="space-y-3 p-6">
      {/* Header bar */}
      <div className="flex flex-wrap items-center justify-between gap-3 rounded-lg border px-4 py-3">
        <div className="space-y-1 min-w-0">
          {diffData ? (
            <>
              <p className="text-sm font-medium">
                {fileCount} {fileCount === 1 ? 'file' : 'files'} changed
                <span className="ml-2 font-normal">
                  <span className="text-emerald-500">+{diffData.stats.total_additions}</span>
                  <span className="mx-1 text-muted-foreground/40">/</span>
                  <span className="text-red-500">-{diffData.stats.total_deletions}</span>
                </span>
              </p>
              <p className="flex items-center gap-1 text-xs text-muted-foreground font-mono">
                <span className="truncate">{diffData.head_ref}</span>
                <ArrowRight size={11} className="shrink-0" />
                <span className="truncate">{diffData.base_ref}</span>
              </p>
            </>
          ) : (
            <p className="text-sm font-medium text-muted-foreground">Workspace diff</p>
          )}
        </div>

        <div className="flex items-center gap-2">
          {/* Segmented toggle: Unified / Split */}
          <div className="inline-flex rounded-md border overflow-hidden text-xs">
            {(['unified', 'split'] as const).map((mode) => (
              <button
                key={mode}
                type="button"
                onClick={() => setDiffViewMode(mode)}
                className={cn(
                  'cursor-pointer px-3 py-1.5 font-medium capitalize transition-colors',
                  diffViewMode === mode
                    ? 'bg-muted text-foreground'
                    : 'text-muted-foreground hover:bg-accent hover:text-foreground',
                )}
              >
                {mode}
              </button>
            ))}
          </div>
          <Button
            size="sm"
            variant="outline"
            disabled={diffQuery.isFetching}
            onClick={() => void diffQuery.refetch()}
          >
            Refresh
          </Button>
        </div>
      </div>

      {/* Body */}
      {diffQuery.isLoading ? (
        <Skeleton className="h-28 w-full" />
      ) : diffIsWorkspaceMissing ? (
        <EmptyState
          title="No workspace yet"
          description={`Launch a ${productTerm('run').toLowerCase()} to start working on this task.`}
          action={
            canLaunch ? (
              <Button
                size="sm"
                variant="outline"
                disabled={!hasAgents}
                onClick={onOpenLaunchDialog}
              >
                Launch {productTerm('run')}
              </Button>
            ) : undefined
          }
        />
      ) : diffIsWorkspaceCleaned ? (
        <EmptyState
          title="Branch merged"
          description="The workspace was cleaned up after merging. No diff is available."
        />
      ) : diffQuery.isError ? (
        <ErrorBanner
          error={diffQuery.error}
          fallback="Failed to load diff"
          onRetry={() => void diffQuery.refetch()}
          showRetry={isTransientApiError(diffQuery.error)}
        />
      ) : diffData && diffData.files.length === 0 ? (
        <EmptyState
          icon={<GitDiff size={28} className="text-muted-foreground/30" />}
          title="No changes"
          description="The workspace matches the base branch."
        />
      ) : diffData ? (
        <div className="grid gap-3 lg:grid-cols-[260px_1fr]">
          {/* File list */}
          <div className="rounded-lg border overflow-hidden">
            <div className="border-b px-3 py-2">
              <span className="text-xs font-medium text-muted-foreground">
                Files
                <span className="ml-1.5 text-muted-foreground/50">({diffData.files.length})</span>
              </span>
            </div>
            <ul className="max-h-[56vh] overflow-y-auto p-1.5 space-y-0.5">
              {diffData.files.map((file) => {
                const style = diffStatusStyles[file.status] ?? diffStatusStyles.modified
                const selected = selectedDiffFile?.path === file.path
                return (
                  <li key={file.path}>
                    <button
                      className={cn(
                        'flex w-full cursor-pointer items-center justify-between gap-2 rounded-md px-2 py-1.5 text-left transition-colors',
                        selected
                          ? 'bg-accent text-foreground'
                          : 'text-muted-foreground hover:bg-accent/50 hover:text-foreground',
                      )}
                      type="button"
                      onClick={() => setSelectedDiffPath(file.path)}
                    >
                      <span className="flex min-w-0 items-center gap-2">
                        <span
                          className={cn(
                            'inline-flex h-4 w-4 shrink-0 items-center justify-center rounded text-[9px] font-bold',
                            style.className,
                          )}
                        >
                          {style.label}
                        </span>
                        <span className="truncate text-xs">{file.path}</span>
                      </span>
                      <span className="shrink-0 text-[11px] font-mono tabular-nums">
                        <span className="text-emerald-500">+{file.additions}</span>
                        <span className="text-muted-foreground/40 mx-0.5">/</span>
                        <span className="text-red-500">-{file.deletions}</span>
                      </span>
                    </button>
                  </li>
                )
              })}
            </ul>
          </div>

          {/* Diff viewer */}
          <div className="rounded-lg border overflow-hidden">
            {selectedDiffFile && selectedDiffViewData ? (
              <>
                <div className="flex items-center justify-between gap-3 border-b px-3 py-2">
                  <span className="truncate font-mono text-xs text-foreground">
                    {selectedDiffFile.path}
                  </span>
                  {selectedDiffSummary ? (
                    <span className="shrink-0 text-[11px] font-mono tabular-nums">
                      <span className="text-emerald-500">+{selectedDiffSummary.additions}</span>
                      <span className="text-muted-foreground/40 mx-0.5">/</span>
                      <span className="text-red-500">-{selectedDiffSummary.deletions}</span>
                    </span>
                  ) : null}
                </div>
                <div className="max-h-[56vh] overflow-auto">
                  <DiffView
                    data={selectedDiffViewData}
                    diffViewHighlight
                    diffViewTheme={theme}
                    diffViewMode={
                      diffViewMode === 'split' ? DiffModeEnum.Split : DiffModeEnum.Unified
                    }
                    diffViewWrap
                  />
                </div>
              </>
            ) : (
              <div className="p-6 text-sm text-muted-foreground">
                No selectable file diff content.
              </div>
            )}
          </div>
        </div>
      ) : null}
    </div>
  )
}

function EmptyState({
  icon,
  title,
  description,
  action,
}: {
  icon?: React.ReactNode
  title: string
  description: string
  action?: React.ReactNode
}) {
  return (
    <div className="flex flex-col items-center gap-2 rounded-lg border border-dashed py-10 text-center">
      {icon}
      <p className="text-sm font-medium text-foreground">{title}</p>
      <p className="text-xs text-muted-foreground">{description}</p>
      {action ? <div className="mt-1">{action}</div> : null}
    </div>
  )
}
