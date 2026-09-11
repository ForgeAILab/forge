import { lazy, Suspense, useEffect, useMemo, useState } from 'react'
import { useNavigate } from '@tanstack/react-router'
import { Command } from 'cmdk'
import { MagnifyingGlass } from '@phosphor-icons/react'
import { useTasksQuery } from '@/api/hooks'
import { useLayoutStore } from '@/stores/layout'
import type { Task } from '@/types/generated'

const TaskCreateDialog = lazy(() =>
  import('@/components/task-create-dialog').then((module) => ({
    default: module.TaskCreateDialog,
  })),
)

const recentTasksKey = 'recentTasks:v1'

type RecentTask = Pick<Task, 'id' | 'title'>

function readRecentTasks(): RecentTask[] {
  try {
    const parsed = JSON.parse(localStorage.getItem(recentTasksKey) ?? '[]') as unknown
    if (!Array.isArray(parsed)) return []
    return parsed.filter(
      (item): item is RecentTask =>
        typeof item === 'object' &&
        item !== null &&
        typeof (item as RecentTask).id === 'string' &&
        typeof (item as RecentTask).title === 'string',
    )
  } catch {
    return []
  }
}

function writeRecentTask(task: RecentTask): RecentTask[] {
  const next = [task, ...readRecentTasks().filter((item) => item.id !== task.id)].slice(0, 5)
  localStorage.setItem(recentTasksKey, JSON.stringify(next))
  return next
}

export function CommandPalette({ projectId }: { projectId: string }) {
  const navigate = useNavigate()
  const [open, setOpen] = useState(false)
  const [createOpen, setCreateOpen] = useState(false)
  const [recentTasks, setRecentTasks] = useState<RecentTask[]>(() => readRecentTasks())
  const theme = useLayoutStore((s) => s.theme)
  const setTheme = useLayoutStore((s) => s.setTheme)
  const tasks = useTasksQuery(projectId, {}, { enabled: open })
  const taskItems = useMemo(() => tasks.data?.pages.flatMap((p) => p.items) ?? [], [tasks.data])

  useEffect(() => {
    const onKeydown = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === 'k') {
        event.preventDefault()
        setOpen((prev) => !prev)
      }
    }
    document.addEventListener('keydown', onKeydown)
    return () => document.removeEventListener('keydown', onKeydown)
  }, [])

  const navigateToTask = (task: RecentTask) => {
    setRecentTasks(writeRecentTask(task))
    setOpen(false)
    void navigate({ to: '/tasks/$taskId', params: { taskId: task.id } })
  }

  return (
    <>
      <button
        type="button"
        className="inline-flex items-center gap-2 rounded-md border px-2 py-1 text-sm"
        onClick={() => setOpen(true)}
      >
        <MagnifyingGlass size={14} />
        Search
      </button>
      <Command.Dialog
        open={open}
        onOpenChange={(nextOpen) => {
          if (nextOpen) setRecentTasks(readRecentTasks())
          setOpen(nextOpen)
        }}
        label="Command palette"
      >
        <button
          type="button"
          aria-label="Close command palette"
          className="fixed inset-0 z-50 bg-black/30"
          onClick={() => setOpen(false)}
        />
        <div className="fixed left-1/2 top-20 z-50 w-full max-w-xl -translate-x-1/2 rounded-md border bg-card p-2 shadow-lg">
          <Command.Input
            className="w-full rounded-md border px-3 py-2 text-sm outline-none"
            placeholder="Type a command or search task..."
          />
          <Command.List className="mt-2 max-h-80 overflow-auto">
            <Command.Empty className="p-2 text-sm text-muted-foreground">No results</Command.Empty>
            {recentTasks.length > 0 ? (
              <>
                <Command.Group heading="Recent tasks">
                  {recentTasks.map((task) => (
                    <Command.Item
                      key={task.id}
                      className="cursor-pointer rounded p-2 text-sm aria-selected:bg-muted"
                      onSelect={() => navigateToTask(task)}
                    >
                      {task.title}
                    </Command.Item>
                  ))}
                </Command.Group>
                <Command.Separator className="my-2 h-px bg-border" />
              </>
            ) : null}
            <Command.Group heading="Actions">
              <Command.Item
                className="cursor-pointer rounded p-2 text-sm aria-selected:bg-muted"
                onSelect={() => {
                  setOpen(false)
                  setCreateOpen(true)
                }}
              >
                Create task
              </Command.Item>
              <Command.Item
                className="cursor-pointer rounded p-2 text-sm aria-selected:bg-muted"
                onSelect={() => {
                  setOpen(false)
                  void navigate({ to: '/projects/$projectId/board', params: { projectId } })
                }}
              >
                Go to board
              </Command.Item>
              <Command.Item
                className="cursor-pointer rounded p-2 text-sm aria-selected:bg-muted"
                onSelect={() => {
                  setOpen(false)
                  void navigate({
                    to: '/projects/$projectId/tasks',
                    params: { projectId },
                    search: { sort_by: 'updated_at', sort_order: 'desc' },
                  })
                }}
              >
                Go to tasks
              </Command.Item>
              <Command.Item
                className="cursor-pointer rounded p-2 text-sm aria-selected:bg-muted"
                onSelect={() => {
                  setOpen(false)
                  void navigate({ to: '/agents' })
                }}
              >
                Go to agents
              </Command.Item>
              <Command.Item
                className="cursor-pointer rounded p-2 text-sm aria-selected:bg-muted"
                onSelect={() => {
                  setOpen(false)
                  void navigate({ to: '/projects/$projectId/settings', params: { projectId } })
                }}
              >
                Go to settings
              </Command.Item>
              <Command.Item
                className="cursor-pointer rounded p-2 text-sm aria-selected:bg-muted"
                onSelect={() => {
                  setTheme(theme === 'light' ? 'dark' : 'light')
                  setOpen(false)
                }}
              >
                Toggle theme
              </Command.Item>
            </Command.Group>
            <Command.Separator className="my-2 h-px bg-border" />
            <Command.Group heading="Tasks">
              {taskItems.slice(0, 50).map((task) => (
                <Command.Item
                  key={task.id}
                  className="cursor-pointer rounded p-2 text-sm aria-selected:bg-muted"
                  onSelect={() => navigateToTask(task)}
                >
                  {task.title}
                </Command.Item>
              ))}
            </Command.Group>
          </Command.List>
        </div>
      </Command.Dialog>
      {createOpen ? (
        <Suspense fallback={null}>
          <TaskCreateDialog open projectId={projectId} onOpenChange={setCreateOpen} />
        </Suspense>
      ) : null}
    </>
  )
}
