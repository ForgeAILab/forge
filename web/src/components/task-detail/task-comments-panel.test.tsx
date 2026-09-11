import {
  QueryClient,
  QueryClientProvider,
  type QueryClient as QueryClientType,
} from '@tanstack/react-query'
import { render, screen, waitFor } from '@testing-library/react'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { qk } from '@/api/query-keys'
import { routeSsePayload } from '@/api/sse'
import type { useCreateComment, useDeleteComment } from '@/api/hooks'
import type { Comment, Task, TaskMediaResponse } from '@/types/generated'
import { TaskCommentsPanel } from './task-comments-panel'

const now = '2026-05-19T12:00:00Z'
const originalCreateObjectURL = URL.createObjectURL
const originalRevokeObjectURL = URL.revokeObjectURL
let objectUrlCounter = 0

const task = {
  id: 'task-1',
  project_id: 'project-1',
  title: 'Task',
  task_type: 'task',
  status: 'todo',
  priority: 0,
  board_position: 0,
  role_assignments: [],
  remaining_retries: {},
  version: 1,
  created_at: now,
  updated_at: now,
} as Task

function comment(content: string): Comment {
  return {
    id: 'comment-1',
    task_id: task.id,
    author_type: 'user',
    author_id: 'user-1',
    author_name: 'You',
    content,
    created_at: now,
    updated_at: now,
  }
}

function media(overrides: Partial<TaskMediaResponse>): TaskMediaResponse {
  return {
    id: 'media-1',
    task_id: task.id,
    filename: 'file.txt',
    content_type: 'text/plain',
    byte_size: 12,
    url: '/api/v1/media/media-1',
    author_type: 'user',
    author_id: 'user-1',
    author_name: 'You',
    created_at: now,
    ...overrides,
  }
}

function mockMediaFetch(items: TaskMediaResponse[]) {
  return vi.spyOn(window, 'fetch').mockImplementation((input) => {
    const url =
      typeof input === 'string' ? input : input instanceof Request ? input.url : input.toString()
    if (url.includes(`/api/v1/tasks/${task.id}/media`)) {
      return Promise.resolve(
        new Response(JSON.stringify({ items, has_more: false }), {
          status: 200,
          headers: { 'content-type': 'application/json' },
        }),
      )
    }
    return Promise.resolve(
      new Response('media bytes', {
        status: 200,
        headers: { 'content-type': 'application/octet-stream' },
      }),
    )
  })
}

function renderPanel(content: string, mediaItems: TaskMediaResponse[] = []) {
  mockMediaFetch(mediaItems)
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: { retry: false },
      mutations: { retry: false },
    },
  })
  const createComment = {
    isPending: false,
    mutate: vi.fn(),
  } as unknown as ReturnType<typeof useCreateComment>
  const deleteComment = {
    isPending: false,
    mutate: vi.fn(),
  } as unknown as ReturnType<typeof useDeleteComment>

  const view = render(
    <QueryClientProvider client={queryClient}>
      <TaskCommentsPanel
        task={task}
        comments={[comment(content)]}
        commentDraft=""
        setCommentDraft={vi.fn()}
        createComment={createComment}
        deleteComment={deleteComment}
        formatDate={(value) => value ?? ''}
        onPostComment={vi.fn()}
      />
    </QueryClientProvider>,
  )

  return { queryClient, ...view }
}

describe('TaskCommentsPanel', () => {
  beforeEach(() => {
    objectUrlCounter = 0
    Object.defineProperty(URL, 'createObjectURL', {
      configurable: true,
      value: vi.fn(() => `blob:forge-media-${++objectUrlCounter}`),
    })
    Object.defineProperty(URL, 'revokeObjectURL', {
      configurable: true,
      value: vi.fn(),
    })
  })

  afterEach(() => {
    vi.restoreAllMocks()
    restoreUrlObjectMethod('createObjectURL', originalCreateObjectURL)
    restoreUrlObjectMethod('revokeObjectURL', originalRevokeObjectURL)
  })

  it('preserves Markdown rendering for text comments', () => {
    renderPanel('**Bold** text with [Forge docs](https://example.com/docs).')

    expect(screen.getByText('Bold').closest('strong')).not.toBeNull()
    const link = screen.getByRole('link', { name: 'Forge docs' }) as HTMLAnchorElement
    expect(link.href).toBe('https://example.com/docs')
  })

  it('renders task-owned image previews inline', async () => {
    renderPanel('![screenshot.png](/api/v1/media/image-1)', [
      media({
        id: 'image-1',
        filename: 'screenshot.png',
        content_type: 'image/png',
        url: '/api/v1/media/image-1',
      }),
    ])

    const image = (await screen.findByRole('img', {
      name: 'screenshot.png',
    })) as HTMLImageElement
    expect(image.getAttribute('src')).toMatch(/^blob:forge-media-/)
    expect(image.dataset.mediaUrl).toBe('/api/v1/media/image-1')
    expect(image.className).toContain('max-h-96')
  })

  it('renders task-owned video links as controlled previews', async () => {
    const { container } = renderPanel('[video: walkthrough.mp4](/api/v1/media/video-1)', [
      media({
        id: 'video-1',
        filename: 'walkthrough.mp4',
        content_type: 'video/mp4',
        url: '/api/v1/media/video-1',
      }),
    ])

    await waitFor(() => {
      expect(container.querySelector('video')).not.toBeNull()
    })
    const video = container.querySelector('video') as HTMLVideoElement
    expect(video.getAttribute('src')).toMatch(/^blob:forge-media-/)
    expect(video.dataset.mediaUrl).toBe('/api/v1/media/video-1')
    expect(video.controls).toBe(true)
    expect(video.className).toContain('max-h-96')
  })

  it('renders non-preview task media as downloadable links with file context', async () => {
    renderPanel('[spec.pdf](/api/v1/media/pdf-1)', [
      media({
        id: 'pdf-1',
        filename: 'spec.pdf',
        content_type: 'application/pdf',
        byte_size: 2048,
        url: '/api/v1/media/pdf-1',
      }),
    ])

    await waitFor(() => {
      const link = screen.getByRole('link', { name: /spec.pdf/ }) as HTMLAnchorElement
      expect(link.getAttribute('download')).toBe('spec.pdf')
      expect(link.textContent).toContain('application/pdf')
      expect(link.textContent).toContain('2 KB')
    })
  })

  it('strips script tags and external image URLs while keeping readable text', () => {
    const { container } = renderPanel(
      'Safe text\n\n<script>alert("x")</script>\n\n![bad](https://example.com/bad.png)',
    )

    expect(screen.getByText('Safe text')).not.toBeNull()
    expect(container.querySelector('script')).toBeNull()
    expect(container.querySelector('img')).toBeNull()
    expect(container.innerHTML).not.toContain('https://example.com/bad.png')
  })

  it('invalidates task media queries when media upload SSE events arrive', () => {
    const invalidateQueries = vi.fn()
    const queryClient = { invalidateQueries } as unknown as QueryClientType
    const dispatch = vi.fn()

    routeSsePayload(
      {
        event_type: 'task.media.uploaded',
        entity_id: 'media-1',
        task_id: task.id,
        media_id: 'media-1',
        timestamp: now,
      },
      queryClient,
      { dispatch },
    )

    expect(invalidateQueries.mock.calls).toEqual(
      expect.arrayContaining([[{ queryKey: qk.taskMedia(task.id) }]]),
    )
    expect(dispatch).not.toHaveBeenCalled()
  })
})

function restoreUrlObjectMethod(
  key: 'createObjectURL' | 'revokeObjectURL',
  original: typeof URL.createObjectURL | typeof URL.revokeObjectURL,
) {
  if (original) {
    Object.defineProperty(URL, key, { configurable: true, value: original })
  } else {
    Reflect.deleteProperty(URL, key)
  }
}
