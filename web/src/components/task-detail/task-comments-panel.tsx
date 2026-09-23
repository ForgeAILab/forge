import { DownloadSimple, GearSix, Paperclip, Spinner } from '@phosphor-icons/react'
import { toast } from 'sonner'
import ReactMarkdown, { defaultUrlTransform, type Components } from 'react-markdown'
import rehypeSanitize, {
  defaultSchema,
  type Options as RehypeSanitizeSchema,
} from 'rehype-sanitize'
import remarkGfm from 'remark-gfm'
import {
  useCreateComment,
  useDeleteComment,
  useTaskMediaQuery,
  useUploadTaskMedia,
} from '@/api/hooks'
import { apiFetchBlob } from '@/api/client'
import { ErrorBanner } from '@/components/error-banner'
import { Button } from '@/components/ui/button'
import { Skeleton } from '@/components/ui/skeleton'
import { Textarea } from '@/components/ui/textarea'
import { Avatar } from '@/components/ui/avatar'
import { getApiErrorMessage, isTransientApiError } from '@/lib/api-error'
import { cn } from '@/lib/cn'
import {
  useEffect,
  useMemo,
  useRef,
  useState,
  type ChangeEvent,
  type Dispatch,
  type ReactNode,
  type SetStateAction,
} from 'react'
import type { Comment, Task, TaskMediaResponse } from '@/types/generated'

const TASK_MEDIA_URL_PREFIX = '/api/v1/media/'
const TASK_MEDIA_URL_PATTERN = /^\/api\/v1\/media\/[^\s]*$/
const API_V1_PREFIX = '/api/v1'

const taskCommentSanitizeSchema: RehypeSanitizeSchema = {
  ...defaultSchema,
  tagNames: [...(defaultSchema.tagNames ?? []), 'video'],
  attributes: {
    ...defaultSchema.attributes,
    img: [
      ...(defaultSchema.attributes?.img ?? []).filter((attribute) => attribute !== 'src'),
      'alt',
      'title',
      ['loading', 'lazy', 'eager'],
      ['src', TASK_MEDIA_URL_PATTERN],
    ],
    video: [
      ['src', TASK_MEDIA_URL_PATTERN],
      ['controls', true, 'true', ''],
      'width',
      'height',
      ['poster', TASK_MEDIA_URL_PATTERN],
      ['preload', 'none', 'metadata', 'auto', ''],
    ],
  },
}

function isTaskMediaUrl(value: unknown): value is string {
  return typeof value === 'string' && value.startsWith(TASK_MEDIA_URL_PREFIX)
}

function isImageMedia(media: Pick<TaskMediaResponse, 'content_type'>): boolean {
  return media.content_type.startsWith('image/')
}

function isVideoMedia(media: Pick<TaskMediaResponse, 'content_type'>): boolean {
  return media.content_type.startsWith('video/')
}

function mediaApiPath(url: string): string {
  return url.startsWith(API_V1_PREFIX) ? url.slice(API_V1_PREFIX.length) : url
}

function useTaskMediaObjectUrls(mediaItems: TaskMediaResponse[] | undefined) {
  const [objectUrls, setObjectUrls] = useState<Map<string, string>>(() => new Map())

  useEffect(() => {
    const previewMedia = (mediaItems ?? []).filter(
      (media) => isImageMedia(media) || isVideoMedia(media),
    )
    if (previewMedia.length === 0) {
      setObjectUrls(new Map())
      return
    }

    let cancelled = false
    const createdObjectUrls: string[] = []

    void Promise.all(
      previewMedia.map(async (media) => {
        try {
          const blob = await apiFetchBlob(mediaApiPath(media.url))
          const objectUrl = URL.createObjectURL(blob)
          if (cancelled) {
            URL.revokeObjectURL(objectUrl)
            return null
          }
          createdObjectUrls.push(objectUrl)
          return [media.url, objectUrl] as const
        } catch {
          return null
        }
      }),
    ).then((entries) => {
      if (cancelled) return
      setObjectUrls(
        new Map(entries.filter((entry): entry is readonly [string, string] => Boolean(entry))),
      )
    })

    return () => {
      cancelled = true
      for (const objectUrl of createdObjectUrls) {
        URL.revokeObjectURL(objectUrl)
      }
    }
  }, [mediaItems])

  return objectUrls
}

function taskCommentUrlTransform(url: string, key: string, node: { tagName?: string }) {
  if (key === 'src' && (node.tagName === 'img' || node.tagName === 'video')) {
    return isTaskMediaUrl(url) ? url : ''
  }
  return defaultUrlTransform(url)
}

function escapeMarkdownLabel(value: string): string {
  return value.replace(/\\/g, '\\\\').replace(/\[/g, '\\[').replace(/\]/g, '\\]')
}

function markdownReferenceForMedia(media: TaskMediaResponse): string {
  const filename = escapeMarkdownLabel(media.filename)
  if (isImageMedia(media)) return `![${filename}](${media.url})`
  if (isVideoMedia(media)) return `[video: ${filename}](${media.url})`
  return `[${filename}](${media.url})`
}

function appendMarkdownReference(draft: string, reference: string): string {
  if (!draft.trim()) return reference
  return `${draft}${draft.endsWith('\n') ? '' : '\n\n'}${reference}`
}

function formatByteSize(bytes: number): string {
  const units = ['B', 'KB', 'MB', 'GB']
  let value = bytes
  let unitIndex = 0
  while (value >= 1024 && unitIndex < units.length - 1) {
    value /= 1024
    unitIndex += 1
  }
  const rounded = value >= 10 || Number.isInteger(value) ? value.toFixed(0) : value.toFixed(1)
  return `${rounded} ${units[unitIndex]}`
}

function TaskMediaDownloadLink({
  href,
  media,
  objectUrl,
  label,
  className,
}: {
  href: string
  media?: TaskMediaResponse
  objectUrl?: string
  label: ReactNode
  className?: string
}) {
  return (
    <a
      href={objectUrl ?? href}
      data-media-url={href}
      download={media?.filename ?? true}
      className={cn(
        'not-prose my-2 inline-flex max-w-full items-center gap-2 rounded-md border bg-card px-3 py-2 text-sm text-foreground hover:bg-muted',
        className,
      )}
    >
      <DownloadSimple size={16} className="shrink-0 text-muted-foreground" />
      <span className="min-w-0 truncate">{media?.filename ?? label}</span>
      {media ? (
        <>
          <span className="shrink-0 rounded bg-muted px-1.5 py-0.5 text-[11px] text-muted-foreground">
            {media.content_type}
          </span>
          <span className="shrink-0 rounded bg-muted px-1.5 py-0.5 text-[11px] text-muted-foreground">
            {formatByteSize(media.byte_size)}
          </span>
        </>
      ) : null}
    </a>
  )
}

function createMarkdownComponents(
  mediaByUrl: Map<string, TaskMediaResponse>,
  mediaObjectUrls: Map<string, string>,
): Components {
  return {
    a({ href, children, className, ...props }) {
      delete (props as { node?: unknown }).node
      if (isTaskMediaUrl(href)) {
        const media = mediaByUrl.get(href)
        const objectUrl = mediaObjectUrls.get(href)
        if (media && isImageMedia(media) && objectUrl) {
          return (
            <img
              src={objectUrl}
              alt={media.filename}
              data-media-url={href}
              loading="lazy"
              className="my-2 max-h-96 max-w-full rounded-md border object-contain"
            />
          )
        }
        if (media && isVideoMedia(media) && objectUrl) {
          return (
            <video
              src={objectUrl}
              controls
              preload="metadata"
              aria-label={media.filename}
              data-media-url={href}
              className="my-2 max-h-96 w-full max-w-full rounded-md border bg-black object-contain"
            />
          )
        }
        return (
          <TaskMediaDownloadLink
            href={href}
            media={media}
            objectUrl={objectUrl}
            label={children}
            className={className}
          />
        )
      }
      return (
        <a href={href} className={className} {...props}>
          {children}
        </a>
      )
    },
    img({ src, alt, title, className, ...props }) {
      delete (props as { node?: unknown }).node
      if (!isTaskMediaUrl(src)) return null
      const media = mediaByUrl.get(src)
      const objectUrl = mediaObjectUrls.get(src)
      if (!media) {
        return <TaskMediaDownloadLink href={src} label={alt ?? src} />
      }
      if (media && !isImageMedia(media)) {
        return (
          <TaskMediaDownloadLink
            href={src}
            media={media}
            objectUrl={objectUrl}
            label={alt ?? media.filename}
          />
        )
      }
      if (media && !objectUrl) {
        return <TaskMediaDownloadLink href={src} media={media} label={alt ?? media.filename} />
      }
      return (
        <img
          src={objectUrl ?? src}
          alt={alt ?? ''}
          title={title}
          data-media-url={src}
          loading="lazy"
          className={cn('my-2 max-h-96 max-w-full rounded-md border object-contain', className)}
          {...props}
        />
      )
    },
    video({ src, poster, preload, className, ...props }) {
      delete (props as { node?: unknown }).node
      if (!isTaskMediaUrl(src)) return null
      const objectUrl = mediaObjectUrls.get(src)
      if (!objectUrl) {
        const media = mediaByUrl.get(src)
        return <TaskMediaDownloadLink href={src} media={media} label={media?.filename ?? src} />
      }
      const safePoster = isTaskMediaUrl(poster) ? poster : undefined
      return (
        <video
          src={objectUrl}
          poster={safePoster}
          controls
          preload={preload ?? 'metadata'}
          data-media-url={src}
          className={cn(
            'my-2 max-h-96 w-full max-w-full rounded-md border bg-black object-contain',
            className,
          )}
          {...props}
        />
      )
    },
  }
}

interface TaskCommentsPanelProps {
  task: Task
  comments: Comment[]
  commentsLoading?: boolean
  commentsIsError?: boolean
  commentsError?: unknown
  onRetryComments?: () => void
  commentDraft: string
  setCommentDraft: Dispatch<SetStateAction<string>>
  createComment: ReturnType<typeof useCreateComment>
  deleteComment: ReturnType<typeof useDeleteComment>
  formatDate: (value?: string | null) => string
  onPostComment: () => void
}

export function TaskCommentsPanel({
  task,
  comments,
  commentsLoading = false,
  commentsIsError = false,
  commentsError,
  onRetryComments,
  commentDraft,
  setCommentDraft,
  createComment,
  deleteComment,
  formatDate,
  onPostComment,
}: TaskCommentsPanelProps) {
  const fileInputRef = useRef<HTMLInputElement>(null)
  const taskMedia = useTaskMediaQuery(task.id)
  const uploadTaskMedia = useUploadTaskMedia()
  const mediaByUrl = useMemo(
    () => new Map((taskMedia.data ?? []).map((media) => [media.url, media])),
    [taskMedia.data],
  )
  const mediaObjectUrls = useTaskMediaObjectUrls(taskMedia.data)
  const markdownComponents = useMemo(
    () => createMarkdownComponents(mediaByUrl, mediaObjectUrls),
    [mediaByUrl, mediaObjectUrls],
  )

  const onFileSelected = (event: ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0]
    event.target.value = ''
    if (!file) return

    const toastId = toast.loading(`Uploading ${file.name}`)
    uploadTaskMedia.mutate(
      { taskId: task.id, file, authorName: 'You' },
      {
        onSuccess: (media) => {
          setCommentDraft((draft) =>
            appendMarkdownReference(draft, markdownReferenceForMedia(media)),
          )
          toast.success('Attachment added to comment draft', { id: toastId })
        },
        onError: (error) => {
          toast.error(getApiErrorMessage(error, 'Upload failed'), { id: toastId })
        },
      },
    )
  }

  return (
    <>
      <div className="space-y-2 rounded-lg border p-4">
        {commentsIsError ? (
          <ErrorBanner
            error={commentsError}
            fallback="Comments failed to load"
            onRetry={() => void onRetryComments?.()}
            showRetry={isTransientApiError(commentsError)}
          />
        ) : null}
        {commentsLoading ? (
          <Skeleton className="h-24 w-full" />
        ) : comments.length > 0 ? (
          comments.map((comment) => (
            <div key={comment.id} className="rounded-md border p-3">
              <div className="flex items-center justify-between gap-2">
                <div className="flex items-center gap-1.5 text-xs text-muted-foreground">
                  {comment.author_type === 'system' ? (
                    <GearSix size={12} />
                  ) : (
                    <Avatar
                      name={comment.author_name}
                      seed={comment.author_id ?? comment.author_name}
                      size="xs"
                    />
                  )}
                  <span className="font-medium text-foreground">{comment.author_name}</span>{' '}
                  <span>· {formatDate(comment.created_at)}</span>
                </div>
                {comment.author_type === 'user' ? (
                  <Button
                    size="sm"
                    variant="ghost"
                    className="h-7 px-2 text-xs"
                    disabled={deleteComment.isPending}
                    onClick={() =>
                      deleteComment.mutate(
                        { taskId: task.id, commentId: comment.id },
                        {
                          onError: (error) =>
                            toast.error(getApiErrorMessage(error, 'Delete failed')),
                        },
                      )
                    }
                  >
                    Delete
                  </Button>
                ) : null}
              </div>
              <div
                className={cn(
                  'mt-2 prose prose-sm max-w-none dark:prose-invert',
                  comment.author_type === 'system' && 'text-muted-foreground',
                )}
              >
                <ReactMarkdown
                  remarkPlugins={[remarkGfm]}
                  rehypePlugins={[[rehypeSanitize, taskCommentSanitizeSchema]]}
                  components={markdownComponents}
                  urlTransform={taskCommentUrlTransform}
                  skipHtml
                >
                  {comment.content}
                </ReactMarkdown>
              </div>
            </div>
          ))
        ) : !commentsIsError ? (
          <p className="text-sm text-muted-foreground">No comments yet.</p>
        ) : null}
      </div>
      <div className="space-y-2 rounded-lg border p-4">
        <Textarea
          placeholder="Add a comment"
          value={commentDraft}
          onChange={(event) => setCommentDraft(event.target.value)}
        />
        <div className="flex items-center justify-between gap-2">
          <input ref={fileInputRef} type="file" className="hidden" onChange={onFileSelected} />
          <Button
            size="sm"
            variant="outline"
            disabled={uploadTaskMedia.isPending}
            onClick={() => fileInputRef.current?.click()}
          >
            {uploadTaskMedia.isPending ? (
              <Spinner className="h-3.5 w-3.5 animate-spin" />
            ) : (
              <Paperclip size={14} />
            )}
            Attach file
          </Button>
          <Button
            size="sm"
            disabled={createComment.isPending || uploadTaskMedia.isPending || !commentDraft.trim()}
            onClick={onPostComment}
          >
            Post
          </Button>
        </div>
      </div>
    </>
  )
}
