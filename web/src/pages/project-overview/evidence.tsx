// Project evidence gallery and attachment previews.

import { useEffect, useState } from 'react'
import { ArrowUpRight, FileText, FilmStrip, ImageSquare } from '@phosphor-icons/react'
import { apiFetchBlob } from '@/api/client'
import { Button } from '@/components/ui/button'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import type { EvidenceAttachment } from '@/types/generated'
import { SectionCard, EmptyInline, StatusLabel } from './shared'
import { EVIDENCE_AVAILABILITY_COPY, formatDate, humanize, shortId, formatDuration } from './utils'

export function EvidenceGallery({
  projectId,
  evidence,
}: {
  projectId: string
  evidence: EvidenceAttachment[]
}) {
  const availableCount = evidence.filter((item) => item.availability === 'available').length
  return (
    <SectionCard
      title="Evidence"
      eyebrow="Bounded proof media"
      action={
        <span className="font-mono text-micro text-muted-foreground">
          Coverage {availableCount}/{evidence.length} available
        </span>
      }
    >
      {evidence.length === 0 ? (
        <EmptyInline text="No evidence is attached to this Project projection yet. Evidence capture remains available from Tasks and Project Agent Chat." />
      ) : (
        <div
          className="min-w-0 overflow-x-auto overscroll-x-contain pb-1 snap-x snap-mandatory focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring min-[520px]:overflow-visible min-[520px]:snap-none"
          role="region"
          aria-label="Evidence gallery"
          tabIndex={0}
        >
          <div className="flex min-w-0 gap-3 min-[520px]:grid min-[520px]:grid-cols-2">
            {evidence.map((item) => (
              <div
                key={item.id}
                className="min-w-[min(18rem,calc(100vw-4rem))] shrink-0 snap-start min-[520px]:min-w-0 min-[520px]:shrink"
              >
                <EvidenceTile projectId={projectId} item={item} />
              </div>
            ))}
          </div>
        </div>
      )}
    </SectionCard>
  )
}

function EvidenceTile({ projectId, item }: { projectId: string; item: EvidenceAttachment }) {
  const [mediaUrl, setMediaUrl] = useState<string | null>(null)
  const [previewLoading, setPreviewLoading] = useState(false)
  const [previewError, setPreviewError] = useState(false)
  const [previewAttempt, setPreviewAttempt] = useState(0)
  const [duration, setDuration] = useState<number | null>(null)
  const [previewOpen, setPreviewOpen] = useState(false)
  const [textPreview, setTextPreview] = useState<string | null>(null)
  const [blobType, setBlobType] = useState<string | null>(null)
  const isVideo = item.kind === 'walkthrough_video'
  const hasVisualPreview = item.kind === 'screenshot' || isVideo
  const mediaPath = `/projects/${projectId}/media/${encodeURIComponent(item.asset_id)}`
  const icon = isVideo ? (
    <FilmStrip size={24} aria-hidden />
  ) : item.kind === 'screenshot' ? (
    <ImageSquare size={24} aria-hidden />
  ) : (
    <FileText size={24} aria-hidden />
  )
  const sourceTaskId = item.source_task_id ?? item.task_id
  const provenance = [
    sourceTaskId ? `Task ${shortId(sourceTaskId)}` : null,
    item.source_run_id ? `run ${shortId(item.source_run_id)}` : null,
    item.source_validation_id ? `validation ${shortId(item.source_validation_id)}` : null,
    item.author ? `uploaded by ${item.author.display_name ?? shortId(item.author.id)}` : null,
  ].filter((value): value is string => Boolean(value))
  const showPreview =
    item.availability === 'available' && hasVisualPreview && !previewError && Boolean(mediaUrl)

  useEffect(() => {
    let cancelled = false
    let objectUrl: string | null = null
    const shouldLoad = item.availability === 'available'

    setMediaUrl(null)
    setPreviewError(false)
    setPreviewLoading(shouldLoad)
    setDuration(null)
    setTextPreview(null)
    setBlobType(null)
    if (!shouldLoad) return

    void apiFetchBlob(mediaPath)
      .then((blob) => {
        if (cancelled) return
        if (typeof URL.createObjectURL !== 'function') {
          setPreviewError(true)
          return
        }
        objectUrl = URL.createObjectURL(blob)
        setMediaUrl(objectUrl)
        setBlobType(blob.type || null)
        // Text-like evidence (logs, reports) previews inline in the modal; the
        // bytes are already authorized and in hand, so read a bounded slice.
        const isTextLike =
          !hasVisualPreview && (blob.type.startsWith('text/') || blob.type === 'application/json')
        if (isTextLike && typeof blob.text === 'function') {
          void blob
            .text()
            .then((text) => {
              if (!cancelled) setTextPreview(text.slice(0, 20_000))
            })
            .catch(() => {})
        }
      })
      .catch(() => {
        if (!cancelled) setPreviewError(true)
      })
      .finally(() => {
        if (!cancelled) setPreviewLoading(false)
      })

    return () => {
      cancelled = true
      if (objectUrl) URL.revokeObjectURL(objectUrl)
    }
  }, [hasVisualPreview, item.availability, mediaPath, previewAttempt])

  function failPreview() {
    setPreviewError(true)
    if (mediaUrl) {
      URL.revokeObjectURL(mediaUrl)
      setMediaUrl(null)
    }
  }

  return (
    <article className="min-w-0 overflow-hidden rounded-md border border-border-subtle bg-background">
      <div className="flex aspect-video items-center justify-center border-b border-border-subtle bg-muted text-muted-foreground">
        {previewLoading ? (
          <div className="flex flex-col items-center gap-2 px-4 text-center">
            {icon}
            <p className="text-xs font-medium">
              Loading authorized {hasVisualPreview ? 'preview' : 'evidence file'}…
            </p>
            <p className="text-micro text-muted-foreground">
              Forge is fetching this evidence with your Project authorization.
            </p>
          </div>
        ) : showPreview ? (
          isVideo ? (
            <video
              src={mediaUrl ?? undefined}
              controls
              preload="metadata"
              poster="/logo.png"
              playsInline
              width="640"
              height="360"
              aria-label={item.caption}
              className="h-full w-full object-cover"
              onLoadedMetadata={(event) => setDuration(event.currentTarget.duration)}
              onError={failPreview}
            />
          ) : (
            <img
              src={mediaUrl ?? undefined}
              alt={item.caption}
              loading="lazy"
              width="640"
              height="360"
              className="h-full w-full object-cover"
              onError={failPreview}
            />
          )
        ) : (
          <div className="flex flex-col items-center gap-2 px-4 text-center">
            {icon}
            <p className="text-xs font-medium">
              {item.availability !== 'available'
                ? EVIDENCE_AVAILABILITY_COPY[item.availability]
                : isVideo
                  ? 'Video poster'
                  : item.kind === 'screenshot'
                    ? 'Image preview'
                    : 'Evidence file'}
            </p>
            <p className="text-micro text-muted-foreground">
              {previewError
                ? `${hasVisualPreview ? 'Preview' : 'File'} could not be loaded; metadata is preserved.`
                : item.availability !== 'available'
                  ? 'Metadata is retained, but this asset is not openable in the current state.'
                  : hasVisualPreview
                    ? 'Preview opens from the authorized asset'
                    : 'Open or download from the authorized asset'}
            </p>
            {previewError ? (
              <button
                type="button"
                className="mt-1 text-xs font-medium text-primary underline-offset-4 hover:underline focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                onClick={() => setPreviewAttempt((attempt) => attempt + 1)}
              >
                Retry authorized asset
              </button>
            ) : null}
          </div>
        )}
      </div>
      <div className="min-w-0 p-3">
        <div className="flex min-w-0 flex-wrap items-start justify-between gap-2">
          <p className="min-w-0 break-words text-sm font-medium text-foreground">{item.caption}</p>
          <StatusLabel status={item.availability} />
        </div>
        <p className="mt-1 break-words text-xs text-muted-foreground">
          {humanize(item.kind)} · captured {formatDate(item.captured_at)}
        </p>
        {isVideo && item.availability === 'available' ? (
          <p className="mt-1 text-xs text-muted-foreground">
            {formatDuration(duration)} · explicit play controls; video never autoplays
          </p>
        ) : null}
        <p className="mt-1 break-all font-mono text-micro text-muted-foreground">
          asset {shortId(item.asset_id)} · checksum {shortId(item.checksum)}
        </p>
        <p className="mt-2 break-words text-xs text-muted-foreground">
          {provenance.length > 0 ? provenance.join(' · ') : 'Source provenance not recorded'}
        </p>
        {item.acceptance_check_ids.length > 0 ? (
          <p className="mt-2 break-all text-xs text-muted-foreground">
            Supports checks: {item.acceptance_check_ids.map(shortId).join(', ')}
          </p>
        ) : (
          <p className="mt-2 text-xs text-warning">
            No acceptance check linkage; not proof for a check.
          </p>
        )}
        <div className="mt-3 flex flex-wrap gap-2">
          {item.availability === 'available' ? (
            <>
              {mediaUrl ? (
                <>
                  <button
                    type="button"
                    onClick={() => setPreviewOpen(true)}
                    className="inline-flex items-center gap-1 rounded-md border border-input px-2 py-1 text-xs font-medium text-foreground transition-colors hover:bg-muted focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  >
                    Preview{' '}
                    {isVideo ? 'video' : item.kind === 'screenshot' ? 'image' : 'evidence file'}
                  </button>
                  <a
                    href={mediaUrl}
                    download
                    className="inline-flex items-center rounded-md px-2 py-1 text-xs font-medium text-primary transition-colors hover:bg-primary/10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                  >
                    Download
                  </a>
                </>
              ) : (
                <span className="text-xs text-muted-foreground">
                  {previewLoading ? 'Loading authorized asset…' : 'Authorized asset unavailable'}
                </span>
              )}
            </>
          ) : (
            <span className="text-xs text-muted-foreground">
              {EVIDENCE_AVAILABILITY_COPY[item.availability]}
            </span>
          )}
        </div>
      </div>
      <Dialog
        open={previewOpen}
        onOpenChange={setPreviewOpen}
        ariaLabel={`Evidence preview: ${item.caption}`}
      >
        <DialogContent className="max-w-4xl">
          <DialogHeader>
            <DialogTitle>{item.caption}</DialogTitle>
            <DialogDescription>
              {humanize(item.kind)} · captured {formatDate(item.captured_at)} · asset{' '}
              {shortId(item.asset_id)}
            </DialogDescription>
          </DialogHeader>
          <div className="mt-3 max-h-[70vh] overflow-auto rounded-md border border-border-subtle bg-muted/20">
            {!mediaUrl ? (
              <p className="p-4 text-sm text-muted-foreground">Authorized asset unavailable.</p>
            ) : isVideo ? (
              <video
                src={mediaUrl}
                controls
                preload="metadata"
                playsInline
                aria-label={`${item.caption} preview`}
                className="mx-auto max-h-[70vh] w-full"
              />
            ) : item.kind === 'screenshot' ? (
              <img
                src={mediaUrl}
                alt={`${item.caption} preview`}
                className="mx-auto max-h-[70vh] w-auto"
              />
            ) : textPreview !== null ? (
              <pre className="whitespace-pre-wrap p-3 font-mono text-xs leading-5 text-foreground">
                {textPreview}
              </pre>
            ) : blobType === 'application/pdf' ? (
              <iframe src={mediaUrl} title={`${item.caption} preview`} className="h-[70vh] w-full" />
            ) : (
              <p className="p-4 text-sm text-muted-foreground">
                No inline preview for this file type. Open it in a new tab or download it.
              </p>
            )}
          </div>
          <DialogFooter className="mt-4 gap-2">
            {mediaUrl ? (
              <>
                <a
                  href={mediaUrl}
                  target="_blank"
                  rel="noreferrer"
                  className="inline-flex items-center gap-1 rounded-md border border-input px-3 py-1.5 text-xs font-medium text-foreground transition-colors hover:bg-muted focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                >
                  Open in new tab <ArrowUpRight size={13} aria-hidden />
                </a>
                <a
                  href={mediaUrl}
                  download
                  className="inline-flex items-center rounded-md px-3 py-1.5 text-xs font-medium text-primary transition-colors hover:bg-primary/10 focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring"
                >
                  Download
                </a>
              </>
            ) : null}
            <Button type="button" onClick={() => setPreviewOpen(false)}>
              Close
            </Button>
          </DialogFooter>
        </DialogContent>
      </Dialog>
    </article>
  )
}
