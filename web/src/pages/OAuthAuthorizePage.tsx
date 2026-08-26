import { useEffect, useMemo, useState } from 'react'
import type { ReactNode } from 'react'
import { useNavigate, useSearch } from '@tanstack/react-router'
import { ApiError } from '@/api/client'
import { useOAuthApprove, useOAuthAuthorizeContext } from '@/api/hooks'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '@/components/ui/card'
import { Skeleton } from '@/components/ui/skeleton'
import { useAuthStore } from '@/stores/auth'
import type { OAuthApproveRequest } from '@/types/generated'

const OAUTH_PARAM_KEYS = [
  'response_type',
  'client_id',
  'redirect_uri',
  'resource',
  'scope',
  'state',
  'code_challenge',
  'code_challenge_method',
] as const

type OAuthParamKey = (typeof OAUTH_PARAM_KEYS)[number]
type OAuthParams = Partial<Record<OAuthParamKey, string>>

function pickOAuthParams(search: Partial<Record<string, unknown>>): OAuthParams {
  const params: OAuthParams = {}
  for (const key of OAUTH_PARAM_KEYS) {
    const value = search[key]
    if (typeof value === 'string') {
      params[key] = value
    }
  }
  return params
}

function getOAuthErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof ApiError) {
    try {
      const parsed = JSON.parse(error.message) as {
        error_description?: unknown
        message?: unknown
      }
      if (typeof parsed.error_description === 'string' && parsed.error_description.trim()) {
        return parsed.error_description
      }
      if (typeof parsed.message === 'string' && parsed.message.trim()) {
        return parsed.message
      }
    } catch {
      // Avoid surfacing raw OAuth request details from unstructured responses.
    }
  }
  return fallback
}

function buildApproveRequest(
  params: OAuthParams,
  decision: OAuthApproveRequest['decision'],
): OAuthApproveRequest | null {
  if (
    !params.response_type ||
    !params.client_id ||
    !params.redirect_uri ||
    !params.resource ||
    !params.scope ||
    !params.code_challenge ||
    !params.code_challenge_method
  ) {
    return null
  }

  return {
    response_type: params.response_type,
    client_id: params.client_id,
    redirect_uri: params.redirect_uri,
    resource: params.resource,
    scope: params.scope,
    state: params.state ?? null,
    code_challenge: params.code_challenge,
    code_challenge_method: params.code_challenge_method,
    decision,
  }
}

function OAuthCard({ children }: { children: ReactNode }) {
  return (
    <main className="flex min-h-dvh items-center justify-center bg-background p-4">
      <Card className="w-full max-w-sm">
        <CardHeader className="items-center text-center">
          <div className="flex items-center gap-2">
            <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-primary shadow-[0_0_16px_rgba(249,115,22,0.3)]">
              <span className="text-sm font-bold text-primary-foreground">F</span>
            </div>
            <span className="text-lg font-semibold tracking-tight text-foreground">Forge</span>
          </div>
          <CardTitle className="text-xl">Authorize MCP access</CardTitle>
          <CardDescription>Approve access for this MCP client.</CardDescription>
        </CardHeader>
        {children}
      </Card>
    </main>
  )
}

function LoadingCard() {
  return (
    <OAuthCard>
      <CardContent className="space-y-4">
        <Skeleton className="h-5 w-3/4" />
        <Skeleton className="h-12 w-full" />
        <div className="grid grid-cols-2 gap-2">
          <Skeleton className="h-9 w-full" />
          <Skeleton className="h-9 w-full" />
        </div>
      </CardContent>
    </OAuthCard>
  )
}

function ErrorCard({ message }: { message: string }) {
  return (
    <OAuthCard>
      <CardContent>
        <p className="rounded-md bg-destructive/10 px-3 py-2 text-sm text-destructive">{message}</p>
      </CardContent>
    </OAuthCard>
  )
}

export function OAuthAuthorizePage() {
  const navigate = useNavigate()
  const rawSearch = useSearch({ strict: false }) as Partial<Record<string, unknown>>
  const oauthParams = useMemo(() => pickOAuthParams(rawSearch), [rawSearch])
  const redirectParams = useMemo(() => JSON.stringify(oauthParams), [oauthParams])
  const accessToken = useAuthStore((s) => s.accessToken)
  const contextQuery = useOAuthAuthorizeContext(accessToken ? oauthParams : {})
  const approveMutation = useOAuthApprove()
  const [actionError, setActionError] = useState<string | null>(null)

  const hasRequiredParams = Boolean(
    oauthParams.client_id && oauthParams.redirect_uri && oauthParams.response_type,
  )

  useEffect(() => {
    if (accessToken) return
    void navigate({
      to: '/login',
      search: { redirect: '/oauth/authorize/consent', redirect_params: redirectParams },
      replace: true,
    })
  }, [accessToken, navigate, redirectParams])

  async function handleDecision(decision: OAuthApproveRequest['decision']) {
    setActionError(null)
    const request = buildApproveRequest(oauthParams, decision)
    if (!request) {
      setActionError('Unable to complete authorization request')
      return
    }

    try {
      const response = await approveMutation.mutateAsync(request)
      window.location.href = response.redirect_to
    } catch (error) {
      setActionError(getOAuthErrorMessage(error, 'Unable to complete authorization request'))
    }
  }

  if (!accessToken) {
    return <LoadingCard />
  }

  if (!hasRequiredParams) {
    return <ErrorCard message="Unable to load authorization request" />
  }

  if (actionError) {
    return <ErrorCard message={actionError} />
  }

  if (contextQuery.isError) {
    return (
      <ErrorCard
        message={getOAuthErrorMessage(contextQuery.error, 'Unable to load authorization request')}
      />
    )
  }

  if (contextQuery.isLoading || !contextQuery.data) {
    return <LoadingCard />
  }

  const clientName = contextQuery.data.client_name?.trim() || 'MCP client'
  const isPending = approveMutation.isPending

  return (
    <OAuthCard>
      <CardContent className="space-y-5">
        <div className="rounded-md border bg-muted/40 p-4">
          <p className="text-xs font-semibold uppercase text-muted-foreground">Client</p>
          <p className="mt-1 text-sm font-medium text-foreground">{clientName}</p>
        </div>
        <div className="rounded-md border bg-muted/40 p-4">
          <p className="text-xs font-semibold uppercase text-muted-foreground">Scope</p>
          <span className="mt-2 inline-flex rounded-md border bg-card px-2 py-1 text-xs font-medium text-foreground">
            mcp
          </span>
        </div>
        <div className="grid grid-cols-2 gap-2">
          <Button
            variant="outline"
            disabled={isPending}
            onClick={() => void handleDecision('deny')}
          >
            Cancel
          </Button>
          <Button disabled={isPending} onClick={() => void handleDecision('approve')}>
            Approve
          </Button>
        </div>
      </CardContent>
    </OAuthCard>
  )
}
