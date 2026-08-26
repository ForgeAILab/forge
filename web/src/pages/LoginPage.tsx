import { useState } from 'react'
import { Link, useNavigate, useSearch } from '@tanstack/react-router'
import { login, getMe } from '@/api/auth'
import { useAuthStore } from '@/stores/auth'
import { Input } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Button } from '@/components/ui/button'

export function LoginPage() {
  const navigate = useNavigate()
  const search = useSearch({ strict: false }) as { redirect?: string; redirect_params?: string }
  const setAuth = useAuthStore((s) => s.setAuth)

  const [email, setEmail] = useState('')
  const [password, setPassword] = useState('')
  const [error, setError] = useState('')
  const [loading, setLoading] = useState(false)

  async function handleSubmit(e: React.FormEvent) {
    e.preventDefault()
    setError('')
    setLoading(true)
    try {
      const auth = await login({ email: email.trim(), password })
      const user = await getMe(auth.access_token)
      setAuth(auth, user)
      const safePath =
        typeof search.redirect === 'string' && search.redirect.startsWith('/')
          ? search.redirect
          : '/'
      let redirectSearch: Record<string, string> | undefined
      if (search.redirect_params) {
        try {
          redirectSearch = JSON.parse(search.redirect_params) as Record<string, string>
        } catch {
          redirectSearch = undefined
        }
      }
      void navigate({ to: safePath as '/', search: redirectSearch })
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Login failed')
    } finally {
      setLoading(false)
    }
  }

  return (
    <main className="flex min-h-dvh items-center justify-center bg-background p-4">
      <div className="w-full max-w-sm space-y-6">
        <div className="flex flex-col items-center gap-2">
          <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-primary shadow-[0_0_16px_rgba(249,115,22,0.3)]">
            <span className="text-sm font-bold text-primary-foreground">F</span>
          </div>
          <h1 className="text-xl font-semibold tracking-tight text-foreground">Sign in to Forge</h1>
        </div>

        <form onSubmit={handleSubmit} className="space-y-4">
          <div className="space-y-1.5">
            <Label htmlFor="email">Email</Label>
            <Input
              id="email"
              type="email"
              autoComplete="email"
              placeholder="you@example.com"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              required
            />
          </div>
          <div className="space-y-1.5">
            <Label htmlFor="password">Password</Label>
            <Input
              id="password"
              type="password"
              autoComplete="current-password"
              placeholder="••••••••"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              required
            />
          </div>

          {error && (
            <p className="rounded-md bg-destructive/10 px-3 py-2 text-sm text-destructive">
              {error}
            </p>
          )}

          <Button type="submit" disabled={loading} className="w-full">
            {loading ? 'Signing in…' : 'Sign in'}
          </Button>
        </form>

        <p className="text-center text-sm text-muted-foreground">
          No account?{' '}
          <Link to="/register" className="font-medium text-primary hover:underline">
            Create one
          </Link>
        </p>
      </div>
    </main>
  )
}
