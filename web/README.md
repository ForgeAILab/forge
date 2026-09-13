# Forge Web UI

The React + TypeScript frontend for Forge. Built with Vite, TanStack Query +
Router, Tailwind, and shadcn/ui. Talks to the Rust backend over `/api/v1/*`.

This is part of the Forge workspace — for the project overview see the root
[README](../README.md), and for the system architecture see
[docs/architecture.md](../docs/architecture.md).

## Stack

- **Vite** — dev server + build, with `/api` and `/mcp` proxy to `:8080`
- **React 18 + TypeScript** strict mode
- **TanStack Query** for server state, **TanStack Router** for routing
- **Tailwind CSS** + **shadcn/ui** (Radix primitives) for styling
- **Vitest** for unit tests, **Playwright** for end-to-end smoke tests
- **pnpm** as the package manager (enforced by `pnpm-lock.yaml`)

## Develop

```bash
pnpm install
pnpm dev            # Vite at http://localhost:5173, proxies /api → :8080
```

The dev server expects the Rust backend on `127.0.0.1:8080`. Start it from the
workspace root with `make dev` (data dir `./test/`) or
`cargo run -p forge-cli`.

## Build

```bash
pnpm build          # Production build to web/dist/
pnpm preview        # Serve the built bundle locally
```

The production `forge` binary serves `web/dist/` from
`/usr/local/share/forge/web/dist` (or whatever `FORGE_WEB_DIST_DIR` points at).

## Test and lint

```bash
pnpm lint           # ESLint, zero warnings allowed
pnpm typecheck      # tsc --noEmit
pnpm test           # Vitest unit tests
pnpm exec playwright install --with-deps chromium
pnpm run e2e        # Playwright smoke against a running stack
```

## Layout

```
src/
├── api/               # REST client, generated/hand-written request fns
├── components/        # Shared components (app-shell, notification-center, ui/)
├── pages/             # Route components
├── hooks/             # Reusable hooks (useTask, useEvents, …)
├── lib/               # Pure utilities, formatters
├── types/generated/   # TS types mirroring the Rust api-types crate
└── router.tsx         # TanStack Router config
```

The `@` path alias resolves to `src/` (see `tsconfig.json`).

## API client and types

`src/api/client.ts` is a thin `fetch` wrapper over `/api/v1/*`. Types in
`src/types/generated/api.ts` must match the `api-types` Rust crate response
shapes — when a backend response shape changes, update both in the same change.

## Branding assets

| File                          | Use                                                         |
| ----------------------------- | ----------------------------------------------------------- |
| `public/logo.png`             | Square mark, used as favicon and in the application top bar |
| `public/apple-touch-icon.png` | iOS touch icon (192px)                                      |
| `public/forge-wordmark.png`   | OG image / social share preview                             |

Sources are committed under [`/assets/`](../assets/) at the repo root —
re-export with `sips` if you need different sizes.
