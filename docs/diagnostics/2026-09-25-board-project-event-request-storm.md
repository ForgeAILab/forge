# Board project-event request storm

Captured on 2026-09-25 against the local Forge listener on
`127.0.0.1:18080`.

## Symptom

The board HTML and application shell loaded quickly, but opening a selected Task
could take more than a second while an Execution was emitting live events. In a
two-second browser sample, the page started 44 workflow requests, 17 task-list
requests, and 16 project-member requests. Blocking only the SSE connection
reduced those endpoints to one request each and rendered the selected Task in
about 324 ms.

## Cause

Project-scoped `domain_event.committed` frames invalidated both
`['projects', projectId]` and `['projects']`. TanStack Query treats those keys as
prefixes unless `exact` is set, so each frame also invalidated every descendant
query used by the board, including Tasks, workflow, members, and Project Agents.
With roughly 12.5 committed-domain-event frames per second, in-flight requests
were repeatedly cancelled and restarted.

## Source fix

Project-event routing now:

- invalidates the exact Project summary and exact non-paginated Project list;
- invalidates paginated Project lists through their dedicated
  `['projects', 'pages']` root;
- does not cancel an in-flight refresh; and
- coalesces bursts into at most one immediate and one trailing refresh per
  500 ms window.

A regression test uses a real `QueryClient` to prove that Project Tasks,
workflow, members, and Project Agents remain valid after a project-scoped
domain event. A second test verifies burst coalescing.

## Verification

- `pnpm exec vitest run src/api/sse.test.ts` — 29 passed
- `pnpm typecheck` — passed
- focused ESLint — passed
- `pnpm build` — passed

The listener running during diagnosis still embeds the previous production
bundle. It must be rebuilt and restarted before the live `:18080` page contains
this source fix.
