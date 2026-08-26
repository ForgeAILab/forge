# Forge Design System

## 1. Atmosphere & Identity

Forge is a focused foundry: calm, compact operational surfaces in warm stone and charcoal, with an ember-orange accent that signals action and live work. Its signature is the ember edge—a restrained orange rail, glow, or focus treatment that makes active work legible without turning the board into a decorative dashboard.

## 2. Color

Colors are implemented as HSL component custom properties in `web/src/index.css` and consumed through semantic Tailwind names. Alpha variants of these tokens are allowed; new raw colors are not.

### Palette

| Role           | Token                                         |                    Light |                      Dark | Usage                                                         |
| -------------- | --------------------------------------------- | -----------------------: | ------------------------: | ------------------------------------------------------------- |
| Canvas         | `background` / `--background`                 |              `60 9% 98%` |               `20 13% 5%` | Application and board canvas                                  |
| Text           | `foreground` / `--foreground`                 |             `24 10% 10%` |               `60 9% 98%` | Primary text and icons                                        |
| Card           | `card` / `--card`                             |              `0 0% 100%` |              `24 10% 10%` | Cards and main content                                        |
| Card text      | `card-foreground` / `--card-foreground`       |             `24 10% 15%` |               `60 9% 98%` | Text on cards                                                 |
| Muted surface  | `muted` / `--muted`                           |             `30 12% 95%` |               `20 9% 15%` | Subdued controls and metadata                                 |
| Muted text     | `muted-foreground` / `--muted-foreground`     |              `25 6% 40%` |               `24 5% 45%` | Secondary labels and disabled text                            |
| Border         | `border` / `--border`                         |             `30 10% 88%` |               `18 6% 21%` | Inputs and strong separators                                  |
| Subtle border  | `border-subtle` / `--border-subtle`           |             `30 10% 92%` |               `10 7% 15%` | Cards and low-contrast panels                                 |
| Input          | `input` / `--input`                           |             `30 10% 88%` |               `18 6% 21%` | Input outlines                                                |
| Focus          | `ring` / `--ring`                             |             `25 95% 53%` |              `25 95% 53%` | Keyboard focus indication                                     |
| Primary ember  | `primary` / `--primary`                       |             `21 90% 40%` |              `25 95% 53%` | Primary actions and active work; calibrated for WCAG contrast |
| Primary text   | `primary-foreground` / `--primary-foreground` |              `0 0% 100%` |              `24 10% 10%` | Text on primary controls                                      |
| Secondary      | `secondary` / `--secondary`                   |             `30 12% 95%` |               `20 9% 15%` | Secondary controls                                            |
| Accent         | `accent` / `--accent`                         |             `30 12% 93%` |               `20 9% 15%` | Hover and selected surfaces                                   |
| Destructive    | `destructive` / `--destructive`               |              `0 84% 60%` |               `0 72% 51%` | Errors and destructive actions                                |
| Success        | `success` / `--success`                       |            `160 84% 39%` |             `160 84% 39%` | Completed and healthy states                                  |
| Warning        | `warning` / `--warning`                       |             `38 92% 50%` |              `38 92% 50%` | Caution and recoverable conflict                              |
| Popover        | `popover` / `--popover`                       |              `0 0% 100%` |              `24 10% 10%` | Menus, tooltips, dialogs                                      |
| Sidebar        | `sidebar` / `--sidebar`                       |              `0 0% 100%` |               `20 13% 4%` | Navigation shell                                              |
| Sidebar hover  | `sidebar-hover` / `--sidebar-hover`           |             `30 12% 95%` |               `20 9% 15%` | Navigation hover state                                        |
| Sidebar active | `sidebar-active` / `--sidebar-active`         |             `25 95% 53%` |              `25 95% 53%` | Active navigation marker                                      |
| Ember surface  | `ember-surface` / `--ember-surface`           | `rgba(234, 88, 12, .08)` | `rgba(249, 115, 22, .08)` | Quiet active backgrounds                                      |
| Ember border   | `ember-border` / `--ember-border`             | `rgba(234, 88, 12, .22)` | `rgba(249, 115, 22, .22)` | Active borders                                                |

### Rules

- Use semantic Tailwind tokens rather than literal colors in components.
- Ember is reserved for primary action, focus, active navigation, current work, and drag/drop intent.
- Destructive red communicates failure; warning communicates a stale-board conflict that can be reconciled.
- Light and dark modes must expose the same semantic hierarchy and interaction states.

## 3. Typography

### Font stacks

- Primary: `Inter, system-ui, -apple-system, sans-serif`.
- Mono: `JetBrains Mono, Fira Code, ui-monospace, monospace`.
- No serif family is used.

### Scale

| Token / utility | Size | Line height | Typical use                                          |
| --------------- | ---: | ----------: | ---------------------------------------------------- |
| `text-micro`    | 10px |         1.2 | Uppercase column labels, counters, compact metadata  |
| `text-xs`       | 12px |        1rem | Secondary labels and dense controls                  |
| `text-ui`       | 13px |        1.45 | Default board cards, menus, and operational controls |
| `text-sm`       | 14px |     1.25rem | Body copy and standard controls                      |
| `text-base`     | 16px |      1.5rem | Dialog titles and emphasized body copy               |
| `text-lg`       | 18px |     1.75rem | Section headings                                     |
| `text-page`     | 22px |         1.3 | Page title                                           |
| `text-2xl`      | 24px |        2rem | Large card/page headings where already established   |

Weights are regular 400, medium 500, semibold 600, and bold 700. Operational overlines use the mono family, semibold weight, uppercase text, and `0.8px` to `1.2px` tracking.

## 4. Spacing & Layout

### Base unit

All new spacing is based on 4px. Existing 2px and 6px compact gaps are accepted legacy half-step values and must not spread into new layout primitives.

| Token       | Value | Usage                                        |
| ----------- | ----: | -------------------------------------------- |
| `space-0.5` |   2px | Existing dense list separation only          |
| `space-1`   |   4px | Icon insets and tight groups                 |
| `space-1.5` |   6px | Existing card metadata gaps                  |
| `space-2`   |   8px | Compact control and card spacing             |
| `space-2.5` |  10px | Existing dense card padding                  |
| `space-3`   |  12px | Inputs and toolbar groups                    |
| `space-4`   |  16px | Mobile page/board padding and dialog spacing |
| `space-5`   |  20px | Desktop content padding                      |
| `space-6`   |  24px | Comfortable panel padding                    |
| `space-8`   |  32px | Major component separation                   |

### Shell and board geometry

- Viewport shell: `min-height: 100vh` fallback plus `100dvh`; the page itself never owns horizontal overflow.
- Shell modes: full 240px navigation at `>=1440px`; 56px rail from `1024px` through `1439px`; closed overlay drawer below `1024px`.
- A user-expanded rail may temporarily render the full navigation without rewriting the persisted wide-desktop preference.
- Board route main: `min-width: 0`, no scrolling, and no generic page padding. Other routes retain the existing 20px content padding and main scroll.
- Board page: toolbar is fixed above a single `min-height: 0` board viewport. The viewport owns both horizontal and vertical drag scrolling.
- Columns: `min-width: 220px` at 1280px, a comfortable tablet width that allows at least two columns at 768px, and `min-width: 280px` at 375px. Column/task-list children never establish another scroll container.
- Board padding/gaps use 16px at mobile/tablet and 20px at desktop, with 8px to 12px column gaps.
- Card width follows its column and must remain at least 280px on a 375px viewport after board padding.

## 5. Components

### Button and icon button

- **Structure:** semantic `button`, optional Phosphor icon, label or accessible name.
- **Variants:** primary, destructive, outline, secondary, ghost, link; text and icon sizes.
- **States:** default, hover, active press, visible focus ring, disabled opacity/cursor, pending/busy.
- **Accessibility:** icon-only controls require an accessible name; disabled state uses native `disabled` where possible.
- **Motion:** color/opacity/transform only, using the micro timing token.

### Form controls

- **Structure:** label plus input/select/textarea/checkbox/switch, supporting help and error text.
- **States:** default, hover where relevant, focus ring, disabled, invalid, loading where asynchronous.
- **Accessibility:** programmatic label, described errors, keyboard operation, and contrast in both themes.

### Card and panel

- **Structure:** semantic section/article with optional header, content, metadata, and actions.
- **Variants:** standard card, compact operational card, elevated popover/dialog.
- **States:** default, hover elevation, active/selected ember treatment, disabled/muted, loading skeleton, empty, error.
- **Depth:** subtle border plus tokenized shadow; do not add isolated shadow recipes.

### App shell navigation

- **Structure:** skip link, navigation, header, and main landmark.
- **Variants:** full sidebar, compact rail, overlay drawer.
- **States:** active item, hover, focus, drawer open/closed, persisted desktop collapse preference.
- **Accessibility:** drawer traps/contains focus through existing dialog/sheet behavior, closes on Escape and outside click, and returns focus to its menu trigger.
- **Motion:** drawer/rail transitions use standard timing, transform, and opacity; reduced motion removes non-essential movement.

### Board toolbar

- **Structure:** page identity, search/filter controls, create action, and a polite status/explanation region.
- **States:** default, filtering, ordering-disabled, committing, conflict, loading, and error.
- **Accessibility:** ordering eligibility is visible text and announced; non-ordering controls remain usable during a move.

### Board viewport and column

- **Structure:** one scroll-owning board viewport containing non-scrolling droppable columns.
- **States:** loading skeleton, empty board/column, error, valid drop target, active drop target, ordering-disabled, and committing.
- **Accessibility:** named regions/columns; board status is announced without stealing focus.
- **Responsive:** follows the column widths and padding in Section 4 without document overflow.

### Kanban task card

- **Structure:** draggable article, detail-navigation body, status/assignment metadata, dedicated drag handle, and overflow menu.
- **States:** default, hover, active press, keyboard focus within, dragging, committing/busy, disabled ordering, blocked/error, terminal-muted.
- **Accessibility:** the card body and drag handle are separate targets; the handle is a visible button-like control with an accessible name and at least a 32px target. Keyboard drag uses the DnD library controls.
- **Motion:** hover/drag uses tokenized shadow plus transform/opacity; active in-progress ember motion respects reduced motion.

### Drag handle

- **Structure:** Phosphor grip icon in a dedicated 32px control; only this control receives `dragHandleProps`.
- **States:** subtle default, visible hover, active press, high-contrast focus ring, disabled, and committing/busy.
- **Accessibility:** accessible name includes the task title; native disabled semantics when DnD permits, plus `aria-disabled`/`aria-busy` when committing.

### Conflict notice

- **Structure:** warning-toned status banner with message and optional refresh/retry-safe action.
- **States:** hidden, reconciling, resolved, and persistent error.
- **Copy:** stale moves say “Board changed while you were dragging; refreshed to the latest version.”
- **Accessibility:** `role="status"` for reconciliation and `role="alert"` only when user action is required.

### Agent scope navigation

- **Structure:** scope is represented only in the application shell. `Main Chat` appears immediately after the Project switcher and before the `Project` section label. The selected Project contributes one `Agent Workspace` entry within that section. `Agent Settings` appears once in `Workspace`; chat pages never render a second global/Project roster.
- **Order:** Project switcher; Main Chat; Project label and Overview, Board, Tasks, Agent Workspace, Project Settings; Workspace label and Agent Settings, Mission Control, Daemons, Operations, Forge Settings.
- **States:** active, ready, setup required, loading, unavailable, and empty Project selection. Setup status stays visible on the destination surface rather than adding a duplicate navigation model.
- **Accessibility:** entries are semantic links with `aria-current="page"`, visible focus rings, and names that include scope where needed. The compact drawer preserves the same order, closes on activation, and restores focus to its trigger.
- **Responsive:** the full sidebar, compact rail, and overlay drawer expose the same hierarchy. Navigation never creates document-wide horizontal overflow.

### Agent chat timeline and composer

- **Structure:** server-authoritative Agent Chat messages, explicit handoffs, and one composer. Do not derive handoffs or target navigation from message text, task IDs, or retired Room/Conversation aliases.
- **Turn states:** finite `sending`, `queued`, `leased`, `running`, `retry_wait`, `succeeded`, `failed`, and `cancelled` states are visible in the timeline and announced with `role="status"` or `role="alert"` as appropriate.
- **Composer behavior:** Enter submits a non-composing message; Shift+Enter inserts a newline; IME composition is never interrupted. The send control is disabled while the current turn is live or the binding is not ready, with truthful copy explaining why.
- **Handoffs:** a handoff's Continue action opens the target Project Agent Chat and never redirects to a board/task view. Context provenance remains inspectable from the explicit manifest identifier.
- **States:** loading, recoverable error with retry, empty timeline, setup required, pending turn, and settled timeline all have explicit copy and keyboard-visible actions.
- **Structured tool outcomes:** model-facing native/MCP command failures remain in-band and are represented by the stable outcome code/status plus typed approval, setup, current-version, digest, or retry data; do not turn them into free-form provider/database prose. If a user-facing surface exposes the result, show the bounded `safe_message`, explicit code/status, and a permitted next action. `replayed` is a boolean provenance marker and never a replacement status.
- **Outcome privacy:** show `correlation_id` only as bounded inspectable diagnostic metadata; never render protected causes, raw persistence errors, payload bodies, or unredacted outcome JSON. Current versions, revisions, and corrective arguments may be shown only after the server has authorized the caller for that exact scope/resource.
- **Refresh:** server events may accelerate updates, but mounted timelines poll messages, turns, handoffs, and chat status at a bounded interval so a completed response never depends on an unavailable event channel.

#### Main Chat topic boundary

- **Structure:** a compact control beside the Main Chat header showing the current topic's label and a `New topic` action. This is a durable, user-owned context epoch *inside* the one account Main Chat (design D21) -- it never renders as, links to, or implies a second chat, binding, or identity. Earlier topics are listed in a disclosure as inspectable/searchable history; opening one never injects it into the live turn's episodic context.
- **Starting a topic:** `New topic` opens a small dialog with an optional label field and explains, in plain language, that Forge keeps the same Main Chat, rotates what the Main Agent sees on the next message to the new topic plus canonical portfolio state and unresolved obligations, and adds a visible divider. On success the dialog closes and the divider appears in place in the timeline as a real, durable, immutable message rendered as a `TimelineDivider` separator rather than a chat bubble.
- **Denial:** starting a topic is refused while a Main turn is live or while a Product Genesis session/approval still needs an explicit finish-or-cancel decision (design D21). The dialog stays open, keeps the user's in-progress label, and shows the server's specific reason inline with `role="alert"`; it is never presented as a generic failure.
- **States:** apply the shared state contract in "Charter, Project Overview, and release primitives" below -- loading (topics not yet fetched, action disabled), disabled (with a truthful reason surfaced via `title` and passed down from the live-turn/Genesis-pending signal), stale (a background refetch badge beside the current label), conflict/denied (inline alert, dialog remains open), error (topic history unavailable, rest of the chat stays usable), and success (dialog closes, current label updates, divider visible).
- **Accessibility:** the current-topic disclosure uses the native `<details>`/`<summary>` pattern (keyboard-operable, screen-reader-exposed open/closed state by default); the dialog traps focus, restores focus to the triggering control on close, and closes on `Escape`; the divider uses `role="separator"` with an accessible label naming the new topic.
- **Responsive:** the control stays inline with the chat header at every width; at 375px the topic label truncates with a `title` tooltip and the dialog's actions stack full width.

### Project Agent Workspace

- **Structure:** `/projects/:projectId/chat` is titled `Agent Workspace` and identifies the active Project and bound Project Agent. At desktop it pairs the durable timeline with a bounded editing rail for Project summary/status, Decisions, artifacts, milestones, and Tasks. All mutations use typed Forge services and return a durable receipt.
- **Edit states:** every editor exposes idle, dirty, saving, saved, conflict, and failed outcomes without collapsing geometry. A conflict preserves the user's draft and shows the current server revision; it never silently overwrites or retries.
- **Authority:** the surface never exposes a repository path, worktree, shell, raw filesystem tool, or Workspace lease. Repository work is represented as a traceable Task for an authorized Task Worker or reviewer.
- **Responsive:** at 1280px, use conversation plus a bounded editing rail; at 768px and 375px, use a labeled `Conversation` / `Project` segmented view. Segment changes preserve drafts and focus context and warn before abandoning unsaved changes.
- **Accessibility:** the segment control follows the tab pattern, mutation results use polite status announcements, actionable failures use alerts, and saved receipts remain inspectable from the timeline or editing rail.

### Global chat launcher

- **Structure:** a bottom-right launcher opens the same account-owned Main Agent timeline as `/chat`; it never creates a second chat or local fork.
- **Accessibility:** the launcher has an accessible name, Escape closes the panel, focus moves into the panel on open, and focus returns to the launcher on close. The panel is responsive to viewport height and keeps the composer reachable above the safe area.

### Settings tab bar

- **Structure:** a `role="tablist"` row of `role="tab"` buttons controlling named tab panels; each tab is a text label plus a mono count badge.
- **States:** the active tab uses foreground text, an ember underline indicator (`bg-primary`), and an ember-tinted count badge (`ember-surface`/`ember-border`); inactive tabs are muted with hover to foreground; keyboard focus uses the `ring` token.
- **Motion:** color transitions only, micro timing.
- **Accessibility:** `aria-selected` and `aria-controls` wire tabs to panels; counts are part of the accessible label.
- **Responsive:** tabs wrap within their own row and never cause page-level overflow.

### Canonical Agent Settings and binding controls

- **Structure:** `/agents` is the only agent configuration destination, organized as three tabs over one settings model. The `Providers` tab is an inventory of configured provider entries (multiple entries per provider type are allowed) plus a `CLI runtimes` group for harnesses discovered on connected runtimes; the `Agents` tab is the searchable/filterable roster with connection health and profile activation; the `Bindings` tab holds the account's Main Agent binding, the optional Project Agent binding, and the read-only Main/Project chat-scope list. Project links may add a non-authoritative `project` query parameter that opens the Bindings tab, and a `tab` query parameter may deep-link any tab.
- **Provider entries:** each card shows provider type, editable display name, credential method, redacted account identity, endpoint, connection status, usage (which agents reference it and when it was last used), a live connection test, and rename/disconnect actions. Disconnecting warns with the dependent agent list first; dependents become visibly unhealthy and are never silently rebound. CLI runtime cards show authentication availability, host, version, usage, and a login-command hint instead of a credential form.
- **Provider setup wizard:** `Add provider` is a four-step wizard — choose a provider from the server catalog, choose an authentication method (guided login never replaces an available API-key path), connect (API-key form or the public OAuth operation view), then verify. The verify step confirms the stored entry, auto-runs the connection test, and offers agent creation as an explicit follow-up; completing a connection never creates an agent.
- **Connection test:** idle, testing, responding (with round-trip latency), and failed states are text plus structure in a `role="status"` region. The failure reason is server-redacted (HTTP class or transport kind only) and a retry stays keyboard-visible; secrets and provider response bodies never render.
- **Agent registration:** `New agent` is a three-step wizard — authentication source (a provider entry or CLI-managed runtime, with an inline add-provider path), runtime (`Direct · built-in runtime` or a harness, enabled/disabled by the server capability matrix with the server's reason shown), then configure (name, model, prompt). Completing a provider connection never creates an agent; the success state offers agent creation as an explicit follow-up.
- **Actions:** add/rename/disconnect a provider entry, create an agent, publish a replacement profile on another entry, activate a profile revision, select exactly one Main identity/profile through `/account/main-agent`, and select exactly one Project identity/profile through `/projects/{id}/project-agent`. Provider authorization, agent creation, and binding remain three separate operations.
- **Truthfulness:** controls show server state and expected version, preserve optimistic-concurrency errors, and display the server-enforced permission ceiling as read-only metadata. Forge-wide runtime defaults remain under `Forge Settings`. Role, primary/steward, participant, archive-membership, and arbitrary capability-grant controls are not part of this surface.
- **States:** loading inventory, empty/filter-empty, connected, authorization pending, refresh required, disconnected, unavailable, stale version, and recoverable failure each have explicit copy and a keyboard-visible next action.
- **Responsive:** filters wrap without truncating scope; inventory becomes one ordered column at compact widths; dialogs keep the primary action and operation status reachable above the safe area.

### Provider capability and connection controls

- **Provider card:** name, support level (`stable`, `experimental`, or `unavailable`), model-discovery support, configuration readiness, and only the credential methods declared by the server. Each method also declares which runtimes its entries can drive (`direct` or harness kinds) with per-combination support levels; the client renders — and disables with the server's reason — from that matrix and never infers compatibility from provider names. Guided login never visually replaces the API-key alternative when both are available.
- **Methods:** `Continue with ChatGPT` uses browser authorization with a device fallback and carries an experimental label; `Continue with xAI` uses a device flow and carries an experimental label; `Continue with Google` is available only for a registered Forge Gemini API OAuth client; OpenRouter and OpenAI-compatible providers use API-key forms.
- **Operation states:** starting, awaiting browser, awaiting device confirmation, polling, exchanging, verifying, publishing, succeeded, denied, expired, cancelled, and failed. Public views show only the opaque operation ID, public URL/code, expiry/polling guidance, and redacted error/recovery copy.
- **Credential states:** API key, renewable OAuth, refreshing, refresh failed, revoked, and disconnected are represented by text plus structure. Secret values, authorization codes, refresh tokens, device secrets, and PKCE material never render, enter URLs owned by Forge, or appear in debug details.
- **Accessibility:** provider method selection uses labeled controls; status changes are announced without stealing focus; the device code has an explicit copy action and readable fallback; cancellation remains available until terminal state; successful completion returns focus to the connected provider card.

### Mission Control and Agent detail

- **Hierarchy:** primary views lead with the singular Main binding, one Project binding per authorized Project, relevant Task Worker/reviewer activity, Attention, and outcomes. Connected profiles without a binding or active Task scope stay in secondary configuration inventory.
- **Coordination activity:** render one titled, durable activity section with two explicit groups: `Durable direct-command receipts` for commands committed through the shared command boundary, and `Pending and approved approval actions` for frozen approval-required records. The groups remain visually and semantically distinct even when one group is empty; never imply that an approval action has applied its domain effect until a committed command receipt exists.
- **Activity provenance:** each row exposes the operation, status, policy result, actor type/ID, scope type/ID, input digest, correlation ID, and occurred-at time in a wrapped metadata grid. Render IDs and digests as inspectable metadata with `break-all`/`title` support; never render raw input or outcome JSON. A recorded outcome may be acknowledged as metadata only (for example, `Outcome recorded`).
- **Activity states:** direct-command rows use the receipt label and committed/terminal status supplied by the server; approval rows use the approval-action label and preserve pending, approved, denied, or terminal status text. The section's empty state explains that no durable receipts or approval actions are currently projected. Status is text plus the existing `StateBadge` treatment, never color alone.
- **Responsive/accessibility:** the section is a named landmark with `h2`/`h3` group headings, a count, and `dl` metadata. At 1280px rows may use a two-column metadata grid; at 768px they collapse to one ordered column; at 375px all identifiers wrap inside the row and no page-level horizontal overflow is introduced. Long metadata remains keyboard/screen-reader inspectable without exposing protected payload bodies.
- **Scope isolation:** a Project Agent view requests only its Project's handoff metadata; the Main timeline may show explicit handoff receipts but never imports Project-private history or memory.
- **Recovery:** live chat turns expose a server-versioned “Cancel turn” action using an idempotency key; terminal turns expose only a bounded “Retry turn” action that re-admits the same request through normal server policy. Leased, queued, and retry-wait turns remain server-controlled and do not expose an unbounded client retry.
- **Containment:** long message, identifier, and error content wraps inside the timeline; the timeline owns horizontal clipping and never creates page-level overflow.

### Charter, Project Overview, and release primitives

These primitives extend the existing card, panel, button, form, status, and chat patterns. They introduce no new semantic color, typography, spacing, motion, or depth tokens: use the semantic tokens in Sections 2–4 and the existing depth levels in Section 7. Status is always conveyed by text and structure as well as color; Phosphor icons may reinforce it, but never replace the label.

#### Shared state contract

Every primitive in this section exposes the following states. State changes use the motion rules in Section 6 and preserve the component's geometry so surrounding content does not jump.

| State             | Surface and behavior                                                                                                                                                                                         |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Default           | `card`/`background` surface, `border-subtle`, normal text hierarchy, canonical current content.                                                                                                              |
| Hover             | Only actionable rows/cards change to `shadow-card-hover` and a subtle border/`accent` shift; non-actionable content does not pretend to be clickable.                                                        |
| Active            | Pressed/selected/current scope uses `ember-surface`, `ember-border`, and `shadow-ember`; the text label remains explicit.                                                                                    |
| Focus             | Keyboard focus uses the existing `ring` token and a visible outline/offset; focus never relies on hover or color alone.                                                                                      |
| Disabled          | Native `disabled` where possible, muted text/surface, `aria-disabled` when a non-form control must remain inspectable, and copy explaining why the action is unavailable.                                    |
| Loading           | A geometry-preserving skeleton and `aria-busy="true"`; retain the section heading and scope so loading is not mistaken for an empty or approved result.                                                      |
| Empty             | A short reason, the scope that is empty, and one safe next action in a `muted` surface; never render a blank panel.                                                                                          |
| Error             | Inline `destructive` status with a safe retry or route; use `role="alert"` only when the user must act and keep the last known content visibly marked as not current.                                        |
| Stale             | `warning` treatment with the source revision/event watermark and refresh/reconcile action; cached content must not carry a current, ready, or released claim.                                                |
| Conflict          | Show the conflicting record/revision or content/render digest, preserve both choices for inspection, and require refresh/explicit resolution; never silently merge, overwrite, or retry against newer truth. |
| Setup required    | Explain the missing setup and the exact route/action (for example `charter_setup_required`); keep unrelated existing Project Tasks, chat, evidence, and document maintenance usable.                         |
| Permission denied | Do not reveal protected bodies, identifiers, filenames, digests, counts, or media existence; show a bounded denial and the safe authorized route.                                                            |

#### Responsive composition contract

All primitives use `min-width: 0`, wrap long labels/identifiers, and keep horizontal clipping inside the owning region. At 1280px, use bounded side rails and side-by-side comparisons where noted; at 768px, collapse to one ordered column and wrap controls without hiding the next action; at 375px, stack actions/content, use full-width primary actions, and contain only explicitly bounded galleries or diff panes. The singular Main/Project Chat navigation and reachable composer remain intact at every width.

#### Charter knowledge label

- **Structure:** a compact inline label or ledger row with the epistemic category, short claim, source/revision reference, and optional impact. Categories are exactly `observed fact`, `user decision`, `research finding`, `assumption`, `hypothesis`, and `open decision`; do not collapse them into a generic “insight” tag.
- **Treatment:** use `text-micro` mono overlines for the category, `text-ui` for the claim, and `text-xs` mono for revision/source IDs. Labels use text plus a shape/icon cue; `user decision` and approved content may use the ember treatment, unresolved/open items may use `warning`, and observed/research content remains neutral unless a source is stale or conflicting.
- **Interaction:** hover/focus reveals the bounded source/provenance popover; active opens the ledger item or filters the diff/readiness view; disabled labels remain readable when the source is not actionable. Long claims and IDs wrap with `min-width: 0` and `overflow-wrap: anywhere`.
- **States:** apply the shared state contract. In `stale`, show the superseded revision and do not promote the claim; in `conflict`, show the competing sources and affected authority domain; in `setup-required`, show the adoption-Charter action; in `permission-denied`, show only the category-level denial.
- **Accessibility:** use a named list/region, expose the category in accessible text, and announce changes to readiness or approval without moving focus.

#### Charter revision diff

- **Structure:** revision header (Charter ID, revision number, base revision, author/provenance, content digest, rendered-view digest), material change summary, and a semantic diff grouped by Charter section. Additions and removals use text labels/icons and restrained `success`/`destructive` treatments; never rely on red/green alone. Provide unified view by default and a side-by-side comparison at wide desktop.
- **Controls:** revision selector, “inspect source”/provenance action, and a bounded `Approve exact Charter revision` action owned by the approval block below. Do not label an action “approve latest.”
- **States:** apply the shared state contract. `empty` means there is no prior revision and renders an “initial Charter” explanation; `stale` means the candidate is no longer current; `conflict` shows the submitted and server content/render digests side by side and disables approval until refreshed; `setup-required` explains that an existing Project needs explicit Charter adoption; `permission-denied` hides diff bodies and digests.
- **Responsive:** at 1280px, keep a two-pane diff with synchronized section headings; at 768px, use one pane with a base/candidate toggle and a persistent change summary; at 375px, use a unified inline diff, wrap IDs, and keep revision/action controls stacked without page overflow.

#### Charter readiness and exact approval

- **Structure:** a readiness summary beside the candidate diff: maturity/mode, required-section checklist, typed gaps, unresolved-item queue, material risks, exact content/render digests, selected Project Agent identity/profile/operating-skill revisions, and a single primary approval action. A ready state must still show assumptions, waivers, and unresolved items.
- **Readiness statuses:** `ready` uses an explicit “Ready for approval” label; `blocked`/`failed` list typed gaps; `stale` identifies the changed source; `conflict` identifies the expected/current version; none of these states becomes approval-ready through field count alone.
- **Exact approval rule:** the primary button reads `Approve exact Charter revision`; it is enabled only when the displayed revision, both digests, expected version, and selected responder revisions match the server candidate. Confirmation names the revision and digests, records the user's explicit event, and then offers the existing `Continue with Project Agent` navigation after the atomic handoff. Silence, continued chat, agent output, or Task progress never enables it.
- **States:** apply the shared state contract. `loading` preserves the checklist shape; `empty` explains which maturity-specific information is not yet known; `error` offers retry without changing the draft; `stale`/`conflict` disable approval and require a refreshed diff; `setup-required` routes to adoption rather than implying approval; `permission-denied` hides approval metadata and the action.
- **Accessibility:** checklist items expose pass/gap text, not color alone; approval has a descriptive label containing the revision, is keyboard reachable, and announces success/failure in a polite status region. A failed or stale approval never moves focus to an unrelated chat.

#### Execution baseline review

- **Structure:** the baseline "Review" dialog (opened from the Project Agent Chat approval card) renders named sections in this order: intended outcomes, plan items, milestones, milestones/acceptance checks, adaptive authority, risks, elevated actions, exclusions, and rollback/recovery. This replaces a raw preformatted JSON blob as the primary view (live-acceptance finding F19); raw JSON survives only inside a collapsed `Technical details` disclosure alongside the content digest. This is presentation only over the exact same approved `content` the frozen `rendered_view`/digests already cover -- it never recomputes, reorders, or overrides the frozen render or its digest.
- **Revision diff:** when a currently active revision exists beside the one under review, each section diffs by exact value (string/id lists) or by record id (acceptance-evidence entries). Additions and removals use a text label (`Added`/`Removed`, exposed to screen readers even though the leading `+`/`−` glyph is `aria-hidden`) plus a restrained `success`/`destructive` treatment, never color or strikethrough alone. A first-ever revision (no prior to compare) renders every entry as plain, undecorated content.
- **Adaptive authority:** `allowed_task_operations` renders as the closed, plain-language vocabulary (`split`, `sequence`, `replace` become full sentences) rather than the bare enum token; an empty grant explicitly states the plan is fixed rather than showing a blank section.
- **States:** apply the shared state contract from the section above. `empty` per-section states name what was not recorded (for example "No exclusions recorded") rather than omitting the section; the approve/activate controls beneath the dialog keep their own loading, disabled, stale, conflict, error, and success states unchanged.
- **Accessibility:** each section is a named `region`/`section` with an `h3` heading; the raw-JSON disclosure uses the native `<details>`/`<summary>` pattern; diff badges carry screen-reader-only "Added"/"Removed" text distinct from the `aria-hidden` glyph.
- **Responsive:** at 1280px the dialog uses its existing bounded width with a scrollable section list; at 768px and 375px sections stack full width, plan-item/milestone identifiers wrap, and the technical-details disclosure never forces page-level horizontal overflow.

#### Project Overview and current outcome

- **Structure:** one Overview landmark with a header (Project name, vision, current approved Charter revision, active milestone labels, explicit `primary_milestone_id`), one next user action, a current-outcome rail, authoritative Task/validation counts, Documents and Decision risks, bounded Evidence, immutable Release history, and links to the singular Project Agent Chat/global Main Chat. Keep live progress and released truth in separate titled surfaces.
- **Current outcome card:** show the milestone identity/outcome, included and excluded scope, lifecycle, blockers, check counts, evidence coverage, and concrete Task counts by workflow state. Ember marks current active work; success marks an actual passing/released result; warning marks stale/waived data. Never render an editable percentage or a released badge from terminal Task counts alone.
- **States:** apply the shared state contract to the Overview and current-outcome card. `empty` is a Project with no active milestone and names the next setup action; `setup-required` explains adoption while keeping Tasks/chat/evidence usable; `stale` shows the projection watermark and separates cached progress from release truth; `conflict` names the affected authority domain; `permission-denied` preserves navigation but withholds protected Project details.
- **Responsive:** at 1280px, use a main outcome/status column with a bounded right rail for Decisions/Evidence/Release history; at 768px, collapse to one ordered column with the next action and outcome first; at 375px, stack header actions, wrap labels/identifiers, keep the composer/deep links reachable, and contain any horizontal gallery inside its own region. The page itself never scrolls horizontally.

#### Project stage orientation

- **Structure:** a persistent, server-derived `Define → Plan → Build → Release` strip directly under the Overview's projection banner (design D17/8.5.1). It is navigation and explanation only -- never a second lifecycle, workflow, or client truth store: every stage's status and detail text is derived, with no independent judgment, from already-canonical fields already served on `ProjectOverview` (`charter_state`, the execution gate, Task/check counts, and Releases). It does not poll or fetch on its own.
- **Stage status:** each of the four stops shows `complete` (check glyph plus text), `active` (in progress, numbered), `blocked` (warning glyph, ember/destructive treatment), or `pending` (muted, numbered) -- text plus icon, never color alone, with the status also exposed as screen-reader-only text beside the visible label.
- **Scoped reconciliation:** the one canonical `ExecutionBlockerProjection` (design D17) already served on execution setup is reused verbatim -- its `stage` names exactly which stop is blocked (a `review`-stage blocker is shown scoped inside `Build`, since the visible strip has four stops, not five) and its `headline`/`safe_explanation`/next action render below the strip. No parallel stage vocabulary or blocker copy is invented here.
- **States:** apply the shared state contract. `loading`/`stale`/`error`/`permission-denied` are inherited from the Overview's own projection state -- the strip does not carry a separate one. An outstanding blocker is the only state that adds visible content (the reconciliation panel beneath the strip); no blocker means no panel.
- **Accessibility:** the strip is a `nav` landmark labeled "Project stage"; the blocker panel is a `role="status"` region with a keyboard-reachable "Review and resolve" link to the affected stage's recovery surface.
- **Responsive:** stops wrap onto their own row at 375px and drop the connecting rule between them (`motion-reduce:` safe, no animation plays); at 768px and 1280px stops stay in one row with a thin connecting rule between them.

#### Project execution readiness state rail

- **Structure:** Project Overview and Project Agent Chat share one compact, titled
  readiness rail with three independent rows: `Coordination`, `Execution setup`,
  and `Execution gate`. Each row renders the server-supplied state, bounded
  detail, and exactly one concrete next action. A ready coordination row never
  upgrades an unconfigured execution row, and an active baseline never upgrades
  a missing repository or role.
- **State copy:** `setup_required` names the missing role or repository action;
  `provisioning` names the checkpoint currently running and says the Project is
  committed but not executable; `failed` names bounded retry/configuration
  guidance; `retrying` preserves the operation identity and attempt status;
  `ready` says execution setup is ready without implying an active baseline;
  `pre_baseline_read_only`, `baseline_approval_required`, `active`, and
  `reconciliation_required` remain distinct execution-gate labels. Unavailable
  or stale projections retain the last known content as non-current and expose a
  safe retry action.
- **Allowed next actions:** select/create Worker, select independent reviewer,
  attach/retry repository, retry provisioning, or the exact authorized baseline
  action supplied by the server. The rail never invents a fallback action,
  grants a coordinator a Worker lease, or presents the Project as operational
  while setup/baseline gates are closed.
- **Accessibility:** use one named `section`/landmark per surface with a
  heading, status text, and a single keyboard-reachable action per state. State
  changes use `role="status"`; actionable failures use `role="alert"`. Status
  meaning is written text plus icon/structure, never color alone. Disabled
  actions use native `disabled` and explain the missing authorization or
  prerequisite. Keep Chat, Documents, Decisions, evidence capture, and safe
  planning usable while implementation dispatch is denied.
- **Responsive:** at 1280px the rail sits beside current outcome or the durable
  timeline; at 768px it becomes the first ordered full-width panel; at 375px
  rows stack, IDs wrap, and the one next action becomes full width. The rail
  owns no page-level horizontal overflow and respects reduced motion.
- **Plain-language compact labels:** the Project Agent side rail translates the
  three authority dimensions into `Project Agent`, `Build setup`, and
  `Permission to build`. When the baseline is proposed, the primary action is
  `Approve plan & start work`; it anchors directly to the exact approval card
  instead of linking generically back to the page. Copy must state that the
  Project already exists, that this is the one execution approval for every
  Task covered by the plan, and that covered Tasks do not require per-Task
  approval. Once all three dimensions are ready and permission is active, the
  rail ends with the status list; it must not invent a redundant `Next action`.

#### Task execution approval notice

- **Structure:** a compact ember-edged status card near the Task title, shown
  only for a non-terminal implementation Task whose Project execution setup or
  execution gate is not ready. It names the Task outcome (`Waiting to start`),
  distinguishes Project creation from permission to change the repository, and
  exposes exactly one route to the relevant Project Agent status or exact
  baseline approval card.
- **Actions:** `Approve plan & start work` when an execution baseline is
  proposed, `Open implementation planning` before a proposal exists, and
  `Finish build setup` when Worker/reviewer/repository setup is incomplete.
  The control is a semantic link with a visible focus ring; it never attempts
  to approve on the user's behalf and never presents a workflow-state gate as
  the execution-baseline approval.
- **States:** loading preserves a single-line status shape; unavailable data is
  omitted in favor of the Task's existing server-authored error surface; an
  active execution gate removes the notice. Planning/discovery and terminal
  Tasks do not render it. Status is text plus icon/structure, never color alone.
- **Responsive/accessibility:** the message and action sit side by side when
  space permits and stack at compact widths with a full-width action. The
  heading is named, copy wraps within the Task content column, and the status
  uses `role="status"` without moving focus.

#### Structured operational state feedback

All server-authored operational states use one bounded status surface: a plain-language
heading, the stable state/code, the safe server message, the smallest authorized metadata
needed to recover, and exactly one clear next action. Never replace a structured state with
provider/database prose, raw outcome JSON, or a client-invented success. Status is conveyed
with text and structure as well as the semantic token/icon treatment, and the action remains
keyboard reachable at every width.

- **Conflict:** use the warning treatment and name the affected authority domain. Show
  expected/current version or revision and digest only when the server authorizes those
  values; preserve the user's draft and disable the stale mutation until the current
  projection is loaded. The one next action is `Refresh`/`Review current revision`, never an
  automatic retry against newer truth. Reconciliation is `role="status"`; a conflict that
  blocks an action is `role="alert"` with the refresh action in the same region.
- **Reconciliation decision:** lead with the concrete change and its consequence, never the
  internal labels “canonical conflict” or “reconciliation review.” Keep record IDs, revisions,
  digests, field paths, and principal metadata inside a native `Technical details` disclosure.
  A server-recommended historical plan repair uses `Accept update & resume work` and `Reject for
  now`; accepting is one atomic user gesture that approves and applies the exact correction, while
  rejecting truthfully leaves work paused. Generic choices use plain verbs such as `Keep the
  current work`, `Cancel the affected work`, and `Discard the conflicting change`; Forge supplies
  the audit reason, so the user is never blocked by an unexplained required textarea or replacement
  identifier field.
- **Approval-required:** use an ember-edged approval card that says what exact operation is
  waiting, names its authorization target/revision/digest, and separates approval from the
  eventual domain effect. The primary control names the exact candidate (for example,
  `Approve exact Charter revision`); it never says “approve latest” and never implies that an
  approval record alone committed the effect. Keep the card pinned near the workflow that
  produced it, preserve it while approval is pending, and announce success/failure politely.
- **Setup-required:** state the blocked scope (`Project Agent`, Worker, reviewer, repository,
  or baseline), explain what remains usable, and provide the server-supplied configuration
  action. Do not disable safe planning, Documents, Decisions, or evidence capture just because
  implementation dispatch is closed. Use `role="status"` for a verified requirement and one
  full-width primary action on compact screens.
- **Provisioning / retrying / failed:** show the committed-but-not-executable distinction,
  current checkpoint, bounded operation ID, attempt/max-attempt count, operation status, and
  next retry time when supplied. `retrying` keeps the operation visible and busy until the
  server confirms `ready`; `failed` keeps the bounded error and retry/configuration action
  visible without claiming success. A retry action is disabled while its request is pending and
  is never duplicated by an optimistic ready badge. Use `role="status"` for provisioning and
  retrying; use `role="alert"` for failed states.
- **Deferred wake:** render `Wake deferred` as durable work that will be reconsidered, not as
  delivered, suppressed, or succeeded. Show the bounded reason, attempt/max-attempt and next
  retry time when authorized, preserve the source/correlation reference, and expose one safe
  action (`Refresh configuration` or the exact setup action supplied by the server). Never
  advance a user-facing progress indicator or imply that an Agent turn ran when the wake was
  only deferred. This state is polite status until user configuration is required, then an
  actionable alert.
- **Semantic progress warning:** label the condition `Waiting for semantic progress`, separate
  it from owner health, and keep a healthy/quiet provider or tool call from being presented as
  failed. Show the last semantic-progress time when available and offer one `Inspect run`
  action. The warning is amber and announced as status; it becomes an actionable alert only
  when the server supplies a terminal interruption or recovery requirement.
- **Owner lease expired / deadline interruption:** distinguish owner loss from a hard deadline.
  `Owner lease expired` means the execution owner stopped renewing and must not be presented as
  semantic failure; `Hard deadline reached` means the immutable deadline ended the run and a
  heartbeat could not extend it. For terminal rows show the bounded interruption reason, kind,
  and time, retain the committed terminal result, and offer one authorized `Retry run` or
  `Continue session` action. Late results never overwrite the visible terminal state.
- **Retry states:** render `Retrying`, `Retry wait`, and `Retry exhausted` with attempt budget,
  next-at time, and the server's bounded recovery action. Keep the composer/action row disabled
  while a turn is live, retain the original request when a retry is offered, and avoid an
  unbounded client retry loop. A terminal retry failure uses an alert; a scheduled retry uses
  polite status and does not steal focus.

At 1280px, these states sit beside the current outcome/timeline with metadata in a bounded
two-column grid and the next action aligned to the state header. At 768px, they become the
first ordered full-width panel, with metadata in one column and the action still visible before
secondary details. At 375px, heading, state, metadata, and action stack; IDs/digests wrap with
`overflow-wrap:anywhere`, the primary action fills the available width, and no page-level
horizontal scrolling is introduced. Preserve the same order and labels at all breakpoints,
keep focus in the state surface after a mutation, and respect `prefers-reduced-motion` by
retaining text/state meaning without relying on spinners or pulse animation.

#### Project Document freshness and Decision/Risk panel

- **Document row/card:** show typed kind, title, current draft and approved revision pointers, approval policy, change summary, author/provenance, digest, and a freshness marker. Actions are inspect, compare, and (when authorized) propose/approve; an exported repository copy is visibly derived and never presented as canonical.
- **Decision/Risk row:** show the question/context, selected outcome or unresolved choice, alternatives, rationale, principal/decision class, affected Document/Task/Milestone IDs, risk/impact, and `active`/`superseded`/`invalidated` effective status. Proposal/editor records are visibly separate from effective DecisionRecords. Use `open decision`, `assumption`, and `hypothesis` labels rather than presenting them as approvals.
- **States:** apply the shared state contract. `empty` explains that optional Documents are intentionally absent for compact Projects and that no risk is currently recorded; `loading` keeps revision/status columns visible; `error` has a safe retry; `stale` marks a superseded pointer or stale memory reference; `conflict` shows the base/current revision and blocks overwrite; `setup-required` permits document maintenance but routes Charter adoption; `permission-denied` hides document bodies and decision details while retaining a safe count only when policy permits.
- **Responsive/accessibility:** at 1280px, keep Documents and Decisions as separate bounded panels in the Overview rail; at 768px and 375px, render them as full-width stacked sections. Each row is a named button/link only when actionable, wraps digests/IDs, and exposes status, principal, and freshness in accessible text.

#### Milestone outcome, acceptance checks, and readiness

- **Milestone outcome card:** show canonical sequence (`M001`), optional human label, lifecycle (`planned`, `active`, `ready_for_release`, `released`, `cancelled`), explicit primary/active marker, outcome, included/excluded scope, linked artifacts/Tasks, dependencies, risks, and evidence expectation. Multiple active milestones remain visible; `primary_milestone_id` changes emphasis only.
- **Acceptance check row/matrix:** each check has stable ID, description, required/optional marker, source kind, expected result, exact source revision, and current result/provenance. Render `passed`, `failed`, `missing`, `stale`, `blocked`, and `waived` as distinct text states; waived checks retain the authorized user, reason, and time and never look like ordinary passes.
- A current `manual` check with no result exposes an explicit user-only **Record attestation** action. Its confirmation dialog repeats the exact immutable check text and definition revision, requires a deliberate Pass or Fail choice plus a short observation, and never defaults to Pass.
- Non-manual checks never expose the attestation action. Their pending state names the authoritative producer (Task validation, document approval, evidence, policy waiver, or Git reference), so the user is not routed to an action they cannot perform.
- A manual attestation satisfies only the check result. Required evidence remains a separate visible contract and readiness blocker; the UI never presents attestation prose as evidence or a ready candidate as an existing release.
- **Readiness panel:** show the immutable `ReadinessSnapshot` ID/digest, candidate milestone/definition revision, event watermark, ordered inputs, check results, evidence attachment IDs/digests/availability, blockers, known issues, and the next safe action. `ready_for_release` is a release candidate, not a release; only an authorized user may see an enabled exact release action.
- **States:** apply the shared state contract. `empty` explains missing checks/evidence; `loading` preserves check-row geometry; `error` offers retry; `stale` returns an unreleased milestone to `active` with typed reasons; `conflict` shows expected/current milestone or readiness versions; `setup-required` blocks release only when the Project lacks an approved Charter; `permission-denied` hides check inputs and release metadata. A `released` milestone stays released while correction readiness is evaluated, and `cancelled` is terminal/muted.
- **Responsive/accessibility:** at 1280px, show the outcome card beside the check/readiness panel; at 768px, stack outcome, checks, then readiness; at 375px, use one-column cards with wrapped IDs and a full-width next action. Checks are a named list/table with status text, not color-only badges; readiness updates use `role="status"` and release errors use `role="alert"` only when action is required.

#### Bounded Evidence gallery and media tile

- **Structure:** a named, Project-authorized gallery of bounded image/video tiles. Each tile includes an image thumbnail or video poster, fixed aspect ratio, caption, evidence kind, source Task/run/validation when present, supported acceptance-check IDs, uploader/time, checksum/freshness, availability, and explicit open/download/play controls. Use `object-fit: cover` for previews; video never autoplays and always exposes poster and duration.
- **Availability:** `available` can render and count; `quarantined` shows pending review and cannot render/count as proof; `redacted` explains that only a policy-permitted derivative/metadata is available; `purged` shows an immutable tombstone/digest/audit note and `evidence_unavailable` rather than a broken image. The gallery exposes no inline purge control; an authorized Project owner/admin disposition uses the explicit media route and the resulting status is rendered as unavailable evidence. A missing caption or check linkage can remain stored but does not satisfy the acceptance check.
- **Interaction:** hover reveals only the bounded action overlay; active opens the authorized detail view; focus makes every open/download/play action visible with the existing ring. Captions, source, and check linkage remain visible outside the overlay for keyboard and screen-reader users.
- **States:** apply the shared state contract. `empty` names whether no evidence is attached or required; `loading` uses aspect-ratio-preserving tiles; `error` offers retry per tile/gallery; `stale` marks outdated evidence and its source revision; `conflict` shows attachment/version mismatch without replacing the asset; `setup-required` explains Charter adoption while leaving evidence capture available; `permission-denied` reveals no filename, URL, checksum, or existence.
- **Responsive:** at 1280px, use a bounded grid in the right rail or outcome section; at 768px, use two columns under the outcome; at 375px, use a contained horizontal gallery with snap/keyboard movement and an overflow affordance inside the gallery only. Preview bounds, captions, and long media titles never create page-level overflow.

#### Immutable Release snapshot and history

- **Snapshot card/history item:** show immutable release revision (`M001-r1`), milestone definition/digest, display label, released-at/principal, summary/changelog, known issues, exact Charter/Document revisions, included Decisions/Tasks/validation outcomes, bounded git/repository references, evidence asset/attachment IDs/checksums/availability, waivers, and whole-snapshot digest. Use a chronological list with an inspect action; no edit/delete affordance is rendered.
- **Live separation:** place current Project outcome and release history in separately titled surfaces. A later Task, Document, branch, caption, or milestone change updates live surfaces only; it never rewrites an earlier snapshot. Corrections append `M001-r2` and retain `M001-r1` byte-for-byte. When the server supplies an audited purge tombstone, released evidence remains visible as unavailable evidence, not silently removed.
- **Release action:** the action names the exact readiness candidate/digest and is enabled only for an authorized user after the release transaction's source re-check. Project Agent proposals and a `ready_for_release` badge never release automatically.
- **States:** apply the shared state contract. `empty` explains that no release exists yet and points to readiness; `loading` preserves the history timeline; `error` offers retry; `stale` marks live data as newer than the selected snapshot without changing snapshot text; `conflict` shows the candidate/source digest mismatch and blocks release; `setup-required` explains that Charter adoption is required before release; `permission-denied` hides snapshot bodies/digests and any protected evidence metadata while preserving only the safe route.
- **Responsive/accessibility:** at 1280px, keep current outcome and history side by side; at 768px, stack them with current state first; at 375px, stack metadata, summary, known issues, and inspect action, wrap all IDs/digests, and keep the history list contained. Each snapshot has a named heading, release number, actor/time, and immutable warning in accessible text.

## 6. Motion & Interaction

| Token     |  Duration | Easing                          | Usage                                        |
| --------- | --------: | ------------------------------- | -------------------------------------------- |
| Micro     | 100–150ms | `ease-out`                      | Hover, active press, focus visibility, menus |
| Standard  |     200ms | `ease-in-out`                   | Sidebar/rail and drawer state changes        |
| Emphasis  | 400–600ms | `cubic-bezier(0.16, 1, 0.3, 1)` | Reserved for meaningful page-level emphasis  |
| Live work |    2200ms | `ease-in-out`                   | Existing in-progress ember pulse             |

- Animate transform, opacity, filter, color, border color, and tokenized shadow only; never animate layout dimensions or positions.
- Every interactive element has hover, active, and focus-visible treatment.
- Drag start freezes an ID-keyed board snapshot. Updates queue until commit/reconciliation, and a second gesture cannot start while a move is committing.
- Conflicts never auto-retry against newer versions; current server truth replaces the frozen snapshot and the result is announced.
- Respect `prefers-reduced-motion`; all core state meaning remains visible without animation.

## 7. Depth & Surface

Forge uses a **mixed border-and-soft-shadow** strategy. Warm tonal shifts define the shell and column hierarchy; subtle borders define card edges; soft shadows communicate card lift and floating surfaces.

| Level        | Token                                | Usage                                |
| ------------ | ------------------------------------ | ------------------------------------ |
| Hairline     | `border-subtle`                      | Columns and quiet cards              |
| Default      | `border`                             | Inputs and emphasized separators     |
| Rest         | `shadow-xs`, `shadow-soft`           | Controls and cards at rest           |
| Hover        | `shadow-card-hover`                  | Interactive card lift                |
| Floating     | `shadow-float`                       | Menus, dialogs, overlay navigation   |
| Active ember | `shadow-ember`, ember surface/border | Current work, focus, and drag intent |

New surfaces must reuse these levels. The current code contains a few legacy literal status colors, arbitrary compact measurements, and generic `shadow-sm`/`shadow-lg` utilities; they are accepted debt outside this change and should be consolidated only in separately approved work.
