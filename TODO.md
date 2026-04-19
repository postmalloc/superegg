# Superegg TODO

## Product Constraints

- Single-user app.
- Local-first on macOS/Linux for v1.
- Deployable later without major architectural changes.
- Minimal stack: Go, SQLite, server-rendered HTML templates, minimal vanilla JS.
- Config-driven setup via file plus env vars.
- Authentication is a single admin password with session cookies.
- OpenRouter is used for summarization.
- Feed ordering is chronological only in v1.
- Read state is manual only: `unread`, `read`, `saved`.
- Sources are file-configured only in v1.
- JS-rendered sites are out of scope for v1.

## UX Goals

- Dense, clean, text-first interface.
- Main page is summary-first and masonry-style with variable card heights.
- Card height must still be capped by truncation so cards do not sprawl.
- Homepage does not inline full article bodies.
- Detail page shows cleaned extracted content by default.
- Detail page can reveal raw extracted text.
- Original article URL should always be available.
- Failed extraction items remain visible as `partial`.

## Initial Sources

- Hacker News
- Lobsters
- Simon Willison's blog
- The Verge
- Ars Technica

## Summary Format

- 1 sentence abstract
- 3 bullet points
- topic tags

## Core Data Model

- `sources`
- `discovery_runs`
- `discovered_items`
- `articles`
- `threads`
- `stories`
- `story_sources`
- `summaries`
- `story_state`
- `jobs`

## Domain Rules

- Separate `story`, `article`, and `discussion thread`.
- For Hacker News and Lobsters:
- External linked article is primary when present.
- Discussion metadata is attached separately.
- Discussion-only posts remain valid story records.
- Deduplicate by canonical article URL.
- Preserve every source/thread that referenced the article.
- Store both raw extracted text and cleaned reader text.
- Store summaries as versioned artifacts keyed by:
- `content_hash`
- `model_id`
- `prompt_version`

## Pipeline

### Discover

- Poll RSS feeds and configured list/homepage/direct URLs.
- Normalize incoming feed/list items.
- Record source fetch runs and failures.
- Create or update discovered item records.

### Extract

- Fetch article HTML server-side.
- Resolve redirects.
- Determine canonical URL.
- Extract title, author, publish date, excerpt, raw text, cleaned text.
- Mark extraction status clearly.
- Keep partial records visible when extraction fails.

### Summarize

- Send cleaned article text to OpenRouter.
- Use configurable model ID from config.
- Use API key from env var.
- Generate fixed summary shape.
- Cache by content hash plus model and prompt version.
- Re-summarize selectively when inputs change.

## Runtime Components

- `cmd/app`
- `internal/config`
- `internal/auth`
- `internal/store`
- `internal/discovery`
- `internal/extract`
- `internal/summarize`
- `internal/pipeline`
- `internal/scheduler`
- `internal/web`

## Config Requirements

- App listen address
- SQLite DB path
- Session secret
- Admin password hash
- OpenRouter model ID
- Source definitions
- Per-source refresh settings
- Optional default polling settings

## Environment Variables

- `OPENROUTER_API_KEY`

## UI Requirements

- Server-rendered HTML templates.
- Minimal JS only for:
- masonry layout support
- filters
- manual mark-read/save actions
- refresh triggers
- progressive load-more / infinite scroll behavior
- Main feed card should show:
- title
- source badges
- time
- abstract
- 3 bullets
- topic tags
- thread metadata when present
- partial/error badge when needed
- read/save controls
- Detail page should show:
- cleaned content
- raw content toggle
- original URL
- source/thread links
- summary and metadata

## Refresh / Scheduling

- Manual refresh by default.
- Optional per-source auto-refresh intervals.
- Single-process internal scheduler for v1.
- Persist job state in SQLite.
- Prevent overlapping work with leases/locks.
- Retry failed stages independently.
- Back off noisy failing sources.

## Auth

- Login form with password only.
- Signed session cookie.
- No user table.

## Build Order

### Phase 1: Skeleton

- Initialize Go module.
- Add basic app entrypoint.
- Add config loading and validation.
- Add SQLite connection and migrations framework.
- Add HTML template layout and static asset serving.
- Add login/logout flow and session middleware.

### Phase 2: Storage

- Create schema for core tables.
- Implement store layer for:
- sources
- jobs
- stories
- articles
- threads
- summaries
- state
- Add indexes for URL dedupe, time sorting, and job lookup.

### Phase 3: Discovery

- Implement RSS ingestion.
- Implement homepage/list URL ingestion abstraction.
- Implement direct article URL ingestion.
- Add initial source configs for the five starting sources.
- Record discovery runs and errors.

### Phase 4: Extraction

- Implement article fetcher with redirect handling.
- Implement canonical URL normalization.
- Implement HTML content extraction.
- Store cleaned and raw text.
- Mark partial items when extraction fails.

### Phase 5: Story Unification

- Link external articles and discussion threads into stories.
- Merge duplicates by canonical URL.
- Preserve multiple source/thread references on each story.

### Phase 6: Summarization

- Implement OpenRouter client.
- Implement prompt versioning.
- Implement summary caching.
- Generate abstract, bullets, and tags.
- Store summarize failures and retryability state.

### Phase 7: Feed UI

- Build chronological feed page.
- Build masonry card layout with bounded card height.
- Add badges, metadata, and truncation rules.
- Add load-more / infinite-scroll behavior over backend pagination.

### Phase 8: Detail UI

- Build story detail page.
- Show cleaned article by default.
- Add raw text toggle.
- Add original URL and linked discussion references.

### Phase 9: User State

- Implement manual mark-read.
- Implement save/unsave.
- Add unread/read/saved filters.

### Phase 10: Refresh / Operations

- Add manual refresh endpoints/actions.
- Add per-source polling scheduler.
- Add locking and retry logic.
- Surface last refresh and last error in UI.

### Phase 11: Diagnostics

- Add minimal admin/debug pages for:
- source health
- failed jobs
- partial extractions
- summary failures

## Non-Goals For V1

- Multi-user accounts
- OAuth
- AI-based feed ranking
- JS-rendered scraping
- In-app source CRUD
- Headless browser support
- Complex search
- Fancy frontend framework
- Decorative animations

## Immediate Next Tasks

- Create Go project skeleton.
- Decide config file format (`toml` vs `yaml`).
- Define initial SQLite schema.
- Implement login and session handling first so the app is protected from the start.
- Implement RSS ingestion for the initial five sources.
