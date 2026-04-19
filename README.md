# Superegg

Superegg is a local-first, single-user news reader built as one Go binary with SQLite, server-rendered HTML, and a persisted `discover -> extract -> summarize` pipeline.

## What Works

- Config-file starter sources plus in-app RSS additions from settings.
- Password-based login with signed session cookies.
- RSS, direct-article, and basic list-page discovery.
- Article extraction with canonical URL normalization and fallback partial items.
- Separate `story`, `article`, and `thread` records.
- Deduping by canonical article URL while preserving source/thread references.
- OpenRouter-based summaries cached by `content_hash + model_id + prompt_version`.
- Dense masonry feed, story detail page, manual read/save actions, and manual refresh.
- Optional per-source background refresh intervals.

## Requirements

- Go 1.21 or newer
- An OpenRouter API key if you want summaries

## Setup

1. Copy the example config:

```bash
cp config.example.yaml config.yaml
```

2. Update `config.yaml`:

- change `server.session_secret`
- set `openrouter.model_id` to the model you want
- optionally change `server.listen_addr`, `server.database_path`, and starter sources

3. If you do not want the example password, generate a new password hash:

```bash
go run ./cmd/superegg hash-password "your-password"
```

4. Put the generated hash into `server.admin_password_hash`.

5. Export your OpenRouter API key:

```bash
export OPENROUTER_API_KEY=...
```

You can also start from `.env.example` if you prefer keeping the key in an env file.

## Run

Start the app:

```bash
go run ./cmd/superegg -config ./config.yaml
```

Then open [http://127.0.0.1:8080](http://127.0.0.1:8080).

If you use the example password hash from `config.example.yaml`, the login password is `changeme`.

If you leave `OPENROUTER_API_KEY` or `openrouter.model_id` unset, the app still runs, but summaries stay in `pending_config` until you configure them and refresh.

## Development

Run the build:

```bash
GOTOOLCHAIN=local go build ./...
```

Run the focused app test already in the repo:

```bash
GOTOOLCHAIN=local go test ./internal/app -run TestAddRSSSourcePersistsUserSource -count=1
```

## Storage And Config

- Database writes go to `./superegg.db` by default.
- SQLite will also create `superegg.db-wal` and `superegg.db-shm` while the app is running.
- `config.yaml` is intended to be local and is not meant to be committed.
- The scheduler is in-process, so one app process should own a given SQLite database.
- JS-rendered sites are intentionally out of scope for this first version.
