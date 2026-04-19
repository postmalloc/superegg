package store

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"

	_ "github.com/mattn/go-sqlite3"

	"superegg/internal/config"
)

const (
	jobKindDiscover  = "discover"
	jobKindExtract   = "extract"
	jobKindSummarize = "summarize"
)

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve database path: %w", err)
	}
	dsn := fmt.Sprintf("file:%s?_busy_timeout=5000&_journal_mode=WAL&_foreign_keys=on", absPath)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	db.SetConnMaxLifetime(0)

	store := &Store{db: db}
	if err := store.migrate(context.Background()); err != nil {
		db.Close()
		return nil, err
	}
	return store, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) migrate(ctx context.Context) error {
	migrations := []string{
		`CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY
		)`,
		`CREATE TABLE IF NOT EXISTS sources (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			key TEXT NOT NULL UNIQUE,
			name TEXT NOT NULL,
			kind TEXT NOT NULL,
			url TEXT NOT NULL,
			enabled INTEGER NOT NULL DEFAULT 1,
			refresh_minutes INTEGER NOT NULL DEFAULT 0,
			discussion INTEGER NOT NULL DEFAULT 0,
			summarize INTEGER NOT NULL DEFAULT 1,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS discovery_runs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			source_id INTEGER NOT NULL,
			status TEXT NOT NULL,
			item_count INTEGER NOT NULL DEFAULT 0,
			error TEXT NOT NULL DEFAULT '',
			started_at TEXT NOT NULL,
			finished_at TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS discovered_items (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			source_id INTEGER NOT NULL,
			external_id TEXT NOT NULL,
			title TEXT NOT NULL,
			article_url TEXT NOT NULL DEFAULT '',
			thread_url TEXT NOT NULL DEFAULT '',
			excerpt TEXT NOT NULL DEFAULT '',
			published_at TEXT NOT NULL DEFAULT '',
			raw_payload TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL DEFAULT 'discovered',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			UNIQUE(source_id, external_id)
		)`,
		`CREATE TABLE IF NOT EXISTS threads (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			source_id INTEGER NOT NULL,
			external_id TEXT NOT NULL,
			title TEXT NOT NULL,
			thread_url TEXT NOT NULL DEFAULT '',
			submitter TEXT NOT NULL DEFAULT '',
			score INTEGER NOT NULL DEFAULT 0,
			comment_count INTEGER NOT NULL DEFAULT 0,
			published_at TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			UNIQUE(source_id, external_id),
			UNIQUE(thread_url)
		)`,
		`CREATE TABLE IF NOT EXISTS articles (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			source_url TEXT NOT NULL DEFAULT '',
			canonical_url TEXT NOT NULL UNIQUE,
			title TEXT NOT NULL,
			author TEXT NOT NULL DEFAULT '',
			published_at TEXT NOT NULL DEFAULT '',
			excerpt TEXT NOT NULL DEFAULT '',
			raw_text TEXT NOT NULL DEFAULT '',
			clean_text TEXT NOT NULL DEFAULT '',
			content_hash TEXT NOT NULL DEFAULT '',
			extraction_status TEXT NOT NULL DEFAULT 'pending',
			last_extracted_at TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS stories (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			article_id INTEGER NOT NULL DEFAULT 0,
			primary_title TEXT NOT NULL,
			primary_url TEXT NOT NULL DEFAULT '',
			published_at TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL DEFAULT 'pending',
			summary_status TEXT NOT NULL DEFAULT 'pending',
			is_partial INTEGER NOT NULL DEFAULT 0,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE UNIQUE INDEX IF NOT EXISTS idx_stories_article_id
			ON stories(article_id)
			WHERE article_id != 0`,
		`CREATE TABLE IF NOT EXISTS story_sources (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			story_id INTEGER NOT NULL,
			source_id INTEGER NOT NULL,
			discovered_item_id INTEGER NOT NULL DEFAULT 0,
			thread_id INTEGER NOT NULL DEFAULT 0,
			source_url TEXT NOT NULL DEFAULT '',
			fingerprint TEXT NOT NULL UNIQUE,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS summaries (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			article_id INTEGER NOT NULL,
			content_hash TEXT NOT NULL,
			model_id TEXT NOT NULL,
			prompt_version TEXT NOT NULL,
			abstract TEXT NOT NULL DEFAULT '',
			bullets_json TEXT NOT NULL DEFAULT '[]',
			tags_json TEXT NOT NULL DEFAULT '[]',
			status TEXT NOT NULL DEFAULT 'pending',
			error TEXT NOT NULL DEFAULT '',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL,
			UNIQUE(article_id, content_hash, model_id, prompt_version)
		)`,
		`CREATE TABLE IF NOT EXISTS story_state (
			story_id INTEGER PRIMARY KEY,
			is_read INTEGER NOT NULL DEFAULT 0,
			is_saved INTEGER NOT NULL DEFAULT 0,
			is_viewed INTEGER NOT NULL DEFAULT 0,
			is_hidden INTEGER NOT NULL DEFAULT 0,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS jobs (
			id INTEGER PRIMARY KEY AUTOINCREMENT,
			kind TEXT NOT NULL,
			status TEXT NOT NULL,
			source_id INTEGER NOT NULL DEFAULT 0,
			discovered_item_id INTEGER NOT NULL DEFAULT 0,
			article_id INTEGER NOT NULL DEFAULT 0,
			story_id INTEGER NOT NULL DEFAULT 0,
			run_after TEXT NOT NULL,
			attempts INTEGER NOT NULL DEFAULT 0,
			last_error TEXT NOT NULL DEFAULT '',
			lease_owner TEXT NOT NULL DEFAULT '',
			lease_until TEXT NOT NULL DEFAULT '',
			payload_json TEXT NOT NULL DEFAULT '{}',
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_pending_run_after ON jobs(status, run_after)`,
		`ALTER TABLE sources ADD COLUMN origin TEXT NOT NULL DEFAULT 'config'`,
		`ALTER TABLE story_state ADD COLUMN is_viewed INTEGER NOT NULL DEFAULT 0`,
		`ALTER TABLE story_state ADD COLUMN is_hidden INTEGER NOT NULL DEFAULT 0`,
		`CREATE TABLE IF NOT EXISTS app_settings (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
		`INSERT OR IGNORE INTO app_settings(key, value) VALUES ('feed_sort', 'fetched')`,
		`ALTER TABLE sources ADD COLUMN summarize INTEGER NOT NULL DEFAULT 1`,
	}

	if _, err := s.db.ExecContext(ctx, migrations[0]); err != nil {
		return fmt.Errorf("bootstrap schema_migrations: %w", err)
	}

	for i, migration := range migrations {
		version := i + 1
		var count int
		if err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM schema_migrations WHERE version = ?`, version).Scan(&count); err != nil {
			return fmt.Errorf("check migration %d: %w", version, err)
		}
		if count > 0 {
			continue
		}
		tx, err := s.db.BeginTx(ctx, nil)
		if err != nil {
			return fmt.Errorf("begin migration %d: %w", version, err)
		}
		shouldRun := true
		skip, err := s.shouldSkipMigration(ctx, tx, migration)
		if err != nil {
			tx.Rollback()
			return fmt.Errorf("inspect migration %d: %w", version, err)
		}
		shouldRun = !skip
		if shouldRun {
			if _, err := tx.ExecContext(ctx, migration); err != nil {
				tx.Rollback()
				return fmt.Errorf("apply migration %d: %w", version, err)
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations(version) VALUES(?)`, version); err != nil {
			tx.Rollback()
			return fmt.Errorf("record migration %d: %w", version, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit migration %d: %w", version, err)
		}
	}

	return nil
}

type queryContexter interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

func (s *Store) columnExists(ctx context.Context, queryer queryContexter, table string, column string) (bool, error) {
	rows, err := queryer.QueryContext(ctx, fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return false, err
	}
	defer rows.Close()

	for rows.Next() {
		var cid int
		var name string
		var dataType string
		var notnull int
		var dflt sql.NullString
		var pk int
		if err := rows.Scan(&cid, &name, &dataType, &notnull, &dflt, &pk); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	if err := rows.Err(); err != nil {
		return false, err
	}
	return false, nil
}

func (s *Store) shouldSkipMigration(ctx context.Context, queryer queryContexter, migration string) (bool, error) {
	fields := strings.Fields(migration)
	if len(fields) < 6 {
		return false, nil
	}

	table := strings.Trim(fields[2], "`\"'")
	column := strings.Trim(fields[5], "`\"'")
	if !strings.EqualFold(fields[0], "alter") || !strings.EqualFold(fields[1], "table") ||
		!strings.EqualFold(fields[3], "add") || !strings.EqualFold(fields[4], "column") {
		return false, nil
	}

	hasColumn, err := s.columnExists(ctx, queryer, table, column)
	if err != nil {
		return false, err
	}
	return hasColumn, nil
}

func (s *Store) getSetting(ctx context.Context, key string) (string, error) {
	var value string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM app_settings WHERE key = ?`, key).Scan(&value)
	return value, err
}

func (s *Store) setSetting(ctx context.Context, key, value string) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO app_settings(key, value)
		VALUES (?, ?)
		ON CONFLICT(key) DO UPDATE SET value = excluded.value
	`, key, value)
	return err
}

func (s *Store) GetFeedSort(ctx context.Context) (string, error) {
	value, err := s.getSetting(ctx, "feed_sort")
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "fetched", nil
		}
		return "", err
	}
	if normalized := normalizeFeedSort(value); normalized != "" {
		return normalized, nil
	}
	return "fetched", nil
}

func (s *Store) SetFeedSort(ctx context.Context, value string) error {
	normalized := normalizeFeedSort(value)
	if normalized == "" {
		return fmt.Errorf("invalid feed sort %q", value)
	}
	return s.setSetting(ctx, "feed_sort", normalized)
}

func normalizeFeedSort(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "published", "publish", "publish_date", "published_at":
		return "published"
	case "fetched", "fetch", "fetch_date", "created", "created_at":
		return "fetched"
	default:
		return ""
	}
}

func feedSortOrderClause(sortBy string) string {
	switch normalizeFeedSort(sortBy) {
	case "published":
		return `CASE WHEN s.published_at = '' THEN s.created_at ELSE s.published_at END DESC, s.id DESC`
	default:
		return `s.created_at DESC, s.id DESC`
	}
}

func (s *Store) SyncSources(ctx context.Context, configs []config.SourceConfig) error {
	now := nowString()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	seen := map[string]struct{}{}
	for _, src := range configs {
		seen[src.Key] = struct{}{}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO sources(key, name, kind, url, enabled, refresh_minutes, discussion, summarize, origin, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'config', ?, ?)
			ON CONFLICT(key) DO UPDATE SET
				name = CASE
					WHEN sources.origin = 'user' THEN sources.name
					WHEN sources.origin = 'removed' THEN sources.name
					ELSE excluded.name
				END,
				kind = excluded.kind,
				url = excluded.url,
				enabled = CASE
					WHEN sources.origin = 'removed' THEN 0
					ELSE excluded.enabled
				END,
				refresh_minutes = CASE
					WHEN sources.origin = 'user' THEN sources.refresh_minutes
					WHEN sources.origin = 'removed' THEN sources.refresh_minutes
					ELSE excluded.refresh_minutes
				END,
				discussion = excluded.discussion,
				summarize = CASE
					WHEN sources.origin IN ('user', 'removed') THEN sources.summarize
					ELSE excluded.summarize
				END,
				origin = CASE
					WHEN sources.origin IN ('user', 'removed') THEN sources.origin
					ELSE excluded.origin
				END,
				updated_at = excluded.updated_at
		`, src.Key, src.Name, src.Kind, src.URL, boolInt(src.Enabled), src.RefreshMinutes, boolInt(src.Discussion), boolInt(src.SummarizeEnabled()), now, now); err != nil {
			return fmt.Errorf("sync source %q: %w", src.Key, err)
		}
	}

	rows, err := tx.QueryContext(ctx, `SELECT key FROM sources WHERE origin = 'config'`)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return err
		}
		if _, ok := seen[key]; !ok {
			if _, err := tx.ExecContext(ctx, `UPDATE sources SET enabled = 0, updated_at = ? WHERE key = ?`, now, key); err != nil {
				return err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	return tx.Commit()
}

func (s *Store) ListSources(ctx context.Context) ([]Source, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT id, key, name, kind, url, enabled, refresh_minutes, discussion, summarize, origin, created_at, updated_at
		FROM sources
		WHERE origin != 'removed'
		ORDER BY name
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []Source
	for rows.Next() {
		src, err := scanSource(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, src)
	}
	return out, rows.Err()
}

func (s *Store) GetSource(ctx context.Context, id int64) (Source, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, key, name, kind, url, enabled, refresh_minutes, discussion, summarize, origin, created_at, updated_at
		FROM sources
		WHERE id = ? AND origin != 'removed'
	`, id)
	return scanSource(row)
}

func (s *Store) GetSourceByURL(ctx context.Context, rawURL string) (Source, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, key, name, kind, url, enabled, refresh_minutes, discussion, summarize, origin, created_at, updated_at
		FROM sources
		WHERE url = ? AND origin != 'removed'
	`, strings.TrimSpace(rawURL))
	return scanSource(row)
}

func (s *Store) SourceKeyExists(ctx context.Context, key string) (bool, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM sources WHERE key = ?`, strings.TrimSpace(key)).Scan(&count)
	return count > 0, err
}

func (s *Store) CreateSource(ctx context.Context, in SourceInput) (Source, error) {
	now := nowString()
	origin := defaultString(in.Origin, "user")
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO sources(key, name, kind, url, enabled, refresh_minutes, discussion, summarize, origin, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, strings.TrimSpace(in.Key), strings.TrimSpace(in.Name), strings.TrimSpace(in.Kind), strings.TrimSpace(in.URL), boolInt(in.Enabled), in.RefreshMinutes, boolInt(in.Discussion), boolInt(in.Summarize), origin, now, now)
	if err != nil {
		return Source{}, err
	}
	return s.GetSourceByURL(ctx, in.URL)
}

func (s *Store) UpdateSource(ctx context.Context, id int64, name string, refreshMinutes int, summarize bool) error {
	name = strings.TrimSpace(name)
	if name == "" {
		return errors.New("source name is required")
	}
	if refreshMinutes < 0 {
		return errors.New("refresh interval must be zero or greater")
	}

	result, err := s.db.ExecContext(ctx, `
		UPDATE sources
		SET name = ?, refresh_minutes = ?, summarize = ?, origin = CASE WHEN origin = 'removed' THEN origin ELSE 'user' END, updated_at = ?
		WHERE id = ? AND origin != 'removed'
	`, name, refreshMinutes, boolInt(summarize), nowString(), id)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (s *Store) DeleteSource(ctx context.Context, id int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := nowString()
	if _, err := tx.ExecContext(ctx, `
		DELETE FROM jobs
		WHERE source_id = ?
		   OR discovered_item_id IN (SELECT id FROM discovered_items WHERE source_id = ?)
	`, id, id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM discovery_runs WHERE source_id = ?`, id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM story_sources WHERE source_id = ?`, id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM threads WHERE source_id = ?`, id); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM discovered_items WHERE source_id = ?`, id); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `
		UPDATE sources
		SET enabled = 0, origin = 'removed', updated_at = ?
		WHERE id = ? AND origin != 'removed'
	`, now, id)
	if err != nil {
		return err
	}
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rowsAffected == 0 {
		return sql.ErrNoRows
	}

	orphanStoryIDs, err := queryInt64ColumnTx(ctx, tx, `
		SELECT s.id
		FROM stories s
		WHERE NOT EXISTS (
			SELECT 1
			FROM story_sources ss
			WHERE ss.story_id = s.id
		)
	`)
	if err != nil {
		return err
	}
	if err := deleteIDsTx(ctx, tx, `DELETE FROM story_state WHERE story_id IN (%s)`, orphanStoryIDs); err != nil {
		return err
	}
	if err := deleteIDsTx(ctx, tx, `DELETE FROM jobs WHERE story_id IN (%s)`, orphanStoryIDs); err != nil {
		return err
	}
	if err := deleteIDsTx(ctx, tx, `DELETE FROM stories WHERE id IN (%s)`, orphanStoryIDs); err != nil {
		return err
	}

	orphanArticleIDs, err := queryInt64ColumnTx(ctx, tx, `
		SELECT a.id
		FROM articles a
		WHERE NOT EXISTS (
			SELECT 1
			FROM stories s
			WHERE s.article_id = a.id
		)
	`)
	if err != nil {
		return err
	}
	if err := deleteIDsTx(ctx, tx, `DELETE FROM jobs WHERE article_id IN (%s)`, orphanArticleIDs); err != nil {
		return err
	}
	if err := deleteIDsTx(ctx, tx, `DELETE FROM summaries WHERE article_id IN (%s)`, orphanArticleIDs); err != nil {
		return err
	}
	if err := deleteIDsTx(ctx, tx, `DELETE FROM articles WHERE id IN (%s)`, orphanArticleIDs); err != nil {
		return err
	}

	return tx.Commit()
}

func (s *Store) StartDiscoveryRun(ctx context.Context, sourceID int64) (int64, error) {
	now := nowString()
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO discovery_runs(source_id, status, item_count, error, started_at, finished_at, created_at, updated_at)
		VALUES (?, 'running', 0, '', ?, '', ?, ?)
	`, sourceID, now, now, now)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

func (s *Store) FinishDiscoveryRun(ctx context.Context, runID int64, status string, itemCount int, runErr string) error {
	now := nowString()
	_, err := s.db.ExecContext(ctx, `
		UPDATE discovery_runs
		SET status = ?, item_count = ?, error = ?, finished_at = ?, updated_at = ?
		WHERE id = ?
	`, status, itemCount, runErr, now, now, runID)
	return err
}

func (s *Store) UpsertDiscoveredItem(ctx context.Context, in DiscoveredItemInput) (DiscoveredItem, error) {
	if in.ExternalID == "" {
		in.ExternalID = hashParts(in.Title, in.ArticleURL, in.ThreadURL)
	}
	now := nowString()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO discovered_items(
			source_id, external_id, title, article_url, thread_url, excerpt, published_at, raw_payload, status, created_at, updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(source_id, external_id) DO UPDATE SET
			title = excluded.title,
			article_url = excluded.article_url,
			thread_url = excluded.thread_url,
			excerpt = excluded.excerpt,
			published_at = excluded.published_at,
			raw_payload = excluded.raw_payload,
			status = excluded.status,
			updated_at = excluded.updated_at
	`, in.SourceID, in.ExternalID, strings.TrimSpace(in.Title), strings.TrimSpace(in.ArticleURL), strings.TrimSpace(in.ThreadURL), strings.TrimSpace(in.Excerpt), timeString(in.PublishedAt), in.RawPayload, defaultString(in.Status, "discovered"), now, now)
	if err != nil {
		return DiscoveredItem{}, err
	}
	return s.lookupDiscoveredItem(ctx, in.SourceID, in.ExternalID)
}

func (s *Store) GetDiscoveredItem(ctx context.Context, id int64) (DiscoveredItem, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, source_id, external_id, title, article_url, thread_url, excerpt, published_at, raw_payload, status, created_at, updated_at
		FROM discovered_items
		WHERE id = ?
	`, id)
	return scanDiscoveredItem(row)
}

func (s *Store) lookupDiscoveredItem(ctx context.Context, sourceID int64, externalID string) (DiscoveredItem, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, source_id, external_id, title, article_url, thread_url, excerpt, published_at, raw_payload, status, created_at, updated_at
		FROM discovered_items
		WHERE source_id = ? AND external_id = ?
	`, sourceID, externalID)
	return scanDiscoveredItem(row)
}

func (s *Store) UpsertThread(ctx context.Context, in ThreadInput) (Thread, error) {
	now := nowString()
	if in.ExternalID == "" {
		in.ExternalID = hashParts(in.ThreadURL, in.Title)
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO threads(source_id, external_id, title, thread_url, submitter, score, comment_count, published_at, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(source_id, external_id) DO UPDATE SET
			title = excluded.title,
			thread_url = excluded.thread_url,
			submitter = excluded.submitter,
			score = excluded.score,
			comment_count = excluded.comment_count,
			published_at = excluded.published_at,
			updated_at = excluded.updated_at
	`, in.SourceID, in.ExternalID, strings.TrimSpace(in.Title), strings.TrimSpace(in.ThreadURL), in.Submitter, in.Score, in.CommentCount, timeString(in.PublishedAt), now, now)
	if err != nil {
		return Thread{}, err
	}

	row := s.db.QueryRowContext(ctx, `
		SELECT id, source_id, external_id, title, thread_url, submitter, score, comment_count, published_at, created_at, updated_at
		FROM threads
		WHERE source_id = ? AND external_id = ?
	`, in.SourceID, in.ExternalID)

	var thread Thread
	var publishedAt, createdAt, updatedAt string
	if err := row.Scan(&thread.ID, &thread.SourceID, &thread.ExternalID, &thread.Title, &thread.ThreadURL, &thread.Submitter, &thread.Score, &thread.CommentCount, &publishedAt, &createdAt, &updatedAt); err != nil {
		return Thread{}, err
	}
	thread.PublishedAt = parseTime(publishedAt)
	thread.CreatedAt = parseTime(createdAt)
	thread.UpdatedAt = parseTime(updatedAt)
	return thread, nil
}

func (s *Store) UpsertArticle(ctx context.Context, in ArticleInput) (Article, error) {
	if in.CanonicalURL == "" {
		in.CanonicalURL = in.SourceURL
	}
	now := nowString()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO articles(
			source_url, canonical_url, title, author, published_at, excerpt, raw_text, clean_text, content_hash, extraction_status, last_extracted_at, created_at, updated_at
		)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(canonical_url) DO UPDATE SET
			source_url = excluded.source_url,
			title = excluded.title,
			author = excluded.author,
			published_at = CASE WHEN excluded.published_at != '' THEN excluded.published_at ELSE articles.published_at END,
			excerpt = excluded.excerpt,
			raw_text = excluded.raw_text,
			clean_text = excluded.clean_text,
			content_hash = excluded.content_hash,
			extraction_status = excluded.extraction_status,
			last_extracted_at = excluded.last_extracted_at,
			updated_at = excluded.updated_at
	`, in.SourceURL, in.CanonicalURL, strings.TrimSpace(in.Title), strings.TrimSpace(in.Author), timeString(in.PublishedAt), strings.TrimSpace(in.Excerpt), in.RawText, in.CleanText, in.ContentHash, defaultString(in.ExtractionStatus, "pending"), now, now, now)
	if err != nil {
		return Article{}, err
	}
	return s.GetArticleByCanonicalURL(ctx, in.CanonicalURL)
}

func (s *Store) GetArticle(ctx context.Context, id int64) (Article, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, source_url, canonical_url, title, author, published_at, excerpt, raw_text, clean_text, content_hash, extraction_status, last_extracted_at, created_at, updated_at
		FROM articles
		WHERE id = ?
	`, id)
	return scanArticle(row)
}

func (s *Store) GetArticleByCanonicalURL(ctx context.Context, canonicalURL string) (Article, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, source_url, canonical_url, title, author, published_at, excerpt, raw_text, clean_text, content_hash, extraction_status, last_extracted_at, created_at, updated_at
		FROM articles
		WHERE canonical_url = ?
	`, canonicalURL)
	return scanArticle(row)
}

func (s *Store) UpsertStoryForArticle(ctx context.Context, article Article, discovered DiscoveredItem, threadID int64) (Story, error) {
	now := nowString()
	title := firstNonEmpty(article.Title, discovered.Title)
	url := firstNonEmpty(article.CanonicalURL, article.SourceURL, discovered.ArticleURL)
	published := article.PublishedAt
	if published.IsZero() {
		published = discovered.PublishedAt
	}
	status := "ready"
	isPartial := 0
	if article.ExtractionStatus != "ready" {
		status = "partial"
		isPartial = 1
	}

	story, err := s.getStoryByArticleID(ctx, article.ID)
	switch {
	case err == nil:
		if _, err := s.db.ExecContext(ctx, `
			UPDATE stories
			SET primary_title = ?, primary_url = ?, published_at = ?, status = ?, is_partial = ?, updated_at = ?
			WHERE id = ?
		`, title, url, timeString(published), status, isPartial, now, story.ID); err != nil {
			return Story{}, err
		}
	case errors.Is(err, sql.ErrNoRows):
		result, err := s.db.ExecContext(ctx, `
			INSERT INTO stories(article_id, primary_title, primary_url, published_at, status, summary_status, is_partial, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?, 'pending', ?, ?, ?)
		`, article.ID, title, url, timeString(published), status, isPartial, now, now)
		if err != nil {
			return Story{}, err
		}
		storyID, err := result.LastInsertId()
		if err != nil {
			return Story{}, err
		}
		story, err = s.GetStory(ctx, storyID)
		if err != nil {
			return Story{}, err
		}
	default:
		return Story{}, err
	}

	if err := s.LinkStorySource(ctx, story.ID, discovered.SourceID, discovered.ID, threadID, firstNonEmpty(discovered.ThreadURL, discovered.ArticleURL)); err != nil {
		return Story{}, err
	}
	return s.GetStory(ctx, story.ID)
}

func (s *Store) UpsertDiscussionStory(ctx context.Context, discovered DiscoveredItem, threadID int64) (Story, error) {
	if threadID == 0 {
		return Story{}, errors.New("discussion story requires thread")
	}
	if story, err := s.getStoryByThreadID(ctx, threadID); err == nil {
		if _, err := s.db.ExecContext(ctx, `
			UPDATE stories
			SET primary_title = ?, primary_url = ?, published_at = ?, status = 'partial', is_partial = 1, updated_at = ?
			WHERE id = ?
		`, discovered.Title, firstNonEmpty(discovered.ThreadURL, discovered.ArticleURL), timeString(discovered.PublishedAt), nowString(), story.ID); err != nil {
			return Story{}, err
		}
		if err := s.LinkStorySource(ctx, story.ID, discovered.SourceID, discovered.ID, threadID, firstNonEmpty(discovered.ThreadURL, discovered.ArticleURL)); err != nil {
			return Story{}, err
		}
		return s.GetStory(ctx, story.ID)
	} else if !errors.Is(err, sql.ErrNoRows) {
		return Story{}, err
	}

	now := nowString()
	result, err := s.db.ExecContext(ctx, `
		INSERT INTO stories(article_id, primary_title, primary_url, published_at, status, summary_status, is_partial, created_at, updated_at)
		VALUES (0, ?, ?, ?, 'partial', 'pending', 1, ?, ?)
	`, discovered.Title, firstNonEmpty(discovered.ThreadURL, discovered.ArticleURL), timeString(discovered.PublishedAt), now, now)
	if err != nil {
		return Story{}, err
	}
	storyID, err := result.LastInsertId()
	if err != nil {
		return Story{}, err
	}
	if err := s.LinkStorySource(ctx, storyID, discovered.SourceID, discovered.ID, threadID, firstNonEmpty(discovered.ThreadURL, discovered.ArticleURL)); err != nil {
		return Story{}, err
	}
	return s.GetStory(ctx, storyID)
}

func (s *Store) LinkStorySource(ctx context.Context, storyID, sourceID, discoveredItemID, threadID int64, sourceURL string) error {
	now := nowString()
	fingerprint := fmt.Sprintf("%d:%d:%d:%d", storyID, sourceID, discoveredItemID, threadID)
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO story_sources(story_id, source_id, discovered_item_id, thread_id, source_url, fingerprint, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(fingerprint) DO UPDATE SET
			source_url = excluded.source_url,
			updated_at = excluded.updated_at
	`, storyID, sourceID, discoveredItemID, threadID, sourceURL, fingerprint, now, now)
	return err
}

func (s *Store) GetStory(ctx context.Context, id int64) (Story, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, article_id, primary_title, primary_url, published_at, status, summary_status, is_partial, created_at, updated_at
		FROM stories
		WHERE id = ?
	`, id)
	return scanStory(row)
}

func (s *Store) getStoryByArticleID(ctx context.Context, articleID int64) (Story, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, article_id, primary_title, primary_url, published_at, status, summary_status, is_partial, created_at, updated_at
		FROM stories
		WHERE article_id = ?
	`, articleID)
	return scanStory(row)
}

func (s *Store) getStoryByThreadID(ctx context.Context, threadID int64) (Story, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT s.id, s.article_id, s.primary_title, s.primary_url, s.published_at, s.status, s.summary_status, s.is_partial, s.created_at, s.updated_at
		FROM stories s
		JOIN story_sources ss ON ss.story_id = s.id
		WHERE ss.thread_id = ?
		LIMIT 1
	`, threadID)
	return scanStory(row)
}

func (s *Store) LookupOrCreateStoryState(ctx context.Context, storyID int64) error {
	now := nowString()
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO story_state(story_id, is_read, is_saved, is_viewed, is_hidden, created_at, updated_at)
		VALUES (?, 0, 0, 0, 0, ?, ?)
		ON CONFLICT(story_id) DO NOTHING
	`, storyID, now, now)
	return err
}

func (s *Store) SetStoryRead(ctx context.Context, storyID int64, read bool) error {
	if err := s.LookupOrCreateStoryState(ctx, storyID); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE story_state
		SET is_read = ?, updated_at = ?
		WHERE story_id = ?
	`, boolInt(read), nowString(), storyID)
	return err
}

func (s *Store) SetStorySaved(ctx context.Context, storyID int64, saved bool) error {
	if err := s.LookupOrCreateStoryState(ctx, storyID); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE story_state
		SET is_saved = ?, updated_at = ?
		WHERE story_id = ?
	`, boolInt(saved), nowString(), storyID)
	return err
}

func (s *Store) SetStoryViewed(ctx context.Context, storyID int64, viewed bool) error {
	if err := s.LookupOrCreateStoryState(ctx, storyID); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE story_state
		SET is_viewed = ?, updated_at = ?
		WHERE story_id = ?
	`, boolInt(viewed), nowString(), storyID)
	return err
}

func (s *Store) SetStoryHidden(ctx context.Context, storyID int64, hidden bool) error {
	if err := s.LookupOrCreateStoryState(ctx, storyID); err != nil {
		return err
	}
	_, err := s.db.ExecContext(ctx, `
		UPDATE story_state
		SET is_hidden = ?, updated_at = ?
		WHERE story_id = ?
	`, boolInt(hidden), nowString(), storyID)
	return err
}

func (s *Store) UpsertSummary(ctx context.Context, in SummaryInput) (Summary, error) {
	bulletsJSON, err := json.Marshal(in.Bullets)
	if err != nil {
		return Summary{}, err
	}
	tagsJSON, err := json.Marshal(in.Tags)
	if err != nil {
		return Summary{}, err
	}
	now := nowString()
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO summaries(article_id, content_hash, model_id, prompt_version, abstract, bullets_json, tags_json, status, error, created_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(article_id, content_hash, model_id, prompt_version) DO UPDATE SET
			abstract = excluded.abstract,
			bullets_json = excluded.bullets_json,
			tags_json = excluded.tags_json,
			status = excluded.status,
			error = excluded.error,
			updated_at = excluded.updated_at
	`, in.ArticleID, in.ContentHash, in.ModelID, in.PromptVersion, in.Abstract, string(bulletsJSON), string(tagsJSON), in.Status, in.Error, now, now)
	if err != nil {
		return Summary{}, err
	}
	if in.Status == "ready" {
		if _, err := s.db.ExecContext(ctx, `UPDATE stories SET summary_status = 'ready', updated_at = ? WHERE article_id = ?`, now, in.ArticleID); err != nil {
			return Summary{}, err
		}
	}
	return s.GetSummary(ctx, in.ArticleID, in.ContentHash, in.ModelID, in.PromptVersion)
}

func (s *Store) GetSummary(ctx context.Context, articleID int64, contentHash, modelID, promptVersion string) (Summary, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, article_id, content_hash, model_id, prompt_version, abstract, bullets_json, tags_json, status, error, created_at, updated_at
		FROM summaries
		WHERE article_id = ? AND content_hash = ? AND model_id = ? AND prompt_version = ?
	`, articleID, contentHash, modelID, promptVersion)
	return scanSummary(row)
}

func (s *Store) GetLatestSummaryForArticle(ctx context.Context, articleID int64) (Summary, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT id, article_id, content_hash, model_id, prompt_version, abstract, bullets_json, tags_json, status, error, created_at, updated_at
		FROM summaries
		WHERE article_id = ? AND status = 'ready'
		ORDER BY updated_at DESC
		LIMIT 1
	`, articleID)
	return scanSummary(row)
}

func (s *Store) ListFeed(ctx context.Context, filter FeedFilter) ([]StoryCard, bool, error) {
	if filter.Page <= 0 {
		filter.Page = 1
	}
	if filter.PageSize <= 0 {
		filter.PageSize = 30
	}

	var args []any
	var where []string
	switch filter.State {
	case "saved":
		where = append(where, `COALESCE(st.is_saved, 0) = 1`)
	case "read":
		where = append(where, `COALESCE(st.is_read, 0) = 1`)
	default:
		if filter.State == "unread" || filter.State == "" {
			where = append(where, `COALESCE(st.is_read, 0) = 0`)
		}
	}
	if filter.SourceKey != "" {
		where = append(where, `EXISTS (
			SELECT 1
			FROM story_sources ss2
			JOIN sources so2 ON so2.id = ss2.source_id
			WHERE ss2.story_id = s.id AND so2.key = ?
		)`)
		args = append(args, filter.SourceKey)
	}
	if filter.Tag != "" {
		where = append(where, `EXISTS (
			SELECT 1
			FROM json_each(COALESCE(sm.tags_json, '[]')) jt
			WHERE LOWER(TRIM(REPLACE(jt.value, '+', ' '))) = ?
		)`)
		args = append(args, normalizeTag(filter.Tag))
	}
	if !filter.IncludeHidden {
		where = append(where, `COALESCE(st.is_hidden, 0) = 0`)
	}
	if cutoff, ok := timeWindowCutoff(filter.TimeWindow, filter.SortBy); ok {
		where = append(where, timeWindowClause(filter.SortBy))
		args = append(args, cutoff)
	}

	query := `
		SELECT
			s.id, s.article_id, s.primary_title, s.primary_url, s.published_at, s.status, s.summary_status, s.is_partial, s.created_at, s.updated_at,
			COALESCE(a.canonical_url, ''), COALESCE(a.excerpt, ''), COALESCE(a.raw_text, ''), COALESCE(a.clean_text, ''), COALESCE(a.extraction_status, ''),
			COALESCE(st.is_read, 0), COALESCE(st.is_saved, 0), COALESCE(st.is_viewed, 0), COALESCE(st.is_hidden, 0),
			COALESCE(sm.id, 0), COALESCE(sm.article_id, 0), COALESCE(sm.content_hash, ''), COALESCE(sm.model_id, ''), COALESCE(sm.prompt_version, ''),
			COALESCE(sm.abstract, ''), COALESCE(sm.bullets_json, '[]'), COALESCE(sm.tags_json, '[]'), COALESCE(sm.status, ''), COALESCE(sm.error, ''),
			COALESCE(sm.created_at, ''), COALESCE(sm.updated_at, '')
		FROM stories s
		LEFT JOIN articles a ON a.id = s.article_id
		LEFT JOIN story_state st ON st.story_id = s.id
		LEFT JOIN summaries sm ON sm.id = (
			SELECT s2.id
			FROM summaries s2
			WHERE s2.article_id = s.article_id AND s2.status = 'ready'
			ORDER BY s2.updated_at DESC
			LIMIT 1
		)
	`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += `
		ORDER BY
			` + feedSortOrderClause(filter.SortBy) + `
		LIMIT ? OFFSET ?
	`
	args = append(args, filter.PageSize+1, (filter.Page-1)*filter.PageSize)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	var cards []StoryCard
	for rows.Next() {
		card, err := scanStoryCard(rows)
		if err != nil {
			return nil, false, err
		}
		cards = append(cards, card)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	if err := rows.Close(); err != nil {
		return nil, false, err
	}

	hasMore := false
	if len(cards) > filter.PageSize {
		hasMore = true
		cards = cards[:filter.PageSize]
	}
	cardPointers := make([]*StoryCard, 0, len(cards))
	for i := range cards {
		cardPointers = append(cardPointers, &cards[i])
	}
	if err := s.attachReferences(ctx, cardPointers); err != nil {
		return nil, false, err
	}
	return cards, hasMore, nil
}

func (s *Store) SearchStories(ctx context.Context, filter SearchFilter) ([]StoryCard, bool, error) {
	if filter.Page <= 0 {
		filter.Page = 1
	}
	if filter.PageSize <= 0 {
		filter.PageSize = 30
	}

	terms := searchTerms(filter.Query)
	if len(terms) == 0 {
		state := filter.State
		if state == "" {
			state = "all"
		}
		return s.ListFeed(ctx, FeedFilter{
			State:         state,
			SourceKey:     filter.SourceKey,
			Tag:           filter.Tag,
			IncludeHidden: true,
			Page:          filter.Page,
			PageSize:      filter.PageSize,
		})
	}

	var args []any
	var where []string
	switch filter.State {
	case "saved":
		where = append(where, `COALESCE(st.is_saved, 0) = 1`)
	case "read":
		where = append(where, `COALESCE(st.is_read, 0) = 1`)
	case "all", "":
	default:
		where = append(where, `COALESCE(st.is_read, 0) = 0`)
	}
	if filter.SourceKey != "" {
		where = append(where, `EXISTS (
			SELECT 1
			FROM story_sources ss2
			JOIN sources so2 ON so2.id = ss2.source_id
			WHERE ss2.story_id = s.id AND so2.key = ?
		)`)
		args = append(args, filter.SourceKey)
	}
	if filter.Tag != "" {
		where = append(where, `EXISTS (
			SELECT 1
			FROM json_each(COALESCE(sm.tags_json, '[]')) jt
			WHERE LOWER(TRIM(REPLACE(jt.value, '+', ' '))) = ?
		)`)
		args = append(args, normalizeTag(filter.Tag))
	}

	for _, term := range terms {
		searchPattern := likePattern(term)
		where = append(where, `(
			LOWER(s.primary_title) LIKE ? ESCAPE '\'
			OR LOWER(COALESCE(a.excerpt, '')) LIKE ? ESCAPE '\'
			OR LOWER(COALESCE(a.clean_text, '')) LIKE ? ESCAPE '\'
			OR LOWER(COALESCE(a.raw_text, '')) LIKE ? ESCAPE '\'
			OR LOWER(COALESCE(sm.abstract, '')) LIKE ? ESCAPE '\'
			OR LOWER(COALESCE(sm.tags_json, '[]')) LIKE ? ESCAPE '\'
			OR LOWER(COALESCE(a.canonical_url, '')) LIKE ? ESCAPE '\'
			OR LOWER(COALESCE(s.primary_url, '')) LIKE ? ESCAPE '\'
		)`)
		for i := 0; i < 8; i++ {
			args = append(args, searchPattern)
		}
	}
	orderPattern := likePattern(strings.ToLower(strings.TrimSpace(filter.Query)))
	if orderPattern == "%%" {
		orderPattern = likePattern(terms[0])
	}

	query := `
		SELECT
			s.id, s.article_id, s.primary_title, s.primary_url, s.published_at, s.status, s.summary_status, s.is_partial, s.created_at, s.updated_at,
			COALESCE(a.canonical_url, ''), COALESCE(a.excerpt, ''), COALESCE(a.raw_text, ''), COALESCE(a.clean_text, ''), COALESCE(a.extraction_status, ''),
			COALESCE(st.is_read, 0), COALESCE(st.is_saved, 0), COALESCE(st.is_viewed, 0), COALESCE(st.is_hidden, 0),
			COALESCE(sm.id, 0), COALESCE(sm.article_id, 0), COALESCE(sm.content_hash, ''), COALESCE(sm.model_id, ''), COALESCE(sm.prompt_version, ''),
			COALESCE(sm.abstract, ''), COALESCE(sm.bullets_json, '[]'), COALESCE(sm.tags_json, '[]'), COALESCE(sm.status, ''), COALESCE(sm.error, ''),
			COALESCE(sm.created_at, ''), COALESCE(sm.updated_at, '')
		FROM stories s
		LEFT JOIN articles a ON a.id = s.article_id
		LEFT JOIN story_state st ON st.story_id = s.id
		LEFT JOIN summaries sm ON sm.id = (
			SELECT s2.id
			FROM summaries s2
			WHERE s2.article_id = s.article_id AND s2.status = 'ready'
			ORDER BY s2.updated_at DESC
			LIMIT 1
		)
		WHERE ` + strings.Join(where, " AND ") + `
		ORDER BY
			CASE
				WHEN LOWER(s.primary_title) LIKE ? ESCAPE '\' THEN 0
				WHEN LOWER(COALESCE(sm.abstract, '')) LIKE ? ESCAPE '\' THEN 1
				WHEN LOWER(COALESCE(a.clean_text, '')) LIKE ? ESCAPE '\' THEN 2
				ELSE 3
			END ASC,
			CASE WHEN s.published_at = '' THEN s.updated_at ELSE s.published_at END DESC,
			s.id DESC
		LIMIT ? OFFSET ?
	`
	args = append(args, orderPattern, orderPattern, orderPattern, filter.PageSize+1, (filter.Page-1)*filter.PageSize)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	var cards []StoryCard
	for rows.Next() {
		card, err := scanStoryCard(rows)
		if err != nil {
			return nil, false, err
		}
		cards = append(cards, card)
	}
	if err := rows.Err(); err != nil {
		return nil, false, err
	}
	if err := rows.Close(); err != nil {
		return nil, false, err
	}

	hasMore := false
	if len(cards) > filter.PageSize {
		hasMore = true
		cards = cards[:filter.PageSize]
	}
	cardPointers := make([]*StoryCard, 0, len(cards))
	for i := range cards {
		cardPointers = append(cardPointers, &cards[i])
	}
	if err := s.attachReferences(ctx, cardPointers); err != nil {
		return nil, false, err
	}
	return cards, hasMore, nil
}

func (s *Store) ListFeedStoryIDs(ctx context.Context, filter FeedFilter) ([]int64, error) {
	var args []any
	var where []string
	switch filter.State {
	case "saved":
		where = append(where, `COALESCE(st.is_saved, 0) = 1`)
	case "read":
		where = append(where, `COALESCE(st.is_read, 0) = 1`)
	default:
		if filter.State == "unread" || filter.State == "" {
			where = append(where, `COALESCE(st.is_read, 0) = 0`)
		}
	}
	if filter.SourceKey != "" {
		where = append(where, `EXISTS (
			SELECT 1
			FROM story_sources ss2
			JOIN sources so2 ON so2.id = ss2.source_id
			WHERE ss2.story_id = s.id AND so2.key = ?
		)`)
		args = append(args, filter.SourceKey)
	}
	if filter.Tag != "" {
		where = append(where, `EXISTS (
			SELECT 1
			FROM json_each(COALESCE(sm.tags_json, '[]')) jt
			WHERE LOWER(TRIM(REPLACE(jt.value, '+', ' '))) = ?
		)`)
		args = append(args, normalizeTag(filter.Tag))
	}
	if !filter.IncludeHidden {
		where = append(where, `COALESCE(st.is_hidden, 0) = 0`)
	}
	if cutoff, ok := timeWindowCutoff(filter.TimeWindow, filter.SortBy); ok {
		where = append(where, timeWindowClause(filter.SortBy))
		args = append(args, cutoff)
	}

	query := `
		SELECT s.id
		FROM stories s
		LEFT JOIN story_state st ON st.story_id = s.id
		LEFT JOIN summaries sm ON sm.id = (
			SELECT s2.id
			FROM summaries s2
			WHERE s2.article_id = s.article_id AND s2.status = 'ready'
			ORDER BY s2.updated_at DESC
			LIMIT 1
		)
	`
	if len(where) > 0 {
		query += " WHERE " + strings.Join(where, " AND ")
	}
	query += `
		ORDER BY
			` + feedSortOrderClause(filter.SortBy) + `
	`
	return s.listStoryIDs(ctx, query, args...)
}

func (s *Store) SearchStoryIDs(ctx context.Context, filter SearchFilter) ([]int64, error) {
	if strings.TrimSpace(filter.Query) == "" {
		state := filter.State
		if state == "" {
			state = "all"
		}
		return s.ListFeedStoryIDs(ctx, FeedFilter{
			State:         state,
			SourceKey:     filter.SourceKey,
			Tag:           filter.Tag,
			IncludeHidden: true,
			SortBy:        filter.SortBy,
		})
	}

	terms := searchTerms(filter.Query)
	if len(terms) == 0 {
		return nil, nil
	}

	var args []any
	var where []string
	switch filter.State {
	case "saved":
		where = append(where, `COALESCE(st.is_saved, 0) = 1`)
	case "read":
		where = append(where, `COALESCE(st.is_read, 0) = 1`)
	case "all", "":
	default:
		where = append(where, `COALESCE(st.is_read, 0) = 0`)
	}
	if filter.SourceKey != "" {
		where = append(where, `EXISTS (
			SELECT 1
			FROM story_sources ss2
			JOIN sources so2 ON so2.id = ss2.source_id
			WHERE ss2.story_id = s.id AND so2.key = ?
		)`)
		args = append(args, filter.SourceKey)
	}
	if filter.Tag != "" {
		where = append(where, `EXISTS (
			SELECT 1
			FROM json_each(COALESCE(sm.tags_json, '[]')) jt
			WHERE LOWER(TRIM(REPLACE(jt.value, '+', ' '))) = ?
		)`)
		args = append(args, normalizeTag(filter.Tag))
	}
	for _, term := range terms {
		searchPattern := likePattern(term)
		where = append(where, `(
			LOWER(s.primary_title) LIKE ? ESCAPE '\'
			OR LOWER(COALESCE(a.excerpt, '')) LIKE ? ESCAPE '\'
			OR LOWER(COALESCE(a.clean_text, '')) LIKE ? ESCAPE '\'
			OR LOWER(COALESCE(a.raw_text, '')) LIKE ? ESCAPE '\'
			OR LOWER(COALESCE(sm.abstract, '')) LIKE ? ESCAPE '\'
			OR LOWER(COALESCE(sm.tags_json, '[]')) LIKE ? ESCAPE '\'
			OR LOWER(COALESCE(a.canonical_url, '')) LIKE ? ESCAPE '\'
			OR LOWER(COALESCE(s.primary_url, '')) LIKE ? ESCAPE '\'
		)`)
		for i := 0; i < 8; i++ {
			args = append(args, searchPattern)
		}
	}

	orderPattern := likePattern(strings.ToLower(strings.TrimSpace(filter.Query)))
	if orderPattern == "%%" {
		orderPattern = likePattern(terms[0])
	}

	query := `
		SELECT s.id
		FROM stories s
		LEFT JOIN articles a ON a.id = s.article_id
		LEFT JOIN story_state st ON st.story_id = s.id
		LEFT JOIN summaries sm ON sm.id = (
			SELECT s2.id
			FROM summaries s2
			WHERE s2.article_id = s.article_id AND s2.status = 'ready'
			ORDER BY s2.updated_at DESC
			LIMIT 1
		)
		WHERE ` + strings.Join(where, " AND ") + `
		ORDER BY
			CASE
				WHEN LOWER(s.primary_title) LIKE ? ESCAPE '\' THEN 0
				WHEN LOWER(COALESCE(sm.abstract, '')) LIKE ? ESCAPE '\' THEN 1
				WHEN LOWER(COALESCE(a.clean_text, '')) LIKE ? ESCAPE '\' THEN 2
				ELSE 3
			END ASC,
			CASE WHEN s.published_at = '' THEN s.updated_at ELSE s.published_at END DESC,
			s.id DESC
	`
	args = append(args, orderPattern, orderPattern, orderPattern)

	return s.listStoryIDs(ctx, query, args...)
}

func (s *Store) GetStoryDetail(ctx context.Context, id int64) (StoryDetail, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			s.id, s.article_id, s.primary_title, s.primary_url, s.published_at, s.status, s.summary_status, s.is_partial, s.created_at, s.updated_at,
			COALESCE(a.canonical_url, ''), COALESCE(a.excerpt, ''), COALESCE(a.raw_text, ''), COALESCE(a.clean_text, ''), COALESCE(a.extraction_status, ''),
			COALESCE(st.is_read, 0), COALESCE(st.is_saved, 0), COALESCE(st.is_viewed, 0), COALESCE(st.is_hidden, 0),
			COALESCE(sm.id, 0), COALESCE(sm.article_id, 0), COALESCE(sm.content_hash, ''), COALESCE(sm.model_id, ''), COALESCE(sm.prompt_version, ''),
			COALESCE(sm.abstract, ''), COALESCE(sm.bullets_json, '[]'), COALESCE(sm.tags_json, '[]'), COALESCE(sm.status, ''), COALESCE(sm.error, ''),
			COALESCE(sm.created_at, ''), COALESCE(sm.updated_at, ''),
			COALESCE(a.author, '')
		FROM stories s
		LEFT JOIN articles a ON a.id = s.article_id
		LEFT JOIN story_state st ON st.story_id = s.id
		LEFT JOIN summaries sm ON sm.id = (
			SELECT s2.id
			FROM summaries s2
			WHERE s2.article_id = s.article_id AND s2.status = 'ready'
			ORDER BY s2.updated_at DESC
			LIMIT 1
		)
		WHERE s.id = ?
	`, id)
	if err != nil {
		return StoryDetail{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		return StoryDetail{}, sql.ErrNoRows
	}

	card, author, err := scanStoryDetail(rows)
	if err != nil {
		return StoryDetail{}, err
	}
	if err := rows.Close(); err != nil {
		return StoryDetail{}, err
	}
	if err := s.attachReferences(ctx, []*StoryCard{&card}); err != nil {
		return StoryDetail{}, err
	}
	return StoryDetail{
		StoryCard:   card,
		Author:      author,
		PublishedAt: card.PublishedAt,
	}, nil
}

func (s *Store) attachReferences(ctx context.Context, cards []*StoryCard) error {
	if len(cards) == 0 {
		return nil
	}
	ids := make([]string, 0, len(cards))
	args := make([]any, 0, len(cards))
	index := map[int64]*StoryCard{}
	for _, card := range cards {
		ids = append(ids, "?")
		args = append(args, card.ID)
		index[card.ID] = card
	}

	query := fmt.Sprintf(`
		SELECT ss.story_id, so.key, so.name, COALESCE(th.thread_url, ''), COALESCE(ss.source_url, '')
		FROM story_sources ss
		JOIN sources so ON so.id = ss.source_id
		LEFT JOIN threads th ON th.id = ss.thread_id
		WHERE ss.story_id IN (%s)
		ORDER BY so.name ASC
	`, strings.Join(ids, ","))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var ref StoryReference
		if err := rows.Scan(&ref.StoryID, &ref.SourceKey, &ref.SourceName, &ref.ThreadURL, &ref.SourceURL); err != nil {
			return err
		}
		if card := index[ref.StoryID]; card != nil {
			card.References = append(card.References, ref)
		}
	}
	return rows.Err()
}

func (s *Store) ListSourceStatus(ctx context.Context) ([]SourceStatus, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			src.id, src.key, src.name, src.kind, src.url, src.enabled, src.refresh_minutes, src.discussion, src.summarize, src.origin, src.created_at, src.updated_at,
			COALESCE((
				SELECT COALESCE(NULLIF(dr.finished_at, ''), dr.started_at)
				FROM discovery_runs dr
				WHERE dr.source_id = src.id
				ORDER BY dr.updated_at DESC, dr.id DESC
				LIMIT 1
			), ''),
			COALESCE((
				SELECT dr.status
				FROM discovery_runs dr
				WHERE dr.source_id = src.id
				ORDER BY dr.updated_at DESC, dr.id DESC
				LIMIT 1
			), ''),
			COALESCE((
				SELECT dr.error
				FROM discovery_runs dr
				WHERE dr.source_id = src.id
				ORDER BY dr.updated_at DESC, dr.id DESC
				LIMIT 1
			), ''),
			COALESCE((
				SELECT COUNT(*)
				FROM jobs j
				WHERE j.source_id = src.id AND j.status IN ('pending', 'running')
			), 0)
		FROM sources src
		WHERE src.origin != 'removed'
		ORDER BY src.name ASC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []SourceStatus
	for rows.Next() {
		var item SourceStatus
		var enabled, discussion, summarize int
		var origin string
		var createdAt, updatedAt, lastAt string
		if err := rows.Scan(&item.ID, &item.Key, &item.Name, &item.Kind, &item.URL, &enabled, &item.RefreshMinutes, &discussion, &summarize, &origin, &createdAt, &updatedAt, &lastAt, &item.LastStatus, &item.LastError, &item.PendingJobs); err != nil {
			return nil, err
		}
		item.Enabled = enabled == 1
		item.Discussion = discussion == 1
		item.Summarize = summarize == 1
		item.Origin = origin
		item.CreatedAt = parseTime(createdAt)
		item.UpdatedAt = parseTime(updatedAt)
		item.LastDiscoveryAt = parseTime(lastAt)
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Store) ListTags(ctx context.Context, limit int) ([]TagCount, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT COALESCE(sm.tags_json, '[]')
		FROM stories s
		LEFT JOIN summaries sm ON sm.id = (
			SELECT s2.id
			FROM summaries s2
			WHERE s2.article_id = s.article_id AND s2.status = 'ready'
			ORDER BY s2.updated_at DESC
			LIMIT 1
		)
		WHERE sm.id IS NOT NULL
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := map[string]int{}
	for rows.Next() {
		var tagsJSON string
		if err := rows.Scan(&tagsJSON); err != nil {
			return nil, err
		}
		var tags []string
		if err := json.Unmarshal([]byte(tagsJSON), &tags); err != nil {
			continue
		}
		seen := map[string]struct{}{}
		for _, tag := range tags {
			tag = normalizeTag(tag)
			if tag == "" {
				continue
			}
			if _, ok := seen[tag]; ok {
				continue
			}
			seen[tag] = struct{}{}
			counts[tag]++
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	out := make([]TagCount, 0, len(counts))
	for tag, count := range counts {
		out = append(out, TagCount{Tag: tag, Count: count})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Count == out[j].Count {
			return out[i].Tag < out[j].Tag
		}
		return out[i].Count > out[j].Count
	})
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, nil
}

func (s *Store) ListJobStatusCounts(ctx context.Context) ([]JobStatusCount, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT kind, status, COUNT(*)
		FROM jobs
		GROUP BY kind, status
		ORDER BY kind, status
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []JobStatusCount
	for rows.Next() {
		var item JobStatusCount
		if err := rows.Scan(&item.Kind, &item.Status, &item.Count); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Store) ListProblemStories(ctx context.Context, limit int) ([]ProblemStory, error) {
	if limit <= 0 {
		limit = 50
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT
			s.id,
			s.primary_title,
			COALESCE((
				SELECT GROUP_CONCAT(so.name, ', ')
				FROM story_sources ss2
				JOIN sources so ON so.id = ss2.source_id
				WHERE ss2.story_id = s.id
			), ''),
			CASE WHEN s.published_at = '' THEN s.updated_at ELSE s.published_at END,
			COALESCE(a.extraction_status, ''),
			s.summary_status,
			COALESCE((
				SELECT s2.error
				FROM summaries s2
				WHERE s2.article_id = s.article_id
				ORDER BY s2.updated_at DESC
				LIMIT 1
			), ''),
			CASE WHEN s.article_id != 0 THEN 1 ELSE 0 END,
			COALESCE(a.canonical_url, '')
		FROM stories s
		LEFT JOIN articles a ON a.id = s.article_id
		WHERE COALESCE(a.extraction_status, '') != 'ready'
		   OR (s.summary_status != 'ready' AND EXISTS (
				SELECT 1
				FROM story_sources ss
				JOIN sources so ON so.id = ss.source_id
				WHERE ss.story_id = s.id
				 AND so.summarize = 1
				 AND so.origin != 'removed'
			))
		ORDER BY
			CASE WHEN s.published_at = '' THEN s.updated_at ELSE s.published_at END DESC,
			s.id DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ProblemStory
	for rows.Next() {
		var item ProblemStory
		var publishedAt, articleURL string
		var hasArticle int
		if err := rows.Scan(&item.StoryID, &item.Title, &item.SourceNames, &publishedAt, &item.ArticleStatus, &item.SummaryStatus, &item.SummaryError, &hasArticle, &articleURL); err != nil {
			return nil, err
		}
		item.PublishedAt = parseTime(publishedAt)
		item.HasArticle = hasArticle == 1
		item.ArticleURL = articleURL
		out = append(out, item)
	}
	return out, rows.Err()
}

func (s *Store) StoryHasSummarizableSource(ctx context.Context, storyID int64) (bool, error) {
	if storyID <= 0 {
		return false, nil
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT 1
		FROM story_sources ss
		JOIN sources so ON so.id = ss.source_id
		WHERE ss.story_id = ? AND so.summarize = 1 AND so.origin != 'removed'
		LIMIT 1
	`, storyID)
	if err != nil {
		return false, err
	}
	defer rows.Close()

	return rows.Next(), rows.Err()
}

func (s *Store) GetStoryMaintenanceTargets(ctx context.Context, storyID int64) (StoryMaintenanceTargets, error) {
	var targets StoryMaintenanceTargets
	targets.StoryID = storyID
	if err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(article_id, 0)
		FROM stories
		WHERE id = ?
	`, storyID).Scan(&targets.ArticleID); err != nil {
		return StoryMaintenanceTargets{}, err
	}

	rows, err := s.db.QueryContext(ctx, `
		SELECT DISTINCT di.id
		FROM story_sources ss
		JOIN discovered_items di ON di.id = ss.discovered_item_id
		WHERE ss.story_id = ?
		  AND di.article_url != ''
		ORDER BY di.id
	`, storyID)
	if err != nil {
		return StoryMaintenanceTargets{}, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return StoryMaintenanceTargets{}, err
		}
		targets.DiscoveredItemIDs = append(targets.DiscoveredItemIDs, id)
	}
	if err := rows.Err(); err != nil {
		return StoryMaintenanceTargets{}, err
	}
	return targets, nil
}

func (s *Store) ListAllExtractTargets(ctx context.Context, limit int) ([]int64, error) {
	if limit <= 0 {
		limit = 100000
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT id
		FROM discovered_items
		WHERE article_url != ''
		ORDER BY id DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

func (s *Store) LastDiscoveryTime(ctx context.Context, sourceID int64) (time.Time, error) {
	var finishedAt string
	err := s.db.QueryRowContext(ctx, `
		SELECT COALESCE(MAX(finished_at), '')
		FROM discovery_runs
		WHERE source_id = ? AND status = 'ready'
	`, sourceID).Scan(&finishedAt)
	if err != nil {
		return time.Time{}, err
	}
	return parseTime(finishedAt), nil
}

func (s *Store) EnqueueJob(ctx context.Context, in JobInput) error {
	if in.RunAfter.IsZero() {
		in.RunAfter = time.Now().UTC()
	}
	var exists int
	err := s.db.QueryRowContext(ctx, `
		SELECT COUNT(*)
		FROM jobs
		WHERE kind = ?
		  AND source_id = ?
		  AND discovered_item_id = ?
		  AND article_id = ?
		  AND story_id = ?
		  AND status IN ('pending', 'running')
	`, in.Kind, in.SourceID, in.DiscoveredItemID, in.ArticleID, in.StoryID).Scan(&exists)
	if err != nil {
		return err
	}
	if exists > 0 {
		return nil
	}

	now := nowString()
	_, err = s.db.ExecContext(ctx, `
		INSERT INTO jobs(kind, status, source_id, discovered_item_id, article_id, story_id, run_after, attempts, last_error, lease_owner, lease_until, payload_json, created_at, updated_at)
		VALUES (?, 'pending', ?, ?, ?, ?, ?, 0, '', '', '', '{}', ?, ?)
	`, in.Kind, in.SourceID, in.DiscoveredItemID, in.ArticleID, in.StoryID, timeString(in.RunAfter), now, now)
	return err
}

func (s *Store) ClaimJobs(ctx context.Context, owner string, limit int, leaseDuration time.Duration) ([]Job, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()

	now := time.Now().UTC()
	rows, err := tx.QueryContext(ctx, `
		SELECT id, kind, status, source_id, discovered_item_id, article_id, story_id, run_after, attempts, last_error, lease_owner, lease_until, created_at, updated_at
		FROM jobs
		WHERE status = 'pending' AND run_after <= ?
		ORDER BY run_after ASC, id ASC
		LIMIT ?
	`, timeString(now), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []Job
	for rows.Next() {
		job, err := scanJob(rows)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, job)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	leaseUntil := timeString(now.Add(leaseDuration))
	for _, job := range jobs {
		if _, err := tx.ExecContext(ctx, `
			UPDATE jobs
			SET status = 'running', attempts = attempts + 1, lease_owner = ?, lease_until = ?, updated_at = ?
			WHERE id = ?
		`, owner, leaseUntil, timeString(now), job.ID); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}

	for i := range jobs {
		jobs[i].Status = "running"
		jobs[i].Attempts++
		jobs[i].LeaseOwner = owner
		jobs[i].LeaseUntil = parseTime(leaseUntil)
	}
	return jobs, nil
}

func (s *Store) CompleteJob(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE jobs
		SET status = 'completed', lease_owner = '', lease_until = '', updated_at = ?
		WHERE id = ?
	`, nowString(), id)
	return err
}

func (s *Store) RetryJob(ctx context.Context, id int64, errText string, delay time.Duration) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE jobs
		SET status = 'pending', last_error = ?, run_after = ?, lease_owner = '', lease_until = '', updated_at = ?
		WHERE id = ?
	`, truncate(errText, 600), timeString(time.Now().UTC().Add(delay)), nowString(), id)
	return err
}

func (s *Store) FailJob(ctx context.Context, id int64, errText string) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE jobs
		SET status = 'failed', last_error = ?, lease_owner = '', lease_until = '', updated_at = ?
		WHERE id = ?
	`, truncate(errText, 600), nowString(), id)
	return err
}

func (s *Store) PendingJobsCount(ctx context.Context) (int, error) {
	var count int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM jobs WHERE status IN ('pending', 'running')`).Scan(&count)
	return count, err
}

func (s *Store) UpdateStorySummaryStatus(ctx context.Context, articleID int64, status string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE stories SET summary_status = ?, updated_at = ? WHERE article_id = ?`, status, nowString(), articleID)
	return err
}

func (s *Store) ListSummaryBacklog(ctx context.Context, limit int) ([]SummaryBacklogItem, error) {
	if limit <= 0 {
		limit = 1000
	}
	rows, err := s.db.QueryContext(ctx, `
		SELECT s.article_id, s.id
		FROM stories s
		LEFT JOIN summaries sm ON sm.id = (
			SELECT s2.id
			FROM summaries s2
			WHERE s2.article_id = s.article_id AND s2.status = 'ready'
			ORDER BY s2.updated_at DESC
			LIMIT 1
		)
		WHERE s.article_id != 0
		  AND sm.id IS NULL
		  AND EXISTS (
				SELECT 1
				FROM story_sources ss
				JOIN sources so ON so.id = ss.source_id
				WHERE ss.story_id = s.id
				  AND so.summarize = 1
				  AND so.origin != 'removed'
		  )
		ORDER BY
			CASE WHEN s.published_at = '' THEN s.updated_at ELSE s.published_at END DESC,
			s.id DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var items []SummaryBacklogItem
	for rows.Next() {
		var item SummaryBacklogItem
		if err := rows.Scan(&item.ArticleID, &item.StoryID); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (s *Store) ClearSummaries(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := nowString()
	if _, err := tx.ExecContext(ctx, `DELETE FROM summaries`); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE stories SET summary_status = 'pending', updated_at = ?`, now); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) ClearSummariesForArticle(ctx context.Context, articleID int64) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	now := nowString()
	if _, err := tx.ExecContext(ctx, `DELETE FROM summaries WHERE article_id = ?`, articleID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `UPDATE stories SET summary_status = 'pending', updated_at = ? WHERE article_id = ?`, now, articleID); err != nil {
		return err
	}
	return tx.Commit()
}

func (s *Store) ClearJobs(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM jobs`)
	return err
}

func (s *Store) ClearAllContent(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	statements := []string{
		`DELETE FROM jobs`,
		`DELETE FROM story_state`,
		`DELETE FROM story_sources`,
		`DELETE FROM summaries`,
		`DELETE FROM stories`,
		`DELETE FROM articles`,
		`DELETE FROM threads`,
		`DELETE FROM discovered_items`,
		`DELETE FROM discovery_runs`,
		`DELETE FROM sqlite_sequence WHERE name IN ('jobs','story_state','story_sources','summaries','stories','articles','threads','discovered_items','discovery_runs')`,
	}
	for _, stmt := range statements {
		if _, err := tx.ExecContext(ctx, stmt); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Store) BuildContentHash(parts ...string) string {
	return hashParts(parts...)
}

func scanDiscoveredItem(row interface{ Scan(dest ...any) error }) (DiscoveredItem, error) {
	var item DiscoveredItem
	var publishedAt, createdAt, updatedAt string
	err := row.Scan(&item.ID, &item.SourceID, &item.ExternalID, &item.Title, &item.ArticleURL, &item.ThreadURL, &item.Excerpt, &publishedAt, &item.RawPayload, &item.Status, &createdAt, &updatedAt)
	if err != nil {
		return DiscoveredItem{}, err
	}
	item.PublishedAt = parseTime(publishedAt)
	item.CreatedAt = parseTime(createdAt)
	item.UpdatedAt = parseTime(updatedAt)
	return item, nil
}

func scanSource(row interface{ Scan(dest ...any) error }) (Source, error) {
	var src Source
	var enabled, discussion int
	var summarize int
	var createdAt, updatedAt string
	err := row.Scan(&src.ID, &src.Key, &src.Name, &src.Kind, &src.URL, &enabled, &src.RefreshMinutes, &discussion, &summarize, &src.Origin, &createdAt, &updatedAt)
	if err != nil {
		return Source{}, err
	}
	src.Enabled = enabled == 1
	src.Discussion = discussion == 1
	src.Summarize = summarize == 1
	src.CreatedAt = parseTime(createdAt)
	src.UpdatedAt = parseTime(updatedAt)
	return src, nil
}

func scanArticle(row interface{ Scan(dest ...any) error }) (Article, error) {
	var article Article
	var publishedAt, lastExtractedAt, createdAt, updatedAt string
	err := row.Scan(&article.ID, &article.SourceURL, &article.CanonicalURL, &article.Title, &article.Author, &publishedAt, &article.Excerpt, &article.RawText, &article.CleanText, &article.ContentHash, &article.ExtractionStatus, &lastExtractedAt, &createdAt, &updatedAt)
	if err != nil {
		return Article{}, err
	}
	article.PublishedAt = parseTime(publishedAt)
	article.LastExtractedAt = parseTime(lastExtractedAt)
	article.CreatedAt = parseTime(createdAt)
	article.UpdatedAt = parseTime(updatedAt)
	return article, nil
}

func scanStory(row interface{ Scan(dest ...any) error }) (Story, error) {
	var story Story
	var publishedAt, createdAt, updatedAt string
	var isPartial int
	err := row.Scan(&story.ID, &story.ArticleID, &story.PrimaryTitle, &story.PrimaryURL, &publishedAt, &story.Status, &story.SummaryStatus, &isPartial, &createdAt, &updatedAt)
	if err != nil {
		return Story{}, err
	}
	story.PublishedAt = parseTime(publishedAt)
	story.IsPartial = isPartial == 1
	story.CreatedAt = parseTime(createdAt)
	story.UpdatedAt = parseTime(updatedAt)
	return story, nil
}

func scanSummary(row interface{ Scan(dest ...any) error }) (Summary, error) {
	var summary Summary
	var bulletsJSON, tagsJSON, createdAt, updatedAt string
	err := row.Scan(&summary.ID, &summary.ArticleID, &summary.ContentHash, &summary.ModelID, &summary.PromptVersion, &summary.Abstract, &bulletsJSON, &tagsJSON, &summary.Status, &summary.Error, &createdAt, &updatedAt)
	if err != nil {
		return Summary{}, err
	}
	if err := json.Unmarshal([]byte(bulletsJSON), &summary.Bullets); err != nil {
		summary.Bullets = nil
	}
	if err := json.Unmarshal([]byte(tagsJSON), &summary.Tags); err != nil {
		summary.Tags = nil
	}
	summary.CreatedAt = parseTime(createdAt)
	summary.UpdatedAt = parseTime(updatedAt)
	return summary, nil
}

func scanStoryCard(row interface{ Scan(dest ...any) error }) (StoryCard, error) {
	var card StoryCard
	var isPartial, isRead, isSaved, isViewed, isHidden int
	var publishedAt, createdAt, updatedAt string
	var summaryID, summaryArticleID int64
	var summaryCreatedAt, summaryUpdatedAt, bulletsJSON, tagsJSON string
	err := row.Scan(
		&card.ID, &card.ArticleID, &card.PrimaryTitle, &card.PrimaryURL, &publishedAt, &card.Status, &card.SummaryStatus, &isPartial, &createdAt, &updatedAt,
		&card.ArticleCanonicalURL, &card.ArticleExcerpt, &card.ArticleRawText, &card.ArticleCleanText, &card.ArticleStatus,
		&isRead, &isSaved, &isViewed, &isHidden,
		&summaryID, &summaryArticleID, &card.Summary.ContentHash, &card.Summary.ModelID, &card.Summary.PromptVersion,
		&card.Summary.Abstract, &bulletsJSON, &tagsJSON, &card.Summary.Status, &card.Summary.Error,
		&summaryCreatedAt, &summaryUpdatedAt,
	)
	if err != nil {
		return StoryCard{}, err
	}
	card.PublishedAt = parseTime(publishedAt)
	card.CreatedAt = parseTime(createdAt)
	card.UpdatedAt = parseTime(updatedAt)
	card.IsPartial = isPartial == 1
	card.IsRead = isRead == 1
	card.IsSaved = isSaved == 1
	card.IsViewed = isViewed == 1
	card.IsHidden = isHidden == 1
	card.Summary.ID = summaryID
	card.Summary.ArticleID = summaryArticleID
	card.Summary.CreatedAt = parseTime(summaryCreatedAt)
	card.Summary.UpdatedAt = parseTime(summaryUpdatedAt)
	if err := json.Unmarshal([]byte(bulletsJSON), &card.Summary.Bullets); err != nil {
		card.Summary.Bullets = nil
	}
	if err := json.Unmarshal([]byte(tagsJSON), &card.Summary.Tags); err != nil {
		card.Summary.Tags = nil
	}
	return card, nil
}

func scanStoryDetail(row interface{ Scan(dest ...any) error }) (StoryCard, string, error) {
	var card StoryCard
	var author string
	var isPartial, isRead, isSaved, isViewed, isHidden int
	var publishedAt, createdAt, updatedAt string
	var summaryID, summaryArticleID int64
	var summaryCreatedAt, summaryUpdatedAt, bulletsJSON, tagsJSON string
	err := row.Scan(
		&card.ID, &card.ArticleID, &card.PrimaryTitle, &card.PrimaryURL, &publishedAt, &card.Status, &card.SummaryStatus, &isPartial, &createdAt, &updatedAt,
		&card.ArticleCanonicalURL, &card.ArticleExcerpt, &card.ArticleRawText, &card.ArticleCleanText, &card.ArticleStatus,
		&isRead, &isSaved, &isViewed, &isHidden,
		&summaryID, &summaryArticleID, &card.Summary.ContentHash, &card.Summary.ModelID, &card.Summary.PromptVersion,
		&card.Summary.Abstract, &bulletsJSON, &tagsJSON, &card.Summary.Status, &card.Summary.Error,
		&summaryCreatedAt, &summaryUpdatedAt,
		&author,
	)
	if err != nil {
		return StoryCard{}, "", err
	}
	card.PublishedAt = parseTime(publishedAt)
	card.CreatedAt = parseTime(createdAt)
	card.UpdatedAt = parseTime(updatedAt)
	card.IsPartial = isPartial == 1
	card.IsRead = isRead == 1
	card.IsSaved = isSaved == 1
	card.IsViewed = isViewed == 1
	card.IsHidden = isHidden == 1
	card.Summary.ID = summaryID
	card.Summary.ArticleID = summaryArticleID
	card.Summary.CreatedAt = parseTime(summaryCreatedAt)
	card.Summary.UpdatedAt = parseTime(summaryUpdatedAt)
	json.Unmarshal([]byte(bulletsJSON), &card.Summary.Bullets)
	json.Unmarshal([]byte(tagsJSON), &card.Summary.Tags)
	return card, author, nil
}

func (s *Store) listStoryIDs(ctx context.Context, query string, args ...any) ([]int64, error) {
	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func scanJob(row interface{ Scan(dest ...any) error }) (Job, error) {
	var job Job
	var runAfter, leaseUntil, createdAt, updatedAt string
	err := row.Scan(&job.ID, &job.Kind, &job.Status, &job.SourceID, &job.DiscoveredItemID, &job.ArticleID, &job.StoryID, &runAfter, &job.Attempts, &job.LastError, &job.LeaseOwner, &leaseUntil, &createdAt, &updatedAt)
	if err != nil {
		return Job{}, err
	}
	job.RunAfter = parseTime(runAfter)
	job.LeaseUntil = parseTime(leaseUntil)
	job.CreatedAt = parseTime(createdAt)
	job.UpdatedAt = parseTime(updatedAt)
	return job, nil
}

func queryInt64ColumnTx(ctx context.Context, tx *sql.Tx, query string, args ...any) ([]int64, error) {
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func deleteIDsTx(ctx context.Context, tx *sql.Tx, queryFmt string, ids []int64) error {
	if len(ids) == 0 {
		return nil
	}
	args := make([]any, 0, len(ids))
	markers := make([]string, 0, len(ids))
	for _, id := range ids {
		args = append(args, id)
		markers = append(markers, "?")
	}
	_, err := tx.ExecContext(ctx, fmt.Sprintf(queryFmt, strings.Join(markers, ",")), args...)
	return err
}

func nowString() string {
	return time.Now().UTC().Format(time.RFC3339)
}

func timeString(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}

func parseTime(value string) time.Time {
	if strings.TrimSpace(value) == "" {
		return time.Time{}
	}
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		return time.Time{}
	}
	return parsed.UTC()
}

func timeWindowClause(sortBy string) string {
	switch normalizeFeedSort(sortBy) {
	case "published":
		return `COALESCE(NULLIF(s.published_at, ''), s.created_at) >= ?`
	default:
		return `s.created_at >= ?`
	}
}

func timeWindowCutoff(window string, _ string) (string, bool) {
	var duration time.Duration
	switch strings.TrimSpace(window) {
	case "1h":
		duration = time.Hour
	case "6h":
		duration = 6 * time.Hour
	case "1d":
		duration = 24 * time.Hour
	case "1w":
		duration = 7 * 24 * time.Hour
	default:
		return "", false
	}
	return time.Now().UTC().Add(-duration).Format(time.RFC3339), true
}

func boolInt(value bool) int {
	if value {
		return 1
	}
	return 0
}

func defaultString(value, fallback string) string {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	return value
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func hashParts(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		if strings.TrimSpace(part) == "" {
			continue
		}
		h.Write([]byte(part))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}

func normalizeTag(value string) string {
	value = strings.TrimSpace(strings.ReplaceAll(value, "+", " "))
	if value == "" {
		return ""
	}
	return strings.ToLower(strings.Join(strings.Fields(value), " "))
}

func likePattern(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return "%" + replacer.Replace(value) + "%"
}

func likeJSONArrayValue(value string) string {
	value = strings.Trim(strings.ToLower(strings.TrimSpace(value)), `"`)
	replacer := strings.NewReplacer(`\`, `\\`, `%`, `\%`, `_`, `\_`)
	return `%\"` + replacer.Replace(value) + `\"%`
}

func searchTerms(value string) []string {
	parts := strings.FieldsFunc(strings.ToLower(strings.TrimSpace(value)), func(r rune) bool {
		return !unicode.IsLetter(r) && !unicode.IsNumber(r)
	})
	seen := make(map[string]struct{}, len(parts))
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if len(part) < 2 {
			continue
		}
		if _, ok := seen[part]; ok {
			continue
		}
		seen[part] = struct{}{}
		out = append(out, part)
	}
	return out
}
