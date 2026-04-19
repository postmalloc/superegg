package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"

	"golang.org/x/crypto/bcrypt"

	"superegg/internal/config"
)

func TestAddRSSSourcePersistsUserSource(t *testing.T) {
	t.Parallel()

	feed := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/rss+xml; charset=utf-8")
		_, _ = w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Example Feed</title>
    <link>https://example.com/</link>
    <description>test feed</description>
    <item>
      <title>Hello</title>
      <link>https://example.com/hello</link>
      <guid>hello</guid>
    </item>
  </channel>
</rss>`))
	}))
	defer feed.Close()

	passwordHash, err := bcrypt.GenerateFromPassword([]byte("test-password"), bcrypt.DefaultCost)
	if err != nil {
		t.Fatal(err)
	}

	cfg := &config.Config{
		Server: config.ServerConfig{
			ListenAddr:        "127.0.0.1:0",
			DatabasePath:      filepath.Join(t.TempDir(), "superegg-test.db"),
			SessionSecret:     "session-secret",
			AdminPasswordHash: string(passwordHash),
			PageSize:          30,
		},
		Scheduler: config.SchedulerConfig{
			PollSeconds: 60,
			Workers:     1,
		},
	}

	app, err := New(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer app.Close()

	if err := app.AddRSSSource(context.Background(), "", feed.URL+"/rss", 30, true); err != nil {
		t.Fatal(err)
	}

	sources, err := app.store.ListSources(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(sources) != 1 {
		t.Fatalf("expected 1 source, got %d", len(sources))
	}
	if sources[0].Name != "Example Feed" {
		t.Fatalf("expected inferred source name, got %q", sources[0].Name)
	}
	if sources[0].Origin != "user" {
		t.Fatalf("expected user origin, got %q", sources[0].Origin)
	}
	if sources[0].Kind != "rss" {
		t.Fatalf("expected rss kind, got %q", sources[0].Kind)
	}
}
