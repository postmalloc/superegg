package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net/http"
	neturl "net/url"
	"strconv"
	"strings"
	"time"
	"unicode"

	"superegg/internal/auth"
	"superegg/internal/config"
	"superegg/internal/discovery"
	"superegg/internal/extract"
	"superegg/internal/store"
	"superegg/internal/summarize"
	"superegg/internal/web"
)

type App struct {
	cfg        *config.Config
	store      *store.Store
	auth       *auth.Manager
	discovery  *discovery.Service
	extractor  *extract.Service
	summarizer *summarize.Client
	httpServer *http.Server
	owner      string
}

func New(ctx context.Context, cfg *config.Config) (*App, error) {
	db, err := store.Open(cfg.Server.DatabasePath)
	if err != nil {
		return nil, err
	}
	if err := db.SyncSources(ctx, cfg.Sources); err != nil {
		db.Close()
		return nil, err
	}

	authManager, err := auth.New(cfg.Server.AdminPasswordHash, cfg.Server.SessionSecret)
	if err != nil {
		db.Close()
		return nil, err
	}

	instance := &App{
		cfg:        cfg,
		store:      db,
		auth:       authManager,
		discovery:  discovery.New(),
		extractor:  extract.New(),
		summarizer: summarize.New(cfg.OpenRouter.BaseURL, cfg.OpenRouter.ModelID, cfg.OpenRouter.APIKey),
		owner:      strconv.FormatInt(time.Now().UnixNano(), 36),
	}

	handler, err := web.NewHandler(web.Dependencies{
		Store:    db,
		Auth:     authManager,
		PageSize: cfg.Server.PageSize,
		RefreshAll: func(ctx context.Context) error {
			return instance.EnqueueAllRefreshes(ctx)
		},
		RefreshSource: func(ctx context.Context, sourceID int64) error {
			return instance.RefreshSource(ctx, sourceID)
		},
		SetRead: func(ctx context.Context, storyID int64, read bool) error {
			return db.SetStoryRead(ctx, storyID, read)
		},
		SetSaved: func(ctx context.Context, storyID int64, saved bool) error {
			return db.SetStorySaved(ctx, storyID, saved)
		},
		SetViewed: func(ctx context.Context, storyID int64, viewed bool) error {
			return db.SetStoryViewed(ctx, storyID, viewed)
		},
		SetHidden: func(ctx context.Context, storyID int64, hidden bool) error {
			return db.SetStoryHidden(ctx, storyID, hidden)
		},
		RerunStoryExtraction: func(ctx context.Context, storyID int64) error {
			return instance.RerunStoryExtraction(ctx, storyID)
		},
		RerunStorySummary: func(ctx context.Context, storyID int64) error {
			return instance.RerunStorySummary(ctx, storyID)
		},
		UpdateSource: func(ctx context.Context, sourceID int64, name string, refreshMinutes int) error {
			return instance.UpdateSource(ctx, sourceID, name, refreshMinutes)
		},
		DeleteSource: func(ctx context.Context, sourceID int64) error {
			return instance.DeleteSource(ctx, sourceID)
		},
		ReindexAll: func(ctx context.Context) error {
			return instance.ReindexAll(ctx)
		},
		ClearAndRebuild: func(ctx context.Context) error {
			return instance.ClearAndRebuild(ctx)
		},
		AddRSSSource: func(ctx context.Context, name, rawURL string, refreshMinutes int) error {
			return instance.AddRSSSource(ctx, name, rawURL, refreshMinutes)
		},
	})
	if err != nil {
		db.Close()
		return nil, err
	}

	instance.httpServer = &http.Server{
		Addr:              cfg.Server.ListenAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}
	if err := instance.EnqueueSummaryBacklog(ctx); err != nil {
		db.Close()
		return nil, err
	}
	return instance, nil
}

func (a *App) Run(ctx context.Context) error {
	serverErr := make(chan error, 1)
	go func() {
		err := a.httpServer.ListenAndServe()
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
			return
		}
		serverErr <- nil
	}()

	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	for i := 0; i < a.cfg.Scheduler.Workers; i++ {
		go a.workerLoop(workerCtx)
	}
	go a.schedulerLoop(workerCtx)

	select {
	case <-ctx.Done():
	case err := <-serverErr:
		if err != nil {
			return err
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return a.httpServer.Shutdown(shutdownCtx)
}

func (a *App) Close() error {
	return a.store.Close()
}

func (a *App) EnqueueAllRefreshes(ctx context.Context) error {
	sources, err := a.store.ListSources(ctx)
	if err != nil {
		return err
	}
	for _, source := range sources {
		if !source.Enabled {
			continue
		}
		if err := a.RefreshSource(ctx, source.ID); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) RefreshSource(ctx context.Context, sourceID int64) error {
	return a.store.EnqueueJob(ctx, store.JobInput{
		Kind:     "discover",
		SourceID: sourceID,
		RunAfter: time.Now().UTC(),
	})
}

func (a *App) EnqueueSummaryBacklog(ctx context.Context) error {
	items, err := a.store.ListSummaryBacklog(ctx, 5000)
	if err != nil {
		return err
	}
	for _, item := range items {
		if err := a.store.EnqueueJob(ctx, store.JobInput{
			Kind:      "summarize",
			ArticleID: item.ArticleID,
			StoryID:   item.StoryID,
			RunAfter:  time.Now().UTC(),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) RerunStoryExtraction(ctx context.Context, storyID int64) error {
	targets, err := a.store.GetStoryMaintenanceTargets(ctx, storyID)
	if err != nil {
		return err
	}
	if targets.ArticleID != 0 {
		if err := a.store.ClearSummariesForArticle(ctx, targets.ArticleID); err != nil {
			return err
		}
	}
	for _, discoveredItemID := range targets.DiscoveredItemIDs {
		if err := a.store.EnqueueJob(ctx, store.JobInput{
			Kind:             "extract",
			DiscoveredItemID: discoveredItemID,
			RunAfter:         time.Now().UTC(),
		}); err != nil {
			return err
		}
	}
	return nil
}

func (a *App) RerunStorySummary(ctx context.Context, storyID int64) error {
	targets, err := a.store.GetStoryMaintenanceTargets(ctx, storyID)
	if err != nil {
		return err
	}
	if targets.ArticleID == 0 {
		return fmt.Errorf("story %d has no article to summarize", storyID)
	}
	if err := a.store.ClearSummariesForArticle(ctx, targets.ArticleID); err != nil {
		return err
	}
	return a.store.EnqueueJob(ctx, store.JobInput{
		Kind:      "summarize",
		ArticleID: targets.ArticleID,
		StoryID:   storyID,
		RunAfter:  time.Now().UTC(),
	})
}

func (a *App) ReindexAll(ctx context.Context) error {
	if err := a.store.ClearJobs(ctx); err != nil {
		return err
	}
	if err := a.store.ClearSummaries(ctx); err != nil {
		return err
	}
	targets, err := a.store.ListAllExtractTargets(ctx, 100000)
	if err != nil {
		return err
	}
	for _, discoveredItemID := range targets {
		if err := a.store.EnqueueJob(ctx, store.JobInput{
			Kind:             "extract",
			DiscoveredItemID: discoveredItemID,
			RunAfter:         time.Now().UTC(),
		}); err != nil {
			return err
		}
	}
	return a.EnqueueAllRefreshes(ctx)
}

func (a *App) ClearAndRebuild(ctx context.Context) error {
	if err := a.store.ClearAllContent(ctx); err != nil {
		return err
	}
	if err := a.store.SyncSources(ctx, a.cfg.Sources); err != nil {
		return err
	}
	return a.EnqueueAllRefreshes(ctx)
}

func (a *App) AddRSSSource(ctx context.Context, name, rawURL string, refreshMinutes int) error {
	rawURL = strings.TrimSpace(rawURL)
	name = strings.TrimSpace(name)
	if rawURL == "" {
		return errors.New("rss url is required")
	}
	if refreshMinutes < 0 {
		return errors.New("refresh interval must be zero or greater")
	}

	probe, err := a.discovery.ProbeRSS(ctx, rawURL)
	if err != nil {
		return fmt.Errorf("validate rss feed: %w", err)
	}
	if probe.URL == "" {
		probe.URL = rawURL
	}
	if _, err := a.store.GetSourceByURL(ctx, probe.URL); err == nil {
		return errors.New("that rss feed already exists")
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	if name == "" {
		name = firstNonEmpty(strings.TrimSpace(probe.Title), sourceNameFromURL(probe.URL), "RSS Source")
	}
	key, err := a.uniqueSourceKey(ctx, name, probe.URL)
	if err != nil {
		return err
	}

	source, err := a.store.CreateSource(ctx, store.SourceInput{
		Key:            key,
		Name:           name,
		Kind:           "rss",
		URL:            probe.URL,
		Enabled:        true,
		RefreshMinutes: refreshMinutes,
		Discussion:     false,
		Origin:         "user",
	})
	if err != nil {
		return err
	}
	return a.RefreshSource(ctx, source.ID)
}

func (a *App) UpdateSource(ctx context.Context, sourceID int64, name string, refreshMinutes int) error {
	return a.store.UpdateSource(ctx, sourceID, name, refreshMinutes)
}

func (a *App) DeleteSource(ctx context.Context, sourceID int64) error {
	return a.store.DeleteSource(ctx, sourceID)
}

func (a *App) DrainQueue(ctx context.Context) error {
	for {
		count, err := a.store.PendingJobsCount(ctx)
		if err != nil {
			return err
		}
		if count == 0 {
			return nil
		}
		jobs, err := a.store.ClaimJobs(ctx, a.owner, 1, 2*time.Minute)
		if err != nil {
			return err
		}
		if len(jobs) == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
				continue
			}
		}
		if err := a.processJob(ctx, jobs[0]); err != nil {
			return err
		}
	}
}

func (a *App) uniqueSourceKey(ctx context.Context, name, rawURL string) (string, error) {
	base := slugify(firstNonEmpty(name, sourceNameFromURL(rawURL), "rss-source"))
	if base == "" {
		base = "rss-source"
	}
	key := base
	for i := 2; i < 1000; i++ {
		exists, err := a.store.SourceKeyExists(ctx, key)
		if err != nil {
			return "", err
		}
		if !exists {
			return key, nil
		}
		key = fmt.Sprintf("%s-%d", base, i)
	}
	return "", errors.New("could not allocate a unique source key")
}

func sourceNameFromURL(rawURL string) string {
	parsed, err := neturl.Parse(strings.TrimSpace(rawURL))
	if err != nil {
		return ""
	}
	host := strings.TrimPrefix(parsed.Hostname(), "www.")
	host = strings.TrimSpace(host)
	if host == "" {
		return ""
	}
	return host
}

func slugify(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	var builder strings.Builder
	lastDash := false
	for _, r := range value {
		switch {
		case unicode.IsLetter(r) || unicode.IsNumber(r):
			builder.WriteRune(r)
			lastDash = false
		case !lastDash:
			builder.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(builder.String(), "-")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func (a *App) workerLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		jobs, err := a.store.ClaimJobs(ctx, a.owner, 1, 2*time.Minute)
		if err != nil {
			log.Printf("claim jobs: %v", err)
			time.Sleep(2 * time.Second)
			continue
		}
		if len(jobs) == 0 {
			time.Sleep(2 * time.Second)
			continue
		}

		if err := a.processJob(ctx, jobs[0]); err != nil {
			log.Printf("process job %d (%s): %v", jobs[0].ID, jobs[0].Kind, err)
		}
	}
}

func (a *App) schedulerLoop(ctx context.Context) {
	ticker := time.NewTicker(time.Duration(a.cfg.Scheduler.PollSeconds) * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := a.enqueueDueSources(ctx); err != nil {
				log.Printf("scheduler: %v", err)
			}
		}
	}
}

func (a *App) enqueueDueSources(ctx context.Context) error {
	sources, err := a.store.ListSources(ctx)
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	for _, source := range sources {
		if !source.Enabled || source.RefreshMinutes <= 0 {
			continue
		}
		lastRun, err := a.store.LastDiscoveryTime(ctx, source.ID)
		if err != nil {
			return err
		}
		if lastRun.IsZero() || lastRun.Add(time.Duration(source.RefreshMinutes)*time.Minute).Before(now) {
			if err := a.RefreshSource(ctx, source.ID); err != nil {
				return err
			}
		}
	}
	return nil
}

func (a *App) processJob(ctx context.Context, job store.Job) error {
	var err error
	switch job.Kind {
	case "discover":
		err = a.handleDiscover(ctx, job)
	case "extract":
		err = a.handleExtract(ctx, job)
	case "summarize":
		err = a.handleSummarize(ctx, job)
	default:
		err = fmt.Errorf("unsupported job kind %q", job.Kind)
	}

	if err == nil {
		return a.store.CompleteJob(ctx, job.ID)
	}
	if job.Attempts >= 4 {
		return a.store.FailJob(ctx, job.ID, err.Error())
	}
	return a.store.RetryJob(ctx, job.ID, err.Error(), time.Duration(job.Attempts)*30*time.Second)
}

func (a *App) handleDiscover(ctx context.Context, job store.Job) error {
	source, err := a.store.GetSource(ctx, job.SourceID)
	if err != nil {
		return err
	}

	runID, err := a.store.StartDiscoveryRun(ctx, source.ID)
	if err != nil {
		return err
	}

	items, discoverErr := a.discovery.Discover(ctx, source)
	if discoverErr != nil {
		_ = a.store.FinishDiscoveryRun(ctx, runID, "failed", 0, discoverErr.Error())
		return discoverErr
	}

	for _, item := range items {
		discovered, err := a.store.UpsertDiscoveredItem(ctx, store.DiscoveredItemInput{
			SourceID:    source.ID,
			ExternalID:  item.ExternalID,
			Title:       item.Title,
			ArticleURL:  item.ArticleURL,
			ThreadURL:   item.ThreadURL,
			Excerpt:     item.Excerpt,
			PublishedAt: item.PublishedAt,
			RawPayload:  item.RawPayload,
			Status:      "discovered",
		})
		if err != nil {
			return err
		}

		var threadID int64
		if item.ThreadURL != "" {
			thread, err := a.store.UpsertThread(ctx, store.ThreadInput{
				SourceID:    source.ID,
				ExternalID:  item.ExternalID,
				Title:       item.Title,
				ThreadURL:   item.ThreadURL,
				Submitter:   item.Submitter,
				PublishedAt: item.PublishedAt,
			})
			if err != nil {
				return err
			}
			threadID = thread.ID
		}

		if discovered.ArticleURL != "" {
			if err := a.store.EnqueueJob(ctx, store.JobInput{
				Kind:             "extract",
				SourceID:         source.ID,
				DiscoveredItemID: discovered.ID,
				RunAfter:         time.Now().UTC(),
			}); err != nil {
				return err
			}
			continue
		}

		if threadID != 0 {
			if _, err := a.store.UpsertDiscussionStory(ctx, discovered, threadID); err != nil {
				return err
			}
		}
	}

	return a.store.FinishDiscoveryRun(ctx, runID, "ready", len(items), "")
}

func (a *App) handleExtract(ctx context.Context, job store.Job) error {
	item, err := a.store.GetDiscoveredItem(ctx, job.DiscoveredItemID)
	if err != nil {
		return err
	}

	source, err := a.store.GetSource(ctx, item.SourceID)
	if err != nil {
		return err
	}

	var threadID int64
	if item.ThreadURL != "" {
		thread, err := a.store.UpsertThread(ctx, store.ThreadInput{
			SourceID:    source.ID,
			ExternalID:  item.ExternalID,
			Title:       item.Title,
			ThreadURL:   item.ThreadURL,
			PublishedAt: item.PublishedAt,
		})
		if err != nil {
			return err
		}
		threadID = thread.ID
	}

	result, extractErr := a.extractor.Extract(ctx, item.ArticleURL)
	if extractErr != nil {
		fallbackText := strings.TrimSpace(item.Title + "\n\n" + item.Excerpt)
		article, err := a.store.UpsertArticle(ctx, store.ArticleInput{
			SourceURL:        extract.NormalizeURL(item.ArticleURL),
			CanonicalURL:     extract.NormalizeURL(item.ArticleURL),
			Title:            item.Title,
			PublishedAt:      item.PublishedAt,
			Excerpt:          firstNonEmpty(item.Excerpt, fallbackText),
			RawText:          fallbackText,
			CleanText:        fallbackText,
			ContentHash:      a.store.BuildContentHash(item.ArticleURL, item.Title, item.Excerpt),
			ExtractionStatus: "partial",
		})
		if err != nil {
			return err
		}
		story, err := a.store.UpsertStoryForArticle(ctx, article, item, threadID)
		if err != nil {
			return err
		}
		if err := a.store.EnqueueJob(ctx, store.JobInput{
			Kind:      "summarize",
			ArticleID: article.ID,
			StoryID:   story.ID,
			RunAfter:  time.Now().UTC(),
		}); err != nil {
			return err
		}
		return nil
	}

	published := result.PublishedAt
	if published.IsZero() {
		published = item.PublishedAt
	}
	title := firstNonEmpty(result.Title, item.Title)
	excerpt := firstNonEmpty(result.Excerpt, item.Excerpt)
	cleanText := firstNonEmpty(result.CleanText, title+"\n\n"+excerpt)
	contentHash := a.store.BuildContentHash(result.CanonicalURL, title, cleanText)

	article, err := a.store.UpsertArticle(ctx, store.ArticleInput{
		SourceURL:        firstNonEmpty(result.SourceURL, item.ArticleURL),
		CanonicalURL:     firstNonEmpty(result.CanonicalURL, item.ArticleURL),
		Title:            title,
		Author:           result.Author,
		PublishedAt:      published,
		Excerpt:          excerpt,
		RawText:          firstNonEmpty(result.RawText, cleanText),
		CleanText:        cleanText,
		ContentHash:      contentHash,
		ExtractionStatus: "ready",
	})
	if err != nil {
		return err
	}

	story, err := a.store.UpsertStoryForArticle(ctx, article, item, threadID)
	if err != nil {
		return err
	}

	return a.store.EnqueueJob(ctx, store.JobInput{
		Kind:      "summarize",
		ArticleID: article.ID,
		StoryID:   story.ID,
		RunAfter:  time.Now().UTC(),
	})
}

func (a *App) handleSummarize(ctx context.Context, job store.Job) error {
	article, err := a.store.GetArticle(ctx, job.ArticleID)
	if err != nil {
		return err
	}

	if article.ContentHash == "" {
		article.ContentHash = a.store.BuildContentHash(article.CanonicalURL, article.Title, article.CleanText, article.Excerpt)
	}

	if existing, err := a.store.GetSummary(ctx, article.ID, article.ContentHash, a.summarizer.ModelID(), summarize.PromptVersion); err == nil {
		if existing.Status == "ready" {
			return nil
		}
	}

	content := firstNonEmpty(article.CleanText, article.RawText, article.Excerpt, article.Title)
	result, sumErr := a.summarizer.Summarize(ctx, article.Title, firstNonEmpty(article.CanonicalURL, article.SourceURL), content)
	if sumErr != nil {
		status := "failed"
		if errors.Is(sumErr, summarize.ErrNotConfigured) {
			status = "pending_config"
		}
		if _, err := a.store.UpsertSummary(ctx, store.SummaryInput{
			ArticleID:     article.ID,
			ContentHash:   article.ContentHash,
			ModelID:       firstNonEmpty(a.summarizer.ModelID(), "unconfigured"),
			PromptVersion: summarize.PromptVersion,
			Status:        status,
			Error:         sumErr.Error(),
		}); err != nil {
			return err
		}
		return a.store.UpdateStorySummaryStatus(ctx, article.ID, status)
	}

	_, err = a.store.UpsertSummary(ctx, store.SummaryInput{
		ArticleID:     article.ID,
		ContentHash:   article.ContentHash,
		ModelID:       a.summarizer.ModelID(),
		PromptVersion: summarize.PromptVersion,
		Abstract:      result.Abstract,
		Bullets:       result.Bullets,
		Tags:          result.Tags,
		Status:        "ready",
	})
	return err
}
