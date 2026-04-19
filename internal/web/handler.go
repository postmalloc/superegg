package web

import (
	"context"
	"embed"
	"html/template"
	"io/fs"
	"net/http"
	neturl "net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"superegg/internal/auth"
	"superegg/internal/store"
)

//go:embed templates/*.html static/*
var assets embed.FS

type Dependencies struct {
	Store                *store.Store
	Auth                 *auth.Manager
	PageSize             int
	RefreshAll           func(context.Context) error
	RefreshSource        func(context.Context, int64) error
	SetRead              func(context.Context, int64, bool) error
	SetSaved             func(context.Context, int64, bool) error
	SetViewed            func(context.Context, int64, bool) error
	SetHidden            func(context.Context, int64, bool) error
	RerunStoryExtraction func(context.Context, int64) error
	RerunStorySummary    func(context.Context, int64) error
	UpdateSource         func(context.Context, int64, string, int) error
	DeleteSource         func(context.Context, int64) error
	ReindexAll           func(context.Context) error
	ClearAndRebuild      func(context.Context) error
	AddRSSSource         func(context.Context, string, string, int) error
}

type handler struct {
	store                *store.Store
	auth                 *auth.Manager
	pageSize             int
	refreshAll           func(context.Context) error
	refreshSource        func(context.Context, int64) error
	setRead              func(context.Context, int64, bool) error
	setSaved             func(context.Context, int64, bool) error
	setViewed            func(context.Context, int64, bool) error
	setHidden            func(context.Context, int64, bool) error
	rerunStoryExtraction func(context.Context, int64) error
	rerunStorySummary    func(context.Context, int64) error
	updateSource         func(context.Context, int64, string, int) error
	deleteSource         func(context.Context, int64) error
	reindexAll           func(context.Context) error
	clearAndRebuild      func(context.Context) error
	addRSSSource         func(context.Context, string, string, int) error
	templates            *template.Template
}

type homeData struct {
	Title      string
	CurrentTab string
	Stories    []store.StoryCard
	HasMore    bool
	Page       int
	State      string
	SourceKey  string
	SourceName string
	Tag        string
	TimeWindow string
	StoryQuery string
	Sources    []store.SourceStatus
	NextPage   string
	PrevPage   string
	Now        time.Time
}

type storyData struct {
	Title      string
	CurrentTab string
	Story      store.StoryDetail
	View       string
	BackURL    string
	BackLabel  string
	CleanURL   string
	RawURL     string
	PrevURL    string
	NextURL    string
	CleanParts []string
	RawParts   []string
}

type searchData struct {
	Title      string
	CurrentTab string
	Query      string
	Stories    []store.StoryCard
	HasMore    bool
	Page       int
	State      string
	SourceKey  string
	Tag        string
	StoryQuery string
	Sources    []store.SourceStatus
	Tags       []store.TagCount
	NextPage   string
	PrevPage   string
}

type settingsData struct {
	Title            string
	CurrentTab       string
	FeedSort         string
	Sources          []store.SourceStatus
	Jobs             []store.JobStatusCount
	Problems         []store.ProblemStory
	ProblemSort      string
	PublishedSortURL string
	Notice           string
	Error            string
}

type aboutData struct {
	Title      string
	CurrentTab string
}

type loginData struct {
	Title string
	Error string
}

func NewHandler(deps Dependencies) (http.Handler, error) {
	tmpl, err := template.New("base").Funcs(template.FuncMap{
		"relativeTime":     relativeTime,
		"formatTime":       formatTime,
		"textParagraphs":   textParagraphs,
		"fallbackText":     fallbackText,
		"isActive":         func(current, want string) bool { return current == want },
		"not":              func(value bool) bool { return !value },
		"queryEscape":      neturl.QueryEscape,
		"storyLink":        storyLink,
		"sourceFilterLink": sourceFilterLink,
	}).ParseFS(assets, "templates/*.html")
	if err != nil {
		return nil, err
	}
	staticFS, err := fs.Sub(assets, "static")
	if err != nil {
		return nil, err
	}

	h := &handler{
		store:                deps.Store,
		auth:                 deps.Auth,
		pageSize:             deps.PageSize,
		refreshAll:           deps.RefreshAll,
		refreshSource:        deps.RefreshSource,
		setRead:              deps.SetRead,
		setSaved:             deps.SetSaved,
		setViewed:            deps.SetViewed,
		setHidden:            deps.SetHidden,
		rerunStoryExtraction: deps.RerunStoryExtraction,
		rerunStorySummary:    deps.RerunStorySummary,
		updateSource:         deps.UpdateSource,
		deleteSource:         deps.DeleteSource,
		reindexAll:           deps.ReindexAll,
		clearAndRebuild:      deps.ClearAndRebuild,
		addRSSSource:         deps.AddRSSSource,
		templates:            tmpl,
	}

	mux := http.NewServeMux()
	mux.Handle("/static/", http.StripPrefix("/static/", http.FileServer(http.FS(staticFS))))
	mux.HandleFunc("/healthz", h.healthz)
	mux.HandleFunc("/login", h.login)
	mux.Handle("/logout", h.auth.Require(http.HandlerFunc(h.logout)))
	mux.Handle("/", h.auth.Require(http.HandlerFunc(h.home)))
	mux.Handle("/search", h.auth.Require(http.HandlerFunc(h.search)))
	mux.Handle("/about", h.auth.Require(http.HandlerFunc(h.about)))
	mux.Handle("/settings", h.auth.Require(http.HandlerFunc(h.settings)))
	mux.Handle("/settings/", h.auth.Require(http.HandlerFunc(h.settingsActions)))
	mux.Handle("/refresh", h.auth.Require(http.HandlerFunc(h.refreshAllStories)))
	mux.Handle("/stories/", h.auth.Require(http.HandlerFunc(h.stories)))
	mux.Handle("/sources/", h.auth.Require(http.HandlerFunc(h.sources)))

	return mux, nil
}

func (h *handler) healthz(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("ok"))
}

func (h *handler) login(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		h.render(w, "login", loginData{Title: "Login"})
	case http.MethodPost:
		if err := r.ParseForm(); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		password := r.FormValue("password")
		if err := h.auth.CheckPassword(password); err != nil {
			h.render(w, "login", loginData{Title: "Login", Error: "invalid password"})
			return
		}
		if err := h.auth.StartSession(w); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		http.Redirect(w, r, "/", http.StatusSeeOther)
	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func (h *handler) logout(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	h.auth.EndSession(w)
	http.Redirect(w, r, "/login", http.StatusSeeOther)
}

func (h *handler) home(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page <= 0 {
		page = 1
	}
	state := r.URL.Query().Get("state")
	sourceKey := r.URL.Query().Get("source")
	tag := normalizeTagQuery(r.URL.Query().Get("tag"))
	timeWindow := normalizeTimeWindow(r.URL.Query().Get("window"))
	feedSort, err := h.store.GetFeedSort(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	stories, hasMore, err := h.store.ListFeed(r.Context(), store.FeedFilter{
		State:      state,
		SourceKey:  sourceKey,
		Tag:        tag,
		TimeWindow: timeWindow,
		SortBy:     feedSort,
		Page:       page,
		PageSize:   h.pageSize,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	sources, err := h.store.ListSourceStatus(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data := homeData{
		Title:      "Superegg",
		CurrentTab: "feed",
		Stories:    stories,
		HasMore:    hasMore,
		Page:       page,
		State:      defaultState(state),
		SourceKey:  sourceKey,
		Tag:        tag,
		TimeWindow: timeWindow,
		StoryQuery: storyContextQuery("feed", "", defaultState(state), sourceKey, tag, timeWindow),
		Sources:    sources,
		Now:        time.Now().UTC(),
	}
	for _, source := range sources {
		if source.Key == sourceKey {
			data.SourceName = source.Name
			break
		}
	}
	data.NextPage = h.pageURL("/", page+1, data.State, sourceKey, tag, timeWindow)
	if page > 1 {
		data.PrevPage = h.pageURL("/", page-1, data.State, sourceKey, tag, timeWindow)
	}

	h.render(w, "home", data)
}

func (h *handler) search(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/search" {
		http.NotFound(w, r)
		return
	}
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page <= 0 {
		page = 1
	}
	query := strings.TrimSpace(r.URL.Query().Get("q"))
	state := r.URL.Query().Get("state")
	sourceKey := r.URL.Query().Get("source")
	tag := normalizeTagQuery(r.URL.Query().Get("tag"))

	stories, hasMore, err := h.store.SearchStories(r.Context(), store.SearchFilter{
		Query:     query,
		State:     state,
		SourceKey: sourceKey,
		Tag:       tag,
		Page:      page,
		PageSize:  h.pageSize,
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	sources, err := h.store.ListSourceStatus(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	tags, err := h.store.ListTags(r.Context(), 40)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data := searchData{
		Title:      "Search",
		CurrentTab: "search",
		Query:      query,
		Stories:    stories,
		HasMore:    hasMore,
		Page:       page,
		State:      state,
		SourceKey:  sourceKey,
		Tag:        tag,
		StoryQuery: storyContextQuery("search", query, state, sourceKey, tag, ""),
		Sources:    sources,
		Tags:       tags,
	}
	data.NextPage = h.searchPageURL(page+1, query, state, sourceKey, tag)
	if page > 1 {
		data.PrevPage = h.searchPageURL(page-1, query, state, sourceKey, tag)
	}

	h.render(w, "search", data)
}

func (h *handler) settings(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/settings" || r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	sources, err := h.store.ListSourceStatus(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	jobs, err := h.store.ListJobStatusCounts(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	problems, err := h.store.ListProblemStories(r.Context(), 100)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	problemSort := normalizeProblemSort(r.URL.Query().Get("problem_sort"))
	sortProblemStories(problems, problemSort)
	feedSort, err := h.store.GetFeedSort(r.Context())
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	h.render(w, "settings", settingsData{
		Title:            "Settings",
		CurrentTab:       "settings",
		FeedSort:         feedSort,
		Sources:          sources,
		Jobs:             jobs,
		Problems:         problems,
		ProblemSort:      problemSort,
		PublishedSortURL: settingsProblemSortURL(problemSort),
		Notice:           strings.TrimSpace(r.URL.Query().Get("notice")),
		Error:            strings.TrimSpace(r.URL.Query().Get("error")),
	})
}

func (h *handler) about(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/about" || r.Method != http.MethodGet {
		http.NotFound(w, r)
		return
	}
	h.render(w, "about", aboutData{
		Title:      "About",
		CurrentTab: "about",
	})
}

func (h *handler) settingsActions(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	action := strings.TrimPrefix(r.URL.Path, "/settings/")
	switch action {
	case "feed-sort":
		if err := r.ParseForm(); err != nil {
			redirectSettings(w, r, "", "invalid feed sort form")
			return
		}
		if err := h.store.SetFeedSort(r.Context(), r.FormValue("sort")); err != nil {
			redirectSettings(w, r, "", err.Error())
			return
		}
		redirectSettings(w, r, "Feed sort updated.", "")
		return
	case "add-rss":
		if err := r.ParseForm(); err != nil {
			redirectSettings(w, r, "", "invalid form")
			return
		}
		refreshMinutes := 30
		if raw := strings.TrimSpace(r.FormValue("refresh_minutes")); raw != "" {
			value, err := strconv.Atoi(raw)
			if err != nil {
				redirectSettings(w, r, "", "refresh interval must be a number")
				return
			}
			refreshMinutes = value
		}
		if err := h.addRSSSource(r.Context(), r.FormValue("name"), r.FormValue("url"), refreshMinutes); err != nil {
			redirectSettings(w, r, "", err.Error())
			return
		}
		redirectSettings(w, r, "RSS source added and queued for refresh.", "")
		return
	case "reindex":
		if err := h.reindexAll(r.Context()); err != nil {
			redirectSettings(w, r, "", err.Error())
			return
		}
		redirectSettings(w, r, "Reindex queued.", "")
		return
	case "clear":
		if err := h.clearAndRebuild(r.Context()); err != nil {
			redirectSettings(w, r, "", err.Error())
			return
		}
		redirectSettings(w, r, "Database cleared and refresh queued.", "")
		return
	default:
		http.NotFound(w, r)
		return
	}
}

func (h *handler) refreshAllStories(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := h.refreshAll(r.Context()); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	redirectBack(w, r, "/")
}

func (h *handler) stories(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/stories/")
	if path == "" {
		http.NotFound(w, r)
		return
	}
	parts := strings.Split(strings.Trim(path, "/"), "/")
	id, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		http.NotFound(w, r)
		return
	}

	if len(parts) == 1 && r.Method == http.MethodGet {
		h.storyDetail(w, r, id)
		return
	}
	if len(parts) == 2 && r.Method == http.MethodPost {
		switch parts[1] {
		case "read":
			h.toggleRead(w, r, id)
			return
		case "save":
			h.toggleSaved(w, r, id)
			return
		case "hide":
			h.toggleHidden(w, r, id)
			return
		case "rerun-extract":
			h.rerunExtract(w, r, id)
			return
		case "rerun-summary":
			h.rerunSummary(w, r, id)
			return
		}
	}

	http.NotFound(w, r)
}

func (h *handler) storyDetail(w http.ResponseWriter, r *http.Request, storyID int64) {
	if h.setViewed != nil {
		_ = h.setViewed(r.Context(), storyID, true)
	}
	story, err := h.store.GetStoryDetail(r.Context(), storyID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	view := r.URL.Query().Get("view")
	if view != "raw" {
		view = "clean"
	}
	context := storyContextFromRequest(r)
	backURL, prevURL, nextURL, err := h.storyNavigation(r.Context(), storyID, context, view)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	data := storyData{
		Title:      story.PrimaryTitle,
		CurrentTab: context.From,
		Story:      story,
		View:       view,
		BackURL:    backURL,
		BackLabel:  storyBackLabel(context.From),
		CleanURL:   buildStoryURL(storyID, "clean", context),
		RawURL:     buildStoryURL(storyID, "raw", context),
		PrevURL:    prevURL,
		NextURL:    nextURL,
		CleanParts: textParagraphs(story.ArticleCleanText),
		RawParts:   textParagraphs(story.ArticleRawText),
	}
	h.render(w, "story", data)
}

func (h *handler) toggleRead(w http.ResponseWriter, r *http.Request, storyID int64) {
	read := r.FormValue("read") == "1"
	if err := h.setRead(r.Context(), storyID, read); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	redirectBack(w, r, "/")
}

func (h *handler) toggleSaved(w http.ResponseWriter, r *http.Request, storyID int64) {
	saved := r.FormValue("saved") == "1"
	if err := h.setSaved(r.Context(), storyID, saved); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	redirectBack(w, r, "/")
}

func (h *handler) toggleHidden(w http.ResponseWriter, r *http.Request, storyID int64) {
	hidden := r.FormValue("hidden") == "1"
	if err := h.setHidden(r.Context(), storyID, hidden); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	redirectBack(w, r, "/")
}

func (h *handler) rerunExtract(w http.ResponseWriter, r *http.Request, storyID int64) {
	if err := h.rerunStoryExtraction(r.Context(), storyID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	redirectBack(w, r, "/settings")
}

func (h *handler) rerunSummary(w http.ResponseWriter, r *http.Request, storyID int64) {
	if err := h.rerunStorySummary(r.Context(), storyID); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	redirectBack(w, r, "/settings")
}

func (h *handler) sources(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/sources/")
	parts := strings.Split(strings.Trim(path, "/"), "/")
	if len(parts) != 2 || r.Method != http.MethodPost {
		http.NotFound(w, r)
		return
	}
	id, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		http.NotFound(w, r)
		return
	}
	switch parts[1] {
	case "refresh":
		if err := h.refreshSource(r.Context(), id); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		redirectBack(w, r, "/")
	case "update":
		if err := r.ParseForm(); err != nil {
			redirectSettings(w, r, "", "invalid source form")
			return
		}
		refreshMinutes, err := strconv.Atoi(strings.TrimSpace(r.FormValue("refresh_minutes")))
		if err != nil {
			redirectSettings(w, r, "", "refresh interval must be a number")
			return
		}
		if err := h.updateSource(r.Context(), id, r.FormValue("name"), refreshMinutes); err != nil {
			redirectSettings(w, r, "", err.Error())
			return
		}
		redirectSettings(w, r, "Source updated.", "")
	case "delete":
		if err := h.deleteSource(r.Context(), id); err != nil {
			redirectSettings(w, r, "", err.Error())
			return
		}
		redirectSettings(w, r, "Source deleted.", "")
	default:
		http.NotFound(w, r)
		return
	}
}

func (h *handler) render(w http.ResponseWriter, name string, data any) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	if err := h.templates.ExecuteTemplate(w, name, data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

func (h *handler) pageURL(path string, page int, state, source, tag, window string) string {
	values := neturl.Values{}
	if state != "" && state != "unread" {
		values.Set("state", state)
	}
	if source != "" {
		values.Set("source", source)
	}
	if tag != "" {
		values.Set("tag", tag)
	}
	if window != "" {
		values.Set("window", window)
	}
	if page > 1 {
		values.Set("page", strconv.Itoa(page))
	}
	if len(values) == 0 {
		return path
	}
	return path + "?" + values.Encode()
}

func (h *handler) searchPageURL(page int, query, state, source, tag string) string {
	values := neturl.Values{}
	if query != "" {
		values.Set("q", query)
	}
	if state != "" && state != "all" {
		values.Set("state", state)
	}
	if source != "" {
		values.Set("source", source)
	}
	if tag != "" {
		values.Set("tag", tag)
	}
	if page > 1 {
		values.Set("page", strconv.Itoa(page))
	}
	if len(values) == 0 {
		return "/search"
	}
	return "/search?" + values.Encode()
}

func redirectBack(w http.ResponseWriter, r *http.Request, fallback string) {
	target := r.Referer()
	if target == "" {
		target = fallback
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}

func redirectSettings(w http.ResponseWriter, r *http.Request, notice, errText string) {
	values := neturl.Values{}
	if strings.TrimSpace(notice) != "" {
		values.Set("notice", strings.TrimSpace(notice))
	}
	if strings.TrimSpace(errText) != "" {
		values.Set("error", strings.TrimSpace(errText))
	}
	target := "/settings"
	if encoded := values.Encode(); encoded != "" {
		target += "?" + encoded
	}
	http.Redirect(w, r, target, http.StatusSeeOther)
}

type storyContext struct {
	From       string
	Query      string
	State      string
	SourceKey  string
	Tag        string
	TimeWindow string
}

func storyContextFromRequest(r *http.Request) storyContext {
	from := strings.TrimSpace(r.URL.Query().Get("from"))
	switch from {
	case "search":
	default:
		from = "feed"
	}

	state := strings.TrimSpace(r.URL.Query().Get("state"))
	if from == "feed" {
		if strings.TrimSpace(r.URL.Query().Get("from")) == "" && state == "" {
			state = "all"
		} else {
			state = defaultState(state)
		}
	}

	return storyContext{
		From:       from,
		Query:      strings.TrimSpace(r.URL.Query().Get("q")),
		State:      state,
		SourceKey:  strings.TrimSpace(r.URL.Query().Get("source")),
		Tag:        normalizeTagQuery(r.URL.Query().Get("tag")),
		TimeWindow: normalizeTimeWindow(r.URL.Query().Get("window")),
	}
}

func storyContextQuery(from, query, state, source, tag, window string) string {
	values := neturl.Values{}
	if from == "search" {
		values.Set("from", "search")
	} else {
		values.Set("from", "feed")
	}
	if strings.TrimSpace(query) != "" {
		values.Set("q", strings.TrimSpace(query))
	}
	if strings.TrimSpace(state) != "" {
		values.Set("state", strings.TrimSpace(state))
	}
	if strings.TrimSpace(source) != "" {
		values.Set("source", strings.TrimSpace(source))
	}
	if strings.TrimSpace(tag) != "" {
		values.Set("tag", strings.TrimSpace(tag))
	}
	if strings.TrimSpace(window) != "" {
		values.Set("window", strings.TrimSpace(window))
	}
	if len(values) == 0 {
		return ""
	}
	return "?" + values.Encode()
}

func buildStoryURL(storyID int64, view string, context storyContext) string {
	values := neturl.Values{}
	if context.From == "search" {
		values.Set("from", "search")
	} else {
		values.Set("from", "feed")
	}
	if strings.TrimSpace(context.Query) != "" {
		values.Set("q", strings.TrimSpace(context.Query))
	}
	if strings.TrimSpace(context.State) != "" {
		values.Set("state", strings.TrimSpace(context.State))
	}
	if strings.TrimSpace(context.SourceKey) != "" {
		values.Set("source", strings.TrimSpace(context.SourceKey))
	}
	if strings.TrimSpace(context.Tag) != "" {
		values.Set("tag", strings.TrimSpace(context.Tag))
	}
	if strings.TrimSpace(context.TimeWindow) != "" {
		values.Set("window", strings.TrimSpace(context.TimeWindow))
	}
	if view == "raw" {
		values.Set("view", "raw")
	}
	base := "/stories/" + strconv.FormatInt(storyID, 10)
	if encoded := values.Encode(); encoded != "" {
		return base + "?" + encoded
	}
	return base
}

func storyBackLabel(from string) string {
	if from == "search" {
		return "Back to search"
	}
	return "Back to feed"
}

func normalizeTagQuery(value string) string {
	value = strings.TrimSpace(strings.ReplaceAll(value, "+", " "))
	if value == "" {
		return ""
	}
	return strings.Join(strings.Fields(value), " ")
}

func normalizeTimeWindow(value string) string {
	switch strings.TrimSpace(value) {
	case "1h", "6h", "1d", "1w":
		return strings.TrimSpace(value)
	default:
		return ""
	}
}

func normalizeProblemSort(value string) string {
	if strings.EqualFold(strings.TrimSpace(value), "oldest") {
		return "oldest"
	}
	return "newest"
}

func settingsProblemSortURL(current string) string {
	if normalizeProblemSort(current) == "oldest" {
		return "/settings"
	}
	return "/settings?problem_sort=oldest"
}

func sortProblemStories(problems []store.ProblemStory, order string) {
	oldestFirst := normalizeProblemSort(order) == "oldest"
	sort.SliceStable(problems, func(i, j int) bool {
		left := problems[i].PublishedAt
		right := problems[j].PublishedAt
		if left.Equal(right) {
			if oldestFirst {
				return problems[i].StoryID < problems[j].StoryID
			}
			return problems[i].StoryID > problems[j].StoryID
		}
		if left.IsZero() {
			return false
		}
		if right.IsZero() {
			return true
		}
		if oldestFirst {
			return left.Before(right)
		}
		return left.After(right)
	})
}

func storyLink(id int64, query string) string {
	base := "/stories/" + strconv.FormatInt(id, 10)
	if strings.TrimSpace(query) == "" {
		return base
	}
	return base + query
}

func sourceFilterLink(query, sourceKey string) string {
	context := parseStoryContextQuery(query)
	context.SourceKey = strings.TrimSpace(sourceKey)
	if context.From == "search" {
		return searchListURL(context.Query, context.State, context.SourceKey, context.Tag)
	}
	return feedListURL(context.State, context.SourceKey, context.Tag, context.TimeWindow)
}

func parseStoryContextQuery(query string) storyContext {
	context := storyContext{
		From:  "feed",
		State: "all",
	}
	raw := strings.TrimSpace(strings.TrimPrefix(query, "?"))
	if raw == "" {
		return context
	}
	values, err := neturl.ParseQuery(raw)
	if err != nil {
		return context
	}
	context.From = strings.TrimSpace(values.Get("from"))
	if context.From != "search" {
		context.From = "feed"
	}
	context.Query = strings.TrimSpace(values.Get("q"))
	context.SourceKey = strings.TrimSpace(values.Get("source"))
	context.Tag = normalizeTagQuery(values.Get("tag"))
	context.TimeWindow = normalizeTimeWindow(values.Get("window"))
	if context.From == "search" {
		context.State = strings.TrimSpace(values.Get("state"))
	} else {
		context.State = defaultState(strings.TrimSpace(values.Get("state")))
		if strings.TrimSpace(values.Get("state")) == "" {
			context.State = "all"
		}
	}
	return context
}

func feedListURL(state, source, tag, window string) string {
	values := neturl.Values{}
	if strings.TrimSpace(state) != "" && strings.TrimSpace(state) != "unread" {
		values.Set("state", strings.TrimSpace(state))
	}
	if strings.TrimSpace(source) != "" {
		values.Set("source", strings.TrimSpace(source))
	}
	if strings.TrimSpace(tag) != "" {
		values.Set("tag", strings.TrimSpace(tag))
	}
	if strings.TrimSpace(window) != "" {
		values.Set("window", strings.TrimSpace(window))
	}
	if len(values) == 0 {
		return "/"
	}
	return "/?" + values.Encode()
}

func searchListURL(query, state, source, tag string) string {
	values := neturl.Values{}
	if strings.TrimSpace(query) != "" {
		values.Set("q", strings.TrimSpace(query))
	}
	if strings.TrimSpace(state) != "" {
		values.Set("state", strings.TrimSpace(state))
	}
	if strings.TrimSpace(source) != "" {
		values.Set("source", strings.TrimSpace(source))
	}
	if strings.TrimSpace(tag) != "" {
		values.Set("tag", strings.TrimSpace(tag))
	}
	if len(values) == 0 {
		return "/search"
	}
	return "/search?" + values.Encode()
}

func (h *handler) storyNavigation(ctx context.Context, storyID int64, context storyContext, view string) (string, string, string, error) {
	var (
		ids []int64
		err error
	)
	feedSort, err := h.store.GetFeedSort(ctx)
	if err != nil {
		return "", "", "", err
	}
	if context.From == "search" {
		ids, err = h.store.SearchStoryIDs(ctx, store.SearchFilter{
			Query:     context.Query,
			State:     context.State,
			SourceKey: context.SourceKey,
			Tag:       context.Tag,
			SortBy:    feedSort,
		})
	} else {
		ids, err = h.store.ListFeedStoryIDs(ctx, store.FeedFilter{
			State:      context.State,
			SourceKey:  context.SourceKey,
			Tag:        context.Tag,
			TimeWindow: context.TimeWindow,
			SortBy:     feedSort,
		})
	}
	if err != nil {
		return "", "", "", err
	}

	index := -1
	for i, id := range ids {
		if id == storyID {
			index = i
			break
		}
	}

	page := 1
	if index >= 0 {
		page = index/h.pageSize + 1
	}

	backURL := h.storyListURL(context, page)
	prevURL := ""
	nextURL := ""
	if index > 0 {
		prevURL = buildStoryURL(ids[index-1], view, context)
	}
	if index >= 0 && index < len(ids)-1 {
		nextURL = buildStoryURL(ids[index+1], view, context)
	}
	return backURL, prevURL, nextURL, nil
}

func (h *handler) storyListURL(context storyContext, page int) string {
	if context.From == "search" {
		return h.searchPageURL(page, context.Query, context.State, context.SourceKey, context.Tag)
	}
	return h.pageURL("/", page, context.State, context.SourceKey, context.Tag, context.TimeWindow)
}

func defaultState(value string) string {
	switch value {
	case "saved", "read", "all":
		return value
	default:
		return "unread"
	}
}

func relativeTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	diff := time.Since(value)
	switch {
	case diff < time.Minute:
		return "just now"
	case diff < time.Hour:
		return strconv.Itoa(int(diff.Minutes())) + "m ago"
	case diff < 24*time.Hour:
		return strconv.Itoa(int(diff.Hours())) + "h ago"
	case diff < 7*24*time.Hour:
		return strconv.Itoa(int(diff.Hours()/24)) + "d ago"
	default:
		return value.Format("2006-01-02")
	}
}

func formatTime(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.Local().Format("2006-01-02 15:04")
}

func textParagraphs(value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}
	parts := strings.Split(value, "\n\n")
	var out []string
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part != "" {
			out = append(out, part)
		}
	}
	return out
}

func fallbackText(card store.StoryCard) string {
	if strings.TrimSpace(card.Summary.Abstract) != "" {
		return card.Summary.Abstract
	}
	if strings.TrimSpace(card.ArticleExcerpt) != "" {
		return card.ArticleExcerpt
	}
	if strings.TrimSpace(card.ArticleCleanText) != "" {
		return card.ArticleCleanText
	}
	return card.PrimaryTitle
}
