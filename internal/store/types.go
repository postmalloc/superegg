package store

import "time"

type Source struct {
	ID             int64
	Key            string
	Name           string
	Kind           string
	URL            string
	Enabled        bool
	RefreshMinutes int
	Discussion     bool
	Origin         string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type DiscoveryRun struct {
	ID         int64
	SourceID   int64
	Status     string
	ItemCount  int
	Error      string
	StartedAt  time.Time
	FinishedAt time.Time
}

type DiscoveredItem struct {
	ID          int64
	SourceID    int64
	ExternalID  string
	Title       string
	ArticleURL  string
	ThreadURL   string
	Excerpt     string
	PublishedAt time.Time
	RawPayload  string
	Status      string
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

type Thread struct {
	ID           int64
	SourceID     int64
	ExternalID   string
	Title        string
	ThreadURL    string
	Submitter    string
	Score        int
	CommentCount int
	PublishedAt  time.Time
	CreatedAt    time.Time
	UpdatedAt    time.Time
}

type Article struct {
	ID               int64
	SourceURL        string
	CanonicalURL     string
	Title            string
	Author           string
	PublishedAt      time.Time
	Excerpt          string
	RawText          string
	CleanText        string
	ContentHash      string
	ExtractionStatus string
	LastExtractedAt  time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type Story struct {
	ID            int64
	ArticleID     int64
	PrimaryTitle  string
	PrimaryURL    string
	PublishedAt   time.Time
	Status        string
	SummaryStatus string
	IsPartial     bool
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type Summary struct {
	ID            int64
	ArticleID     int64
	ContentHash   string
	ModelID       string
	PromptVersion string
	Abstract      string
	Bullets       []string
	Tags          []string
	Status        string
	Error         string
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

type StoryReference struct {
	StoryID    int64
	SourceKey  string
	SourceName string
	ThreadURL  string
	SourceURL  string
}

type StoryCard struct {
	Story
	ArticleCanonicalURL string
	ArticleExcerpt      string
	ArticleRawText      string
	ArticleCleanText    string
	ArticleStatus       string
	Summary             Summary
	IsRead              bool
	IsSaved             bool
	IsViewed            bool
	IsHidden            bool
	References          []StoryReference
}

type StoryDetail struct {
	StoryCard
	Author      string
	PublishedAt time.Time
}

type SourceStatus struct {
	Source
	LastDiscoveryAt time.Time
	LastStatus      string
	LastError       string
	PendingJobs     int
}

type FeedFilter struct {
	State         string
	SourceKey     string
	Tag           string
	TimeWindow    string
	SortBy        string
	IncludeHidden bool
	Page          int
	PageSize      int
}

type SearchFilter struct {
	Query     string
	State     string
	SourceKey string
	Tag       string
	SortBy    string
	Page      int
	PageSize  int
}

type Job struct {
	ID               int64
	Kind             string
	Status           string
	SourceID         int64
	DiscoveredItemID int64
	ArticleID        int64
	StoryID          int64
	RunAfter         time.Time
	Attempts         int
	LastError        string
	LeaseOwner       string
	LeaseUntil       time.Time
	CreatedAt        time.Time
	UpdatedAt        time.Time
}

type DiscoveredItemInput struct {
	SourceID    int64
	ExternalID  string
	Title       string
	ArticleURL  string
	ThreadURL   string
	Excerpt     string
	PublishedAt time.Time
	RawPayload  string
	Status      string
}

type ThreadInput struct {
	SourceID     int64
	ExternalID   string
	Title        string
	ThreadURL    string
	Submitter    string
	Score        int
	CommentCount int
	PublishedAt  time.Time
}

type ArticleInput struct {
	SourceURL        string
	CanonicalURL     string
	Title            string
	Author           string
	PublishedAt      time.Time
	Excerpt          string
	RawText          string
	CleanText        string
	ContentHash      string
	ExtractionStatus string
}

type SummaryInput struct {
	ArticleID     int64
	ContentHash   string
	ModelID       string
	PromptVersion string
	Abstract      string
	Bullets       []string
	Tags          []string
	Status        string
	Error         string
}

type JobInput struct {
	Kind             string
	SourceID         int64
	DiscoveredItemID int64
	ArticleID        int64
	StoryID          int64
	RunAfter         time.Time
}

type SourceInput struct {
	Key            string
	Name           string
	Kind           string
	URL            string
	Enabled        bool
	RefreshMinutes int
	Discussion     bool
	Origin         string
}

type SummaryBacklogItem struct {
	ArticleID int64
	StoryID   int64
}

type TagCount struct {
	Tag   string
	Count int
}

type JobStatusCount struct {
	Kind   string
	Status string
	Count  int
}

type ProblemStory struct {
	StoryID       int64
	Title         string
	SourceNames   string
	PublishedAt   time.Time
	ArticleStatus string
	SummaryStatus string
	SummaryError  string
	HasArticle    bool
	ArticleURL    string
}

type StoryMaintenanceTargets struct {
	StoryID           int64
	ArticleID         int64
	DiscoveredItemIDs []int64
}
