package discovery

import (
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"strings"
	"time"

	"github.com/mmcdole/gofeed"
	"golang.org/x/net/html"

	"superegg/internal/store"
)

type Service struct {
	client *http.Client
	parser *gofeed.Parser
}

type Item struct {
	ExternalID  string
	Title       string
	ArticleURL  string
	ThreadURL   string
	Excerpt     string
	PublishedAt time.Time
	RawPayload  string
	Submitter   string
}

type FeedProbe struct {
	Title string
	URL   string
}

func New() *Service {
	return &Service{
		client: &http.Client{Timeout: 20 * time.Second},
		parser: gofeed.NewParser(),
	}
}

func (s *Service) ProbeRSS(ctx context.Context, rawURL string) (FeedProbe, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimSpace(rawURL), nil)
	if err != nil {
		return FeedProbe{}, err
	}
	req.Header.Set("User-Agent", "Superegg/0.1 (+https://localhost)")

	resp, err := s.client.Do(req)
	if err != nil {
		return FeedProbe{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return FeedProbe{}, fmt.Errorf("feed request failed: %s", resp.Status)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return FeedProbe{}, err
	}

	feed, err := s.parser.Parse(strings.NewReader(string(body)))
	if err != nil {
		return FeedProbe{}, err
	}

	finalURL := normalizeURL(rawURL)
	if resp.Request != nil && resp.Request.URL != nil {
		finalURL = normalizeURL(resp.Request.URL.String())
	}

	return FeedProbe{
		Title: strings.TrimSpace(feed.Title),
		URL:   finalURL,
	}, nil
}

func (s *Service) Discover(ctx context.Context, source store.Source) ([]Item, error) {
	switch source.Kind {
	case "rss":
		return s.discoverRSS(ctx, source)
	case "article":
		return s.discoverArticleSource(ctx, source)
	case "list":
		return s.discoverListSource(ctx, source)
	default:
		return nil, fmt.Errorf("unsupported source kind %q", source.Kind)
	}
}

func (s *Service) discoverRSS(ctx context.Context, source store.Source) ([]Item, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, source.URL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Superegg/0.1 (+https://localhost)")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("feed request failed: %s", resp.Status)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}

	feed, err := s.parser.Parse(strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	commentsByIndex := parseRSSComments(body)

	var items []Item
	for index, entry := range feed.Items {
		threadURL := ""
		if index < len(commentsByIndex) {
			threadURL = commentsByIndex[index]
		}
		item := Item{
			ExternalID:  firstNonEmpty(entry.GUID, entry.Link, entry.Title),
			Title:       strings.TrimSpace(entry.Title),
			ArticleURL:  normalizeURL(entry.Link),
			ThreadURL:   normalizeURL(threadURL),
			Excerpt:     stripSpace(entry.Description),
			PublishedAt: firstTime(entry.PublishedParsed, entry.UpdatedParsed),
			Submitter:   authorName(entry),
		}

		if source.Discussion && sameDiscussionLink(item.ArticleURL, item.ThreadURL, source.URL) {
			item.ArticleURL = ""
		}

		rawPayload, _ := json.Marshal(map[string]any{
			"title":         entry.Title,
			"link":          entry.Link,
			"guid":          entry.GUID,
			"comments":      threadURL,
			"description":   entry.Description,
			"published":     entry.Published,
			"updated":       entry.Updated,
			"author":        item.Submitter,
			"categories":    entry.Categories,
			"content":       entry.Content,
			"feed_title":    feed.Title,
			"feed_homepage": feed.Link,
		})
		item.RawPayload = string(rawPayload)
		items = append(items, item)
	}

	return items, nil
}

func (s *Service) discoverArticleSource(_ context.Context, source store.Source) ([]Item, error) {
	payload, _ := json.Marshal(map[string]any{
		"kind": "article",
		"url":  source.URL,
	})
	return []Item{{
		ExternalID: source.URL,
		Title:      source.Name,
		ArticleURL: normalizeURL(source.URL),
		RawPayload: string(payload),
	}}, nil
}

func (s *Service) discoverListSource(ctx context.Context, source store.Source) ([]Item, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, source.URL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Superegg/0.1 (+https://localhost)")

	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("list request failed: %s", resp.Status)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, err
	}

	doc, err := html.Parse(strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}

	baseURL, _ := neturl.Parse(source.URL)
	seen := map[string]struct{}{}
	var items []Item
	var walk func(*html.Node)
	walk = func(node *html.Node) {
		if node.Type == html.ElementNode && node.Data == "a" {
			href := attr(node, "href")
			title := stripSpace(textContent(node))
			resolved := resolveURL(baseURL, href)
			if resolved != "" && title != "" && sameHost(baseURL.String(), resolved) {
				normalized := normalizeURL(resolved)
				if _, ok := seen[normalized]; !ok {
					seen[normalized] = struct{}{}
					payload, _ := json.Marshal(map[string]any{
						"kind":      "list",
						"source":    source.URL,
						"href":      href,
						"resolved":  normalized,
						"link_text": title,
					})
					items = append(items, Item{
						ExternalID: normalized,
						Title:      title,
						ArticleURL: normalized,
						RawPayload: string(payload),
					})
				}
			}
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(doc)

	return items, nil
}

func normalizeURL(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	parsed, err := neturl.Parse(strings.TrimSpace(raw))
	if err != nil {
		return strings.TrimSpace(raw)
	}
	parsed.Fragment = ""
	if parsed.Scheme == "" {
		parsed.Scheme = "https"
	}
	return parsed.String()
}

func resolveURL(base *neturl.URL, raw string) string {
	if base == nil || strings.TrimSpace(raw) == "" {
		return ""
	}
	parsed, err := neturl.Parse(strings.TrimSpace(raw))
	if err != nil {
		return ""
	}
	return normalizeURL(base.ResolveReference(parsed).String())
}

func sameHost(a, b string) bool {
	ua, err := neturl.Parse(a)
	if err != nil {
		return false
	}
	ub, err := neturl.Parse(b)
	if err != nil {
		return false
	}
	return strings.TrimPrefix(ua.Hostname(), "www.") == strings.TrimPrefix(ub.Hostname(), "www.")
}

func sameDiscussionLink(articleURL, threadURL, sourceURL string) bool {
	if articleURL == "" {
		return true
	}
	if threadURL != "" && articleURL == threadURL {
		return true
	}
	return sameHost(sourceURL, articleURL)
}

func firstTime(values ...*time.Time) time.Time {
	for _, value := range values {
		if value != nil && !value.IsZero() {
			return value.UTC()
		}
	}
	return time.Time{}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func stripSpace(value string) string {
	return strings.Join(strings.Fields(strings.TrimSpace(value)), " ")
}

func authorName(item *gofeed.Item) string {
	if item.Author != nil {
		if name := strings.TrimSpace(item.Author.Name); name != "" {
			return name
		}
	}
	for _, author := range item.Authors {
		if author != nil {
			if name := strings.TrimSpace(author.Name); name != "" {
				return name
			}
		}
	}
	return ""
}

func parseRSSComments(body []byte) []string {
	var parsed struct {
		Items []struct {
			Comments string `xml:"comments"`
		} `xml:"channel>item"`
	}
	if err := xml.Unmarshal(body, &parsed); err != nil {
		return nil
	}
	out := make([]string, 0, len(parsed.Items))
	for _, item := range parsed.Items {
		out = append(out, strings.TrimSpace(item.Comments))
	}
	return out
}

func attr(node *html.Node, key string) string {
	for _, attribute := range node.Attr {
		if strings.EqualFold(attribute.Key, key) {
			return attribute.Val
		}
	}
	return ""
}

func textContent(node *html.Node) string {
	if node == nil {
		return ""
	}
	if node.Type == html.TextNode {
		return node.Data
	}
	var builder strings.Builder
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		builder.WriteString(textContent(child))
		builder.WriteByte(' ')
	}
	return builder.String()
}
