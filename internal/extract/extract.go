package extract

import (
	"context"
	"fmt"
	"io"
	"net/http"
	neturl "net/url"
	"strings"
	"time"
	"unicode"

	readability "github.com/go-shiori/go-readability"
	"golang.org/x/net/html"
)

type Service struct {
	client *http.Client
}

type Result struct {
	SourceURL    string
	CanonicalURL string
	Title        string
	Author       string
	PublishedAt  time.Time
	Excerpt      string
	RawText      string
	CleanText    string
}

func New() *Service {
	return &Service{
		client: &http.Client{Timeout: 25 * time.Second},
	}
}

func (s *Service) Extract(ctx context.Context, rawURL string) (Result, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return Result{}, err
	}
	req.Header.Set("User-Agent", "Superegg/0.1 (+https://localhost)")

	resp, err := s.client.Do(req)
	if err != nil {
		return Result{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return Result{}, fmt.Errorf("extract request failed: %s", resp.Status)
	}

	body, err := io.ReadAll(io.LimitReader(resp.Body, 6<<20))
	if err != nil {
		return Result{}, err
	}

	finalURL := rawURL
	if resp.Request != nil && resp.Request.URL != nil {
		finalURL = resp.Request.URL.String()
	}

	return extractFromHTML(string(body), finalURL)
}

func extractFromHTML(sourceHTML, finalURL string) (Result, error) {
	doc, err := html.Parse(strings.NewReader(sourceHTML))
	if err != nil {
		return Result{}, err
	}

	bodyNode := findElement(doc, "body")
	rawText := normalizeWhitespace(textTree(pruneClone(bodyNode, map[string]struct{}{
		"script":   {},
		"style":    {},
		"noscript": {},
		"svg":      {},
	})))

	mainNode := chooseMainNode(doc)
	cleanText := normalizeWhitespace(textTree(pruneClone(mainNode, map[string]struct{}{
		"script":   {},
		"style":    {},
		"noscript": {},
		"nav":      {},
		"header":   {},
		"footer":   {},
		"aside":    {},
		"form":     {},
		"svg":      {},
	})))
	if len(cleanText) < 250 {
		cleanText = rawText
	}

	readableArticle, readabilityErr := readability.FromDocument(doc, mustParseURL(finalURL))
	if readabilityErr == nil {
		readabilityText := normalizeWhitespace(readableArticle.TextContent)
		if len(readabilityText) >= 200 {
			cleanText = readabilityText
		}
	}

	title := firstNonEmpty(
		trimWhitespace(readableArticle.Title),
		metaValue(doc, "property", "og:title"),
		metaValue(doc, "name", "twitter:title"),
		titleText(doc),
	)
	canonical := firstNonEmpty(metaCanonical(doc, finalURL), metaValue(doc, "property", "og:url"), finalURL)
	author := firstNonEmpty(
		trimWhitespace(readableArticle.Byline),
		metaValue(doc, "name", "author"),
		metaValue(doc, "property", "article:author"),
	)
	publishedAt := parseAnyTime(firstNonEmpty(
		readabilityPublished(readableArticle),
		metaValue(doc, "property", "article:published_time"),
		metaValue(doc, "name", "article:published_time"),
		metaValue(doc, "name", "pubdate"),
		attr(findElement(doc, "time"), "datetime"),
	))
	excerpt := firstNonEmpty(trimWhitespace(readableArticle.Excerpt), excerptFromText(cleanText))

	return Result{
		SourceURL:    NormalizeURL(finalURL),
		CanonicalURL: NormalizeURL(resolveURL(finalURL, canonical)),
		Title:        title,
		Author:       author,
		PublishedAt:  publishedAt,
		Excerpt:      excerpt,
		RawText:      rawText,
		CleanText:    cleanText,
	}, nil
}

func mustParseURL(raw string) *neturl.URL {
	parsed, err := neturl.Parse(raw)
	if err != nil {
		return &neturl.URL{}
	}
	return parsed
}

func readabilityPublished(article readability.Article) string {
	if article.PublishedTime == nil || article.PublishedTime.IsZero() {
		return ""
	}
	return article.PublishedTime.UTC().Format(time.RFC3339)
}

func trimWhitespace(value string) string {
	return strings.TrimSpace(value)
}

func NormalizeURL(raw string) string {
	if strings.TrimSpace(raw) == "" {
		return ""
	}
	parsed, err := neturl.Parse(strings.TrimSpace(raw))
	if err != nil {
		return strings.TrimSpace(raw)
	}
	parsed.Fragment = ""
	return parsed.String()
}

func resolveURL(base, raw string) string {
	baseURL, err := neturl.Parse(base)
	if err != nil {
		return raw
	}
	ref, err := neturl.Parse(raw)
	if err != nil {
		return raw
	}
	return baseURL.ResolveReference(ref).String()
}

func chooseMainNode(doc *html.Node) *html.Node {
	for _, tag := range []string{"article", "main"} {
		if node := findElement(doc, tag); node != nil {
			return node
		}
	}

	best := findLargestTextContainer(doc)
	if best != nil {
		return best
	}

	if body := findElement(doc, "body"); body != nil {
		return body
	}
	return doc
}

func findLargestTextContainer(root *html.Node) *html.Node {
	var best *html.Node
	bestScore := 0
	var walk func(*html.Node)
	walk = func(node *html.Node) {
		if node == nil {
			return
		}
		if node.Type == html.ElementNode {
			switch node.Data {
			case "div", "section", "article", "main":
				score := len(normalizeWhitespace(textTree(node)))
				if score > bestScore {
					bestScore = score
					best = node
				}
			}
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(root)
	return best
}

func findElement(root *html.Node, tag string) *html.Node {
	if root == nil {
		return nil
	}
	var found *html.Node
	var walk func(*html.Node)
	walk = func(node *html.Node) {
		if found != nil || node == nil {
			return
		}
		if node.Type == html.ElementNode && node.Data == tag {
			found = node
			return
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(root)
	return found
}

func titleText(doc *html.Node) string {
	if title := findElement(doc, "title"); title != nil {
		return normalizeWhitespace(textTree(title))
	}
	return ""
}

func metaCanonical(doc *html.Node, fallback string) string {
	var out string
	var walk func(*html.Node)
	walk = func(node *html.Node) {
		if node == nil || out != "" {
			return
		}
		if node.Type == html.ElementNode && node.Data == "link" {
			rel := strings.ToLower(attr(node, "rel"))
			if strings.Contains(rel, "canonical") {
				out = attr(node, "href")
				return
			}
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(doc)
	if out == "" {
		return fallback
	}
	return out
}

func metaValue(doc *html.Node, attrKey, attrValue string) string {
	var out string
	var walk func(*html.Node)
	walk = func(node *html.Node) {
		if node == nil || out != "" {
			return
		}
		if node.Type == html.ElementNode && node.Data == "meta" {
			if strings.EqualFold(attr(node, attrKey), attrValue) {
				out = attr(node, "content")
				return
			}
		}
		for child := node.FirstChild; child != nil; child = child.NextSibling {
			walk(child)
		}
	}
	walk(doc)
	return strings.TrimSpace(out)
}

func pruneClone(root *html.Node, skip map[string]struct{}) *html.Node {
	if root == nil {
		return nil
	}
	if root.Type == html.ElementNode {
		if _, ok := skip[root.Data]; ok {
			return nil
		}
	}

	copyNode := &html.Node{
		Type: root.Type,
		Data: root.Data,
		Attr: append([]html.Attribute(nil), root.Attr...),
	}
	for child := root.FirstChild; child != nil; child = child.NextSibling {
		clonedChild := pruneClone(child, skip)
		if clonedChild != nil {
			copyNode.AppendChild(clonedChild)
		}
	}
	return copyNode
}

func textTree(node *html.Node) string {
	if node == nil {
		return ""
	}
	if node.Type == html.TextNode {
		return node.Data + " "
	}
	var builder strings.Builder
	for child := node.FirstChild; child != nil; child = child.NextSibling {
		builder.WriteString(textTree(child))
		if child.Type == html.ElementNode && (child.Data == "p" || child.Data == "div" || child.Data == "li" || child.Data == "section" || child.Data == "article" || child.Data == "br") {
			builder.WriteString("\n")
		}
	}
	return builder.String()
}

func normalizeWhitespace(value string) string {
	value = strings.ReplaceAll(value, "\r", "\n")
	value = strings.ReplaceAll(value, "\t", " ")
	lines := strings.Split(value, "\n")
	var kept []string
	for _, line := range lines {
		line = strings.TrimSpace(strings.Map(func(r rune) rune {
			if unicode.IsControl(r) && r != '\n' {
				return -1
			}
			return r
		}, line))
		line = strings.Join(strings.Fields(line), " ")
		if line != "" {
			kept = append(kept, line)
		}
	}
	return strings.Join(kept, "\n\n")
}

func excerptFromText(text string) string {
	text = strings.TrimSpace(text)
	if len(text) <= 280 {
		return text
	}
	return strings.TrimSpace(text[:280]) + "..."
}

func parseAnyTime(raw string) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	layouts := []string{
		time.RFC3339,
		time.RFC3339Nano,
		time.RFC1123Z,
		time.RFC1123,
		"2006-01-02 15:04:05",
		"2006-01-02",
	}
	for _, layout := range layouts {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return parsed.UTC()
		}
	}
	return time.Time{}
}

func attr(node *html.Node, key string) string {
	if node == nil {
		return ""
	}
	for _, attribute := range node.Attr {
		if strings.EqualFold(attribute.Key, key) {
			return strings.TrimSpace(attribute.Val)
		}
	}
	return ""
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
