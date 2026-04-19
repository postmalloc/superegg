package extract

import (
	"strings"
	"testing"
)

func TestExtractFromHTMLUsesReadabilityContent(t *testing.T) {
	html := `
<!doctype html>
<html>
<head>
  <title>Ignored Outer Title</title>
  <meta property="og:url" content="https://example.com/posts/test">
</head>
<body>
  <header>Navigation and clutter</header>
  <article>
    <h1>Readable Title</h1>
    <p>This is the first paragraph of the actual article body and it contains enough text to be considered the main readable content for extraction.</p>
    <p>This is the second paragraph of the article body, adding more details so readability has enough density to keep the article content intact.</p>
    <p>This is the third paragraph, making the extracted text long enough to beat the fallback heuristic and proving the main article survives.</p>
  </article>
  <aside>Sidebar promo</aside>
</body>
</html>`

	result, err := extractFromHTML(html, "https://example.com/posts/test")
	if err != nil {
		t.Fatalf("extractFromHTML returned error: %v", err)
	}
	if result.Title != "Readable Title" {
		t.Fatalf("unexpected title: %q", result.Title)
	}
	if !strings.Contains(result.CleanText, "first paragraph of the actual article body") {
		t.Fatalf("clean text did not include readable article content: %q", result.CleanText)
	}
	if strings.Contains(result.CleanText, "Navigation and clutter") {
		t.Fatalf("clean text still contains obvious page chrome: %q", result.CleanText)
	}
}
