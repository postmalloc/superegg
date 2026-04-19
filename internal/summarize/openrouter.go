package summarize

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

const PromptVersion = "v1"

var ErrNotConfigured = errors.New("openrouter not configured")

type Client struct {
	baseURL string
	modelID string
	apiKey  string
	client  *http.Client
}

type Result struct {
	Abstract string
	Bullets  []string
	Tags     []string
}

type requestBody struct {
	Model    string        `json:"model"`
	Messages []messageBody `json:"messages"`
}

type messageBody struct {
	Role    string `json:"role"`
	Content string `json:"content"`
}

type responseBody struct {
	Choices []struct {
		Message struct {
			Content string `json:"content"`
		} `json:"message"`
	} `json:"choices"`
	Error *struct {
		Message string `json:"message"`
	} `json:"error,omitempty"`
}

type summaryJSON struct {
	Abstract string   `json:"abstract"`
	Bullets  []string `json:"bullets"`
	Tags     []string `json:"tags"`
}

func New(baseURL, modelID, apiKey string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		modelID: strings.TrimSpace(modelID),
		apiKey:  strings.TrimSpace(apiKey),
		client:  &http.Client{Timeout: 45 * time.Second},
	}
}

func (c *Client) Configured() bool {
	return c.modelID != "" && c.apiKey != ""
}

func (c *Client) ModelID() string {
	return c.modelID
}

func (c *Client) Summarize(ctx context.Context, title, sourceURL, content string) (Result, error) {
	if !c.Configured() {
		return Result{}, ErrNotConfigured
	}

	payload := requestBody{
		Model: c.modelID,
		Messages: []messageBody{
			{
				Role: "system",
				Content: "You summarize articles for a dense technical news reader. Respond with strict JSON only. " +
					`Use shape {"abstract":"...","bullets":["...","...","..."],"tags":["..."]}. ` +
					"Abstract must be one sentence. Bullets must be concise factual takeaways. Tags should be short lowercase topics.",
			},
			{
				Role:    "user",
				Content: fmt.Sprintf("Title: %s\nURL: %s\n\nArticle:\n%s", title, sourceURL, truncate(content, 12000)),
			},
		},
	}

	body, err := json.Marshal(payload)
	if err != nil {
		return Result{}, err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/chat/completions", bytes.NewReader(body))
	if err != nil {
		return Result{}, err
	}
	req.Header.Set("Authorization", "Bearer "+c.apiKey)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("HTTP-Referer", "http://localhost")
	req.Header.Set("X-Title", "Superegg")

	resp, err := c.client.Do(req)
	if err != nil {
		return Result{}, err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return Result{}, err
	}

	var parsed responseBody
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return Result{}, fmt.Errorf("decode openrouter response: %w", err)
	}
	if parsed.Error != nil {
		return Result{}, errors.New(parsed.Error.Message)
	}
	if len(parsed.Choices) == 0 {
		return Result{}, errors.New("no completion choices returned")
	}

	contentText := strings.TrimSpace(stripCodeFence(parsed.Choices[0].Message.Content))
	var summary summaryJSON
	if err := json.Unmarshal([]byte(contentText), &summary); err != nil {
		return Result{}, fmt.Errorf("decode summary json: %w", err)
	}
	if len(summary.Bullets) > 3 {
		summary.Bullets = summary.Bullets[:3]
	}
	for len(summary.Bullets) < 3 {
		summary.Bullets = append(summary.Bullets, "")
	}
	summary.Bullets = compact(summary.Bullets)
	summary.Tags = compact(summary.Tags)

	return Result{
		Abstract: strings.TrimSpace(summary.Abstract),
		Bullets:  summary.Bullets,
		Tags:     summary.Tags,
	}, nil
}

func stripCodeFence(value string) string {
	value = strings.TrimSpace(value)
	value = strings.TrimPrefix(value, "```json")
	value = strings.TrimPrefix(value, "```")
	value = strings.TrimSuffix(value, "```")
	return strings.TrimSpace(value)
}

func truncate(value string, limit int) string {
	if len(value) <= limit {
		return value
	}
	return value[:limit]
}

func compact(values []string) []string {
	var out []string
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	return out
}
