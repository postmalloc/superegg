package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server     ServerConfig     `yaml:"server"`
	OpenRouter OpenRouterConfig `yaml:"openrouter"`
	Scheduler  SchedulerConfig  `yaml:"scheduler"`
	Sources    []SourceConfig   `yaml:"sources"`
}

type ServerConfig struct {
	ListenAddr        string `yaml:"listen_addr"`
	DatabasePath      string `yaml:"database_path"`
	SessionSecret     string `yaml:"session_secret"`
	AdminPasswordHash string `yaml:"admin_password_hash"`
	PageSize          int    `yaml:"page_size"`
}

type OpenRouterConfig struct {
	BaseURL string `yaml:"base_url"`
	ModelID string `yaml:"model_id"`
	APIKey  string `yaml:"-"`
}

type SchedulerConfig struct {
	PollSeconds int `yaml:"poll_seconds"`
	Workers     int `yaml:"workers"`
}

type SourceConfig struct {
	Key            string `yaml:"key"`
	Name           string `yaml:"name"`
	Kind           string `yaml:"kind"`
	URL            string `yaml:"url"`
	Enabled        bool   `yaml:"enabled"`
	RefreshMinutes int    `yaml:"refresh_minutes"`
	Discussion     bool   `yaml:"discussion"`
	Summarize      *bool  `yaml:"summarize"`
}

func (c SourceConfig) SummarizeEnabled() bool {
	if c.Summarize == nil {
		return true
	}
	return *c.Summarize
}

func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	applyDefaults(&cfg)
	applyEnvOverrides(&cfg)
	if err := validate(&cfg); err != nil {
		return nil, err
	}

	return &cfg, nil
}

func applyDefaults(cfg *Config) {
	if cfg.Server.ListenAddr == "" {
		cfg.Server.ListenAddr = "127.0.0.1:8080"
	}
	if cfg.Server.DatabasePath == "" {
		cfg.Server.DatabasePath = "./superegg.db"
	}
	if cfg.Server.PageSize <= 0 {
		cfg.Server.PageSize = 30
	}
	if cfg.OpenRouter.BaseURL == "" {
		cfg.OpenRouter.BaseURL = "https://openrouter.ai/api/v1"
	}
	if cfg.Scheduler.PollSeconds <= 0 {
		cfg.Scheduler.PollSeconds = 30
	}
	if cfg.Scheduler.Workers <= 0 {
		cfg.Scheduler.Workers = 2
	}
}

func applyEnvOverrides(cfg *Config) {
	applyStringOverride(&cfg.Server.DatabasePath, "SUPEREGG_DATABASE_PATH")
	applyStringOverride(&cfg.Server.SessionSecret, "SUPEREGG_SESSION_SECRET")
	applyStringOverride(&cfg.Server.AdminPasswordHash, "SUPEREGG_ADMIN_PASSWORD_HASH")
	applyStringOverride(&cfg.OpenRouter.ModelID, "SUPEREGG_OPENROUTER_MODEL")
	applyStringOverride(&cfg.OpenRouter.APIKey, "OPENROUTER_API_KEY")
}

func applyStringOverride(target *string, key string) {
	if value := strings.TrimSpace(os.Getenv(key)); value != "" {
		*target = value
	}
}

func validate(cfg *Config) error {
	if cfg.Server.SessionSecret == "" {
		return errors.New("config error: server.session_secret is required")
	}
	if cfg.Server.AdminPasswordHash == "" {
		return errors.New("config error: server.admin_password_hash is required")
	}
	if len(cfg.Sources) == 0 {
		return errors.New("config error: at least one source is required")
	}

	seen := map[string]struct{}{}
	for _, source := range cfg.Sources {
		if source.Key == "" {
			return errors.New("config error: each source needs a key")
		}
		if source.Name == "" {
			return fmt.Errorf("config error: source %q needs a name", source.Key)
		}
		if source.URL == "" {
			return fmt.Errorf("config error: source %q needs a url", source.Key)
		}
		switch source.Kind {
		case "rss", "list", "article":
		default:
			return fmt.Errorf("config error: source %q has unsupported kind %q", source.Key, source.Kind)
		}
		if _, ok := seen[source.Key]; ok {
			return fmt.Errorf("config error: duplicate source key %q", source.Key)
		}
		seen[source.Key] = struct{}{}
	}

	return nil
}
