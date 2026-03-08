package config_test

import (
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/miladhzz/gkit/pkg/config"
)

type testCfg struct {
	Host    string        `env:"TEST_HOST"    default:"localhost"`
	Port    int           `env:"TEST_PORT"    default:"8080"`
	Debug   bool          `env:"TEST_DEBUG"   default:"false"`
	Timeout time.Duration `env:"TEST_TIMEOUT" default:"5s"`
	Tags    []string      `env:"TEST_TAGS"    default:"a,b,c"`
	DBURL   url.URL       `env:"TEST_DB_URL"  default:"postgres://localhost/test"`

	Nested struct {
		Key string `env:"TEST_NESTED_KEY" default:"nested-default"`
	}
}

func TestLoad_Defaults(t *testing.T) {
	// Clear any existing env vars.
	for _, k := range []string{"TEST_HOST", "TEST_PORT", "TEST_DEBUG", "TEST_TIMEOUT", "TEST_TAGS", "TEST_DB_URL", "TEST_NESTED_KEY"} {
		os.Unsetenv(k)
	}

	var cfg testCfg
	if err := config.Load(&cfg); err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Host != "localhost" {
		t.Errorf("Host = %q, want %q", cfg.Host, "localhost")
	}
	if cfg.Port != 8080 {
		t.Errorf("Port = %d, want 8080", cfg.Port)
	}
	if cfg.Debug {
		t.Error("Debug should be false")
	}
	if cfg.Timeout != 5*time.Second {
		t.Errorf("Timeout = %v, want 5s", cfg.Timeout)
	}
	if len(cfg.Tags) != 3 || cfg.Tags[0] != "a" {
		t.Errorf("Tags = %v, want [a b c]", cfg.Tags)
	}
	if cfg.DBURL.Host != "localhost" {
		t.Errorf("DBURL.Host = %q, want localhost", cfg.DBURL.Host)
	}
	if cfg.Nested.Key != "nested-default" {
		t.Errorf("Nested.Key = %q, want nested-default", cfg.Nested.Key)
	}
}

func TestLoad_EnvOverride(t *testing.T) {
	t.Setenv("TEST_HOST", "example.com")
	t.Setenv("TEST_PORT", "9090")
	t.Setenv("TEST_DEBUG", "true")
	t.Setenv("TEST_TIMEOUT", "30s")
	t.Setenv("TEST_TAGS", "x, y, z")

	var cfg testCfg
	if err := config.Load(&cfg); err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Host != "example.com" {
		t.Errorf("Host = %q", cfg.Host)
	}
	if cfg.Port != 9090 {
		t.Errorf("Port = %d", cfg.Port)
	}
	if !cfg.Debug {
		t.Error("Debug should be true")
	}
	if cfg.Timeout != 30*time.Second {
		t.Errorf("Timeout = %v", cfg.Timeout)
	}
	if len(cfg.Tags) != 3 || cfg.Tags[1] != "y" {
		t.Errorf("Tags = %v", cfg.Tags)
	}
}

func TestLoad_Required_Missing(t *testing.T) {
	type req struct {
		Secret string `env:"TEST_SECRET" required:"true"`
	}
	os.Unsetenv("TEST_SECRET")

	var cfg req
	err := config.Load(&cfg)
	if err == nil {
		t.Fatal("expected error for missing required field")
	}
}

func TestLoad_InvalidType(t *testing.T) {
	t.Setenv("TEST_PORT", "not-a-number")

	var cfg testCfg
	err := config.Load(&cfg)
	if err == nil {
		t.Fatal("expected error for invalid int")
	}
	t.Setenv("TEST_PORT", "8080")
}

func TestLoad_EnvFile(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), ".env")
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString("TEST_HOST=from-file\nTEST_PORT=1234\n# comment\n")
	f.Close()

	// Make sure process env doesn't override.
	os.Unsetenv("TEST_HOST")
	os.Unsetenv("TEST_PORT")

	var cfg testCfg
	if err := config.Load(&cfg, config.WithEnvFile(f.Name())); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if cfg.Host != "from-file" {
		t.Errorf("Host = %q, want from-file", cfg.Host)
	}
	if cfg.Port != 1234 {
		t.Errorf("Port = %d, want 1234", cfg.Port)
	}
}
