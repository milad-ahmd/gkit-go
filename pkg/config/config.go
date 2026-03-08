// Package config loads application configuration from environment variables
// (and optionally a .env file) into a typed struct using reflection.
//
// Struct field tags:
//
//	env:"VAR_NAME"          // env var name (required)
//	default:"value"         // fallback if env var is unset
//	required:"true"         // error if env var is unset and no default
//
// Supported field types: string, bool, int, int8, int16, int32, int64,
// uint, uint8, uint16, uint32, uint64, float32, float64,
// time.Duration, []string (comma-separated), url.URL, net.IP.
package config

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Load reads environment variables into dst, which must be a non-nil pointer
// to a struct. It processes tags in field declaration order and returns an
// aggregated error listing all missing/invalid fields.
func Load(dst any, opts ...Option) error {
	cfg := &options{}
	for _, o := range opts {
		o(cfg)
	}

	if cfg.envFile != "" {
		if err := loadEnvFile(cfg.envFile); err != nil && !errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("config: load env file %q: %w", cfg.envFile, err)
		}
	}

	rv := reflect.ValueOf(dst)
	if rv.Kind() != reflect.Ptr || rv.IsNil() || rv.Elem().Kind() != reflect.Struct {
		return errors.New("config: dst must be a non-nil pointer to a struct")
	}
	return parseStruct(rv.Elem())
}

// MustLoad is like Load but panics on error.
func MustLoad(dst any, opts ...Option) {
	if err := Load(dst, opts...); err != nil {
		panic(err)
	}
}

// Option configures Load behaviour.
type Option func(*options)

type options struct {
	envFile string
}

// WithEnvFile loads variables from a .env-style file before reading
// the process environment. Process env vars always take precedence.
func WithEnvFile(path string) Option {
	return func(o *options) { o.envFile = path }
}

// --------------------------------------------------------------------------
// parsing

func parseStruct(rv reflect.Value) error {
	rt := rv.Type()
	var errs []string

	for i := range rt.NumField() {
		field := rt.Field(i)
		fv := rv.Field(i)

		// Recurse into embedded/nested structs (no env tag needed).
		if field.Type.Kind() == reflect.Struct && field.Tag.Get("env") == "" {
			if err := parseStruct(fv); err != nil {
				errs = append(errs, err.Error())
			}
			continue
		}

		tag := field.Tag.Get("env")
		if tag == "" {
			continue
		}

		raw, set := os.LookupEnv(tag)
		if !set || raw == "" {
			def := field.Tag.Get("default")
			if def != "" {
				raw = def
				set = true
			}
		}

		if !set || raw == "" {
			if field.Tag.Get("required") == "true" {
				errs = append(errs, fmt.Sprintf("required env var %q is not set", tag))
			}
			continue
		}

		if err := setField(fv, field.Type, raw); err != nil {
			errs = append(errs, fmt.Sprintf("env var %q: %v", tag, err))
		}
	}

	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

func setField(fv reflect.Value, ft reflect.Type, raw string) error {
	// Handle pointer types by allocating.
	if ft.Kind() == reflect.Ptr {
		ptr := reflect.New(ft.Elem())
		if err := setField(ptr.Elem(), ft.Elem(), raw); err != nil {
			return err
		}
		fv.Set(ptr)
		return nil
	}

	// Special types first.
	switch ft {
	case reflect.TypeOf(time.Duration(0)):
		d, err := time.ParseDuration(raw)
		if err != nil {
			return fmt.Errorf("invalid duration %q: %w", raw, err)
		}
		fv.Set(reflect.ValueOf(d))
		return nil

	case reflect.TypeOf(url.URL{}):
		u, err := url.Parse(raw)
		if err != nil {
			return fmt.Errorf("invalid URL %q: %w", raw, err)
		}
		fv.Set(reflect.ValueOf(*u))
		return nil

	case reflect.TypeOf(net.IP{}):
		ip := net.ParseIP(raw)
		if ip == nil {
			return fmt.Errorf("invalid IP %q", raw)
		}
		fv.Set(reflect.ValueOf(ip))
		return nil

	case reflect.TypeOf([]string{}):
		parts := splitTrimmed(raw, ",")
		fv.Set(reflect.ValueOf(parts))
		return nil
	}

	switch ft.Kind() {
	case reflect.String:
		fv.SetString(raw)

	case reflect.Bool:
		b, err := strconv.ParseBool(raw)
		if err != nil {
			return fmt.Errorf("invalid bool %q", raw)
		}
		fv.SetBool(b)

	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		n, err := strconv.ParseInt(raw, 10, ft.Bits())
		if err != nil {
			return fmt.Errorf("invalid int %q", raw)
		}
		fv.SetInt(n)

	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		n, err := strconv.ParseUint(raw, 10, ft.Bits())
		if err != nil {
			return fmt.Errorf("invalid uint %q", raw)
		}
		fv.SetUint(n)

	case reflect.Float32, reflect.Float64:
		n, err := strconv.ParseFloat(raw, ft.Bits())
		if err != nil {
			return fmt.Errorf("invalid float %q", raw)
		}
		fv.SetFloat(n)

	default:
		return fmt.Errorf("unsupported type %s", ft)
	}
	return nil
}

// --------------------------------------------------------------------------
// .env file loader

// loadEnvFile reads a simple KEY=VALUE file and sets env vars that are not
// already present in the process environment (process env wins).
func loadEnvFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, val, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		key = strings.TrimSpace(key)
		val = strings.Trim(strings.TrimSpace(val), `"'`)
		if _, exists := os.LookupEnv(key); !exists {
			_ = os.Setenv(key, val)
		}
	}
	return sc.Err()
}

func splitTrimmed(s, sep string) []string {
	parts := strings.Split(s, sep)
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
