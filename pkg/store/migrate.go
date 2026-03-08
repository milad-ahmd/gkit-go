package store

import (
	"errors"
	"fmt"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5" // pgx/v5 driver
	_ "github.com/golang-migrate/migrate/v4/source/file"      // file:// source
)

// MigrateUp runs all pending up migrations from the given source URL.
//
// Example source URLs:
//
//	file://migrations
//	file:///abs/path/to/migrations
func MigrateUp(dsn, source string) error {
	m, err := newMigrate(dsn, source)
	if err != nil {
		return err
	}
	if err := m.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("store: migrate up: %w", err)
	}
	return nil
}

// MigrateDown rolls back all applied migrations.
func MigrateDown(dsn, source string) error {
	m, err := newMigrate(dsn, source)
	if err != nil {
		return err
	}
	if err := m.Down(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("store: migrate down: %w", err)
	}
	return nil
}

// MigrateSteps runs n migration steps (positive = up, negative = down).
func MigrateSteps(dsn, source string, n int) error {
	m, err := newMigrate(dsn, source)
	if err != nil {
		return err
	}
	if err := m.Steps(n); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		return fmt.Errorf("store: migrate steps %d: %w", n, err)
	}
	return nil
}

// MigrateVersion returns the current schema version and dirty flag.
func MigrateVersion(dsn, source string) (version uint, dirty bool, err error) {
	m, err := newMigrate(dsn, source)
	if err != nil {
		return 0, false, err
	}
	v, d, err := m.Version()
	if errors.Is(err, migrate.ErrNilVersion) {
		return 0, false, nil
	}
	return v, d, err
}

func newMigrate(dsn, source string) (*migrate.Migrate, error) {
	// golang-migrate pgx/v5 driver expects the DSN prefixed with "pgx5://".
	dbURL := "pgx5://" + stripScheme(dsn)
	m, err := migrate.New(source, dbURL)
	if err != nil {
		return nil, fmt.Errorf("store: create migrator: %w", err)
	}
	return m, nil
}

// stripScheme removes an existing "postgres://" or "postgresql://" prefix
// so we can replace it with the driver-specific scheme.
func stripScheme(dsn string) string {
	for _, prefix := range []string{"postgres://", "postgresql://"} {
		if len(dsn) > len(prefix) && dsn[:len(prefix)] == prefix {
			return dsn[len(prefix):]
		}
	}
	return dsn
}
