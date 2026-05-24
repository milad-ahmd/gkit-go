//go:build integration

package testutil

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/milad-ahmd/gkit-go/pkg/store"
)

// StartPostgres starts a Postgres testcontainer and returns an open store.DB.
// It waits until Postgres accepts connections before returning.
func StartPostgres(t *testing.T) *store.DB {
	t.Helper()
	ctx := context.Background()

	c, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:        "postgres:16-alpine",
			ExposedPorts: []string{"5432/tcp"},
			Env: map[string]string{
				"POSTGRES_USER":     "gkit",
				"POSTGRES_PASSWORD": "secret",
				"POSTGRES_DB":       "gkit",
			},
			WaitingFor: wait.ForAll(
				wait.ForListeningPort("5432/tcp"),
				wait.ForLog("database system is ready to accept connections").
					WithOccurrence(2).
					WithStartupTimeout(60*time.Second),
			),
		},
		Started: true,
	})
	if err != nil {
		t.Fatalf("testutil.StartPostgres: start container: %v", err)
	}
	t.Cleanup(func() { _ = c.Terminate(ctx) })

	host, err := c.Host(ctx)
	if err != nil {
		t.Fatalf("testutil.StartPostgres: host: %v", err)
	}
	port, err := c.MappedPort(ctx, "5432/tcp")
	if err != nil {
		t.Fatalf("testutil.StartPostgres: mapped port: %v", err)
	}

	dsn := fmt.Sprintf("postgres://gkit:secret@%s:%s/gkit?sslmode=disable", host, port.Port())

	var db *store.DB
	deadline := time.Now().Add(30 * time.Second)
	for {
		db, err = store.Open(ctx, dsn)
		if err == nil {
			t.Cleanup(db.Close)
			return db
		}
		if time.Now().After(deadline) {
			t.Fatalf("testutil.StartPostgres: open store: %v", err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}
