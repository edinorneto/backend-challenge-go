package main

import (
	"bytes"
	"context"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestRunRejectsInvalidUsage(t *testing.T) {
	for name, args := range map[string][]string{
		"unknown direction": {"-direction", "sideways"},
		"zero steps":        {"-direction", "down", "-steps", "0"},
		"unknown flag":      {"-force"},
	} {
		t.Run(name, func(t *testing.T) {
			var stdout, stderr bytes.Buffer
			if code := run(args, &stdout, &stderr); code != 2 {
				t.Fatalf("expected usage exit code 2, got %d (%s)", code, stderr.String())
			}
		})
	}
}

// The command runs against a fresh schema selected through search_path in
// DATABASE_URL, exactly as an operator would point it at a database.
func TestRunAppliesAndRevertsMigrations(t *testing.T) {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:postgres@localhost:5432/betting?sslmode=disable"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	base, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		t.Skipf("PostgreSQL unavailable: %v", err)
	}
	t.Cleanup(base.Close)
	if err := base.Ping(ctx); err != nil {
		t.Skipf("PostgreSQL unavailable: %v", err)
	}
	schema := "migrate_cli_" + uuid.New().String()[:8]
	if _, err := base.Exec(ctx, `CREATE SCHEMA `+schema); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = base.Exec(context.Background(), `DROP SCHEMA `+schema+` CASCADE`) })

	parsed, err := url.Parse(databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	query := parsed.Query()
	query.Set("search_path", schema)
	parsed.RawQuery = query.Encode()
	t.Setenv("DATABASE_URL", parsed.String())

	applied := func() int {
		var count int
		if err := base.QueryRow(context.Background(), `SELECT COUNT(*) FROM `+schema+`.schema_migrations`).Scan(&count); err != nil {
			t.Fatal(err)
		}
		return count
	}
	execute := func(args ...string) {
		t.Helper()
		var stdout, stderr bytes.Buffer
		if code := run(args, &stdout, &stderr); code != 0 {
			t.Fatalf("migrate %v exited %d: %s", args, code, stderr.String())
		}
		if !strings.Contains(stdout.String(), "completed") {
			t.Fatalf("migrate %v did not report completion: %s", args, stdout.String())
		}
	}

	execute("-direction", "up")
	latest := applied()
	if latest < 4 {
		t.Fatalf("expected every migration applied, got %d", latest)
	}
	execute("-direction", "down", "-steps", "2")
	if got := applied(); got != latest-2 {
		t.Fatalf("expected %d migrations after reverting two, got %d", latest-2, got)
	}
	execute("-direction", "up")
	if got := applied(); got != latest {
		t.Fatalf("expected %d migrations after re-applying, got %d", latest, got)
	}
	execute("-direction", "up")
	if got := applied(); got != latest {
		t.Fatalf("a second up must be a no-op, got %d migrations", got)
	}
}
