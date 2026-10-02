package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/database/migrations"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run applies or reverts migrations and returns the process exit code:
// 0 on success, 1 on a database or migration failure and 2 on invalid usage.
func run(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("migrate", flag.ContinueOnError)
	flags.SetOutput(stderr)
	direction := flags.String("direction", "up", "migration direction: up or down")
	steps := flags.Int("steps", 1, "number of migrations to revert when direction=down")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *direction != "up" && *direction != "down" {
		fmt.Fprintln(stderr, "direction must be up or down")
		return 2
	}
	if *direction == "down" && *steps < 1 {
		fmt.Fprintln(stderr, "steps must be at least 1")
		return 2
	}

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:postgres@localhost:5432/betting?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		fmt.Fprintf(stderr, "create PostgreSQL pool: %v\n", err)
		return 1
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		fmt.Fprintf(stderr, "ping PostgreSQL: %v\n", err)
		return 1
	}

	if *direction == "up" {
		err = migrations.Run(ctx, pool)
	} else {
		err = migrations.Revert(ctx, pool, *steps)
	}
	if err != nil {
		fmt.Fprintf(stderr, "migration %s failed: %v\n", *direction, err)
		return 1
	}

	fmt.Fprintf(stdout, "migration %s completed\n", *direction)
	return 0
}
