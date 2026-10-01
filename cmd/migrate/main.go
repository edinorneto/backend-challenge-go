package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/edinorneto/backend-challenge-go/internal/infrastructure/database/migrations"
)

func main() {
	direction := flag.String("direction", "up", "migration direction: up or down")
	steps := flag.Int("steps", 1, "number of migrations to revert when direction=down")
	flag.Parse()

	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		databaseURL = "postgres://postgres:postgres@localhost:5432/betting?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "create PostgreSQL pool: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "ping PostgreSQL: %v\n", err)
		os.Exit(1)
	}

	switch *direction {
	case "up":
		err = migrations.Run(ctx, pool)
	case "down":
		err = migrations.Revert(ctx, pool, *steps)
	default:
		fmt.Fprintln(os.Stderr, "direction must be up or down")
		os.Exit(2)
	}

	if err != nil {
		fmt.Fprintf(os.Stderr, "migration %s failed: %v\n", *direction, err)
		os.Exit(1)
	}

	fmt.Printf("migration %s completed\n", *direction)
}
