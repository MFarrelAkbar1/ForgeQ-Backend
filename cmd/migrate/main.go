// Command migrate applies the embedded database migrations.
//
//	migrate [up|down|status]   (default: up)
package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	"github.com/pressly/goose/v3"

	"github.com/MFarrelAkbar1/ForgeQ-Backend/migrations"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
}

func run() error {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	cmd := "up"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}

	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	p, err := migrations.NewProvider(db)
	if err != nil {
		return err
	}
	ctx := context.Background()

	switch cmd {
	case "up":
		results, err := p.Up(ctx)
		printResults(results)
		return err
	case "down":
		result, err := p.Down(ctx)
		if result != nil {
			printResults([]*goose.MigrationResult{result})
		}
		return err
	case "status":
		statuses, err := p.Status(ctx)
		if err != nil {
			return err
		}
		for _, s := range statuses {
			fmt.Printf("%-8s %s\n", s.State, s.Source.Path)
		}
		return nil
	default:
		return fmt.Errorf("unknown command %q (want up, down or status)", cmd)
	}
}

func printResults(results []*goose.MigrationResult) {
	if len(results) == 0 {
		fmt.Println("no migrations to apply")
	}
	for _, r := range results {
		fmt.Println(r)
	}
}
