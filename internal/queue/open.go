package queue

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

const DefaultURL = "postgres://localhost:5432/taskq?sslmode=disable"

// Open connects using DATABASE_URL, or the local dev database.
func Open(ctx context.Context, maxConns int32) (*Queue, error) {
	url := os.Getenv("DATABASE_URL")
	if url == "" {
		url = DefaultURL
	}

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", url, err)
	}
	if maxConns > 0 {
		cfg.MaxConns = maxConns
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", url, err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping %s: %w", url, err)
	}
	return New(pool), nil
}

// Pool is for handlers that write side effects in the same database.
func (q *Queue) Pool() *pgxpool.Pool { return q.pool }

func (q *Queue) Close() { q.pool.Close() }
