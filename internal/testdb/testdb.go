// Package testdb opens the integration test database, skipping when there is none.
package testdb

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const DefaultURL = "postgres://localhost:5432/taskq_test?sslmode=disable"

// serialises test packages that share the database
const lockKey = 0x7a5c_9001

// Pool returns a pool against the test database, holding the lock until the test ends.
func Pool(t *testing.T, maxConns int32) *pgxpool.Pool {
	t.Helper()

	url := os.Getenv("TASKQ_TEST_DATABASE_URL")
	if url == "" {
		url = DefaultURL
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		t.Fatalf("parse %s: %v", url, err)
	}
	// one connection holds the advisory lock
	cfg.MaxConns = maxConns + 1

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Skipf("no test database at %s: %v", url, err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		t.Skipf("no test database at %s: %v", url, err)
	}
	t.Cleanup(pool.Close)

	// advisory locks belong to a session, so pin one connection
	guard, err := pool.Acquire(context.Background())
	if err != nil {
		t.Fatalf("acquire lock connection: %v", err)
	}
	if _, err := guard.Exec(context.Background(), `select pg_advisory_lock($1)`, lockKey); err != nil {
		guard.Release()
		t.Fatalf("take test database lock: %v", err)
	}
	t.Cleanup(func() {
		if _, err := guard.Exec(context.Background(), `select pg_advisory_unlock($1)`, lockKey); err != nil {
			t.Logf("release test database lock: %v", err)
		}
		guard.Release()
	})

	return pool
}
