package worker_test

import (
	"context"
	"testing"
	"time"

	"github.com/iviraa/taskq/internal/queue"
	"github.com/iviraa/taskq/internal/testdb"
	"github.com/iviraa/taskq/internal/worker"
)

func runPool(t *testing.T, ctx context.Context, q *queue.Queue, cfg worker.Config) (*worker.Pool, func()) {
	t.Helper()

	p := worker.NewPool(q, worker.Default(nil).Handle, cfg)
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Run(ctx)
	}()
	return p, func() { <-done }
}

func TestShutdownFinishesInFlightTask(t *testing.T) {
	pool := testdb.Pool(t, 4)
	q := queue.New(pool)
	ctx := context.Background()

	if err := q.Reset(ctx); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if _, err := q.EnqueueBatch(ctx, "sleep", []any{map[string]int{"ms": 400}}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	runCtx, stop := context.WithCancel(ctx)
	p, wait := runPool(t, runCtx, q, worker.Config{
		ID: "shutdown", Size: 1, Poll: 10 * time.Millisecond,
		Lease: time.Minute, Grace: 10 * time.Second,
	})

	time.Sleep(150 * time.Millisecond)
	stop()
	wait()

	if got := p.Stats().Succeeded; got != 1 {
		t.Errorf("succeeded = %d, want 1, the in-flight task was abandoned", got)
	}
	audit, err := q.Stat(ctx)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if audit.Succeeded != 1 || audit.Unfinished != 0 {
		t.Errorf("audit succeeded=%d unfinished=%d, want 1 and 0",
			audit.Succeeded, audit.Unfinished)
	}
}

func TestShutdownAbandonsAfterGrace(t *testing.T) {
	pool := testdb.Pool(t, 4)
	q := queue.New(pool)
	ctx := context.Background()

	if err := q.Reset(ctx); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if _, err := q.EnqueueBatch(ctx, "sleep", []any{map[string]int{"ms": 10000}}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	runCtx, stop := context.WithCancel(ctx)
	p, wait := runPool(t, runCtx, q, worker.Config{
		ID: "impatient", Size: 1, Poll: 10 * time.Millisecond,
		Lease: time.Minute, Grace: 200 * time.Millisecond,
	})

	time.Sleep(150 * time.Millisecond)
	stop()

	start := time.Now()
	wait()
	waited := time.Since(start)

	if waited > 3*time.Second {
		t.Errorf("shutdown took %s, the grace period did not bound it", waited)
	}
	if got := p.Stats().Succeeded; got != 0 {
		t.Errorf("succeeded = %d, want 0, a 10s task cannot finish in a 200ms grace", got)
	}

	// not lost: its lease will expire
	audit, err := q.Stat(ctx)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if audit.Unfinished != 1 {
		t.Errorf("unfinished = %d, want 1", audit.Unfinished)
	}
}
