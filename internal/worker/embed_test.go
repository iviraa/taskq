package worker_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/iviraa/taskq/internal/queue"

	"github.com/iviraa/taskq/internal/testdb"
	"github.com/iviraa/taskq/internal/worker"
)

func TestDuplicateDeliveryProducesOneSideEffect(t *testing.T) {
	pool := testdb.Pool(t, 8)
	q := queue.New(pool)
	ctx := context.Background()

	if err := q.Reset(ctx); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if _, err := q.EnqueueBatch(ctx, "embed", []any{map[string]int{"doc_id": 77, "ms": 0}}); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	handler := worker.Embed(pool)
	before := worker.SuppressedDuplicates.Load()

	first, err := q.Claim(ctx, "worker-a", 200*time.Millisecond)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}
	if err := handler(ctx, first); err != nil {
		t.Fatalf("first delivery: %v", err)
	}

	// lease expires while the first worker is still running
	time.Sleep(300 * time.Millisecond)

	second, err := q.Claim(ctx, "worker-b", time.Minute)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("second delivery was task %d, want %d", second.ID, first.ID)
	}
	if err := handler(ctx, second); err != nil {
		t.Fatalf("second delivery: %v", err)
	}

	effects, err := q.SideEffects(ctx)
	if err != nil {
		t.Fatalf("side effects: %v", err)
	}

	if effects.Deliveries != 2 {
		t.Errorf("deliveries = %d, want 2, the duplicate this test needs did not happen",
			effects.Deliveries)
	}
	if effects.Documents != 1 {
		t.Errorf("documents = %d, want 1", effects.Documents)
	}
	if !effects.Clean() {
		t.Errorf("side effects not clean: %+v", effects)
	}
	if got := worker.SuppressedDuplicates.Load() - before; got != 1 {
		t.Errorf("suppressed %d duplicates, want 1", got)
	}
}

func TestIdempotentEnqueueCreatesOneTask(t *testing.T) {
	pool := testdb.Pool(t, 4)
	q := queue.New(pool)
	ctx := context.Background()

	if err := q.Reset(ctx); err != nil {
		t.Fatalf("reset: %v", err)
	}

	payload := map[string]int{"doc_id": 9, "ms": 0}
	first, created, err := q.EnqueueIdempotent(ctx, "embed", payload, "submit-9")
	if err != nil {
		t.Fatalf("first enqueue: %v", err)
	}
	if !created {
		t.Error("first enqueue reported the task already existed")
	}

	second, created, err := q.EnqueueIdempotent(ctx, "embed", payload, "submit-9")
	if err != nil {
		t.Fatalf("second enqueue: %v", err)
	}
	if created {
		t.Error("second enqueue created a duplicate task")
	}
	if second != first {
		t.Errorf("second enqueue returned task %d, want the existing %d", second, first)
	}

	audit, err := q.Stat(ctx)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if audit.Total != 1 {
		t.Errorf("queue holds %d tasks, want 1", audit.Total)
	}
}

func TestEmbedRejectsBadPayload(t *testing.T) {
	pool := testdb.Pool(t, 4)
	q := queue.New(pool)
	ctx := context.Background()

	if err := q.Reset(ctx); err != nil {
		t.Fatalf("reset: %v", err)
	}

	handler := worker.Embed(pool)
	bad := &queue.Task{ID: 1, Kind: "embed", Payload: json.RawMessage(`{"doc_id":0}`)}
	if err := handler(ctx, bad); err == nil {
		t.Fatal("embed accepted doc_id 0")
	}

	effects, err := q.SideEffects(ctx)
	if err != nil {
		t.Fatalf("side effects: %v", err)
	}
	if effects.Deliveries != 0 || effects.Documents != 0 {
		t.Errorf("a rejected payload still wrote something: %+v", effects)
	}
}
