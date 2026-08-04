package queue_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iviraa/taskq/internal/queue"
)

const longLease = time.Minute

func TestExpiredLeaseIsReclaimed(t *testing.T) {
	q, _ := testQueue(t)
	ctx := context.Background()
	seed(t, q, 1)

	first, err := q.Claim(ctx, "worker-a", 300*time.Millisecond)
	if err != nil {
		t.Fatalf("first claim: %v", err)
	}

	// worker A is gone; the lease still hides the task
	if _, err := q.Claim(ctx, "worker-b", longLease); !errors.Is(err, queue.ErrNoTask) {
		t.Fatalf("claimed a leased task: %v", err)
	}

	time.Sleep(400 * time.Millisecond)

	second, err := q.Claim(ctx, "worker-b", longLease)
	if err != nil {
		t.Fatalf("reclaim after lease expiry: %v", err)
	}
	if second.ID != first.ID {
		t.Fatalf("reclaimed task %d, want %d", second.ID, first.ID)
	}
	if second.Attempts != 2 {
		t.Errorf("claim_count = %d, want 2", second.Attempts)
	}
	if second.LeaseToken == first.LeaseToken {
		t.Error("lease token was not regenerated, so A could still overwrite B")
	}
}

func TestHeartbeatHoldsLease(t *testing.T) {
	q, _ := testQueue(t)
	ctx := context.Background()
	seed(t, q, 1)

	held, err := q.Claim(ctx, "worker-a", 300*time.Millisecond)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}

	for i := 0; i < 3; i++ {
		time.Sleep(200 * time.Millisecond)
		if err := q.Heartbeat(ctx, held.ID, held.LeaseToken, 300*time.Millisecond); err != nil {
			t.Fatalf("heartbeat %d: %v", i, err)
		}
	}

	if _, err := q.Claim(ctx, "worker-b", longLease); !errors.Is(err, queue.ErrNoTask) {
		t.Fatalf("a heartbeating worker lost its task: %v", err)
	}
}

func TestLostLeaseCannotRecordResult(t *testing.T) {
	q, _ := testQueue(t)
	ctx := context.Background()
	seed(t, q, 1)

	a, err := q.Claim(ctx, "worker-a", 200*time.Millisecond)
	if err != nil {
		t.Fatalf("claim a: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	b, err := q.Claim(ctx, "worker-b", longLease)
	if err != nil {
		t.Fatalf("claim b: %v", err)
	}

	if err := q.Heartbeat(ctx, a.ID, a.LeaseToken, longLease); !errors.Is(err, queue.ErrLeaseLost) {
		t.Errorf("stale heartbeat returned %v, want ErrLeaseLost", err)
	}
	if err := q.Succeed(ctx, a.ID, a.LeaseToken); !errors.Is(err, queue.ErrLeaseLost) {
		t.Errorf("stale succeed returned %v, want ErrLeaseLost", err)
	}
	if _, err := q.Fail(ctx, a.ID, a.LeaseToken, errors.New("stale"), 0); !errors.Is(err, queue.ErrLeaseLost) {
		t.Errorf("stale fail returned %v, want ErrLeaseLost", err)
	}

	if err := q.Succeed(ctx, b.ID, b.LeaseToken); err != nil {
		t.Fatalf("b could not finish its own task: %v", err)
	}

	audit, err := q.Stat(ctx)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if audit.Succeeded != 1 || audit.Dead != 0 {
		t.Errorf("audit succeeded=%d failed=%d, want 1 and 0", audit.Succeeded, audit.Dead)
	}
}

func TestNotYetVisibleIsNotClaimable(t *testing.T) {
	q, pool := testQueue(t)
	ctx := context.Background()
	seed(t, q, 1)

	if _, err := pool.Exec(ctx, `update tasks set visible_at = now() + interval '1 hour'`); err != nil {
		t.Fatalf("hide task: %v", err)
	}
	if _, err := q.Claim(ctx, "worker-a", longLease); !errors.Is(err, queue.ErrNoTask) {
		t.Fatalf("claimed a task scheduled for the future: %v", err)
	}
}
