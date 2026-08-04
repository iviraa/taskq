package queue_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/iviraa/taskq/internal/queue"
)

var errBoom = errors.New("boom")

func TestFailedTaskRetriesUntilExhausted(t *testing.T) {
	q, pool := testQueue(t)
	ctx := context.Background()
	seed(t, q, 1)

	if _, err := pool.Exec(ctx, `update tasks set max_attempts = 3`); err != nil {
		t.Fatalf("set max_attempts: %v", err)
	}

	for attempt := 1; attempt <= 3; attempt++ {
		got, err := q.Claim(ctx, "worker", longLease)
		if err != nil {
			t.Fatalf("claim on attempt %d: %v", attempt, err)
		}
		if got.Attempts != attempt {
			t.Errorf("attempts = %d, want %d", got.Attempts, attempt)
		}

		retrying, err := q.Fail(ctx, got.ID, got.LeaseToken, errBoom, 0)
		if err != nil {
			t.Fatalf("fail on attempt %d: %v", attempt, err)
		}
		if want := attempt < 3; retrying != want {
			t.Errorf("attempt %d: retrying = %v, want %v", attempt, retrying, want)
		}
	}

	if _, err := q.Claim(ctx, "worker", longLease); !errors.Is(err, queue.ErrNoTask) {
		t.Fatalf("claimed a task past its attempt limit: %v", err)
	}

	// already dead, the sweep only catches tasks that never reported
	n, err := q.SweepDead(ctx)
	if err != nil {
		t.Fatalf("sweep: %v", err)
	}
	if n != 0 {
		t.Errorf("sweep touched %d tasks, want 0, Fail should have marked it already", n)
	}

	dead, err := q.Dead(ctx, 10)
	if err != nil {
		t.Fatalf("list dead: %v", err)
	}
	if len(dead) != 1 {
		t.Fatalf("dead-letter queue holds %d tasks, want 1", len(dead))
	}
	if dead[0].LastError == nil || *dead[0].LastError != errBoom.Error() {
		t.Errorf("last_error = %v, want %q", dead[0].LastError, errBoom)
	}
}

// a task that kills its worker never reports, so attempts are counted at claim
func TestCrashedTaskStillDies(t *testing.T) {
	q, pool := testQueue(t)
	ctx := context.Background()
	seed(t, q, 1)

	if _, err := pool.Exec(ctx, `update tasks set max_attempts = 3`); err != nil {
		t.Fatalf("set max_attempts: %v", err)
	}

	// claim and vanish, no Fail or Succeed
	for attempt := 1; attempt <= 3; attempt++ {
		if _, err := q.Claim(ctx, "doomed", time.Millisecond); err != nil {
			t.Fatalf("claim on attempt %d: %v", attempt, err)
		}
		time.Sleep(5 * time.Millisecond)
	}

	if _, err := q.Claim(ctx, "worker", longLease); !errors.Is(err, queue.ErrNoTask) {
		t.Fatalf("a task that crashed its worker three times was handed out again: %v", err)
	}
	if _, err := q.SweepDead(ctx); err != nil {
		t.Fatalf("sweep: %v", err)
	}

	audit, err := q.Stat(ctx)
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	if audit.Dead != 1 {
		t.Errorf("dead = %d, want 1", audit.Dead)
	}
}

func TestBackoffDelaysRedelivery(t *testing.T) {
	q, _ := testQueue(t)
	ctx := context.Background()
	seed(t, q, 1)

	got, err := q.Claim(ctx, "worker", longLease)
	if err != nil {
		t.Fatalf("claim: %v", err)
	}
	if _, err := q.Fail(ctx, got.ID, got.LeaseToken, errBoom, 30*time.Second); err != nil {
		t.Fatalf("fail: %v", err)
	}
	if _, err := q.Claim(ctx, "worker", longLease); !errors.Is(err, queue.ErrNoTask) {
		t.Fatalf("claimed a task still inside its backoff window: %v", err)
	}
}

func TestRequeueRestoresAttemptBudget(t *testing.T) {
	q, pool := testQueue(t)
	ctx := context.Background()
	seed(t, q, 1)

	if _, err := pool.Exec(ctx,
		`update tasks set state = 'dead', attempts = 3, max_attempts = 3`); err != nil {
		t.Fatalf("kill task: %v", err)
	}

	n, err := q.Requeue(ctx, 0)
	if err != nil {
		t.Fatalf("requeue: %v", err)
	}
	if n != 1 {
		t.Fatalf("requeued %d, want 1", n)
	}
	if _, err := q.Claim(ctx, "worker", longLease); err != nil {
		t.Fatalf("requeued task was not claimable: %v", err)
	}
}

func TestBackoffIsBoundedAndJittered(t *testing.T) {
	b := queue.Backoff{Base: time.Second, Max: time.Minute}

	seen := make(map[time.Duration]bool)
	for i := 0; i < 200; i++ {
		d := b.Delay(3) // window is min(60s, 1s << 3) = 8s
		if d < 0 || d >= 8*time.Second {
			t.Fatalf("delay %s outside [0, 8s)", d)
		}
		seen[d] = true
	}
	if len(seen) < 100 {
		t.Errorf("only %d distinct delays in 200 draws, jitter is not working", len(seen))
	}

	// large attempts clamp to Max instead of overflowing
	for _, attempt := range []int{40, 62, 63, 100, 1000} {
		if d := b.Delay(attempt); d < 0 || d >= time.Minute {
			t.Errorf("delay for attempt %d was %s, want inside [0, 1m)", attempt, d)
		}
	}
}
