package queue_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/iviraa/taskq/internal/queue"
	"github.com/iviraa/taskq/internal/testdb"
)

const (
	tasks   = 2000
	workers = 32
)

func testQueue(t *testing.T) (*queue.Queue, *pgxpool.Pool) {
	t.Helper()

	pool := testdb.Pool(t, workers+2)
	q := queue.New(pool)
	if err := q.Reset(context.Background()); err != nil {
		t.Fatalf("reset: %v", err)
	}
	return q, pool
}

func seed(t *testing.T, q *queue.Queue, n int) {
	t.Helper()
	payloads := make([]any, n)
	for i := range payloads {
		payloads[i] = map[string]int{"ms": 0}
	}
	if _, err := q.EnqueueBatch(context.Background(), "sleep", payloads); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
}

// claimRace drains the queue concurrently and counts distinct and duplicate claims.
func claimRace(t *testing.T, claim func(ctx context.Context, id string) (int64, error)) (distinct, dupes int) {
	t.Helper()

	var (
		mu   sync.Mutex
		seen = make(map[int64]bool)
		dup  int
		wg   sync.WaitGroup
	)
	ctx := context.Background()

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			id := fmt.Sprintf("test#%d", slot)
			for {
				taskID, err := claim(ctx, id)
				if errors.Is(err, queue.ErrNoTask) {
					return
				}
				if err != nil {
					t.Errorf("claim: %v", err)
					return
				}
				mu.Lock()
				if seen[taskID] {
					dup++
				}
				seen[taskID] = true
				mu.Unlock()
			}
		}(i)
	}
	wg.Wait()
	return len(seen), dup
}

func TestClaimIsExclusiveUnderConcurrency(t *testing.T) {
	q, _ := testQueue(t)
	seed(t, q, tasks)

	distinct, dupes := claimRace(t, func(ctx context.Context, id string) (int64, error) {
		task, err := q.Claim(ctx, id, time.Minute)
		if err != nil {
			return 0, err
		}
		return task.ID, q.Succeed(ctx, task.ID, task.LeaseToken)
	})

	if dupes != 0 {
		t.Errorf("%d tasks claimed more than once", dupes)
	}
	if distinct != tasks {
		t.Errorf("claimed %d distinct tasks, want %d", distinct, tasks)
	}

	audit, err := q.Stat(context.Background())
	if err != nil {
		t.Fatalf("audit: %v", err)
	}
	switch {
	case audit.Total != tasks:
		t.Errorf("total = %d, want %d", audit.Total, tasks)
	case audit.Redelivered != 0:
		t.Errorf("%d rows were redelivered under long leases, which should be impossible", audit.Redelivered)
	case audit.NeverClaimed != 0:
		t.Errorf("%d rows were never claimed", audit.NeverClaimed)
	case audit.Unfinished != 0:
		t.Errorf("%d rows never finished", audit.Unfinished)
	case audit.Claimants < 2:
		t.Errorf("only %d distinct claimants, the test did not run concurrently", audit.Claimants)
	}
}

// control: a select-then-update claim must duplicate, proving the race test can catch it
func TestNaiveClaimDuplicates(t *testing.T) {
	q, pool := testQueue(t)
	seed(t, q, tasks)

	naive := func(ctx context.Context, id string) (int64, error) {
		var taskID int64
		err := pool.QueryRow(ctx,
			`select id from tasks where state = 'pending' order by id limit 1`).Scan(&taskID)
		if errors.Is(err, pgx.ErrNoRows) {
			return 0, queue.ErrNoTask
		}
		if err != nil {
			return 0, err
		}
		_, err = pool.Exec(ctx,
			`update tasks set state = 'running', claimed_by = $2, attempts = attempts + 1
			 where id = $1`, taskID, id)
		if err != nil {
			return 0, err
		}
		return taskID, nil
	}

	distinct, dupes := claimRace(t, naive)
	audit, err := q.Stat(context.Background())
	if err != nil {
		t.Fatalf("audit: %v", err)
	}

	t.Logf("naive claim: %d distinct tasks, %d duplicate deliveries, %d rows with claim_count > 1",
		distinct, dupes, audit.Redelivered)

	if dupes == 0 && audit.Redelivered == 0 {
		t.Fatal("the naive claim did not duplicate, so this harness cannot detect a broken claim " +
			"and TestClaimIsExclusiveUnderConcurrency proves nothing")
	}
}

func TestClaimOnEmptyQueue(t *testing.T) {
	q, _ := testQueue(t)
	if _, err := q.Claim(context.Background(), "test", time.Minute); !errors.Is(err, queue.ErrNoTask) {
		t.Fatalf("got %v, want ErrNoTask", err)
	}
}
