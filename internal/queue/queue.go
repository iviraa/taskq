// Package queue holds every statement that touches the tasks table.
package queue

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrNoTask = errors.New("queue: no task available")

	// ErrLeaseLost means the task now belongs to another worker.
	ErrLeaseLost = errors.New("queue: lease lost")
)

type Queue struct {
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Queue {
	return &Queue{pool: pool}
}

// EnqueueIdempotent returns the existing task when the key was already used.
func (q *Queue) EnqueueIdempotent(ctx context.Context, kind string, payload any, key string) (id int64, created bool, err error) {
	body, err := json.Marshal(payload)
	if err != nil {
		return 0, false, fmt.Errorf("marshal payload: %w", err)
	}

	// union arm: ON CONFLICT DO NOTHING returns no row
	err = q.pool.QueryRow(ctx, `
        with ins as (
            insert into tasks (kind, payload, idempotency_key)
            values ($1, $2, $3)
            on conflict (idempotency_key) do nothing
            returning id
        )
        select id, true from ins
        union all
        select id, false from tasks where idempotency_key = $3
        limit 1`, kind, body, key).Scan(&id, &created)
	if err != nil {
		return 0, false, fmt.Errorf("enqueue idempotent: %w", err)
	}
	return id, created, nil
}

// EnqueueBatch inserts tasks with one COPY.
func (q *Queue) EnqueueBatch(ctx context.Context, kind string, payloads []any) (int64, error) {
	rows := make([][]any, len(payloads))
	for i, p := range payloads {
		body, err := json.Marshal(p)
		if err != nil {
			return 0, fmt.Errorf("marshal payload %d: %w", i, err)
		}
		rows[i] = []any{kind, body}
	}

	n, err := q.pool.CopyFrom(ctx,
		pgx.Identifier{"tasks"},
		[]string{"kind", "payload"},
		pgx.CopyFromRows(rows),
	)
	if err != nil {
		return 0, fmt.Errorf("enqueue batch: %w", err)
	}
	return n, nil
}

// select and update in one statement; SKIP LOCKED skips rows other workers hold, expired leases are reclaimable
const claimSQL = `
update tasks
set state       = 'running',
    claimed_at  = now(),
    claimed_by  = $1,
    attempts    = attempts + 1,
    visible_at  = now() + make_interval(secs => $2),
    lease_token = gen_random_uuid()
where id = (
    select id from tasks
    where state in ('pending', 'running')
      and visible_at <= now()
      and attempts < max_attempts
    order by visible_at, id
    for update skip locked
    limit 1
)
returning id, kind, payload, attempts, max_attempts, lease_token::text, created_at`

// Claim leases the oldest claimable task.
func (q *Queue) Claim(ctx context.Context, claimedBy string, lease time.Duration) (*Task, error) {
	var t Task
	err := q.pool.QueryRow(ctx, claimSQL, claimedBy, lease.Seconds()).
		Scan(&t.ID, &t.Kind, &t.Payload, &t.Attempts, &t.MaxAttempts, &t.LeaseToken, &t.CreatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrNoTask
	}
	if err != nil {
		return nil, fmt.Errorf("claim: %w", err)
	}
	t.State = StateRunning
	t.ClaimedBy = &claimedBy
	return &t, nil
}

// Heartbeat extends the lease, or returns ErrLeaseLost.
func (q *Queue) Heartbeat(ctx context.Context, id int64, token string, lease time.Duration) error {
	tag, err := q.pool.Exec(ctx, `
        update tasks set visible_at = now() + make_interval(secs => $3)
        where id = $1 and lease_token = $2::uuid and state = 'running'`,
		id, token, lease.Seconds())
	if err != nil {
		return fmt.Errorf("heartbeat task %d: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

func (q *Queue) Succeed(ctx context.Context, id int64, token string) error {
	tag, err := q.pool.Exec(ctx, `
        update tasks set state = 'succeeded', finished_at = now()
        where id = $1 and lease_token = $2::uuid`, id, token)
	if err != nil {
		return fmt.Errorf("succeed task %d: %w", id, err)
	}
	if tag.RowsAffected() == 0 {
		return ErrLeaseLost
	}
	return nil
}

// Fail schedules a retry after the backoff and reports whether attempts remain.
func (q *Queue) Fail(ctx context.Context, id int64, token string, cause error, delay time.Duration) (retrying bool, err error) {
	// no backoff on the last attempt so SweepDead can see it
	err = q.pool.QueryRow(ctx, `
        update tasks
        set last_error  = $3,
            state       = case when attempts >= max_attempts then 'dead' else 'pending' end,
            visible_at  = case when attempts >= max_attempts
                               then now()
                               else now() + make_interval(secs => $4) end,
            finished_at = case when attempts >= max_attempts then now() else null end,
            lease_token = null
        where id = $1 and lease_token = $2::uuid
        returning attempts < max_attempts`,
		id, token, cause.Error(), delay.Seconds()).Scan(&retrying)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, ErrLeaseLost
	}
	if err != nil {
		return false, fmt.Errorf("fail task %d: %w", id, err)
	}
	return retrying, nil
}

// SweepDead marks exhausted tasks dead.
func (q *Queue) SweepDead(ctx context.Context) (int64, error) {
	tag, err := q.pool.Exec(ctx, `
        update tasks
        set state = 'dead', finished_at = now(),
            last_error = coalesce(last_error, 'exhausted max_attempts without reporting a failure')
        where state in ('pending', 'running')
          and attempts >= max_attempts
          and visible_at <= now()`)
	if err != nil {
		return 0, fmt.Errorf("sweep dead: %w", err)
	}
	return tag.RowsAffected(), nil
}

// Dead lists the dead-letter queue.
func (q *Queue) Dead(ctx context.Context, limit int) ([]Task, error) {
	rows, err := q.pool.Query(ctx, `
        select id, kind, payload, attempts, max_attempts, created_at, finished_at, last_error
        from tasks where state = 'dead' order by id limit $1`, limit)
	if err != nil {
		return nil, fmt.Errorf("list dead: %w", err)
	}
	defer rows.Close()

	var out []Task
	for rows.Next() {
		var t Task
		if err := rows.Scan(&t.ID, &t.Kind, &t.Payload, &t.Attempts, &t.MaxAttempts,
			&t.CreatedAt, &t.FinishedAt, &t.LastError); err != nil {
			return nil, fmt.Errorf("scan dead: %w", err)
		}
		t.State = StateDead
		out = append(out, t)
	}
	return out, rows.Err()
}

// Requeue resets dead tasks; id 0 requeues all.
func (q *Queue) Requeue(ctx context.Context, id int64) (int64, error) {
	tag, err := q.pool.Exec(ctx, `
        update tasks
        set state = 'pending', attempts = 0, visible_at = now(),
            finished_at = null, lease_token = null
        where state = 'dead' and ($1 = 0 or id = $1)`, id)
	if err != nil {
		return 0, fmt.Errorf("requeue: %w", err)
	}
	return tag.RowsAffected(), nil
}

type Stats struct {
	Depth       int64         // not finished and still allowed to run
	Ready       int64         // of those, claimable right now
	InFlight    int64         // running under a live lease
	OldestReady time.Duration // worker lag: how long the oldest ready task has waited

	Total        int64
	Succeeded    int64
	Dead         int64
	Unfinished   int64 // still pending or running
	NeverClaimed int64 // attempts = 0
	Redelivered  int64 // attempts > 1, expected once leases can expire
	Claimants    int64 // distinct claimed_by values
}

// Complete reports whether every task reached a terminal state.
func (s Stats) Complete() bool {
	return s.Unfinished == 0 && s.NeverClaimed == 0 && s.Succeeded+s.Dead == s.Total
}

func (q *Queue) Stat(ctx context.Context) (Stats, error) {
	const claimable = `state in ('pending','running') and attempts < max_attempts`

	var s Stats
	var oldest float64
	err := q.pool.QueryRow(ctx, `
        select
            count(*) filter (where `+claimable+`),
            count(*) filter (where `+claimable+` and visible_at <= now()),
            count(*) filter (where state = 'running' and visible_at > now()),
            coalesce(extract(epoch from now() - min(visible_at) filter (
                where `+claimable+` and visible_at <= now())), 0),
            count(*),
            count(*) filter (where state = 'succeeded'),
            count(*) filter (where state = 'dead'),
            count(*) filter (where state in ('pending', 'running')),
            count(*) filter (where attempts = 0),
            count(*) filter (where attempts > 1),
            count(distinct claimed_by)
        from tasks`).
		Scan(&s.Depth, &s.Ready, &s.InFlight, &oldest,
			&s.Total, &s.Succeeded, &s.Dead, &s.Unfinished,
			&s.NeverClaimed, &s.Redelivered, &s.Claimants)
	if err != nil {
		return s, fmt.Errorf("stat: %w", err)
	}
	s.OldestReady = time.Duration(oldest * float64(time.Second))
	return s, nil
}

// Remaining counts unfinished tasks, including ones waiting on a backoff.
func (q *Queue) Remaining(ctx context.Context) (int64, error) {
	var n int64
	err := q.pool.QueryRow(ctx, `
        select count(*) from tasks
        where state in ('pending', 'running') and attempts < max_attempts`).Scan(&n)
	if err != nil {
		return 0, fmt.Errorf("remaining: %w", err)
	}
	return n, nil
}

func (q *Queue) SetMaxAttempts(ctx context.Context, n int) error {
	_, err := q.pool.Exec(ctx,
		`update tasks set max_attempts = $1 where state in ('pending', 'running')`, n)
	if err != nil {
		return fmt.Errorf("set max_attempts: %w", err)
	}
	return nil
}

func (q *Queue) Reset(ctx context.Context) error {
	_, err := q.pool.Exec(ctx, `truncate tasks, documents, documents_naive restart identity`)
	if err != nil {
		return fmt.Errorf("reset: %w", err)
	}
	return nil
}

// Latency is enqueue to terminal state.
type Latency struct {
	Count int64
	P50   time.Duration
	P95   time.Duration
	P99   time.Duration
	Max   time.Duration
}

// LatencyStats computes exact percentiles in SQL.
func (q *Queue) LatencyStats(ctx context.Context) (Latency, error) {
	var l Latency
	var p50, p95, p99, max float64
	err := q.pool.QueryRow(ctx, `
        select count(*),
               coalesce(percentile_disc(0.50) within group (order by seconds), 0),
               coalesce(percentile_disc(0.95) within group (order by seconds), 0),
               coalesce(percentile_disc(0.99) within group (order by seconds), 0),
               coalesce(max(seconds), 0)
        from (
            select extract(epoch from finished_at - created_at) as seconds
            from tasks
            where finished_at is not null
        ) t`).Scan(&l.Count, &p50, &p95, &p99, &max)
	if err != nil {
		return l, fmt.Errorf("latency stats: %w", err)
	}
	secs := func(f float64) time.Duration { return time.Duration(f * float64(time.Second)) }
	l.P50, l.P95, l.P99, l.Max = secs(p50), secs(p95), secs(p99), secs(max)
	return l, nil
}

// SideEffects compares deliveries with rows actually written.
type SideEffects struct {
	Deliveries   int64 // handler runs that reached the side-effect write
	DistinctDocs int64 // documents those deliveries covered
	Documents    int64 // rows in the idempotent table
	Duplicates   int64 // deliveries that repeated work already done
}

// Clean means duplicates were delivered but none were written.
func (s SideEffects) Clean() bool {
	return s.Documents == s.DistinctDocs
}

func (q *Queue) SideEffects(ctx context.Context) (SideEffects, error) {
	var s SideEffects
	err := q.pool.QueryRow(ctx, `
        select (select count(*) from documents_naive),
               (select count(distinct doc_id) from documents_naive),
               (select count(*) from documents)`).
		Scan(&s.Deliveries, &s.DistinctDocs, &s.Documents)
	if err != nil {
		return s, fmt.Errorf("side effects: %w", err)
	}
	s.Duplicates = s.Deliveries - s.DistinctDocs
	return s, nil
}
