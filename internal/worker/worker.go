// Package worker runs N goroutines that each claim their own tasks.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/iviraa/taskq/internal/metrics"
	"github.com/iviraa/taskq/internal/queue"
)

const finishTimeout = 5 * time.Second

// renew three times per lease
const heartbeatDivisor = 3

type Config struct {
	ID            string
	Size          int
	Poll          time.Duration
	Lease         time.Duration
	Backoff       queue.Backoff
	SweepInterval time.Duration
	Drain         bool // exit when the queue empties instead of polling for more

	// how long shutdown waits for in-flight tasks
	Grace time.Duration
}

type Pool struct {
	q       *queue.Queue
	handler Handler
	cfg     Config

	succeeded atomic.Int64
	failed    atomic.Int64
	retried   atomic.Int64
	dead      atomic.Int64
	lost      atomic.Int64
	inFlight  atomic.Int64
}

func NewPool(q *queue.Queue, h Handler, cfg Config) *Pool {
	if cfg.Size < 1 {
		cfg.Size = 1
	}
	if cfg.Poll <= 0 {
		cfg.Poll = 100 * time.Millisecond
	}
	if cfg.Lease <= 0 {
		cfg.Lease = 30 * time.Second
	}
	if cfg.Backoff == (queue.Backoff{}) {
		cfg.Backoff = queue.DefaultBackoff
	}
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = 5 * time.Second
	}
	if cfg.Grace <= 0 {
		cfg.Grace = 30 * time.Second
	}
	return &Pool{q: q, handler: h, cfg: cfg}
}

// Run blocks until ctx is cancelled or, in drain mode, the queue is empty.
func (p *Pool) Run(ctx context.Context) {
	tasks, abandon := context.WithCancel(context.WithoutCancel(ctx))
	defer abandon()

	workersDone := make(chan struct{})
	go p.awaitShutdown(ctx, workersDone, abandon)

	sweepCtx, stopSweep := context.WithCancel(tasks)
	sweepDone := make(chan struct{})
	go func() {
		defer close(sweepDone)
		p.sweepDead(sweepCtx)
	}()

	var wg sync.WaitGroup
	for i := 0; i < p.cfg.Size; i++ {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			p.loop(ctx, tasks, fmt.Sprintf("%s#%d", p.cfg.ID, slot))
		}(i)
	}
	wg.Wait()
	close(workersDone)

	stopSweep()
	<-sweepDone
}

func (p *Pool) awaitShutdown(ctx context.Context, workersDone <-chan struct{}, abandon func()) {
	select {
	case <-workersDone:
		return
	case <-ctx.Done():
	}

	inFlight := p.inFlight.Load()
	if inFlight == 0 {
		abandon()
		return
	}
	slog.Info("shutting down, waiting for in-flight tasks",
		"worker", p.cfg.ID, "in_flight", inFlight, "grace", p.cfg.Grace)

	timer := time.NewTimer(p.cfg.Grace)
	defer timer.Stop()

	select {
	case <-workersDone:
		slog.Info("in-flight tasks finished, exiting cleanly", "worker", p.cfg.ID)
	case <-timer.C:
		slog.Warn("grace period expired, abandoning tasks to their leases",
			"worker", p.cfg.ID, "still_running", p.inFlight.Load())
	}
	abandon()
}

// loop stops claiming on stop but finishes held tasks under tasks.
func (p *Pool) loop(stop, tasks context.Context, workerID string) {
	for stop.Err() == nil && tasks.Err() == nil {
		ran, err := p.processOne(tasks, workerID)
		if err != nil {
			slog.Error("processing task", "worker", workerID, "err", err)
		}
		if !ran {
			if p.cfg.Drain && p.queueEmpty(tasks) {
				return
			}
			sleep(stop, p.cfg.Poll)
		}
	}
}

func (p *Pool) processOne(ctx context.Context, workerID string) (bool, error) {
	t, err := p.q.Claim(ctx, workerID, p.cfg.Lease)
	if errors.Is(err, queue.ErrNoTask) {
		return false, nil
	}
	if err != nil {
		if ctx.Err() != nil {
			return false, nil
		}
		return false, err
	}
	slog.Debug("claimed", "worker", workerID, "task", t.ID, "kind", t.Kind, "attempt", t.Attempts)

	p.inFlight.Add(1)
	defer p.inFlight.Add(-1)

	// cancelled when the lease is lost
	taskCtx, cancelTask := context.WithCancel(ctx)
	defer cancelTask()

	var lostLease atomic.Bool
	heartbeatDone := p.heartbeat(taskCtx, t, &lostLease, cancelTask)

	start := time.Now()
	runErr := p.handler(taskCtx, t)
	elapsed := time.Since(start).Round(time.Millisecond)

	cancelTask()
	<-heartbeatDone

	if lostLease.Load() {
		p.lost.Add(1)
		metrics.LeasesLost.Inc()
		slog.Warn("lost lease mid-task, another worker owns it now",
			"worker", workerID, "task", t.ID, "ran_for", elapsed)
		return true, nil
	}

	// record the outcome even if ctx was cancelled
	finish, cancel := context.WithTimeout(context.WithoutCancel(ctx), finishTimeout)
	defer cancel()

	switch {
	case runErr == nil:
		if err := p.q.Succeed(finish, t.ID, t.LeaseToken); err != nil {
			return true, p.leaseAware(workerID, t.ID, err)
		}
		p.succeeded.Add(1)
		metrics.Succeeded.Inc()
		slog.Debug("succeeded", "worker", workerID, "task", t.ID, "took", elapsed)

	case errors.Is(runErr, context.Canceled):
		slog.Info("interrupted, task returns when its lease expires",
			"worker", workerID, "task", t.ID, "ran_for", elapsed)

	default:
		delay := p.cfg.Backoff.Delay(t.Attempts)
		retrying, err := p.q.Fail(finish, t.ID, t.LeaseToken, runErr, delay)
		if err != nil {
			return true, p.leaseAware(workerID, t.ID, err)
		}
		p.failed.Add(1)
		if retrying {
			p.retried.Add(1)
			metrics.Retried.Inc()
			slog.Debug("attempt failed, retrying",
				"worker", workerID, "task", t.ID, "attempt", t.Attempts,
				"max_attempts", t.MaxAttempts, "retry_in", delay.Round(time.Millisecond),
				"err", runErr)
		} else {
			p.dead.Add(1)
			metrics.Dead.Inc()
			slog.Warn("task dead, moved to the dead-letter queue",
				"worker", workerID, "task", t.ID, "attempts", t.Attempts, "err", runErr)
		}
	}
	return true, nil
}

func (p *Pool) queueEmpty(ctx context.Context) bool {
	n, err := p.q.Remaining(ctx)
	if err != nil {
		if ctx.Err() == nil {
			slog.Error("counting remaining tasks", "err", err)
		}
		return false
	}
	return n == 0
}

func (p *Pool) sweepDead(ctx context.Context) {
	ticker := time.NewTicker(p.cfg.SweepInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			// final pass before exit
			last, cancel := context.WithTimeout(context.WithoutCancel(ctx), finishTimeout)
			defer cancel()
			if _, err := p.q.SweepDead(last); err != nil {
				slog.Error("final dead-letter sweep", "err", err)
			}
			return
		case <-ticker.C:
			if _, err := p.q.SweepDead(ctx); err != nil && ctx.Err() == nil {
				slog.Error("dead-letter sweep", "err", err)
			}
		}
	}
}

// heartbeat renews the lease and cancels the task if it is lost.
func (p *Pool) heartbeat(ctx context.Context, t *queue.Task, lost *atomic.Bool, cancelTask func()) <-chan struct{} {
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(p.cfg.Lease / heartbeatDivisor)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				err := p.q.Heartbeat(ctx, t.ID, t.LeaseToken, p.cfg.Lease)
				switch {
				case err == nil:
				case errors.Is(err, queue.ErrLeaseLost):
					lost.Store(true)
					cancelTask()
					return
				case ctx.Err() != nil:
					return
				default:
					slog.Error("heartbeat", "task", t.ID, "err", err)
				}
			}
		}
	}()
	return done
}

func (p *Pool) leaseAware(workerID string, id int64, err error) error {
	if errors.Is(err, queue.ErrLeaseLost) {
		p.lost.Add(1)
		slog.Warn("lease expired before the result could be recorded",
			"worker", workerID, "task", id)
		return nil
	}
	return err
}

type Stats struct {
	Succeeded int64
	Failed    int64 // attempts that returned an error, including ones that retried
	Retried   int64
	Dead      int64
	Lost      int64
}

func (p *Pool) Stats() Stats {
	return Stats{
		Succeeded: p.succeeded.Load(),
		Failed:    p.failed.Load(),
		Retried:   p.retried.Load(),
		Dead:      p.dead.Load(),
		Lost:      p.lost.Load(),
	}
}

func sleep(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}
