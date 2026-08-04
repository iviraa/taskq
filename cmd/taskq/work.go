package main

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/iviraa/taskq/internal/metrics"
	"github.com/iviraa/taskq/internal/worker"
)

func work(args []string) {
	fs := flags("work")
	size := fs.Int("n", 4, "goroutines in the pool")
	id := fs.String("id", hostID(), "worker identity recorded in tasks.claimed_by")
	poll := fs.Duration("poll", 100*time.Millisecond, "wait between empty claims")
	lease := fs.Duration("lease", 30*time.Second, "visibility timeout on a claimed task")
	grace := fs.Duration("grace", 30*time.Second, "how long a shutdown waits for in-flight tasks")
	drain := fs.Bool("drain", false, "exit once the queue is empty")
	addr := fs.String("metrics", "", "serve Prometheus metrics here, e.g. :9090")
	format := fs.String("log", "text", "log format: text or json")
	debug := fs.Bool("v", false, "log every task at debug level")
	faults := fs.Bool("fault-injection", false,
		"register the failure-injection handlers, one of which terminates the process on purpose")
	fs.Parse(args)

	setupLogging(*format, *debug)
	ctx, stop := signalContext()
	defer stop()

	q := open(ctx, int32(*size)+2)
	defer q.Close()

	if *addr != "" {
		go metrics.Serve(*addr)
		go metrics.Poll(ctx, q, time.Second)
		slog.Info("serving metrics", "worker", *id, "addr", "http://"+*addr+"/metrics")
	}

	registry := worker.Default(q.Pool())
	if *faults {
		registry = worker.WithFaults(registry)
		slog.Warn("fault injection enabled, do not run this in a deployment", "worker", *id)
	}

	p := worker.NewPool(q, registry.Handle, worker.Config{
		ID: *id, Size: *size, Poll: *poll, Lease: *lease, Grace: *grace, Drain: *drain,
	})

	slog.Info("starting", "worker", *id, "goroutines", *size, "lease", *lease, "grace", *grace)
	start := time.Now()
	p.Run(ctx)

	s := p.Stats()
	slog.Info("stopped", "worker", *id,
		"succeeded", s.Succeeded, "failed", s.Failed, "retried", s.Retried,
		"dead", s.Dead, "lost_lease", s.Lost,
		"uptime", time.Since(start).Round(time.Millisecond))
}

func produce(args []string) {
	fs := flags("produce")
	kind := fs.String("kind", "embed", "task kind")
	n := fs.Int("n", 1, "number of tasks")
	ms := fs.Int("ms", 5, "simulated work per task, milliseconds")
	linger := fs.Int("linger", 0,
		"embed only: hold the handler open this long after the side effect lands, "+
			"widening the window where a kill loses the acknowledgement")
	payload := fs.String("payload", "", "raw JSON payload, overrides the generated one")
	maxAttempts := fs.Int("max-attempts", 0, "override max_attempts on the enqueued tasks")
	reset := fs.Bool("reset", false, "truncate the tables first")
	fs.Parse(args)

	ctx, cancel := signalContext()
	defer cancel()

	q := open(ctx, 4)
	defer q.Close()

	if *reset {
		if err := q.Reset(ctx); err != nil {
			fatal(err)
		}
	}

	payloads, err := buildPayloads(*kind, *n, *ms, *linger, *payload)
	if err != nil {
		fatal(err)
	}

	start := time.Now()
	count, err := q.EnqueueBatch(ctx, *kind, payloads)
	if err != nil {
		fatal(err)
	}
	if *maxAttempts > 0 {
		if err := q.SetMaxAttempts(ctx, *maxAttempts); err != nil {
			fatal(err)
		}
	}

	elapsed := time.Since(start)
	fmt.Printf("enqueued %d %s tasks in %s (%.0f/sec)\n",
		count, *kind, elapsed.Round(time.Millisecond), float64(count)/elapsed.Seconds())
}

// buildPayloads gives each embed task its own doc_id.
func buildPayloads(kind string, n, ms, linger int, raw string) ([]any, error) {
	out := make([]any, n)

	if raw != "" {
		var body any
		if err := json.Unmarshal([]byte(raw), &body); err != nil {
			return nil, fmt.Errorf("bad -payload: %w", err)
		}
		for i := range out {
			out[i] = body
		}
		return out, nil
	}

	for i := range out {
		if kind == "embed" {
			out[i] = map[string]int{"doc_id": i + 1, "ms": ms, "linger_ms": linger}
		} else {
			out[i] = map[string]int{"ms": ms}
		}
	}
	return out, nil
}

func setupLogging(format string, debug bool) {
	level := slog.LevelInfo
	if debug {
		level = slog.LevelDebug
	}
	opts := &slog.HandlerOptions{Level: level}

	var h slog.Handler = slog.NewTextHandler(os.Stderr, opts)
	if format == "json" {
		h = slog.NewJSONHandler(os.Stderr, opts)
	}
	slog.SetDefault(slog.New(h))
}

func hostID() string {
	host, err := os.Hostname()
	if err != nil {
		host = "worker"
	}
	return fmt.Sprintf("%s-%d", host, os.Getpid())
}
