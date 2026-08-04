package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/iviraa/taskq/internal/queue"
	"github.com/iviraa/taskq/internal/worker"
)

// bench measures drain throughput per pool width, shuffling runs and reporting medians.
func bench(args []string) {
	fs := flags("bench")
	tasks := fs.Int("tasks", 20000, "tasks per run")
	widths := fs.String("workers", "1,2,4,8,12,16,24,32", "pool widths to compare")
	repeats := fs.Int("repeats", 5, "runs per width")
	ms := fs.Int("ms", 0, "work per task, milliseconds")
	fs.Parse(args)

	ctx, cancel := signalContext()
	defer cancel()

	parsed, err := parseWidths(*widths)
	if err != nil {
		fatal(err)
	}

	q := open(ctx, int32(slices.Max(parsed))+2)
	defer q.Close()

	type run struct {
		width int
		rate  float64
	}
	var plan []run
	for _, w := range parsed {
		for r := 0; r < *repeats; r++ {
			plan = append(plan, run{width: w})
		}
	}
	rand.Shuffle(len(plan), func(i, j int) { plan[i], plan[j] = plan[j], plan[i] })

	fmt.Printf("%d tasks, %dms each, widths %v, %d repeats, %d runs in shuffled order\n",
		*tasks, *ms, parsed, *repeats, len(plan))

	for i := range plan {
		if ctx.Err() != nil {
			fatal(fmt.Errorf("interrupted"))
		}
		rate, err := drainOnce(ctx, q, plan[i].width, *tasks, *ms)
		if err != nil {
			fatal(err)
		}
		plan[i].rate = rate
		fmt.Printf("  [%2d/%d] width=%-3d %.0f tasks/sec\n", i+1, len(plan), plan[i].width, rate)
	}

	fmt.Printf("\n%-9s %-10s %-10s %-10s %s\n", "workers", "median", "min", "max", "spread")
	for _, w := range parsed {
		var rates []float64
		for _, r := range plan {
			if r.width == w {
				rates = append(rates, r.rate)
			}
		}
		sort.Float64s(rates)
		lo, hi := rates[0], rates[len(rates)-1]
		fmt.Printf("%-9d %-10.0f %-10.0f %-10.0f %.2fx\n", w, rates[len(rates)/2], lo, hi, hi/lo)
	}
}

func drainOnce(ctx context.Context, q *queue.Queue, width, tasks, ms int) (float64, error) {
	if err := q.Reset(ctx); err != nil {
		return 0, err
	}
	payloads := make([]any, tasks)
	for i := range payloads {
		payloads[i] = map[string]int{"ms": ms}
	}
	if _, err := q.EnqueueBatch(ctx, "sleep", payloads); err != nil {
		return 0, err
	}

	p := worker.NewPool(q, worker.Sleep, worker.Config{
		ID: "bench", Size: width, Poll: 10 * time.Millisecond,
		Lease: 30 * time.Second, Drain: true,
	})

	start := time.Now()
	p.Run(ctx)
	elapsed := time.Since(start)

	audit, err := q.Stat(ctx)
	if err != nil {
		return 0, err
	}
	if !audit.Complete() {
		return 0, fmt.Errorf("audit failed at width %d: %+v", width, audit)
	}
	return float64(tasks) / elapsed.Seconds(), nil
}

// loadgen enqueues at a steady rate and reports end-to-end latency.
func loadgen(args []string) {
	const tick = 10 * time.Millisecond

	fs := flags("loadgen")
	rate := fs.Int("rate", 500, "tasks submitted per second")
	duration := fs.Duration("duration", 30*time.Second, "how long to submit for")
	workers := fs.Int("workers", 16, "goroutines in the pool")
	ms := fs.Int("ms", 5, "work per task, milliseconds")
	fs.Parse(args)

	ctx, cancel := signalContext()
	defer cancel()

	q := open(ctx, int32(*workers)+4)
	defer q.Close()

	if err := q.Reset(ctx); err != nil {
		fatal(err)
	}

	// not drain mode: the queue empties between arrivals
	p := worker.NewPool(q, worker.Default(q.Pool()).Handle, worker.Config{
		ID: "loadgen", Size: *workers, Poll: 5 * time.Millisecond, Lease: 30 * time.Second,
	})

	runCtx, stopWorkers := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() {
		defer close(done)
		p.Run(runCtx)
	}()

	// small batches per tick to keep round trips out of the measurement
	perTick := max(1, *rate*int(tick)/int(time.Second))
	payloads := make([]any, perTick)
	deadline := time.Now().Add(*duration)
	docID, sent := 0, 0

	for time.Now().Before(deadline) && ctx.Err() == nil {
		time.Sleep(tick)
		for i := range payloads {
			docID++
			payloads[i] = map[string]int{"doc_id": docID, "ms": *ms}
		}
		if _, err := q.EnqueueBatch(ctx, "embed", payloads); err != nil {
			if ctx.Err() != nil {
				break
			}
			fatal(err)
		}
		sent += perTick
	}
	fmt.Printf("submitted %d tasks at a target of %d/sec, waiting for drain\n", sent, *rate)

	for ctx.Err() == nil {
		n, err := q.Remaining(ctx)
		if err != nil || n == 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	stopWorkers()
	<-done

	lat, err := q.LatencyStats(ctx)
	if err != nil {
		fatal(err)
	}
	ms2 := func(d time.Duration) string { return d.Round(time.Millisecond).String() }
	fmt.Printf("%d workers, %dms each, target %d/sec\n", *workers, *ms, *rate)
	fmt.Printf("end-to-end latency n=%d p50=%s p95=%s p99=%s max=%s\n",
		lat.Count, ms2(lat.P50), ms2(lat.P95), ms2(lat.P99), ms2(lat.Max))
}

func parseWidths(s string) ([]int, error) {
	var out []int
	for _, f := range strings.Split(s, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(f))
		if err != nil || n < 1 {
			return nil, fmt.Errorf("bad worker count %q", f)
		}
		out = append(out, n)
	}
	return out, nil
}
