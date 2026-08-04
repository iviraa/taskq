package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"text/tabwriter"
	"time"

	"github.com/iviraa/taskq/internal/queue"
)

func stats(args []string) {
	fs := flags("stats")
	watch := fs.Duration("watch", 0, "repeat at this interval instead of printing once")
	fs.Parse(args)

	ctx, cancel := signalContext()
	defer cancel()

	q := open(ctx, 2)
	defer q.Close()

	for ctx.Err() == nil {
		if err := printStats(ctx, q); err != nil {
			fatal(err)
		}
		if *watch <= 0 {
			return
		}
		select {
		case <-ctx.Done():
		case <-time.After(*watch):
			fmt.Println()
		}
	}
}

func printStats(ctx context.Context, q *queue.Queue) error {
	s, err := q.Stat(ctx)
	if err != nil {
		return err
	}
	lat, err := q.LatencyStats(ctx)
	if err != nil {
		return err
	}
	effects, err := q.SideEffects(ctx)
	if err != nil {
		return err
	}

	ms := func(d time.Duration) string { return d.Round(time.Millisecond).String() }

	fmt.Printf("queue     depth=%d ready=%d in_flight=%d dead=%d\n",
		s.Depth, s.Ready, s.InFlight, s.Dead)
	fmt.Printf("lag       oldest ready task waiting %s\n", ms(s.OldestReady))
	fmt.Printf("tasks     total=%d succeeded=%d dead=%d unfinished=%d redelivered=%d\n",
		s.Total, s.Succeeded, s.Dead, s.Unfinished, s.Redelivered)
	fmt.Printf("latency   n=%d p50=%s p95=%s p99=%s max=%s\n",
		lat.Count, ms(lat.P50), ms(lat.P95), ms(lat.P99), ms(lat.Max))
	fmt.Printf("effects   deliveries=%d distinct_docs=%d documents=%d duplicates_absorbed=%d\n",
		effects.Deliveries, effects.DistinctDocs, effects.Documents, effects.Duplicates)
	return nil
}

func dlq(args []string) {
	fs := flags("dlq")
	limit := fs.Int("limit", 50, "rows to list")
	fs.Parse(args)

	ctx, cancel := signalContext()
	defer cancel()

	q := open(ctx, 2)
	defer q.Close()

	switch fs.Arg(0) {
	case "", "list":
		listDead(ctx, q, *limit)
	case "requeue":
		requeueDead(ctx, q, fs.Arg(1))
	default:
		fatal(fmt.Errorf("unknown dlq command %q, want list or requeue", fs.Arg(0)))
	}
}

func listDead(ctx context.Context, q *queue.Queue, limit int) {
	// mark exhausted tasks dead first so they show up
	if _, err := q.SweepDead(ctx); err != nil {
		fatal(err)
	}

	dead, err := q.Dead(ctx, limit)
	if err != nil {
		fatal(err)
	}
	if len(dead) == 0 {
		fmt.Println("dead-letter queue is empty")
		return
	}

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tKIND\tATTEMPTS\tPAYLOAD\tLAST ERROR")
	for _, t := range dead {
		last := ""
		if t.LastError != nil {
			last = *t.LastError
		}
		fmt.Fprintf(w, "%d\t%s\t%d/%d\t%s\t%s\n",
			t.ID, t.Kind, t.Attempts, t.MaxAttempts, t.Payload, last)
	}
	w.Flush()
	fmt.Printf("\n%d dead tasks\n", len(dead))
}

// requeueDead resets the attempt budget; no id requeues everything.
func requeueDead(ctx context.Context, q *queue.Queue, arg string) {
	var id int64
	if arg != "" {
		parsed, err := strconv.ParseInt(arg, 10, 64)
		if err != nil {
			fatal(fmt.Errorf("bad id %q: %w", arg, err))
		}
		id = parsed
	}

	n, err := q.Requeue(ctx, id)
	if err != nil {
		fatal(err)
	}
	fmt.Printf("requeued %d tasks with a fresh attempt budget\n", n)
}
