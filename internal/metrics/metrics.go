// Package metrics serves counters and gauges in Prometheus text format.
package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/iviraa/taskq/internal/queue"
)

type Counter struct{ n atomic.Int64 }
type Gauge struct{ bits atomic.Uint64 }

func (c *Counter) Inc()          { c.n.Add(1) }
func (c *Counter) read() float64 { return float64(c.n.Load()) }
func (g *Gauge) Set(v float64)   { g.bits.Store(math.Float64bits(v)) }
func (g *Gauge) read() float64   { return math.Float64frombits(g.bits.Load()) }

type metric struct {
	read func() float64
	kind string
	help string
}

// only written during package init
var registry = map[string]metric{}

func counter(name, help string) *Counter {
	c := &Counter{}
	registry[name] = metric{c.read, "counter", help}
	return c
}

func gauge(name, help string) *Gauge {
	g := &Gauge{}
	registry[name] = metric{g.read, "gauge", help}
	return g
}

var (
	Succeeded  = counter("taskq_tasks_succeeded_total", "Attempts that completed.")
	Retried    = counter("taskq_retries_total", "Failed attempts requeued with a backoff.")
	Dead       = counter("taskq_tasks_dead_total", "Tasks that exhausted their attempts.")
	LeasesLost = counter("taskq_leases_lost_total", "Attempts abandoned because the lease expired mid-task.")
	Duplicates = counter("taskq_duplicate_side_effects_suppressed_total",
		"Deliveries that found their work already done and skipped it.")

	Depth     = gauge("taskq_queue_depth", "Tasks not yet finished and still allowed to run.")
	Ready     = gauge("taskq_queue_ready", "Tasks claimable now, excluding ones in a backoff.")
	InFlight  = gauge("taskq_tasks_in_flight", "Tasks running under a live lease.")
	Lag       = gauge("taskq_worker_lag_seconds", "How long the oldest claimable task has been waiting.")
	DeadDepth = gauge("taskq_dead_letter_depth", "Tasks waiting in the dead-letter queue.")
)

// Serve exposes /metrics and blocks.
func Serve(addr string) {
	mux := http.NewServeMux()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4")
		fmt.Fprint(w, render())
	})

	srv := &http.Server{Addr: addr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		slog.Error("metrics server", "err", err)
	}
}

// sorted so scrapes diff cleanly
func render() string {
	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	sort.Strings(names)

	var b strings.Builder
	for _, name := range names {
		m := registry[name]
		fmt.Fprintf(&b, "# HELP %s %s\n# TYPE %s %s\n%s %g\n",
			name, m.help, name, m.kind, name, m.read())
	}
	return b.String()
}

// Poll refreshes the gauges from the database.
func Poll(ctx context.Context, q *queue.Queue, every time.Duration) {
	for {
		s, err := q.Stat(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("reading queue stats", "err", err)
		} else {
			Depth.Set(float64(s.Depth))
			Ready.Set(float64(s.Ready))
			InFlight.Set(float64(s.InFlight))
			DeadDepth.Set(float64(s.Dead))
			Lag.Set(s.OldestReady.Seconds())
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(every):
		}
	}
}
