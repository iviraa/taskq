// Command taskq runs workers, enqueues tasks, and inspects and benchmarks the queue.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/iviraa/taskq/internal/queue"
)

type command struct {
	name string
	help string
	run  func(args []string)
}

func main() {
	commands := []command{
		{"work", "run a pool of workers", work},
		{"produce", "enqueue tasks", produce},
		{"stats", "queue depth, worker lag and exact latency percentiles", stats},
		{"dlq", "inspect and requeue the dead-letter queue", dlq},
		{"bench", "drain throughput against pool width", bench},
		{"loadgen", "submit at a steady rate and report latency", loadgen},
	}

	if len(os.Args) < 2 {
		usage(commands)
	}
	for _, c := range commands {
		if c.name == os.Args[1] {
			c.run(os.Args[2:])
			return
		}
	}
	fmt.Fprintf(os.Stderr, "taskq: unknown command %q\n\n", os.Args[1])
	usage(commands)
}

func usage(commands []command) {
	fmt.Fprintln(os.Stderr, "usage: taskq <command> [flags]")
	for _, c := range commands {
		fmt.Fprintf(os.Stderr, "  %-8s %s\n", c.name, c.help)
	}
	fmt.Fprintf(os.Stderr, "\nDATABASE_URL overrides %s\n", queue.DefaultURL)
	os.Exit(2)
}

func flags(name string) *flag.FlagSet {
	return flag.NewFlagSet("taskq "+name, flag.ExitOnError)
}

func open(ctx context.Context, maxConns int32) *queue.Queue {
	q, err := queue.Open(ctx, maxConns)
	if err != nil {
		fatal(err)
	}
	return q
}

// signalContext is cancelled on Ctrl-C or SIGTERM; workers finish in-flight tasks.
func signalContext() (context.Context, func()) {
	return signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "taskq:", err)
	os.Exit(1)
}
