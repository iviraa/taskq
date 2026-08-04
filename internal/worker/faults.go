package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/iviraa/taskq/internal/queue"
)

// Fault injection for tests and demos; only registered through WithFaults.

func WithFaults(r Registry) Registry {
	r["flaky"] = Flaky
	r["poison"] = Poison
	r["crash"] = Crash
	return r
}

// Flaky fails until it has been attempted fail_until times.
func Flaky(ctx context.Context, t *queue.Task) error {
	var args struct {
		FailUntil int `json:"fail_until"`
	}
	if err := json.Unmarshal(t.Payload, &args); err != nil {
		return fmt.Errorf("bad flaky payload: %w", err)
	}
	if t.Attempts <= args.FailUntil {
		return fmt.Errorf("transient failure on attempt %d", t.Attempts)
	}
	return nil
}

// Poison always fails.
func Poison(ctx context.Context, t *queue.Task) error {
	return fmt.Errorf("poison task, attempt %d of %d", t.Attempts, t.MaxAttempts)
}

// Crash exits the process without reporting anything.
func Crash(ctx context.Context, t *queue.Task) error {
	os.Exit(3)
	return nil
}
