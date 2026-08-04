package worker

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"
	"time"

	"github.com/iviraa/taskq/internal/queue"
)

// Handler runs one task; an error marks the attempt failed.
type Handler func(ctx context.Context, t *queue.Task) error

// Registry maps task kind to handler; handlers run concurrently.
type Registry map[string]Handler

func (r Registry) Handle(ctx context.Context, t *queue.Task) error {
	h, ok := r[t.Kind]
	if !ok {
		return fmt.Errorf("unknown kind %q", t.Kind)
	}
	return h(ctx, t)
}

// Default excludes the fault-injection handlers.
func Default(pool *pgxpool.Pool) Registry {
	return Registry{
		"embed": Embed(pool),
		"sleep": Sleep,
	}
}

// Sleep simulates slow work.
func Sleep(ctx context.Context, t *queue.Task) error {
	var args struct {
		MS int `json:"ms"`
	}
	if err := json.Unmarshal(t.Payload, &args); err != nil {
		return fmt.Errorf("bad sleep payload: %w", err)
	}
	if args.MS < 0 {
		return fmt.Errorf("bad sleep payload: ms is %d", args.MS)
	}
	if args.MS == 0 {
		return nil
	}

	select {
	case <-time.After(time.Duration(args.MS) * time.Millisecond):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
