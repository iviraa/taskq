package queue

import (
	"encoding/json"
	"time"
)

// no failed state: a failure is either a retry or dead
type State string

const (
	StatePending   State = "pending"
	StateRunning   State = "running"
	StateSucceeded State = "succeeded"
	StateDead      State = "dead" // exhausted its attempts, a human has to look
)

type Task struct {
	ID        int64
	Kind      string
	Payload   json.RawMessage
	State     State
	CreatedAt time.Time

	// counted at claim time, so a crashing task still runs out
	Attempts    int
	MaxAttempts int

	// regenerated on every claim to fence stale workers
	LeaseToken string

	ClaimedBy  *string
	FinishedAt *time.Time
	LastError  *string
}
