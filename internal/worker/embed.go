package worker

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/iviraa/taskq/internal/metrics"

	"github.com/iviraa/taskq/internal/queue"
)

// SuppressedDuplicates counts deliveries the idempotent insert ignored.
var SuppressedDuplicates atomic.Int64

// Embed writes one document per doc_id, so a second delivery is a no-op.
func Embed(pool *pgxpool.Pool) Handler {
	return func(ctx context.Context, t *queue.Task) error {
		var args struct {
			DocID int64 `json:"doc_id"`
			MS    int   `json:"ms"`
			// widens the gap between the write and the ack for chaos tests
			LingerMS int `json:"linger_ms"`
		}
		if err := json.Unmarshal(t.Payload, &args); err != nil {
			return fmt.Errorf("bad embed payload: %w", err)
		}
		if args.DocID <= 0 {
			return fmt.Errorf("bad embed payload: doc_id is %d", args.DocID)
		}

		embedding, err := computeEmbedding(ctx, args.DocID, args.MS)
		if err != nil {
			return err
		}

		// unconditional, so deliveries can be counted
		if _, err := pool.Exec(ctx,
			`insert into documents_naive (doc_id, task_id, attempt) values ($1, $2, $3)`,
			args.DocID, t.ID, t.Attempts); err != nil {
			return fmt.Errorf("record delivery: %w", err)
		}

		tag, err := pool.Exec(ctx, `
            insert into documents (doc_id, embedding, task_id, attempt)
            values ($1, $2, $3, $4)
            on conflict (doc_id) do nothing`,
			args.DocID, embedding, t.ID, t.Attempts)
		if err != nil {
			return fmt.Errorf("store embedding for doc %d: %w", args.DocID, err)
		}
		if tag.RowsAffected() == 0 {
			SuppressedDuplicates.Add(1)
			metrics.Duplicates.Inc()
		}

		if args.LingerMS > 0 {
			select {
			case <-time.After(time.Duration(args.LingerMS) * time.Millisecond):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
}

// computeEmbedding fakes the model call, deterministic in doc_id.
func computeEmbedding(ctx context.Context, docID int64, ms int) ([]byte, error) {
	if ms > 0 {
		select {
		case <-time.After(time.Duration(ms) * time.Millisecond):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], uint64(docID))
	sum := sha256.Sum256(buf[:])
	return sum[:], nil
}
