package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lib/pq"
)

type Record struct {
	MsgID      int
	EnqueuedAt time.Time
	VisibleAt  time.Time
	Message    json.RawMessage
}

type Broker struct {
	db     *sql.DB
	cursor time.Time
}

func NewBroker(db *sql.DB) *Broker {
	return &Broker{
		db: db,
	}
}

func (b *Broker) SetCursor(cursor time.Time) {
	b.cursor = cursor
}

func (b *Broker) GetCursor() time.Time {
	return b.cursor
}

func (b *Broker) Produce(ctx context.Context, messages []json.RawMessage, delay time.Duration) (records []Record, err error) {
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return
	}
	defer func() {
		if err != nil {
			_ = tx.Rollback()
			return
		}
		_ = tx.Commit()
	}()

	msgs := make([]string, len(messages))
	for i, msg := range messages {
		msgs[i] = string(msg)
	}

	query := fmt.Sprintf("INSERT INTO queue (vt, message) SELECT clock_timestamp() + interval '%d milliseconds', unnest($1::jsonb[]) RETURNING *", delay.Milliseconds())
	rows, err := tx.QueryContext(ctx, query, pq.Array(msgs))
	if err != nil {
		return
	}
	defer rows.Close()

	records = make([]Record, 0, len(messages))
	for rows.Next() {
		var record Record
		if err = rows.Scan(&record.MsgID, &record.EnqueuedAt, &record.VisibleAt, &record.Message); err != nil {
			return
		}
		records = append(records, record)
	}

	return
}

func (b *Broker) Consume(ctx context.Context, size int) ([]Record, error) {
	query := fmt.Sprintf("SELECT msg_id, enqueued_at, vt, message FROM queue WHERE $1 < vt AND vt <= clock_timestamp() ORDER BY vt, msg_id LIMIT %d", size)
	rows, err := b.db.QueryContext(ctx, query, b.cursor)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var newCursor time.Time
	var records []Record
	for rows.Next() {
		var record Record
		if err := rows.Scan(&record.MsgID, &record.EnqueuedAt, &record.VisibleAt, &record.Message); err != nil {
			return nil, err
		}
		records = append(records, record)
		if record.VisibleAt.After(newCursor) {
			newCursor = record.VisibleAt
		}
	}
	if newCursor.After(b.cursor) {
		b.cursor = newCursor
	}
	return records, nil
}
