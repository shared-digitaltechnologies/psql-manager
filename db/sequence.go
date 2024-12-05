package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

type Sequence struct {
	tx pgx.Tx
	id pgx.Identifier

	lastValue   int64
	incrementBy int64

	called bool
	dirty  bool

	Err error
}

func (seq *Sequence) StateString() string {
	var ann string
	if seq.Err != nil {
		ann = fmt.Sprintf("(Err: %v)", seq.Err)
	} else if !seq.called {
		ann = "(-1)"
	} else if seq.dirty {
		ann = "(dirty)"
	} else {
		ann = "(synced)"
	}

	return fmt.Sprintf("%d%s", seq.lastValue, ann)
}

func (seq *Sequence) String() string {
	return strings.Join(seq.id, ".")
}

func (seq *Sequence) Id() (schema *string, name string) {
	n := len(seq.id)
	if n == 0 {
		panic("Sequence name empty")
	} else if n == 1 {
		return nil, seq.id[0]
	} else if n == 2 {
		schema := seq.id[0]
		return &schema, seq.id[1]
	} else {
		panic("Sequence name has to many (>2) parts")
	}
}

func (seq *Sequence) Name() string {
	_, name := seq.Id()
	return name
}

func (seq *Sequence) Schema() *string {
	schema, _ := seq.Id()
	return schema
}

func (seq *Sequence) Peek() int64 {
	if seq.called {
		return seq.lastValue + seq.incrementBy
	} else {
		return seq.lastValue
	}
}

func (seq *Sequence) PeekN(n int) []int64 {
	res := make([]int64, n)
	next := seq.Peek()
	for i := 0; i < n; i++ {
		res[i] = next
		next += seq.incrementBy
	}
	return res
}

func (seq *Sequence) TryNext() (next int64, err error) {
	next = seq.Peek()

	if seq.Err != nil {
		err = seq.Err
		return
	}

	err = seq.SetLast(next)
	return
}

func (seq *Sequence) Next() int64 {
	next, err := seq.TryNext()
	if err != nil {
		panic(err)
	}
	return next
}

func (seq *Sequence) NextN(n int) []int64 {
	res := make([]int64, n)
	for i := 0; i < n; i++ {
		res[i] = seq.Next()
	}
	return res
}

func (seq *Sequence) Last() (int64, bool) {
	return seq.lastValue, seq.called
}

func (seq *Sequence) SetLast(value int64) error {
	if seq.Err != nil {
		return seq.Err
	}
	seq.dirty = !seq.called || seq.lastValue != value
	seq.called = true
	seq.lastValue = value
	return nil
}

func (seq *Sequence) update(ctx context.Context) (err error) {
	if seq.dirty {
		_, err := seq.tx.Exec(ctx, "SELECT setval(($1)::regclass,$2,$3)", seq.String(), seq.lastValue, seq.called)
		if err != nil {
			seq.Err = err
			return err
		}

		seq.dirty = false
	}

	return nil
}

func (seq *Sequence) Reload(ctx context.Context) (err error) {
	schema, name := seq.Id()

	var args []any
	query := `SELECT start_value, last_value, increment_by
FROM pg_catalog.pg_sequences
WHERE sequencename = $1`
	if schema == nil {
		query = query + " AND schemaname = $2"
		args = []any{name, schema}
	} else {
		args = []any{name}
	}

	var startValue int64
	var lastValue *int64

	err = seq.tx.QueryRow(ctx, query, args...).Scan(&startValue, &lastValue, &seq.incrementBy)
	if err != nil {
		seq.Err = fmt.Errorf("Failed to load state of sequence %s: %v", seq, err)
		return
	}

	if lastValue == nil {
		seq.lastValue = startValue
		seq.called = false
	} else {
		seq.lastValue = *lastValue
		seq.called = true
	}
	seq.dirty = false
	return
}

func (seq *Sequence) MustReload(ctx context.Context) {
	err := seq.Reload(ctx)
	if err != nil {
		panic(err)
	}
}

func (seq *Sequence) Sync(ctx context.Context) error {
	if seq.dirty {
		return seq.update(ctx)
	} else {
		return seq.Reload(ctx)
	}
}

func (seq *Sequence) MustSync(ctx context.Context) {
	err := seq.Sync(ctx)
	if err != nil {
		panic(err)
	}
}

func NewSequence(ctx context.Context, tx pgx.Tx, identifier pgx.Identifier) *Sequence {
	seq := Sequence{
		tx:          tx,
		id:          identifier,
		dirty:       false,
		incrementBy: 1,
		lastValue:   1,
		called:      false,
	}
	seq.Reload(ctx)
	return &seq
}
