package db

import (
	"context"
	"encoding/binary"
	"fmt"
	"strings"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

type NestedSetSource[N any] interface {
	DataAndChildren(depth int, parent *NestedSetNode[N]) (N, []NestedSetSource[N])
}

type ConstNestedSetSource[N any] struct {
	Data     N
	Children []NestedSetSource[N]
}

func (s ConstNestedSetSource[N]) DataAndChildren(_ int, _ *NestedSetNode[N]) (N, []NestedSetSource[N]) {
	return s.Data, s.Children
}

type NestedSetNode[N any] struct {
	lft    int64
	rgt    int64
	depth  int
	Data   N
	parent *NestedSetNode[N]
	id     any
}

func (n *NestedSetNode[N]) Depth() int {
	if n == nil {
		return -1
	}
	return n.depth
}

func (n *NestedSetNode[N]) IsRoot() bool {
	return n.parent == nil
}

func (n *NestedSetNode[N]) IsLeaf() bool {
	return n.rgt-n.lft == 1
}

func (n *NestedSetNode[N]) DescendantsCount() int64 {
	return (n.rgt - n.lft - 1) / 2
}

type NestedSet[N any] struct {
	records []*NestedSetNode[N]
	rgt     int64
}

func NewNestedSet[N any]() NestedSet[N] {
	return NestedSet[N]{nil, 1}
}

func (n *NestedSet[N]) reset() {
	n.records = nil
	n.rgt = 1
}

func (n *NestedSet[N]) appendNode(lft int64, depth int, parent *NestedSetNode[N], source NestedSetSource[N]) *NestedSetNode[N] {
	data, children := source.DataAndChildren(depth, parent)

	record := NestedSetNode[N]{
		lft:    lft,
		depth:  depth,
		Data:   data,
		parent: parent,
	}

	n.records = append(n.records, &record)

	nextLft := lft + 1
	for _, child := range children {
		rec := n.appendNode(nextLft, depth, parent, child)
		nextLft = rec.rgt + 1
	}
	record.rgt = nextLft

	return &record
}

func (n *NestedSet[N]) Append(nodes ...NestedSetSource[N]) {
	lft := n.rgt
	for _, node := range nodes {
		rec := n.appendNode(lft, 0, nil, node)
		lft = rec.rgt + 1
	}
	n.rgt = lft
}

type NestedSetSourceFn[N any] func(depth int, parent *NestedSetNode[N]) (data N, children []NestedSetSource[N])

func (fn NestedSetSourceFn[N]) DataAndChildren(depth int, parent *NestedSetNode[N]) (N, []NestedSetSource[N]) {
	return fn(depth, parent)
}

type NestedSetOpts struct {
	Table pgx.Identifier
	Scope []NestedSetScopeItem

	IdCol       string
	ParentIdCol string
	LftCol      string
	RgtCol      string
}

func NewNestedSetOpts(table pgx.Identifier, scope []NestedSetScopeItem) *NestedSetOpts {
	return &NestedSetOpts{
		Table: table,
		Scope: scope,

		IdCol:       "id",
		ParentIdCol: "parent_id",
		LftCol:      "_lft",
		RgtCol:      "_rgt",
	}
}

func (tx Tx) GetNestedSetMaxRgt(ctx context.Context, opts *NestedSetOpts) (int64, error) {
	where := ""
	args := make([]any, len(opts.Scope))
	if len(opts.Scope) > 0 {
		cases := make([]string, len(opts.Scope))
		for i, v := range opts.Scope {
			cases[i] = fmt.Sprintf("\"%s\" = $%d", v.ColumnName, i+1)
			args[i] = v.Value
		}

		where = "WHERE " + strings.Join(cases, " AND ")
	}

	query := fmt.Sprintf("SELECT COALESCE(max(_rgt),0) FROM %s %s", opts.Table.Sanitize(), where)

	var result int64
	err := tx.QueryRow(ctx, query, args...).Scan(&result)
	return result, err
}

func (tx Tx) InsertNestedSetInt64(
	ctx context.Context,
	idSequenceName pgx.Identifier,
	opts *NestedSetOpts,
	dataFields []string,
	ns *NestedSet[[]any],
) (ids []int64, err error) {
	seq := NewSequence(ctx, tx, idSequenceName)
	if seq.Err != nil {
		return nil, seq.Err
	}
	defer seq.Sync(ctx)

	nextId := func(_ int) int64 { return seq.Next() }
	return copyFromNestedSet(ctx, tx, nextId, opts, dataFields, ns)
}

func (tx Tx) MustInsertNestedSetInt64(
	ctx context.Context,
	idSequenceName pgx.Identifier,
	opts *NestedSetOpts,
	dataFields []string,
	ns *NestedSet[[]any],
) []int64 {
	result, err := tx.InsertNestedSetInt64(ctx, idSequenceName, opts, dataFields, ns)
	if err != nil {
		panic(err)
	}
	return result
}

func (tx Tx) InsertNestedSetUUID(
	ctx context.Context,
	idNamespace uuid.UUID,
	opts *NestedSetOpts,
	dataFields []string,
	ns *NestedSet[[]any],
) (ids []uuid.UUID, err error) {
	nextId := func(i int) uuid.UUID {
		bs := make([]byte, 4)
		binary.BigEndian.PutUint32(bs, uint32(i))
		return uuid.NewSHA1(idNamespace, bs)
	}
	return copyFromNestedSet(ctx, tx, nextId, opts, dataFields, ns)
}

func (tx Tx) MustInsertNestedSetUUID(
	ctx context.Context,
	idNamespace uuid.UUID,
	opts *NestedSetOpts,
	dataFields []string,
	ns *NestedSet[[]any],
) []uuid.UUID {
	result, err := tx.InsertNestedSetUUID(ctx, idNamespace, opts, dataFields, ns)
	if err != nil {
		panic(err)
	}
	return result
}

type NestedSetScopeItem struct {
	ColumnName string
	Value      any
}

func copyFromNestedSet[I int64 | uuid.UUID](
	ctx context.Context,
	tx Tx,
	nextId func(i int) I,
	opts *NestedSetOpts,
	dataFields []string,
	ns *NestedSet[[]any],
) ([]I, error) {
	// Get lft offset
	lftOffset, err := tx.GetNestedSetMaxRgt(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("Failed to get max _rgt: %v", err)
	}

	// Set ids
	ids := make([]I, len(ns.records))
	for i := range ids {
		id := nextId(i)
		ids[i] = id
		ns.records[i].id = id
	}

	// gather copy from info
	sn := len(opts.Scope)
	fieldCount := sn + 4 + len(dataFields)
	fields := make([]string, fieldCount)
	scopeArgs := make([]any, sn)

	for i, v := range opts.Scope {
		fields[i] = v.ColumnName
		scopeArgs[i] = v.Value
	}

	fields[sn] = opts.IdCol
	fields[sn+1] = opts.ParentIdCol
	fields[sn+2] = opts.LftCol
	fields[sn+3] = opts.RgtCol

	for j, v := range dataFields {
		fields[sn+4+j] = v
	}

	// Perform copy from
	_, err = tx.CopyFrom(ctx,
		opts.Table,
		fields,
		pgx.CopyFromSlice(len(ns.records), func(i int) ([]any, error) {
			rec := ns.records[i]

			args := make([]any, fieldCount)
			for j, a := range scopeArgs {
				args[j] = a
			}

			j := sn
			args[j] = rec.id
			if rec.parent != nil {
				args[j+1] = rec.parent.id
			} else {
				args[j+1] = nil
			}
			args[j+2] = rec.lft + lftOffset
			args[j+3] = rec.rgt + lftOffset
			j = j + 4

			for k, v := range rec.Data {
				args[j+k] = v
			}

			return args, nil
		}),
	)

	if err != nil {
		return ids, err
	}

	return ids, nil
}
