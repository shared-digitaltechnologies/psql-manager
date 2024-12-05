package db

import (
	"context"
	"fmt"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type Tx struct{ pgx.Tx }

// MUST COMMANDS
func (tx Tx) MustExec(ctx context.Context, sql string, arguments ...any) pgconn.CommandTag {
	c, err := tx.Exec(ctx, sql, arguments...)
	if err != nil {
		panic(fmt.Errorf("ERROR WHILE EXECUTING\n\n%s\n\n%v\n\n", sql, err))
	}
	return c
}
func (tx Tx) MustQuery(ctx context.Context, sql string, arguments ...any) pgx.Rows {
	rows, err := tx.Query(ctx, sql, arguments...)
	if err != nil {
		panic(fmt.Errorf("ERROR WHILE EXECUTING\n\n%s\n\n%v\n\n", sql, err))
	}
	return rows
}

func (tx Tx) MustCopyFrom(ctx context.Context, table pgx.Identifier, colums []string, source pgx.CopyFromSource) int64 {
	res, err := tx.CopyFrom(ctx, table, colums, source)
	if err != nil {
		panic(fmt.Errorf("ERROR WHILE COPYING %v TO TABLE %v\n\n%v\n\n", colums, table, err))
	}
	return res
}

// Query columns
func (tx Tx) QueryColInt64(ctx context.Context, sql string, arguments ...any) ([]int64, error) {
	return queryColumn[int64](ctx, tx, sql, arguments...)
}
func (tx Tx) MustQueryColInt64(ctx context.Context, sql string, arguments ...any) []int64 {
	res, err := queryColumn[int64](ctx, tx, sql, arguments...)
	if err != nil {
		panic(fmt.Errorf("ERROR WHILE QUERYING COLUMN\n\n%s\n\n%v\n\n", sql, err))
	}
	return res
}

func (tx Tx) QueryColInt(ctx context.Context, sql string, arguments ...any) ([]int, error) {
	return queryColumn[int](ctx, tx, sql, arguments...)
}
func (tx Tx) MustQueryColInt(ctx context.Context, sql string, arguments ...any) []int {
	res, err := queryColumn[int](ctx, tx, sql, arguments...)
	if err != nil {
		panic(fmt.Errorf("ERROR WHILE QUERYING COLUMN\n\n%s\n\n%v\n\n", sql, err))
	}
	return res
}

func (tx Tx) QueryColString(ctx context.Context, sql string, arguments ...any) ([]string, error) {
	return queryColumn[string](ctx, tx, sql, arguments...)
}
func (tx Tx) MustQueryColString(ctx context.Context, sql string, arguments ...any) []string {
	res, err := queryColumn[string](ctx, tx, sql, arguments...)
	if err != nil {
		panic(fmt.Errorf("ERROR WHILE QUERYING COLUMN\n\n%s\n\n%v\n\n", sql, err))
	}
	return res
}

func (tx Tx) QueryColUUID(ctx context.Context, sql string, arguments ...any) ([]uuid.UUID, error) {
	return queryColumn[uuid.UUID](ctx, tx, sql, arguments...)
}
func (tx Tx) MustQueryColUUID(ctx context.Context, sql string, arguments ...any) []uuid.UUID {
	res, err := queryColumn[uuid.UUID](ctx, tx, sql, arguments...)
	if err != nil {
		panic(fmt.Errorf("ERROR WHILE QUERYING COLUMN\n\n%s\n\n%v\n\n", sql, err))
	}
	return res
}

func queryColumn[T any](ctx context.Context, tx Tx, sql string, arguments ...any) (res []T, err error) {
	rows, err := tx.Query(ctx, sql, arguments...)
	if err != nil {
		return
	}

	for rows.Next() {
		var val T
		err = rows.Scan(&val)
		if err != nil {
			return
		}
		res = append(res, val)
	}
	return
}
