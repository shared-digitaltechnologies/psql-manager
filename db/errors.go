package db

import (
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5/pgconn"
)

func ErrWithPgRowCol(err error, filename string, query string) error {
	if err == nil {
		return err
	}

	var pgErr *pgconn.PgError
	if errors.As(err, &pgErr) {
		pos := pgErr.Position
		if pos > 0 {
			row, col := posToRowCol(query, pos)
			return fmt.Errorf("%w @%s:%d:%d", err, filename, row, col)
		}
	}

	return err
}

func posToRowCol(query string, pos int32) (row int, col int) {
	row = 1
	col = 1
	p := int(pos)
	for i, char := range query {
		if i >= p {
			return
		}

		if char == '\n' {
			col = 1
			row += 1
		} else {
			col += 1
		}
	}
	return
}
