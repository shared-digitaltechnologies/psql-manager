package db

import "github.com/jackc/pgx/v5"

type Conn struct{ *pgx.Conn }
