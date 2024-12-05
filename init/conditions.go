package psqlinit

import (
	"context"
	"fmt"
	"math/bits"
	"strings"

	"github.com/jackc/pgx/v5"
)

type Condition interface {
	Description() string
	Evaluate(ctx context.Context, conn *pgx.Conn) (bool, error)
}

// FN Condition
type fnCondition struct {
	description string
	impl        func(ctx context.Context, conn *pgx.Conn) (bool, error)
}

func (v *fnCondition) Description() string {
	return "[CUSTOM] " + v.description
}

func (v *fnCondition) Evaluate(ctx context.Context, conn *pgx.Conn) (bool, error) {
	return v.impl(ctx, conn)
}

func CustomCond(description string, eval func(ctx context.Context, conn *pgx.Conn) (bool, error)) Condition {
	return &fnCondition{
		description: description,
		impl:        eval,
	}
}

// Schemas exist condition
type schemaExistsCond struct {
	schemas []string
}

func SchemaExistsCond(schemas ...string) Condition {
	if len(schemas) == 0 {
		panic("Schemas missing condition needs at least one schema!")
	}

	return &schemaExistsCond{schemas}
}

func (v *schemaExistsCond) Description() string {
	if len(v.schemas) == 0 {
		panic("SchemaExists condition needs at least one schema!")
	}

	if len(v.schemas) == 1 {
		return fmt.Sprintf("Schema \"%s\" exists", v.schemas[0])
	}

	schemasStr := strings.Join(v.schemas, "\", \"")

	return fmt.Sprintf("One of schemas \"%s\" exists", schemasStr)
}

func (v *schemaExistsCond) Evaluate(ctx context.Context, conn *pgx.Conn) (bool, error) {
	if len(v.schemas) == 0 {
		panic("SchemaExists condition needs at least one schema!")
	}

	var res bool
	err := conn.QueryRow(ctx, `
SELECT EXISTS(
  SELECT
  FROM pg_catalog.pg_namespace
  WHERE nspname = ANY($1)
)
`, v.schemas).Scan(&res)

	return res, err
}

// Tables exists
type relExistsCond struct {
	relnamespace string
	relname      string
	relkind      RelCond
}

func RelExistsCond(name string, kind RelCond) Condition {
	res := relExistsCond{relkind: kind}

	parts := strings.Split(name, ".")
	if len(parts) > 2 {
		panic("Relation name with more than 2 parts!")
	}

	if len(parts) == 2 {
		res.relnamespace = parts[0]
		res.relname = parts[1]
	} else {
		res.relname = parts[0]
	}

	return &res
}

func TableExistsCond(name string) Condition {
	return RelExistsCond(name, ANY_TABLE)
}

func ViewExistsCond(name string) Condition {
	return RelExistsCond(name, ANY_VIEW)
}

func TableOrViewExistsCond(name string) Condition {
	return RelExistsCond(name, ANY_TABLE|ANY_VIEW)
}

func SequenceExistsCond(name string) Condition {
	return RelExistsCond(name, SEQUENCE)
}

func IndexExistsCond(name string) Condition {
	return RelExistsCond(name, INDEX)
}

func (r *relExistsCond) fqRelName() string {
	if len(r.relnamespace) > 0 {
		return r.relnamespace + "." + r.relname
	} else {
		return r.relname
	}
}

func (v *relExistsCond) Description() string {
	return fmt.Sprintf("Relation \"%s\" of kind %s exists", v.fqRelName(), v.relkind)
}

func (v *relExistsCond) Evaluate(ctx context.Context, conn *pgx.Conn) (bool, error) {
	hasNamespace := len(v.relnamespace) > 0

	var args []any
	query := `SELECT FROM pg_catalog.pg_class c`
	if hasNamespace {
		query += ` LEFT JOIN pg_catalog.pg_namespace n ON c.relnamespace = n.oid
WHERE n.nspname=$1 AND c.relname=$2 AND c.relkind = ANY($3)`
		args = []any{v.relnamespace, v.relname, v.relkind.ToKinds()}
	} else {
		query += ` WHERE c.relname=$1 AND c.relkind = ANY($2)`
		args = []any{v.relname, v.relkind.ToKinds()}
	}

	var res bool
	err := conn.QueryRow(ctx, "SELECT EXISTS ("+query+")", args...).Scan(&res)
	return res, err
}

type RelCond uint16

const (
	TABLE RelCond = 1 << iota
	INDEX
	SEQUENCE
	TOAST_TABLE
	VIEW
	MATERIALIZED_VIEW
	COMPOSITE_TYPE
	FOREIGN_TABLE
	PARTITIONED_TABLE
	PARTITIONED_INDEX
)

const (
	ANY       RelCond = TABLE | INDEX | SEQUENCE | TOAST_TABLE | VIEW | MATERIALIZED_VIEW | COMPOSITE_TYPE | FOREIGN_TABLE | PARTITIONED_TABLE | PARTITIONED_INDEX
	ANY_INDEX RelCond = INDEX | PARTITIONED_INDEX
	ANY_TABLE RelCond = TABLE | FOREIGN_TABLE | PARTITIONED_TABLE
	ANY_VIEW  RelCond = VIEW | MATERIALIZED_VIEW
)

func (c RelCond) ToKinds() []RelKind {
	count := bits.OnesCount16(uint16(c & ANY))
	res := make([]RelKind, count)

	i := 0
	add := func(cond RelCond, kind RelKind) {
		if c&cond > 0 {
			res[i] = kind
			i++
		}
	}

	add(TABLE, RELKIND_TABLE)
	add(INDEX, RELKIND_INDEX)
	add(SEQUENCE, RELKIND_SEQUENCE)
	add(TOAST_TABLE, RELKIND_TOAST_TABLE)
	add(VIEW, RELKIND_VIEW)
	add(MATERIALIZED_VIEW, RELKIND_MATERIALIZED_VIEW)
	add(COMPOSITE_TYPE, RELKIND_COMPOSITE_TYPE)
	add(FOREIGN_TABLE, RELKIND_FOREIGN_TABLE)
	add(PARTITIONED_TABLE, RELKIND_PARTITIONED_TABLE)
	add(PARTITIONED_INDEX, RELKIND_PARTITIONED_INDEX)

	return res
}

func (c RelCond) String() string {
	x := c & ANY
	if x == 0 {
		return "Nothing"
	}

	if x == ANY {
		return "Any"
	}

	res := make([]string, 0, 10)
	if x&ANY_TABLE > 0 {
		res = append(res, "AnyTable")
		x = x & (^ANY_TABLE)
	}

	if x&ANY_INDEX > 0 {
		res = append(res, "AnyIndex")
		x = x & (^ANY_INDEX)
	}

	if x&ANY_VIEW > 0 {
		res = append(res, "AnyView")
		x = x & (^ANY_INDEX)
	}

	for _, k := range x.ToKinds() {
		res = append(res, k.String())
	}

	return strings.Join(res, "|")
}

type RelKind uint8

const (
	RELKIND_TABLE             RelKind = 'r'
	RELKIND_INDEX             RelKind = 'i'
	RELKIND_SEQUENCE          RelKind = 'S'
	RELKIND_TOAST_TABLE       RelKind = 't'
	RELKIND_VIEW              RelKind = 'v'
	RELKIND_MATERIALIZED_VIEW RelKind = 'm'
	RELKIND_COMPOSITE_TYPE    RelKind = 'c'
	RELKIND_FOREIGN_TABLE     RelKind = 'f'
	RELKIND_PARTITIONED_TABLE RelKind = 'p'
	RELKIND_PARTITIONED_INDEX RelKind = 'I'
)

func (k RelKind) String() string {
	switch k {
	case RELKIND_TABLE:
		return "Table"
	case RELKIND_INDEX:
		return "Index"
	case RELKIND_SEQUENCE:
		return "Sequence"
	case RELKIND_TOAST_TABLE:
		return "ToastTable"
	case RELKIND_VIEW:
		return "View"
	case RELKIND_MATERIALIZED_VIEW:
		return "MaterializedView"
	case RELKIND_COMPOSITE_TYPE:
		return "CompositeType"
	case RELKIND_FOREIGN_TABLE:
		return "ForeignTable"
	case RELKIND_PARTITIONED_TABLE:
		return "PartitionedTable"
	case RELKIND_PARTITIONED_INDEX:
		return "PartitionedIndex"
	default:
		panic(fmt.Sprintf("Unknown RelKind %d", k))
	}
}
