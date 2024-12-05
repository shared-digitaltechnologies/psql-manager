package db

import (
	"context"
	"fmt"
)

// DEBUG
func printHline() {
	for j := 0; j < 120; j++ {
		fmt.Print("-")
	}
	fmt.Print("\n")
}

func (tx Tx) DebugQueryFormat(ctx context.Context, format string, sql string, args ...any) {
	row := tx.MustQuery(ctx, sql, args...)

	printHline()
	fmt.Println("  DEBUG SQL QUERY  `" + format + "`")
	printHline()
	fmt.Println("\033[35m" + sql + "\033[0m")
	fmt.Println()

	i := 0
	for row.Next() {
		i++
		v, err := row.Values()
		if err != nil {
			panic(fmt.Errorf("ERROR AT ROW %d: %v", i, err))
		}

		fmt.Printf("\033[0;90m %4d | \033[0m", i)
		fmt.Printf(format+"\n", v...)
	}

	printHline()
	fmt.Printf("   TOTAL ROWS: %d   \n", i)
	printHline()
}
