package psqlseed

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/shared-digitaltechnologies/psql-manager/db"
	"github.com/shared-digitaltechnologies/psql-manager/seed/fake"
)

// SEEDER STATUS
type SeederStatus int

const (
	RUNNING SeederStatus = iota
	OK
	FAIL
	TXERR
)

func (s SeederStatus) String() string {
	switch s {
	case RUNNING:
		return "RUNNING"
	case OK:
		return "OK"
	case FAIL:
		return "FAIL"
	case TXERR:
		return "TXERR"
	default:
		return "???"
	}
}

// LOG LEVEL
type LogLevel int

const (
	NONE LogLevel = iota - 1
	RUNS
)

// RUN REPORT
type RunReport struct {
	Seeder    Seeder
	Seed      fake.Seed
	Status    SeederStatus
	Err       error
	StartedAt time.Time
	EndedAt   time.Time
}

func (s *RunReport) Duration() time.Duration {
	return s.EndedAt.Sub(s.StartedAt)
}

func (s *RunReport) String() string {
	if s.Err == nil {
		return fmt.Sprintf(
			"    %-5s %-40s (%6.2fms)",
			s.Status.String(),
			s.Seeder.Name(),
			s.Duration().Seconds()*1000,
		)
	} else {
		return fmt.Sprintf(
			"    %-5s %-40s (%6.2fms)\n    Err: %v",
			s.Status.String(),
			s.Seeder.Name(),
			s.Duration().Seconds()*1000,
			s.Err,
		)
	}
}

// RUNNER
type Runner struct {
	*Repository
	Seed fake.Seed

	RunReports  []*RunReport
	LogLevel    LogLevel
	BailOnError bool
}

func (r *Runner) SeedersNamed(seederNames ...string) error {
	store, err := r.Repository.SubStore(seederNames...)
	r.Repository = &store
	return err
}

func (r *Runner) SeedersTill(seederNamePrefix string) {
	store := r.Repository.SubStoreTill(seederNamePrefix)
	r.Repository = &store
}

func (r *Runner) Run(ctx context.Context, conn *pgx.Conn) error {
	seeders := r.Repository.Seeders()
	for _, seeder := range seeders {
		seeder.Prepare(ctx, r.Seed)
	}

	for _, seeder := range seeders {
		err := r.runSeeder(ctx, conn, seeder)
		if err != nil {
			return fmt.Errorf("Seeder '%s' Run: %w", seeder, err)
		}
	}

	return nil
}

func (r *Runner) runSeeder(ctx context.Context, conn *pgx.Conn, seeder Seeder) error {
	var err error
	var txerr error

	report := RunReport{
		Seeder: seeder,
		Seed:   r.Seed,
		Status: RUNNING,
	}

	r.RunReports = append(r.RunReports, &report)

	tx := db.Tx{}
	tx.Tx, txerr = conn.BeginTx(ctx, pgx.TxOptions{})
	if txerr != nil {
		report.Status = TXERR
		return fmt.Errorf("conn.BeginTx failed: %v", txerr)
	}

	report.StartedAt = time.Now()
	err = seeder.RunSeederTx(ctx, r.Seed, tx)
	report.EndedAt = time.Now()

	if err != nil {
		report.Status = FAIL
		report.Err = err
		txerr = tx.Rollback(ctx)
		if txerr != nil {
			report.Status = TXERR
			txerr = fmt.Errorf("tx.Rollback failed: %v\nSeeder Error: %v", txerr, err)
		}
	} else {
		report.Status = OK
		txerr = tx.Commit(ctx)
		if txerr != nil {
			report.Status = TXERR
			report.Err = txerr
			txerr = fmt.Errorf("tx.Commit failed: %v", txerr)
		}
	}

	if r.LogLevel > NONE {
		fmt.Printf("%s\n", &report)
	}

	if r.BailOnError && report.Err != nil {
		return err
	}

	return txerr
}
