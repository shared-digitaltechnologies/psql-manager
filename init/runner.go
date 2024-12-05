package psqlinit

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
)

type Runner struct {
	*Repository

	LogLevel         LogLevel
	IgnoreConditions bool
}

var GlobalRunner = Runner{LogLevel: NAMES_AND_EVALUATED_CONDITIONS}

func (r *Runner) runScript(ctx context.Context, conn *pgx.Conn, script InitScript) (dur time.Duration, err error) {
	tic := time.Now()
	err = script.Apply(ctx, conn)
	toc := time.Now()

	dur = toc.Sub(tic)

	return
}

func (r *Runner) Run(ctx context.Context, conn *pgx.Conn) error {
	var err error

	if r == nil {
		r = &GlobalRunner
	}

	alwaysRun := r.IgnoreConditions

	for _, step := range r.Repository.steps() {
		// Initialize report for later logging.
		rep := runReport{step: &step}

		// Evaluate conditions to determine if the init step should be skipped.
		skip := false
		if r.LogLevel == NAMES_AND_ALL_CONDITIONS {
			rep.conditions = step.evalConditions(ctx, conn)
			for _, c := range rep.conditions {
				if c.err != nil {
					return fmt.Errorf("InitScript '%s': condition '%s': %v",
						step.script.Name(),
						c.cond.Description(),
						c.err,
					)
				}
			}

			for _, c := range rep.conditions {
				if c.matches && !alwaysRun {
					skip = true
				}
			}
		} else if !alwaysRun {
			rep.conditions, skip, err = step.evalConditionsTillFirstMatch(ctx, conn)
			if err != nil {
				return fmt.Errorf("InitScript '%s': %v", step.script.Name(), err)
			}
		}

		// Apply or skip step
		if skip {
			rep.status = SKIPPED
		} else {
			rep.duration, err = r.runScript(ctx, conn, step.script)
			if err != nil {
				rep.status = FAILED
				return fmt.Errorf("InitScript.Apply '%s': %v", step.script.Name(), err)
			}
			rep.status = APPLIED
			alwaysRun = true
		}

		// Log Results
		if r.LogLevel >= NAMES_ONLY {
			fmt.Printf("    %s\n", rep.String())
		}
		if r.LogLevel >= NAMES_AND_EVALUATED_CONDITIONS {
			rep.printConditions()
		}
	}

	return nil
}

type LogLevel int8

const (
	NONE LogLevel = iota - 1
	NAMES_ONLY
	NAMES_AND_EVALUATED_CONDITIONS
	NAMES_AND_ALL_CONDITIONS
)

type InitStepStatus int8

const (
	RUNNING InitStepStatus = iota
	APPLIED
	SKIPPED
	FAILED
)

func (s InitStepStatus) String() string {
	switch s {
	case RUNNING:
		return "RUNNING"
	case APPLIED:
		return "OK"
	case SKIPPED:
		return "SKIPPED"
	case FAILED:
		return "FAILED"
	default:
		panic("Unknown InitStepStatus")
	}

}

type runReport struct {
	step       *initStep
	status     InitStepStatus
	duration   time.Duration
	conditions []initCondResult
}

func (s *runReport) String() string {
	if s.status == SKIPPED {
		return fmt.Sprintf(
			"%-6s %-40s",
			&s.status,
			s.step.script.Name(),
		)
	} else {
		return fmt.Sprintf(
			"%-5s %-40s (%6.2fms)",
			&s.status,
			s.step.script.Name(),
			s.duration.Seconds()*1000,
		)
	}
}

func (s *runReport) printConditions() {
	for _, c := range s.conditions {
		var prefix rune
		if c.err != nil {
			prefix = '!'
		} else if c.matches {
			prefix = '✓'
		} else {
			prefix = '✗'
		}

		fmt.Printf("    %s %s\n", string(prefix), c.cond.Description())
		if c.err != nil {
			fmt.Printf("        %v\n", c.err)
		}
	}
}
