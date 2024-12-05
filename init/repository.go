package psqlinit

import (
	"context"
	"fmt"
	"io/fs"

	"github.com/jackc/pgx/v5"
)

type Repository struct {
	initSteps []initStep
}

type initStep struct {
	script   InitScript
	skipWhen []Condition
}

var globalRepository = Repository{}

func (s *Repository) Copy() *Repository {
	if s == nil {
		s = &globalRepository
	}
	initSteps := make([]initStep, len(s.initSteps))
	copy(initSteps, s.initSteps)

	return &Repository{initSteps: initSteps}
}

func (s *Repository) Add(script InitScript, skipWhen ...Condition) {
	if s == nil {
		s = &globalRepository
	}
	s.initSteps = append(s.initSteps, initStep{script, skipWhen})
}

func Add(script InitScript, skipWhen ...Condition) {
	globalRepository.Add(script, skipWhen...)
}

func (s *Repository) AddSql(name string, sql string, skipWhen ...Condition) {
	if s == nil {
		s = &globalRepository
	}
	s.Add(InitSql(name, sql), skipWhen...)
}

func AddSql(name string, sql string, skipWhen ...Condition) {
	globalRepository.AddSql(name, sql, skipWhen...)
}

func (s *Repository) AddSqlFile(fs fs.FS, filename string, skipWhen ...Condition) {
	if s == nil {
		s = &globalRepository
	}
	s.Add(InitSqlFile(fs, filename), skipWhen...)
}

func AddSqlFile(fs fs.FS, filename string, skipWhen ...Condition) {
	globalRepository.AddSqlFile(fs, filename, skipWhen...)
}

func (s *Repository) AddFn(name string, impl func(context.Context, *pgx.Conn) error, skipWhen ...Condition) {
	if s == nil {
		s = &globalRepository
	}
	s.Add(InitFn(name, impl), skipWhen...)
}

func (s *Repository) steps() []initStep {
	if s == nil {
		s = &globalRepository
	}
	return s.initSteps
}

func AddFn(name string, impl func(context.Context, *pgx.Conn) error, skipWhen ...Condition) {
	globalRepository.AddFn(name, impl, skipWhen...)
}

func (k *initStep) firstMatchingCondition(ctx context.Context, conn *pgx.Conn) (int, *Condition, error) {
	for i, c := range k.skipWhen {
		matches, err := c.Evaluate(ctx, conn)
		if err != nil {
			return i, &c, fmt.Errorf("condition '%s': %v", c.Description(), err)
		}

		if matches {
			return i, &c, nil
		}
	}

	return -1, nil, nil
}

func (k *initStep) matchingConditions(ctx context.Context, conn *pgx.Conn) ([]Condition, error) {
	res := make([]Condition, 0, len(k.skipWhen))
	for _, c := range k.skipWhen {
		matches, err := c.Evaluate(ctx, conn)
		if err != nil {
			return nil, fmt.Errorf("condition '%s': %v", c.Description(), err)
		}

		if matches {
			res = append(res, c)
		}
	}

	return res, nil
}

type initCondResult struct {
	cond    Condition
	matches bool
	err     error
}

func (k *initStep) evalConditions(ctx context.Context, conn *pgx.Conn) []initCondResult {
	res := make([]initCondResult, len(k.skipWhen))
	for i, c := range k.skipWhen {
		matches, err := c.Evaluate(ctx, conn)
		res[i] = initCondResult{c, matches, err}
	}
	return res
}

func (k *initStep) evalConditionsTillFirstMatch(ctx context.Context, conn *pgx.Conn) ([]initCondResult, bool, error) {
	res := make([]initCondResult, 0, len(k.skipWhen))
	for _, c := range k.skipWhen {
		matches, err := c.Evaluate(ctx, conn)
		res = append(res, initCondResult{c, matches, err})
		if err != nil {
			return res, false, fmt.Errorf("condition '%s': %v", c.Description(), err)
		}

		if matches {
			return res, true, nil
		}
	}
	return res, false, nil
}
