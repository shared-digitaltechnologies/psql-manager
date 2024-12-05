package psqlmigrate

import (
	"context"
	"fmt"

	"github.com/pressly/goose/v3"
)

type AllowedDirection uint8

const (
	ALLOW_NONE AllowedDirection = 0
	ALLOW_UP   AllowedDirection = 0b01
	ALLOW_DOWN AllowedDirection = 0b10
	ALLOW_BOTH AllowedDirection = 0b11
)

func (d AllowedDirection) String() string {
	switch d {
	case ALLOW_NONE:
		return "none"
	case ALLOW_DOWN:
		return "down"
	case ALLOW_UP:
		return "up"
	case ALLOW_BOTH:
		return "up or down"
	default:
		panic("Invalid allowed migration direction")
	}
}

func (d AllowedDirection) AllowsUp() bool {
	return d&ALLOW_UP > 0
}

func (d AllowedDirection) AllowsDown() bool {
	return d&ALLOW_DOWN > 0
}

type MigrateAction interface {
	String() string
	RunUsing(ctx context.Context, runner *Runner) ([]*goose.MigrationResult, error)
}

type migrateTo struct {
	AllowedDirection
	Version int64
}

func MigrateToActionWithAllow(version int64, allowedDirection AllowedDirection) MigrateAction {
	return &migrateTo{
		AllowedDirection: allowedDirection,
		Version:          version,
	}
}

func MigrateToAction(version int64) MigrateAction {
	return &migrateTo{
		AllowedDirection: ALLOW_BOTH,
		Version:          version,
	}
}

func UpToAction(version int64) MigrateAction {
	return &migrateTo{
		AllowedDirection: ALLOW_UP,
		Version:          version,
	}
}

func DownToAction(version int64) MigrateAction {
	return &migrateTo{
		AllowedDirection: ALLOW_DOWN,
		Version:          version,
	}
}

func (m *migrateTo) String() string {
	return fmt.Sprintf("Migrate %s to version %d", m.AllowedDirection.String(), m.Version)
}

func (m *migrateTo) RunUsing(ctx context.Context, runner *Runner) ([]*goose.MigrationResult, error) {

	version, err := runner.GetDBVersion(ctx)
	if err != nil {
		return nil, err
	}

	if version == m.Version {
		return nil, nil
	}

	switch m.AllowedDirection {
	case ALLOW_NONE:
		return nil, fmt.Errorf("Current version %d and target version %d do not match", version, m.Version)

	case ALLOW_UP:
		if version > m.Version {
			return nil, fmt.Errorf("Could not migrate up. Current version %d is newer than target version %d", version, m.Version)
		}

		return runner.UpTo(ctx, version)

	case ALLOW_DOWN:
		if version < m.Version {
			return nil, fmt.Errorf("Could not migrate down. Current version %d is older than target version %d", version, m.Version)
		}
		return runner.DownTo(ctx, version)

	case ALLOW_BOTH:
		if version < m.Version {
			return runner.DownTo(ctx, version)
		} else {
			return runner.UpTo(ctx, version)
		}

	default:
		panic("Invalid allowed migration direction")
	}
}

type migrateBy struct {
	delta int64
}

func UpByAction(delta int64) MigrateAction {
	return &migrateBy{delta}
}

func DownByAction(delta int64) MigrateAction {
	return &migrateBy{-delta}
}

func (m *migrateBy) String() string {
	if m.delta == 0 {
		return fmt.Sprintf("Do nothing")
	} else if m.delta < 0 {
		return fmt.Sprintf("Migrate down %d versions", m.delta)
	} else {
		return fmt.Sprintf("Migrate up %d version", m.delta)
	}
}

func (m *migrateBy) RunUsing(ctx context.Context, runner *Runner) ([]*goose.MigrationResult, error) {
	if m.delta < 0 {
		return runner.DownBy(ctx, -m.delta)
	} else if m.delta == 0 {
		return nil, nil
	} else {
		return runner.UpBy(ctx, m.delta)
	}
}

type redo struct {
	amount int64
}

func RedoAction(amount int64) MigrateAction {
	if amount < 0 {
		amount = -amount
	}
	return &migrateBy{amount}
}

func (m *redo) String() string {
	return fmt.Sprintf("Redo %d migrations", m.amount)
}

func (m *redo) RunUsing(ctx context.Context, runner *Runner) ([]*goose.MigrationResult, error) {
	if m.amount == 0 {
		return nil, nil
	}

	return runner.Redo(ctx, m.amount)
}

type upToLatest struct{}

var UpToLatestAction upToLatest

func (upToLatest) String() string {
	return fmt.Sprintf("Migrate up to latest version")
}

func (upToLatest) RunUsing(ctx context.Context, runner *Runner) ([]*goose.MigrationResult, error) {
	res, err := runner.Up(ctx)
	return res, err
}

type reset struct{}

var ResetAction reset

func (reset) String() string {
	return fmt.Sprintf("Rollback all migrations")
}

func (reset) RunUsing(ctx context.Context, runner *Runner) ([]*goose.MigrationResult, error) {
	return runner.Reset(ctx)
}
