package psqlmigrate

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/pressly/goose/v3"
)

type Runner struct {
	*goose.Provider
}

type MigrateActionResult struct {
	Action   MigrateAction
	Results  []*goose.MigrationResult
	Duration time.Duration
	Err      error
}

func (r *MigrateActionResult) Title() string {
	if r == nil {
		return ">> [NIL] MIGRATE ACTION"
	}

	status := "SUCCESS"
	if r.Err != nil {
		status = "FAILED"
	}

	title := "nil"
	if r.Action != nil {
		title = r.Action.String()
	}

	durMs := r.Duration.Seconds() * 1000

	return fmt.Sprintf(">> [%s] MIGRATE ACTION '%s' (%fms)", status, title, durMs)
}

func (r *MigrateActionResult) ResultSummary(prefix string) string {
	if r == nil {
		return ""
	}

	parts := make([]string, len(r.Results))

	for i, r := range r.Results {
		parts[i] = prefix + fmt.Sprintf("%s", r.String())
	}

	return strings.Join(parts, "\n")
}

func (r *MigrateActionResult) String() string {
	title := r.Title()

	if r == nil {
		return title
	}

	return title + "\n" + r.ResultSummary("    ")
}

func (r *Runner) Run(ctx context.Context, action MigrateAction) (*MigrateActionResult, error) {
	start := time.Now()
	results, err := action.RunUsing(ctx, r)
	end := time.Now()

	result := &MigrateActionResult{
		Action:   action,
		Results:  results,
		Duration: end.Sub(start),
		Err:      err,
	}

	return result, err
}

func (r *Runner) Close() error {
	return r.Provider.Close()
}

func (r *Runner) getUpSources(ctx context.Context) ([]*goose.Source, error) {
	dbVersion, err := r.Provider.GetDBVersion(ctx)
	if err != nil {
		return nil, err
	}

	sources := r.Provider.ListSources()
	lastDbVersionIx := 0

	for i, s := range sources {
		if s.Version <= dbVersion {
			lastDbVersionIx = i
		}
	}

	return sources[lastDbVersionIx:], nil
}

func (r *Runner) UpBy(ctx context.Context, delta int64) ([]*goose.MigrationResult, error) {
	if delta <= 0 {
		return nil, nil
	}

	upSources, err := r.getUpSources(ctx)
	if err != nil {
		return nil, err
	}

	if int64(len(upSources)) <= delta {
		return r.Up(ctx)
	}

	targetVersion := upSources[delta].Version
	return r.UpTo(ctx, targetVersion)
}

func (r *Runner) AppliedVersions(ctx context.Context) ([]int64, error) {
	statuses, err := r.Status(ctx)
	if err != nil {
		return nil, err
	}

	count := 0
	for _, status := range statuses {
		if status.State == goose.StateApplied {
			count++
		}
	}

	versions := make([]int64, count)
	i := 0
	for _, status := range statuses {
		if status.State == goose.StateApplied {
			versions[i] = status.Source.Version
			i++
		}
	}

	return versions, nil
}

func (r *Runner) downVersionByDelta(ctx context.Context, currentVersion int64, delta int64) (version int64, err error) {
	if delta <= 0 {
		return currentVersion, nil
	}

	appliedVersions, err := r.AppliedVersions(ctx)
	if err != nil {
		return currentVersion, err
	}

	if len(appliedVersions) == 0 {
		return currentVersion, nil
	}

	ix := len(appliedVersions) - 1 - int(delta)
	if ix < 0 {
		return 0, nil
	}

	return appliedVersions[ix], nil
}

func (r *Runner) DownBy(ctx context.Context, delta int64) ([]*goose.MigrationResult, error) {
	if delta <= 0 {
		return nil, nil
	}

	currentVersion, err := r.GetDBVersion(ctx)
	if err != nil {
		return nil, err
	}
	version, err := r.downVersionByDelta(ctx, currentVersion, delta)
	if err != nil || currentVersion == version {
		return nil, err
	}

	return r.DownTo(ctx, version)
}

func (r *Runner) Redo(ctx context.Context, delta int64) ([]*goose.MigrationResult, error) {
	if delta <= 0 {
		return nil, nil
	}

	currentVersion, err := r.GetDBVersion(ctx)
	if err != nil {
		return nil, err
	}
	targetVersion, err := r.downVersionByDelta(ctx, currentVersion, delta)
	if err != nil || currentVersion == targetVersion {
		return nil, err
	}

	downResults, downErr := r.DownTo(ctx, targetVersion)
	upResults, upErr := r.UpTo(ctx, currentVersion)

	results := append(downResults, upResults...)

	return results, errors.Join(downErr, upErr)
}

func (r *Runner) Reset(ctx context.Context) ([]*goose.MigrationResult, error) {
	return r.DownTo(ctx, 0)
}
