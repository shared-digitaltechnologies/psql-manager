package cli

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/shared-digitaltechnologies/psql-manager/db"
	"github.com/shared-digitaltechnologies/psql-manager/migrate"
)

func parseNameAtVersionArg(argval string, database *db.Database, migrateAction psqlmigrate.MigrateAction) error {
	argParts := strings.Split(argval, "@")

	if len(argParts) > 2 {
		return fmt.Errorf("Invalid version argument '%s': more than one '@' character.", argval)
	}

	if len(argParts) > 0 {
		database.Name = argParts[0]
	}

	if len(argParts) > 1 {
		versionInput := argParts[1]
		version, err := strconv.Atoi(versionInput)
		if err != nil {
			return fmt.Errorf("Invalid version input '%s': %v", versionInput, err)
		}
		migrateAction = psqlmigrate.UpToAction(int64(version))
	}

	return nil
}
