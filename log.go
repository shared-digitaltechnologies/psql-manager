package psqlmanager

import (
	"fmt"
	"strings"
)

type LogColorMode int8

const (
	COLOR_NEVER  LogColorMode = -1
	COLOR_AUTO   LogColorMode = 0
	COLOR_ALWAYS LogColorMode = 1
)

func (c *LogColorMode) String() string {
	if *c > 0 {
		return "always"
	} else if *c < 0 {
		return "never"
	} else {
		return "auto"
	}
}

func (c *LogColorMode) Set(val string) error {
	switch strings.ToLower(val) {
	case "always":
		*c = COLOR_ALWAYS
		return nil
	case "never":
		*c = COLOR_NEVER
		return nil
	case "auto":
		*c = COLOR_AUTO
		return nil
	default:
		return fmt.Errorf("Invalid color mode '%s'. Valid color modes are 'never', 'auto' or 'always'", val)
	}
}

func (c *LogColorMode) Type() string {
	return "string"
}

type LogVerbosity int8

const (
	LOG_QUIET   LogVerbosity = -1
	LOG_DEFAULT LogVerbosity = 0
	LOG_VERBOSE LogVerbosity = 1
)
