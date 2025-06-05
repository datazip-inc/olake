package generator

import (
	"github.com/datazip-inc/olake/utils/logger"
)

// LogLevel is an enum specifying log verbosity
type LogLevel uint8

const (
	// QuietLevel level. Makes everything silent.
	QuietLevel LogLevel = iota
	// InfoLevel level.
	InfoLevel
	// DebugLevel level. Usually only enabled when debugging. chatty logging.
	DebugLevel
	// VerboseLevel level. Usually only enabled when debugging. Very verbose logging.
	VerboseLevel
)

// LogInfo writes an info message to std out
func (g *JSONSchemaGenerator) LogInfo(args ...interface{}) {
	if g.options.LogLevel >= InfoLevel {
		logger.Info(args...)
	}
}

// LogInfoF writes an info formatted message to std out
func (g *JSONSchemaGenerator) LogInfoF(format string, args ...interface{}) {
	if g.options.LogLevel >= InfoLevel {
		logger.Infof(format, args...)
	}
}

// LogDebug writes a debug message to std out
func (g *JSONSchemaGenerator) LogDebug(args ...interface{}) {
	if g.options.LogLevel >= DebugLevel {
		logger.Debug(args...)
	}
}

// LogDebugF writes a debug formatted message to std out
func (g *JSONSchemaGenerator) LogDebugF(format string, args ...interface{}) {
	if g.options.LogLevel >= DebugLevel {
		logger.Debugf(format, args...)
	}
}

// LogVerbose writes a verbose message to std out
func (g *JSONSchemaGenerator) LogVerbose(args ...interface{}) {
	if g.options.LogLevel >= VerboseLevel {
		logger.Debug(args...)
	}
}

// LogVerboseF writes a verbose formatted message to std out
func (g *JSONSchemaGenerator) LogVerboseF(format string, args ...interface{}) {
	if g.options.LogLevel >= VerboseLevel {
		logger.Debugf(format, args...)
	}
}
