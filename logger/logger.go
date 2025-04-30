package logger

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httputil"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"strings"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/spf13/viper"
	"gopkg.in/natefinch/lumberjack.v2"
)

var logger zerolog.Logger

// Info writes record into os.stdout with log level INFO
func Info(v ...interface{}) {
	if len(v) == 1 {
		logger.Info().Interface("message", v[0]).Send()
	} else {
		logger.Info().Msgf("%s", v...)
	}
}

// Info writes record into os.stdout with log level INFO
func Infof(format string, v ...interface{}) {
	logger.Info().Msgf(format, v...)
}

// Debug writes record into os.stdout with log level DEBUG
func Debug(v ...interface{}) {
	logger.Debug().Msgf("%s", v...)
}

// Debugf writes record into os.stdout with log level DEBUG
func Debugf(format string, v ...interface{}) {
	logger.Debug().Msgf(format, v...)
}

// Error writes record into os.stdout with log level ERROR
func Error(v ...interface{}) {
	logger.Error().Msgf("%s", v...)
}

// Fatal writes record into os.stdout with log level ERROR and exits
func Fatal(v ...interface{}) {
	logger.Fatal().Msgf("%s", v...)
	os.Exit(1)
}

// Fatal writes record into os.stdout with log level ERROR
func Fatalf(format string, v ...interface{}) {
	logger.Fatal().Msgf(format, v...)
	os.Exit(1)
}

// Error writes record into os.stdout with log level ERROR
func Errorf(format string, v ...interface{}) {
	logger.Error().Msgf(format, v...)
}

// Warn writes record into os.stdout with log level WARN
func Warn(v ...interface{}) {
	logger.Warn().Msgf("%s", v...)
}

// Warn writes record into os.stdout with log level WARN
func Warnf(format string, v ...interface{}) {
	logger.Warn().Msgf(format, v...)
}

func LogResponse(response *http.Response) {
	respDump, err := httputil.DumpResponse(response, true)
	if err != nil {
		Fatal(err)
	}

	fmt.Println(string(respDump))
}

func LogRequest(req *http.Request) {
	requestDump, err := httputil.DumpRequest(req, true)
	if err != nil {
		Fatal(err)
	}

	fmt.Println(string(requestDump))
}

// CreateFile creates a new file or overwrites an existing one with the specified filename, path, extension,
func FileLogger(content any, fileName, fileExtension string) error {
	// get config folder
	filePath := viper.GetString("CONFIG_FOLDER")
	if filePath == "" {
		return fmt.Errorf("config folder is not set")
	}
	// Construct the full file path
	contentBytes, err := json.Marshal(content)
	if err != nil {
		return fmt.Errorf("failed to marshal content: %s", err)
	}

	fullPath := filepath.Join(filePath, fileName+fileExtension)

	// Create or truncate the file
	file, err := os.OpenFile(fullPath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("failed to create or open file: %s", err)
	}
	defer file.Close()

	// Write data to the file
	_, err = file.Write(contentBytes)
	if err != nil {
		return fmt.Errorf("failed to write data to file: %s", err)
	}

	return nil
}

func StatsLogger(ctx context.Context, statsFunc func() (int64, int64, int64)) {
	startTime := time.Now()
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				Info("Monitoring stopped")
				return
			case <-ticker.C:
				syncedRecords, runningThreads, recordsToSync := statsFunc()
				memStats := new(runtime.MemStats)
				runtime.ReadMemStats(memStats)
				speed := float64(syncedRecords) / time.Since(startTime).Seconds()
				timeElapsed := time.Since(startTime).Seconds()
				remainingRecords := recordsToSync - syncedRecords
				estimatedSeconds := "Not Determined"
				if speed > 0 && remainingRecords >= 0 {
					estimatedSeconds = fmt.Sprintf("%.2f s", float64(remainingRecords)/speed)
				}
				stats := map[string]interface{}{
					"Running Threads":          runningThreads,
					"Synced Records":           syncedRecords,
					"Memory":                   fmt.Sprintf("%d mb", memStats.HeapInuse/(1024*1024)),
					"Speed":                    fmt.Sprintf("%.2f rps", speed),
					"Seconds Elapsed":          fmt.Sprintf("%.2f", timeElapsed),
					"Estimated Remaining Time": estimatedSeconds,
				}
				if err := FileLogger(stats, "stats", ".json"); err != nil {
					Fatalf("failed to write stats in file: %s", err)
				}
			}
		}
	}()
}

func Init() {
	// Configure lumberjack for log rotation
	currentTimestamp := time.Now().UTC()
	timestamp := fmt.Sprintf("%d-%02d-%02d_%02d-%02d-%02d", currentTimestamp.Year(), currentTimestamp.Month(), currentTimestamp.Day(), currentTimestamp.Hour(), currentTimestamp.Minute(), currentTimestamp.Second())
	rotatingFile := &lumberjack.Logger{
		Filename:   fmt.Sprintf("%s/logs/sync_%s/olake.log", viper.GetString("CONFIG_FOLDER"), timestamp), // Log file path
		MaxSize:    100,                                                                                   // Max size in MB before log rotation
		MaxBackups: 5,                                                                                     // Max number of old log files to retain
		MaxAge:     30,                                                                                    // Max age in days to retain old log files
		Compress:   true,                                                                                  // Compress old log files
	}
	zerolog.TimestampFunc = func() time.Time {
		return time.Now().UTC()
	}
	var currentLevel string
	// LogColors defines ANSI color codes for log levels
	var logColors = map[string]string{
		"debug": "\033[36m", // Cyan
		"info":  "\033[32m", // Green
		"warn":  "\033[33m", // Yellow
		"error": "\033[31m", // Red
		"fatal": "\033[31m", // Red
	}
	// Create console writer
	console := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: "2006-01-02 15:04:05",
		FormatLevel: func(i interface{}) string {
			level := i.(string)
			currentLevel = level
			color := logColors[level]
			return fmt.Sprintf("%s%s\033[0m", color, strings.ToUpper(level))
		},
		FormatMessage: func(i interface{}) string {
			msg := ""
			switch v := i.(type) {
			case string:
				msg = v
			default:
				jsonMsg, err := json.Marshal(v)
				if err != nil {
					Errorf("failed to marshal log message: %s", err)
					return err.Error()
				}
				return string(jsonMsg)
			}
			// Get the current log level from the context
			if currentLevel == zerolog.ErrorLevel.String() || currentLevel == zerolog.FatalLevel.String() {
				msg = fmt.Sprintf("\033[31m%s\033[0m", msg) // Make entire message red for error level
			}
			return msg
		},
		FormatTimestamp: func(i interface{}) string {
			return fmt.Sprintf("\033[90m%s\033[0m", i)
		},
	}
	// Create a multiwriter to log both console and file
	multiwriter := zerolog.MultiLevelWriter(console, rotatingFile)

	logger = zerolog.New(multiwriter).With().Timestamp().Logger()
}

// ProcessOutputReader is a struct that manages reading output from a process
// and forwarding it to the logger
type ProcessOutputReader struct {
	Name      string // Name to identify the process in logs
	IsError   bool   // Whether this reader handles error output
	reader    *bufio.Scanner
	closeFn   func() error
	closeOnce sync.Once
}

// NewProcessOutputReader creates a new ProcessOutputReader for a given process
// name is a prefix to identify the process in logs
// isError determines whether to log as Error (true) or Info (false)
// returns the reader and a write end that should be connected to the process output
func NewProcessLogger(name string, isError bool) (*ProcessOutputReader, *os.File, error) {
	r, w, err := os.Pipe()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create pipe: %v", err)
	}

	reader := &ProcessOutputReader{
		Name:    name,
		IsError: isError,
		reader:  bufio.NewScanner(r),
		closeFn: r.Close,
	}

	return reader, w, nil
}

// StartReading starts reading from the process output in a goroutine
// and logging each line with the appropriate log level
func (p *ProcessOutputReader) StartReading() {
	// Compile regex patterns for Java error detection
	errorLinePattern := regexp.MustCompile(`(?i)(ERROR|FATAL|Exception|Error:|Failed to|java\.lang\.\w+Exception|^\s*Caused by:)`)
	stackTraceLinePattern := regexp.MustCompile(`^\s*at\s+[\w$.]+\([\w$]+\.java:\d+\)`)

	go func() {
		defer p.Close()
		// Track if we're in an error stack trace
		inStackTrace := false

		for p.reader.Scan() {
			line := p.reader.Text()

			// Check if this is an error line
			isErrorLine := p.IsError || errorLinePattern.MatchString(line)
			isStackTraceLine := stackTraceLinePattern.MatchString(line)

			// Determine if we're starting or continuing an error
			if isErrorLine || isStackTraceLine {
				inStackTrace = true
			} else if inStackTrace && !strings.HasPrefix(strings.TrimSpace(line), "at ") {
				// If this is not a stack trace line and doesn't start with "at ",
				// we're likely out of the stack trace
				inStackTrace = false
			}

			if isErrorLine || isStackTraceLine || inStackTrace {
				Error(fmt.Sprintf("[%s] %s", p.Name, line))
			} else {
				Info(fmt.Sprintf("[%s] %s", p.Name, line))
			}
		}
	}()
}

// Close closes the reader
func (p *ProcessOutputReader) Close() {
	p.closeOnce.Do(func() {
		if p.closeFn != nil {
			if err := p.closeFn(); err != nil {
				fmt.Printf("Error closing ProcessOutputReader: %v\n", err)
			}
		}
	})
}

// SetupProcessLogger creates stdout and stderr readers for a process
// and returns write-ends that should be connected to the process stdout and stderr
func SetupProcessLogger(processName string) (*ProcessOutputReader, *ProcessOutputReader, *os.File, *os.File, error) {
	// Setup stdout reader
	stdoutReader, stdoutWriter, err := NewProcessLogger(processName, false)
	if err != nil {
		return nil, nil, nil, nil, fmt.Errorf("failed to create stdout reader: %v", err)
	}

	// Setup stderr reader
	stderrReader, stderrWriter, err := NewProcessLogger(processName, true)
	if err != nil {
		stdoutReader.Close()
		if stdoutWriter != nil {
			stdoutWriter.Close()
		}
		return nil, nil, nil, nil, fmt.Errorf("failed to create stderr reader: %v", err)
	}

	return stdoutReader, stderrReader, stdoutWriter, stderrWriter, nil
}

// SetupAndStartProcess creates and starts a process with stdout and stderr logged via the logger.
// It handles the complete process lifecycle including starting the command and managing pipes.
func SetupAndStartProcess(processName string, cmd *exec.Cmd) error {
	// Set up process output capture using the logger utility
	stdoutReader, stderrReader, stdoutWriter, stderrWriter, err := SetupProcessLogger(processName)
	if err != nil {
		return fmt.Errorf("failed to set up process output capture: %v", err)
	}

	// Set the command's stdout and stderr to our pipes
	cmd.Stdout = stdoutWriter
	cmd.Stderr = stderrWriter

	if err := cmd.Start(); err != nil {
		stdoutReader.Close()
		stderrReader.Close()
		stdoutWriter.Close()
		stderrWriter.Close()
		return fmt.Errorf("failed to start process: %v", err)
	}

	// Start reading from the process output
	stdoutReader.StartReading()
	stderrReader.StartReading()

	// Close the write end of the pipes in the parent process
	// since they're only needed by the child process
	stdoutWriter.Close()
	stderrWriter.Close()

	return nil
}
