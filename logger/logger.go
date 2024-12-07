package logger

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"os"

	"github.com/datazip-inc/olake/logger/console"
	"github.com/datazip-inc/olake/types"
)

// Info writes record into os.stdout with log level INFO
func Info(v ...interface{}) {
	console.Log("", console.INFO, v...)
}

// Info writes record into os.stdout with log level INFO
func Infof(format string, v ...interface{}) {
	console.Log(format, console.INFO, v...)
}

// Debug writes record into os.stdout with log level DEBUG
func Debug(v ...interface{}) {
	console.Log("", console.DEBUG, v...)
}

// Debugf writes record into os.stdout with log level DEBUG
func Debugf(format string, v ...interface{}) {
	console.Log(format, console.DEBUG, v...)
}

// Error writes record into os.stdout with log level ERROR
func Error(v ...interface{}) {
	console.Log("", console.ERROR, v...)
}

// Fatal writes record into os.stdout with log level ERROR and exits
func Fatal(v ...interface{}) {
	console.Log("", console.ERROR, v...)
	os.Exit(1)
}

// Fatal writes record into os.stdout with log level ERROR
func Fatalf(format string, v ...interface{}) {
	console.Log(format, console.ERROR, v...)
	os.Exit(1)
}

// Error writes record into os.stdout with log level ERROR
func Errorf(format string, v ...interface{}) {
	console.Log(format, console.ERROR, v...)
}

// Warn writes record into os.stdout with log level WARN
func Warn(v ...interface{}) {
	console.Log("", console.WARN, v...)
}

// Warn writes record into os.stdout with log level WARN
func Warnf(format string, v ...interface{}) {
	console.Log(format, console.WARN, v...)
}

func LogSpec(spec map[string]interface{}) {
	message := types.Message{}
	message.Spec = spec
	message.Type = types.SpecMessage

	Info("logging spec")
	err := console.Print(console.INFO, message)
	if err != nil {
		Fatalf("failed to encode spec %v: %s", spec, err)
	}
}

func LogCatalog(streams []*types.Stream) {
	message := types.Message{}
	message.Type = types.CataLogMessage
	message.Catalog = types.GetWrappedCatalog(streams)
	Info("logging catalog")
	err := console.Print(console.INFO, message)
	if err != nil {
		Fatalf("failed to encode catalog %v: %s", streams, err)
	}
}

func LogConnectionStatus(err error) {
	message := types.Message{}
	message.Type = types.ConnectionStatusMessage
	message.ConnectionStatus = &types.StatusRow{}
	if err != nil {
		message.ConnectionStatus.Message = err.Error()
		message.ConnectionStatus.Status = types.ConnectionFailed
	} else {
		message.ConnectionStatus.Status = types.ConnectionSucceed
	}

	err = console.Print(console.INFO, message)
	if err != nil {
		Fatalf("failed to encode connection status: %s", err)
	}
}

func LogResponse(response *http.Response) {
	respDump, err := httputil.DumpResponse(response, true)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(respDump))
}

func LogRequest(req *http.Request) {
	requestDump, err := httputil.DumpRequest(req, true)
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(string(requestDump))
}

func LogState(state *types.State) {
	state.Lock()
	defer state.Unlock()

	message := types.Message{}
	message.Type = types.StateMessage
	message.State = state

	err := console.Print(console.INFO, message)
	if err != nil {
		Fatalf("failed to encode connection status: %s", err)
	}
}
